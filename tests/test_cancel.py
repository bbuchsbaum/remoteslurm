"""WP-E1: stub pool split, client-generated request ids, and `cancel`.

The flagship coverage runs through the *daemon* path (an in-process daemon backed by a
LocalTransport "fake ssh" host, exactly like tests/test_daemon.py): a long `run` in the slow
pool must not block a `ping` in the fast pool, and a `cancel` of the running op must kill its
whole process group and return partial output with ``cancelled: true``.
"""

from __future__ import annotations

import os
import tempfile
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from remoteslurm import daemon
from remoteslurm.config import Config
from remoteslurm.errors import RemoteTimeout
from remoteslurm.session import Session
from remoteslurm.transport import LocalTransport

FAKESLURM = Path(__file__).parent / "fakeslurm"


# --------------------------------------------------------------------------- helpers
def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # pragma: no cover - exists but not ours
        return True
    return True


def _wait_for_file(path: Path, timeout: float = 10.0) -> int:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            txt = path.read_text().strip()
            if txt:
                return int(txt)
        except (OSError, ValueError):
            pass
        time.sleep(0.02)
    raise AssertionError(f"{path} never got a pid")


def _wait_dead(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _alive(pid):
            return True
        time.sleep(0.02)
    return False


def _cancel_when_running(sess, rid: str, timeout: float = 5.0):  # type: ignore[no-untyped-def]
    """Poll `cancel` until it actually reaches the (just-spawned) process, or time out.

    There is an unavoidable gap between submitting a slow op and the stub spawning + registering
    its Popen; retrying absorbs it without a fixed sleep.
    """
    deadline = time.time() + timeout
    last = {"cancelled": False, "id": rid, "reason": "not running"}
    while time.time() < deadline:
        last = sess.cancel(rid)
        if last.get("cancelled"):
            return last
        time.sleep(0.05)
    return last


# child that records its own (backgrounded) grandchild pid so we can assert the whole group died
PIDMARKER_CMD = "sleep 30 & echo $! > $HOME/rs_pid; wait"


# --------------------------------------------------------------------------- daemon fixture
@pytest.fixture
def daemon_env(sandbox: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """An in-process daemon serving a 'fake ssh' host that is really a local transport."""
    from remoteslurm import cluster as cluster_mod

    cfg = tmp_path / "config.toml"
    path = f"{FAKESLURM}{os.pathsep}{os.environ['PATH']}"
    cfg.write_text(
        'default_host = "fake"\n[hosts.fake]\nssh = "fakehost"\nmfa = false\n'
        f'[hosts.fake.env]\nHOME = "{sandbox}"\nPATH = "{path}"\n'
    )
    monkeypatch.setenv("REMOTESLURM_CONFIG", str(cfg))
    monkeypatch.setenv("REMOTESLURM_STATE_DIR", str(tmp_path / "state"))
    sock = Path(tempfile.mkdtemp(prefix="rs")) / "d.sock"
    monkeypatch.setenv("REMOTESLURM_SOCKET", str(sock))
    monkeypatch.delenv("REMOTESLURM_NO_DAEMON", raising=False)

    def fake_transport(host):  # type: ignore[no-untyped-def]
        env = {str(k): str(v) for k, v in host.extra.get("env", {}).items()}
        return LocalTransport(env=env)

    monkeypatch.setattr(cluster_mod.Cluster, "_transport_for", staticmethod(fake_transport))
    srv = daemon.DaemonServer(sock, idle_seconds=60, config_path=cfg)
    t = threading.Thread(target=srv.serve, daemon=True)
    t.start()
    try:
        yield sock
    finally:
        srv.shutdown()
        t.join(timeout=5)
        for c in list(cluster_mod._clusters.values()):
            c.close()
        cluster_mod._clusters.clear()


# --------------------------------------------------------------------------- flagship
def test_daemon_pool_separation_and_cancel(daemon_env: Path, sandbox: Path) -> None:
    c = daemon.connect_via_daemon("fake", Config.load(), autostart=False)
    assert c is not None
    rid = Session.new_request_id()
    result: dict[str, object] = {}

    def run_it() -> None:
        result["value"] = c.session.call(
            "run", {"cmd": PIDMARKER_CMD, "timeout": 30}, timeout=30, request_id=rid
        )

    t = threading.Thread(target=run_it, daemon=True)
    t.start()
    child = _wait_for_file(sandbox / "rs_pid")  # run has really started
    assert _alive(child)

    # A ping (fast pool) must not queue behind the 30 s run (slow pool).
    t0 = time.time()
    assert c.ping()["protocol"] == 2
    assert time.time() - t0 < 1.0, "ping blocked behind the slow run -> pools not separated"

    cancel = c.session.cancel(rid)
    assert cancel["cancelled"] is True and cancel["id"] == rid

    t.join(timeout=10)
    assert not t.is_alive()
    run_res = result["value"]
    assert isinstance(run_res, dict) and run_res.get("cancelled") is True
    assert _wait_dead(child), "cancel did not kill the whole process group"


def test_daemon_run_timeout_kills_remote_process(daemon_env: Path, sandbox: Path) -> None:
    """Cluster.run(cancel_on_timeout) through the daemon leaves no process behind on timeout."""
    c = daemon.connect_via_daemon("fake", Config.load(), autostart=False)
    assert c is not None
    marker = sandbox / "rs_pid"
    if marker.exists():
        marker.unlink()
    with pytest.raises(RemoteTimeout):
        c.run(PIDMARKER_CMD, timeout=1)
    child = _wait_for_file(marker)
    assert _wait_dead(child), "the timed-out run left its process group alive"


# --------------------------------------------------------------------------- direct session
def test_pool_separation_direct_session() -> None:
    """On a bare LocalTransport session, a `ping` resolves while a slow `run` is in flight."""
    s = Session(LocalTransport())
    s.start()
    try:
        run_rid = Session.new_request_id()
        run_fut = s.submit("run", {"argv": ["sleep", "5"]}, request_id=run_rid)
        t0 = time.time()
        assert s.call("ping", timeout=2)["protocol"] == 2
        assert time.time() - t0 < 1.0
        assert not run_fut.done()  # the slow op is still running, not blocking us
        _cancel_when_running(s, run_rid)  # kill the sleep so teardown is quick
    finally:
        s.close()


def test_ping_future_resolves_before_slow_run() -> None:
    s = Session(LocalTransport())
    s.start()
    try:
        run_rid = Session.new_request_id()
        run_fut = s.submit("run", {"argv": ["sleep", "5"]}, request_id=run_rid)
        ping_fut = s.submit("ping", {})
        assert ping_fut.result(timeout=2)["protocol"] == 2
        assert not run_fut.done()
        _cancel_when_running(s, run_rid)
    finally:
        s.close()


# --------------------------------------------------------------------------- client ids
def test_request_ids_are_client_generated_and_unique() -> None:
    ids = {Session.new_request_id() for _ in range(5000)}
    assert len(ids) == 5000
    assert all(len(i) == 12 and all(ch in "0123456789abcdef" for ch in i) for i in ids)


def test_explicit_request_id_round_trips() -> None:
    s = Session(LocalTransport())
    s.start()
    try:
        rid = "feedface0001"
        fut = s.submit("run", {"argv": ["sleep", "10"]}, request_id=rid)
        assert rid in s._pending  # the client-chosen id is what the session tracks
        cancel = _cancel_when_running(s, rid)
        assert cancel["cancelled"] is True and cancel["id"] == rid
        assert fut.result(timeout=5).get("cancelled") is True
    finally:
        s.close()


# ------------------------------------------------------------------------- cancel finished/unknown
def test_cancel_unknown_id_is_not_running() -> None:
    s = Session(LocalTransport())
    s.start()
    try:
        r = s.cancel("does-not-exist")
        assert r["cancelled"] is False and r["reason"] == "not running"
    finally:
        s.close()


def test_cancel_finished_id_returns_false() -> None:
    s = Session(LocalTransport())
    s.start()
    try:
        rid = "cafebabe0002"
        fut = s.submit("run", {"argv": ["true"]}, request_id=rid)
        assert fut.result(timeout=5)["rc"] == 0  # let it finish first
        r = s.cancel(rid)
        assert r["cancelled"] is False and r["reason"] == "not running"
    finally:
        s.close()


# --------------------------------------------------------------------------- timeout kills process
def test_run_timeout_kills_remote_process(cluster) -> None:  # type: ignore[no-untyped-def]
    """A client `run(timeout=1)` on a 30 s command times out and leaves nothing lingering."""
    marker = Path(cluster.info()["home"]) / "rs_pid"
    if marker.exists():
        marker.unlink()
    with pytest.raises(RemoteTimeout):
        cluster.run(PIDMARKER_CMD, timeout=1)
    child = _wait_for_file(marker)
    assert _wait_dead(child), "the timed-out run left its process group alive"


def test_run_cancelled_returns_partial_output(cluster) -> None:  # type: ignore[no-untyped-def]
    """A cancelled run comes back with cancelled:true and whatever output it managed to emit."""
    rid = Session.new_request_id()
    box: dict[str, object] = {}

    def run_it() -> None:
        box["v"] = cluster.session.call(
            "run",
            {"cmd": "echo starting; sleep 30", "timeout": 30},
            timeout=30,
            request_id=rid,
        )

    t = threading.Thread(target=run_it, daemon=True)
    t.start()
    # wait until the op is registered and running
    deadline = time.time() + 5
    while time.time() < deadline and rid not in getattr(cluster.session, "_pending", {}):
        time.sleep(0.02)
    time.sleep(0.3)  # let `echo` flush before we kill it
    assert cluster.session.cancel(rid)["cancelled"] is True
    t.join(timeout=10)
    res = box["v"]
    assert isinstance(res, dict)
    assert res.get("cancelled") is True
    assert "starting" in res.get("stdout", "")
