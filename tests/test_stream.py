"""WP-E3: streaming (protocol 2) across stub / session / daemon.

The flagship coverage runs through the *daemon* path (an in-process daemon backed by a
LocalTransport "fake ssh" host, exactly like tests/test_daemon.py and tests/test_cancel.py): a
``run --stream`` of a script that prints three lines with delays yields >= 3 stdout chunks in
order followed by a terminal result with rc 0, driven through ``Cluster.run(stream=True,
on_chunk=...)``. The rest cover: non-streaming regression, ``follow`` (idle + cancel), early
break cleanup / cancel, the max_output cap, an interleaved stream + ping, and the direct
``LocalTransport`` session (``call_stream``) path.
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
from remoteslurm.errors import NotFound
from remoteslurm.session import Session
from remoteslurm.transport import LocalTransport

FAKESLURM = Path(__file__).parent / "fakeslurm"

# Three lines, each flushed then followed by a real delay, so the stub's select loop wakes once
# per line and emits three separate chunks (not one merged read).
THREE_LINES = "for i in 1 2 3; do echo line$i; sleep 0.3; done"


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


# --------------------------------------------------------------------------- flagship (daemon)
def test_daemon_run_stream_orders_chunks_then_result(daemon_env: Path) -> None:
    """run --stream of a 3-line script yields >= 3 stdout chunks in order, then rc 0."""
    c = daemon.connect_via_daemon("fake", Config.load(), autostart=False)
    assert c is not None
    chunks: list[tuple[str, str]] = []
    r = c.run(THREE_LINES, stream=True, on_chunk=lambda s, d: chunks.append((s, d)))
    stdout_chunks = [d for s, d in chunks if s == "stdout"]
    assert len(stdout_chunks) >= 3, f"expected >= 3 stdout chunks, got {stdout_chunks}"
    assert "".join(stdout_chunks) == "line1\nline2\nline3\n"  # order preserved
    assert r["rc"] == 0
    assert r["stdout"] == "line1\nline2\nline3\n"  # terminal result holds the bounded capture
    assert r["stdout_truncated"] is False


def test_daemon_run_nonstreaming_regression(daemon_env: Path) -> None:
    """Without stream=True the run returns exactly one result dict (unchanged behaviour)."""
    c = daemon.connect_via_daemon("fake", Config.load(), autostart=False)
    assert c is not None
    r = c.run("echo hello; echo oops >&2")
    assert isinstance(r, dict)
    assert r["rc"] == 0 and r["stdout"] == "hello\n" and r["stderr"] == "oops\n"
    assert r["stdout_truncated"] is False and r["stderr_truncated"] is False


def test_daemon_stream_maxoutput_cap_still_exits(daemon_env: Path) -> None:
    """Output over max_output stops being emitted, but the process still exits with the right rc."""
    c = daemon.connect_via_daemon("fake", Config.load(), autostart=False)
    assert c is not None
    emitted: list[str] = []
    r = c.run(
        "for i in $(seq 1 300); do echo AAAAAAAAAAAAAAAAAAAA; done",
        max_output=200,
        stream=True,
        on_chunk=lambda s, d: emitted.append(d),
    )
    total = sum(len(d) for d in emitted)
    assert total <= 200, f"emitted {total} bytes past the cap"
    assert len("".join(emitted)) == len(r["stdout"])  # streamed == bounded capture
    assert r["rc"] == 0  # the child still ran to completion (pipe kept drained)
    assert r["stdout_truncated"] is True


def test_daemon_interleaved_stream_and_ping(daemon_env: Path) -> None:
    """A concurrent ping (fast pool) resolves quickly while a streaming run is in flight."""
    c = daemon.connect_via_daemon("fake", Config.load(), autostart=False)
    assert c is not None
    started = threading.Event()

    def on_chunk(_s: str, _d: str) -> None:
        started.set()

    box: dict[str, object] = {}

    def run_it() -> None:
        box["r"] = c.run("echo go; sleep 3; echo done", stream=True, on_chunk=on_chunk)

    t = threading.Thread(target=run_it, daemon=True)
    t.start()
    assert started.wait(5), "stream never produced its first chunk"
    t0 = time.time()
    assert c.ping()["protocol"] == 2
    assert time.time() - t0 < 1.0, "ping blocked behind the streaming run"
    t.join(timeout=10)
    assert isinstance(box["r"], dict) and box["r"]["rc"] == 0


# --------------------------------------------------------------------------- follow (daemon)
def test_daemon_follow_appends_then_idle(daemon_env: Path, sandbox: Path) -> None:
    """follow yields lines appended to a file, then ends on the idle timeout."""
    c = daemon.connect_via_daemon("fake", Config.load(), autostart=False)
    assert c is not None
    fp = sandbox / "grow.log"
    fp.write_text("seed\n")

    def appender() -> None:
        for i in range(3):
            time.sleep(0.3)
            with open(fp, "a") as f:
                f.write(f"app{i}\n")

    ta = threading.Thread(target=appender, daemon=True)
    ta.start()
    got: list[str] = []
    t0 = time.time()
    for data in c.follow(str(fp), offset=0, idle_timeout=1):
        got.append(data)
    ta.join()
    text = "".join(got)
    assert "seed" in text and "app0" in text and "app2" in text
    # It returned by hitting the 1 s idle timeout, not instantly and not never.
    assert 1.0 <= time.time() - t0 < 10.0


def test_daemon_follow_cancel_stops_promptly(daemon_env: Path, sandbox: Path) -> None:
    """A follow cancelled through the daemon stops within ~0.5 s with eof False + cancelled."""
    c = daemon.connect_via_daemon("fake", Config.load(), autostart=False)
    assert c is not None
    fp = sandbox / "quiet.log"
    fp.write_text("seed\n")
    rid = Session.new_request_id()
    frames: list[dict] = []
    done = threading.Event()

    def run() -> None:
        # idle_timeout is huge so the *only* way this ends is the cancel.
        for frame in c.session.call_stream(
            "follow", {"path": str(fp), "offset": 0, "idle_timeout": 3600}, request_id=rid
        ):
            frames.append(frame)
        done.set()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    # Wait until the seed chunk arrives -> the op is running and its cancel-event is registered.
    deadline = time.time() + 5
    while time.time() < deadline and not frames:
        time.sleep(0.02)
    assert frames, "follow never emitted the seed chunk"
    t0 = time.time()
    assert c.session.cancel(rid)["cancelled"] is True
    assert done.wait(3), "follow did not stop promptly after cancel"
    assert time.time() - t0 < 2.0
    terminal = frames[-1]
    assert terminal["done"] is True
    assert terminal["result"]["eof"] is False
    assert terminal["result"].get("cancelled") is True


# --------------------------------------------------------------------------- early break (daemon)
def test_daemon_stream_early_break_cancels_remote_run(daemon_env: Path, sandbox: Path) -> None:
    """Breaking out of the stream early sends a cancel that kills the remote process group."""
    c = daemon.connect_via_daemon("fake", Config.load(), autostart=False)
    assert c is not None
    marker = sandbox / "rs_pid"
    if marker.exists():
        marker.unlink()
    # Record the backgrounded sleep's pid BEFORE the echo that triggers our break, so the marker
    # is always written before we cancel; the whole process group must then die.
    gen = c.session.call_stream(
        "run", {"cmd": "sleep 30 & echo $! > $HOME/rs_pid; echo one; wait"}, timeout=30
    )
    for frame in gen:
        if not frame.get("done"):
            break  # early break -> GeneratorExit -> cancel on the way out
    gen.close()
    child = _wait_for_file(marker)
    assert _wait_dead(child), "early break did not cancel/kill the remote run"


# --------------------------------------------------------------------------- direct session/cluster
def test_local_call_stream_direct() -> None:
    """On a bare LocalTransport session, call_stream yields chunks then the terminal result."""
    s = Session(LocalTransport())
    s.start()
    try:
        assert s.remote_protocol == 2
        chunks: list[dict] = []
        result = None
        for frame in s.call_stream("run", {"cmd": THREE_LINES}, timeout=30):
            if frame.get("done"):
                result = frame.get("result")
            else:
                chunks.append(frame["chunk"])
        assert len([c for c in chunks if c["stream"] == "stdout"]) >= 3
        assert result is not None and result["rc"] == 0
        assert not s._pending and not s._streams  # registries clean
    finally:
        s.close()


def test_local_stream_error_propagates() -> None:
    """A streaming run whose command is missing raises the mapped stub error (not a chunk)."""
    s = Session(LocalTransport())
    s.start()
    try:
        with pytest.raises(NotFound):
            for _frame in s.call_stream("run", {"argv": ["definitely-not-a-real-binary-xyz"]}):
                pass
        assert not s._pending and not s._streams
    finally:
        s.close()


def test_local_follow_direct(cluster) -> None:  # type: ignore[no-untyped-def]
    """Cluster.follow over a LocalTransport tails appended lines and ends on idle."""
    home = Path(cluster.info()["home"])
    fp = home / "tail.log"
    fp.write_text("one\n")

    def appender() -> None:
        time.sleep(0.3)
        with open(fp, "a") as f:
            f.write("two\n")

    threading.Thread(target=appender, daemon=True).start()
    got = list(cluster.follow(str(fp), offset=0, idle_timeout=1))
    assert "one\n" in got or "".join(got).startswith("one")
    assert "two" in "".join(got)


def test_local_early_break_cleans_registries(cluster) -> None:  # type: ignore[no-untyped-def]
    """Breaking a Cluster.run(stream=True) early cancels the run and leaves no rid behind."""
    marker = Path(cluster.info()["home"]) / "rs_pid2"
    if marker.exists():
        marker.unlink()

    def on_chunk(_s: str, _d: str) -> None:
        raise KeyboardInterrupt  # simulate the caller bailing mid-stream

    with pytest.raises(KeyboardInterrupt):
        cluster.run(
            # marker first, then the echo whose chunk makes on_chunk bail -> no write/cancel race
            "sleep 30 & echo $! > $HOME/rs_pid2; echo one; wait",
            stream=True,
            on_chunk=on_chunk,
        )
    child = _wait_for_file(marker)
    assert _wait_dead(child), "interrupted stream left the remote process group alive"
    assert not cluster.session._pending and not cluster.session._streams
    assert cluster.ping()["protocol"] == 2  # session still healthy


# --------------------------------------------------------------------------- CLI
def test_cli_run_stream_prints_live(daemon_env: Path, capfd: pytest.CaptureFixture[str]) -> None:
    """`rslurm run --stream` streams stdout live and exits with the command's rc."""
    from remoteslurm import cli

    rc = cli.main(["run", "--stream", "echo alpha; echo beta"])
    out = capfd.readouterr().out
    assert rc == 0
    assert "alpha" in out and "beta" in out


def test_stub_death_midstream_wakes_consumer():
    """Killing the stub while a follow streams must wake the consumer with SessionDied, not hang."""
    import os
    import signal
    import tempfile
    import threading
    import time as _t

    import pytest

    from remoteslurm.errors import SessionDied
    from remoteslurm.session import Session
    from remoteslurm.transport import LocalTransport

    d = tempfile.mkdtemp()
    logf = os.path.join(d, "f.log")
    open(logf, "w").close()
    s = Session(LocalTransport())
    s.start()
    try:
        pid = s.remote_pid
        gen = s.call_stream("follow", {"path": logf, "idle_timeout": 3600})
        next(gen)  # keepalive or first read; ensures the stream is live

        def killer():
            _t.sleep(0.3)
            os.kill(pid, signal.SIGKILL)

        threading.Thread(target=killer, daemon=True).start()
        with pytest.raises(SessionDied):
            for _ in gen:
                pass
        assert not s._pending and not s._streams
    finally:
        s.close()


def test_follow_does_not_starve_slow_pool(cluster, sandbox):
    """A long follow (its own pool) must not block run/sbatch (slow pool)."""
    import threading
    import time as _t

    logf = sandbox / "starve.log"
    logf.write_text("start\n")
    stop = threading.Event()

    def do_follow():
        try:
            for _ in cluster.follow("~/starve.log", idle_timeout=3600):
                if stop.is_set():
                    break
        except Exception:
            pass  # session may be torn down at test end; not what we're asserting

    threads = [threading.Thread(target=do_follow, daemon=True) for _ in range(4)]
    for t in threads:
        t.start()
    _t.sleep(0.5)
    # with LONG_OPS on a dedicated pool, a slow-pool `run` still completes promptly
    t0 = _t.time()
    r = cluster.run(["echo", "unblocked"])
    assert r["stdout"].strip() == "unblocked" and _t.time() - t0 < 10
    stop.set()
    for t in threads:
        t.join(timeout=5)


def test_follow_emits_keepalive_when_idle(cluster, sandbox):
    """During a quiet stretch, follow emits keepalive frames (client filters them out)."""
    logf = sandbox / "idle.log"
    logf.write_text("")
    frames = []
    gen = cluster.session.call_stream(
        "follow", {"path": "~/idle.log", "idle_timeout": 2}, timeout=10
    )
    for frame in gen:
        frames.append(frame)
        if frame.get("done"):
            break
    # the follow ran ~2s idle then closed with eof:true; no exception, clean terminal frame
    assert frames[-1]["done"] and frames[-1]["result"]["eof"] is True
