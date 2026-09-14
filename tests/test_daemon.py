from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from remoteslurm import daemon
from remoteslurm.config import Config
from remoteslurm.errors import ExecutionMismatch, NotFound

FAKESLURM = Path(__file__).parent / "fakeslurm"


@pytest.fixture
def daemon_env(sandbox: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """An in-process daemon serving a 'fake ssh' host that is really a local transport."""
    from remoteslurm import cluster as cluster_mod
    from remoteslurm.transport import LocalTransport

    cfg = tmp_path / "config.toml"
    path = f"{FAKESLURM}{os.pathsep}{os.environ['PATH']}"
    cfg.write_text(
        'default_host = "fake"\n[hosts.fake]\nssh = "fakehost"\nmfa = false\n'
        f'[hosts.fake.env]\nHOME = "{sandbox}"\nPATH = "{path}"\n'
    )
    monkeypatch.setenv("REMOTESLURM_CONFIG", str(cfg))
    monkeypatch.setenv("REMOTESLURM_STATE_DIR", str(tmp_path / "state"))
    sock = Path(tempfile.mkdtemp(prefix="rs")) / "d.sock"  # short path: AF_UNIX limit
    monkeypatch.setenv("REMOTESLURM_SOCKET", str(sock))
    monkeypatch.delenv("REMOTESLURM_NO_DAEMON", raising=False)

    # make the "ssh" host use a local transport inside the daemon
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


def test_daemon_roundtrip_and_errors(daemon_env: Path) -> None:
    assert daemon.daemon_available(daemon_env)
    c = daemon.connect_via_daemon("fake", Config.load(), autostart=False)
    assert c is not None
    assert c.read_text("~/proj/a.txt") == "alpha\nbeta\ngamma\n"
    with pytest.raises(NotFound):
        c.ls("~/nope")
    st = daemon.daemon_status(daemon_env)
    assert st["calls"] >= 3 and "fake" in st["hosts"] and st["hosts"]["fake"]["alive"]


def test_daemon_reused_across_clients(daemon_env: Path) -> None:
    c1 = daemon.connect_via_daemon("fake", Config.load(), autostart=False)
    c2 = daemon.connect_via_daemon("fake", Config.load(), autostart=False)
    assert c1 is not None and c2 is not None
    p1 = c1.ping()["pid"]
    p2 = c2.ping()["pid"]
    assert p1 == p2  # same warm stub behind the daemon
    assert daemon.daemon_status(daemon_env)["hosts"]["fake"]["spawns"] == 1


def test_cli_uses_daemon(daemon_env: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from remoteslurm import cli

    before = daemon.daemon_status(daemon_env)["calls"]
    rc = cli.main(["ls", "~/proj", "--json"])
    out = json.loads(capsys.readouterr().out)
    assert rc == 0 and [e["name"] for e in out["entries"]][1] == "a.txt"
    assert daemon.daemon_status(daemon_env)["calls"] > before
    rc = cli.main(["daemon", "status", "--json"])
    st = json.loads(capsys.readouterr().out)
    assert st["running"] and "fake" in st["hosts"]


def test_no_daemon_env_bypasses(daemon_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REMOTESLURM_NO_DAEMON", "1")
    assert daemon.connect_via_daemon("fake", Config.load(), autostart=False) is None


def test_stale_daemon_build_is_rejected(
    daemon_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        daemon,
        "daemon_status",
        lambda path=None: {"build_id": "old-build", "pid": 99},
    )
    with pytest.raises(ExecutionMismatch) as exc:
        daemon.connect_via_daemon("fake", Config.load(), autostart=False)
    assert "daemon stop" in str(exc.value)


def test_stale_socket_is_cleaned() -> None:
    sock = Path(tempfile.mkdtemp(prefix="rs")) / "stale.sock"
    sock.write_text("")
    assert not daemon.daemon_available(sock)
    assert not sock.exists()


def test_idle_exit() -> None:
    sock = Path(tempfile.mkdtemp(prefix="rs")) / "idle.sock"
    srv = daemon.DaemonServer(sock, idle_seconds=0.1)
    t = threading.Thread(target=srv.serve, daemon=True)
    t.start()
    deadline = time.time() + 15
    while t.is_alive() and time.time() < deadline:
        time.sleep(0.2)
    assert not t.is_alive() and not sock.exists()
