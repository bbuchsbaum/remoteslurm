"""Local session daemon: keeps stub sessions warm across CLI invocations.

Opening a new ssh channel costs ~2 s on many clusters (PAM session + rc files), which would
make every CLI call slow. The CLI therefore talks to a small per-user daemon over a unix
socket; the daemon holds the live :class:`Cluster` objects and forwards stub calls. It is
spawned on demand and exits after an idle period. All higher-level logic (job status,
registry, ...) stays in the client — the daemon only multiplexes ``Cluster.call``.

Wire format: one JSON object per line in each direction.
  -> {"host": str|null, "op": str, "args": {...}, "timeout": float|null}
  <- {"ok": true, "result": ...} | {"ok": false, "error": {"code", "message", "action", ...}}
Control ops start with an underscore: ``_status``, ``_stop``, ``_close`` (drop one host).
"""

from __future__ import annotations

import json
import logging
import os
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from .cluster import Cluster
from .config import Config, state_dir
from .errors import RemoteSlurmError, from_stub_error

log = logging.getLogger(__name__)

ENV_NO_DAEMON = "REMOTESLURM_NO_DAEMON"
ENV_SOCKET = "REMOTESLURM_SOCKET"
ENV_IDLE = "REMOTESLURM_DAEMON_IDLE"
DEFAULT_IDLE_SECONDS = 4 * 3600
SPAWN_WAIT_SECONDS = 8.0


def socket_path() -> Path:
    if p := os.environ.get(ENV_SOCKET):
        return Path(p).expanduser()
    # unix socket paths are length-limited (~104 bytes); prefer a short runtime dir.
    run = os.environ.get("XDG_RUNTIME_DIR")
    base = Path(run) if run else state_dir()
    p = base / "remoteslurm.sock"
    if len(str(p).encode()) > 90:
        p = Path(tempfile.gettempdir()) / f"remoteslurm-{os.getuid()}.sock"
    return p


def _error_payload(e: RemoteSlurmError) -> dict[str, Any]:
    d: dict[str, Any] = {"code": e.code, "message": e.message}
    if e.action:
        d["action"] = e.action
    d.update(e.details)
    return d


# --------------------------------------------------------------------------- server side
class _Handler(socketserver.StreamRequestHandler):
    server: DaemonServer

    def handle(self) -> None:
        raw = self.rfile.readline()
        if not raw:
            return
        try:
            req = json.loads(raw)
        except ValueError:
            self._send({"ok": False, "error": {"code": "invalid_arg", "message": "bad json"}})
            return
        self.server.touch()
        try:
            result = self.server.dispatch(req)
            self._send({"ok": True, "result": result})
        except RemoteSlurmError as e:
            self._send({"ok": False, "error": _error_payload(e)})
        except Exception as e:  # pragma: no cover - defensive
            log.exception("daemon request failed")
            self._send(
                {"ok": False, "error": {"code": "error", "message": f"{type(e).__name__}: {e}"}}
            )

    def _send(self, obj: dict[str, Any]) -> None:
        try:
            self.wfile.write(json.dumps(obj, separators=(",", ":")).encode("utf-8") + b"\n")
            self.wfile.flush()
        except (BrokenPipeError, OSError):
            pass


class DaemonServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, path: Path, idle_seconds: float, config_path: Path | None = None) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()
        super().__init__(str(path), _Handler)
        os.chmod(str(path), 0o600)
        self.path = path
        self.idle_seconds = idle_seconds
        self.config_path = config_path
        self.started = time.time()
        self.last_activity = time.time()
        self.calls = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()

    def touch(self) -> None:
        self.last_activity = time.time()

    def _cluster(self, host: str | None) -> Cluster:
        cfg = Config.load(self.config_path)
        return Cluster.connect(host, config=cfg)

    def dispatch(self, req: dict[str, Any]) -> Any:
        op = str(req.get("op", ""))
        host = req.get("host")
        args = req.get("args") or {}
        timeout = req.get("timeout")
        with self._lock:
            self.calls += 1
        if op == "_status":
            from .cluster import _clusters

            return {
                "pid": os.getpid(),
                "socket": str(self.path),
                "uptime": round(time.time() - self.started, 1),
                "idle_seconds": self.idle_seconds,
                "calls": self.calls,
                "hosts": {
                    n: {
                        "alive": c.session.alive,
                        "remote_pid": c.session.remote_pid,
                        "spawns": c.session.spawn_count,
                        "transport": c.transport.describe(),
                    }
                    for n, c in list(_clusters.items())
                },
            }
        if op == "_stop":
            self._stop.set()
            threading.Thread(target=self.shutdown, daemon=True).start()
            return {"stopping": True}
        if op == "_close":
            from .cluster import _clusters

            c = _clusters.get(str(host))
            if c:
                c.close()
            return {"closed": bool(c)}
        if op == "_host":
            c = self._cluster(host)
            return {k: v for k, v in c.host.__dict__.items()}
        c = self._cluster(host)
        return c.session.call(op, args, timeout=timeout if timeout is not None else 60.0)

    def serve(self) -> None:
        watchdog = threading.Thread(target=self._idle_watch, daemon=True)
        watchdog.start()
        try:
            self.serve_forever(poll_interval=0.5)
        finally:
            from .cluster import _clusters

            for c in list(_clusters.values()):
                try:
                    c.close()
                except Exception:
                    pass
            try:
                self.path.unlink()
            except OSError:
                pass

    def _idle_watch(self) -> None:
        while not self._stop.is_set():
            time.sleep(5)
            if time.time() - self.last_activity > self.idle_seconds:
                log.info("idle for %ss; exiting", self.idle_seconds)
                self._stop.set()
                self.shutdown()
                return


def serve(path: Path | None = None, idle_seconds: float | None = None) -> None:
    path = path or socket_path()
    idle = (
        idle_seconds
        if idle_seconds is not None
        else float(os.environ.get(ENV_IDLE, DEFAULT_IDLE_SECONDS))
    )
    cfgp = os.environ.get("REMOTESLURM_CONFIG")
    srv = DaemonServer(path, idle, Path(cfgp).expanduser() if cfgp else None)
    srv.serve()


# --------------------------------------------------------------------------- client side
def _request(path: Path, req: dict[str, Any], timeout: float | None) -> Any:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(5.0)
        s.connect(str(path))
        s.settimeout((timeout or 60.0) + 10.0)
        s.sendall(json.dumps(req, separators=(",", ":")).encode("utf-8") + b"\n")
        buf = b""
        while not buf.endswith(b"\n"):
            chunk = s.recv(1 << 16)
            if not chunk:
                break
            buf += chunk
    if not buf:
        raise RemoteSlurmError("daemon closed the connection without a reply")
    msg = json.loads(buf)
    if msg.get("ok"):
        return msg.get("result")
    raise from_stub_error(msg.get("error") or {})


def daemon_available(path: Path | None = None) -> bool:
    path = path or socket_path()
    if not path.exists():
        return False
    try:
        _request(path, {"op": "_status"}, timeout=5)
        return True
    except (OSError, RemoteSlurmError, ValueError):
        try:
            path.unlink()  # stale socket
        except OSError:
            pass
        return False


def spawn_daemon(path: Path | None = None) -> bool:
    """Start a detached daemon and wait until its socket answers."""
    path = path or socket_path()
    env = dict(os.environ)
    env[ENV_SOCKET] = str(path)
    logdir = state_dir()
    logdir.mkdir(parents=True, exist_ok=True)
    logf = open(logdir / "daemon.log", "ab")
    subprocess.Popen(
        [sys.executable, "-m", "remoteslurm.daemon"],
        stdin=subprocess.DEVNULL,
        stdout=logf,
        stderr=logf,
        env=env,
        start_new_session=True,
        close_fds=True,
    )
    deadline = time.time() + SPAWN_WAIT_SECONDS
    while time.time() < deadline:
        if path.exists():
            try:
                _request(path, {"op": "_status"}, timeout=5)
                return True
            except (OSError, RemoteSlurmError, ValueError):
                pass
        time.sleep(0.05)
    return False


def daemon_status(path: Path | None = None) -> dict[str, Any]:
    return dict(_request(path or socket_path(), {"op": "_status"}, timeout=5))


def stop_daemon(path: Path | None = None) -> bool:
    path = path or socket_path()
    try:
        _request(path, {"op": "_stop"}, timeout=5)
        return True
    except (OSError, RemoteSlurmError, ValueError):
        return False


class DaemonSession:
    """Duck-types the parts of :class:`Session` that :class:`Cluster` uses."""

    def __init__(self, path: Path, host: str | None) -> None:
        self.path = path
        self.host = host
        self.remote_pid: int | None = None
        self.spawn_count = 0

    @property
    def alive(self) -> bool:
        return True

    def start(self) -> None:
        return None

    def close(self) -> None:
        return None

    def call(
        self, op: str, args: dict[str, Any] | None = None, *, timeout: float | None = 60.0
    ) -> Any:
        return _request(
            self.path,
            {"host": self.host, "op": op, "args": args or {}, "timeout": timeout},
            timeout,
        )


def connect_via_daemon(
    host: str | None, config: Config, *, autostart: bool = True
) -> Cluster | None:
    """Return a Cluster whose calls go through the daemon, or None if unavailable/disabled."""
    if os.environ.get(ENV_NO_DAEMON):
        return None
    hc = config.host(host)
    if hc.ssh == "local":
        return None  # nothing to keep warm
    path = socket_path()
    if not daemon_available(path) and not (autostart and spawn_daemon(path)):
        return None
    from .transport import SSHTransport

    transport = SSHTransport(alias=hc.ssh, mfa=hc.mfa, control_path=hc.control_path)
    return Cluster(hc, transport, session=DaemonSession(path, hc.name))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    serve()


if __name__ == "__main__":  # pragma: no cover
    main()
