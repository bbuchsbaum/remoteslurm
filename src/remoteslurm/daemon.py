"""Local session daemon: keeps stub sessions warm across CLI invocations.

Opening a new ssh channel costs ~2 s on many clusters (PAM session + rc files), which would
make every CLI call slow. The CLI therefore talks to a small per-user daemon over a unix
socket; the daemon holds the live :class:`Cluster` objects and forwards stub calls. It is
spawned on demand and exits after an idle period. All higher-level logic (job status,
registry, ...) stays in the client — the daemon only multiplexes ``Cluster.call``.

Wire format: one JSON object per line in each direction.
  -> {"host": str|null, "op": str, "args": {...}, "timeout": float|null,
      "id": str|null, "cancel_on_timeout": bool}
  <- {"ok": true, "result": ...} | {"ok": false, "error": {"code", "message", "action", ...}}
Control ops start with an underscore: ``_status``, ``_stop``, ``_close`` (drop one host),
``_cancel`` ({host, id}: kill the slow op running under that client-generated id). The request
``id`` is generated client-side so it survives the daemon hop and a later ``_cancel`` (sent on
a second connection when the first call times out or is interrupted) can name the same op.
"""

from __future__ import annotations

import fcntl
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
import uuid
from collections.abc import Generator
from pathlib import Path
from typing import Any

from .cluster import Cluster
from .config import Config, state_dir
from .errors import RemoteSlurmError, RemoteTimeout, SessionDied, from_stub_error
from .identity import control_identity

log = logging.getLogger(__name__)

ENV_NO_DAEMON = "REMOTESLURM_NO_DAEMON"
ENV_SOCKET = "REMOTESLURM_SOCKET"
ENV_IDLE = "REMOTESLURM_DAEMON_IDLE"
DEFAULT_IDLE_SECONDS = 4 * 3600
SPAWN_WAIT_SECONDS = 8.0
READY_GRACE = 60.0  # first call may include a 45 s stub bootstrap


def socket_path() -> Path:
    if env := os.environ.get(ENV_SOCKET):
        return Path(env).expanduser()
    # unix socket paths are length-limited (~104 bytes); prefer a short runtime dir.
    run = os.environ.get("XDG_RUNTIME_DIR")
    base = Path(run) if run else state_dir()
    sock = base / "remoteslurm.sock"
    if len(str(sock).encode()) > 90:
        sock = Path(tempfile.gettempdir()) / f"remoteslurm-{os.getuid()}.sock"
    return sock


def _error_payload(e: RemoteSlurmError) -> dict[str, Any]:
    d: dict[str, Any] = {"code": e.code, "message": e.message}
    if e.action:
        d["action"] = e.action
    d.update(e.details)
    return d


# --------------------------------------------------------------------------- server side
class DaemonAlreadyRunning(RuntimeError):
    pass


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
        if req.get("stream"):
            self._handle_stream(req)
            return
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

    def _handle_stream(self, req: dict[str, Any]) -> None:
        """Forward a streaming (multi-frame) response: one stub frame per line, terminal last.

        Each yielded frame is the raw stub frame (``{"id","ok","chunk"|"result","done"}``); an
        error terminates the stream with a ``done: true`` error frame. If the client goes away
        (write fails), we stop iterating and close the generator, which runs ``call_stream``'s
        cleanup and cancels the still-running stub op.
        """
        rid = req.get("id")
        gen: Generator[dict[str, Any], None, None] | None = None
        try:
            gen = self.server.dispatch_stream(req)
            for frame in gen:
                if not self._send_frame(frame):
                    break
        except RemoteSlurmError as e:
            self._send_frame({"id": rid, "ok": False, "error": _error_payload(e), "done": True})
        except Exception as e:  # pragma: no cover - defensive
            log.exception("daemon stream failed")
            self._send_frame(
                {
                    "id": rid,
                    "ok": False,
                    "error": {"code": "error", "message": f"{type(e).__name__}: {e}"},
                    "done": True,
                }
            )
        finally:
            if gen is not None:
                gen.close()  # runs call_stream's finally -> cancels the stub op if still live

    def _send(self, obj: dict[str, Any]) -> None:
        self._send_frame(obj)

    def _send_frame(self, frame: dict[str, Any]) -> bool:
        try:
            self.wfile.write(json.dumps(frame, separators=(",", ":")).encode("utf-8") + b"\n")
            self.wfile.flush()
            return True
        except (BrokenPipeError, OSError):
            return False


class DaemonServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, path: Path, idle_seconds: float, config_path: Path | None = None) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        # One daemon per socket: hold an exclusive lock for our whole lifetime so two CLIs
        # racing to spawn cannot both bind (the loser would orphan the winner's ssh session).
        self._lockfile = open(path.with_suffix(".lock"), "w")
        try:
            fcntl.flock(self._lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            self._lockfile.close()
            raise DaemonAlreadyRunning(str(path)) from e
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

            ident = control_identity()
            return {
                "pid": os.getpid(),
                "build_id": ident["build_id"],
                "version": ident["version"],
                "stub_sha": ident["stub_sha"],
                "socket": str(self.path),
                "uptime": round(time.time() - self.started, 1),
                "idle_seconds": self.idle_seconds,
                "calls": self.calls,
                "hosts": {
                    n: {
                        "alive": c.session.alive,
                        "remote_pid": c.session.remote_pid,
                        "remote_stub_sha": c.session.remote_stub_sha,
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
        if op == "_cancel":
            from .cluster import _clusters

            rid = str(req.get("id"))
            c = _clusters.get(str(host))
            if c is None:
                return {"cancelled": False, "id": rid, "reason": "no session"}
            return c.session.cancel(rid)
        if op == "_host":
            c = self._cluster(host)
            return {k: v for k, v in c.host.__dict__.items()}
        c = self._cluster(host)
        return c.session.call(
            op,
            args,
            timeout=timeout if timeout is not None else 60.0,
            cancel_on_timeout=bool(req.get("cancel_on_timeout")),
            request_id=req.get("id"),
        )

    def dispatch_stream(self, req: dict[str, Any]) -> Generator[dict[str, Any], None, None]:
        """Return the stream of stub frames for a ``stream: true`` request.

        ``timeout`` is passed through as-is (``None`` = no client-side stream deadline, e.g. a
        long ``follow``); the ``id`` is the client-generated request id so a later ``_cancel``
        on a second connection targets the same op.
        """
        op = str(req.get("op", ""))
        host = req.get("host")
        args = req.get("args") or {}
        with self._lock:
            self.calls += 1
        c = self._cluster(host)
        return c.session.call_stream(op, args, timeout=req.get("timeout"), request_id=req.get("id"))

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
            try:
                fcntl.flock(self._lockfile, fcntl.LOCK_UN)
                self._lockfile.close()
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
    try:
        srv = DaemonServer(path, idle, Path(cfgp).expanduser() if cfgp else None)
    except DaemonAlreadyRunning:
        log.info("another daemon owns %s; exiting", path)
        return
    srv.serve()


# --------------------------------------------------------------------------- client side
def _request(path: Path, req: dict[str, Any], timeout: float | None) -> Any:
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(5.0)
            s.connect(str(path))
            s.settimeout((timeout or 60.0) + READY_GRACE)
            s.sendall(json.dumps(req, separators=(",", ":")).encode("utf-8") + b"\n")
            buf = b""
            while not buf.endswith(b"\n"):
                chunk = s.recv(1 << 16)
                if not chunk:
                    break
                buf += chunk
    except TimeoutError as e:
        raise RemoteTimeout(
            f"daemon did not answer {req.get('op')} in time", op=req.get("op")
        ) from e
    except OSError as e:
        raise SessionDied(
            f"cannot reach the remoteslurm daemon at {path}: {e}",
            action="retry (the daemon restarts on demand) or use --no-daemon",
        ) from e
    if not buf:
        raise SessionDied("daemon closed the connection without a reply", action="retry")
    try:
        msg = json.loads(buf)
    except ValueError as e:
        raise SessionDied("daemon sent an undecodable reply") from e
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
    # A source checkout may be running against an installed remoteslurm of the same version.
    # Start the daemon from the exact package tree that made this request so the build-identity
    # handshake is meaningful in editable installs as well as wheels.
    package_root = str(Path(__file__).resolve().parents[1])
    env["PYTHONPATH"] = package_root + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )
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
        self.remote_protocol: int | None = None
        self.remote_python: str | None = None
        self.remote_stub_sha: str | None = None
        self.spawn_count = 0

    @property
    def alive(self) -> bool:
        return True

    def start(self) -> None:
        return None

    def close(self) -> None:
        return None

    def call(
        self,
        op: str,
        args: dict[str, Any] | None = None,
        *,
        timeout: float | None = 60.0,
        cancel_on_timeout: bool = False,
        request_id: str | None = None,
    ) -> Any:
        # The id is generated client-side so it survives the hop to the daemon-side session;
        # if this call times out or the user interrupts it, `_cancel` (a second connection)
        # can name the same op and kill it.
        rid = request_id or uuid.uuid4().hex[:12]
        req = {
            "host": self.host,
            "op": op,
            "args": args or {},
            "timeout": timeout,
            "id": rid,
            "cancel_on_timeout": cancel_on_timeout,
        }
        try:
            return _request(self.path, req, timeout)
        except RemoteTimeout:
            if cancel_on_timeout:
                self.cancel(rid)
            raise
        except KeyboardInterrupt:
            self.cancel(rid)
            raise

    def call_stream(
        self,
        op: str,
        args: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
        request_id: str | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Stream a multi-frame response over the daemon socket, yielding each stub frame.

        Opens its own connection, sends the request with ``stream: true``, and reads frames
        line-by-line until the terminal frame (``done: true``). ``timeout`` is the total stream
        budget (``None`` = block indefinitely, used by ``follow``/``tail -f``). On early break,
        timeout, KeyboardInterrupt, or a truncated stream, sends ``_cancel`` on a second
        connection so the remote op is not left running.
        """
        rid = request_id or uuid.uuid4().hex[:12]
        req = {
            "host": self.host,
            "op": op,
            "args": args or {},
            "timeout": timeout,
            "id": rid,
            "stream": True,
        }
        finished = False
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            try:
                s.settimeout(5.0)
                s.connect(str(self.path))
                s.settimeout(None if timeout is None else timeout + READY_GRACE)
                s.sendall(json.dumps(req, separators=(",", ":")).encode("utf-8") + b"\n")
            except OSError as e:
                raise SessionDied(
                    f"cannot reach the remoteslurm daemon at {self.path}: {e}",
                    action="retry (the daemon restarts on demand) or use --no-daemon",
                ) from e
            buf = b""
            while True:
                nl = buf.find(b"\n")
                if nl < 0:
                    try:
                        chunk = s.recv(1 << 16)
                    except TimeoutError as e:
                        raise RemoteTimeout(
                            f"daemon did not answer {op} stream in time", op=op
                        ) from e
                    except OSError as e:
                        raise SessionDied(f"lost the daemon stream: {e}") from e
                    if not chunk:
                        break  # EOF
                    buf += chunk
                    continue
                line, buf = buf[:nl], buf[nl + 1 :]
                if not line.strip():
                    continue
                try:
                    frame = json.loads(line)
                except ValueError:
                    continue
                if not frame.get("done"):
                    yield frame
                    continue
                finished = True
                if frame.get("ok"):
                    yield frame
                    return
                raise from_stub_error(frame.get("error") or {})
            if not finished:
                raise SessionDied(
                    "daemon closed the stream before it finished",
                    action="retry (the daemon restarts on demand) or use --no-daemon",
                )
        finally:
            try:
                s.close()
            except OSError:
                pass
            if not finished:
                self.cancel(rid)

    def cancel(self, request_id: str, *, timeout: float = 5.0) -> dict[str, Any]:
        """Kill the slow op running under ``request_id`` (best effort, never raises)."""
        try:
            return dict(
                _request(
                    self.path,
                    {"host": self.host, "op": "_cancel", "id": request_id},
                    timeout,
                )
            )
        except Exception:
            return {"cancelled": False, "id": request_id}


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
    status = daemon_status(path)
    expected = control_identity()
    if status.get("build_id") != expected["build_id"]:
        from .errors import ExecutionMismatch

        raise ExecutionMismatch(
            "the running remoteslurm daemon was loaded from a different build",
            action="run `rslurm daemon stop`, then repeat the command so the daemon restarts",
            client_build=expected["build_id"],
            daemon_build=status.get("build_id"),
            daemon_pid=status.get("pid"),
        )
    from .transport import SSHTransport

    # The stub calls go through the daemon, but client-side rsync (sync/put/get) reads this
    # transport's ssh options, so carry the full set (control_path, ProxyJump/extra opts).
    transport = SSHTransport(
        alias=hc.ssh,
        mfa=hc.mfa,
        python=hc.python,
        install_dir=hc.install_dir,
        control_path=hc.control_path,
        control_persist=hc.control_persist,
        extra_ssh_opts=list(hc.ssh_opts),
    )
    return Cluster(hc, transport, session=DaemonSession(path, hc.name))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    serve()


if __name__ == "__main__":  # pragma: no cover
    main()
