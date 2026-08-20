"""A Session owns one running stub process and multiplexes requests over it."""

from __future__ import annotations

import itertools
import json
import logging
import threading
import time
from concurrent.futures import Future
from typing import Any

from .errors import NotConnected, RemoteTimeout, SessionDied, from_stub_error
from .transport import Transport

log = logging.getLogger(__name__)

RS = b"\x1e"
READY_PREFIX = b"REMOTESLURM-READY"
ERROR_PREFIX = b"REMOTESLURM-ERROR"
DEFAULT_TIMEOUT = 60.0
READY_TIMEOUT = 45.0
STALE_SECONDS = 90.0  # idle longer than this -> probe before reuse
PROBE_TIMEOUT = 10.0


class Session:
    def __init__(self, transport: Transport, *, ready_timeout: float = READY_TIMEOUT) -> None:
        self.transport = transport
        self.ready_timeout = ready_timeout
        self._proc: Any = None
        self._reader: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._pending: dict[str, Future[Any]] = {}
        self._lock = threading.Lock()  # protects _pending, _proc and stdin writes
        self._ids = itertools.count(1)
        self._ready = threading.Event()
        self._ready_error: str | None = None
        self.preamble: list[str] = []  # noise printed before READY (rc files, MOTD)
        self.stderr_tail: list[str] = []
        self.remote_pid: int | None = None
        self.remote_python: str | None = None
        self.spawn_count = 0
        self.last_used = 0.0

    # -- lifecycle ---------------------------------------------------------------------
    @property
    def alive(self) -> bool:
        return (
            self._proc is not None
            and self._proc.poll() is None
            and self._ready.is_set()
            and self.remote_pid is not None
        )

    def start(self) -> None:
        with self._lock:
            if self.alive:
                return
            self._start_locked()

    def _start_locked(self) -> None:
        self._ready.clear()
        self._ready_error = None
        self.remote_pid = None
        self.preamble = []
        self.stderr_tail = []
        self._proc = self.transport.spawn()
        self.spawn_count += 1
        self._reader = threading.Thread(
            target=self._read_loop, name="remoteslurm-reader", daemon=True
        )
        self._reader.start()
        self._stderr_thread = threading.Thread(
            target=self._stderr_loop, name="remoteslurm-stderr", daemon=True
        )
        self._stderr_thread.start()
        if not self._ready.wait(self.ready_timeout):
            self._kill_locked()
            raise SessionDied(
                f"stub did not become ready within {self.ready_timeout}s "
                f"via {self.transport.describe()}",
                preamble=self.preamble[-20:],
                stderr=self.stderr_tail[-20:],
            )
        if self._ready_error:
            err = self._ready_error
            self._kill_locked()
            raise SessionDied(err, stderr=self.stderr_tail[-20:])
        if self._proc is None or self._proc.poll() is not None or self.remote_pid is None:
            pre, err_tail = self.preamble[-20:], self.stderr_tail[-20:]
            self._kill_locked()
            raise SessionDied(
                f"stub exited during startup via {self.transport.describe()}",
                preamble=pre,
                stderr=err_tail,
            )

    def close(self) -> None:
        with self._lock:
            if self._proc is None:
                return
            try:
                if self._proc.poll() is None and self._ready.is_set():
                    self._write_locked({"id": "shutdown", "op": "shutdown"})
                    self._proc.stdin.close()
                    self._proc.wait(timeout=5)
            except Exception:
                pass
            self._kill_locked()

    def _kill_locked(self) -> None:
        proc, self._proc = self._proc, None
        if proc is not None and proc.poll() is None:
            try:
                proc.kill()
                proc.wait(timeout=5)
            except Exception:
                pass
        self._fail_pending_locked(SessionDied("stub process ended", stderr=self.stderr_tail[-20:]))
        self._ready.clear()

    def _fail_pending_locked(self, exc: Exception) -> None:
        pending, self._pending = self._pending, {}
        for fut in pending.values():
            if not fut.done():
                fut.set_exception(exc)

    # -- io threads ----------------------------------------------------------------------
    def _stderr_loop(self) -> None:
        proc = self._proc
        if proc is None or proc.stderr is None:
            return
        for raw in proc.stderr:
            line = raw.decode("utf-8", errors="replace").rstrip("\n")
            self.stderr_tail.append(line)
            if len(self.stderr_tail) > 200:
                del self.stderr_tail[:-200]
            if raw.startswith(ERROR_PREFIX):
                self._ready_error = line
                self._ready.set()
            log.debug("stub stderr: %s", line)

    def _read_loop(self) -> None:
        proc = self._proc
        if proc is None or proc.stdout is None:
            return
        try:
            for raw in proc.stdout:
                if raw.startswith(RS):
                    self._dispatch(raw[1:])
                elif raw.startswith(READY_PREFIX):
                    parts = raw.decode("utf-8", errors="replace").split()
                    try:
                        self.remote_pid = int(parts[2])
                        self.remote_python = parts[3]
                    except (IndexError, ValueError):
                        pass
                    self._ready.set()
                elif raw.startswith(ERROR_PREFIX):
                    self._ready_error = raw.decode("utf-8", errors="replace").strip()
                    self._ready.set()
                else:
                    text = raw.decode("utf-8", errors="replace").rstrip("\n")
                    if not self._ready.is_set():
                        self.preamble.append(text)
                    else:
                        log.debug("stray stub stdout: %s", text)
        finally:
            # Unblock start() first (it may be holding _lock while waiting on _ready).
            self._ready.set()
            with self._lock:
                if self._proc is proc:
                    self._fail_pending_locked(
                        SessionDied(
                            "stub connection closed",
                            stderr=self.stderr_tail[-20:],
                            preamble=self.preamble[-20:],
                        )
                    )

    def _dispatch(self, payload: bytes) -> None:
        try:
            msg = json.loads(payload)
        except ValueError:
            log.warning("undecodable stub frame: %r", payload[:200])
            return
        rid = str(msg.get("id"))
        with self._lock:
            fut = self._pending.pop(rid, None)
        if fut is None:
            log.debug("late/unknown response id=%s", rid)
            return
        if msg.get("ok"):
            fut.set_result(msg.get("result"))
        else:
            fut.set_exception(from_stub_error(msg.get("error") or {}))

    # -- requests -------------------------------------------------------------------------
    def _write_locked(self, obj: dict[str, Any]) -> None:
        data = json.dumps(obj, separators=(",", ":")).encode("utf-8") + b"\n"
        assert self._proc is not None and self._proc.stdin is not None
        self._proc.stdin.write(data)
        self._proc.stdin.flush()

    def submit(self, op: str, args: dict[str, Any] | None = None) -> Future[Any]:
        with self._lock:
            if not self.alive:
                if self._proc is not None and self._proc.poll() is not None:
                    self._kill_locked()
                self._start_locked()
            rid = str(next(self._ids))
            fut: Future[Any] = Future()
            self._pending[rid] = fut
            try:
                self._write_locked({"id": rid, "op": op, "args": args or {}})
            except (BrokenPipeError, OSError) as e:
                self._pending.pop(rid, None)
                self._kill_locked()
                raise SessionDied(f"lost connection to stub: {e}") from e
            self.last_used = time.time()
        return fut

    def call(
        self,
        op: str,
        args: dict[str, Any] | None = None,
        *,
        timeout: float | None = DEFAULT_TIMEOUT,
    ) -> Any:
        if op != "ping" and self.alive and time.time() - self.last_used > STALE_SECONDS:
            self._probe()
        fut = self.submit(op, args)
        try:
            return fut.result(timeout=timeout)
        except TimeoutError as e:
            with self._lock:
                for rid, f in list(self._pending.items()):
                    if f is fut:
                        self._pending.pop(rid, None)
            self._raise_if_master_dead()
            raise RemoteTimeout(f"{op} did not complete within {timeout}s", op=op) from e

    def _probe(self) -> None:
        """Cheap ping before reusing a session that sat idle (laptop sleep, dead master)."""
        fut = self.submit("ping", {})
        try:
            fut.result(timeout=PROBE_TIMEOUT)
        except TimeoutError:
            with self._lock:
                self._kill_locked()
            self._raise_if_master_dead()
            # master alive but stub wedged: the next submit() respawns it

    def _raise_if_master_dead(self) -> None:
        """If the transport can tell us the ssh master is gone, fail fast with an action."""
        check = getattr(self.transport, "master_alive", None)
        if check is None or check():
            return
        with self._lock:
            self._kill_locked()
        alias = getattr(self.transport, "alias", "?")
        raise NotConnected(
            f"ssh connection to {alias} is gone (laptop sleep or ControlPersist expired)",
            action=f"run in a terminal: remoteslurm connect {alias}",
        )
