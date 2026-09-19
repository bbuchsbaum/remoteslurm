# remoteslurm remote stub.
#
# This single file is shipped to the remote login node and executed with the
# system python3 there. It MUST stay:
#   * stdlib-only,
#   * Python 3.6 compatible (no walrus, no f-string "=", no PEP 585/604 types,
#     no `from __future__ import annotations`),
#   * free of any shell usage (argv lists only).
#
# Protocol (JSON lines over stdin/stdout):
#   client -> stub : {"id": <str>, "op": <str>, "args": {...}}
#   stub   -> client: "\x1e" + json({"id", "ok": bool, "result"|"error", "done": true})
# The stub announces readiness with a single line:
#   REMOTESLURM-READY <protocol> <pid> <python-version>
# Everything printed before that line (rc files, module noise) is discarded by
# the client, and every response line is prefixed with the RS byte (0x1e) so
# stray output from other sources is skipped.

import base64
import codecs
import difflib
import errno
import fnmatch
import getpass
import glob
import hashlib
import io
import json
import os
import re
import select
import selectors
import shutil
import signal
import socket
import stat as statmod
import subprocess
import sys
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

PROTOCOL = 2
RS = "\x1e"


def _stub_sha():
    try:
        with io.open(__file__, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()[:16]
    except OSError:
        return None


# Two pools so a long `run` (slow) can never queue behind a `ping`/`ls` (fast). Only the
# escape-hatch ops that spawn a genuinely long-lived subprocess go to the slow pool; they
# register their Popen so `cancel` can reach them. Everything else (including the short,
# bounded Slurm queries) stays in the responsive fast pool. `cancel` itself is a fast op so
# it can never deadlock behind the very ops it is meant to interrupt.
FAST_WORKERS = 8
SLOW_WORKERS = 4
# `follow` and other long-lived streaming ops get their OWN pool so a `tail -f` (which can
# hold a worker for hours) can never starve the slow pool that runs/srun/sbatch depend on.
LONG_WORKERS = 6
# Ops that spawn a genuinely long-lived subprocess; they register their Popen so `cancel`
# can reach the whole process group.
SLOW_OPS = frozenset(
    (
        "run",
        "srun",
        "sbatch",
        "task_ensure",
        "task_fingerprint",
        "progress_count",
        "campaign_attempt_ensure",
        "campaign_markers",
    )
)
# Ops that can stream multi-frame responses when the request carries ``stream: true``.
STREAM_OPS = frozenset(("run", "srun"))
# Long-lived ops with no subprocess, served by the long pool and cancelled via a per-request
# ``threading.Event`` in the registry: `follow` tails a file (always streams); `waitfor` polls
# for a detached run's exit or a file/log line (one response).
LONG_OPS = frozenset(("follow", "waitfor"))
ALWAYS_STREAM_OPS = frozenset(("follow",))
KILL_GRACE = 0.5  # seconds between SIGTERM and SIGKILL when cancelling a process group
# A `run` returns once its command has exited and its pipes are drained. A background child that
# inherited stdout/stderr would hold the pipes open indefinitely, so after the command exits the
# pipes are read for at most this long before returning (the result is flagged `lingering`).
RUN_DRAIN_GRACE = 2.0
KILL_DRAIN = 2.0  # after a timeout kill, read leftover output for at most this long


def _env_int(name, default):
    try:
        return int(os.environ.get(name) or default)
    except ValueError:
        return default


# After a run turns `lingering`, a thread keeps draining its background children's output into
# a log so they are not killed by SIGPIPE; it stops (closing the pipes) after this many bytes.
LINGER_LOG_MAX = _env_int("REMOTESLURM_LINGER_MAX_BYTES", 16 * 1024 * 1024)
LINGER_MAX_DRAINS = 16  # concurrent drains; beyond this a lingering run's pipes are just closed
LINGER_KEEP_DAYS = 7  # lingering-output logs untouched for longer are pruned
_LINGER_LOCK = threading.Lock()
_LINGER_ACTIVE = [0]
PIPE_BUF = getattr(select, "PIPE_BUF", 512)  # stdin write size that never blocks a ready pipe
# Detached runs (`run(detach=True)`): records, rc files and default logs live here, under the
# remote home (shared between login nodes on most clusters), else next to the stub.
PROC_SUBDIR = "procs"
PROC_LIST_DEFAULT = 20
PROC_LIST_MAX = 200
PROC_KILL_GRACE = 5  # seconds between the requested signal and SIGKILL in `proc_kill`
WAIT_POLL = 0.5  # `waitfor`: seconds between checks
WAIT_DEFAULT = 60
WAIT_MAX = 3600
WAIT_SCAN_BYTES = 8 * 1024 * 1024  # max log bytes a `waitfor` scans per check (stays responsive)
WAIT_CARRY_MAX = 64 * 1024  # longest partial (newline-less) line kept between checks

DEFAULT_READ_BYTES = 64 * 1024
MAX_READ_BYTES = 4 * 1024 * 1024
MAX_WRITE_BYTES = 8 * 1024 * 1024
DEFAULT_LS_LIMIT = 200
MAX_LS_LIMIT = 2000
DEFAULT_GREP_MATCHES = 200
MAX_GREP_MATCHES = 5000
GREP_MAX_FILE_SIZE = 50 * 1024 * 1024
DEFAULT_GLOB_LIMIT = 500
MAX_GLOB_LIMIT = 10000
DEFAULT_PROGRESS_SCAN = 1000000
MAX_PROGRESS_SCAN = 10000000
DEFAULT_RUN_TIMEOUT = 60
MAX_RUN_TIMEOUT = 3600
DEFAULT_RUN_OUTPUT = 64 * 1024
MAX_RUN_OUTPUT = 4 * 1024 * 1024
DEFAULT_QUEUE_TIMEOUT = 600  # `srun`: default seconds to wait for an allocation
# `srun` may legitimately run for days (queue wait + walltime); its total budget is computed
# client-side, so allow a far larger ceiling than the plain `run` cap.
MAX_SRUN_TIMEOUT = 8 * 24 * 3600
RS_NODE_SENTINEL = "RS_NODE="  # emitted on stderr once the step is actually allocated
MAX_EDIT_BYTES = 8 * 1024 * 1024
EDIT_PREVIEW_BYTES = 4096
FOLLOW_IDLE_DEFAULT = 60  # `follow`: stop after this many seconds with no new bytes
FOLLOW_IDLE_MAX = 24 * 3600
FOLLOW_CHUNK_DEFAULT = 64 * 1024  # bytes emitted per `follow` chunk (never load the whole file)
FOLLOW_CHUNK_MAX = 4 * 1024 * 1024
FOLLOW_POLL = 0.5  # seconds between `follow` polls of a file with no new data
FOLLOW_KEEPALIVE = 20  # emit an empty keepalive chunk after this many idle seconds
STREAM_READ_BYTES = 64 * 1024  # os.read size when streaming a run's pipes
DEFAULT_DIFF_LINES = 500
MAX_DIFF_LINES = 5000
DIFF_MAX_LINE = 2000
CAMPAIGN_SCHEMA = 1
CAMPAIGN_MAX_DOCUMENTS = 1024
CAMPAIGN_MAX_EVENTS = 10000
CAMPAIGN_MAX_TRANSACTION_BYTES = 64 * 1024 * 1024
CAMPAIGN_MAX_READ_BYTES = 4 * 1024 * 1024
CAMPAIGN_LIST_MAX = 500
CONTRACT_MAX_ITEMS = 1024
CONTRACT_MAX_MATCHES = 1024
CONTRACT_MAX_TOTAL_MATCHES = 2048
CONTRACT_MAX_HASH_BYTES = 256 * 1024 * 1024
CONTRACT_MAX_TOTAL_HASH_BYTES = 1024 * 1024 * 1024


class StubError(Exception):
    def __init__(self, code, message, **details):
        Exception.__init__(self, message)
        self.code = code
        self.message = message
        self.details = details

    def to_dict(self):
        d = {"code": self.code, "message": self.message}
        d.update(self.details)
        return d


# --------------------------------------------------------------------------- helpers


def _path(p, must_exist=False):
    if not isinstance(p, str) or not p:
        raise StubError("invalid_arg", "path must be a non-empty string")
    if "\x00" in p:
        raise StubError("invalid_arg", "path contains NUL byte")
    p = os.path.expanduser(os.path.expandvars(p))
    if not os.path.isabs(p):
        p = os.path.join(os.getcwd(), p)
    p = os.path.normpath(p)
    if must_exist and not os.path.lexists(p):
        raise StubError("not_found", "no such path: %s" % p, path=p)
    return p


def _os_error(e, p):
    if isinstance(e, FileNotFoundError):
        return StubError("not_found", "no such path: %s" % p, path=p)
    if isinstance(e, PermissionError):
        return StubError("permission", "permission denied: %s" % p, path=p)
    if isinstance(e, IsADirectoryError):
        return StubError("invalid_arg", "is a directory: %s" % p, path=p)
    if isinstance(e, NotADirectoryError):
        return StubError("invalid_arg", "not a directory: %s" % p, path=p)
    return StubError("error", "%s: %s" % (type(e).__name__, e), path=p)


def _clamp(v, default, hi, lo=1):
    if v is None:
        return default
    try:
        v = int(v)
    except (TypeError, ValueError):
        raise StubError("invalid_arg", "expected integer, got %r" % (v,))
    return max(lo, min(v, hi))


def _ftype(st):
    m = st.st_mode
    if statmod.S_ISDIR(m):
        return "dir"
    if statmod.S_ISREG(m):
        return "file"
    if statmod.S_ISLNK(m):
        return "link"
    return "other"


def _stat_entry(path, name=None, follow=True):
    try:
        lst = os.lstat(path)
    except OSError as e:
        raise _os_error(e, path)
    entry = {
        "name": name if name is not None else os.path.basename(path),
        "type": _ftype(lst),
        "size": lst.st_size,
        "mtime": lst.st_mtime,
        "mode": statmod.S_IMODE(lst.st_mode),
        "uid": lst.st_uid,
    }
    if statmod.S_ISLNK(lst.st_mode):
        try:
            entry["target"] = os.readlink(path)
            if follow:
                st = os.stat(path)
                entry["type"] = _ftype(st)
                entry["size"] = st.st_size
                entry["link"] = True
        except OSError:
            entry["broken"] = True
    return entry


def _is_binary(chunk):
    return b"\x00" in chunk


def _decode(b):
    return b.decode("utf-8", errors="replace")


def _tail_bytes(f, size, nlines, max_bytes):
    """Return (last nlines lines bounded by max_bytes, bytes skipped before them)."""
    if size == 0:
        return b"", 0
    want = min(size, max_bytes)
    start = size - want
    f.seek(start)
    data = f.read(want)
    partial_first = False
    if start > 0:
        f.seek(start - 1)
        partial_first = f.read(1) != b"\n"
    lines = data.splitlines(True)
    if partial_first and len(lines) > 1:
        lines = lines[1:]
    if len(lines) > nlines:
        lines = lines[-nlines:]
    out = b"".join(lines)
    return out, size - len(out)


def _kill_group(proc, sig):
    """Signal ``proc``'s whole process group (spawned with ``start_new_session=True``).

    Falls back to signalling just the process if the group can't be resolved. Swallows the
    races where the process/group has already gone away.
    """
    try:
        os.killpg(os.getpgid(proc.pid), sig)
        return True
    except (ProcessLookupError, OSError):
        try:
            proc.send_signal(sig)
            return True
        except (ProcessLookupError, OSError):
            return False


def _kill_proc(proc, new_session):
    """SIGKILL ``proc`` (its whole process group when it was started in a new session)."""
    if new_session:
        _kill_group(proc, signal.SIGKILL)
        return
    try:
        proc.kill()
    except OSError:
        pass


def _close_quiet(f):
    if f is None:
        return
    try:
        f.close()
    except (OSError, ValueError):
        pass


def _start_linger_drain(files):
    """Hand a lingering run's still-open pipes to a drain thread.

    Returns ``(log_path_or_None, started)``. ``started`` is false when ``LINGER_MAX_DRAINS``
    drains are already running; the caller then closes the pipes itself.
    """
    with _LINGER_LOCK:
        if _LINGER_ACTIVE[0] >= LINGER_MAX_DRAINS:
            return None, False
        _LINGER_ACTIVE[0] += 1
    try:
        stamp = time.strftime("%Y%m%d-%H%M%S")
        tag = codecs.encode(os.urandom(3), "hex").decode()
        d = _proc_dir()
        _prune_lingering_logs(d)
        path = os.path.join(d, "lingering-%s-%s.log" % (stamp, tag))
        out = io.open(path, "ab")
    except (StubError, OSError):
        path, out = None, None
    t = threading.Thread(target=_drain_lingering, args=(files, out), name="rs-linger")
    t.daemon = True
    try:
        t.start()
    except Exception:  # e.g. "can't start new thread": fall back to closing the pipes
        _close_quiet(out)
        if path:
            try:
                os.unlink(path)
            except OSError:
                pass
        with _LINGER_LOCK:
            _LINGER_ACTIVE[0] -= 1
        return None, False
    return path, True


def _prune_lingering_logs(d):
    """Remove lingering-output logs untouched for ``LINGER_KEEP_DAYS`` (best effort)."""
    cutoff = time.time() - LINGER_KEEP_DAYS * 86400
    try:
        names = os.listdir(d)
    except OSError:
        return
    for n in names:
        if n.startswith("lingering-") and n.endswith(".log"):
            p = os.path.join(d, n)
            try:
                if os.path.getmtime(p) < cutoff:
                    os.unlink(p)
            except OSError:
                pass


def _drain_lingering(files, out):
    """Copy what a run's background children still write into ``out`` until they close the
    pipes. After ``LINGER_LOG_MAX`` bytes it stops and closes the pipes (the writers then get
    SIGPIPE). With no ``out`` the bytes are discarded but still count against the cap."""
    sel = selectors.DefaultSelector()
    seen = 0
    try:
        for f in files:
            sel.register(f, selectors.EVENT_READ)
        live = len(files)
        while live and seen < LINGER_LOG_MAX:
            for key, _events in sel.select():
                try:
                    data = os.read(key.fd, STREAM_READ_BYTES)
                except OSError:
                    data = b""
                if not data:
                    sel.unregister(key.fileobj)
                    live -= 1
                    continue
                keep = data[: max(0, LINGER_LOG_MAX - seen)]
                seen += len(keep)
                if out is not None and keep:
                    out.write(keep)
                    out.flush()
        if live and out is not None:
            out.write(b"\n[remoteslurm: lingering-output cap reached; stopped reading]\n")
    except Exception:  # pragma: no cover - a drain thread must never die noisily
        pass
    finally:
        sel.close()
        for f in files:
            _close_quiet(f)
        _close_quiet(out)
        with _LINGER_LOCK:
            _LINGER_ACTIVE[0] -= 1


def _communicate(proc, argv, timeout, stdin, max_output, emit, new_session, t0):
    """Drive ``proc`` to completion with bounded memory and bounded time.

    One selector loop feeds ``stdin`` and reads stdout/stderr. At most ``max_output`` bytes per
    stream are decoded and kept (and, when ``emit`` is given, passed on as
    ``{"stream": "stdout"|"stderr", "data": <text>}`` as they arrive); past that the stream is
    flagged truncated but still drained so the child never blocks on a full pipe.

    Every wait is bounded, so a stray process holding the pipes can never pin a worker:

    * once the command itself exits, the pipes are read for at most ``RUN_DRAIN_GRACE`` more
      seconds. If a background child still holds them, the result comes back with
      ``lingering: true`` and the still-open pipes are handed to a drain thread that copies
      the child's further output into ``lingering_log`` (see ``_drain_lingering``), so the
      child keeps running while this stub lives.
    * on timeout the process (group) is killed and leftover output is read for at most
      ``KILL_DRAIN`` seconds before ``StubError("timeout")`` is raised with what was captured.
      A child that left the group (``setsid``) survives the kill but can no longer block us.
    """
    sel = selectors.DefaultSelector()
    open_reads = set()
    for name, f in (("stdout", proc.stdout), ("stderr", proc.stderr)):
        if f is not None:
            try:
                os.set_blocking(f.fileno(), False)
            except (AttributeError, OSError):  # pragma: no cover - older/non-POSIX fallback
                pass
            sel.register(f, selectors.EVENT_READ, name)
            open_reads.add(name)
    in_buf = stdin.encode("utf-8") if stdin is not None else b""
    in_off = 0
    writing = False
    if proc.stdin is not None:
        if in_buf:
            try:
                os.set_blocking(proc.stdin.fileno(), False)
            except (AttributeError, OSError):  # pragma: no cover - older/non-POSIX fallback
                pass
            sel.register(proc.stdin, selectors.EVENT_WRITE, "stdin")
            writing = True
        else:
            _close_quiet(proc.stdin)
    kept = {"stdout": [], "stderr": []}  # decoded text within the cap (for the result)
    kept_bytes = {"stdout": 0, "stderr": 0}
    truncated = {"stdout": False, "stderr": False}
    dec = {
        "stdout": codecs.getincrementaldecoder("utf-8")("replace"),
        "stderr": codecs.getincrementaldecoder("utf-8")("replace"),
    }
    deadline = t0 + timeout
    timed_out = False
    lingering = False
    exited_at = None  # when the command exited while its pipes were still open
    drain_until = None  # after a timeout kill: stop reading at this time
    try:
        while open_reads or writing:
            now = time.time()
            if drain_until is not None:
                if now >= drain_until:
                    break
            elif exited_at is None and now >= deadline:  # an exited command is in its grace
                timed_out = True
                _kill_proc(proc, new_session)
                drain_until = now + KILL_DRAIN
            elif proc.poll() is not None:
                if exited_at is None:
                    exited_at = now
                elif now - exited_at >= RUN_DRAIN_GRACE:
                    lingering = True
                    break
            if writing and (drain_until is not None or exited_at is not None):
                sel.unregister(proc.stdin)  # nobody is left to read the rest of stdin
                _close_quiet(proc.stdin)
                writing = False
                continue
            if drain_until is not None:
                limit = drain_until
            elif exited_at is not None:
                limit = exited_at + RUN_DRAIN_GRACE
            else:
                limit = deadline
            for key, _events in sel.select(max(0.0, min(0.25, limit - now))):
                name = key.data
                if name == "stdin":
                    try:
                        in_off += os.write(key.fd, in_buf[in_off : in_off + PIPE_BUF])
                    except BlockingIOError:
                        continue
                    except OSError:  # EPIPE: the child closed its stdin
                        in_off = len(in_buf)
                    if in_off >= len(in_buf):
                        sel.unregister(key.fileobj)
                        _close_quiet(proc.stdin)
                        writing = False
                    continue
                try:
                    data = os.read(key.fd, STREAM_READ_BYTES)
                except BlockingIOError:
                    continue
                except OSError:
                    data = b""
                if not data:
                    sel.unregister(key.fileobj)
                    open_reads.discard(name)
                    continue
                room = max_output - kept_bytes[name]
                if room > 0:
                    take = data[:room]
                    kept_bytes[name] += len(take)
                    text = dec[name].decode(take)
                    if text:
                        kept[name].append(text)
                        if emit is not None:
                            emit({"stream": name, "data": text})
                    if len(data) > room:
                        truncated[name] = True
                else:
                    truncated[name] = True
    finally:
        sel.close()
    if not timed_out and not lingering:
        try:
            proc.wait(timeout=max(0.1, deadline - time.time()))
        except subprocess.TimeoutExpired:
            # The pipes closed but the command is still running (it closed its own stdio).
            timed_out = True
            _kill_proc(proc, new_session)
    if timed_out:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            pass
    handed = []  # pipes passed on to a lingering drain, which closes them itself
    linger_log = None
    if lingering:
        still_open = [proc.stdout if n == "stdout" else proc.stderr for n in sorted(open_reads)]
        linger_log, started = _start_linger_drain(still_open)
        if started:
            handed = still_open
    for f in (proc.stdin, proc.stdout, proc.stderr):
        if f not in handed:
            _close_quiet(f)
    for name in ("stdout", "stderr"):
        tail = dec[name].decode(b"", True)
        if tail:
            kept[name].append(tail)
    if timed_out:
        raise StubError(
            "timeout",
            "command timed out after %ss: %s" % (timeout, " ".join(list(argv)[:4])),
            stdout="".join(kept["stdout"]),
            stderr="".join(kept["stderr"]),
        )
    dur = time.time() - t0
    res = {
        "rc": proc.returncode,
        "stdout": "".join(kept["stdout"]),
        "stderr": "".join(kept["stderr"]),
        "stdout_truncated": truncated["stdout"],
        "stderr_truncated": truncated["stderr"],
        "duration": round(dur, 3),
    }
    if lingering:
        res["lingering"] = True
        head = "the command exited but a background process it started still holds stdout/stderr"
        if handed:
            res["lingering_log"] = linger_log
            where = (
                "goes to lingering_log (wait on it with wait(path=..., pattern=...))"
                if linger_log
                else "is discarded (no writable log directory)"
            )
            res["note"] = (
                "%s; its further output %s. It keeps running only while this stub session "
                "lives: use run(detach=True) for work that must outlive the session."
                % (head, where)
            )
        else:
            res["note"] = (
                "%s; too many lingering runs are already being drained, so this one's pipes "
                "were closed and that process will die (SIGPIPE) at its next write. Use "
                "run(detach=True) for background work." % head
            )
    return res


def _run(
    argv,
    timeout=DEFAULT_RUN_TIMEOUT,
    cwd=None,
    env=None,
    stdin=None,
    max_output=DEFAULT_RUN_OUTPUT,
    on_spawn=None,
    new_session=False,
    emit=None,
):
    if not isinstance(argv, (list, tuple)) or not argv or not all(isinstance(a, str) for a in argv):
        raise StubError("invalid_arg", "argv must be a non-empty list of strings")
    if cwd is not None:
        cwd = _path(cwd, must_exist=True)
    full_env = None
    if env:
        full_env = dict(os.environ)
        for k, v in env.items():
            full_env[str(k)] = str(v)
    t0 = time.time()
    try:
        # ``start_new_session=True`` puts the child in its own process group so that a cancel
        # (or a timeout) can kill the whole tree with ``killpg``, not just the immediate child.
        proc = subprocess.Popen(
            list(argv),
            cwd=cwd,
            env=full_env,
            stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=new_session,
        )
    except FileNotFoundError:
        raise StubError("not_found", "command not found: %s" % argv[0], command=argv[0])
    except PermissionError:
        raise StubError("permission", "cannot execute: %s" % argv[0], command=argv[0])
    if on_spawn is not None:
        # Register the live process so `cancel` can find it; must happen before we block in
        # communicate(). Registration failure must never take the command down.
        try:
            on_spawn(proc)
        except Exception:  # pragma: no cover - defensive
            pass
    # Streaming and non-streaming runs share one bounded loop (``emit`` is None for the latter).
    return _communicate(proc, argv, timeout, stdin, max_output, emit, new_session, t0)


def _which(name):
    return shutil.which(name)


# --------------------------------------------------------------------------- ops


def op_ping(args):
    return {
        "pid": os.getpid(),
        "time": time.time(),
        "protocol": PROTOCOL,
        "stub_sha": _stub_sha(),
    }


def op_info(args):
    requested = args.get("env_vars") or []
    valid_names = isinstance(requested, (list, tuple)) and all(
        isinstance(k, str) and re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", k) for k in requested
    )
    if not valid_names or len(requested) > 64:
        raise StubError("invalid_arg", "env_vars must contain at most 64 environment names")
    env_keys = ["HOME", "USER", "SLURM_CLUSTER_NAME", "TMPDIR"]
    env_keys.extend(k for k in requested if k not in env_keys)
    env = {k: os.environ[k] for k in env_keys if k in os.environ}
    slurm_version = None
    if _which("sinfo"):
        try:
            r = _run(["sinfo", "--version"], timeout=10)
            slurm_version = (r["stdout"] or r["stderr"]).strip()
        except StubError:
            pass
    return {
        "user": getpass.getuser(),
        "hostname": socket.gethostname(),
        "home": os.path.expanduser("~"),
        "cwd": os.getcwd(),
        "python": sys.version.split()[0],
        "stub": os.path.abspath(__file__),
        "protocol": PROTOCOL,
        "stub_sha": _stub_sha(),
        "env": env,
        "slurm_version": slurm_version,
        "slurm_tools": {
            n: bool(_which(n))
            for n in ("sbatch", "squeue", "sstat", "sacct", "scancel", "scontrol", "sinfo")
        },
    }


def op_ls(args):
    p = _path(args.get("path", "~"), must_exist=True)
    limit = _clamp(args.get("limit"), DEFAULT_LS_LIMIT, MAX_LS_LIMIT)
    hidden = bool(args.get("hidden", True))
    token = args.get("token")
    start = 0
    if token:
        try:
            start = int(token)
        except ValueError:
            raise StubError("invalid_arg", "bad pagination token")
    if not os.path.isdir(p):
        return {
            "path": p,
            "entries": [_stat_entry(p)],
            "total": 1,
            "truncated": False,
            "next_token": None,
        }
    try:
        names = os.listdir(p)
    except OSError as e:
        raise _os_error(e, p)
    if not hidden:
        names = [n for n in names if not n.startswith(".")]
    names.sort()
    total = len(names)
    sel = names[start : start + limit]
    entries = []
    for n in sel:
        try:
            entries.append(_stat_entry(os.path.join(p, n), name=n))
        except StubError as e:
            entries.append({"name": n, "type": "other", "error": e.message})
    end = start + len(sel)
    return {
        "path": p,
        "entries": entries,
        "total": total,
        "offset": start,
        "truncated": end < total,
        "next_token": str(end) if end < total else None,
    }


def op_stat(args):
    p = _path(args.get("path"), must_exist=True)
    e = _stat_entry(p)
    e["path"] = p
    return e


def op_read(args):
    p = _path(args.get("path"), must_exist=True)
    max_bytes = _clamp(args.get("max_bytes"), DEFAULT_READ_BYTES, MAX_READ_BYTES)
    offset = args.get("offset", 0) or 0
    tail = args.get("tail")  # last N lines
    head = args.get("head")  # first N lines
    if os.path.isdir(p):
        raise StubError("invalid_arg", "is a directory: %s" % p, path=p)
    try:
        size = os.path.getsize(p)
        with io.open(p, "rb") as f:
            probe = f.read(8192)
            binary = _is_binary(probe)
            f.seek(0)
            if tail is not None:
                nlines = _clamp(tail, 50, 100000)
                data, skipped = _tail_bytes(f, size, nlines, max_bytes)
                offset = skipped
                eof = True
                truncated = skipped > 0
            else:
                if offset < 0:
                    offset = max(0, size + offset)
                f.seek(offset)
                if head is not None:
                    nlines = _clamp(head, 50, 100000)
                    buf = []
                    n = 0
                    got = 0
                    while n < nlines and got < max_bytes:
                        line = f.readline(max_bytes - got)
                        if not line:
                            break
                        buf.append(line)
                        got += len(line)
                        n += 1
                    data = b"".join(buf)
                    eof = f.tell() >= size
                    truncated = not eof
                else:
                    data = f.read(max_bytes)
                    eof = f.tell() >= size
                    truncated = not eof
    except OSError as e:
        raise _os_error(e, p)
    res = {
        "path": p,
        "size": size,
        "offset": offset,
        "length": len(data),
        "eof": eof,
        "truncated": truncated,
        "binary": binary,
    }
    if binary:
        res["content_b64"] = base64.b64encode(data).decode("ascii")
    else:
        res["content"] = _decode(data)
        res["lines"] = data.count(b"\n")
    return res


def _atomic_write(p, data, keep_mode=True, mode=None):
    """Write `data` to `p` atomically. Follows a symlink to its target (so editing a symlinked
    file edits the target, not the link), uses a unique tmp name (safe across the stub's worker
    threads), preserves the existing mode unless `mode` is given, and never leaves the tmp behind.
    """
    real = os.path.realpath(p)
    target = real if os.path.islink(p) else p
    prev_mode = None
    try:
        prev_mode = statmod.S_IMODE(os.stat(target).st_mode)
    except OSError:
        pass
    tmp = "%s.%d.%d.tmp" % (target, os.getpid(), threading.get_ident())
    try:
        with io.open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, target)
    except OSError:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise
    if mode is not None:
        os.chmod(target, int(mode))
    elif keep_mode and prev_mode is not None:
        os.chmod(target, prev_mode)
    return target


def op_expandpath(args):
    """Expand ``~``/``~user``/``$VARS`` in a path using the *remote* environment, shell-free.

    Used by sync/put/get so remote paths like ``$WORK/proj`` resolve correctly without ever
    interpolating user input into a shell (no command substitution, no word splitting, no globbing).
    """
    raw = args.get("path")
    if not isinstance(raw, str) or not raw:
        raise StubError("invalid_arg", "path must be a non-empty string")
    if "\x00" in raw:
        raise StubError("invalid_arg", "path contains NUL byte")
    expanded = os.path.expanduser(os.path.expandvars(raw))
    if "$" in expanded:
        # A variable that isn't set on the remote survives expandvars unchanged; producing a
        # path anyway would silently target the wrong place (e.g. $WORK unset -> "/analysis").
        raise StubError(
            "invalid_arg",
            "path contains an environment variable that is not set on the remote: %s" % raw,
            path=raw,
            action="check the variable is exported on the login node (e.g. echo $WORK)",
        )
    if not os.path.isabs(expanded):
        expanded = os.path.join(os.path.expanduser("~"), expanded)
    return {"input": raw, "path": os.path.normpath(expanded)}


def op_write(args):
    p = _path(args.get("path"))
    if "content_b64" in args:
        data = base64.b64decode(args["content_b64"])
    else:
        c = args.get("content", "")
        if not isinstance(c, str):
            raise StubError("invalid_arg", "content must be a string")
        data = c.encode("utf-8")
    if len(data) > MAX_WRITE_BYTES:
        raise StubError("too_large", "write exceeds %d bytes" % MAX_WRITE_BYTES, size=len(data))
    append = bool(args.get("append", False))
    mkdirs = bool(args.get("mkdirs", False))
    mode = args.get("mode")
    d = os.path.dirname(p)
    try:
        if mkdirs and d and not os.path.isdir(d):
            os.makedirs(d, exist_ok=True)
        if append:
            # append follows a symlink to its target, matching _atomic_write's behaviour
            with io.open(p, "ab") as f:
                f.write(data)
            if mode is not None:
                os.chmod(p, int(mode))
        else:
            _atomic_write(p, data, keep_mode=True, mode=mode)
        st = os.stat(p)
    except OSError as e:
        raise _os_error(e, p)
    return {"path": p, "size": st.st_size, "written": len(data)}


def _read_all_text_bytes(p):
    """Whole-file read for edit/diff: bounded to MAX_EDIT_BYTES, binaries refused."""
    if os.path.isdir(p):
        raise StubError("invalid_arg", "is a directory: %s" % p, path=p)
    try:
        size = os.path.getsize(p)
        if size > MAX_EDIT_BYTES:
            raise StubError(
                "too_large", "file exceeds %d bytes: %s" % (MAX_EDIT_BYTES, p), path=p, size=size
            )
        with io.open(p, "rb") as f:
            data = f.read()
    except OSError as e:
        raise _os_error(e, p)
    if _is_binary(data[:8192]):
        raise StubError("invalid_arg", "binary file: %s" % p, path=p)
    return data


def _closest_lines(data, old_b):
    """Up to 3 lines of the file that look like the (missing) old string."""
    target_lines = _decode(old_b).splitlines()
    target = target_lines[0].strip()[:200] if target_lines else ""
    if not target:
        return []
    cands = []
    seen = set()
    for ln in _decode(data).splitlines()[:5000]:
        s = ln.strip()[:200]
        if s and s not in seen:
            seen.add(s)
            cands.append(s)
    return difflib.get_close_matches(target, cands, n=3, cutoff=0.4)


def _occurrence_lines(data, old_b, cap=20):
    """1-based line numbers of each occurrence of old_b (at most cap)."""
    lines = []
    idx = data.find(old_b)
    while idx != -1 and len(lines) < cap:
        lines.append(data.count(b"\n", 0, idx) + 1)
        idx = data.find(old_b, idx + len(old_b))
    return lines


def _diff_preview(a, b):
    """Unified diff (context 3) of the change, bounded to EDIT_PREVIEW_BYTES chars."""
    buf = []
    n = 0
    for ln in difflib.unified_diff(
        _decode(a).splitlines(), _decode(b).splitlines(), n=3, lineterm=""
    ):
        buf.append(ln)
        n += len(ln) + 1
        if n > EDIT_PREVIEW_BYTES:
            break
    return "\n".join(buf)[:EDIT_PREVIEW_BYTES]


def op_edit(args):
    """Replace exact occurrences of `old` with `new` in a text file, atomically.

    Byte-based replacement, so line endings are preserved by construction. The
    file is replaced via `path.<pid>.tmp` + os.replace: the inode changes and
    hard links are not preserved; the previous mode is restored with chmod.
    """
    p = _path(args.get("path"), must_exist=True)
    old = args.get("old")
    new = args.get("new")
    if not isinstance(old, str) or not isinstance(new, str):
        raise StubError("invalid_arg", "old and new must be strings")
    if old == "":
        raise StubError("invalid_arg", "old must not be empty", path=p)
    if old == new:
        raise StubError("invalid_arg", "old and new are identical", path=p)
    replace_all = bool(args.get("all", False))
    expect = args.get("expect", 1)
    try:
        expect = int(expect)
    except (TypeError, ValueError):
        raise StubError("invalid_arg", "expect must be an integer")
    if expect < 1:
        raise StubError("invalid_arg", "expect must be >= 1")
    data = _read_all_text_bytes(p)
    old_b = old.encode("utf-8")
    new_b = new.encode("utf-8")
    count = data.count(old_b)
    if count == 0:
        raise StubError(
            "not_found",
            "old string not found in %s" % p,
            path=p,
            closest=_closest_lines(data, old_b),
        )
    if not replace_all and count != expect:
        raise StubError(
            "invalid_arg",
            "found %d occurrence(s) of old in %s, expected %d" % (count, p, expect),
            path=p,
            count=count,
            expect=expect,
            lines=_occurrence_lines(data, old_b),
            action="pass all=true to replace every occurrence, or make old more specific",
        )
    first_idx = data.find(old_b)
    first_line = data.count(b"\n", 0, first_idx) + 1
    new_data = data.replace(old_b, new_b)
    if len(new_data) > MAX_EDIT_BYTES:
        raise StubError("too_large", "edited file would exceed %d bytes" % MAX_EDIT_BYTES, path=p)
    try:
        _atomic_write(p, new_data, keep_mode=True)
    except OSError as e:
        raise _os_error(e, p)
    return {
        "path": p,
        "replacements": count,
        "first_line": first_line,
        "preview": _diff_preview(data, new_data),
    }


def op_diff(args):
    """Unified diff between a remote text file and given content (or another file)."""
    p = _path(args.get("path"), must_exist=True)
    a = _read_all_text_bytes(p)
    label_b = "<content>"
    if args.get("content_b64") is not None:
        b = base64.b64decode(args["content_b64"])
    elif args.get("content") is not None:
        c = args["content"]
        if not isinstance(c, str):
            raise StubError("invalid_arg", "content must be a string")
        b = c.encode("utf-8")
    elif args.get("path_b"):
        pb = _path(args["path_b"], must_exist=True)
        b = _read_all_text_bytes(pb)
        label_b = pb
    else:
        raise StubError("invalid_arg", "one of content, content_b64 or path_b is required")
    if _is_binary(b[:8192]):
        raise StubError("invalid_arg", "binary content")
    context = _clamp(args.get("context"), 3, 100, lo=0)
    max_lines = _clamp(args.get("max_lines"), DEFAULT_DIFF_LINES, MAX_DIFF_LINES)
    identical = a == b
    lines = []
    truncated = False
    if not identical:
        for ln in difflib.unified_diff(
            _decode(a).splitlines(),
            _decode(b).splitlines(),
            fromfile=p,
            tofile=label_b,
            n=context,
            lineterm="",
        ):
            if len(lines) >= max_lines:
                truncated = True
                break
            lines.append(ln[:DIFF_MAX_LINE])
    return {
        "path": p,
        "path_b": label_b,
        "diff": "\n".join(lines),
        "lines": len(lines),
        "identical": identical,
        "truncated": truncated,
    }


def op_mkdir(args):
    p = _path(args.get("path"))
    try:
        os.makedirs(p, exist_ok=True)
    except OSError as e:
        raise _os_error(e, p)
    return {"path": p}


def _rm_roots(configured):
    """Directories recursive ``rm`` must never remove wholesale: home + configured roots."""
    valid_roots = isinstance(configured, (list, tuple)) and all(
        isinstance(v, str) for v in configured
    )
    if not valid_roots:
        raise StubError("invalid_arg", "protected_roots must be a list of strings")
    roots = set()
    for raw in configured:
        v = os.path.expanduser(os.path.expandvars(raw))
        if "$" in v:
            raise StubError(
                "invalid_arg",
                "protected root contains an environment variable that is not set: %s" % raw,
                path=raw,
                action="fix protected_roots or export the variable on the login node",
            )
        if v:
            roots.add(os.path.normpath(v))
    roots.add(os.path.normpath(os.path.expanduser("~")))
    return roots


def op_rm(args):
    p = _path(args.get("path"), must_exist=True)
    recursive = bool(args.get("recursive", False))
    home = os.path.expanduser("~")
    if p in ("/", home) or p == os.path.dirname(home):
        raise StubError("invalid_arg", "refusing to remove %s" % p, path=p)
    if recursive:
        # A recursive delete is the dangerous one: refuse home/configured roots themselves
        # and anything shallower than three path components
        # (e.g. /scratch/<user>), which are almost always a fat-fingered target.
        if p in _rm_roots(args.get("protected_roots") or []):
            raise StubError(
                "invalid_arg",
                "refusing to recursively remove the root directory %s" % p,
                path=p,
                action="delete a specific subdirectory, not the whole root",
            )
        depth = len([seg for seg in p.split("/") if seg])
        if depth < 3:
            raise StubError(
                "invalid_arg",
                "refusing recursive remove of shallow path %s (depth %d < 3)" % (p, depth),
                path=p,
                action="target a deeper subdirectory, or remove entries individually",
            )
    try:
        if os.path.isdir(p) and not os.path.islink(p):
            if recursive:
                shutil.rmtree(p)
            else:
                os.rmdir(p)
        else:
            os.remove(p)
    except OSError as e:
        if getattr(e, "errno", None) == errno.ENOTEMPTY:
            raise StubError("invalid_arg", "directory not empty (use recursive): %s" % p, path=p)
        raise _os_error(e, p)
    return {"path": p, "removed": True}


def op_glob(args):
    root = _path(args.get("path", "."), must_exist=True)
    pattern = args.get("pattern", "*")
    limit = _clamp(args.get("limit"), DEFAULT_GLOB_LIMIT, MAX_GLOB_LIMIT)
    max_depth = _clamp(args.get("max_depth"), 10, 100, lo=0)
    want_type = args.get("type")  # file|dir|None
    hidden = bool(args.get("hidden", False))
    results = []
    scanned = 0
    truncated = False
    root_depth = root.rstrip("/").count("/")
    for dirpath, dirnames, filenames in os.walk(root):
        depth = dirpath.rstrip("/").count("/") - root_depth
        if not hidden:
            dirnames[:] = [d for d in dirnames if not d.startswith(".")]
            filenames = [f for f in filenames if not f.startswith(".")]
        if depth >= max_depth:
            dirnames[:] = []
        dirnames.sort()
        cands = []
        if want_type in (None, "dir"):
            cands.extend((d, "dir") for d in dirnames)
        if want_type in (None, "file"):
            cands.extend((f, "file") for f in sorted(filenames))
        for name, t in cands:
            scanned += 1
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, root)
            if fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(rel, pattern):
                if len(results) >= limit:
                    truncated = True
                    break
                try:
                    st = os.lstat(full)
                    results.append(
                        {"path": full, "type": t, "size": st.st_size, "mtime": st.st_mtime}
                    )
                except OSError:
                    results.append({"path": full, "type": t})
        if truncated:
            break
    return {"root": root, "matches": results, "scanned": scanned, "truncated": truncated}


def op_progress_count(args):
    """Count matching files without returning a potentially huge artifact listing."""
    raw = args.get("path")
    if not isinstance(raw, str) or not raw or "\x00" in raw:
        raise StubError("invalid_arg", "progress path must be non-empty text without NUL")
    if not os.path.isabs(os.path.expanduser(os.path.expandvars(raw))):
        base = _path(args.get("base") or "~")
        raw = os.path.join(base, raw)
    root = _path(raw)
    pattern = args.get("pattern", "*")
    if not isinstance(pattern, str) or not pattern or "\x00" in pattern:
        raise StubError("invalid_arg", "progress pattern must be non-empty text without NUL")
    max_depth = _clamp(args.get("max_depth"), 10, 100, lo=0)
    max_scan = _clamp(args.get("max_scan"), DEFAULT_PROGRESS_SCAN, MAX_PROGRESS_SCAN, lo=1)
    hidden = bool(args.get("hidden", False))
    if not os.path.lexists(root):
        return {
            "kind": "file_count",
            "path": root,
            "pattern": pattern,
            "exists": False,
            "observed": 0,
            "scanned": 0,
            "truncated": False,
        }
    if not os.path.isdir(root):
        name = os.path.basename(root)
        observed = int(os.path.isfile(root) and fnmatch.fnmatch(name, pattern))
        return {
            "kind": "file_count",
            "path": root,
            "pattern": pattern,
            "exists": True,
            "observed": observed,
            "scanned": 1,
            "truncated": False,
        }
    observed = 0
    scanned = 0
    truncated = False
    root_depth = root.rstrip("/").count("/")
    for dirpath, dirnames, filenames in os.walk(root):
        depth = dirpath.rstrip("/").count("/") - root_depth
        if not hidden:
            dirnames[:] = [name for name in dirnames if not name.startswith(".")]
            filenames = [name for name in filenames if not name.startswith(".")]
        if depth >= max_depth:
            dirnames[:] = []
        for name in filenames:
            if scanned >= max_scan:
                truncated = True
                break
            scanned += 1
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, root)
            if (fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(rel, pattern)) and os.path.isfile(
                full
            ):
                observed += 1
        if truncated:
            break
    return {
        "kind": "file_count",
        "path": root,
        "pattern": pattern,
        "exists": True,
        "observed": observed,
        "scanned": scanned,
        "truncated": truncated,
    }


def op_grep(args):
    pattern = args.get("pattern")
    if not isinstance(pattern, str) or not pattern:
        raise StubError("invalid_arg", "pattern required")
    root = _path(args.get("path", "."), must_exist=True)
    glob = args.get("glob")
    limit = _clamp(args.get("max_matches"), DEFAULT_GREP_MATCHES, MAX_GREP_MATCHES)
    ignore_case = bool(args.get("ignore_case", False))
    max_depth = _clamp(args.get("max_depth"), 10, 100, lo=0)
    max_file_size = _clamp(args.get("max_file_size"), GREP_MAX_FILE_SIZE, 1 << 31)
    hidden = bool(args.get("hidden", False))
    context = _clamp(args.get("context"), 0, 5, lo=0)
    max_line = 500
    try:
        rx = re.compile(pattern.encode("utf-8"), re.IGNORECASE if ignore_case else 0)
    except re.error as e:
        raise StubError("invalid_arg", "bad regex: %s" % e)
    matches = []
    files_scanned = 0
    files_skipped = 0
    truncated = False

    def scan(full):
        nonlocal truncated
        try:
            st = os.stat(full)
            if not statmod.S_ISREG(st.st_mode):
                return
            if st.st_size > max_file_size:
                return "skipped"
            with io.open(full, "rb") as f:
                head = f.read(8192)
                if _is_binary(head):
                    return "skipped"
                f.seek(0)
                prev = []
                for i, line in enumerate(f, 1):
                    if rx.search(line):
                        m = {
                            "file": full,
                            "line": i,
                            "text": _decode(line.rstrip(b"\r\n")[:max_line]),
                        }
                        if context:
                            m["before"] = [_decode(x.rstrip(b"\r\n")[:max_line]) for x in prev]
                        matches.append(m)
                        if len(matches) >= limit:
                            truncated = True
                            return
                    if context:
                        prev.append(line)
                        if len(prev) > context:
                            prev.pop(0)
        except OSError:
            return "skipped"

    if os.path.isfile(root):
        files_scanned = 1
        if scan(root) == "skipped":
            files_skipped += 1
    else:
        root_depth = root.rstrip("/").count("/")
        for dirpath, dirnames, filenames in os.walk(root):
            depth = dirpath.rstrip("/").count("/") - root_depth
            if not hidden:
                dirnames[:] = [d for d in dirnames if not d.startswith(".")]
                filenames = [f for f in filenames if not f.startswith(".")]
            if depth >= max_depth:
                dirnames[:] = []
            dirnames.sort()
            for name in sorted(filenames):
                if glob and not fnmatch.fnmatch(name, glob):
                    continue
                files_scanned += 1
                if scan(os.path.join(dirpath, name)) == "skipped":
                    files_skipped += 1
                if truncated:
                    break
            if truncated:
                break
    return {
        "root": root,
        "matches": matches,
        "files_scanned": files_scanned,
        "files_skipped": files_skipped,
        "truncated": truncated,
    }


def op_run(args):
    argv = args.get("argv")
    cmd = args.get("cmd")
    if cmd is not None:
        if not isinstance(cmd, str):
            raise StubError("invalid_arg", "cmd must be a string")
        shell = ["bash", "-lc", cmd] if args.get("login") else ["bash", "-c", cmd]
        argv = shell
    timeout = _clamp(args.get("timeout"), DEFAULT_RUN_TIMEOUT, MAX_RUN_TIMEOUT)
    max_output = _clamp(args.get("max_output"), DEFAULT_RUN_OUTPUT, MAX_RUN_OUTPUT)
    return _run(
        argv,
        timeout=timeout,
        cwd=args.get("cwd"),
        env=args.get("env"),
        stdin=args.get("stdin"),
        max_output=max_output,
        on_spawn=args.get("_register"),
        new_session=True,
        emit=args.get("_emit"),
    )


def _srun_flags(args):
    """Build the ``srun`` option flags (no shell) from the resource args."""
    # A compute run executes one command. Some sites infer the step task count from the whole
    # allocation when it is omitted (for example, 192 CPUs with `-c 2` can become 96 tasks),
    # duplicating the command once per inferred task. Pin the step to one task while `-c`
    # continues to describe the CPUs assigned to that task.
    flags = ["srun", "--quiet", "--unbuffered", "--ntasks=1"]
    part = args.get("partition")
    if part:
        flags += ["-p", str(part)]
    walltime = args.get("time")
    if walltime:
        flags += ["-t", str(walltime)]
    cpus = args.get("cpus")
    if cpus:
        flags += ["-c", str(cpus)]
    mem = args.get("mem")
    if mem:
        flags += ["--mem", str(mem)]
    gpus = args.get("gpus")
    if gpus:
        flags.append("--gres=gpu:%s" % gpus)
    account = args.get("account")
    if account:
        flags += ["-A", str(account)]
    return flags


def _extract_node(stderr):
    """Pull the ``RS_NODE=<name>`` sentinel out of stderr; return (node, cleaned_stderr).

    Its presence means the wrapped command actually started (the allocation was granted), which
    is how we tell a real run from one that only ever sat in the queue.
    """
    node = None
    kept = []
    for line in stderr.splitlines(True):
        stripped = line.strip()
        if node is None and stripped.startswith(RS_NODE_SENTINEL):
            node = stripped[len(RS_NODE_SENTINEL) :] or None
            continue
        kept.append(line)
    return node, "".join(kept)


def _srun_result(stdout, stderr, rc, queue_timeout, elapsed, timed_out=False):
    """Shape a raw srun run into the started/queued result the client expects."""
    node, clean_err = _extract_node(stderr)
    if node is not None:
        return {
            "started": True,
            "rc": rc,
            "stdout": stdout,
            "stderr": clean_err,
            "node": node,
            "elapsed": elapsed,
            "timed_out": timed_out,
        }
    # Never allocated: distinguish "still queued" from a hard srun error.
    if "queued and waiting for resources" in stderr or timed_out:
        reason = "still queued after %ss" % queue_timeout
    else:
        reason = (clean_err.strip().splitlines() or ["srun did not start the job"])[-1]
    return {
        "started": False,
        "reason": reason,
        "rc": rc,
        "stdout": stdout,
        "stderr": clean_err,
    }


def op_srun(args):
    """Run a command on a *compute node* via ``srun`` (a SLOW, cancellable op).

    Wraps the command so it first echoes an ``RS_NODE=$SLURMD_NODENAME`` sentinel to stderr;
    the client uses that to report the node and to tell a real run from one that only sat in
    the queue. The total client budget (``timeout``) is queue-wait + walltime + slack, computed
    client-side; ``queue_timeout`` is used only for the "still queued" message. Because the step
    runs in its own session, ``cancel``/timeout kills the whole ``srun`` tree, which releases
    the allocation.
    """
    if not _which("srun"):
        raise StubError("slurm_error", "srun not found on PATH (is this a Slurm login node?)")
    argv_in = args.get("argv")
    cmd = args.get("cmd")
    login = bool(args.get("login"))
    flag = "-lc" if login else "-c"
    sentinel = 'echo "%s$SLURMD_NODENAME" >&2; ' % RS_NODE_SENTINEL
    if cmd is not None:
        if not isinstance(cmd, str):
            raise StubError("invalid_arg", "cmd must be a string")
        wrapped = ["bash", flag, sentinel + cmd]
    elif argv_in is not None:
        if (
            not isinstance(argv_in, (list, tuple))
            or not argv_in
            or not all(isinstance(a, str) for a in argv_in)
        ):
            raise StubError("invalid_arg", "argv must be a non-empty list of strings")
        # `exec "$@"` runs the argv vector with no shell parsing of the user's arguments; the
        # sentinel echo is the only shell we add.
        wrapped = ["bash", flag, sentinel + 'exec "$@"', "rs-srun"] + list(argv_in)
    else:
        raise StubError("invalid_arg", "either argv or cmd is required")
    full = _srun_flags(args) + ["--"] + wrapped
    timeout = _clamp(args.get("timeout"), DEFAULT_QUEUE_TIMEOUT + 30, MAX_SRUN_TIMEOUT, lo=1)
    max_output = _clamp(args.get("max_output"), DEFAULT_RUN_OUTPUT, MAX_RUN_OUTPUT)
    queue_timeout = args.get("queue_timeout") or DEFAULT_QUEUE_TIMEOUT
    cwd = args.get("cwd")
    t0 = time.time()
    try:
        res = _run(
            full,
            timeout=timeout,
            cwd=cwd,
            env=args.get("env"),
            stdin=args.get("stdin"),
            max_output=max_output,
            on_spawn=args.get("_register"),
            new_session=True,
            # When streaming, output is emitted live; the RS_NODE sentinel line (echoed to
            # stderr before the command runs) is stripped from the final captured stderr but
            # is also emitted as a stderr chunk. No client surface streams srun today.
            emit=args.get("_emit"),
        )
    except StubError as e:
        if e.code == "timeout":
            # Budget exhausted: turn it into a structured started/queued result rather than a
            # bare timeout error (the process group was already killed by `_run`).
            return _srun_result(
                e.details.get("stdout", ""),
                e.details.get("stderr", ""),
                None,
                queue_timeout,
                round(time.time() - t0, 3),
                timed_out=True,
            )
        raise
    return _srun_result(res["stdout"], res["stderr"], res["rc"], queue_timeout, res.get("duration"))


def op_follow(args):
    """Tail a file, emitting appended bytes as stream chunks until idle or cancelled.

    A streaming, cancellable SLOW op: seek to ``offset`` (negative = from the end), then poll
    every ``FOLLOW_POLL`` seconds, emitting any appended bytes as
    ``{"stream": "stdout", "data": <text>}`` chunks. Stops when ``idle_timeout`` seconds pass
    with no new data (final ``eof: true``) or the request is cancelled via its registered
    ``threading.Event`` (final ``eof: false``). Each read is bounded by ``max_bytes_per_chunk``
    so the whole file is never loaded. Final result: ``{offset, eof}``.

    This follows a single open fd (like ``tail -f``, not ``tail -F``): if the file is truncated
    or rotated mid-follow, appended content on the new inode is not picked up (it idles out).
    """
    p = _path(args.get("path"), must_exist=True)
    if os.path.isdir(p):
        raise StubError("invalid_arg", "is a directory: %s" % p, path=p)
    emit = args.get("_emit")
    cancel_event = args.get("_cancel_event")
    idle_timeout = _clamp(args.get("idle_timeout"), FOLLOW_IDLE_DEFAULT, FOLLOW_IDLE_MAX, lo=1)
    max_chunk = _clamp(args.get("max_bytes_per_chunk"), FOLLOW_CHUNK_DEFAULT, FOLLOW_CHUNK_MAX)
    raw_offset = args.get("offset", 0) or 0
    try:
        raw_offset = int(raw_offset)
    except (TypeError, ValueError):
        raise StubError("invalid_arg", "offset must be an integer")
    dec = codecs.getincrementaldecoder("utf-8")("replace")
    eof = False
    offset = raw_offset
    try:
        size = os.path.getsize(p)
        if offset < 0:
            offset = max(0, size + offset)
        elif offset > size:
            offset = size
        last_data = time.time()
        last_keepalive = time.time()
        with io.open(p, "rb") as f:
            f.seek(offset)
            while True:
                if cancel_event is not None and cancel_event.is_set():
                    break
                chunk = f.read(max_chunk)
                if chunk:
                    offset += len(chunk)
                    last_data = time.time()
                    text = dec.decode(chunk)
                    if text and emit is not None:
                        emit({"stream": "stdout", "data": text})
                    continue
                now = time.time()
                if now - last_data >= idle_timeout:
                    eof = True
                    break
                # Emit an empty keepalive frame during quiet stretches: if the client (or the
                # daemon forwarding for it) has gone away, this write fails and the op is torn
                # down promptly instead of pinning a worker until idle_timeout (which is 24h for
                # `tail -f`). The client ignores "keepalive" chunks.
                if emit is not None and now - last_keepalive >= FOLLOW_KEEPALIVE:
                    last_keepalive = now
                    emit({"stream": "keepalive", "data": ""})
                # Sleep between polls, but wake immediately if cancel fires.
                if cancel_event is not None:
                    if cancel_event.wait(FOLLOW_POLL):
                        break
                else:
                    time.sleep(FOLLOW_POLL)
    except OSError as e:
        raise _os_error(e, p)
    tail = dec.decode(b"", True)
    if tail and emit is not None:
        emit({"stream": "stdout", "data": tail})
    return {"offset": offset, "eof": eof}


# --------------------------------------------------------------------------- detached runs

# Runs the command, then records its exit status atomically in the rc file, so the status
# survives this stub (and the ssh connection) going away. The trap keeps the wrapper alive
# through HUP/INT/TERM long enough to record the child's status; the child itself still gets the
# default dispositions (caught signals are reset on exec).
_DETACH_WRAPPER = (
    'rs_rc=$1; shift; trap : HUP INT TERM; "$@"; rc=$?; '
    'printf "%s\\n" "$rc" > "$rs_rc.tmp" && mv -f "$rs_rc.tmp" "$rs_rc"; exit "$rc"'
)
_KILL_SIGNALS = {
    "TERM": signal.SIGTERM,
    "INT": signal.SIGINT,
    "HUP": signal.SIGHUP,
    "KILL": signal.SIGKILL,
}
_PROC_DIR = []  # type: list[str]  # the resolved proc dir, cached per stub process


def _proc_dir_candidates():
    home = os.path.expanduser(os.path.join("~", ".cache", "remoteslurm", PROC_SUBDIR))
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), PROC_SUBDIR)
    return [home] if here == home else [home, here]


def _proc_dir():
    """The directory for detached-run records and logs (created owner-only on first use)."""
    if _PROC_DIR:
        return _PROC_DIR[0]
    err = None
    for d in _proc_dir_candidates():
        try:
            os.makedirs(d, 0o700, exist_ok=True)
        except OSError as e:
            err = e
            continue
        if os.access(d, os.W_OK):
            _PROC_DIR.append(d)
            return d
    raise StubError("permission", "no writable directory for detached runs (%s)" % err)


def _hostname():
    return socket.gethostname()


def _proc_starttime(pid):
    """Kernel start time of ``pid`` (Linux ``/proc``), to tell a reused pid apart; else None."""
    try:
        with io.open("/proc/%d/stat" % pid, "rb") as f:
            data = f.read().decode("ascii", "replace")
        return int(data.rsplit(")", 1)[1].split()[19])  # field 22; `comm` may contain ") "
    except (OSError, ValueError, IndexError):
        return None


def _write_atomic_text(path, text):
    tmp = "%s.tmp.%d.%d" % (path, os.getpid(), threading.current_thread().ident or 0)
    with io.open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.rename(tmp, path)


def _read_rc(path):
    """``(rc, finished_at)`` from a detached run's rc file, or None while none is recorded."""
    try:
        with io.open(path, "r", encoding="utf-8") as f:
            rc = int(f.read().split()[0])
        return rc, os.path.getmtime(path)
    except (OSError, ValueError, IndexError):
        return None


def _write_rc(path, rc):
    try:
        _write_atomic_text(path, "%d\n" % rc)
    except OSError:
        pass


def _reap_detached(proc, rc_path):
    """Reap a detached wrapper this stub started; record a signal death the wrapper could not."""
    try:
        rc = proc.wait()
    except Exception:  # pragma: no cover - defensive
        return
    if _read_rc(rc_path) is None:
        _write_rc(rc_path, 128 - rc if rc < 0 else rc)


def _pid_arg(args, required=True):
    pid = args.get("pid")
    if pid is None:
        if required:
            raise StubError("invalid_arg", "pid is required")
        return None
    if isinstance(pid, bool):
        raise StubError("invalid_arg", "pid must be an integer, got %r" % (pid,))
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        raise StubError("invalid_arg", "pid must be an integer, got %r" % (pid,))
    if pid <= 1:
        raise StubError("invalid_arg", "invalid pid: %d" % pid)
    return pid


def _no_record(pid):
    return StubError(
        "not_found",
        "no detached run with pid %d (only processes started with run(detach=True) are tracked)"
        % pid,
        pid=pid,
    )


def _load_record(path):
    try:
        with io.open(path, "r", encoding="utf-8") as f:
            rec = json.load(f)
    except (OSError, ValueError):
        return None
    if not isinstance(rec, dict) or not isinstance(rec.get("pid"), int):
        return None
    return rec


def _record_files():
    """``(mtime, path)`` of every detached-run record, newest first."""
    out = []
    for d in _proc_dir_candidates():
        try:
            names = os.listdir(d)
        except OSError:
            continue
        for n in names:
            if n.endswith(".json"):
                p = os.path.join(d, n)
                try:
                    out.append((os.path.getmtime(p), p))
                except OSError:
                    pass
    out.sort(reverse=True)
    return out


def _find_record(pid):
    """This login node's record for ``pid``, else the newest record from another node."""
    name = "%s.%d.json" % (_hostname(), pid)
    for d in _proc_dir_candidates():
        rec = _load_record(os.path.join(d, name))
        if rec is not None:
            return rec
    suffix = ".%d.json" % pid
    for _m, p in _record_files():
        if p.endswith(suffix):
            rec = _load_record(p)
            if rec is not None:
                return rec
    return None


def _group_alive(rec):
    """Is any process of the run's group still alive (and is it still *our* group)?"""
    try:
        os.killpg(rec.get("pgid") or rec["pid"], 0)
    except OSError:  # ProcessLookupError: all gone; PermissionError: the id is someone else's
        return False
    want = rec.get("starttime")
    if want is not None:
        have = _proc_starttime(rec["pid"])
        if have is not None and have != want:
            return False  # the pid (hence the group id) now belongs to an unrelated process
    return True


def _proc_view(rec):
    """A detached run's public state: ``running``, ``exited`` (+``rc``), ``gone``, ``unknown``."""
    here = _hostname()
    same_host = rec.get("host") == here
    out = {
        "pid": rec["pid"],
        "pgid": rec.get("pgid") or rec["pid"],
        "host": rec.get("host"),
        "cmd": rec.get("cmd"),
        "cwd": rec.get("cwd"),
        "log": rec.get("log"),
        "started": rec.get("started"),
    }
    alive = same_host and _group_alive(rec)
    rc = _read_rc(rec.get("rc_path") or "")
    end = time.time()
    if rc is not None:
        out["state"] = "exited"
        out["rc"], out["finished"] = rc
        end = rc[1]
        if alive:
            out["group_alive"] = True
            out["note"] = "the command exited but processes it started are still running"
    elif alive:
        out["state"] = "running"
    elif not same_host:
        out["state"] = "unknown"
        out["note"] = "started on login node %s; this session is on %s" % (rec.get("host"), here)
    else:
        out["state"] = "gone"
        out["note"] = "no exit status was recorded (killed with SIGKILL, or the node rebooted)"
    started = rec.get("started")
    if out["state"] in ("running", "exited") and isinstance(started, (int, float)):
        out["elapsed"] = round(max(0.0, end - started), 1)
    return out


def op_detach(args):
    """Start a command detached from this stub and the ssh connection; return at once.

    The command runs in its own session with stdin from /dev/null and stdout+stderr appended to
    ``log`` (default: a new file in the proc dir). A small bash wrapper records the exit status
    in an rc file, so ``proc_status`` keeps working after the stub or the connection is gone.
    Returns ``{pid, pgid, log, host, started}``; ``pid`` is the wrapper, which leads the group.
    """
    argv_in = args.get("argv")
    cmd = args.get("cmd")
    env = args.get("env")
    if cmd is not None:
        if not isinstance(cmd, str) or not cmd.strip():
            raise StubError("invalid_arg", "cmd must be a non-empty string")
        target = ["bash", "-lc" if args.get("login") else "-c", cmd]
        shown = cmd
    elif argv_in is not None:
        if (
            not isinstance(argv_in, (list, tuple))
            or not argv_in
            or not all(isinstance(a, str) for a in argv_in)
        ):
            raise StubError("invalid_arg", "argv must be a non-empty list of strings")
        if "/" not in argv_in[0] and not (env and "PATH" in env) and not _which(argv_in[0]):
            raise StubError("not_found", "command not found: %s" % argv_in[0], command=argv_in[0])
        target = list(argv_in)
        shown = " ".join(argv_in)
    else:
        raise StubError("invalid_arg", "either argv or cmd is required")
    cwd = args.get("cwd")
    if cwd is not None:
        cwd = _path(cwd, must_exist=True)
        if not os.path.isdir(cwd):
            raise StubError("invalid_arg", "not a directory: %s" % cwd, path=cwd)
    full_env = None
    if env:
        full_env = dict(os.environ)
        for k, v in env.items():
            full_env[str(k)] = str(v)
    d = _proc_dir()
    host = _hostname()
    stem = "%s-%s" % (time.strftime("%Y%m%d-%H%M%S"), codecs.encode(os.urandom(3), "hex").decode())
    rc_path = os.path.join(d, stem + ".rc")
    log = args.get("log")
    if log:
        log = _path(log)
        try:
            os.makedirs(os.path.dirname(log), exist_ok=True)
        except OSError as e:
            raise _os_error(e, os.path.dirname(log))
    else:
        log = os.path.join(d, stem + ".log")
    try:
        logf = io.open(log, "ab")
        log_start = os.fstat(logf.fileno()).st_size  # a reused log's old content ends here
    except OSError as e:
        raise _os_error(e, log)
    try:
        proc = subprocess.Popen(
            ["bash", "-c", _DETACH_WRAPPER, "rs-detach", rc_path] + target,
            cwd=cwd,
            env=full_env,
            stdin=subprocess.DEVNULL,
            stdout=logf,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as e:
        raise StubError("error", "cannot start detached command: %s" % e)
    finally:
        logf.close()
    started = time.time()
    rec = {
        "pid": proc.pid,
        "pgid": proc.pid,
        "host": host,
        "cmd": shown,
        "cwd": cwd or os.getcwd(),
        "log": log,
        "rc_path": rc_path,
        "started": started,
        "starttime": _proc_starttime(proc.pid),
        "log_start": log_start,
    }
    t = threading.Thread(target=_reap_detached, args=(proc, rc_path), name="rs-reap-%d" % proc.pid)
    t.daemon = True
    t.start()
    out = {"pid": proc.pid, "pgid": proc.pid, "log": log, "host": host, "started": started}
    try:
        _write_atomic_text(os.path.join(d, "%s.%d.json" % (host, proc.pid)), json.dumps(rec))
    except OSError as e:
        out["warning"] = "running, but its record could not be saved (%s); proc_* won't find it" % e
    return out


def op_proc_status(args):
    """One detached run's state (``pid``), or the most recent runs (``{procs, count, total}``)."""
    pid = _pid_arg(args, required=False)
    if pid is not None:
        rec = _find_record(pid)
        if rec is None:
            raise _no_record(pid)
        return _proc_view(rec)
    files = _record_files()
    limit = _clamp(args.get("limit"), PROC_LIST_DEFAULT, PROC_LIST_MAX)
    procs = []
    for _m, p in files[:limit]:
        rec = _load_record(p)
        if rec is not None:
            procs.append(_proc_view(rec))
    return {"procs": procs, "count": len(procs), "total": len(files), "host": _hostname()}


def op_proc_tail(args):
    """The last ``lines`` lines of a detached run's log, plus its current state."""
    pid = _pid_arg(args)
    rec = _find_record(pid)
    if rec is None:
        raise _no_record(pid)
    lines = _clamp(args.get("lines"), 50, 100000)
    max_bytes = _clamp(args.get("max_bytes"), DEFAULT_READ_BYTES, MAX_READ_BYTES)
    out = _proc_view(rec)
    log = rec.get("log") or ""
    try:
        with io.open(log, "rb") as f:
            size = os.fstat(f.fileno()).st_size
            data, skipped = _tail_bytes(f, size, lines, max_bytes)
    except OSError as e:
        raise _os_error(e, log)
    out.update({"content": _decode(data), "size": size, "truncated": skipped > 0})
    return out


def op_proc_kill(args):
    """Signal a detached run's whole process group; escalate to SIGKILL after ``grace`` s."""
    pid = _pid_arg(args)
    rec = _find_record(pid)
    if rec is None:
        raise _no_record(pid)
    here = _hostname()
    if rec.get("host") != here:
        raise StubError(
            "invalid_arg",
            "pid %d runs on login node %s; this session is on %s and cannot signal it"
            % (pid, rec.get("host"), here),
            pid=pid,
        )
    name = str(args.get("signal") or "TERM").upper()
    if name.startswith("SIG"):
        name = name[3:]
    sig = _KILL_SIGNALS.get(name)
    if sig is None:
        raise StubError(
            "invalid_arg", "signal must be one of %s" % ", ".join(sorted(_KILL_SIGNALS))
        )
    grace = _clamp(args.get("grace"), PROC_KILL_GRACE, 60, lo=0)
    if not _group_alive(rec):
        out = _proc_view(rec)
        out.update({"killed": False, "reason": "not running"})
        return out
    pgid = rec.get("pgid") or pid
    sent = [name]
    last = sig
    try:
        os.killpg(pgid, sig)
    except OSError:
        pass
    deadline = time.time() + grace
    while time.time() < deadline and _group_alive(rec):
        time.sleep(0.05)
    if sig != signal.SIGKILL and _group_alive(rec):
        try:
            os.killpg(pgid, signal.SIGKILL)
        except OSError:
            pass
        sent.append("KILL")
        last = signal.SIGKILL
        deadline = time.time() + 2
        while time.time() < deadline and _group_alive(rec):
            time.sleep(0.05)
    # The wrapper (or this stub's reaper) records the exit status as it dies; if neither will
    # (a SIGKILLed wrapper started by an earlier stub), record the signal ourselves.
    rc_path = rec.get("rc_path") or ""
    deadline = time.time() + 1.0
    while time.time() < deadline and _read_rc(rc_path) is None:
        time.sleep(0.05)
    if rc_path and _read_rc(rc_path) is None and not _group_alive(rec):
        _write_rc(rc_path, 128 + int(last))
    out = _proc_view(rec)
    out.update({"killed": out["state"] != "running", "signals": sent})
    return out


def op_waitfor(args):
    """Check every ``WAIT_POLL`` s, for up to ``timeout`` s, until a condition holds.

    ``pid`` (a detached run): until it stops running. ``path``: until it exists. ``pattern`` (a
    regex): until a line of ``path`` (default: the ``pid``'s log) matches; with a ``pid`` it also
    stops when the process exits first. Returns ``{done, met, reason, waited, ...}``: ``done`` =
    stop waiting (``reason`` matched | exists | exited), ``met`` = the requested condition held.
    On timeout ``done`` is false and ``offset`` is where the next call should resume scanning.
    ``offset`` defaults to 0 or, for the ``pid``'s own log, to where this run's output began.
    A line without its newline yet matches only once it has stopped growing for a whole check.
    A cancellable LONG op (its registered event ends the wait early).
    """
    cancel_event = args.get("_cancel_event")
    timeout = _clamp(args.get("timeout"), WAIT_DEFAULT, WAIT_MAX)
    pid = _pid_arg(args, required=False)
    path = args.get("path")
    pattern = args.get("pattern")
    rec = None
    own_log = False  # watching the pid's own log rather than a caller-given path
    if pid is not None:
        rec = _find_record(pid)
        if rec is None:
            raise _no_record(pid)
        if path is None and pattern is not None:
            path = rec.get("log")
            own_log = True
        if path is None and _proc_view(rec)["state"] == "unknown":
            raise StubError(
                "invalid_arg",
                "pid %d runs on login node %s; this session is on %s and cannot watch it"
                % (pid, rec.get("host"), _hostname()),
                pid=pid,
            )
    elif path is None:
        raise StubError("invalid_arg", "waitfor needs a pid and/or a path")
    rx = None
    if pattern is not None:
        if not isinstance(pattern, str) or not pattern:
            raise StubError("invalid_arg", "pattern must be a non-empty string")
        try:
            rx = re.compile(pattern)
        except re.error as e:
            raise StubError("invalid_arg", "bad regex %r: %s" % (pattern, e))
    p = _path(path) if path is not None else None
    if rx is not None and os.path.isdir(p):
        raise StubError("invalid_arg", "is a directory: %s" % p, path=p)
    raw_offset = args.get("offset")
    if raw_offset is None:
        # A reused log may hold an old match: by default scan only this run's own output.
        raw_offset = rec.get("log_start", 0) if own_log else 0
    try:
        raw_offset = int(raw_offset)
    except (TypeError, ValueError):
        raise StubError("invalid_arg", "offset must be an integer")
    scan_state = {"pos": None, "carry": b""}  # bytes read so far; trailing partial line

    def scan(final=False):
        """Scan what was appended to ``p`` since the last check; a match dict or None.

        Reads at most ``WAIT_SCAN_BYTES`` per check, or everything up to EOF when ``final``
        (the writer has exited, so the file is finite).
        """
        try:
            f = io.open(p, "rb")
        except OSError:
            return None  # not there yet (or unreadable): keep waiting
        with f:
            size = os.fstat(f.fileno()).st_size
            pos = scan_state["pos"]
            carry = scan_state["carry"]
            if pos is None:
                pos = max(0, size + raw_offset) if raw_offset < 0 else min(raw_offset, size)
            elif size < pos:  # truncated or replaced: start over
                pos, carry = 0, b""
            f.seek(pos)
            start = pos
            budget = None if final else WAIT_SCAN_BYTES
            hit = None
            while (budget is None or budget > 0) and hit is None:
                chunk = f.read(1024 * 1024 if budget is None else min(1024 * 1024, budget))
                if not chunk:
                    break
                if budget is not None:
                    budget -= len(chunk)
                o = pos - len(carry)
                pos += len(chunk)
                lines = (carry + chunk).split(b"\n")
                carry = lines.pop()
                for ln in lines:
                    if rx.search(_decode(ln)):
                        hit = (ln, o, o + len(ln) + 1)
                        break
                    o += len(ln) + 1
                if len(carry) > WAIT_CARRY_MAX:
                    carry = carry[-WAIT_CARRY_MAX:]
            # A line without its newline yet (e.g. a prompt) counts only once nothing was
            # appended for a whole check, or the writer has exited, so a `$`-anchored pattern
            # can't fire on half a line.
            settled = final or pos == start
            if hit is None and carry and settled and rx.search(_decode(carry)):
                hit = (carry, pos - len(carry), pos)
            if hit is not None:
                scan_state["pos"], scan_state["carry"] = hit[2], b""
                return {"line": _decode(hit[0])[:DIFF_MAX_LINE], "line_offset": hit[1]}
            scan_state["pos"], scan_state["carry"] = pos, carry
            return None

    t0 = time.time()

    def result(done, met, reason, **extra):
        out = {"done": done, "met": met, "reason": reason, "waited": round(time.time() - t0, 1)}
        if p is not None:
            out["path"] = p
        if rx is not None and scan_state["pos"] is not None:
            out["offset"] = scan_state["pos"] - len(scan_state["carry"])
        if rec is not None:
            out["process"] = _proc_view(rec)
        out.update(extra)
        return out

    def check_path(final=False):
        if p is None:
            return None
        if rx is not None:
            hit = scan(final)
            return None if hit is None else result(True, True, "matched", **hit)
        return result(True, True, "exists") if os.path.lexists(p) else None

    while True:
        found = check_path()
        if found is not None:
            return found
        # Re-read every check: a run on another login node reports `unknown` until its rc file
        # shows up on the shared home.
        if rec is not None and _proc_view(rec)["state"] in ("exited", "gone"):
            found = check_path(final=True)  # to EOF: output written just before the exit counts
            return found if found is not None else result(True, p is None, "exited")
        waited = time.time() - t0
        if waited >= timeout:
            return result(False, False, "timeout")
        pause = min(WAIT_POLL, timeout - waited)
        if cancel_event is not None:
            if cancel_event.wait(pause):
                return result(False, False, "cancelled")
        else:
            time.sleep(pause)


def _slurm(argv, timeout=60, cwd=None, stdin=None, on_spawn=None, new_session=False):
    if not _which(argv[0]):
        raise StubError(
            "slurm_error", "%s not found on PATH (is this a Slurm login node?)" % argv[0]
        )
    return _run(
        argv,
        timeout=timeout,
        cwd=cwd,
        stdin=stdin,
        max_output=MAX_RUN_OUTPUT,
        on_spawn=on_spawn,
        new_session=new_session,
    )


def _slurm_soft(argv, timeout=60, cwd=None):
    """Run a query tool but, when it is *absent* from PATH, return a soft ``rc != 0`` result
    instead of raising.

    The queue-intelligence tools (``sshare``/``sacctmgr``/``diskusage_report``) are optional and
    site-specific; a missing one must degrade to ``available: false`` on the client, never a hard
    error. A tool that *is* present but exits non-zero simply returns its rc/stderr.
    """
    if not _which(argv[0]):
        return {
            "rc": 127,
            "stdout": "",
            "stderr": "%s: not found" % argv[0],
            "stdout_truncated": False,
            "stderr_truncated": False,
            "missing": True,
        }
    return _run(argv, timeout=timeout, cwd=cwd, max_output=MAX_RUN_OUTPUT)


def op_sbatch(args):
    """Submit a job. Either `script` (content) or `path` (existing file) must be given.

    Returns the raw sbatch result plus the script path; the client parses the job id.
    """
    script = args.get("script")
    path = args.get("path")
    extra = args.get("args") or []
    cwd = args.get("cwd")
    if cwd is not None:
        cwd = _path(cwd, must_exist=True)
    if not isinstance(extra, list) or not all(isinstance(a, str) for a in extra):
        raise StubError("invalid_arg", "args must be a list of strings")
    if script is not None:
        if not isinstance(script, str) or not script.strip():
            raise StubError("invalid_arg", "script content is empty")
        if path is None:
            name = args.get("name") or "job"
            name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)[:64]
            jobdir = _path(
                args.get("script_dir")
                or os.path.join(os.path.expanduser("~"), ".remoteslurm", "scripts")
            )
            os.makedirs(jobdir, exist_ok=True)
            path = os.path.join(
                jobdir, "%s-%s-%d.sh" % (name, time.strftime("%Y%m%d-%H%M%S"), os.getpid())
            )
        else:
            path = _path(path)
        if not script.endswith("\n"):
            script += "\n"
        op_write({"path": path, "content": script, "mkdirs": True, "mode": 0o700})
    elif path is not None:
        path = _path(path, must_exist=True)
    else:
        raise StubError("invalid_arg", "either script or path is required")
    argv = ["sbatch", "--parsable"] + extra + [path]
    res = _slurm(
        argv,
        timeout=120,
        cwd=cwd or os.path.expanduser("~"),
        on_spawn=args.get("_register"),
        new_session=True,
    )
    res["script_path"] = path
    res["argv"] = argv
    return res


TASK_SCHEMA = 1
TASK_LOCK_WAIT = 10.0
TASK_LOCK_STALE = 180.0
TASK_STATES = frozenset(
    (
        "PREPARED",
        "SUBMITTING",
        "ACCEPTED",
        "PENDING",
        "RUNNING",
        "COMPLETED",
        "VERIFIED",
        "INVALID",
        "FAILED",
        "REJECTED",
        "UNKNOWN",
    )
)


def _task_location(args, task_id):
    if not isinstance(task_id, str) or not re.match(r"^[0-9a-f]{64}$", task_id):
        raise StubError("invalid_arg", "task_id must be a 64-character lowercase sha256")
    root = _path(args.get("task_dir") or "~/.remoteslurm/tasks")
    return os.path.join(root, task_id[:2], task_id)


def _task_read(path):
    try:
        with io.open(path, "r", encoding="utf-8") as f:
            value = json.load(f)
    except OSError:
        return None
    except ValueError as e:
        raise StubError("error", "invalid durable task record %s: %s" % (path, e), path=path)
    if not isinstance(value, dict):
        raise StubError("error", "durable task record is not an object: %s" % path, path=path)
    return value


def _task_write(path, value):
    value["updated_at"] = time.time()
    data = json.dumps(value, indent=1, sort_keys=True, ensure_ascii=False).encode("utf-8")
    _atomic_write(path, data, keep_mode=False, mode=0o600)


def _task_lock_owner_dead(lockdir):
    try:
        with io.open(os.path.join(lockdir, "owner.json"), "r", encoding="utf-8") as f:
            owner = json.load(f)
    except (OSError, ValueError):
        owner = {}
    if owner.get("hostname") == socket.gethostname():
        try:
            os.kill(int(owner.get("pid")), 0)
        except (OSError, TypeError, ValueError):
            return True
    try:
        return time.time() - os.path.getmtime(lockdir) > TASK_LOCK_STALE
    except OSError:
        return False


def _task_break_stale_lock(lockdir):
    stale = "%s.stale.%d.%s" % (lockdir, os.getpid(), uuid.uuid4().hex[:8])
    try:
        os.rename(lockdir, stale)
    except OSError:
        return False
    shutil.rmtree(stale, ignore_errors=True)
    return True


@contextmanager
def _task_lock(taskdir):
    os.makedirs(taskdir, exist_ok=True)
    lockdir = taskdir + ".lock"
    deadline = time.time() + TASK_LOCK_WAIT
    while True:
        try:
            os.mkdir(lockdir, 0o700)
            owner = {
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "created_at": time.time(),
            }
            _atomic_write(
                os.path.join(lockdir, "owner.json"),
                json.dumps(owner, sort_keys=True).encode("utf-8"),
                keep_mode=False,
                mode=0o600,
            )
            break
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise _os_error(e, lockdir)
            if _task_lock_owner_dead(lockdir) and _task_break_stale_lock(lockdir):
                continue
            if time.time() >= deadline:
                raise StubError(
                    "task_busy",
                    "another ensure request holds the durable task lease",
                    task_dir=taskdir,
                    action="retry ensure; a stale lease is recovered after %d seconds"
                    % TASK_LOCK_STALE,
                )
            time.sleep(0.1)
    try:
        yield
    finally:
        try:
            os.remove(os.path.join(lockdir, "owner.json"))
        except OSError:
            pass
        try:
            os.rmdir(lockdir)
        except OSError:
            pass


def _fingerprint_file(raw, required=True, hash_content=True, base=None):
    expanded = os.path.expanduser(os.path.expandvars(raw))
    if not os.path.isabs(expanded) and base:
        expanded = os.path.join(base, expanded)
    p = _path(expanded)
    if not os.path.lexists(p):
        if required:
            raise StubError("not_found", "no such declared file: %s" % p, path=p)
        return {"path": p, "exists": False}
    if not os.path.isfile(p):
        raise StubError(
            "invalid_arg",
            "durable task fingerprints support regular files only: %s" % p,
            path=p,
        )
    st = os.stat(p)
    out = {"path": p, "exists": True, "size": st.st_size}
    if hash_content:
        digest = hashlib.sha256()
        try:
            with io.open(p, "rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
        except OSError as e:
            raise _os_error(e, p)
        out["sha256"] = digest.hexdigest()
    return out


def op_task_fingerprint(args):
    paths = args.get("paths")
    if not isinstance(paths, list) or not all(isinstance(p, str) and p for p in paths):
        raise StubError("invalid_arg", "paths must be a list of non-empty strings")
    if len(paths) > 1024:
        raise StubError("invalid_arg", "at most 1024 files may be fingerprinted per request")
    required = bool(args.get("required", True))
    hash_content = bool(args.get("hash", True))
    base = args.get("base")
    if base is not None:
        base = _path(base, must_exist=True)
    return {
        "files": [
            _fingerprint_file(p, required=required, hash_content=hash_content, base=base)
            for p in paths
        ]
    }


def _attempt_find_jobs(marker, since):
    """Find an attempt marker in live queue and accounting without relying on client state."""
    found = {}

    def parent_job_id(value):
        """Collapse array-task and step identifiers to one scheduler submission."""
        value = value.split(".", 1)[0]
        match = re.match(r"^(\d+)(?:_.+)?$", value)
        return match.group(1) if match else value

    user = getpass.getuser()
    sq = _slurm_soft(["squeue", "-h", "-u", user, "-o", "%i|%j|%T"], timeout=30)
    if sq.get("rc") == 0:
        for line in sq.get("stdout", "").splitlines():
            fields = line.split("|")
            if len(fields) >= 3 and fields[1] == marker:
                jid = parent_job_id(fields[0])
                found[jid] = {"job_id": jid, "state": fields[2], "source": "squeue"}
    start = time.strftime(
        "%Y-%m-%dT%H:%M:%S", time.localtime(max(0, float(since or time.time()) - 86400))
    )
    sa = _slurm_soft(
        [
            "sacct",
            "-X",
            "-n",
            "-P",
            "-u",
            user,
            "-S",
            start,
            "-o",
            "JobID,JobName,State",
        ],
        timeout=30,
    )
    if sa.get("rc") == 0:
        for line in sa.get("stdout", "").splitlines():
            fields = line.split("|")
            if len(fields) >= 3 and fields[1] == marker:
                jid = parent_job_id(fields[0])
                if jid not in found:
                    found[jid] = {"job_id": jid, "state": fields[2], "source": "sacct"}
    return list(found.values())


def _attempt_current(record):
    current = record.get("current_attempt")
    for attempt in reversed(record.get("attempts") or []):
        if attempt.get("attempt_id") == current:
            return attempt
    return None


def _attempt_reconcile_submitting(record, current, write_record):
    """Resolve one ambiguous marked submission for every durable attempt owner."""
    if not current or current.get("state") != "SUBMITTING" or current.get("job_id"):
        return
    matches = _attempt_find_jobs(current.get("marker"), current.get("created_at"))
    if len(matches) == 1:
        current["job_id"] = matches[0]["job_id"]
        current["state"] = "ACCEPTED"
        current["recovered"] = True
        current["recovered_from"] = matches[0]["source"]
        record["state"] = "ACCEPTED"
    else:
        current["state"] = "UNKNOWN"
        current["reconciliation_matches"] = matches
        record["state"] = "UNKNOWN"
        record["reason"] = (
            "submission may have reached Slurm but no unique scheduler record was found"
            if not matches
            else "multiple scheduler jobs carry the same durable attempt marker"
        )
    write_record(record)


def op_task_ensure(args):
    """Persist intent remotely, recover an attempt, or submit exactly one marked batch job."""
    task_id = args.get("task_id")
    taskdir = _task_location(args, task_id)
    record_path = os.path.join(taskdir, "task.json")
    contract = args.get("contract")
    script = args.get("script")
    flags = args.get("args") or []
    cwd = args.get("cwd")
    attempt_id = args.get("attempt_id")
    if not isinstance(contract, dict):
        raise StubError("invalid_arg", "contract must be an object")
    canonical = json.dumps(contract, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    if hashlib.sha256(canonical.encode("utf-8")).hexdigest() != task_id:
        raise StubError("invalid_arg", "task_id does not match the canonical contract")
    if not isinstance(script, str) or not script.strip():
        raise StubError("invalid_arg", "script must be non-empty content")
    if hashlib.sha256(script.encode("utf-8")).hexdigest() != contract.get("script_sha256"):
        raise StubError("invalid_arg", "script content does not match contract script_sha256")
    if not isinstance(flags, list) or not all(isinstance(v, str) for v in flags):
        raise StubError("invalid_arg", "args must be a list of strings")
    if cwd is not None:
        cwd = _path(cwd, must_exist=True)
    if not isinstance(attempt_id, str) or not re.match(r"^[0-9a-f]{32}$", attempt_id):
        raise StubError("invalid_arg", "attempt_id must be a 32-character lowercase hex id")

    with _task_lock(taskdir):
        record = _task_read(record_path)
        if record is None:
            record = {
                "schema": TASK_SCHEMA,
                "task_id": task_id,
                "name": args.get("name"),
                "contract": contract,
                "created_at": time.time(),
                "state": "PREPARED",
                "attempts": [],
                "receipt_history": [],
            }
            _task_write(record_path, record)
        elif record.get("contract") != contract:
            raise StubError(
                "error",
                "durable task hash collision or incompatible canonicalization",
                task_id=task_id,
            )

        current = _attempt_current(record)
        _attempt_reconcile_submitting(
            record, current, lambda value: _task_write(record_path, value)
        )

        state = record.get("state")
        current = _attempt_current(record)
        retry = bool(args.get("retry"))
        retry_unknown = bool(args.get("retry_unknown"))
        if current is not None:
            if state == "UNKNOWN" and not retry_unknown:
                return {"record": record, "task_dir": taskdir, "submitted": False}
            if state in ("FAILED", "INVALID", "REJECTED") and not retry:
                return {"record": record, "task_dir": taskdir, "submitted": False}
            if state not in ("FAILED", "INVALID", "REJECTED", "UNKNOWN"):
                return {"record": record, "task_dir": taskdir, "submitted": False}

        if record.get("receipt") is not None:
            record.setdefault("receipt_history", []).append(record["receipt"])
            record.pop("receipt", None)
        record.pop("reason", None)
        marker = "rse-%s-%s" % (task_id[:10], attempt_id[:10])
        script_path = os.path.join(taskdir, "attempt-%s.sh" % attempt_id)
        if not script.endswith("\n"):
            script += "\n"
        _atomic_write(script_path, script.encode("utf-8"), keep_mode=False, mode=0o700)
        attempt = {
            "attempt_id": attempt_id,
            "marker": marker,
            "state": "SUBMITTING",
            "created_at": time.time(),
            "script_path": script_path,
            "control": args.get("control") or {},
        }
        record.setdefault("attempts", []).append(attempt)
        record["current_attempt"] = attempt_id
        record["state"] = "SUBMITTING"
        _task_write(record_path, record)

        argv = ["sbatch", "--parsable", "--job-name=" + marker] + flags + [script_path]
        result = _slurm(
            argv,
            timeout=120,
            cwd=cwd or os.path.expanduser("~"),
            on_spawn=args.get("_register"),
            new_session=True,
        )
        attempt["sbatch_rc"] = result.get("rc")
        attempt["sbatch_stderr"] = (result.get("stderr") or "")[-4000:]
        if result.get("rc") != 0:
            attempt["state"] = "REJECTED"
            record["state"] = "REJECTED"
            record["reason"] = attempt["sbatch_stderr"] or "sbatch rejected the submission"
            _task_write(record_path, record)
            return {"record": record, "task_dir": taskdir, "submitted": False}
        match = re.match(r"^\s*(\d+)", result.get("stdout") or "")
        if not match:
            attempt["state"] = "UNKNOWN"
            record["state"] = "UNKNOWN"
            record["reason"] = "sbatch returned success without a parseable job id"
            _task_write(record_path, record)
            return {"record": record, "task_dir": taskdir, "submitted": False}
        if os.environ.get("REMOTESLURM_TEST_LOSE_AFTER_ACCEPT") == "1":
            # Fault injection models a lost response after scheduler acceptance. The real
            # client-disconnect path lets this worker finish and release its lease; remove it
            # explicitly before the hard process exit so the test exercises reconciliation.
            lockdir = taskdir + ".lock"
            try:
                os.remove(os.path.join(lockdir, "owner.json"))
                os.rmdir(lockdir)
            except OSError:
                pass
            os._exit(91)
        attempt["job_id"] = match.group(1)
        attempt["state"] = "ACCEPTED"
        attempt["accepted_at"] = time.time()
        record["state"] = "ACCEPTED"
        _task_write(record_path, record)
        return {"record": record, "task_dir": taskdir, "submitted": True}


def op_task_update(args):
    task_id = args.get("task_id")
    taskdir = _task_location(args, task_id)
    record_path = os.path.join(taskdir, "task.json")
    state = args.get("state")
    if state not in TASK_STATES:
        raise StubError("invalid_arg", "invalid durable task state: %r" % state)
    with _task_lock(taskdir):
        record = _task_read(record_path)
        if record is None:
            raise StubError("not_found", "durable task record does not exist", task_id=task_id)
        attempt = _attempt_current(record)
        if attempt is None or attempt.get("attempt_id") != args.get("attempt_id"):
            raise StubError("invalid_arg", "attempt is not the task's current attempt")
        progress = {"SUBMITTING": 0, "ACCEPTED": 1, "PENDING": 2, "RUNNING": 3, "COMPLETED": 4}
        previous = attempt.get("state")
        if state in progress and (
            previous not in progress or progress[state] < progress.get(previous, -1)
        ):
            return {"record": record, "task_dir": taskdir}
        if state == "UNKNOWN" and previous in ("COMPLETED", "VERIFIED", "INVALID", "FAILED"):
            return {"record": record, "task_dir": taskdir}
        attempt["state"] = state
        if isinstance(args.get("scheduler"), dict):
            attempt["scheduler"] = args["scheduler"]
        record["state"] = state
        if isinstance(args.get("validation"), dict):
            record["last_validation"] = args["validation"]
        if isinstance(args.get("receipt"), dict):
            record["receipt"] = args["receipt"]
        if args.get("reason"):
            record["reason"] = str(args["reason"])
        elif state in ("ACCEPTED", "PENDING", "RUNNING", "COMPLETED", "VERIFIED"):
            record.pop("reason", None)
        _task_write(record_path, record)
        return {"record": record, "task_dir": taskdir}


# --------------------------------------------------------------------------- campaign store


def _campaign_name(value, label):
    if not isinstance(value, str) or not re.match(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$", value):
        raise StubError("invalid_arg", "%s is not a safe campaign identifier" % label)
    return value


def _campaign_id(value, label):
    if not isinstance(value, str) or not re.match(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$", value):
        raise StubError("invalid_arg", "%s is not a safe store identifier" % label)
    return value


def _campaign_root(args):
    return _path(args.get("campaign_dir") or "~/.remoteslurm/campaigns")


def _campaign_base(args):
    return os.path.join(_campaign_root(args), _campaign_name(args.get("name"), "name"))


def _campaign_run(args):
    return os.path.join(_campaign_base(args), "runs", _campaign_id(args.get("run_id"), "run_id"))


def _campaign_json_bytes(value):
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError) as e:
        raise StubError("invalid_arg", "value is not canonical JSON: %s" % e)


def _campaign_read_json(path, required=True, max_bytes=CAMPAIGN_MAX_READ_BYTES):
    try:
        size = os.path.getsize(path)
        if size > max_bytes:
            raise StubError(
                "too_large", "campaign document exceeds read limit", path=path, size=size
            )
        with io.open(path, "r", encoding="utf-8") as f:
            value = json.load(f)
    except OSError as e:
        if not required and e.errno == errno.ENOENT:
            return None
        raise _os_error(e, path)
    except ValueError as e:
        raise StubError("error", "invalid campaign JSON %s: %s" % (path, e), path=path)
    return value


def _campaign_write_json(path, value):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    _atomic_write(path, _campaign_json_bytes(value), keep_mode=False, mode=0o600)


def _campaign_document_path(value):
    if not isinstance(value, str) or not value or "\x00" in value:
        raise StubError("invalid_arg", "campaign document path must be non-empty text")
    normalized = value.replace("\\", "/")
    parts = normalized.split("/")
    if any(not part or part in (".", "..") for part in parts):
        raise StubError("invalid_arg", "unsafe campaign document path: %r" % value)
    return os.path.join(*parts)


@contextmanager
def _campaign_lock(path):
    os.makedirs(path, exist_ok=True)
    lockdir = os.path.join(path, ".lock")
    deadline = time.time() + TASK_LOCK_WAIT
    while True:
        try:
            os.mkdir(lockdir, 0o700)
            owner = {
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "created_at": time.time(),
            }
            _atomic_write(
                os.path.join(lockdir, "owner.json"),
                _campaign_json_bytes(owner),
                keep_mode=False,
                mode=0o600,
            )
            break
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise _os_error(e, lockdir)
            if _task_lock_owner_dead(lockdir) and _task_break_stale_lock(lockdir):
                continue
            if time.time() >= deadline:
                raise StubError(
                    "store_conflict",
                    "another writer holds the campaign store lease",
                    action="retry the transaction with the same transaction_id",
                )
            time.sleep(0.1)
    try:
        yield
    finally:
        try:
            os.remove(os.path.join(lockdir, "owner.json"))
        except OSError:
            pass
        try:
            os.rmdir(lockdir)
        except OSError:
            pass


CAMPAIGN_ATTEMPT_STATES = frozenset(
    (
        "INTENDED",
        "SUBMITTING",
        "ACCEPTED",
        "PENDING",
        "RUNNING",
        "COMPLETED",
        "FAILED",
        "INVALID",
        "REJECTED",
        "CANCELLED",
        "UNKNOWN",
    )
)


def _campaign_attempt_dir(args):
    group_id = args.get("group_id")
    if not isinstance(group_id, str) or not re.match(r"^[0-9a-f]{64}$", group_id):
        raise StubError("invalid_arg", "group_id must be a 64-character lowercase sha256")
    return os.path.join(_campaign_run(args), "attempts", group_id), group_id


def _campaign_attempt_fault(attempt_dir, step):
    if os.environ.get("REMOTESLURM_TEST_CAMPAIGN_SUBMIT_FAIL_STEP") != step:
        return
    run_dir = os.path.dirname(os.path.dirname(attempt_dir))
    for lockdir in (
        os.path.join(attempt_dir, ".lock"),
        os.path.join(run_dir, "execution", ".lock"),
    ):
        try:
            os.remove(os.path.join(lockdir, "owner.json"))
            os.rmdir(lockdir)
        except OSError:
            pass
    os._exit(92)


def _campaign_execution_paths(args):
    execution_dir = os.path.join(_campaign_run(args), "execution")
    return execution_dir, os.path.join(execution_dir, "ledger.json")


def _campaign_attempt_path(args, group_id):
    return os.path.join(_campaign_run(args), "attempts", group_id, "attempt.json")


def _campaign_contract_units(contract):
    mapping = contract.get("mapping") if isinstance(contract, dict) else None
    if not isinstance(mapping, list) or not mapping:
        raise StubError("invalid_arg", "campaign group contract requires a non-empty mapping")
    unit_ids = []
    for item in mapping:
        unit_id = item.get("unit_id") if isinstance(item, dict) else None
        if not isinstance(unit_id, str) or not unit_id:
            raise StubError("invalid_arg", "campaign group mapping has an invalid unit_id")
        unit_ids.append(unit_id)
    if len(set(unit_ids)) != len(unit_ids):
        raise StubError("invalid_arg", "campaign group mapping repeats a unit_id")
    return unit_ids


def _campaign_execution_ledger(args, ledger_path):
    """Load the run ledger, importing pre-ledger group records once."""
    ledger = _campaign_read_json(
        ledger_path, required=False, max_bytes=CAMPAIGN_MAX_TRANSACTION_BYTES
    )
    if ledger is not None:
        if ledger.get("schema") != 1:
            raise StubError("store_conflict", "unsupported campaign execution ledger schema")
        ledger.setdefault("groups", {})
        ledger.setdefault("units", {})
        ledger.setdefault("consumed_authorizations", {})
        return ledger

    ledger = {
        "schema": 1,
        "created_at": time.time(),
        "groups": {},
        "units": {},
        "consumed_authorizations": {},
    }
    attempts_dir = os.path.join(_campaign_run(args), "attempts")
    try:
        names = sorted(os.listdir(attempts_dir))
    except OSError as e:
        if e.errno == errno.ENOENT:
            names = []
        else:
            raise _os_error(e, attempts_dir)
    if len(names) > 10000:
        raise StubError(
            "too_large",
            "campaign has too many attempt groups to initialize the execution ledger",
            count=len(names),
        )
    for group_id in names:
        if not re.match(r"^[0-9a-f]{64}$", group_id):
            continue
        record = _campaign_read_json(
            os.path.join(attempts_dir, group_id, "attempt.json"), required=False
        )
        if not isinstance(record, dict) or record.get("group_id") != group_id:
            continue
        ledger["groups"][group_id] = record
        current = _attempt_current(record)
        if current is None:
            continue
        for unit_id in _campaign_contract_units(record.get("contract")):
            old = ledger["units"].get(unit_id)
            old_record = ledger["groups"].get(old.get("group_id")) if old else None
            old_attempt = _attempt_current(old_record or {})
            if old_attempt is None or float(current.get("created_at", 0)) >= float(
                old_attempt.get("created_at", 0)
            ):
                ledger["units"][unit_id] = {
                    "group_id": group_id,
                    "attempt_id": current.get("attempt_id"),
                }
    return ledger


def _campaign_write_execution(args, ledger_path, ledger, group_ids):
    """Commit canonical run state, then refresh inspectable group mirrors."""
    ledger["updated_at"] = time.time()
    data = _campaign_json_bytes(ledger)
    if len(data) > CAMPAIGN_MAX_TRANSACTION_BYTES:
        raise StubError(
            "too_large",
            "campaign execution ledger exceeds its durable write limit",
            size=len(data),
            limit=CAMPAIGN_MAX_TRANSACTION_BYTES,
        )
    os.makedirs(os.path.dirname(ledger_path), exist_ok=True)
    _atomic_write(ledger_path, data, keep_mode=False, mode=0o600)
    for group_id in group_ids:
        record = ledger.get("groups", {}).get(group_id)
        if record is not None:
            _campaign_write_json(_campaign_attempt_path(args, group_id), record)


def _campaign_group_value(group_id, record):
    value = {"group_id": group_id}
    value.update(dict(record.get("contract") or {}))
    return value


def _campaign_drive_attempt(args, ledger_path, ledger, group_id):
    """Reconcile or perform the one scheduler mutation journaled for a group."""
    record = ledger["groups"][group_id]

    def write_record(value):
        ledger["groups"][group_id] = value
        _campaign_write_execution(args, ledger_path, ledger, (group_id,))

    current = _attempt_current(record)
    _attempt_reconcile_submitting(record, current, write_record)
    current = _attempt_current(record)
    if current is None or current.get("state") != "INTENDED":
        return {
            "group": _campaign_group_value(group_id, record),
            "record": record,
            "submitted": False,
        }

    current["state"] = "SUBMITTING"
    record["state"] = "SUBMITTING"
    write_record(record)
    attempt_dir = os.path.dirname(_campaign_attempt_path(args, group_id))
    _campaign_attempt_fault(attempt_dir, "submitting")
    argv = ["sbatch", "--parsable", "--job-name=" + current["marker"]]
    argv += list(current.get("args") or []) + [current["script_path"]]
    result = _slurm(
        argv,
        timeout=120,
        cwd=current.get("cwd") or os.path.expanduser("~"),
        on_spawn=args.get("_register"),
        new_session=True,
    )
    current["sbatch_rc"] = result.get("rc")
    current["sbatch_stderr"] = (result.get("stderr") or "")[-4000:]
    if result.get("rc") != 0:
        current["state"] = "REJECTED"
        record["state"] = "REJECTED"
        record["reason"] = current["sbatch_stderr"] or "sbatch rejected the submission"
        write_record(record)
        return {
            "group": _campaign_group_value(group_id, record),
            "record": record,
            "submitted": False,
        }
    match = re.match(r"^\s*(\d+)", result.get("stdout") or "")
    if not match:
        current["state"] = "UNKNOWN"
        record["state"] = "UNKNOWN"
        record["reason"] = "sbatch returned success without a parseable job id"
        write_record(record)
        return {
            "group": _campaign_group_value(group_id, record),
            "record": record,
            "submitted": False,
        }
    _campaign_attempt_fault(attempt_dir, "accepted")
    current["job_id"] = match.group(1)
    current["state"] = "ACCEPTED"
    current["accepted_at"] = time.time()
    record["state"] = "ACCEPTED"
    write_record(record)
    return {
        "group": _campaign_group_value(group_id, record),
        "record": record,
        "submitted": True,
    }


def op_campaign_attempt_ensure(args):
    """Reserve units, consume retries, and recover or submit under one run lease."""
    attempt_dir, group_id = _campaign_attempt_dir(args)
    contract = args.get("contract")
    script = args.get("script")
    flags = args.get("args") or []
    cwd = args.get("cwd")
    requested_attempt_id = args.get("attempt_id")
    if not isinstance(contract, dict):
        raise StubError("invalid_arg", "contract must be an object")
    if hashlib.sha256(_campaign_json_bytes(contract)).hexdigest() != group_id:
        raise StubError("invalid_arg", "group_id does not match the canonical contract")
    if not isinstance(script, str) or not script.strip():
        raise StubError("invalid_arg", "script must be non-empty content")
    script_sha = hashlib.sha256(script.encode("utf-8")).hexdigest()
    if args.get("script_sha256") != script_sha:
        raise StubError("invalid_arg", "script bytes do not match script_sha256")
    if not isinstance(flags, list) or not all(isinstance(value, str) for value in flags):
        raise StubError("invalid_arg", "args must be a list of strings")
    if cwd is not None:
        cwd = _path(cwd, must_exist=True)
    if not isinstance(requested_attempt_id, str) or not re.match(
        r"^[0-9a-f]{32}$", requested_attempt_id
    ):
        raise StubError("invalid_arg", "attempt_id must be a 32-character lowercase hex id")
    unit_ids = _campaign_contract_units(contract)
    claims = args.get("retry_claims") or []
    if not isinstance(claims, list) or not all(isinstance(value, dict) for value in claims):
        raise StubError("invalid_arg", "retry_claims must be a list of objects")
    claims_by_unit = {}
    for claim in claims:
        unit_id = claim.get("unit_id")
        authorization_id = claim.get("authorization_id")
        if unit_id not in unit_ids or unit_id in claims_by_unit:
            raise StubError("invalid_arg", "retry_claims do not uniquely match group units")
        if not isinstance(authorization_id, str) or not re.match(
            r"^[0-9a-f]{32}$", authorization_id
        ):
            raise StubError("invalid_arg", "retry authorization_id must be a lowercase hex id")
        claims_by_unit[unit_id] = claim

    execution_dir, ledger_path = _campaign_execution_paths(args)
    with _campaign_lock(execution_dir):
        ledger = _campaign_execution_ledger(args, ledger_path)
        reservations = ledger["units"]
        consumed = ledger["consumed_authorizations"]

        replacements = set()
        consumed_claims = False
        for unit_id, claim in claims_by_unit.items():
            use = consumed.get(claim["authorization_id"] + ":" + unit_id)
            if use is not None:
                consumed_claims = True
                replacements.add(use.get("replacement_group_id"))
        if consumed_claims:
            if None in replacements or not replacements:
                raise StubError(
                    "store_conflict", "retry authorization has incomplete durable state"
                )
            outcomes = [
                _campaign_drive_attempt(args, ledger_path, ledger, existing_group)
                for existing_group in sorted(replacements)
            ]
            return {
                "outcomes": outcomes,
                "record": outcomes[0]["record"],
                "group": outcomes[0]["group"],
                "attempt_dir": os.path.dirname(
                    _campaign_attempt_path(args, outcomes[0]["group"]["group_id"])
                ),
                "submitted": any(item["submitted"] for item in outcomes),
            }

        # An unclaimed reservation is an outstanding exact intent. Recover its whole group
        # instead of allowing the caller's current selection to create a new group identity.
        blocking_groups = set()
        for unit_id in unit_ids:
            reservation = reservations.get(unit_id)
            claim = claims_by_unit.get(unit_id)
            if reservation is None:
                # Adopted and output-only work has no managed group reservation. Its explicit
                # retry authorization still becomes durable here before the first managed attempt.
                if claim is not None and claim.get("expected_group_id") is not None:
                    raise StubError(
                        "store_conflict",
                        "retry claim has no matching durable unit reservation",
                        unit_id=unit_id,
                    )
                continue
            if claim is None:
                blocking_groups.add(reservation.get("group_id"))
                continue
            if reservation.get("group_id") != claim.get("expected_group_id") or reservation.get(
                "attempt_id"
            ) != claim.get("expected_attempt_id"):
                raise StubError(
                    "store_conflict",
                    "retry claim no longer matches the unit's current durable attempt",
                    unit_id=unit_id,
                )
        if blocking_groups:
            if None in blocking_groups:
                raise StubError("store_conflict", "unit reservation has no group identity")
            outcomes = [
                _campaign_drive_attempt(args, ledger_path, ledger, existing_group)
                for existing_group in sorted(blocking_groups)
            ]
            return {
                "outcomes": outcomes,
                "record": outcomes[0]["record"],
                "group": outcomes[0]["group"],
                "attempt_dir": os.path.dirname(
                    _campaign_attempt_path(args, outcomes[0]["group"]["group_id"])
                ),
                "submitted": any(item["submitted"] for item in outcomes),
            }

        # Orphaned script bytes cannot mutate the scheduler. The ledger commit below records
        # the intent, unit reservations, and retry consumption in one atomic replacement.
        if not script.endswith("\n"):
            script += "\n"
        script_path = os.path.join(attempt_dir, "attempt-%s.sh" % requested_attempt_id)
        os.makedirs(attempt_dir, exist_ok=True)
        _atomic_write(script_path, script.encode("utf-8"), keep_mode=False, mode=0o700)
        record = ledger["groups"].get(group_id)
        if record is None:
            record = {
                "schema": 1,
                "group_id": group_id,
                "contract": contract,
                "created_at": time.time(),
                "state": "INTENDED",
                "attempts": [],
            }
            ledger["groups"][group_id] = record
        elif record.get("contract") != contract:
            raise StubError("store_conflict", "campaign group contract differs from durable record")
        current = {
            "attempt_id": requested_attempt_id,
            "marker": "rsc-%s-%s" % (group_id[:10], requested_attempt_id[:10]),
            "state": "INTENDED",
            "created_at": time.time(),
            "script_path": script_path,
            "script_sha256": hashlib.sha256(script.encode("utf-8")).hexdigest(),
            "args": flags,
            "cwd": cwd,
            "control": args.get("control") or {},
        }
        record.setdefault("attempts", []).append(current)
        record["current_attempt"] = requested_attempt_id
        record["state"] = "INTENDED"
        record.pop("reason", None)
        for unit_id in unit_ids:
            reservations[unit_id] = {
                "group_id": group_id,
                "attempt_id": requested_attempt_id,
            }
            claim = claims_by_unit.get(unit_id)
            if claim is not None:
                consumed[claim["authorization_id"] + ":" + unit_id] = {
                    "authorization_id": claim["authorization_id"],
                    "unit_id": unit_id,
                    "previous_group_id": claim.get("expected_group_id"),
                    "previous_attempt_id": claim.get("expected_attempt_id"),
                    "replacement_group_id": group_id,
                    "replacement_attempt_id": requested_attempt_id,
                    "consumed_at": time.time(),
                }
        _campaign_write_execution(args, ledger_path, ledger, (group_id,))
        _campaign_attempt_fault(attempt_dir, "intent")
        outcome = _campaign_drive_attempt(args, ledger_path, ledger, group_id)
        return {
            "outcomes": [outcome],
            "record": outcome["record"],
            "group": outcome["group"],
            "attempt_dir": attempt_dir,
            "submitted": outcome["submitted"],
        }


def op_campaign_attempt_update(args):
    attempt_dir, group_id = _campaign_attempt_dir(args)
    state = args.get("state")
    if state not in CAMPAIGN_ATTEMPT_STATES:
        raise StubError("invalid_arg", "invalid campaign attempt state: %r" % state)
    execution_dir, ledger_path = _campaign_execution_paths(args)
    with _campaign_lock(execution_dir):
        ledger = _campaign_execution_ledger(args, ledger_path)
        record = ledger.get("groups", {}).get(group_id)
        if record is None:
            raise StubError("not_found", "campaign attempt group does not exist")
        if record.get("group_id") != group_id:
            raise StubError("store_conflict", "campaign attempt group identity differs")
        current = _attempt_current(record)
        if current is None:
            raise StubError("not_found", "campaign attempt record has no current attempt")
        if current.get("attempt_id") != args.get("attempt_id"):
            raise StubError(
                "store_conflict", "campaign attempt update does not match the current attempt"
            )
        current["state"] = state
        record["state"] = state
        _campaign_write_execution(args, ledger_path, ledger, (group_id,))
        return {"record": record, "attempt_dir": attempt_dir}


def op_campaign_markers(args):
    paths = args.get("paths")
    if not isinstance(paths, list) or not all(isinstance(path, str) and path for path in paths):
        raise StubError("invalid_arg", "paths must be a list of non-empty marker directories")
    if len(paths) > 1024:
        raise StubError("invalid_arg", "at most 1024 marker directories may be read")
    root = os.path.realpath(_campaign_root(args))
    items = []
    for raw in paths:
        path = os.path.realpath(_path(raw))
        try:
            inside = os.path.commonpath((root, path)) == root
        except ValueError:
            inside = False
        if not inside:
            raise StubError("permission", "marker path is outside campaign_dir", path=path)
        item = {"path": path}
        for name in ("started.json", "finished.json"):
            value = _campaign_read_json(
                os.path.join(path, name), required=False, max_bytes=64 * 1024
            )
            if value is not None:
                item[name[:-5]] = value
        items.append(item)
    return {"items": items, "count": len(items)}


def op_campaign_put_immutable(args):
    base = _campaign_base(args)
    kind = args.get("kind")
    object_id = _campaign_id(args.get("object_id"), "object_id")
    value = args.get("value")
    if not isinstance(value, dict):
        raise StubError("invalid_arg", "immutable campaign value must be an object")
    if kind == "definition":
        path = os.path.join(base, "definitions", object_id + ".json")
    elif kind == "run":
        path = os.path.join(base, "runs", object_id, "run.json")
    elif kind == "preflight_receipt":
        path = os.path.join(base, "receipts", "preflight", object_id + ".json")
    elif kind == "validation_receipt":
        run_id = _campaign_id(args.get("run_id"), "run_id")
        path = os.path.join(base, "runs", run_id, "receipts", "validation", object_id + ".json")
    else:
        raise StubError(
            "invalid_arg",
            "kind must be definition, run, preflight_receipt, or validation_receipt",
        )
    data = _campaign_json_bytes(value)
    if kind in ("preflight_receipt", "validation_receipt"):
        expected = hashlib.sha256(data).hexdigest()
        if object_id != expected:
            raise StubError(
                "invalid_arg",
                "receipt id does not match its canonical bytes",
                expected=expected,
                observed=object_id,
            )
    if len(data) > CAMPAIGN_MAX_TRANSACTION_BYTES:
        raise StubError("too_large", "immutable campaign value exceeds store limit")
    with _campaign_lock(base):
        existing = _campaign_read_json(path, required=False)
        if existing is not None:
            if _campaign_json_bytes(existing) != data:
                raise StubError(
                    "store_conflict",
                    "immutable campaign object already exists with different bytes",
                    kind=kind,
                    object_id=object_id,
                )
            return {"created": False, "path": path, "value": existing}
        _campaign_write_json(path, value)
        return {"created": True, "path": path, "value": value}


def _campaign_head(run_dir):
    head = _campaign_read_json(os.path.join(run_dir, "HEAD"), required=False)
    if head is None:
        return {"schema": CAMPAIGN_SCHEMA, "revision": 0, "transaction_id": None}
    if not isinstance(head, dict) or not isinstance(head.get("revision"), int):
        raise StubError("error", "invalid campaign HEAD", path=os.path.join(run_dir, "HEAD"))
    return head


def _campaign_quarantine(run_dir, txid, txdir, view_path=None, journal_path=None, reason=None):
    orphan_root = os.path.join(run_dir, "orphaned")
    os.makedirs(orphan_root, exist_ok=True)
    orphan = os.path.join(orphan_root, "%s-%d-%s" % (txid, int(time.time()), uuid.uuid4().hex[:8]))
    if os.path.exists(txdir):
        os.rename(txdir, orphan)
    else:
        os.makedirs(orphan, exist_ok=True)
    if view_path and os.path.exists(view_path):
        os.rename(view_path, os.path.join(orphan, "published-view"))
    if journal_path and os.path.exists(journal_path):
        os.rename(journal_path, os.path.join(orphan, "published-journal.json"))
    _campaign_write_json(
        os.path.join(orphan, "orphan.json"),
        {
            "transaction_id": txid,
            "quarantined_at": time.time(),
            "reason": reason or "unreachable",
        },
    )
    return orphan


def _campaign_reachable(run_dir, head, target_txid):
    """Return the target's committed pointer only when linked from current HEAD."""
    cursor = dict(head)
    for _unused in range(100000):
        txid = cursor.get("transaction_id")
        revision = cursor.get("revision")
        if txid == target_txid:
            basename = "%020d-%s" % (revision, txid)
            return {
                "schema": CAMPAIGN_SCHEMA,
                "revision": revision,
                "transaction_id": txid,
                "journal": "journal/" + basename + ".json",
                "view": "views/" + basename,
            }
        if not txid or not revision:
            return None
        journal_rel = cursor.get("journal")
        if not journal_rel:
            journal_rel = "journal/%020d-%s.json" % (revision, txid)
        journal = _campaign_read_json(os.path.join(run_dir, journal_rel), required=False)
        if not isinstance(journal, dict):
            return None
        parent = journal.get("parent")
        if not isinstance(parent, dict):
            return None
        cursor = parent
    raise StubError("too_large", "campaign history exceeds reachability traversal limit")


def op_campaign_commit(args):
    run_dir = _campaign_run(args)
    txid = _campaign_id(args.get("transaction_id"), "transaction_id")
    expected = args.get("expected_revision")
    if isinstance(expected, bool) or not isinstance(expected, int) or expected < 0:
        raise StubError("invalid_arg", "expected_revision must be a non-negative integer")
    events = args.get("events")
    documents = args.get("documents")
    if not isinstance(events, list) or not all(isinstance(event, dict) for event in events):
        raise StubError("invalid_arg", "events must be an array of objects")
    if len(events) > CAMPAIGN_MAX_EVENTS:
        raise StubError("too_large", "campaign transaction contains too many events")
    if not isinstance(documents, dict) or len(documents) > CAMPAIGN_MAX_DOCUMENTS:
        raise StubError("invalid_arg", "documents must be a bounded object")
    normalized_documents = {}
    for relative, value in documents.items():
        normalized_documents[_campaign_document_path(relative)] = value
    request_value = {
        "schema": CAMPAIGN_SCHEMA,
        "expected_revision": expected,
        "events": events,
        "documents": documents,
    }
    request_bytes = _campaign_json_bytes(request_value)
    if len(request_bytes) > CAMPAIGN_MAX_TRANSACTION_BYTES:
        raise StubError("too_large", "campaign transaction exceeds byte limit")
    request_digest = hashlib.sha256(request_bytes).hexdigest()
    run_record = os.path.join(run_dir, "run.json")
    if not os.path.isfile(run_record):
        raise StubError("not_found", "campaign run does not exist", run_id=args.get("run_id"))

    with _campaign_lock(run_dir):
        head = _campaign_head(run_dir)
        txdir = os.path.join(run_dir, "transactions", txid)
        request_path = os.path.join(txdir, "request.json")
        result_path = os.path.join(txdir, "result.json")
        prior_request = _campaign_read_json(request_path, required=False)
        if prior_request is not None:
            if prior_request.get("request_digest") != request_digest:
                raise StubError(
                    "store_conflict", "transaction_id was already used for different content"
                )
            result = _campaign_read_json(result_path, required=False)
            if result is not None:
                result["idempotent"] = True
                return result
            committed_head = _campaign_reachable(run_dir, head, txid)
            if committed_head is not None:
                result = {
                    "revision": committed_head["revision"],
                    "transaction_id": txid,
                    "head": committed_head,
                    "idempotent": True,
                    "recovered": True,
                }
                _campaign_write_json(result_path, result)
                return result

        revision = expected + 1
        basename = "%020d-%s" % (revision, txid)
        view_path = os.path.join(run_dir, "views", basename)
        journal_path = os.path.join(run_dir, "journal", basename + ".json")
        if prior_request is not None and os.path.isfile(journal_path) and os.path.isdir(view_path):
            if head.get("revision") == expected:
                new_head = {
                    "schema": CAMPAIGN_SCHEMA,
                    "revision": revision,
                    "transaction_id": txid,
                    "journal": "journal/" + basename + ".json",
                    "view": "views/" + basename,
                }
                _campaign_write_json(os.path.join(run_dir, "HEAD"), new_head)
                result = {
                    "revision": revision,
                    "transaction_id": txid,
                    "head": new_head,
                    "recovered": True,
                }
                _campaign_write_json(result_path, result)
                return result

        if head.get("revision") != expected:
            raise StubError(
                "store_conflict",
                "campaign HEAD changed",
                expected_revision=expected,
                actual_revision=head.get("revision"),
                actual_transaction_id=head.get("transaction_id"),
            )

        if prior_request is not None:
            _campaign_quarantine(
                run_dir,
                txid,
                txdir,
                view_path=view_path,
                journal_path=journal_path,
                reason="incomplete transaction replayed before HEAD publication",
            )
        os.makedirs(txdir, exist_ok=False)
        _campaign_write_json(
            request_path,
            {
                "request_digest": request_digest,
                "expected_revision": expected,
                "created_at": time.time(),
            },
        )
        staged_view = os.path.join(txdir, "view")
        document_digests = {}
        for relative, value in normalized_documents.items():
            target = os.path.join(staged_view, relative)
            data = _campaign_json_bytes(value)
            document_digests[relative.replace(os.sep, "/")] = hashlib.sha256(data).hexdigest()
            _campaign_write_json(target, value)
        _campaign_write_json(os.path.join(txdir, "events.json"), events)
        if os.environ.get("REMOTESLURM_TEST_CAMPAIGN_FAIL_STEP") == "staged":
            raise StubError("error", "injected campaign failure after staging")
        os.makedirs(os.path.dirname(view_path), exist_ok=True)
        os.rename(staged_view, view_path)
        if os.environ.get("REMOTESLURM_TEST_CAMPAIGN_FAIL_STEP") == "view":
            raise StubError("error", "injected campaign failure after view publication")
        journal = {
            "schema": CAMPAIGN_SCHEMA,
            "revision": revision,
            "transaction_id": txid,
            "parent": {
                "revision": head.get("revision"),
                "transaction_id": head.get("transaction_id"),
            },
            "committed_at": time.time(),
            "events": events,
            "events_sha256": hashlib.sha256(_campaign_json_bytes(events)).hexdigest(),
            "documents": document_digests,
            "request_digest": request_digest,
        }
        _campaign_write_json(journal_path, journal)
        if os.environ.get("REMOTESLURM_TEST_CAMPAIGN_FAIL_STEP") == "journal":
            raise StubError("error", "injected campaign failure after journal publication")
        new_head = {
            "schema": CAMPAIGN_SCHEMA,
            "revision": revision,
            "transaction_id": txid,
            "journal": "journal/" + basename + ".json",
            "view": "views/" + basename,
        }
        _campaign_write_json(os.path.join(run_dir, "HEAD"), new_head)
        if os.environ.get("REMOTESLURM_TEST_CAMPAIGN_FAIL_STEP") == "head":
            raise StubError("error", "injected campaign failure after HEAD publication")
        result = {"revision": revision, "transaction_id": txid, "head": new_head}
        _campaign_write_json(result_path, result)
        return result


def op_campaign_read(args):
    run_dir = _campaign_run(args)
    head = _campaign_head(run_dir)
    document = _campaign_document_path(args.get("document") or "summary.json")
    if head.get("revision") == 0 or not head.get("view"):
        raise StubError("not_found", "campaign run has no committed snapshot")
    path = os.path.join(run_dir, head["view"], document)
    max_bytes = args.get("max_bytes", CAMPAIGN_MAX_READ_BYTES)
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int):
        raise StubError("invalid_arg", "max_bytes must be an integer")
    max_bytes = max(1, min(CAMPAIGN_MAX_READ_BYTES, max_bytes))
    value = _campaign_read_json(path, max_bytes=max_bytes)
    return {"head": head, "document": document.replace(os.sep, "/"), "value": value}


def op_campaign_events(args):
    run_dir = _campaign_run(args)
    head = _campaign_head(run_dir)
    limit = args.get("limit", 200)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
        raise StubError("invalid_arg", "limit must be from 1 to 500")
    cursor = args.get("cursor")
    offset = 0
    if cursor is None:
        pointer = dict(head)
    else:
        if not isinstance(cursor, str):
            raise StubError("invalid_arg", "cursor must be text")
        parts = cursor.split(":", 2)
        if len(parts) != 3 or not parts[0].isdigit() or not parts[2].isdigit():
            raise StubError("invalid_arg", "invalid campaign event cursor")
        revision = int(parts[0])
        txid = _campaign_id(parts[1], "cursor transaction_id")
        offset = int(parts[2])
        pointer = _campaign_reachable(run_dir, head, txid)
        if pointer is None or pointer.get("revision") != revision:
            raise StubError("store_conflict", "campaign event cursor is not reachable from HEAD")
    events = []
    next_cursor = None
    while pointer and pointer.get("transaction_id") and len(events) < limit:
        revision = pointer["revision"]
        txid = pointer["transaction_id"]
        journal_rel = pointer.get("journal") or "journal/%020d-%s.json" % (revision, txid)
        journal = _campaign_read_json(os.path.join(run_dir, journal_rel))
        if journal.get("revision") != revision or journal.get("transaction_id") != txid:
            raise StubError("error", "campaign journal identity does not match its pointer")
        batch = journal.get("events")
        if not isinstance(batch, list):
            raise StubError("error", "campaign journal events are malformed")
        remaining = limit - len(events)
        events.extend(batch[offset : offset + remaining])
        consumed = offset + min(remaining, max(0, len(batch) - offset))
        if consumed < len(batch):
            next_cursor = "%d:%s:%d" % (revision, txid, consumed)
            break
        parent = journal.get("parent")
        if not isinstance(parent, dict) or not parent.get("transaction_id"):
            pointer = None
            next_cursor = None
            break
        pointer = _campaign_reachable(run_dir, head, parent["transaction_id"])
        offset = 0
        if len(events) >= limit and pointer is not None:
            next_cursor = "%d:%s:0" % (pointer["revision"], pointer["transaction_id"])
    return {
        "head": head,
        "events": events,
        "count": len(events),
        "next_cursor": next_cursor,
    }


def op_campaign_list(args):
    root = _campaign_root(args)
    limit = args.get("limit", 100)
    offset = args.get("offset", 0)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= CAMPAIGN_LIST_MAX:
        raise StubError("invalid_arg", "limit must be from 1 to %d" % CAMPAIGN_LIST_MAX)
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise StubError("invalid_arg", "offset must be a non-negative integer")
    name = args.get("name")
    records = []
    if name is None:
        try:
            names = sorted(
                item for item in os.listdir(root) if os.path.isdir(os.path.join(root, item))
            )
        except OSError as e:
            if e.errno == errno.ENOENT:
                names = []
            else:
                raise _os_error(e, root)
        records = [
            {"name": item} for item in names if re.match(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$", item)
        ]
    else:
        base = os.path.join(root, _campaign_name(name, "name"), "runs")
        try:
            run_ids = sorted(
                item for item in os.listdir(base) if os.path.isdir(os.path.join(base, item))
            )
        except OSError as e:
            if e.errno == errno.ENOENT:
                run_ids = []
            else:
                raise _os_error(e, base)
        for run_id in run_ids:
            run_dir = os.path.join(base, run_id)
            run = _campaign_read_json(os.path.join(run_dir, "run.json"), required=False)
            if isinstance(run, dict):
                head = _campaign_head(run_dir)
                summary = None
                if head.get("view"):
                    summary = _campaign_read_json(
                        os.path.join(run_dir, head["view"], "summary.json"), required=False
                    )
                records.append({"run": run, "head": head, "summary": summary})
    page = records[offset : offset + limit]
    next_offset = offset + len(page) if offset + len(page) < len(records) else None
    return {"items": page, "count": len(page), "next_offset": next_offset}


def op_campaign_receipts(args):
    base = _campaign_base(args)
    kind = args.get("kind")
    if kind == "preflight":
        receipt_dir = os.path.join(base, "receipts", "preflight")
    elif kind == "validation":
        run_id = _campaign_id(args.get("run_id"), "run_id")
        receipt_dir = os.path.join(base, "runs", run_id, "receipts", "validation")
    else:
        raise StubError("invalid_arg", "receipt kind must be preflight or validation")
    receipt_id = args.get("receipt_id")
    if receipt_id is not None:
        receipt_id = _campaign_id(receipt_id, "receipt_id")
        value = _campaign_read_json(os.path.join(receipt_dir, receipt_id + ".json"))
        return {"receipt_id": receipt_id, "value": value}
    limit = args.get("limit", 100)
    offset = args.get("offset", 0)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
        raise StubError("invalid_arg", "limit must be from 1 to 500")
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise StubError("invalid_arg", "offset must be a non-negative integer")
    try:
        names = sorted(
            (name[:-5] for name in os.listdir(receipt_dir) if name.endswith(".json")),
            reverse=True,
        )
    except OSError as e:
        if e.errno == errno.ENOENT:
            names = []
        else:
            raise _os_error(e, receipt_dir)
    selected = names[offset : offset + limit]
    items = []
    for name in selected:
        value = _campaign_read_json(os.path.join(receipt_dir, name + ".json"))
        items.append({"receipt_id": name, "value": value})
    next_offset = offset + len(items) if offset + len(items) < len(names) else None
    return {"items": items, "count": len(items), "next_offset": next_offset}


def op_campaign_check(args):
    root = _campaign_root(args)
    os.makedirs(root, exist_ok=True)
    token = uuid.uuid4().hex
    staged = os.path.join(root, ".doctor-" + token + ".staged")
    published = os.path.join(root, ".doctor-" + token + ".published")
    payload = {"schema": CAMPAIGN_SCHEMA, "token": token, "written_at": time.time()}
    try:
        _campaign_write_json(staged, payload)
        os.rename(staged, published)
        observed = _campaign_read_json(published)
        if observed != payload:
            raise StubError("error", "campaign store atomic-rename check read different bytes")
        mode = statmod.S_IMODE(os.stat(root).st_mode)
        return {
            "path": root,
            "writable": True,
            "atomic_rename": True,
            "mode": "%04o" % mode,
            "compute_visibility": "not_tested",
        }
    finally:
        for path in (staged, published):
            try:
                os.remove(path)
            except OSError:
                pass


def op_path_stats(args):
    paths = args.get("paths")
    if not isinstance(paths, list) or not all(isinstance(path, str) and path for path in paths):
        raise StubError("invalid_arg", "paths must be an array of non-empty strings")
    if len(paths) > 1024:
        raise StubError("invalid_arg", "at most 1024 paths may be inspected per request")
    records = []
    for raw in paths:
        path = _path(raw)
        try:
            info = os.lstat(path)
            if statmod.S_ISREG(info.st_mode):
                kind = "file"
            elif statmod.S_ISDIR(info.st_mode):
                kind = "directory"
            elif statmod.S_ISLNK(info.st_mode):
                kind = "symlink"
            else:
                kind = "other"
            records.append(
                {
                    "path": path,
                    "exists": True,
                    "kind": kind,
                    "size": info.st_size,
                    "mtime": info.st_mtime,
                    "readable": os.access(path, os.R_OK),
                    "writable": os.access(path, os.W_OK),
                    "executable": os.access(path, os.X_OK),
                }
            )
        except OSError as e:
            if e.errno == errno.ENOENT:
                parent = os.path.dirname(path)
                while parent and not os.path.exists(parent):
                    next_parent = os.path.dirname(parent)
                    if next_parent == parent:
                        break
                    parent = next_parent
                records.append(
                    {
                        "path": path,
                        "exists": False,
                        "nearest_parent": parent,
                        "parent_writable": bool(parent and os.access(parent, os.W_OK)),
                    }
                )
            else:
                records.append(
                    {"path": path, "exists": None, "error": _os_error(e, path).to_dict()}
                )
    return {"paths": records, "count": len(records)}


def _contract_match(path, want_hash, hash_budget):
    try:
        info = os.lstat(path)
        if statmod.S_ISREG(info.st_mode):
            kind = "file"
        elif statmod.S_ISDIR(info.st_mode):
            kind = "directory"
        elif statmod.S_ISLNK(info.st_mode):
            kind = "symlink"
        else:
            kind = "other"
        result = {
            "path": path,
            "exists": True,
            "kind": kind,
            "size": info.st_size,
            "mtime": info.st_mtime,
        }
        if want_hash and kind == "file":
            if info.st_size > CONTRACT_MAX_HASH_BYTES or info.st_size > hash_budget[0]:
                result["error"] = {
                    "code": "too_large",
                    "message": "file exceeds bounded contract hash limit",
                    "size": info.st_size,
                    "max_bytes": min(CONTRACT_MAX_HASH_BYTES, hash_budget[0]),
                }
            else:
                digest = hashlib.sha256()
                with io.open(path, "rb") as handle:
                    while True:
                        chunk = handle.read(1024 * 1024)
                        if not chunk:
                            break
                        digest.update(chunk)
                hash_budget[0] -= info.st_size
                result["sha256"] = digest.hexdigest()
        elif want_hash and kind == "symlink":
            result["sha256"] = hashlib.sha256(os.readlink(path).encode("utf-8")).hexdigest()
        return result
    except OSError as e:
        if e.errno == errno.ENOENT:
            return {"path": path, "exists": False}
        return {"path": path, "exists": None, "error": _os_error(e, path).to_dict()}


def op_contract_observe(args):
    items = args.get("items")
    if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
        raise StubError("invalid_arg", "items must be an array of contract observation objects")
    if len(items) > CONTRACT_MAX_ITEMS:
        raise StubError(
            "invalid_arg", "at most %d contract items may be observed" % CONTRACT_MAX_ITEMS
        )
    hash_budget = [CONTRACT_MAX_TOTAL_HASH_BYTES]
    match_budget = [CONTRACT_MAX_TOTAL_MATCHES]
    observations = []
    for item in items:
        raw = item.get("path")
        if not isinstance(raw, str) or not raw or "\x00" in raw:
            raise StubError("invalid_arg", "contract path must be non-empty text without NUL")
        path = _path(raw)
        use_glob = bool(item.get("glob", False))
        max_matches = item.get("max_matches", CONTRACT_MAX_MATCHES)
        if (
            isinstance(max_matches, bool)
            or not isinstance(max_matches, int)
            or not 1 <= max_matches <= CONTRACT_MAX_MATCHES
        ):
            raise StubError(
                "invalid_arg", "max_matches must be from 1 to %d" % CONTRACT_MAX_MATCHES
            )
        want_hash = bool(item.get("sha256", False))
        if use_glob:
            found = []
            truncated = False
            item_limit = min(max_matches, match_budget[0])
            try:
                for candidate in glob.iglob(path, recursive=True):
                    if len(found) >= item_limit:
                        truncated = True
                        break
                    found.append(candidate)
            except (OSError, re.error) as e:
                observations.append(
                    {
                        "path": path,
                        "matches": [],
                        "truncated": False,
                        "error": {"code": "error", "message": str(e)},
                    }
                )
                continue
            match_budget[0] -= len(found)
            matches = [
                _contract_match(candidate, want_hash, hash_budget) for candidate in sorted(found)
            ]
        else:
            match = _contract_match(path, want_hash, hash_budget)
            matches = [] if match.get("exists") is False else [match]
            truncated = False
        observations.append(
            {
                "path": path,
                "matches": matches,
                "truncated": truncated,
                "observed_at": time.time(),
            }
        )
    return {
        "observed_at": time.time(),
        "items": observations,
        "count": len(observations),
        "limits": {
            "max_items": CONTRACT_MAX_ITEMS,
            "max_matches": CONTRACT_MAX_MATCHES,
            "max_total_matches": CONTRACT_MAX_TOTAL_MATCHES,
            "max_hash_bytes": CONTRACT_MAX_HASH_BYTES,
            "max_total_hash_bytes": CONTRACT_MAX_TOTAL_HASH_BYTES,
        },
    }


def op_tool_check(args):
    names = args.get("names")
    if not isinstance(names, list) or not all(isinstance(name, str) and name for name in names):
        raise StubError("invalid_arg", "names must be an array of non-empty tool names")
    if len(names) > 64:
        raise StubError("invalid_arg", "at most 64 tools may be checked")
    tools = []
    for name in names:
        if "\x00" in name:
            raise StubError("invalid_arg", "tool name contains NUL")
        path = shutil.which(name)
        tools.append({"name": name, "available": path is not None, "path": path})
    return {"tools": tools, "count": len(tools)}


def op_squeue(args):
    fmt = args.get("format")
    if not isinstance(fmt, str) or not fmt:
        raise StubError("invalid_arg", "format required")
    argv = ["squeue", "-h", "-o", fmt]
    jobs = args.get("jobs")
    if jobs:
        argv += ["-j", ",".join(str(j) for j in jobs)]
    user = args.get("user")
    # `--me` needs Slurm >= 20.02; `-u <user>` works on every version we target.
    if not user and not jobs:
        user = getpass.getuser()
    if user:
        argv += ["-u", user]
    if args.get("states"):
        argv += ["-t", ",".join(args["states"])]
    return _slurm(argv, timeout=60)


def op_sacct(args):
    fields = args.get("fields")
    if not isinstance(fields, list) or not fields:
        raise StubError("invalid_arg", "fields required")
    argv = ["sacct", "-n", "-P", "--format=" + ",".join(fields)]
    jobs = args.get("jobs")
    if jobs:
        argv += ["-j", ",".join(str(j) for j in jobs)]
    if args.get("since"):
        argv += ["-S", str(args["since"])]
    if args.get("user"):
        argv += ["-u", str(args["user"])]
    # `-X`/--allocations suppresses the per-step (.batch/.extern) rows. Default to allocations
    # only (cheap: one row per job/array task) and include steps only when the caller asks —
    # steps are needed just for MaxRSS folding on a single job / diagnose.
    if not args.get("all_steps"):
        argv += ["-X"]
    return _slurm(argv, timeout=120)


def op_sstat(args):
    fields = args.get("fields")
    jobs = args.get("jobs")
    if not isinstance(fields, list) or not fields:
        raise StubError("invalid_arg", "fields required")
    if not isinstance(jobs, list) or not jobs:
        raise StubError("invalid_arg", "jobs required")
    if not all(re.match(r"^\d+(?:_\d+)?(?:\.(?:batch|extern|\d+))?$", str(job)) for job in jobs):
        raise StubError("invalid_arg", "malformed job step id", jobs=jobs)
    argv = ["sstat", "-n", "-P", "--format=" + ",".join(fields), "-j", ",".join(jobs)]
    return _slurm(argv, timeout=60)


def op_scontrol(args):
    what = args.get("what", "job")
    ident = args.get("id")
    if what not in ("job", "partition", "node", "config"):
        raise StubError("invalid_arg", "unsupported scontrol entity")
    if what == "job" and (ident is None or not re.match(r"^\d+(_\d+)?$", str(ident))):
        raise StubError("invalid_arg", "scontrol show job requires a single job id", id=ident)
    argv = ["scontrol", "-o", "show", what]
    if ident is not None:
        argv.append(str(ident))
    return _slurm(argv, timeout=60)


def op_scancel(args):
    jobs = args.get("jobs")
    if not isinstance(jobs, list) or not jobs or not all(isinstance(j, str) for j in jobs):
        raise StubError("invalid_arg", "jobs must be a non-empty list of job id strings")
    if not all(re.match(r"^\d+(_\d+|_\[[\d,-]+\])?$", j) for j in jobs):
        raise StubError("invalid_arg", "malformed job id(s)", jobs=jobs)
    me = getpass.getuser()
    # ownership check: only cancel jobs squeue attributes to us
    chk = _slurm(["squeue", "-h", "-o", "%i|%u", "-j", ",".join(jobs)], timeout=60)
    owned = set()
    for line in chk["stdout"].splitlines():
        parts = line.strip().split("|")
        if len(parts) == 2 and parts[1] == me:
            owned.add(parts[0])
    base_ids = set(j.split("_")[0] for j in owned)
    to_cancel = [j for j in jobs if j in owned or j.split("_")[0] in base_ids]
    skipped = [j for j in jobs if j not in to_cancel]
    result = {"cancelled": [], "skipped": skipped, "rc": 0, "stderr": ""}
    if to_cancel:
        res = _slurm(["scancel"] + to_cancel, timeout=60)
        result["rc"] = res["rc"]
        result["stderr"] = res["stderr"]
        if res["rc"] == 0:
            result["cancelled"] = to_cancel
    return result


def op_sinfo(args):
    fmt = args.get("format") or "%P|%a|%l|%D|%T|%c|%m|%G"
    return _slurm(["sinfo", "-h", "-o", fmt], timeout=60)


# --------------------------------------------------------------------------- queue intelligence
# Each of these returns a raw ``{rc, stdout, stderr}`` (like the other Slurm ops); the client
# parses the text and tolerates missing columns/tools. They are FAST ops (short, bounded queries).


def op_squeue_start(args):
    """``squeue --start -h -o "%i|%S|%r"`` — scheduler start-time estimates for pending jobs."""
    argv = ["squeue", "--start", "-h", "-o", "%i|%S|%r"]
    jobs = args.get("jobs")
    if jobs:
        argv += ["-j", ",".join(str(j) for j in jobs)]
    else:
        # `--me` needs Slurm >= 20.02; `-u <user>` works on every version we target.
        user = args.get("user") or getpass.getuser()
        argv += ["-u", str(user)]
    return _slurm_soft(argv, timeout=60)


def op_sshare(args):
    """``sshare -U -P`` — this user's fair-share numbers (parsable, one row per account)."""
    return _slurm_soft(["sshare", "-U", "-P"], timeout=60)


def op_qos(args):
    """``sacctmgr -P -n show qos format=...`` — QOS limits (walltime, jobs/user, priority)."""
    fmt = "name,maxwall,maxjobspu,maxtresperuser,priority"
    return _slurm_soft(["sacctmgr", "-P", "-n", "show", "qos", "format=" + fmt], timeout=60)


def op_assoc(args):
    """``sacctmgr -P -n show assoc user=<me> format=...`` — this user's account/QOS associations."""
    me = args.get("user") or getpass.getuser()
    fmt = "account,partition,qos,grptres,maxjobs"
    return _slurm_soft(
        ["sacctmgr", "-P", "-n", "show", "assoc", "user=" + str(me), "format=" + fmt], timeout=60
    )


def op_quota(args):
    """Disk usage: run the host-provided ``command`` (argv) if given, else ``df -h`` of ``paths``.

    ``command`` is a pre-split argv list (the client splits the configured ``quota_command``);
    it is run with no shell, matching the stub's no-shell invariant. When no command is given,
    ``df -h`` is run over the given ``paths`` (``~``/``$VARS`` expanded here, non-existent paths
    dropped). A missing tool yields a soft ``rc != 0`` (``available: false`` on the client).
    """
    # A configured quota_command runs in a login shell so site-provided functions and modules
    # resolve. It is a trusted local configuration value, not request-controlled shell input.
    shell_cmd = args.get("command_shell")
    if shell_cmd is not None:
        if not isinstance(shell_cmd, str) or not shell_cmd.strip():
            raise StubError("invalid_arg", "quota command_shell must be a non-empty string")
        return _slurm_soft(["bash", "-lc", shell_cmd], timeout=90)
    cmd = args.get("command")
    if cmd:
        if (
            not isinstance(cmd, (list, tuple))
            or not cmd
            or not all(isinstance(a, str) for a in cmd)
        ):
            raise StubError("invalid_arg", "quota command must be a non-empty list of strings")
        return _slurm_soft(list(cmd), timeout=90)
    raw_paths = args.get("paths") or []
    if not isinstance(raw_paths, (list, tuple)):
        raise StubError("invalid_arg", "paths must be a list of strings")
    paths = []
    for p in raw_paths:
        if not isinstance(p, str) or not p:
            continue
        ep = os.path.expanduser(os.path.expandvars(p))
        if "$" in ep:  # an unset variable survived expansion; skip rather than df the wrong place
            continue
        if os.path.exists(ep) and ep not in paths:
            paths.append(ep)
    return _slurm_soft(["df", "-hP"] + paths, timeout=60)  # -P: POSIX, one line per fs


OPS = {
    "ping": op_ping,
    "info": op_info,
    "ls": op_ls,
    "stat": op_stat,
    "read": op_read,
    "write": op_write,
    "expandpath": op_expandpath,
    "edit": op_edit,
    "diff": op_diff,
    "mkdir": op_mkdir,
    "rm": op_rm,
    "glob": op_glob,
    "progress_count": op_progress_count,
    "grep": op_grep,
    "run": op_run,
    "srun": op_srun,
    "follow": op_follow,
    "detach": op_detach,
    "proc_status": op_proc_status,
    "proc_tail": op_proc_tail,
    "proc_kill": op_proc_kill,
    "waitfor": op_waitfor,
    "sbatch": op_sbatch,
    "task_fingerprint": op_task_fingerprint,
    "task_ensure": op_task_ensure,
    "task_update": op_task_update,
    "campaign_attempt_ensure": op_campaign_attempt_ensure,
    "campaign_attempt_update": op_campaign_attempt_update,
    "campaign_markers": op_campaign_markers,
    "campaign_put_immutable": op_campaign_put_immutable,
    "campaign_commit": op_campaign_commit,
    "campaign_read": op_campaign_read,
    "campaign_events": op_campaign_events,
    "campaign_list": op_campaign_list,
    "campaign_receipts": op_campaign_receipts,
    "campaign_check": op_campaign_check,
    "path_stats": op_path_stats,
    "contract_observe": op_contract_observe,
    "tool_check": op_tool_check,
    "squeue": op_squeue,
    "sstat": op_sstat,
    "sacct": op_sacct,
    "scontrol": op_scontrol,
    "scancel": op_scancel,
    "sinfo": op_sinfo,
    "squeue_start": op_squeue_start,
    "sshare": op_sshare,
    "qos": op_qos,
    "assoc": op_assoc,
    "quota": op_quota,
}


# --------------------------------------------------------------------------- server loop


class Server(object):
    def __init__(self, out):
        self.out = out
        self.lock = threading.Lock()  # serialises writes to stdout
        self.fast = ThreadPoolExecutor(max_workers=FAST_WORKERS)
        self.slow = ThreadPoolExecutor(max_workers=SLOW_WORKERS)
        self.long = ThreadPoolExecutor(max_workers=LONG_WORKERS)
        self.reg_lock = threading.Lock()  # protects the five registries below
        self.running = {}  # request id -> live Popen for cancellable slow ops
        self.events = {}  # request id -> threading.Event for non-subprocess cancellables (follow)
        self.cancelled = set()  # request ids that `cancel` has just killed
        # Cancellable requests received but not finished, so a cancel that overtakes one still
        # queued behind a busy pool (or not yet spawned) is not lost; and those so cancelled.
        self.pending = set()
        self.precancelled = set()
        self.alive = True

    def send(self, payload):
        line = RS + json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n"
        with self.lock:
            try:
                self.out.write(line)
                self.out.flush()
            except (BrokenPipeError, OSError):
                self.alive = False

    # -- cancellation registry ------------------------------------------------------------
    def _register(self, rid, proc):
        with self.reg_lock:
            self.running[rid] = proc
            early = rid in self.precancelled
            if early:
                self.precancelled.discard(rid)
                self.cancelled.add(rid)
        if early:  # its cancel arrived while it was still queued: stop it at once
            _kill_group(proc, signal.SIGKILL)

    def _unregister(self, rid):
        with self.reg_lock:
            self.running.pop(rid, None)

    def _register_event(self, rid, event):
        with self.reg_lock:
            self.events[rid] = event
            if rid in self.precancelled:
                self.precancelled.discard(rid)
                self.cancelled.add(rid)
                event.set()

    def _unregister_event(self, rid):
        with self.reg_lock:
            self.events.pop(rid, None)

    def _take_cancelled(self, rid):
        with self.reg_lock:
            if rid in self.cancelled:
                self.cancelled.discard(rid)
                return True
            return False

    def _cancel(self, args):
        """Kill the process group of a running slow op and mark it cancelled.

        Returns ``{cancelled: bool, id}``; ``cancelled`` is false (with ``reason``) when the
        target op is unknown or has already finished (the finish/cancel race).
        """
        target = args.get("id")
        if not isinstance(target, str) or not target:
            raise StubError("invalid_arg", "cancel requires a string request id")
        with self.reg_lock:
            proc = self.running.get(target)
            event = self.events.get(target)
            if proc is not None:
                if proc.poll() is not None:
                    return {"cancelled": False, "id": target, "reason": "not running"}
                self.cancelled.add(target)
                # fall through to kill the process group outside the lock
            elif event is not None:
                # A non-subprocess cancellable (e.g. `follow`): flag it and set its event so the
                # op's poll loop breaks out promptly and sends its terminal frame.
                self.cancelled.add(target)
                event.set()
                return {"cancelled": True, "id": target}
            elif target in self.pending:
                # Received but not started (queued behind a busy pool, or about to spawn): it is
                # stopped the moment it registers.
                self.precancelled.add(target)
                return {"cancelled": True, "id": target, "queued": True}
            else:
                return {"cancelled": False, "id": target, "reason": "not running"}
        _kill_group(proc, signal.SIGTERM)
        deadline = time.time() + KILL_GRACE
        while time.time() < deadline and proc.poll() is None:
            time.sleep(0.02)
        if proc.poll() is None:
            _kill_group(proc, signal.SIGKILL)
        return {"cancelled": True, "id": target}

    def handle(self, req):
        rid = req.get("id")
        op = req.get("op")
        args = req.get("args") or {}
        try:
            if not isinstance(args, dict):
                raise StubError("invalid_arg", "args must be an object")
            if op == "cancel":
                self.send({"id": rid, "ok": True, "result": self._cancel(args), "done": True})
                return
            fn = OPS.get(op)
            if fn is None:
                raise StubError("invalid_arg", "unknown op: %r" % (op,))
            registers_proc = op in SLOW_OPS
            registers_event = op in LONG_OPS
            # `run`/`srun` stream only when asked; `follow` always streams; `waitfor` never does.
            streaming = (op in STREAM_OPS and bool(args.get("stream"))) or (op in ALWAYS_STREAM_OPS)
            cancellable = registers_proc or registers_event
            if registers_proc or registers_event or streaming:
                # Copy args so the caller's dict is never mutated and the internal `_`-prefixed
                # keys (register/emit/cancel-event callbacks) can't leak back out.
                args = dict(args)
            if registers_proc:
                args["_register"] = lambda proc, _rid=rid: self._register(_rid, proc)
            if registers_event:
                event = threading.Event()
                self._register_event(rid, event)
                args["_cancel_event"] = event
            if streaming:
                # Intermediate frames carry a `chunk` and `done: false`; the terminal frame
                # (sent below, after the op returns) carries the `result` and `done: true`.
                args["_emit"] = lambda chunk, _rid=rid: self.send(
                    {"id": _rid, "ok": True, "chunk": chunk, "done": False}
                )
            try:
                result = fn(args)
            finally:
                if registers_proc:
                    self._unregister(rid)
                if registers_event:
                    self._unregister_event(rid)
            if cancellable and self._take_cancelled(rid) and isinstance(result, dict):
                # The op returned partial output after cancel killed its process/loop.
                result["cancelled"] = True
            self.send({"id": rid, "ok": True, "result": result, "done": True})
        except StubError as e:
            if op in SLOW_OPS or op in LONG_OPS:
                self._take_cancelled(rid)
            self.send({"id": rid, "ok": False, "error": e.to_dict(), "done": True})
        except Exception as e:  # pragma: no cover - defensive
            self.send(
                {
                    "id": rid,
                    "ok": False,
                    "error": {
                        "code": "error",
                        "message": "%s: %s" % (type(e).__name__, e),
                        "traceback": traceback.format_exc()[-2000:],
                    },
                    "done": True,
                }
            )
        finally:
            if op in SLOW_OPS or op in LONG_OPS:
                with self.reg_lock:
                    self.pending.discard(rid)
                    self.precancelled.discard(rid)

    def serve(self, inp):
        for raw in inp:
            if not self.alive:
                break
            raw = raw.strip()
            if not raw:
                continue
            try:
                req = json.loads(raw)
            except ValueError:
                self.send(
                    {
                        "id": None,
                        "ok": False,
                        "error": {"code": "invalid_arg", "message": "bad json"},
                        "done": True,
                    }
                )
                continue
            op = req.get("op")
            if op == "shutdown":
                self.send({"id": req.get("id"), "ok": True, "result": {"bye": True}, "done": True})
                break
            if op in SLOW_OPS or op in LONG_OPS:
                # Known from the moment it is read, so a cancel sent right behind it (and
                # dispatched to the fast pool first) finds it even while it is still queued.
                with self.reg_lock:
                    self.pending.add(req.get("id"))
            pool = self.long if op in LONG_OPS else (self.slow if op in SLOW_OPS else self.fast)
            pool.submit(self.handle, req)
        self.fast.shutdown(wait=True)
        self.slow.shutdown(wait=True)
        self.long.shutdown(wait=True)


def main():
    # Force utf-8, line-buffered, no inheritance of odd locale settings.
    out = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
    )
    inp = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")
    try:
        os.chdir(os.path.expanduser("~"))
    except OSError:
        pass
    out.write(
        "REMOTESLURM-READY %d %d %s %s\n"
        % (PROTOCOL, os.getpid(), sys.version.split()[0], _stub_sha() or "unknown")
    )
    out.flush()
    srv = Server(out)
    try:
        srv.serve(inp)
    except (BrokenPipeError, KeyboardInterrupt):
        pass


if __name__ == "__main__":
    main()
