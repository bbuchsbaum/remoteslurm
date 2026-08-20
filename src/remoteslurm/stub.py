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
import io
import json
import os
import re
import select
import shutil
import signal
import socket
import stat as statmod
import subprocess
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

PROTOCOL = 2
RS = "\x1e"
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
SLOW_OPS = frozenset(("run", "srun", "sbatch"))
# Ops that can stream multi-frame responses when the request carries ``stream: true``.
STREAM_OPS = frozenset(("run", "srun"))
# Long-lived streaming ops with no subprocess (e.g. `follow` tails a file): served by the slow
# pool, always stream, and are cancelled via a per-request ``threading.Event`` in the registry.
LONG_OPS = frozenset(("follow",))
KILL_GRACE = 0.5  # seconds between SIGTERM and SIGKILL when cancelling a process group

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


def _stream_communicate(proc, argv, timeout, stdin, max_output, emit, new_session, t0):
    """Drive ``proc`` to completion, emitting stdout/stderr as it arrives via ``emit``.

    ``emit`` is called with ``{"stream": "stdout"|"stderr", "data": <text>}`` for each piece of
    output. At most ``max_output`` bytes per stream are decoded/emitted/kept; past that the
    stream is flagged truncated but the pipe is still drained so the child never blocks on a
    full buffer. Returns the same result shape as the non-streaming path (rc/stdout/stderr/…),
    where ``stdout``/``stderr`` hold the bounded capture. A timeout kills the process group and
    raises ``StubError("timeout")`` with whatever output was captured, matching ``_run``.
    """
    if stdin is not None and proc.stdin is not None:
        try:
            proc.stdin.write(stdin.encode("utf-8"))
            proc.stdin.close()
        except (BrokenPipeError, OSError):
            pass
    pipes = {}  # fd -> ("stdout"|"stderr", fileobj)
    if proc.stdout is not None:
        pipes[proc.stdout.fileno()] = ("stdout", proc.stdout)
    if proc.stderr is not None:
        pipes[proc.stderr.fileno()] = ("stderr", proc.stderr)
    kept = {"stdout": [], "stderr": []}  # decoded text within the cap (for the result)
    kept_bytes = {"stdout": 0, "stderr": 0}
    truncated = {"stdout": False, "stderr": False}
    dec = {
        "stdout": codecs.getincrementaldecoder("utf-8")("replace"),
        "stderr": codecs.getincrementaldecoder("utf-8")("replace"),
    }
    open_fds = set(pipes)
    deadline = t0 + timeout
    timed_out = False
    while open_fds:
        remaining = deadline - time.time()
        if remaining <= 0:
            timed_out = True
            break
        try:
            ready, _, _ = select.select(list(open_fds), [], [], min(remaining, 0.5))
        except (OSError, ValueError):
            break
        for fd in ready:
            name, _f = pipes[fd]
            try:
                data = os.read(fd, STREAM_READ_BYTES)
            except OSError:
                open_fds.discard(fd)
                continue
            if not data:
                open_fds.discard(fd)
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
    if not timed_out:
        try:
            proc.wait(timeout=max(0.1, deadline - time.time()))
        except subprocess.TimeoutExpired:
            timed_out = True
    if timed_out:
        if new_session:
            _kill_group(proc, signal.SIGKILL)
        else:
            proc.kill()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            pass
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
    return {
        "rc": proc.returncode,
        "stdout": "".join(kept["stdout"]),
        "stderr": "".join(kept["stderr"]),
        "stdout_truncated": truncated["stdout"],
        "stderr_truncated": truncated["stderr"],
        "duration": round(dur, 3),
    }


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
    if emit is not None:
        # Streaming variant: read the pipes incrementally and emit chunks as output arrives.
        return _stream_communicate(proc, argv, timeout, stdin, max_output, emit, new_session, t0)
    try:
        out, err = proc.communicate(
            input=stdin.encode("utf-8") if stdin is not None else None, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        if new_session:
            _kill_group(proc, signal.SIGKILL)
        else:
            proc.kill()
        out, err = proc.communicate()
        raise StubError(
            "timeout",
            "command timed out after %ss: %s" % (timeout, " ".join(argv[:4])),
            stdout=_decode(out[-max_output:]),
            stderr=_decode(err[-max_output:]),
        )
    dur = time.time() - t0
    res = {
        "rc": proc.returncode,
        "stdout": _decode(out[:max_output]),
        "stderr": _decode(err[:max_output]),
        "stdout_truncated": len(out) > max_output,
        "stderr_truncated": len(err) > max_output,
        "duration": round(dur, 3),
    }
    return res


def _which(name):
    return shutil.which(name)


# --------------------------------------------------------------------------- ops


def op_ping(args):
    return {"pid": os.getpid(), "time": time.time(), "protocol": PROTOCOL}


def op_info(args):
    env_keys = ["SCRATCH", "PROJECT", "HOME", "USER", "SLURM_CLUSTER_NAME", "CC_CLUSTER", "TMPDIR"]
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
        "env": env,
        "slurm_version": slurm_version,
        "slurm_tools": {
            n: bool(_which(n))
            for n in ("sbatch", "squeue", "sacct", "scancel", "scontrol", "sinfo")
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

    Used by sync/put/get so remote paths like ``$SCRATCH/proj`` resolve correctly without ever
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
        # path anyway would silently target the wrong place (e.g. $PROJECT unset -> "/mvpa").
        raise StubError(
            "invalid_arg",
            "path contains an environment variable that is not set on the remote: %s" % raw,
            path=raw,
            action="check the variable is exported on the login node (e.g. echo $SCRATCH)",
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


def _rm_roots():
    """Directories a recursive `rm` must never take out wholesale: home + the big shared roots."""
    roots = set()
    for name in ("HOME", "SCRATCH", "PROJECT"):
        v = os.environ.get(name)
        if v:
            roots.add(os.path.normpath(os.path.expanduser(os.path.expandvars(v))))
    roots.add(os.path.normpath(os.path.expanduser("~")))
    return roots


def op_rm(args):
    p = _path(args.get("path"), must_exist=True)
    recursive = bool(args.get("recursive", False))
    home = os.path.expanduser("~")
    if p in ("/", home) or p == os.path.dirname(home):
        raise StubError("invalid_arg", "refusing to remove %s" % p, path=p)
    if recursive:
        # A recursive delete is the dangerous one: refuse the shared roots themselves
        # ($HOME/$SCRATCH/$PROJECT) and anything shallower than three path components
        # (e.g. /scratch/<user>), which are almost always a fat-fingered target.
        if p in _rm_roots():
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
    flags = ["srun", "--quiet", "--unbuffered"]
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
                or os.path.join(cwd or os.path.expanduser("~"), ".remoteslurm", "scripts")
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
    # The configured quota_command is run in a LOGIN shell: on Alliance clusters
    # `diskusage_report` is a module-provided shell function, and the bare binary reports
    # different numbers, so we must let the login profile define it. This is a user-configured,
    # trusted command (not stub-internal input), so the shell here is intentional.
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
    "grep": op_grep,
    "run": op_run,
    "srun": op_srun,
    "follow": op_follow,
    "sbatch": op_sbatch,
    "squeue": op_squeue,
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
        self.reg_lock = threading.Lock()  # protects `running`, `events` and `cancelled`
        self.running = {}  # request id -> live Popen for cancellable slow ops
        self.events = {}  # request id -> threading.Event for non-subprocess cancellables (follow)
        self.cancelled = set()  # request ids that `cancel` has just killed
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

    def _unregister(self, rid):
        with self.reg_lock:
            self.running.pop(rid, None)

    def _register_event(self, rid, event):
        with self.reg_lock:
            self.events[rid] = event

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
            # `run`/`srun` stream only when asked; `follow` always streams.
            streaming = (op in STREAM_OPS and bool(args.get("stream"))) or (op in LONG_OPS)
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
    out.write("REMOTESLURM-READY %d %d %s\n" % (PROTOCOL, os.getpid(), sys.version.split()[0]))
    out.flush()
    srv = Server(out)
    try:
        srv.serve(inp)
    except (BrokenPipeError, KeyboardInterrupt):
        pass


if __name__ == "__main__":
    main()
