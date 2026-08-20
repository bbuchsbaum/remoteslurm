"""Client-side watch support (F2): a per-host terminal-event log and desktop notifications.

Everything here runs on the laptop — there is no daemon-side watching. ``rslurm watch`` polls
the normal :class:`~remoteslurm.cluster.Cluster` and, when a job reaches a terminal state,
appends one JSON line to ``<state>/<host>/events.jsonl`` and (optionally) fires a desktop
notification. ``rslurm events`` / the MCP ``events`` tool drain that log so an agent can ask
"did anything finish while I was working?" without blocking.
"""

from __future__ import annotations

import datetime
import fcntl
import json
import shlex
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from .config import HostConfig, state_dir

_lock = threading.Lock()


def events_path(host: str) -> Path:
    return state_dir() / host / "events.jsonl"


def cursor_path(host: str) -> Path:
    return state_dir() / host / "events.cursor"


def append_event(host: str, event: dict[str, Any]) -> None:
    """Append one terminal-job event as a JSON line (flocked; best effort)."""
    p = events_path(host)
    line = json.dumps(event, separators=(",", ":")) + "\n"
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        with _lock, open(p, "a", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                f.write(line)
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)
    except OSError:
        pass


def _load_lines(data: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for ln in data.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.append(json.loads(ln))
        except ValueError:
            continue
    return out


def read_all_events(host: str) -> list[dict[str, Any]]:
    p = events_path(host)
    if not p.exists():
        return []
    try:
        return _load_lines(p.read_text("utf-8"))
    except OSError:
        return []


def _since_epoch(since: str) -> float | None:
    """Parse an ISO-8601 timestamp (or a bare epoch) into epoch seconds; None if unparseable."""
    since = since.strip()
    if not since:
        return None
    try:
        return float(since)
    except ValueError:
        pass
    try:
        return datetime.datetime.fromisoformat(since).timestamp()
    except ValueError:
        return None


def drain_events(host: str, *, since: str | None = None, all: bool = False) -> list[dict[str, Any]]:
    """Return events. Default: only those not yet seen (a byte-offset cursor is advanced).

    ``all=True`` returns everything and does not touch the cursor; ``since`` (ISO or epoch)
    filters by event time and also leaves the cursor alone (an explicit, repeatable query).
    """
    p = events_path(host)
    if not p.exists():
        return []
    if all:
        return read_all_events(host)
    if since is not None:
        cut = _since_epoch(since)
        evs = read_all_events(host)
        if cut is None:
            return evs
        return [e for e in evs if float(e.get("t", 0) or 0) >= cut]
    # Unseen-only: read from the stored byte offset, then advance it to end-of-file.
    try:
        size = p.stat().st_size
    except OSError:
        return []
    cur = cursor_path(host)
    off = 0
    if cur.exists():
        try:
            off = int(cur.read_text("utf-8").strip() or "0")
        except (OSError, ValueError):
            off = 0
    if off > size:  # file was truncated/rotated under us
        off = 0
    try:
        with open(p, "rb") as f:
            f.seek(off)
            data = f.read()
    except OSError:
        return []
    out = _load_lines(data.decode("utf-8", "replace"))
    try:
        cur.parent.mkdir(parents=True, exist_ok=True)
        cur.write_text(str(size), "utf-8")
    except OSError:
        pass
    return out


def notify(host: HostConfig, message: str) -> bool:
    """Fire a desktop notification (best effort; never raises). Returns whether it ran cleanly.

    Uses the host's ``notify_command`` when set (``MSG`` is substituted with ``message``, or the
    message is appended), else a platform default: ``osascript`` on macOS, ``notify-send`` on
    Linux. Any failure is swallowed — a missing notifier must not break ``watch``.
    """
    cmd = host.notify_command
    if cmd:
        full = cmd.replace("MSG", message) if "MSG" in cmd else cmd + " " + shlex.quote(message)
        try:
            argv = shlex.split(full)
        except ValueError:
            return False
    elif sys.platform == "darwin":
        argv = [
            "osascript",
            "-e",
            f'display notification "{message}" with title "remoteslurm"',
        ]
    else:
        argv = ["notify-send", "remoteslurm", message]
    try:
        r = subprocess.run(argv, capture_output=True, timeout=10)
        return r.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False
