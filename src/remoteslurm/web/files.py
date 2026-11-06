"""Queue bounded log tails and directory listings for a job through the daemon's observer.

The page names a job, never a path: paths come from the job's own record (registry, scontrol
or the documented WorkDir default), so this cannot be used to read arbitrary remote files.
"""

from __future__ import annotations

import base64
import re
from typing import Any

from ..activity.producer import target_id, within
from ..activity.store import ActivityStore
from ..errors import RemoteSlurmError

_TOKEN = re.compile(r"%(\d*)([AajxuN%])")


def expand(pattern: str, job: dict, task: int | None) -> str | None:
    """Expand Slurm filename patterns we can resolve; None if any token is unknowable."""
    unknown = False

    def sub(m: re.Match) -> str:
        nonlocal unknown
        width, code = m.group(1), m.group(2)
        value: str | None
        if code == "%":
            return "%"
        if code == "A":
            value = job["id"]
        elif code == "a":
            value = None if task is None else str(task)
        elif code == "j":
            value = job["id"] if not job.get("array") else None
        elif code == "x":
            value = job.get("name")
        elif code == "u":
            value = job.get("user")
        else:
            value = None
        if value is None:
            unknown = True
            return ""
        return value.zfill(int(width)) if width and value.isdigit() else value

    out = _TOKEN.sub(sub, pattern)
    return None if unknown else out


def task_path(path: str, job: dict, task: int | None) -> str | None:
    """A per-task path: expand patterns, or swap the task id of the one scontrol reported."""
    if "%" in path:
        return expand(path, job, task)
    array = job.get("array")
    if array and task is not None:
        known = array.get("pathsTask")
        if known is None:
            return None  # scontrol reported the pending meta-record, not a task's own path
        if str(known) != str(task):
            marker = f"{job['id']}_{known}"
            if marker not in path:
                return None
            return path.replace(marker, f"{job['id']}_{task}")
    return path


def page(request: dict, *, kind: str) -> dict:
    """Shape an inspection receipt (or cached capture) for the page."""

    def result(captured: dict | None) -> dict | None:
        if not captured:
            return None
        if kind == "list":
            return {
                "observedAt": captured.get("observed_at"),
                "entries": [
                    {k: e.get(k) for k in ("name", "type", "size", "mtime")}
                    for e in captured.get("entries", [])
                ],
                "partial": bool(captured.get("partial") or captured.get("next_offset")),
            }
        text = captured.get("content")
        if text is None and captured.get("content_b64"):
            text = base64.b64decode(captured["content_b64"]).decode("utf-8", "replace")
        lines = (text or "").splitlines()
        return {
            "lines": lines[-200:],
            "observedAt": captured.get("observed_at"),
            "size": captured.get("size"),
            "truncated": bool(captured.get("truncated")),
            "gap": captured.get("gap"),
            "binary": bool(captured.get("binary")),
        }

    state = request.get("state", "UNAVAILABLE")
    out = {
        "request": request.get("request_id"),
        "state": state,
        "path": request.get("path"),
        "pathGuessed": request.get("path_guessed", False),
        "error": request.get("error"),
        "action": request.get("action"),
        "result": result(request.get("result")) if state == "DONE" else None,
        "cached": result(request.get("cached")),
    }
    if state == "ERROR" and request.get("error") == "budget_exhausted":
        out["action"] = "This host's read budget is in use; try again in a few seconds."
    return out


def unavailable(error: str, action: str | None = None, path: str | None = None) -> dict:
    return {
        "request": None,
        "state": "UNAVAILABLE",
        "path": path,
        "pathGuessed": False,
        "error": error,
        "action": action,
        "result": None,
        "cached": None,
    }


def queue(hc: Any, job: dict, kind: str, path: str, *, guessed: bool) -> dict:
    """Queue a read for ``path``: under an enabled workspace if one contains it, else lease."""
    from ..workspace.service import inspect_request

    store = ActivityStore()
    ident = target_id(hc)
    spaces = [
        w
        for w in store.documents("workspace", limit=1000)
        if w["target_id"] == ident and w["enabled"] and within(path, w["remote_root"])
    ]
    workspace = (
        spaces[0]
        if spaces
        else {
            "workspace_id": None,
            "target_id": ident,
            "host": hc.name,
            "remote_root": "/",
        }
    )
    try:
        request = inspect_request(workspace, kind, path)
    except RemoteSlurmError as exc:
        return unavailable(
            getattr(exc, "code", type(exc).__name__),
            str(exc) if not getattr(exc, "action", None) else exc.action,
            path,
        )
    request["path_guessed"] = guessed
    return page(request, kind=kind)


def poll(request_id: str, *, kind: str) -> dict:
    store = ActivityStore()
    request = store.get("inspection", request_id)
    if request is None or request.get("kind") != kind:
        return unavailable("expired", "This read finished a while ago; open the tab again.")
    return page(request, kind=kind)
