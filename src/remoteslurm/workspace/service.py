"""Workspace identity, cached snapshots and bounded inspection requests."""

from __future__ import annotations

import hashlib
import os
import posixpath
import time
import uuid
from pathlib import Path
from typing import Any

from ..activity.attention import project_attention
from ..activity.producer import target_id
from ..activity.store import ActivityStore, encoded
from ..config import Config, config_path, state_dir
from ..errors import InvalidArgument, NotFound
from ..identity import control_identity
from ..sync import resolve_project


def select(
    config: Config,
    host: str | None,
    project: str | None = None,
    path: str | None = None,
    *,
    cluster: Any = None,
) -> dict:
    hc = config.host(host)
    if project and path:
        raise InvalidArgument("select a project or path, not both")
    p = resolve_project(hc, project, Path.cwd()) if path is None else None
    source = p.remote if p else str(path)
    target = target_id(hc)
    store = ActivityStore()
    previous_mapping = None
    for saved in sorted(
        store.documents("workspace", limit=1000), key=lambda w: w["created_at"], reverse=True
    ):
        if saved["target_id"] == target and saved["source_root"] == source:
            if cluster is None:
                return saved
            previous_mapping = saved
            break
    if cluster is None:
        raise NotFound("workspace has no cached root", action="open `rslurm view` online first")
    resolved = cluster.call("workspace_anchor", path=source, _timeout=10)
    root, anchor = resolved["path"], resolved["anchor"]
    ident = hashlib.sha256(encoded([target, root, anchor]).encode()).hexdigest()
    previous = store.get("workspace", ident)
    result = {
        "workspace_id": ident,
        "target_id": target,
        "host": hc.name,
        "project": p.name if p else None,
        "source_root": source,
        "remote_root": root,
        "anchor": anchor,
        "min_registered_at": (
            previous.get("min_registered_at", 0)
            if previous
            else time.time()
            if previous_mapping
            else 0
        ),
        "local_root": str(p.local_path()) if p else None,
        "enabled": previous.get("enabled", False) if previous else False,
        "config_path": str(config.path or config_path()),
        "created_at": previous["created_at"] if previous else time.time(),
        "next_poll": 0,
    }
    store.save("workspace", ident, result)
    return result


def subscribe(workspace: dict, enabled: bool = True) -> dict:
    from ..activity.monitor import control

    if enabled:
        control(True)
    store = ActivityStore()
    with store.transaction() as db:
        current = store.get_in(db, "workspace", workspace["workspace_id"])
        if current is None:
            raise NotFound("workspace no longer exists")
        current.update(enabled=enabled, next_poll=0)
        store.put(db, "workspace", current["workspace_id"], current)
    return current


def snapshot(workspace: dict, *, after: str | None = None) -> dict:
    store = ActivityStore()
    workspace = store.get("workspace", workspace["workspace_id"]) or workspace
    page = store.query(
        target=workspace["target_id"],
        workspace=workspace["workspace_id"],
        after=after,
        max_bytes=96 * 1024,
        snapshot=("view", workspace["workspace_id"]),
    )
    view = page.pop("snapshot") or {"work": [], "observed_at": None, "state": "UNOBSERVED"}
    observed = view.get("observed_at")
    age = time.time() - observed if observed is not None else None
    health = store.get("observer", "health") or {}
    heartbeat = health.get("heartbeat")
    from ..activity.monitor import control

    fresh = (
        workspace.get("enabled", False)
        and control()["enabled"]
        and age is not None
        and 0 <= age <= 90
        and heartbeat is not None
        and (0 <= time.time() - heartbeat <= 90)
        and view.get("state") == "HEALTHY"
    )
    attention = attention_view(view, fresh)
    return {
        **page,
        "workspace": workspace,
        "work": view.get("work", []),
        "work_omitted": view.get("work_omitted", 0),
        "attention": {key: value for key, value in attention.items() if key != "issues"},
        "monitor": {
            **health,
            "state": view.get("state"),
            "error": view.get("error"),
            "action": view.get("action"),
            "age_seconds": age,
            "fresh": fresh,
            "enabled": workspace.get("enabled", False),
        },
        "source_gaps": [
            w["work_id"] for w in view.get("work", []) if work_freshness(w, fresh) != "FRESH"
        ],
        "diagnostics": {
            "config_path": workspace.get("config_path", str(config_path())),
            "state_path": str(state_dir()),
            "client": control_identity(),
        },
        "coverage": (
            "Recorded work with a known cwd under this root; external work is not discovered"
        ),
    }


def attention_view(view: dict, fresh: bool) -> dict:
    return project_attention(
        {
            "units": [
                {
                    "unit_id": w["work_id"],
                    "execution": {"state": w.get("execution", "UNKNOWN")},
                    "artifacts": {"state": w.get("artifacts", "UNCHECKED")},
                    "validation": {"state": w.get("validation", "NOT_RUN")},
                    "dependency": {"state": w.get("dependency", "NOT_APPLICABLE")},
                    "freshness": {
                        "state": work_freshness(w, fresh),
                        "observed_at": w.get("observed_at"),
                    },
                }
                for w in view.get("work", [])
                if not w.get("historical")
            ],
            "monitor_health": {
                "state": "PARTIAL"
                if view.get("work_omitted")
                else "HEALTHY"
                if fresh
                else "SUSPENDED",
                "observed_at": view.get("observed_at"),
            },
        }
    )


def work_freshness(work: dict, observer_fresh: bool) -> str:
    if not observer_fresh:
        return "STALE"
    if not work.get("campaign"):
        return "FRESH"
    sources = work.get("source_health", {})
    states = [
        s.get("freshness", "UNAVAILABLE")
        if 0 <= time.time() - s.get("observed_at", 0) <= 90
        else "STALE"
        for s in sources.values()
    ]
    for state in ("CONFLICT", "UNAVAILABLE", "STALE", "PARTIAL"):
        if state in states:
            return state
    return "FRESH" if sources.get("scheduler") and sources.get("artifacts") else "PARTIAL"


def inspect_request(
    workspace: dict,
    kind: str,
    path: str = ".",
    *,
    request_id: str | None = None,
    offset: int = 0,
    token: str | None = None,
    cached_only: bool = False,
) -> dict:
    if kind not in {"list", "preview", "tail", "refresh"} or offset < 0:
        raise InvalidArgument("invalid workspace inspection")
    store = ActivityStore()
    if request_id:
        request = store.get("inspection", request_id)
        if request is None or request["workspace_id"] != workspace["workspace_id"]:
            raise NotFound("inspection request unavailable")
        return request
    full = posixpath.normpath(posixpath.join(workspace["remote_root"], path))
    # Explicit navigation is allowed; the full remote path is always displayed.
    if not full.startswith("/") or "\x00" in full:
        raise InvalidArgument("invalid inspection path")
    key = hashlib.sha256(
        encoded([workspace["target_id"], workspace["workspace_id"], kind, full]).encode()
    ).hexdigest()
    cached = store.get("file", key)
    if token:
        if not cached or cached["capture_id"] != token or time.time() - cached["observed_at"] > 300:
            raise InvalidArgument("directory capture expired; refresh it")
        return file_page(cached, offset)
    if cached_only:
        if cached is None:
            raise NotFound("no cached file observation; reopen online to inspect this path")
        return {"request_id": None, "state": "DONE", "result": file_page(cached, offset)}
    if os.environ.get("REMOTESLURM_NO_DAEMON"):
        raise InvalidArgument(
            "inspection requires the local observer; cached workspace remains readable"
        )
    from ..activity.monitor import control

    if not control()["enabled"]:
        raise InvalidArgument(
            "monitoring is disabled; run `rslurm monitor enable` before inspection"
        )
    request = {
        "request_id": uuid.uuid4().hex,
        "workspace_id": workspace["workspace_id"],
        "target_id": workspace["target_id"],
        "host": workspace["host"],
        "kind": kind,
        "path": full,
        "key": key,
        "state": "QUEUED",
        "created_at": time.time(),
    }
    with store.transaction() as db:
        current = store.get_in(db, "workspace", workspace["workspace_id"])
        if not current or not current["enabled"]:
            raise InvalidArgument("workspace monitoring is disabled; attach an online view first")
        rows = list(db.execute("SELECT data FROM documents WHERE namespace='inspection'"))
        import json

        pending = [json.loads(row[0]) for row in rows if json.loads(row[0])["state"] == "QUEUED"]
        recent = [json.loads(row[0]) for row in rows if json.loads(row[0])["state"] == "DONE"]
        for old in pending:
            if old["key"] == key:
                return old
        for old in recent:
            if old["key"] == key and time.time() - old["completed_at"] < 5:
                return old
        if sum(r["host"] == workspace["host"] for r in pending) >= 32:
            raise InvalidArgument("inspection queue busy; retry after pending reads complete")
        store.put(db, "inspection", request["request_id"], request)
        # Retain completed receipts for five minutes, bounded by the cache budget.
        db.execute(
            "DELETE FROM documents WHERE namespace='inspection' AND updated<? "
            "AND json_extract(data,'$.state')!='QUEUED'",
            (time.time() - 300,),
        )
    from ..activity.model import EvidenceRef, Subject
    from ..activity.producer import CLIENT

    try:
        store.append(
            subject=Subject(workspace["target_id"], workspace["host"]),
            kind="file.inspect.requested",
            source="workspace.inspection",
            provenance="request",
            payload={"path": full, "kind": kind, "request_id": request["request_id"]},
            operation_id=request["request_id"],
            actor=CLIENT.get(),
            workspace=workspace["workspace_id"],
            evidence=(EvidenceRef("local_request", "inspection.queue", request["request_id"]),),
        )
    except Exception:
        request["activity_recorded"] = False
    return {**request, "cached": file_page(cached) if cached else None}


def file_page(captured: dict, offset: int = 0) -> dict:
    data = dict(captured)
    entries = data.pop("entries", [])
    page, size = [], 0
    for entry in entries[offset : offset + 200]:
        length = len(encoded(entry).encode())
        if size + length > 60 * 1024:
            break
        page.append(entry)
        size += length
    if "entries" in captured:
        data.update(
            entries=page,
            next_offset=offset + len(page) if offset + len(page) < len(entries) else None,
            token=captured["capture_id"],
            captured_count=len(entries),
        )
    return data
