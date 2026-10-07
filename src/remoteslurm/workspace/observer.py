"""Workspace collection run by the existing job-monitor owner. Remote reads only."""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from types import SimpleNamespace
from typing import Any

from ..activity.model import EvidenceRef, Subject
from ..activity.producer import CLIENT, record_job, target_id, within
from ..activity.store import ActivityStore, encoded, stamp
from ..errors import RemoteSlurmError
from .service import file_page


class WorkspaceObserver:
    def __init__(self, get_cluster: Any, get_host: Any):
        self.store = ActivityStore()
        self.get_cluster, self.get_host = get_cluster, get_host

    def active(self) -> bool:
        from ..activity.monitor import control

        if not control()["enabled"]:
            return False
        return any(w["enabled"] for w in self.store.documents("workspace", limit=1000))

    def tick(self) -> None:
        from ..activity.monitor import control
        from ..identity import control_identity

        now = time.time()
        self.store.save(
            "observer",
            "health",
            {"heartbeat": now, "pid": os.getpid(), "build_id": control_identity()["build_id"]},
        )
        if not control()["enabled"]:
            return
        spaces = self.store.documents("workspace", limit=1000)
        due = sorted(
            (w for w in spaces if w["enabled"] and w["next_poll"] <= now),
            key=lambda w: w["next_poll"],
        )
        if due:
            self.observe(due[0])  # Fair persisted continuation; no catch-up burst.
        with self.store.connection() as db:
            row = db.execute(
                "SELECT data FROM documents WHERE namespace='inspection' "
                "AND json_extract(data,'$.state')='QUEUED' ORDER BY updated LIMIT 1"
            ).fetchone()
        if row:
            self.inspect(json.loads(row[0]))

    def observe(self, workspace: dict) -> None:
        from ..job_monitor import shared_collect

        key = workspace["workspace_id"]
        previous = self.store.get("view", key) or {}
        now = time.time()
        value: dict[str, Any] = {"state": "HEALTHY", "observed_at": now, "work": []}
        try:
            host = self.get_host(workspace["host"])
            if target_id(host) != workspace["target_id"]:
                raise RemoteSlurmError("configured target changed; attach a new workspace")
            cluster = self.get_cluster(workspace["host"])
            if target_id(cluster.host) != workspace["target_id"]:
                raise RemoteSlurmError("cached connection target changed; reconnect")
            self.check_anchor(cluster, workspace)
            from ..activity.recovery import task_record

            tasks = sorted(
                (
                    t
                    for t in self.store.documents("task", limit=1000)
                    if t["target_id"] == workspace["target_id"]
                    and within(t.get("workdir"), workspace["remote_root"])
                    and t.get("created_at", 0) >= workspace.get("min_registered_at", 0)
                ),
                key=lambda t: t["task_id"],
            )
            if tasks:
                task = next(
                    (t for t in tasks if t["task_id"] > workspace.get("task_after", "")), tasks[0]
                )
                remote = cluster.call(
                    "task_read",
                    task_dir=cluster.host.task_dir,
                    task_id=task["task_id"],
                    _timeout=10,
                )
                if remote["available"]:
                    task_record(cluster, remote["record"], store=self.store)
                workspace["task_after"] = task["task_id"]
            # Import pre-view registrations (including durable tasks) without remote recovery.
            token = CLIENT.set("observer")
            try:
                known = {
                    (w["job_id"], w.get("submit_time"))
                    for w in self.store.documents("work", limit=1000)
                    if w["target_id"] == workspace["target_id"]
                }
                for rec in cluster.registry.all()[-1000:]:
                    if (
                        (rec.job_id, rec.submit_time) not in known
                        and within(rec.workdir, workspace["remote_root"])
                        and rec.submit_time >= workspace.get("min_registered_at", 0)
                    ):
                        record_job(
                            cluster, SimpleNamespace(job_id=rec.job_id, recorded=True), record=rec
                        )
            finally:
                CLIENT.reset(token)
            all_work = self.store.documents("work", limit=1000)
            work = [
                w
                for w in all_work
                if w["target_id"] == workspace["target_id"]
                and within(w.get("workdir"), workspace["remote_root"])
                and w.get("observed_at", 0) >= workspace.get("min_registered_at", 0)
            ]
            canonical = {w["job_id"] for w in work if w.get("attempt_id") and w.get("job_id")}
            work = [w for w in work if w.get("attempt_id") or w["job_id"] not in canonical]
            before = {w["work_id"]: w for w in previous.get("work", [])}
            work.sort(
                key=lambda w: (
                    0
                    if before.get(w["work_id"], {}).get("execution") in {"RUNNING", "PENDING"}
                    else 2
                    if before.get(w["work_id"], {}).get("scheduler", {}).get("terminal")
                    else 1,
                    -w.get("submit_time", 0),
                )
            )
            # Terminal records retire from scheduler polling. Explicit refresh can recheck them.
            ids = sorted(
                {
                    w["job_id"]
                    for w in work[:25]
                    if w.get("job_id")
                    if not w.get("campaign")
                    if not before.get(w["work_id"], {}).get("scheduler", {}).get("terminal")
                    or workspace.get("force_refresh")
                }
            )
            observations = shared_collect(cluster, ids) if ids else {}
            counts: dict[str, int] = {}
            for w in work:
                counts[w["job_id"]] = counts.get(w["job_id"], 0) + 1
            for item in work[:25]:
                jid = item["job_id"]
                old = before.get(item["work_id"], {})
                if item.get("campaign") or not jid:
                    value["work"].append(item)
                    continue
                if counts[jid] > 1:
                    item = {**item, "execution": "UNKNOWN", "identity_conflict": True}
                else:
                    prior = old.get("scheduler", {})
                    observation = observations.get(jid, {})
                    retained = False
                    if observation.get("state", "UNKNOWN") == "UNKNOWN" and prior.get("terminal"):
                        observation = dict(prior, retained=True)
                        retained = True
                    elif set(prior.get("seen_tasks", [])) - set(observation.get("seen_tasks", [])):
                        observation = dict(
                            observation,
                            state="UNKNOWN",
                            terminal=False,
                            seen_tasks=sorted(
                                set(prior["seen_tasks"]) | set(observation.get("seen_tasks", []))
                            ),
                        )
                    item = {
                        **item,
                        "execution": observation.get("state", "UNKNOWN"),
                        "scheduler": observation,
                        "observed_at": old.get("observed_at") if retained else now,
                        "identity_basis": "local registration; scheduler job id",
                    }
                value["work"].append(item)
            value["work_omitted"] = max(0, len(work) - 25)
            value["registry_coverage"] = "partial" if len(all_work) == 1000 else "recorded"
        except Exception as exc:
            value = {
                **previous,
                "state": "SUSPENDED",
                "error": getattr(exc, "code", type(exc).__name__),
                "action": getattr(exc, "action", None)
                or f"check connection with rslurm connect {workspace['host']}",
            }
        subject = Subject(workspace["target_id"], workspace["host"])

        def comparison(v: dict) -> list:
            return [
                v.get("state"),
                v.get("error"),
                [[w["work_id"], w.get("execution")] for w in v.get("work", [])],
            ]

        if comparison(previous) != comparison(value):
            self.store.append(
                subject=subject,
                kind="workspace.observed",
                source="workspace.observer",
                payload={"state": value["state"], "work_count": len(value.get("work", []))},
                workspace=key,
                freshness="FRESH" if value["state"] == "HEALTHY" else "UNAVAILABLE",
                evidence=(
                    EvidenceRef(
                        "scheduler",
                        "squeue/sacct",
                        uuid.uuid4().hex,
                        limitations=("known local registrations only",),
                    ),
                ),
                documents=(("view", key, value),),
            )
        else:
            self.store.save("view", key, value)
        from ..activity.attention import PROJECTION_VERSION
        from .service import attention_view, work_freshness

        advisory = attention_view(value, value["state"] == "HEALTHY")
        inputs = {
            "state": value["state"],
            "work_omitted": value.get("work_omitted", 0),
            "units": [
                {
                    "work_id": w["work_id"],
                    "attempt_id": w.get("attempt_id"),
                    "execution": w.get("execution"),
                    "artifacts": w.get("artifacts"),
                    "validation": w.get("validation"),
                    "dependency": w.get("dependency"),
                    "freshness": work_freshness(w, value["state"] == "HEALTHY"),
                }
                for w in value.get("work", [])
                if not w.get("historical")
            ],
        }
        digest = hashlib.sha256(encoded(inputs).encode()).hexdigest()
        previous_attention = self.store.get("attention", key) or {}
        if digest != previous_attention.get("input_digest"):
            self.store.append(
                subject=subject,
                kind="attention.changed",
                source="workspace.attention",
                provenance="derived",
                projection_version=PROJECTION_VERSION,
                payload={
                    "kind": advisory["kind"],
                    "reason": advisory["reason"],
                    "input_digest": digest,
                    "authorization": advisory["authorization"],
                },
                workspace=key,
                evidence=(EvidenceRef("local_request", "captured_workspace_axes", digest),),
                documents=(
                    ("attention", key, {"input_digest": digest, "kind": advisory["kind"]}),
                    ("attention_inputs", digest, inputs),
                ),
            )
        with self.store.transaction() as db:
            current = self.store.get_in(db, "workspace", key)
            if current:
                current["next_poll"] = now + (30 if value["state"] == "HEALTHY" else 60)
                current["force_refresh"] = False
                current["task_after"] = workspace.get("task_after", "")
                self.store.put(db, "workspace", key, current)  # A concurrent stop wins.

    def inspect(self, request: dict) -> None:
        now = time.time()
        workspace = self.store.get("workspace", request["workspace_id"])
        try:
            if not workspace or not workspace["enabled"] or now - request["created_at"] > 30:
                raise RemoteSlurmError("inspection expired or workspace stopped")
            host = self.get_host(workspace["host"])
            if target_id(host) != request["target_id"]:
                raise RemoteSlurmError("inspection target changed")
            cluster = self.get_cluster(workspace["host"])
            if target_id(cluster.host) != request["target_id"]:
                raise RemoteSlurmError("cached inspection target changed")
            self.check_anchor(cluster, workspace)
            if request["kind"] == "list":
                result = cluster.call("browse", path=request["path"], _timeout=10)
            elif request["kind"] == "refresh":
                workspace["next_poll"] = 0
                with self.store.transaction() as db:
                    current = self.store.get_in(db, "workspace", workspace["workspace_id"])
                    if current:
                        current["next_poll"] = 0
                        current["force_refresh"] = True
                        self.store.put(db, "workspace", current["workspace_id"], current)
                result = {"path": request["path"], "refresh_requested": True}
            else:
                result = cluster.call(
                    "workspace_read",
                    path=request["path"],
                    max_bytes=65536,
                    tail=200 if request["kind"] == "tail" else None,
                    _timeout=10,
                )
                if result.get("binary"):
                    result.pop("content_b64", None)
                    result["note"] = "binary file; preview is metadata only"
            captured = {
                **result,
                "capture_id": uuid.uuid4().hex,
                "observed_at": now,
                "workspace_id": workspace["workspace_id"],
                "key": request["key"],
            }
            old = self.store.get("file", request["key"])
            if request["kind"] == "tail":
                if old and old.get("file_identity") != captured.get("file_identity"):
                    captured["gap"] = "log replaced or rotated; showing its current tail"
                elif old and captured.get("size", 0) < old.get("size", 0):
                    captured["gap"] = "log truncated; showing its current tail"
                elif captured.get("truncated"):
                    captured["gap"] = "bounded tail; earlier bytes are omitted"
            with self.store.transaction() as db:
                # Preview and metadata caches have an independent 64 MiB combined bound.
                self.store.put(db, "file", request["key"], captured)
                total = db.execute(
                    "SELECT coalesce(sum(length(data)),0) FROM documents WHERE namespace='file'"
                ).fetchone()[0]
                for row in db.execute(
                    "SELECT key,length(data) FROM documents WHERE namespace='file' ORDER BY updated"
                ):
                    if total <= 64 * 1024 * 1024:
                        break
                    db.execute("DELETE FROM documents WHERE namespace='file' AND key=?", (row[0],))
                    total -= row[1]
            if (
                old is None
                or hashlib.sha256(
                    encoded({k: v for k, v in result.items() if k != "observed_at"}).encode()
                ).hexdigest()
                != hashlib.sha256(
                    encoded(
                        {
                            k: v
                            for k, v in old.items()
                            if k
                            not in {
                                "capture_id",
                                "observed_at",
                                "workspace_id",
                                "key",
                                "gap",
                            }
                        }
                    ).encode()
                ).hexdigest()
            ):
                self.store.append(
                    subject=Subject(workspace["target_id"], workspace["host"]),
                    kind="files.observed",
                    source="workspace.inspect",
                    workspace=workspace["workspace_id"],
                    payload={
                        "path": request["path"],
                        "kind": request["kind"],
                        "partial": result.get("partial", False),
                    },
                    observed_at=stamp(now),
                    operation_id=request["request_id"],
                    correlation_id=request["request_id"],
                    causation_id=request["request_id"],
                    actor="observer",
                    evidence=(EvidenceRef("filesystem", request["path"], captured["capture_id"]),),
                )
            request.update(state="DONE", result=file_page(captured))
        except Exception as exc:
            request.update(
                state="ERROR",
                error=getattr(exc, "code", type(exc).__name__),
                action=getattr(exc, "action", None),
                result=None,
            )
        request["completed_at"] = time.time()
        self.store.save("inspection", request["request_id"], request)
        try:
            self.store.append(
                subject=Subject(request["target_id"], request["host"]),
                kind="file.inspect.returned"
                if request["state"] == "DONE"
                else "file.inspect.failed",
                source="workspace.inspection",
                payload={
                    "path": request["path"],
                    "kind": request["kind"],
                    "state": request["state"],
                    "error": request.get("error"),
                },
                operation_id=request["request_id"],
                correlation_id=request["request_id"],
                actor="observer",
                workspace=request["workspace_id"],
                freshness="FRESH" if request["state"] == "DONE" else "UNAVAILABLE",
            )
        except Exception:
            pass

    @staticmethod
    def check_anchor(cluster: Any, workspace: dict) -> None:
        resolved = cluster.call("workspace_anchor", path=workspace["source_root"], _timeout=10)
        if (
            resolved["anchor"] != workspace["anchor"]
            or resolved["path"] != workspace["remote_root"]
        ):
            raise RemoteSlurmError(
                "remote endpoint or workspace root changed",
                action="reopen the online view to attach to the current root; "
                "old state remains cached",
            )
