"""Build the dashboard's state and activity feed from the local journal. No remote access."""

from __future__ import annotations

import hashlib
import posixpath
import time
from datetime import datetime
from typing import Any

from .. import slurm
from ..activity.discovery import human_bytes
from ..activity.producer import target_id, within
from ..activity.store import ActivityStore, encoded

STALE_AFTER = 90
MAX_ARRAY_CELLS = 2000
FAILED = {"FAILED", "OUT_OF_MEMORY", "TIMEOUT", "NODE_FAIL", "BOOT_FAIL", "DEADLINE"}


def clock(epoch: float | None) -> str:
    return datetime.fromtimestamp(epoch).strftime("%H:%M") if epoch else "?"


def walltime(seconds: float | None) -> str:
    if seconds is None:
        return "?"
    seconds = int(seconds)
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, secs = divmod(rest, 60)
    text = f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{days}-{text}" if days else text


def pending_reason(reason: str | None) -> str | None:
    if not reason:
        return None
    for prefix, text in slurm._PENDING_REASONS:
        if reason.startswith(prefix):
            return text
    return None


def hint(job: dict) -> str | None:
    """One or two plain sentences built only from scheduler facts we actually hold."""
    state = job["state"]
    if job.get("array") and job["array"].get("exits"):
        failed = sorted(job["array"]["exits"], key=int)
        first = failed[0]
        shown = ", ".join(f"task {t} (exit {job['array']['exits'][t]})" for t in failed[:3])
        more = f" and {len(failed) - 3} more" if len(failed) > 3 else ""
        return (
            f"{len(failed)} of {job['array']['n']} tasks failed: {shown}{more}. "
            f"Open task {first} for its logs."
        )
    if state == "OUT_OF_MEMORY":
        if job.get("maxrss"):
            return (
                f"Killed for exceeding its {job.get('mem') or '?'} memory request "
                f"(peak {job['maxrss']}). The error log shows where it was killed."
            )
        return (
            f"Killed for exceeding its {job.get('mem') or '?'} memory request; "
            "peak usage is unknown. The error log shows where it was killed."
        )
    if state == "TIMEOUT":
        return (
            f"Reached its {walltime(job.get('timelimit'))} time limit"
            + (f" after {walltime(job.get('elapsed'))}" if job.get("elapsed") else "")
            + ". The output log shows how far it got."
        )
    if state == "NODE_FAIL":
        return (
            "A node failed under the job; the job itself did not fail. Resubmitting usually works."
        )
    if state in {"FAILED", "BOOT_FAIL", "DEADLINE"}:
        return (
            f"Exited with code {job['exit']}. The error log usually says why."
            if job.get("exit")
            else "Failed; the exit code is unknown. The error log usually says why."
        )
    if state == "UNKNOWN" and job.get("vanishedAt"):
        return (
            f"Left the queue at {clock(job['vanishedAt'])}. Accounting has not yet reported "
            "how it ended."
        )
    if state == "PENDING":
        return pending_reason(job.get("reason"))
    return None


class Journal:
    """Reads one consistent picture of hosts, jobs and activity for the configured scope."""

    def __init__(self, config: Any, hosts: list[Any], *, demo: bool = False):
        self.config, self.hosts, self.demo = config, hosts, demo
        self.store = ActivityStore()
        self.targets = {target_id(h): h for h in hosts}

    # ------------------------------------------------------------------ observer
    def observer(self) -> dict:
        from ..activity.monitor import control
        from ..daemon import daemon_available, daemon_status
        from ..identity import control_identity

        enabled = control()["enabled"]
        health = self.store.get("observer", "health") or {}
        running, build_ok = False, True
        try:
            running = daemon_available()
            if running:
                build_ok = daemon_status().get("build_id") == control_identity()["build_id"]
        except Exception:
            running = False
        heartbeat = health.get("heartbeat")
        note = None
        if not enabled:
            note = "Monitoring is turned off, so nothing is being read. Run: rslurm monitor enable"
        elif not running:
            note = (
                "The background observer is not running, so this page is not updating. "
                "Restart: rslurm dash"
            )
        elif not build_ok:
            note = (
                "The background observer is running an older remoteslurm build. "
                "Restart it with: rslurm dash --restart-daemon"
            )
        elif heartbeat is None or time.time() - heartbeat > STALE_AFTER:
            note = (
                "The background observer has stopped reporting. "
                "Restart: rslurm dash --restart-daemon"
            )
        return {
            "running": running,
            "enabled": enabled,
            "heartbeat": heartbeat,
            "build_ok": build_ok,
            "note": note,
        }

    # ------------------------------------------------------------------ state
    def state(self) -> dict:
        now = time.time()
        workspaces = [
            w
            for w in self.store.documents("workspace", limit=1000)
            if w["target_id"] in self.targets
        ]
        views = {
            w["workspace_id"]: self.store.get("view", w["workspace_id"]) or {} for w in workspaces
        }
        registered: dict[tuple[str, str], dict] = {}
        for work in self.store.documents("work", limit=1000):
            if work.get("target_id") in self.targets and work.get("job_id"):
                key = (work["target_id"], str(work["job_id"]))
                if work.get("submit_time", 0) >= registered.get(key, {}).get("submit_time", 0):
                    registered[key] = work
        hosts, jobs = [], []
        for ident, hc in self.targets.items():
            disc = self.store.get("discovery", ident) or {}
            host_views = [views[w["workspace_id"]] for w in workspaces if w["target_id"] == ident]
            host_spaces = [w for w in workspaces if w["target_id"] == ident and w["enabled"]]
            if disc.get("observed_at"):
                observed, coverage = disc["observed_at"], "all"
                error, action = disc.get("error"), disc.get("action")
            else:
                healthy = [v["observed_at"] for v in host_views if v.get("state") == "HEALTHY"]
                observed = max(healthy) if healthy else None
                coverage = "tracked"
                errs = [v for v in host_views if v.get("error")]
                error = disc.get("error") or (errs[0]["error"] if errs else None)
                action = disc.get("action") or (errs[0].get("action") if errs else None)
            if error == "not_connected":  # name the configured host, not its ssh alias
                action = f"Reconnect: rslurm connect {hc.name}"
            hosts.append(
                {
                    "id": hc.name,
                    "login": None,
                    "observedAt": observed,
                    "error": error,
                    "action": action,
                    "accounting": disc.get("accounting", "unknown"),
                    "accountingSince": disc.get("accounting_at"),
                    "alloc": None,
                    "coresPerNode": None,
                    "coverage": coverage,
                }
            )
            roots = [w["remote_root"] for w in host_spaces]
            seen: set[str] = set()
            for raw in disc.get("jobs", {}).values():
                job = self.discovered(hc.name, raw, disc)
                self.join(job, registered.get((ident, job["id"])), roots)
                seen.add(job["id"])
                jobs.append(job)
            # Registered work this host's discovery does not hold (no discovery, or older).
            for view in host_views:
                for work in view.get("work", []):
                    jid = str(work.get("job_id") or "")
                    if not jid or jid in seen:
                        continue
                    seen.add(jid)
                    jobs.append(self.tracked_only(hc.name, work, view, roots))
        for job in jobs:
            job["hint"] = hint(job)
        jobs.sort(key=lambda j: -(j.get("submit") or 0))
        covered = [h["id"] for h in hosts if h["coverage"] == "all"]
        coverage = (
            "Every job you own on " + ", ".join(covered) + " (squeue and 7 days of sacct)"
            if covered
            else "Only work submitted or adopted through remoteslurm"
        )
        if covered and len(covered) < len(hosts):
            coverage += "; registered work only elsewhere"
        return {
            "now": now,
            "journal": self.journal_id(),
            "demo": False,
            "observer": self.observer(),
            "hosts": hosts,
            "jobs": jobs,
            "coverage": coverage + ".",
        }

    def journal_id(self) -> str:
        with self.store.connection() as db:
            return db.execute("SELECT value FROM meta WHERE key='journal'").fetchone()[0]

    @staticmethod
    def discovered(host: str, raw: dict, disc: dict) -> dict:
        state = raw.get("state", "UNKNOWN")
        terminal = state in slurm.TERMINAL_STATES
        maxrss = human_bytes(raw.get("maxrss")) if raw.get("maxrss") is not None else None
        job: dict[str, Any] = {
            "key": f"{host}:{raw['id']}",
            "id": raw["id"],
            "name": raw.get("name") or raw["id"],
            "host": host,
            "partition": raw.get("partition"),
            "account": raw.get("account"),
            "user": raw.get("user"),
            "tracked": False,
            "workspace": None,
            "state": state,
            "reason": raw.get("reason") if state == "PENDING" else None,
            "submit": raw.get("submit"),
            "start": raw.get("start"),
            "end": raw.get("end") if terminal else None,
            "estStart": raw.get("est_start") if state == "PENDING" else None,
            "timelimit": raw.get("timelimit"),
            "elapsed": raw.get("elapsed"),
            "nodes": raw.get("nodes"),
            "nodelist": raw.get("nodelist"),
            "cpus": raw.get("cpus"),
            "mem": raw.get("mem"),
            "maxrss": maxrss,
            "exit": raw.get("exit") if terminal else None,
            "exitSource": "sacct" if terminal and raw.get("exit") else None,
            "workdir": raw.get("workdir"),
            "stdout": raw.get("stdout"),
            "stderr": raw.get("stderr"),
            "pathsGuessed": False,
            "source": raw.get("source", "squeue"),
            "observedAt": raw.get("seen_at")
            if raw.get("source") == "squeue"
            else disc.get("accounting_at"),
            "array": None,
            "vanishedAt": raw.get("vanished_at"),
        }
        if raw.get("array"):
            tasks = raw.get("tasks", {})
            order = sorted(tasks, key=int)
            if len(order) > MAX_ARRAY_CELLS:
                rank = {"FAILED": 0, "OUT_OF_MEMORY": 0, "TIMEOUT": 0, "RUNNING": 1, "PENDING": 2}
                order = sorted(
                    sorted(order, key=lambda t: rank.get(tasks[t], 3))[:MAX_ARRAY_CELLS], key=int
                )
            info = raw.get("task_info", {})
            job["array"] = {
                "n": len(tasks),
                "ids": [int(t) for t in order],
                "states": [tasks[t] for t in order],
                "limit": raw.get("limit"),
                "pendingReason": raw.get("reason"),
                "exits": {
                    t: i["exit"]
                    for t, i in info.items()
                    if i.get("exit") and tasks.get(t) in slurm.ARRAY_FAILED_STATES
                },
                "taskInfo": {
                    t: {
                        "elapsed": i.get("elapsed"),
                        "maxrss": human_bytes(i["maxrss"]) if i.get("maxrss") is not None else None,
                        "node": i.get("node"),
                        "end": i.get("end"),
                    }
                    for t, i in info.items()
                },
                "pathsTask": raw.get("paths_task"),
            }
            job["reason"] = raw.get("reason") if state in {"PENDING", "RUNNING"} else None
            job["exit"], job["exitSource"] = None, None
        return job

    @staticmethod
    def join(job: dict, work: dict | None, roots: list[str]) -> None:
        if work is not None:
            job["tracked"] = True
            job["stdout"] = work.get("stdout_path") or job["stdout"]
            job["stderr"] = work.get("stderr_path") or job["stderr"]
            job["workdir"] = job["workdir"] or work.get("workdir")
            if not job["name"] or job["name"] == job["id"]:
                job["name"] = work.get("name") or job["name"]
        for name in ("stdout", "stderr"):
            path = job[name]
            if path and not path.startswith("/") and job["workdir"]:
                job[name] = posixpath.join(job["workdir"], path)
        if not job["stdout"] and job["workdir"]:
            pattern = "slurm-%A_%a.out" if job.get("array") else f"slurm-{job['id']}.out"
            job["stdout"] = posixpath.join(job["workdir"], pattern)
            job["pathsGuessed"] = True
        job["workspace"] = next((r for r in roots if within(job["workdir"], r)), None)

    @staticmethod
    def tracked_only(host: str, work: dict, view: dict, roots: list[str]) -> dict:
        sched = work.get("scheduler") or {}
        state = work.get("execution") or "UNKNOWN"
        terminal = state in slurm.TERMINAL_STATES
        code = sched.get("exit_code")
        job = {
            "key": f"{host}:{work['job_id']}",
            "id": str(work["job_id"]),
            "name": work.get("name") or str(work["job_id"]),
            "host": host,
            "partition": None,
            "account": None,
            "user": None,
            "tracked": True,
            "workspace": None,
            "state": state,
            "reason": None,
            "submit": work.get("submit_time"),
            "start": None,
            "end": None,
            "estStart": None,
            "timelimit": None,
            "elapsed": slurm.walltime_to_seconds(sched.get("elapsed"))
            if sched.get("elapsed")
            else None,
            "nodes": None,
            "nodelist": None,
            "cpus": sched.get("allocated_cpus"),
            "mem": None,
            "maxrss": None,
            "exit": f"{code}:0" if terminal and code is not None and code < 128 else None,
            "exitSource": "sacct" if terminal and code is not None and code < 128 else None,
            "workdir": work.get("workdir"),
            "stdout": work.get("stdout_path"),
            "stderr": work.get("stderr_path"),
            "pathsGuessed": False,
            "source": "retained" if sched.get("retained") else "registry",
            "observedAt": work.get("observed_at") or view.get("observed_at"),
            "array": None,
            "vanishedAt": None,
        }
        Journal.join(job, None, roots)
        return job

    # ------------------------------------------------------------------ activity
    def activity(self, after: str | None = None, limit: int = 200) -> dict:
        names = {h.name for h in self.hosts}
        page = self.store.query(
            after=after, limit=1000 if after is None else 500, max_bytes=1024 * 1024
        )
        items = [item for e in page["events"] if (item := feed_item(e, names)) is not None]
        if after is None:
            items = items[-limit:]
        return {
            "events": items,
            "cursor": page["resync_cursor"] if page["resync_required"] else page["next_cursor"],
            "resync": page["resync_required"],
            "more": page["has_more"],
        }


VERB = {
    "COMPLETED": ("ok", "completed"),
    "FAILED": ("fail", "failed"),
    "OUT_OF_MEMORY": ("fail", "ran out of memory"),
    "TIMEOUT": ("fail", "hit its time limit"),
    "NODE_FAIL": ("fail", "lost its node"),
    "BOOT_FAIL": ("fail", "failed to boot"),
    "DEADLINE": ("fail", "missed its deadline"),
    "PREEMPTED": ("warn", "was preempted"),
    "CANCELLED": ("info", "was cancelled"),
    "RUNNING": ("run", "started"),
    "PENDING": ("pend", "queued"),
}


def feed_item(event: dict, hosts: set[str]) -> dict | None:
    subject, payload, kind = event["subject"], event["payload"], event["kind"]
    host = subject["host_alias"]
    if host not in hosts:
        return None
    try:
        t = datetime.fromisoformat(event["recorded_at"]).timestamp()
    except (KeyError, ValueError):
        return None
    base = {
        "seq": event["seq"],
        "id": event["event_id"],
        "t": t,
        "host": host,
        "job": None,
        "detail": None,
        "alert": False,
    }
    if kind == "job.state.changed":
        name = payload.get("name") or payload.get("job_id")
        state = payload.get("state")
        base["job"] = payload.get("job_id")
        if payload.get("new_failed_tasks"):
            n = payload["new_failed_tasks"]
            return {
                **base,
                "kind": "fail",
                "alert": True,
                "text": f"{name}: {n} task{'s' if n != 1 else ''} failed",
                "detail": f"{payload.get('failed_tasks')} of {payload.get('tasks')} failed so far",
            }
        tone, verb = VERB.get(state, ("info", f"is {str(state).lower()}"))
        if state == "PENDING" and payload.get("from"):
            tone, verb = "pend", "is waiting again"
        detail = None
        if state in slurm.TERMINAL_STATES:
            detail = f"exit {payload['exit_code']}" if payload.get("exit_code") else None
            if payload.get("tasks"):
                detail = f"{payload['tasks']} tasks" + (
                    f", {payload['failed_tasks']} failed" if payload.get("failed_tasks") else ""
                )
        elif state == "PENDING":
            detail = payload.get("reason")
        return {
            **base,
            "kind": tone,
            "text": f"{name} {verb}",
            "detail": detail,
            "alert": state in slurm.TERMINAL_STATES,
        }
    if kind == "job.registered":
        return {
            **base,
            "kind": "pend",
            "job": payload.get("job_id"),
            "text": f"{payload.get('name') or payload.get('job_id')} submitted",
            "detail": "through remoteslurm",
        }
    if kind == "jobs.discovered":
        return {
            **base,
            "kind": "info",
            "text": f"Caught up with {host} after a pause"
            if payload.get("resumed")
            else f"Started watching every job on {host}",
            "detail": payload.get("summary"),
        }
    if kind == "discovery.health":
        if payload.get("state") == "HEALTHY":
            return {**base, "kind": "info", "text": f"Reading {host} again"}
        return {
            **base,
            "kind": "warn",
            "text": f"Can't read {host}",
            "detail": f"not connected · reconnect: rslurm connect {host}"
            if payload.get("error") == "not_connected"
            else payload.get("summary") or payload.get("error"),
        }
    if kind == "discovery.accounting":
        if payload.get("state") == "ok":
            return {**base, "kind": "info", "text": f"Accounting is answering again on {host}"}
        return {
            **base,
            "kind": "warn",
            "text": f"Accounting (sacct) stopped answering on {host}",
            "detail": "finished jobs show scheduler state only",
        }
    return None


def digest(value: Any) -> str:
    """A change detector for SSE pushes that ignores the clock fields."""
    data = {k: v for k, v in value.items() if k != "now"}
    if isinstance(data.get("observer"), dict):
        data["observer"] = {k: v for k, v in data["observer"].items() if k != "heartbeat"}
    return hashlib.sha256(encoded(data).encode()).hexdigest()
