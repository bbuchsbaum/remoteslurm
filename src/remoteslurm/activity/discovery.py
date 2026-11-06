"""Read-only discovery of every job the user owns on a host, whatever submitted it.

One ``squeue -u $USER`` per host every 30 s and one windowed ``sacct -X`` every 60 s, plus
bounded enrichment (``scontrol show job`` for log paths, ``sacct -j`` with steps for MaxRSS).
Every call goes through the daemon's per-host read budget. Nothing here submits, cancels,
adopts, or writes to the cluster or the job registry; results live in the local journal
(``discovery`` documents and ``job.state.changed`` events).

A host is discovered while it has an enabled workspace (the user attached a view) or while a
dashboard holds a lease on it (``discover`` documents, renewed by ``rslurm dash``).
"""

from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any

from .. import slurm
from ..errors import RemoteSlurmError
from .model import EvidenceRef, Subject
from .producer import target_id
from .store import ActivityStore

SQUEUE_INTERVAL = 30
SACCT_INTERVAL = 60
RETRY_AFTER_BUDGET = 5
FIRST_WINDOW = 7 * 86400
KEEP_FINISHED = 7 * 86400
MAX_JOBS = 1000
MAX_ENRICH_STEPS = 50
MAX_TASK_INFO = 200  # failed-task details kept per array; counts stay exact
GAP_REBASELINE = 600  # unobserved this long: catch up quietly instead of replaying news
MIN_FIRST_WINDOW = 6 * 3600
LEASE_SECONDS = 120
# squeue fields beyond slurm.SQUEUE_FORMAT: allocated CPUs, requested memory, (expected) end.
EXTRA_FIELDS = ["cpus", "min_memory", "end_time"]
SQUEUE_FORMAT = slurm.SQUEUE_FORMAT + "|%C|%m|%e"
_COLLAPSED = re.compile(r"^(\d+)_\[([^\]]*)\]$")
_THROTTLE = re.compile(r"%(\d+)")


def parse_time(value: str | None) -> float | None:
    """Slurm prints local wall-clock times without a zone; interpret them in local time."""
    if not value or value in {"N/A", "Unknown", "None", "NONE", "(null)"}:
        return None
    try:
        return datetime.fromisoformat(value).timestamp()
    except ValueError:
        return None


def parse_squeue(stdout: str) -> tuple[list[dict[str, str]], dict[str, int]]:
    """Rows from :func:`slurm.parse_squeue` plus the extra columns; array throttles by base."""
    rows: list[dict[str, str]] = []
    throttles: dict[str, int] = {}
    width = len(slurm.SQUEUE_FIELDS)
    for line in stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        base_part, extra = parts[:width], parts[width:]
        extras = dict(zip(EXTRA_FIELDS, extra + [""] * len(EXTRA_FIELDS), strict=False))
        collapsed = _COLLAPSED.match(base_part[0]) if base_part else None
        if collapsed:
            limit = _THROTTLE.search(collapsed.group(2))
            if limit:
                throttles[collapsed.group(1)] = int(limit.group(1))
        for row in slurm.parse_squeue("|".join(base_part)):
            row.update(extras)
            rows.append(row)
    return rows, throttles


def _int(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _mem(value: str | None) -> str | None:
    """'4000M' / '32G' / '1000Mn' -> a short human size; '' -> None."""
    raw = (value or "").strip()
    if not raw or raw in {"0", "0M", "0n"}:
        return None
    return human_bytes(slurm._parse_mem(raw))


def human_bytes(n: int | None) -> str | None:
    if n is None:
        return None
    value = float(n)
    for unit in ("B", "K", "M", "G", "T"):
        if value < 1024 or unit == "T":
            if unit == "B":
                return f"{value:.0f}B"
            return f"{value:.0f}{unit}" if value >= 100 else f"{value:.1f}{unit}".replace(".0", "")
        value /= 1024
    return None


def _clean(row: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    return {k: row[k] for k in keys if row.get(k) not in (None, "", [], {})}


class DiscoveryObserver:
    """Owned by the daemon's ``JobMonitor``; one host observation per tick at most."""

    def __init__(self, get_cluster: Any, get_host: Any):
        self.store = ActivityStore()
        self.get_cluster, self.get_host = get_cluster, get_host

    # ------------------------------------------------------------------ enrollment
    def hosts(self) -> list[str]:
        from .monitor import control

        if not control()["enabled"]:
            return []
        now = time.time()
        names = {w["host"] for w in self.store.documents("workspace", limit=1000) if w["enabled"]}
        names |= {
            d["host"]
            for d in self.store.documents("discover", limit=1000)
            if d.get("lease_until", 0) >= now
        }
        return sorted(names)

    def active(self) -> bool:
        return bool(self.hosts())

    def tick(self) -> None:
        now = time.time()
        due = []
        for name in self.hosts():
            try:
                host = self.get_host(name)
            except Exception:
                continue
            doc = self.store.get("discovery", target_id(host)) or {}
            if doc.get("next_poll", 0) <= now:
                due.append((doc.get("next_poll", 0), name, host))
        if due:
            due.sort(key=lambda item: item[0])
            _, name, host = due[0]
            self.observe(name, host)

    # ------------------------------------------------------------------ observation
    def observe(self, name: str, host: Any, *, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        ident = target_id(host)
        previous = self.store.get("discovery", ident) or {}
        doc: dict[str, Any] = {
            "host": name,
            "target_id": ident,
            "jobs": {k: dict(v) for k, v in previous.get("jobs", {}).items()},
            "observed_at": previous.get("observed_at"),
            "accounting_at": previous.get("accounting_at"),
            "accounting": previous.get("accounting", "unknown"),
            "accounting_error": previous.get("accounting_error"),
            "baseline": previous.get("baseline", False),
            "baseline_at": previous.get("baseline_at"),
            "error": None,
            "action": None,
            "attempted_at": now,
        }
        try:
            cluster = self.get_cluster(name)
            if target_id(cluster.host) != ident:
                raise RemoteSlurmError("cached connection target changed; reconnect")
            queue = cluster.call("squeue", format=SQUEUE_FORMAT, _timeout=25)
            if queue.get("rc") or queue.get("stdout_truncated"):
                raise RemoteSlurmError(
                    "squeue failed or was truncated",
                    action=(queue.get("stderr") or "").strip()[:200] or None,
                )
            rows, throttles = parse_squeue(queue["stdout"])
            accounting: dict[str, dict] | None = None
            if now - (previous.get("accounting_tried_at") or 0) >= SACCT_INTERVAL:
                doc["accounting_tried_at"] = now
                last = previous.get("accounting_at")
                first = previous.get("first_window", FIRST_WINDOW)
                window = first if not last else min(FIRST_WINDOW, now - last + 600)
                doc["first_window"] = first
                try:
                    acct = cluster.call(
                        "sacct",
                        fields=slurm.SACCT_FIELDS,
                        since=f"now-{int(window // 60) + 1}minutes",
                        all_steps=False,
                        _timeout=25,
                    )
                    if acct.get("rc") or acct.get("stdout_truncated"):
                        raise RemoteSlurmError("sacct failed or was truncated")
                    accounting = slurm.parse_sacct(acct["stdout"])
                    doc.update(accounting="ok", accounting_at=now, accounting_error=None)
                except Exception as exc:
                    if type(exc).__name__ == "BudgetExhausted":
                        doc["accounting_tried_at"] = previous.get("accounting_tried_at", 0)
                    else:
                        if not last:  # a heavy first window may time out or truncate: shrink
                            doc["first_window"] = max(MIN_FIRST_WINDOW, first / 3)
                        doc.update(
                            accounting="unavailable",
                            accounting_error=getattr(exc, "code", type(exc).__name__),
                        )
            else:
                doc["accounting_tried_at"] = previous.get("accounting_tried_at")
            seen = previous.get("observed_at")
            if doc["baseline"] and seen and now - seen > GAP_REBASELINE:
                doc["baseline"] = False  # catch up after a gap; record() summarizes instead
            changes = self.merge(doc, rows, throttles, accounting, now)
            doc["observed_at"] = now
            self.enrich(cluster, doc, now)
            doc["next_poll"] = now + SQUEUE_INTERVAL
        except Exception as exc:
            changes = []
            budget = type(exc).__name__ == "BudgetExhausted"
            doc.update(
                jobs=previous.get("jobs", {}),
                error=None if budget else getattr(exc, "code", type(exc).__name__),
                action=None
                if budget
                else getattr(exc, "action", None) or f"check the connection: rslurm connect {name}",
                next_poll=now + (RETRY_AFTER_BUDGET if budget else 2 * SQUEUE_INTERVAL),
                accounting_tried_at=previous.get("accounting_tried_at"),
            )
            if budget:
                doc["error"] = previous.get("error")
        self.prune(doc, now)
        try:
            self.record(doc, previous, changes, host)
        except Exception as exc:
            # Never loop on a document we can't store (e.g. over the 2 MiB bound): keep the
            # previous one, say why, and back off like any other failed read.
            fallback = {
                **previous,
                "host": name,
                "target_id": ident,
                "error": getattr(exc, "code", type(exc).__name__),
                "action": "too much job history to store; it will shrink as jobs age out",
                "next_poll": now + 2 * SQUEUE_INTERVAL,
                "attempted_at": now,
            }
            self.store.save("discovery", ident, fallback)
            return fallback
        return doc

    def merge(
        self,
        doc: dict,
        rows: list[dict[str, str]],
        throttles: dict[str, int],
        accounting: dict[str, dict] | None,
        now: float,
    ) -> list[tuple[str, dict, dict]]:
        jobs: dict[str, dict] = doc["jobs"]
        before = {k: dict(v) for k, v in jobs.items()}
        live: set[str] = set()
        # 1. Accounting first; the queue overrides it for jobs that are still active.
        for key, rec in (accounting or {}).items():
            base, _, task = key.partition("_")
            if not base.isdigit():
                continue
            job = jobs.setdefault(base, {"id": base, "first_seen": now})
            facts = {
                "name": rec.get("name"),
                "partition": rec.get("partition"),
                "account": rec.get("account"),
                "user": rec.get("user"),
                "workdir": rec.get("workdir"),
                "submit": parse_time(rec.get("submit_time")),
                "timelimit": slurm.walltime_to_seconds(rec.get("time_limit"))
                if rec.get("time_limit") not in (None, "", "UNLIMITED", "Partition_Limit")
                else None,
                "mem": _mem(rec.get("req_mem")),
            }
            for k, v in facts.items():
                if v is not None and (not task or k not in job):
                    job[k] = v
            state = rec.get("state", "UNKNOWN")
            if task:
                tasks = job.setdefault("tasks", {})
                if task.isdigit():
                    tasks[task] = state
                    if state in slurm.TERMINAL_STATES:
                        ended = parse_time(rec.get("end_time"))
                        if ended and ended > (job.get("last_end") or 0):
                            job["last_end"] = ended
                        info = job.setdefault("task_info", {})
                        if state != "COMPLETED" or slurm.exit_code_int(rec.get("exit_code")):
                            info[task] = _clean(
                                {
                                    "exit": rec.get("exit_code"),
                                    "elapsed": slurm.accounting_time_to_seconds(rec.get("elapsed")),
                                    "node": rec.get("nodelist")
                                    if rec.get("nodelist") not in ("None assigned",)
                                    else None,
                                    "end": parse_time(rec.get("end_time")),
                                    "start": parse_time(rec.get("start_time")),
                                },
                                ["exit", "elapsed", "node", "end", "start"],
                            )
                        else:
                            info.pop(task, None)
                else:
                    spec = task.strip("[]")
                    for t in slurm.expand_array_tasks(spec.split("%")[0]):
                        if tasks.get(str(t)) not in slurm.TERMINAL_STATES:
                            tasks[str(t)] = state
                    limit = _THROTTLE.search(spec)
                    if limit:
                        job["limit"] = int(limit.group(1))
                job["array"] = True
                continue
            job.update(
                _clean(
                    {
                        "state": state,
                        "exit": rec.get("exit_code") if state in slurm.TERMINAL_STATES else None,
                        "elapsed": slurm.accounting_time_to_seconds(rec.get("elapsed")),
                        "start": parse_time(rec.get("start_time")),
                        "end": parse_time(rec.get("end_time")),
                        "nodelist": rec.get("nodelist")
                        if rec.get("nodelist") not in ("None assigned",)
                        else None,
                        "cpus": _int(rec.get("alloc_cpus")),
                    },
                    ["state", "exit", "elapsed", "start", "end", "nodelist", "cpus"],
                )
            )
            job["source"] = "sacct"
            if state in slurm.TERMINAL_STATES:
                job["reason"] = None
                job["est_start"] = None
            job.pop("vanished_at", None)
        # 2. The queue: authoritative for active jobs and tasks.
        queued_tasks: dict[str, dict[str, str]] = {}
        for row in rows:
            base = row["array_base"] or row["job_id"]
            if not base.isdigit():
                continue
            live.add(base)
            job = jobs.setdefault(base, {"id": base, "first_seen": now})
            job.update(
                _clean(
                    {
                        "name": row.get("name"),
                        "partition": row.get("partition"),
                        "account": row.get("account"),
                        "user": row.get("user"),
                        "workdir": row.get("workdir"),
                        "submit": parse_time(row.get("submit_time")),
                        "timelimit": slurm.walltime_to_seconds(row.get("time_limit"))
                        if row.get("time_limit") not in ("UNLIMITED", "")
                        else None,
                        "mem": _mem(row.get("min_memory")),
                    },
                    [
                        "name",
                        "partition",
                        "account",
                        "user",
                        "workdir",
                        "submit",
                        "timelimit",
                        "mem",
                    ],
                )
            )
            job["source"] = "squeue"
            job["seen_at"] = now
            job.pop("vanished_at", None)
            if row["array_base"]:
                job["array"] = True
                queued_tasks.setdefault(base, {})[row["array_task"]] = row["state"]
                if row["state"] == "PENDING" and row.get("reason") not in (None, "", "None"):
                    job["reason"] = row["reason"]
                if base in throttles:
                    job["limit"] = throttles[base]
                if row["state"] != "PENDING":
                    start = parse_time(row.get("start_time"))
                    if start and (not job.get("start") or start < job["start"]):
                        job["start"] = start
                continue
            state = row["state"]
            job.update(
                {
                    "state": state,
                    "reason": row.get("reason") if state == "PENDING" else None,
                    "start": parse_time(row.get("start_time")) if state != "PENDING" else None,
                    "est_start": parse_time(row.get("start_time")) if state == "PENDING" else None,
                    "elapsed": slurm.walltime_to_seconds(row.get("time_used")),
                    "nodes": _int(row.get("nodes")),
                    "nodelist": row.get("nodelist") if state != "PENDING" else None,
                    "cpus": _int(row.get("cpus")),
                    "exit": None,
                    "end": None,
                }
            )
        # 3. Arrays: merge active task states over accounting, then roll up.
        for base, tasks in queued_tasks.items():
            job = jobs[base]
            merged = job.setdefault("tasks", {})
            for task, state in tasks.items():
                merged[task] = state
        for base, job in jobs.items():
            if not job.get("array"):
                continue
            tasks = job.setdefault("tasks", {})
            # A task the queue no longer lists has left it; until accounting reports how it
            # ended, its state is unknown (never "still running" or "still pending").
            for task, state in list(tasks.items()):
                if state not in slurm.TERMINAL_STATES and task not in queued_tasks.get(base, {}):
                    tasks[task] = "UNKNOWN"
            unknown = sum(s == "UNKNOWN" for s in tasks.values())
            summary = slurm.aggregate_array(
                base,
                [],
                {f"{base}_{t}": {"state": s} for t, s in tasks.items() if s != "UNKNOWN"},
            )
            state = summary["state"]
            if unknown and (state in slurm.TERMINAL_STATES or not summary["task_states"]):
                state = "UNKNOWN"  # some tasks left the queue unaccounted: not done, not running
            job["state"] = state
            job["failed_tasks"] = len(summary["failed_tasks"])
            if len(job.get("task_info", {})) > MAX_TASK_INFO:
                kept = sorted(job["task_info"].items(), key=lambda kv: -(kv[1].get("end") or 0))
                job["task_info"] = dict(kept[:MAX_TASK_INFO])
            if base not in live:
                terminal = summary["terminal"] and not unknown
                job["source"] = "sacct" if terminal else "retained"
                job.pop("reason", None)
                if terminal:
                    job["end"] = job.get("last_end")
                elif state == "UNKNOWN":
                    job.setdefault("vanished_at", now)
        # 4. Single jobs that left the queue without an accounting record yet.
        for base, job in jobs.items():
            if job.get("array") or base in live:
                continue
            if job.get("source") == "squeue" or job.get("state") in slurm.ACTIVE_STATES:
                job["source"] = "retained"
                job["vanished_at"] = now
                job["last_state"] = job.get("state")
                job["state"] = "UNKNOWN"
                job["reason"] = None
        # 5. Transitions (after the first full baseline only).
        changes: list[tuple[str, dict, dict]] = []
        for base, job in jobs.items():
            old = before.get(base)
            if job.get("state") == "UNKNOWN":
                continue  # left the queue; the event comes when accounting says how it ended
            if old is None:
                # Only work that appeared after the first scan is news, not the 7-day backfill.
                # Anything active at that scan was in it, so a newly queued job is always new;
                # one first seen in accounting must have been submitted near or after it
                # (Slurm times are whole seconds and the cluster clock may drift).
                if doc["baseline"] and (
                    job.get("source") == "squeue"
                    or (job.get("submit") or now) >= doc.get("baseline_at", now) - 120
                ):
                    changes.append((base, {}, job))
                continue
            if old.get("state") == "UNKNOWN":
                old = {**old, "state": old.get("last_state")}
            if old.get("state") != job.get("state") or (
                job.get("failed_tasks", 0) > old.get("failed_tasks", 0)
            ):
                changes.append((base, old, job))
        return changes

    def enrich(self, cluster: Any, doc: dict, now: float) -> None:
        """At most one ``scontrol`` and one stepped ``sacct`` per observation."""
        jobs = doc["jobs"]
        for job in sorted(jobs.values(), key=lambda j: -(j.get("submit") or 0)):
            if job.get("paths_checked") or job.get("source") != "squeue":
                continue
            job["paths_checked"] = now
            try:
                out = cluster.call("scontrol", what="job", id=job["id"], _timeout=10)
                if out.get("rc"):
                    break
                first = (out.get("stdout") or "").strip().splitlines()[:1]
                if first:
                    fields = slurm.parse_scontrol_job(first[0])
                    for key, name in (("StdOut", "stdout"), ("StdErr", "stderr")):
                        if fields.get(key) and fields[key] not in ("(null)",):
                            job[name] = fields[key]
                    if fields.get("ArrayTaskId", "").isdigit():
                        job["paths_task"] = fields["ArrayTaskId"]
                    if fields.get("WorkDir"):
                        job.setdefault("workdir", fields["WorkDir"])
            except Exception as exc:
                if type(exc).__name__ == "BudgetExhausted":
                    job.pop("paths_checked", None)
            break
        if doc.get("accounting") != "ok":
            return
        wanted: list[str] = []
        for job in jobs.values():
            if job.get("steps_checked"):
                continue
            if job.get("array"):
                for task, info in job.get("task_info", {}).items():
                    if "maxrss" not in info and "maxrss_checked" not in info:
                        wanted.append(f"{job['id']}_{task}")
            elif job.get("state") in slurm.TERMINAL_STATES and job.get("source") == "sacct":
                wanted.append(job["id"])
            if len(wanted) >= MAX_ENRICH_STEPS:
                break
        if not wanted:
            return
        wanted = wanted[:MAX_ENRICH_STEPS]
        try:
            out = cluster.call(
                "sacct", jobs=wanted, fields=slurm.SACCT_FIELDS, all_steps=True, _timeout=25
            )
            parsed = (
                {}
                if out.get("rc") or out.get("stdout_truncated")
                else slurm.parse_sacct(out["stdout"])
            )
        except Exception as exc:
            if type(exc).__name__ == "BudgetExhausted":
                return  # not tried; ask again next time
            parsed = {}  # tried and failed: mark checked below so it is not retried forever
        for ident in wanted:
            base, _, task = ident.partition("_")
            rec = parsed.get(ident) or {}
            job = jobs.get(base)
            if job is None:
                continue
            if task:
                info = job.setdefault("task_info", {}).setdefault(task, {})
                info["maxrss_checked"] = now
                if rec.get("max_rss") is not None:
                    info["maxrss"] = rec["max_rss"]
            else:
                job["steps_checked"] = now
                if rec.get("max_rss") is not None:
                    job["maxrss"] = rec["max_rss"]
                if rec.get("batch_exit_code") and not job.get("exit"):
                    job["exit"] = rec["batch_exit_code"]

    @staticmethod
    def prune(doc: dict, now: float) -> None:
        jobs = doc["jobs"]
        for base in list(jobs):
            job = jobs[base]
            done = job.get("end") or job.get("vanished_at")
            if job.get("state") in slurm.TERMINAL_STATES | {"UNKNOWN"} and done:
                if now - done > KEEP_FINISHED:
                    del jobs[base]
        if len(jobs) > MAX_JOBS:
            finished = sorted(
                (j for j in jobs.values() if j.get("state") not in slurm.ACTIVE_STATES),
                key=lambda j: j.get("end") or j.get("vanished_at") or j.get("submit") or 0,
            )
            for job in finished[: len(jobs) - MAX_JOBS]:
                del jobs[job["id"]]

    def record(self, doc: dict, previous: dict, changes: list, host: Any) -> None:
        subject = Subject(doc["target_id"], doc["host"])
        documents = (("discovery", doc["target_id"], doc),)
        first = doc["observed_at"] is not None and not doc["baseline"]
        resumed = bool(previous.get("baseline"))
        if first:
            doc["baseline"] = True
            doc["baseline_at"] = doc["observed_at"]
            active = sum(j.get("state") in slurm.ACTIVE_STATES for j in doc["jobs"].values())
            self.store.append(
                subject=subject,
                kind="jobs.discovered",
                source="discovery",
                payload={
                    "active": active,
                    "finished": len(doc["jobs"]) - active,
                    "summary": f"{active} active, {len(doc['jobs']) - active} finished (7 days)",
                    "resumed": resumed,
                },
                evidence=(EvidenceRef("scheduler", "squeue -u/sacct -X", doc["target_id"]),),
                documents=documents,
            )
            return
        if previous.get("error") != doc["error"] and (doc["error"] or previous.get("error")):
            self.store.append(
                subject=subject,
                kind="discovery.health",
                source="discovery",
                payload={
                    "state": "UNAVAILABLE" if doc["error"] else "HEALTHY",
                    "error": doc["error"],
                    "summary": doc["action"],
                },
                freshness="UNAVAILABLE" if doc["error"] else "FRESH",
            )
        if previous.get("accounting") != doc["accounting"] and previous.get("accounting"):
            self.store.append(
                subject=subject,
                kind="discovery.accounting",
                source="discovery",
                payload={"state": doc["accounting"], "error": doc.get("accounting_error")},
                freshness="FRESH" if doc["accounting"] == "ok" else "UNAVAILABLE",
            )
        events: list[dict] = []
        for base, old, job in changes:
            payload = {
                "job_id": base,
                "name": job.get("name"),
                "from": old.get("state") if old else None,
                "state": job.get("state"),
                "exit_code": job.get("exit"),
                "source": job.get("source"),
            }
            if job.get("array"):
                payload["tasks"] = len(job.get("tasks", {}))
                payload["failed_tasks"] = job.get("failed_tasks", 0)
                payload["new_failed_tasks"] = max(
                    0, job.get("failed_tasks", 0) - (old.get("failed_tasks", 0) if old else 0)
                )
            if job.get("state") == "PENDING" and job.get("reason"):
                payload["reason"] = job["reason"]
            events.append(
                dict(
                    subject=Subject(doc["target_id"], doc["host"], job_id=base),
                    kind="job.state.changed",
                    source="discovery",
                    payload=payload,
                    evidence=(EvidenceRef("scheduler", "squeue -u/sacct -X", base),),
                )
            )
        self.store.save("discovery", doc["target_id"], doc)
        for item in events:
            self.store.append(**item)


def lease(hosts: list[Any], seconds: int = LEASE_SECONDS) -> None:
    """Ask the daemon to discover these hosts for a while (renewed by an open dashboard)."""
    store = ActivityStore()
    until = time.time() + seconds
    with store.transaction() as db:
        for host in hosts:
            store.put(
                db,
                "discover",
                target_id(host),
                {"host": host.name, "target_id": target_id(host), "lease_until": until},
            )
