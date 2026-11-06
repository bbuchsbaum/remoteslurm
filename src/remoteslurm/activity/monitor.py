"""Persistent campaign enrollment and one read-only collector owned by JobMonitor."""

from __future__ import annotations

import json
import re
import time
from typing import Any

from ..errors import InvalidArgument, RemoteSlurmError
from .model import EvidenceRef, Subject
from .producer import target_id
from .recovery import Recovery
from .semantic import key
from .store import ActivityStore, encoded, stamp

POLICY = {
    "scheduler_seconds": 30,
    "recovery_seconds": 60,
    "usage_seconds": 60,
    "artifacts_seconds": 60,
    "max_calls": 8,
    "read_seconds": 10,
    "max_jobs": 1000,
    "max_outputs": 1024,
    "max_hash_bytes": 64 * 1024 * 1024,
    "max_log_excerpts": 3,
    "max_host_workers": 2,
}
READS = {
    "squeue",
    "sacct",
    "sstat",
    "workspace_anchor",
    "browse",
    "workspace_read",
    "campaign_read",
    "campaign_changes",
    "campaign_ledger_read",
    "task_read",
    "attempt_matches",
    "contract_observe",
    "campaign_markers",
    "scontrol",
}


class BudgetExhausted(RemoteSlurmError):
    code = "budget_exhausted"


def control(enabled: bool | None = None) -> dict:
    store = ActivityStore()
    if enabled is not None:
        store.save(
            "monitor", "control", {**(store.get("monitor", "control") or {}), "enabled": enabled}
        )
    return store.get("monitor", "control") or {"enabled": True}


def policy(**overrides: int) -> dict:
    store = ActivityStore()
    value = {**POLICY, **(control().get("policy") or {})}
    for name, setting in overrides.items():
        minimum, maximum = (
            (30, 3600)
            if name == "scheduler_seconds"
            else (60, 3600)
            if name.endswith("_seconds") and name != "read_seconds"
            else (1, POLICY.get(name, 0))
        )
        if name not in POLICY or type(setting) is not int or not minimum <= setting <= maximum:
            raise InvalidArgument("invalid observation budget or cadence")
        value[name] = setting
    if overrides:
        store.save("monitor", "control", {**control(), "policy": value})
    return value


def enroll(host: Any, campaign: str, run_id: str, *, enabled: bool = True) -> dict:
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,63}", campaign) or not re.fullmatch(
        r"[A-Za-z0-9_-]{1,128}", run_id
    ):
        raise InvalidArgument("monitor requires a valid campaign and explicit run ID")
    if enabled:
        control(True)
    store = ActivityStore()
    ident = key(target_id(host), campaign, run_id)
    with store.transaction() as db:
        existing = store.get_in(db, "subscription", ident) or {}
        value = {
            "id": ident,
            "host": host.name,
            "target_id": target_id(host),
            "campaign": campaign,
            "run_id": run_id,
            "enabled": enabled,
            "next_poll": existing.get("next_poll", 0),
            "created_at": existing.get("created_at", time.time()),
        }
        store.put(db, "subscription", ident, value)
    return value


def status(host: str | None = None) -> dict:
    store = ActivityStore()
    now = time.time()
    sections: dict[str, list[dict]] = {}
    omitted: dict[str, int] = {}
    used = 16 * 1024
    for name in ("subscription", "source", "budget"):
        with store.connection() as db:
            scope = "namespace=?" + (" AND json_extract(data,'$.host')=?" if host else "")
            args = [name, host] if host else [name]
            count = db.execute("SELECT count(*) FROM documents WHERE " + scope, args).fetchone()[0]
            rows = db.execute(
                "SELECT data FROM documents WHERE " + scope + " ORDER BY updated DESC LIMIT 100",
                args,
            ).fetchall()
        selected = []
        for row in rows:
            size = len(row[0].encode())
            if used + size > 112 * 1024:
                break
            selected.append(json.loads(row[0]))
            used += size
        sections[name] = selected
        omitted[name] = count - len(selected)
    sources = sections["source"]
    for source in sources:
        source["stale"] = (
            bool(source.get("error"))
            or not source.get("observed_at")
            or now - source["observed_at"] > 2 * source["cadence"]
        )
    return {
        **control(),
        "policy": policy(),
        "observer": store.get("observer", "health"),
        "subscriptions": sections["subscription"],
        "sources": sources,
        "budgets": sections["budget"],
        "omitted": omitted,
        "response_budget_bytes": 128 * 1024,
    }


class ReadBudget:
    """Persist admission before dispatch so a restart cannot burst above the host ceiling."""

    def __init__(self, cluster: Any, store: ActivityStore):
        if isinstance(cluster, ReadBudget):
            cluster = cluster.cluster
        object.__setattr__(self, "cluster", cluster)
        object.__setattr__(self, "store", store)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.cluster, name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self.cluster, name, value)

    def call(self, op: str, **args: Any) -> Any:
        if op not in READS:
            raise RemoteSlurmError("passive collector refused a remote mutation")
        store, now, ident = self.store, time.time(), target_id(self.host)
        limits = policy()
        with store.transaction() as db:
            budget = store.get_in(db, "budget", ident) or {}
            if not budget or now - budget["started"] >= 30 or now < budget["started"]:
                budget = {"host": self.host.name, "started": now, "calls": 0, "bytes": 0}
            cost = 2 if op == "attempt_matches" else 1
            if budget["calls"] + cost > limits["max_calls"]:
                raise BudgetExhausted("host observation budget exhausted")
            if op == "workspace_read":
                if budget.get("excerpts", 0) >= limits["max_log_excerpts"]:
                    raise BudgetExhausted("host excerpt budget exhausted")
                budget["excerpts"] = budget.get("excerpts", 0) + 1
            budget["calls"] += cost
            store.put(db, "budget", ident, budget)
        args["_timeout"] = min(args.get("_timeout", 10), limits["read_seconds"])
        result = self.cluster.call(op, **args)
        with store.transaction() as db:
            current = store.get_in(db, "budget", ident)
            if current and current["started"] == budget["started"]:
                current["bytes"] += len(encoded(result).encode())
                store.put(db, "budget", ident, current)
        return result


class CampaignObserver:
    def __init__(self, get_cluster: Any, get_host: Any):
        self.store = ActivityStore()
        self.get_cluster, self.get_host = get_cluster, get_host

    def active(self) -> bool:
        return control()["enabled"] and any(
            s["enabled"] for s in self.store.documents("subscription", limit=1000)
        )

    def tick(self) -> None:
        if not control()["enabled"]:
            return
        now = time.time()
        with self.store.connection() as db:
            row = db.execute(
                "SELECT data FROM documents WHERE namespace='subscription' "
                "AND json_extract(data,'$.enabled')=1 AND json_extract(data,'$.next_poll')<=? "
                "ORDER BY json_extract(data,'$.next_poll'),key LIMIT 1",
                (now,),
            ).fetchone()
        if row is None:
            return
        subscription = json.loads(row[0])
        from .collectors import KINDS, sample

        sources: list[dict[str, Any]] = [
            self.store.get("source", key(subscription["id"], k)) or {"kind": k, "next_poll": 0}
            for k in KINDS
        ]
        prior = min(sources, key=lambda s: s["next_poll"])
        kind = prior["kind"]
        source_key = key(subscription["id"], kind)
        cadence = policy()[kind + "_seconds"]
        source: dict[str, Any]
        try:
            host = self.get_host(subscription["host"])
            cluster = self.get_cluster(subscription["host"])
            if (
                target_id(host) != subscription["target_id"]
                or target_id(cluster.host) != subscription["target_id"]
            ):
                raise RemoteSlurmError("monitor target changed; enroll the new target explicitly")
            result = (
                Recovery(self.store, cluster).page(subscription["campaign"], subscription["run_id"])
                if kind == "recovery"
                else sample(self.store, cluster, subscription, kind)
            )
            source = {
                "state": "HEALTHY"
                if result["coverage"] == "complete" and result.get("ledger_available", True)
                else "PARTIAL",
                "error": None,
                "observed_at": now,
                "result": result,
            }
        except Exception as exc:
            source = {
                "state": "SUSPENDED",
                "error": getattr(exc, "code", type(exc).__name__),
                "failed_at": now,
                "action": getattr(exc, "action", None),
                "observed_at": prior.get("observed_at"),
            }
            if source["error"] == "store_conflict":
                # Mutable ledger pagination must restart; never silently skip its remainder.
                checkpoint = key(
                    "campaign-recovery",
                    subscription["target_id"],
                    subscription["campaign"],
                    subscription["run_id"],
                )
                cp = self.store.get("checkpoint", checkpoint) or {}
                cp.update(ledger_offset=0, ledger_digest=None)
                for field in ("pending_summary", "pending_index", "pending_ledger", "units_ready"):
                    cp.pop(field, None)
                cp.update(snapshot_commit=None, unit_page=0, history_gap=True)
                self.store.save("checkpoint", checkpoint, cp)
        source.update(
            id=source_key,
            subscription_id=subscription["id"],
            kind=kind,
            host=subscription["host"],
            cadence=cadence,
            next_poll=now + cadence + int(source_key[:2], 16) % 5,
        )
        previous = self.store.get("source", source_key) or {}
        if [source["state"], source.get("error")] != [previous.get("state"), previous.get("error")]:
            self.store.append(
                subject=Subject(
                    subscription["target_id"],
                    subscription["host"],
                    campaign=subscription["campaign"],
                    run_id=subscription["run_id"],
                ),
                kind="monitor.gap" if source["state"] != "HEALTHY" else "monitor.resumed",
                source="campaign.observer",
                payload={"state": source["state"], "error": source.get("error")},
                freshness="UNAVAILABLE" if source["state"] == "SUSPENDED" else "PARTIAL",
                observed_at=stamp(now),
                evidence=(EvidenceRef("local_request", "monitor", subscription["id"]),),
                documents=(("source", source_key, source),),
            )
        else:
            self.store.save("source", source_key, source)
        with self.store.transaction() as db:
            current = self.store.get_in(db, "subscription", subscription["id"])
            if current and current["enabled"]:
                current["next_poll"] = min(
                    float(source["next_poll"] if s["kind"] == kind else s["next_poll"])
                    for s in sources
                )
                self.store.put(db, "subscription", current["id"], current)
