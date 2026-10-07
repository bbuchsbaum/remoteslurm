"""Passive imports of bounded canonical records. All remote calls in this module are reads."""

from __future__ import annotations

import logging
from dataclasses import asdict, fields, replace
from typing import Any

from .model import EvidenceRef, SemanticRef, Subject
from .producer import CLIENT, CORRELATION, OPERATION, target_id, workspace_for
from .semantic import cache_snapshot, key
from .store import ActivityStore, stamp


def import_group(
    store: ActivityStore,
    cluster: Any,
    campaign: str,
    run_id: str,
    record: dict,
    *,
    imported: bool = True,
) -> int:
    target, group = target_id(cluster.host), record["group_id"]
    contract = record.get("contract", {})
    count = 0
    for attempt in record.get("attempts", []):
        attempt_id = attempt["attempt_id"]
        for member in contract.get("mapping", [])[:1000]:
            unit_id = member["unit_id"]
            canonical = record.get("unit_bindings", {}).get(unit_id)
            current = (
                canonical == {"group_id": group, "attempt_id": attempt_id}
                if canonical is not None
                else record.get("current_attempt") == attempt_id
            )
            mode = contract.get("mode", "single")
            job = str(attempt["job_id"]) if attempt.get("job_id") else None
            index = str(member.get("index", 0)) if mode != "single" else None
            jid = f"{job}_{index}" if job and index is not None else job
            card = store.get("campaign_unit", key(target, campaign, run_id, unit_id))
            ref = SemanticRef(**card["semantic_ref"]) if card else None
            semantic_document: tuple = ()
            if card and ref:
                binding = {
                    "attempt_id": attempt_id,
                    "group_id": group,
                    "job_id": job,
                    "array_task_id": index,
                    "packed_member": str(member["slot"]) if "slot" in member else None,
                }
                ref = replace(ref, contract_digest=key(ref.contract_digest, binding, unit_id))
                card = {
                    **card,
                    **binding,
                    "job_id": jid,
                    "semantic_ref": asdict(ref),
                    "attempt_members": [binding],
                    "mapping_source": "canonical execution ledger",
                    "context": {
                        "availability": "attached",
                        "meaning": "at_event",
                        "ref": asdict(ref),
                    },
                }
                semantic_document = (("semantic", key(target, asdict(ref), unit_id), card),)
            subject = Subject(
                target,
                cluster.host.name,
                campaign=campaign,
                run_id=run_id,
                stage=contract.get("stage"),
                unit_id=unit_id,
                group_id=group,
                attempt_id=attempt_id,
                job_id=job,
                array_task_id=index,
                packed_member=str(member["slot"]) if "slot" in member else None,
                submit_time=(attempt.get("scheduler") or {}).get("submit_time"),
                identity_confidence="canonical_ledger",
                semantic_ref=ref,
            )
            payload = {
                "state": attempt.get("state", "UNKNOWN"),
                "job_id": jid,
                "current": current,
                "historical": not current,
                "recovered": imported,
                "workdir": attempt.get("cwd") or contract.get("cwd"),
                "accepted_at": attempt.get("accepted_at"),
                "marker": attempt.get("marker"),
                "mapping_omitted": max(0, len(contract.get("mapping", [])) - 1000),
            }
            digest = key({k: v for k, v in payload.items() if k != "recovered"})
            cp_key = key(target, campaign, run_id, group, attempt_id, unit_id)
            prior = store.get("checkpoint", cp_key) or {}
            if prior.get("digest") == digest:
                continue
            ordinal = prior.get("ordinal", 0) + 1
            work_id = key(target, campaign, run_id, group, attempt_id, unit_id)
            state = payload["state"]
            work = {
                **(card or {}),
                **payload,
                "work_id": work_id,
                "target_id": target,
                "host": cluster.host.name,
                "name": f"{campaign}/{contract.get('stage', '')} "
                + (
                    ", ".join(
                        f"{k}={v}" for k, v in list((card or {}).get("label", {}).items())[:4]
                    )
                    or unit_id[:12]
                )
                + (" [previous attempt]" if not current else ""),
                "campaign": campaign,
                "run_id": run_id,
                "unit_id": unit_id,
                "attempt_id": attempt_id,
                "group_id": group,
                "job_id": jid,
                "submit_time": attempt.get("accepted_at") or attempt.get("created_at", 0),
                "submit_time_basis": (
                    "ledger acceptance or creation time; scheduler submit time unavailable"
                ),
                "observed_at": attempt.get("accepted_at") or attempt.get("created_at", 0),
                "execution": "PENDING"
                if state == "ACCEPTED"
                else "UNKNOWN"
                if state in {"SUBMITTING", "INTENDED"}
                else state,
                "artifacts": (card or {}).get("artifacts", "UNCHECKED") if current else "UNCHECKED",
                "validation": (card or {}).get("validation", "NOT_RUN") if current else "NOT_RUN",
                "context": (card or {}).get("context", {"availability": "unavailable"}),
                "operation_id": OPERATION.get() or (card or {}).get("operation_id"),
                "correlation_id": CORRELATION.get() or (card or {}).get("correlation_id"),
                "marker_dir": (
                    f"{contract['marker_root'].rstrip('/')}/{group}/{attempt_id}/{unit_id}"
                )
                if mode == "pack" and contract.get("marker_root")
                else None,
                "subject": {
                    f.name: getattr(subject, f.name)
                    for f in fields(subject)
                    if f.name != "semantic_ref"
                },
            }
            occurred = attempt.get("accepted_at") or attempt.get("created_at")
            if work.get("marker_dir"):
                work["stdout_path"] = work["marker_dir"] + "/stdout.log"
                work["stderr_path"] = work["marker_dir"] + "/stderr.log"
            store.append(
                subject=subject,
                kind="submission.recovered" if imported else "submission.recorded",
                source="execution_ledger",
                provenance="remote_record",
                payload=payload,
                source_key=key(cp_key, ordinal, digest),
                workspace=workspace_for(store, target, payload["workdir"]),
                occurred_at=stamp(occurred) if occurred else None,
                evidence=(EvidenceRef("execution_ledger", "campaign_ledger_read", digest),),
                actor="observer" if imported else CLIENT.get(),
                operation_id=OPERATION.get(),
                correlation_id=CORRELATION.get(),
                documents=(("work", work_id, work), *semantic_document),
                checkpoint=(cp_key, {"digest": digest, "ordinal": ordinal}),
            )
            count += 1
    return count


def acknowledged(cluster: Any, campaign: str, run_id: str, result: dict) -> bool:
    """Import the durable ack before the caller publishes any campaign view."""
    try:
        store = ActivityStore()
        for outcome in result.get("outcomes", [result]):
            record = dict(outcome["record"])
            record.setdefault("group_id", outcome["group"]["group_id"])
            import_group(store, cluster, campaign, run_id, record, imported=False)
        return True
    except Exception as exc:
        logging.getLogger(__name__).warning("Attempt activity unavailable (%s)", type(exc).__name__)
        return False


def task_record(cluster: Any, record: dict, *, store: ActivityStore | None = None) -> bool:
    """Import only attempt-bound durable receipts, excluding validator commands and output."""
    try:
        store = store or ActivityStore()
        target = target_id(cluster.host)
        contract = record.get("contract", {})
        receipt = record.get("receipt") or {}
        for attempt in record.get("attempts", []):
            aid, task = attempt["attempt_id"], record["task_id"]
            matches = receipt.get("attempt_id") == aid and receipt.get("task_id") == task
            state = attempt.get("state", "UNKNOWN")
            work_id = key(target, task, aid)
            payload = {
                "state": state,
                "task_id": task,
                "attempt_id": aid,
                "job_id": attempt.get("job_id"),
                "name": record.get("name"),
                "workdir": contract.get("cwd"),
                "historical": record.get("current_attempt") != aid,
                "receipt_digest": key(receipt) if matches else None,
            }
            digest = key(payload)
            cp_key = key("task-record", work_id)
            previous = store.get("checkpoint", cp_key) or {}
            if previous.get("digest") == digest:
                continue
            ordinal = previous.get("ordinal", 0) + 1
            work = {
                **payload,
                "work_id": work_id,
                "host": cluster.host.name,
                "target_id": target,
                "execution": "COMPLETED"
                if state in {"VERIFIED", "INVALID"}
                else "PENDING"
                if state == "ACCEPTED"
                else "UNKNOWN"
                if state in {"INTENDED", "SUBMITTING"}
                else state,
                "validation": "PASSED"
                if matches and state == "VERIFIED"
                else "FAILED"
                if state == "INVALID"
                else "NOT_RUN",
                "artifacts": "PRESENT" if matches and state == "VERIFIED" else "UNCHECKED",
                "outputs": contract.get("outputs", [])[:64],
                "outputs_omitted": max(0, len(contract.get("outputs", [])) - 64),
                "submit_time": attempt.get("accepted_at") or attempt.get("created_at", 0),
                "observed_at": record.get("updated_at", 0),
                "context": {
                    "availability": "attached",
                    "meaning": "durable task contract",
                    "digest": task,
                },
                "operation_id": OPERATION.get(),
                "correlation_id": CORRELATION.get(),
            }
            store.append(
                subject=Subject(
                    target,
                    cluster.host.name,
                    task_id=task,
                    attempt_id=aid,
                    job_id=attempt.get("job_id"),
                    identity_confidence="durable_task_record",
                ),
                kind="validation.recorded" if matches else "task.recorded",
                source="task.record",
                provenance="remote_record",
                payload=payload,
                evidence=(
                    EvidenceRef(
                        "validation" if matches else "execution_ledger", "task.json", digest
                    ),
                ),
                occurred_at=stamp(record["updated_at"]) if record.get("updated_at") else None,
                actor=CLIENT.get(),
                operation_id=OPERATION.get(),
                correlation_id=CORRELATION.get(),
                workspace=workspace_for(store, target, contract.get("cwd")),
                source_key=key(cp_key, ordinal, digest),
                checkpoint=(cp_key, {"digest": digest, "ordinal": ordinal}),
                documents=(
                    ("work", work_id, work),
                    (
                        "task",
                        key(target, task),
                        {
                            "task_id": task,
                            "target_id": target,
                            "host": cluster.host.name,
                            "workdir": contract.get("cwd"),
                        },
                    ),
                ),
            )
        return True
    except Exception as exc:
        logging.getLogger(__name__).warning("Task context unavailable (%s)", type(exc).__name__)
        return False


class Recovery:
    def __init__(self, store: ActivityStore, cluster: Any):
        self.store, self.cluster = store, cluster

    def page(self, campaign: str, run_id: str) -> dict:
        """At most five 10-second reads/pass; continuation persists across process loss."""
        target = target_id(self.cluster.host)
        cp_key = key("campaign-recovery", target, campaign, run_id)
        cp = self.store.get("checkpoint", cp_key) or {}
        base = {"campaign_dir": self.cluster.host.campaign_dir, "name": campaign, "run_id": run_id}

        calls = 0

        def read(op: str, **args: Any) -> dict:
            nonlocal calls
            calls += 1
            return dict(self.cluster.call(op, **base, **args, _timeout=10))

        # Every document belongs to one immutable reachable commit, including unit continuation.
        summary = cp.get("pending_summary")
        if summary is None:
            summary = read("campaign_read", commit=cp.get("snapshot_commit"))
            value = summary["value"]
            summary = {
                "head": summary["head"],
                "value": {
                    k: v
                    for k, v in value.items()
                    if k in {"campaign", "run_id", "definition_id", "workspace", "refreshed_at"}
                },
            }
            summary["value"]["stage_specs"] = {
                k: {f: v for f, v in spec.items() if f in {"script", "resources"}}
                for k, spec in value.get("stage_specs", {}).items()
            }
            cp["pending_summary"] = summary
            self.store.save("checkpoint", cp_key, cp)
        head = summary["head"]
        snapshot = dict(summary["value"], store=head)
        index = cp.get("pending_index")
        if index is None:
            index = read("campaign_read", document="index.json", commit=head["transaction_id"])[
                "value"
            ]
            cp["pending_index"] = index
            self.store.save("checkpoint", cp_key, cp)
        offset = cp.get("unit_page", 0)
        pages = index.get("pages", [])
        units = []
        if not cp.get("units_ready") and offset < len(pages):
            units = read(
                "campaign_read", document=pages[offset]["document"], commit=head["transaction_id"]
            )["value"]["units"]
        snapshot["units"] = units
        snapshot["unit_total"] = index["unit_count"]
        documents = (
            cache_snapshot(self.store, self.cluster, snapshot) if not cp.get("units_ready") else []
        )
        # Make immutable context available before any imported attempt cites it.
        with self.store.transaction() as db:
            for namespace, doc_key, doc in documents:
                self.store.put(db, namespace, doc_key, doc)
            cp["units_ready"] = True
            self.store.put(db, "checkpoint", cp_key, cp)
        ledger = cp.get("pending_ledger")
        imported = 0
        if ledger is None:
            ledger = read(
                "campaign_ledger_read",
                offset=cp.get("ledger_offset", 0),
                digest=cp.get("ledger_digest"),
            )
            imported = sum(
                import_group(self.store, self.cluster, campaign, run_id, g)
                for g in ledger["groups"]
            )
            ledger = {k: v for k, v in ledger.items() if k != "groups"}
            cp["pending_ledger"] = ledger
            self.store.save("checkpoint", cp_key, cp)
        changes = read("campaign_changes", after=cp.get("after"))
        for change in changes["changes"]:
            checkpoint = key(cp_key, change["commit"])
            if self.store.get("checkpoint", checkpoint):
                continue
            self.store.append(
                subject=Subject(target, self.cluster.host.name, campaign=campaign, run_id=run_id),
                kind="campaign.imported",
                source="campaign_changes",
                provenance="remote_record",
                payload={
                    "commit": change["commit"],
                    "revision": change["revision"],
                    "events": [
                        {
                            k: v
                            for k, v in e.items()
                            if k
                            in {
                                "type",
                                "event_id",
                                "unit_id",
                                "attempt_id",
                                "group_id",
                                "state",
                                "recorded_at",
                            }
                        }
                        for e in change["events"][:100]
                    ],
                    "events_omitted": max(0, len(change["events"]) - 100),
                    "coverage": changes["coverage"],
                },
                occurred_at=stamp(change["committed_at"]),
                evidence=(EvidenceRef("campaign", "campaign_changes", change["commit"]),),
                source_key=checkpoint,
                checkpoint=(checkpoint, {"imported": True}),
                workspace=workspace_for(
                    self.store, target, snapshot.get("workspace", {}).get("remote_root")
                ),
            )
        next_cp = {
            "after": changes["next_after"],
            "snapshot_commit": head["transaction_id"] if offset + 1 < len(pages) else None,
            "unit_page": offset + 1 if offset + 1 < len(pages) else 0,
            "ledger_offset": ledger.get("next_offset") or 0,
            "ledger_digest": ledger.get("digest") if ledger.get("next_offset") else None,
            "history_gap": cp.get("history_gap", False) or changes["coverage"] == "gap",
        }
        # Projection page and its watermark publish together; imported events already checkpointed.
        with self.store.transaction() as db:
            for namespace, doc_key, doc in documents:
                self.store.put(db, namespace, doc_key, doc)
            self.store.put(db, "checkpoint", cp_key, next_cp)
        return {
            "imported": imported,
            "head": head,
            "coverage": "gap" if next_cp["history_gap"] else changes["coverage"],
            "ledger_available": ledger["available"],
            "unit_total": index["unit_count"],
            "continuation": next_cp,
            "remote_calls": calls,
        }
