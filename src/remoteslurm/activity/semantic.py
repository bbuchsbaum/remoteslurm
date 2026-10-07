"""Commit-pinned presentation context. Never an execution or validation authority."""

from __future__ import annotations

import hashlib
import logging
from dataclasses import asdict
from typing import Any

from .model import EvidenceRef, SemanticRef, Subject
from .producer import CLIENT, CORRELATION, OPERATION, target_id, workspace_for
from .store import ActivityStore, encoded


def key(*parts: Any) -> str:
    return hashlib.sha256(encoded(parts).encode()).hexdigest()


def unit_card(snapshot: dict, unit: dict) -> dict:
    """Bound metadata; executable bodies/environment and validator commands stay remote."""
    stage = snapshot.get("stage_specs", {}).get(unit.get("stage"), {})
    execution = unit.get("execution", {})
    attempt = execution.get("current_attempt") or {}
    contract = unit.get("contract", {})
    commit = snapshot["store"]["transaction_id"]
    ref = SemanticRef(
        snapshot["campaign"],
        snapshot["run_id"],
        commit,
        key(snapshot.get("workspace", {})),
        key(contract),
    )
    return {
        "campaign": snapshot["campaign"],
        "run_id": snapshot["run_id"],
        "unit_id": unit["unit_id"],
        "stage": unit.get("stage"),
        "phase": unit.get("stage"),
        "label": unit.get("keys", {}),
        "command_label": stage.get("script"),
        "resources": stage.get("resources", {}),
        "dependencies": unit.get("dependency_requirements", []),
        "outputs": [
            {k: v for k, v in o.items() if k in {"name", "path", "kind"}}
            for o in contract.get("outputs", [])[:64]
        ],
        "outputs_omitted": max(0, len(contract.get("outputs", [])) - 64),
        "output_contracts": contract.get("outputs", [])[:64],
        "execution": execution.get("state", "UNKNOWN"),
        "artifacts": unit.get("artifacts", {}).get("state", "UNCHECKED"),
        "validation": unit.get("validation", {}).get("state", "NOT_RUN"),
        "validation_receipt": unit.get("validation", {}).get("receipt_id"),
        "validation_artifact_signature": unit.get("validation", {}).get("artifact_signature"),
        "dependency": unit.get("dependency", {}).get("state", "NOT_APPLICABLE"),
        "freshness": unit.get("freshness", {}),
        "attempt_id": attempt.get("attempt_id"),
        "group_id": attempt.get("group_id"),
        "job_id": attempt.get("job_id"),
        "array_task_id": str(attempt["array_index"])
        if attempt.get("mode") in {"array", "pack"} and "array_index" in attempt
        else None,
        "packed_member": str(attempt["slot"]) if "slot" in attempt else None,
        "accepted_at": attempt.get("accepted_at"),
        "attempt_members": [
            {
                "attempt_id": a.get("attempt_id"),
                "group_id": a.get("group_id"),
                "job_id": a.get("group_job_id") or a.get("job_id"),
                "array_task_id": str(a["array_index"])
                if a.get("mode") in {"array", "pack"} and "array_index" in a
                else None,
                "packed_member": str(a["slot"]) if "slot" in a else None,
            }
            for a in execution.get("attempts", [])[-100:]
        ],
        "semantic_ref": asdict(ref),
        "context": {"availability": "attached", "meaning": "at_event", "ref": asdict(ref)},
    }


def cache_snapshot(
    store: ActivityStore, cluster: Any, snapshot: dict
) -> list[tuple[str, str, dict]]:
    target = target_id(cluster.host)
    units = snapshot.get("units", [])
    documents = []
    for unit in units[:1000]:
        card = unit_card(snapshot, unit)
        card.update(
            target_id=target,
            host=cluster.host.name,
            workdir=snapshot.get("workspace", {}).get("remote_root"),
            work_id=key(target, snapshot["campaign"], snapshot["run_id"], unit["unit_id"]),
            operation_id=OPERATION.get(),
            correlation_id=CORRELATION.get(),
        )
        documents.append(("semantic", key(target, card["semantic_ref"], unit["unit_id"]), card))
        documents.append(("campaign_unit", card["work_id"], card))
        if card.get("attempt_id") and card.get("group_id"):
            attempt_key = key(
                target,
                snapshot["campaign"],
                snapshot["run_id"],
                card["group_id"],
                card["attempt_id"],
                unit["unit_id"],
            )
            existing = store.get("work", attempt_key)
            if existing:
                metadata = {
                    k: v
                    for k, v in card.items()
                    if k
                    in {
                        "semantic_ref",
                        "context",
                        "label",
                        "command_label",
                        "resources",
                        "dependencies",
                        "outputs",
                        "output_contracts",
                        "outputs_omitted",
                        "dependency",
                        "stage",
                        "phase",
                    }
                }
                updated = {**existing, **metadata}
                if card.get("validation_receipt") and card["validation_receipt"] != existing.get(
                    "validation_receipt"
                ):
                    updated.update(
                        validation=card["validation"],
                        validation_receipt=card["validation_receipt"],
                        validation_artifact_signature=card["validation_artifact_signature"],
                    )
                documents.append(("work", attempt_key, updated))
    documents.append(
        (
            "campaign",
            key(target, snapshot["campaign"], snapshot["run_id"]),
            {
                "target_id": target,
                "host": cluster.host.name,
                "campaign": snapshot["campaign"],
                "run_id": snapshot["run_id"],
                "store": snapshot["store"],
                "workdir": snapshot.get("workspace", {}).get("remote_root"),
                "unit_count": snapshot.get("unit_total", len(units)),
                "units_omitted": max(
                    0, snapshot.get("unit_total", len(units)) - min(1000, len(units))
                ),
                "refreshed_at": snapshot.get("refreshed_at"),
            },
        )
    )
    return documents


def committed(cluster: Any, snapshot: dict, events: list[dict]) -> bool:
    try:
        store = ActivityStore()
        documents = cache_snapshot(store, cluster, snapshot)
        target, commit = target_id(cluster.host), snapshot["store"]["transaction_id"]
        store.append(
            subject=Subject(
                target,
                cluster.host.name,
                campaign=snapshot["campaign"],
                run_id=snapshot["run_id"],
                definition_id=snapshot.get("definition_id"),
            ),
            kind="campaign.changed",
            source="campaign.commit",
            provenance="remote_record",
            payload={
                "commit": commit,
                "revision": snapshot["store"]["revision"],
                "event_kinds": [e.get("type") for e in events[:100]],
                "events_omitted": max(0, len(events) - 100),
            },
            source_key=key(target, snapshot["campaign"], snapshot["run_id"], commit),
            evidence=(EvidenceRef("campaign", "campaign.commit", commit),),
            actor=CLIENT.get(),
            operation_id=OPERATION.get(),
            correlation_id=CORRELATION.get(),
            workspace=workspace_for(
                store, target, snapshot.get("workspace", {}).get("remote_root")
            ),
            documents=tuple(documents),
        )
        return True
    except Exception as exc:
        logging.getLogger(__name__).warning("Campaign context unavailable (%s)", type(exc).__name__)
        return False


def attach_events(store: ActivityStore, page: dict, *, max_bytes: int = 128 * 1024) -> dict:
    size = len(encoded(page).encode()) + 128
    omitted = 0
    for event in page.get("events", []):
        subject = event["subject"]
        ref = subject.get("semantic_ref")
        card = (
            store.get("semantic", key(subject["target_id"], ref, subject.get("unit_id")))
            if ref
            else None
        )
        context = {
            "availability": "attached" if card else "unavailable",
            "meaning": "at_event",
            "ref": ref,
        }
        extra = {"work_card": card, "context": context}
        added = len(encoded(extra).encode())
        if size + added <= max_bytes:
            event.update(extra)
            size += added
        else:
            omitted += 1
    page["work_cards_omitted"] = omitted
    return page
