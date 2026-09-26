"""Shared, side-effect-free projection of already reconciled campaign evidence."""

from __future__ import annotations

import math
import time
from collections.abc import Mapping
from datetime import datetime
from typing import Any

PROJECTION_VERSION = 1
MAX_AGE_SECONDS = 120.0
AXES = ("execution", "artifacts", "validation", "dependency", "freshness")
# Order is presentation priority, not source precedence. Every matching category is retained.
RULES = (
    ("conflicting_evidence", "uncertainty", "Resolve conflicting identities or evidence."),
    (
        "unresolved_execution",
        "uncertainty",
        "Refresh or resolve the attempt before changing intent.",
    ),
    (
        "execution_failed",
        "attention",
        "Inspect scheduler evidence and logs before authorizing retries.",
    ),
    (
        "execution_cancelled",
        "attention",
        "Inspect the cancelled attempt and decide whether work is still needed.",
    ),
    (
        "validation_failed",
        "attention",
        "Inspect the validation receipt before changing execution intent.",
    ),
    (
        "validation_stale",
        "attention",
        "Review changed artifacts or contracts and explicitly verify again.",
    ),
    (
        "evidence_unavailable",
        "uncertainty",
        "Restore evidence access and refresh the affected units.",
    ),
    ("evidence_stale", "uncertainty", "Refresh observations before relying on this assessment."),
    ("monitor_unhealthy", "uncertainty", "Inspect monitor health and refresh evidence explicitly."),
    ("dependency_blocked", "attention", "Inspect upstream requirements and their evidence."),
    ("artifacts_missing", "attention", "Inspect declared output paths and the attempt's logs."),
    ("artifacts_changed", "attention", "Inspect changed outputs and explicitly verify them."),
    ("artifacts_unchecked", "uncertainty", "Observe declared outputs before assessing completion."),
    ("artifacts_settling", "uncertainty", "Recheck outputs after their stability window."),
    ("awaiting_validation", "attention", "Run explicit verification when the outputs are ready."),
    ("ready", "attention", "Review execution intent for units with no attempt."),
    ("active", "info", "Continue observing the active attempts and independent evidence axes."),
)


def _epoch(value: Any) -> float | None:
    try:
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return None
            result = parsed.timestamp()
        else:
            result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def freshness_at(evidence: Mapping[str, Any], now: float, max_age: float) -> dict[str, Any]:
    """Age at read time; never rewrite the recorded observation."""
    result = dict(evidence)
    observed = _epoch(evidence.get("observed_at"))
    age = now - observed if observed is not None else None
    result["age_seconds"] = age
    if evidence.get("state") == "FRESH":
        if age is None or age < 0:
            result.update(state="UNAVAILABLE", reason="missing/invalid time or clock skew")
        elif age > max_age:
            result.update(state="STALE", reason="observation age exceeds freshness budget")
    return result


def project_attention(
    snapshot: Mapping[str, Any], *, now: float | None = None, max_age: float = MAX_AGE_SECONDS
) -> dict[str, Any]:
    """Project normalized evidence identically for refresh, recovery, and observer callers.

    Recommendations confer no authorization. No I/O, validation, or workload mutations.
    """
    now = time.time() if now is None else now
    if not math.isfinite(now) or not math.isfinite(max_age) or max_age < 0:
        raise ValueError("finite time and nonnegative freshness budget required")
    selected: dict[str, list[dict[str, Any]]] = {kind: [] for kind, _, _ in RULES}
    counts = dict.fromkeys(selected, 0)
    units = snapshot.get("units", [])
    monitor = snapshot.get("monitor_health")
    monitor_bad = False
    if monitor is not None:
        health = dict(monitor)
        if health.get("state") == "HEALTHY":
            monitor_bad = (
                freshness_at(
                    {"state": "FRESH", "observed_at": health.get("observed_at")}, now, max_age
                )["state"]
                != "FRESH"
            )
        else:
            monitor_bad = health.get("state") != "DISABLED"
    for unit in units:
        axes = {axis: dict(unit.get(axis) or {}) for axis in AXES}
        fresh = freshness_at(axes["freshness"], now, max_age)
        execution, artifacts, validation, dependency = (
            axes[axis].get("state") for axis in AXES[:4]
        )
        stale_validation = validation == "STALE" or (
            validation == "PASSED"
            and any(
                old is not None and new is not None and old != new
                for old, new in (
                    (
                        axes["validation"].get("artifact_signature"),
                        axes["artifacts"].get("signature"),
                    ),
                    (
                        axes["validation"].get("contract_id"),
                        unit.get("contract", {}).get("contract_id"),
                    ),
                )
            )
        )
        conflict = (
            fresh.get("state") == "CONFLICT"
            or dependency == "CONFLICT"
            or axes["execution"].get("scheduler_source") == "conflict"
            or axes["execution"].get("scheduler", {}).get("source") == "conflict"
        )
        unavailable = (
            fresh.get("state") not in {"FRESH", "STALE", "CONFLICT"}
            or artifacts not in {"PRESENT", "MISSING", "CHANGED", "UNCHECKED", "SETTLING"}
            or validation not in {"PASSED", "NOT_RUN", "FAILED", "STALE"}
            or dependency not in {"SATISFIED", "NOT_APPLICABLE", "BLOCKED", "CONFLICT"}
        )
        matches = {
            "conflicting_evidence": conflict,
            "unresolved_execution": execution
            not in {"COMPLETED", "FAILED", "CANCELLED", "UNBOUND", "PENDING", "RUNNING"},
            "execution_failed": execution == "FAILED",
            "execution_cancelled": execution == "CANCELLED",
            "validation_failed": validation == "FAILED",
            "validation_stale": stale_validation,
            "evidence_unavailable": unavailable,
            "evidence_stale": fresh.get("state") == "STALE",
            "monitor_unhealthy": monitor_bad,
            "dependency_blocked": dependency == "BLOCKED",
            "artifacts_missing": artifacts == "MISSING",
            "artifacts_changed": artifacts == "CHANGED",
            "artifacts_unchecked": artifacts == "UNCHECKED",
            "artifacts_settling": artifacts == "SETTLING",
            "awaiting_validation": validation == "NOT_RUN",
            "ready": execution == "UNBOUND" and dependency in {"SATISFIED", "NOT_APPLICABLE"},
            "active": execution in {"PENDING", "RUNNING"},
        }
        # Bounded evidence examples: exact view/receipt references, never output bodies or logs.
        example = {
            "unit_id": unit["unit_id"],
            "observed_fact": {axis: axes[axis].get("state", "UNKNOWN") for axis in AXES},
            "freshness": fresh,
            "evidence": {
                "campaign": snapshot.get("campaign"),
                "run_id": snapshot.get("run_id"),
                "store": snapshot.get("store"),
                "attempt": axes["execution"].get("current_attempt"),
                "scheduler_source": axes["execution"].get("scheduler_source"),
                "scheduler_observed_at": axes["execution"].get("scheduler_observed_epoch"),
                "artifacts_observed_at": axes["artifacts"].get("observed_at"),
                "validation_receipt": axes["validation"].get("receipt_id"),
                "validation_verified_at": axes["validation"].get("verified_at"),
                "source_availability": snapshot.get("source_availability", {}),
            },
        }
        for kind, matches_kind in matches.items():
            if matches_kind:
                counts[kind] += 1
                if len(selected[kind]) < 5:
                    selected[kind].append(example)
    issues = []
    for kind, severity, action in RULES:
        if not counts[kind] and not (kind == "monitor_unhealthy" and monitor_bad):
            continue
        issues.append(
            {
                "key": [snapshot.get("campaign"), snapshot.get("run_id"), kind],
                "kind": kind,
                "severity": severity,
                "count": counts[kind],
                "examples": [item["unit_id"] for item in selected[kind]],
                "facts": selected[kind],
                "reason": action,
                "recommended_action": action,
                "authorization": {"state": "unknown", "grants_authority": False},
            }
        )
    if issues:
        result = dict(issues[0])
    else:
        result = {
            "kind": "complete" if units else "empty",
            "count": len(units),
            "reason": "All units completed; outputs present, validation passed, evidence fresh."
            if units
            else "No units are available to assess.",
            "authorization": {"state": "unknown", "grants_authority": False},
        }
    result.update(projection_version=PROJECTION_VERSION, issues=issues, monitor_health=monitor)
    return result
