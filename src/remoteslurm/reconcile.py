"""Pure reconciliation shared by jobs, durable tasks, and campaigns."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .evidence import Observation

TERMINAL_STATES = frozenset(
    {
        "BOOT_FAIL",
        "CANCELLED",
        "COMPLETED",
        "DEADLINE",
        "FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PREEMPTED",
        "REVOKED",
        "SPECIAL_EXIT",
        "TIMEOUT",
    }
)
SOURCE_PRIORITY = {"squeue": 40, "sacct": 30, "scontrol": 20, "registry": 10}


@dataclass(frozen=True)
class ReconciledJob:
    """The scheduler conclusion plus an audit-sized account of its evidence."""

    job_id: str
    state: str
    source: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    terminal: bool = False
    accounting_pending: bool = False
    freshness: str = "unresolved"
    observed_at: float | None = None
    sources: tuple[Mapping[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "state": self.state,
            "source": self.source,
            "payload": dict(self.payload),
            "terminal": self.terminal,
            "accounting_pending": self.accounting_pending,
            "freshness": self.freshness,
            "observed_at": self.observed_at,
            "sources": [dict(source) for source in self.sources],
        }


def _source_summary(observation: Observation) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "source": observation.source,
        "available": observation.available,
        "observed_at": observation.observed_at,
        "matched": observation.payload is not None,
    }
    if observation.limitation:
        summary["limitation"] = observation.limitation
    if observation.durable:
        summary["durable"] = True
    if observation.identity is not None:
        summary["identity"] = {
            "cluster": observation.identity.cluster,
            "job_id": observation.identity.job_id,
            "submit_time": observation.identity.submit_time,
            "attempt_marker": observation.identity.attempt_marker,
        }
    return summary


def reconcile_job(
    job_id: str,
    observations: Iterable[Observation],
    *,
    now: float,
    grace_seconds: float,
) -> ReconciledJob:
    """Reconcile scheduler evidence without performing I/O.

    Live queue evidence wins while a job is visible.  Durable accounting wins
    once it exists.  A retained terminal registry observation remains terminal
    after scheduler retention expires.  A retained non-terminal state is only
    used inside the accounting grace window; after that the result is genuinely
    unresolved.
    """

    items = list(observations)
    summaries = tuple(_source_summary(item) for item in items)
    identities = [item.identity for item in items if item.identity is not None and item.payload]
    conflicts: list[str] = []
    clusters = {identity.cluster for identity in identities}
    job_ids = {identity.job_id for identity in identities}
    submit_times = {identity.submit_time for identity in identities if identity.submit_time}
    markers = {identity.attempt_marker for identity in identities if identity.attempt_marker}
    if len(clusters) > 1:
        conflicts.append("cluster identity differs across observations")
    if job_ids - {job_id}:
        conflicts.append("job id differs across observations")
    if len(submit_times) > 1:
        conflicts.append("submission time differs across observations")
    if len(markers) > 1:
        conflicts.append("attempt marker differs across observations")
    if conflicts:
        return ReconciledJob(
            job_id=job_id,
            state="UNKNOWN",
            source="conflict",
            payload={"conflicts": conflicts},
            freshness="conflict",
            observed_at=max((item.observed_at for item in items), default=None),
            sources=summaries,
        )
    matched = [item for item in items if item.available and item.payload is not None]
    matched.sort(
        key=lambda item: (SOURCE_PRIORITY.get(item.source, 0), item.observed_at), reverse=True
    )

    # A currently visible queue row is authoritative even when older accounting
    # happens to contain a previous record with a reused identifier.
    selected = next((item for item in matched if item.source == "squeue"), None)
    if selected is None:
        selected = next((item for item in matched if item.source == "sacct"), None)
    if selected is None:
        selected = next((item for item in matched if item.source == "scontrol"), None)

    registry = next((item for item in matched if item.source == "registry"), None)
    if selected is None and registry is not None:
        payload = dict(registry.payload or {})
        state = str(payload.get("state") or "UNKNOWN")
        last_seen = float(payload.get("last_seen") or registry.observed_at)
        if state in TERMINAL_STATES:
            return ReconciledJob(
                job_id=job_id,
                state=state,
                source="registry",
                payload=payload,
                terminal=True,
                freshness="retained",
                observed_at=last_seen,
                sources=summaries,
            )
        if now - last_seen < grace_seconds:
            return ReconciledJob(
                job_id=job_id,
                state=state,
                source="registry",
                payload=payload,
                terminal=False,
                accounting_pending=True,
                freshness="accounting_pending",
                observed_at=last_seen,
                sources=summaries,
            )

    if selected is None:
        return ReconciledJob(
            job_id=job_id,
            state="UNKNOWN",
            source="unknown",
            freshness="unresolved",
            observed_at=max((item.observed_at for item in items), default=None),
            sources=summaries,
        )

    payload = dict(selected.payload or {})
    state = str(payload.get("state") or "UNKNOWN")
    return ReconciledJob(
        job_id=job_id,
        state=state,
        source=selected.source,
        payload=payload,
        terminal=state in TERMINAL_STATES,
        freshness="current" if selected.source == "squeue" else "retained",
        observed_at=selected.observed_at,
        sources=summaries,
    )
