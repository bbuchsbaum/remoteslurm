"""V1 wire contracts. Journal storage and remote collection are deliberately separate."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Literal

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class SemanticRef:
    campaign: str
    run_id: str
    campaign_commit: str
    workspace_commit: str
    contract_digest: str

    def __post_init__(self) -> None:
        if not all(asdict(self).values()):
            raise ValueError("semantic references require complete immutable identity")


@dataclass(frozen=True)
class Subject:
    target_id: str  # resolved endpoint/user/roots identity, not just an SSH alias
    host_alias: str
    campaign: str | None = None
    run_id: str | None = None
    definition_id: str | None = None
    stage: str | None = None
    unit_id: str | None = None
    group_id: str | None = None
    attempt_id: str | None = None
    task_id: str | None = None
    job_id: str | None = None
    array_task_id: str | None = None
    packed_member: str | None = None
    submit_time: str | None = None
    identity_confidence: str = "unknown"
    semantic_ref: SemanticRef | None = None


@dataclass(frozen=True)
class SemanticSnapshot:
    """Immutable JSON captures names, keys, contracts, and attempt-member mappings."""

    ref: SemanticRef
    content_json: str

    def __post_init__(self) -> None:
        content = json.loads(self.content_json)
        if not isinstance(content, dict) or not isinstance(content.get("units"), list):
            raise ValueError("semantic snapshot requires a units array")
        object.__setattr__(
            self, "content_json", json.dumps(content, sort_keys=True, separators=(",", ":"))
        )


def resolve_semantics(
    subject: Subject,
    snapshots: tuple[SemanticSnapshot, ...],
    *,
    meaning: Literal["at_event", "current"] = "at_event",
    current_ref: SemanticRef | None = None,
) -> dict:
    """Resolve only the explicitly pinned meaning; never substitute today's unit for history."""
    if meaning not in {"at_event", "current"}:
        raise ValueError("meaning must be at_event or current")
    ref = subject.semantic_ref if meaning == "at_event" else current_ref
    base = {"meaning": meaning, "ref": asdict(ref) if ref else None}
    if ref is None or (ref.campaign, ref.run_id) != (subject.campaign, subject.run_id):
        return {**base, "availability": "unavailable", "unit": None}
    matching = [snapshot for snapshot in snapshots if snapshot.ref == ref]
    if len({snapshot.content_json for snapshot in matching}) > 1:
        return {
            **base,
            "availability": "stale",
            "unit": None,
            "reason": "conflicting content for immutable reference",
        }
    if not matching:
        return {**base, "availability": "unavailable", "unit": None}
    units = json.loads(matching[0].content_json)["units"]
    # A job id alone is insufficient: arrays and packs share an allocation.
    if subject.unit_id is not None:
        found = [unit for unit in units if unit.get("unit_id") == subject.unit_id]
    else:
        found = units
    if subject.array_task_id is not None or subject.packed_member is not None:
        found = [
            unit
            for unit in found
            if subject.attempt_id is not None
            and subject.job_id is not None
            and any(
                mapping.get("attempt_id") == subject.attempt_id
                and mapping.get("job_id") == subject.job_id
                and mapping.get("array_task_id") == subject.array_task_id
                and mapping.get("packed_member") == subject.packed_member
                for mapping in unit.get("attempt_members", [])
            )
        ]
    elif subject.unit_id is None:
        found = []
    return {
        **base,
        "availability": "attached" if len(found) == 1 else "unavailable",
        "unit": found[0] if len(found) == 1 else None,
    }


@dataclass(frozen=True)
class EvidenceRef:
    authority: Literal[
        "execution_ledger", "scheduler", "filesystem", "validation", "campaign", "local_request"
    ]
    source: str
    identity: str  # commit, receipt, or bounded captured observation id
    digest: str | None = None
    limitations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if (
            self.authority
            not in {
                "execution_ledger",
                "scheduler",
                "filesystem",
                "validation",
                "campaign",
                "local_request",
            }
            or not self.source
            or not self.identity
        ):
            raise ValueError("evidence requires a known authority, source, and identity")


@dataclass(frozen=True)
class ActivityEvent:
    event_id: str
    journal_id: str
    seq: int
    kind: str
    subject: Subject
    provenance: Literal["request", "remote_record", "observation", "derived"]
    source: str
    recorded_at: str
    observed_at: str | None
    evidence_refs: tuple[EvidenceRef, ...]
    payload_json: str
    freshness: Literal["FRESH", "STALE", "PARTIAL", "UNAVAILABLE", "CONFLICT"]
    actor: str | None = None
    operation_id: str | None = None
    occurred_at: str | None = None
    causation_id: str | None = None
    correlation_id: str | None = None
    projection_version: int | None = None
    limitations: tuple[str, ...] = ()
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        from .attention import _epoch

        if self.schema_version != SCHEMA_VERSION or type(self.seq) is not int or self.seq < 1:
            raise ValueError("unsupported activity version or invalid ingestion sequence")
        if self.provenance not in {"request", "remote_record", "observation", "derived"}:
            raise ValueError("unknown provenance")
        if self.freshness not in {"FRESH", "STALE", "PARTIAL", "UNAVAILABLE", "CONFLICT"}:
            raise ValueError("unknown freshness")
        if not self.kind or not self.source or self.recorded_at is None:
            raise ValueError("kind, source, and ingestion time required")
        if not self.event_id or not self.journal_id or not self.subject.target_id:
            raise ValueError("event, journal, and target identities are required")
        for value in (self.recorded_at, self.observed_at, self.occurred_at):
            if value is not None and _epoch(value) is None:
                raise ValueError("timestamps must include a timezone")
        if self.freshness == "FRESH" and self.observed_at is None:
            raise ValueError("fresh evidence requires an observation time")
        if self.provenance == "derived" and (
            self.projection_version is None or not self.evidence_refs
        ):
            raise ValueError("derived events require input evidence and projection version")
        payload = json.loads(self.payload_json)
        if not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object")

    def to_dict(self) -> dict:
        result = asdict(self)
        result["payload"] = json.loads(result.pop("payload_json"))
        return result
