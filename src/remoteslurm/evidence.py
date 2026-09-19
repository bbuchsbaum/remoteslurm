"""Typed envelopes for scheduler and workspace observations.

Observations preserve where a fact came from, when it was observed, and whether
the source was actually available.  Reconciliation consumes these envelopes;
campaign aggregation must not infer source failures from an empty result.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class ObservationIdentity:
    """Identity carried by an observation to prevent cross-attempt joins."""

    cluster: str
    job_id: str
    submit_time: str | None = None
    attempt_marker: str | None = None


@dataclass(frozen=True)
class Observation:
    """One bounded read from a named source.

    ``available=False`` means the source could not answer.  An available
    observation with ``payload=None`` means the source answered and had no
    matching record.  Those cases are deliberately distinct.
    """

    source: str
    observed_at: float
    available: bool
    payload: Mapping[str, Any] | None = None
    identity: ObservationIdentity | None = None
    limitation: str | None = None
    durable: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        return {k: v for k, v in value.items() if v not in (None, {}, [])}


def observed(
    source: str,
    observed_at: float,
    payload: Mapping[str, Any] | None,
    *,
    identity: ObservationIdentity | None = None,
    limitation: str | None = None,
    durable: bool = False,
    metadata: Mapping[str, Any] | None = None,
) -> Observation:
    """Build an available observation, including an explicit empty result."""

    return Observation(
        source=source,
        observed_at=observed_at,
        available=True,
        payload=payload,
        identity=identity,
        limitation=limitation,
        durable=durable,
        metadata=metadata or {},
    )


def unavailable(source: str, observed_at: float, reason: str) -> Observation:
    """Build an unavailable-source observation."""

    return Observation(
        source=source,
        observed_at=observed_at,
        available=False,
        limitation=reason,
    )
