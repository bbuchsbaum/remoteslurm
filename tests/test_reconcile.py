from __future__ import annotations

from remoteslurm.evidence import ObservationIdentity, observed, unavailable
from remoteslurm.reconcile import reconcile_job


def test_reconcile_prefers_live_queue_over_other_sources() -> None:
    result = reconcile_job(
        "12",
        [
            observed("sacct", 90, {"state": "COMPLETED"}, durable=True),
            observed("squeue", 100, {"state": "RUNNING"}),
        ],
        now=100,
        grace_seconds=600,
    )
    assert result.state == "RUNNING"
    assert result.source == "squeue"
    assert result.terminal is False


def test_reconcile_preserves_retained_terminal_evidence() -> None:
    result = reconcile_job(
        "12",
        [
            observed("squeue", 1000, None),
            observed("sacct", 1000, None, durable=True),
            observed("registry", 100, {"state": "COMPLETED", "last_seen": 100}),
        ],
        now=10000,
        grace_seconds=600,
    )
    assert result.state == "COMPLETED"
    assert result.source == "registry"
    assert result.terminal is True
    assert result.freshness == "retained"


def test_reconcile_nonterminal_registry_expires_to_unknown() -> None:
    result = reconcile_job(
        "12",
        [
            unavailable("squeue", 1000, "controller unavailable"),
            observed("sacct", 1000, None, durable=True),
            observed("registry", 100, {"state": "RUNNING", "last_seen": 100}),
        ],
        now=10000,
        grace_seconds=600,
    )
    assert result.state == "UNKNOWN"
    assert result.source == "unknown"
    assert result.freshness == "unresolved"
    assert result.sources[0]["available"] is False


def test_reconcile_nonterminal_registry_uses_accounting_grace() -> None:
    result = reconcile_job(
        "12",
        [observed("registry", 100, {"state": "RUNNING", "last_seen": 100})],
        now=200,
        grace_seconds=600,
    )
    assert result.state == "RUNNING"
    assert result.accounting_pending is True
    assert result.terminal is False


def test_reconcile_refuses_job_id_reuse_identity_conflict() -> None:
    result = reconcile_job(
        "12",
        [
            observed(
                "squeue",
                200,
                {"state": "RUNNING"},
                identity=ObservationIdentity("cluster", "12", submit_time="2026-09-15T10:00:00"),
            ),
            observed(
                "registry",
                100,
                {"state": "COMPLETED", "last_seen": 100},
                identity=ObservationIdentity("cluster", "12", submit_time="2026-09-14T10:00:00"),
                durable=True,
            ),
        ],
        now=200,
        grace_seconds=600,
    )
    assert result.state == "UNKNOWN"
    assert result.source == "conflict"
    assert result.freshness == "conflict"
    assert "submission time" in result.payload["conflicts"][0]
