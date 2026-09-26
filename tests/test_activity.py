"""A0 behavior and wire-contract fixtures, independent of collection route."""

import copy
import json
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from remoteslurm.activity.attention import project_attention
from remoteslurm.activity.model import (
    ActivityEvent,
    EvidenceRef,
    SemanticRef,
    SemanticSnapshot,
    Subject,
    resolve_semantics,
)
from remoteslurm.campaigns.manager import CampaignManager
from remoteslurm.evidence import ObservationIdentity, observed
from remoteslurm.reconcile import reconcile_job

NOW = datetime(2026, 9, 26, tzinfo=UTC).timestamp()
STAMP = datetime.fromtimestamp(NOW, UTC).isoformat()


def snapshot():
    return {
        "campaign": "study",
        "run_id": "r1",
        "units": [
            {
                "unit_id": "u1",
                "execution": {"state": "COMPLETED"},
                "artifacts": {"state": "PRESENT", "signature": "artifact1"},
                "validation": {
                    "state": "PASSED",
                    "receipt_id": "receipt1",
                    "artifact_signature": "artifact1",
                    "contract_id": "contract1",
                },
                "contract": {"contract_id": "contract1"},
                "dependency": {"state": "SATISFIED"},
                "freshness": {"state": "FRESH", "observed_at": STAMP},
            }
        ],
    }


@pytest.mark.parametrize(
    "axis,state,kind",
    [
        ("execution", "FAILED", "execution_failed"),
        ("execution", "CANCELLED", "execution_cancelled"),
        ("execution", "UNKNOWN", "unresolved_execution"),
        ("execution", "INTENDED", "unresolved_execution"),
        ("execution", "UNBOUND", "ready"),
        ("artifacts", "MISSING", "artifacts_missing"),
        ("artifacts", "CHANGED", "artifacts_changed"),
        ("artifacts", "ERROR", "evidence_unavailable"),
        ("artifacts", "UNCHECKED", "artifacts_unchecked"),
        ("artifacts", "SETTLING", "artifacts_settling"),
        ("validation", "NOT_RUN", "awaiting_validation"),
        ("validation", "FAILED", "validation_failed"),
        ("validation", "STALE", "validation_stale"),
        ("validation", "ERROR", "evidence_unavailable"),
        ("dependency", "BLOCKED", "dependency_blocked"),
        ("dependency", "CONFLICT", "conflicting_evidence"),
        ("freshness", "STALE", "evidence_stale"),
        ("freshness", "PARTIAL", "evidence_unavailable"),
        ("freshness", "UNAVAILABLE", "evidence_unavailable"),
        ("freshness", "CONFLICT", "conflicting_evidence"),
    ],
)
def test_each_axis_can_prevent_all_clear(axis, state, kind):
    data = snapshot()
    data["units"][0][axis]["state"] = state
    result = project_attention(data, now=NOW)
    assert result["kind"] == kind
    assert result["authorization"] == {"state": "unknown", "grants_authority": False}
    fact = result["facts"][0]
    assert fact["observed_fact"][axis] == state
    assert fact["evidence"]["validation_receipt"] == "receipt1"
    assert fact["freshness"]["observed_at"] == STAMP
    assert result["recommended_action"]


def test_complete_missing_not_run_retains_both_problems():
    data = snapshot()
    data["units"][0]["artifacts"]["state"] = "MISSING"
    data["units"][0]["validation"]["state"] = "NOT_RUN"
    result = project_attention(data, now=NOW)
    assert result["kind"] == "artifacts_missing"
    assert {issue["kind"] for issue in result["issues"]} == {
        "artifacts_missing",
        "awaiting_validation",
    }


def test_active_work_does_not_hide_missing_or_unavailable_evidence():
    data = snapshot()
    data["units"][0]["execution"]["state"] = "RUNNING"
    data["units"][0]["artifacts"]["state"] = "ERROR"
    data["units"][0]["freshness"]["state"] = "UNAVAILABLE"
    result = project_attention(data, now=NOW)
    assert result["kind"] == "evidence_unavailable"
    assert "active" in {issue["kind"] for issue in result["issues"]}


@pytest.mark.parametrize("field,value", [("artifact_signature", "old"), ("contract_id", "old")])
def test_passed_receipt_for_old_content_is_stale(field, value):
    data = snapshot()
    data["units"][0]["validation"][field] = value
    assert project_attention(data, now=NOW)["kind"] == "validation_stale"


def test_age_is_evaluated_on_cached_status_without_writing(monkeypatch):
    data = snapshot()
    data["next_action"] = {"kind": "complete"}
    original = copy.deepcopy(data)
    manager = CampaignManager(MagicMock())
    manager.store = MagicMock()
    manager.store.read_complete_snapshot.return_value = data
    monkeypatch.setattr("remoteslurm.activity.attention.time.time", lambda: NOW + 121)
    result = manager.status("study", run_id="r1")
    assert result["next_action"]["kind"] == "evidence_stale"
    assert data == original
    manager.store.read_complete_snapshot.assert_called_once_with("study", "r1")
    manager.store.commit.assert_not_called()


@pytest.mark.parametrize("stamp", [None, "invalid", "2026-09-26T00:00:00", NOW + 1])
def test_missing_invalid_or_future_time_cannot_be_fresh(stamp):
    data = snapshot()
    data["units"][0]["freshness"]["observed_at"] = stamp
    assert project_attention(data, now=NOW)["kind"] == "evidence_unavailable"


@pytest.mark.parametrize(
    "health",
    [
        {"state": "SUSPENDED"},
        {"state": "UNAVAILABLE"},
        {"state": "HEALTHY", "observed_at": NOW - 121},
    ],
)
def test_monitor_health_is_independent(health):
    data = snapshot()
    data["monitor_health"] = health
    assert project_attention(data, now=NOW)["kind"] == "monitor_unhealthy"


def test_reconciled_conflicting_identity_never_becomes_complete():
    observations = [
        observed(
            source,
            NOW,
            {"state": "COMPLETED"},
            identity=ObservationIdentity(cluster="test", job_id="42", attempt_marker=marker),
        )
        for source, marker in [("squeue", "attempt-a"), ("sacct", "attempt-b")]
    ]
    job = reconcile_job("42", observations, now=NOW, grace_seconds=30)
    data = snapshot()
    data["units"][0]["execution"].update(state=job.state, scheduler=job.to_dict())
    assert project_attention(data, now=NOW)["kind"] == "conflicting_evidence"


def test_collection_route_does_not_change_decision_or_mutate_evidence():
    data = snapshot()
    data["units"][0]["artifacts"]["state"] = "MISSING"
    original = copy.deepcopy(data)
    expected = project_attention(data, now=NOW)
    for route in ("direct_refresh", "passive_recovery", "observer"):
        routed = {**data, "collection_route": route}
        assert project_attention(routed, now=NOW) == expected
    assert data == original
    assert project_attention(snapshot(), now=NOW)["kind"] == "complete"
    assert project_attention({"units": []}, now=NOW)["kind"] == "empty"


def semantic_fixture():
    old = SemanticRef("study", "r1", "campaign-old", "workspace-old", "contract-old")
    new = replace(old, campaign_commit="campaign-new", contract_digest="contract-new")
    unit = {
        "unit_id": "u1",
        "label": "original name",
        "parameter": 1,
        "attempt_members": [
            {"attempt_id": "a1", "job_id": "42", "array_task_id": "3", "packed_member": None},
            {"attempt_id": "a2", "job_id": "43", "array_task_id": None, "packed_member": "2"},
        ],
    }
    snapshots = (
        SemanticSnapshot(old, json.dumps({"units": [unit]})),
        SemanticSnapshot(
            new, json.dumps({"units": [{**unit, "label": "renamed", "parameter": 2}]})
        ),
    )
    subject = Subject(
        "endpoint-user-roots",
        "fixture",
        campaign="study",
        run_id="r1",
        unit_id="u1",
        semantic_ref=old,
    )
    return subject, snapshots, new


def test_historical_semantics_survive_rename_and_reconfiguration():
    subject, snapshots, new = semantic_fixture()
    before = resolve_semantics(subject, snapshots[:1])
    assert resolve_semantics(subject, snapshots) == before
    assert before["availability"] == "attached" and before["unit"]["parameter"] == 1
    current = resolve_semantics(subject, snapshots, meaning="current", current_ref=new)
    assert current["unit"]["label"] == "renamed" and current["unit"]["parameter"] == 2
    # Returned dictionaries cannot alter captured immutable content.
    before["unit"]["label"] = "mutated"
    assert resolve_semantics(subject, snapshots)["unit"]["label"] == "original name"
    assert resolve_semantics(subject, snapshots[1:])["availability"] == "unavailable"
    assert resolve_semantics(subject, snapshots, meaning="current")["availability"] == "unavailable"
    conflict = SemanticSnapshot(subject.semantic_ref, '{"units": []}')
    assert resolve_semantics(subject, (*snapshots, conflict))["availability"] == "stale"


@pytest.mark.parametrize(
    "attempt,job,task,member", [("a1", "42", "3", None), ("a2", "43", None, "2")]
)
def test_array_and_packed_members_resolve_exact_attempt(attempt, job, task, member):
    subject, snapshots, _ = semantic_fixture()
    member_subject = replace(
        subject,
        unit_id=None,
        attempt_id=attempt,
        job_id=job,
        array_task_id=task,
        packed_member=member,
    )
    assert resolve_semantics(member_subject, snapshots)["unit"]["unit_id"] == "u1"
    assert (
        resolve_semantics(replace(member_subject, attempt_id="other"), snapshots)["availability"]
        == "unavailable"
    )
    assert (
        resolve_semantics(replace(subject, unit_id=None, job_id=job), snapshots)["availability"]
        == "unavailable"
    )


def test_activity_envelope_wire_fields_and_derived_evidence():
    subject, _, _ = semantic_fixture()
    event = ActivityEvent(
        event_id="stable-event",
        journal_id="local-journal",
        seq=1,
        kind="attention.raised",
        subject=subject,
        provenance="derived",
        source="attention",
        recorded_at=STAMP,
        observed_at=STAMP,
        evidence_refs=(EvidenceRef("validation", "receipt", "sha256:receipt"),),
        payload_json='{"state":"NOT_RUN"}',
        freshness="FRESH",
        projection_version=1,
        actor="python",
        operation_id="op",
        causation_id="request",
        correlation_id="batch",
    )
    wire = json.loads(json.dumps(event.to_dict()))
    assert wire["schema_version"] == 1 and wire["seq"] == 1
    assert (
        wire["occurred_at"] is None
        and wire["subject"]["semantic_ref"]["campaign_commit"] == "campaign-old"
    )
    assert wire["payload"] == {"state": "NOT_RUN"}
    with pytest.raises(FrozenInstanceError):
        event.seq = 2
    for change in (
        {"schema_version": 2},
        {"provenance": "invented"},
        {"freshness": "invented"},
        {"seq": True},
        {"seq": 0},
        {"evidence_refs": ()},
        {"projection_version": None},
        {"observed_at": None},
        {"recorded_at": "bad"},
        {"payload_json": "[]"},
    ):
        with pytest.raises(ValueError):
            replace(event, **change)


def test_semantic_member_identity_must_agree_with_explicit_unit():
    subject, snapshots, _ = semantic_fixture()
    subject = replace(subject, attempt_id="wrong", job_id="42", array_task_id="3")
    assert resolve_semantics(subject, snapshots)["availability"] == "unavailable"
    with pytest.raises(ValueError):
        replace(subject.semantic_ref, campaign_commit="")
    with pytest.raises(ValueError):
        EvidenceRef("scheduler", "squeue", "")
