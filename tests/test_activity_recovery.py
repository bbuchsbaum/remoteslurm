import json

import pytest

from remoteslurm.activity.producer import target_id
from remoteslurm.activity.recovery import Recovery, import_group
from remoteslurm.activity.store import ActivityStore
from remoteslurm.campaigns.spec import compile_campaign


def definition(sandbox, mode="array"):
    (sandbox / "analysis.sh").write_text("#!/bin/sh\ntrue\n")
    return compile_campaign(
        {
            "schema": 1,
            "name": "recovery",
            "host": "local",
            "workspace": {"remote_root": str(sandbox), "output_root": str(sandbox / "outputs")},
            "inventories": {
                "items": {"key": ["subject"], "rows": [{"subject": str(i)} for i in range(3)]}
            },
            "stages": {
                "analysis": {
                    "foreach": "items",
                    "script": "analysis.sh",
                    "execution": {"mode": mode},
                }
            },
        },
        base_dir=sandbox,
    )


@pytest.mark.parametrize("mode", ["array", "pack"])
def test_recovery_after_acceptance_before_campaign_commit_is_read_only(
    cluster, sandbox, monkeypatch, mode
):
    cluster.campaigns.start(definition(sandbox, mode), run_id="run-1")

    def crash(*args, **kwargs):
        raise RuntimeError("lost client before view publication")

    monkeypatch.setattr(type(cluster.campaigns), "_merge_submission_outcomes", crash)
    with pytest.raises(RuntimeError):
        cluster.campaigns.apply("recovery", run_id="run-1")
    before = json.loads((sandbox / ".fakeslurm.json").read_text())["jobs"]
    # A fresh local machine has no producer records or semantic cache.
    store = ActivityStore(sandbox / "second-client")
    real = cluster.call
    calls = []
    allowed = {"campaign_read", "campaign_ledger_read", "campaign_changes"}

    def readonly(op, **kwargs):
        assert op in allowed, f"passive recovery attempted {op}"
        assert kwargs["_timeout"] == 10
        calls.append(op)
        return real(op, **kwargs)

    monkeypatch.setattr(cluster, "call", readonly)
    first = Recovery(store, cluster).page("recovery", "run-1")
    assert first["imported"] == 3 and len(calls) <= 5
    events = store.query()["events"]
    recovered = [e for e in events if e["kind"] == "submission.recovered"]
    assert len(recovered) == 3
    assert all(e["subject"]["semantic_ref"] for e in recovered)
    assert all(e["subject"]["attempt_id"] and e["occurred_at"] for e in recovered)
    assert len({e["subject"]["unit_id"] for e in recovered}) == 3
    assert all(e["subject"]["packed_member"] is not None for e in recovered) == (mode == "pack")
    assert Recovery(store, cluster).page("recovery", "run-1")["imported"] == 0
    assert len(store.query()["events"]) == len(events)
    assert json.loads((sandbox / ".fakeslurm.json").read_text())["jobs"] == before


def test_historical_failure_cannot_inherit_current_validation_or_replace_retry(cluster, sandbox):
    store = ActivityStore(sandbox / "local-history")
    record = {
        "group_id": "group",
        "current_attempt": "new",
        "contract": {
            "mode": "single",
            "cwd": str(sandbox),
            "mapping": [{"unit_id": "u", "index": 0}],
        },
        "unit_bindings": {"u": {"group_id": "group", "attempt_id": "new"}},
        "attempts": [
            {"attempt_id": "old", "job_id": "1", "state": "FAILED", "created_at": 1},
            {"attempt_id": "new", "job_id": "2", "state": "RUNNING", "created_at": 2},
        ],
    }
    assert import_group(store, cluster, "study", "run", record) == 2
    work = store.documents("work")
    old = next(w for w in work if w["attempt_id"] == "old")
    current = next(w for w in work if w["attempt_id"] == "new")
    assert old["historical"] and old["validation"] == "NOT_RUN"
    assert not current["historical"] and current["execution"] == "RUNNING"
    assert all(w["target_id"] == target_id(cluster.host) for w in work)


def test_restart_after_atomic_import_and_before_global_watermark(cluster, sandbox, monkeypatch):
    store = ActivityStore(sandbox / "restart")
    record = {
        "group_id": "g",
        "current_attempt": "a",
        "contract": {"mapping": [{"unit_id": "u"}]},
        "attempts": [{"attempt_id": "a", "state": "ACCEPTED", "job_id": "12", "created_at": 1}],
    }
    original = store.append

    def crash(**kwargs):
        original(**kwargs)
        raise RuntimeError("crash after durable insertion")

    monkeypatch.setattr(store, "append", crash)
    with pytest.raises(RuntimeError):
        import_group(store, cluster, "study", "run", record)
    fresh = ActivityStore(sandbox / "restart")
    assert import_group(fresh, cluster, "study", "run", record) == 0
    assert len(fresh.query()["events"]) == 1
    record["attempts"][0]["state"] = "RUNNING"
    assert import_group(fresh, cluster, "study", "run", record) == 1
    record["attempts"][0]["state"] = "ACCEPTED"
    assert import_group(fresh, cluster, "study", "run", record) == 1
    assert len(fresh.query()["events"]) == 3


def test_recovery_checkpoints_each_read_under_a_one_call_budget(cluster, sandbox, monkeypatch):
    from remoteslurm.activity.monitor import BudgetExhausted

    cluster.campaigns.start(definition(sandbox), run_id="r1")
    store = ActivityStore(sandbox / "incremental")
    actual = cluster.call
    dispatched = []
    remaining = 1

    def read(op, **kw):
        nonlocal remaining
        if remaining == 0:
            raise BudgetExhausted("next pass")
        remaining -= 1
        dispatched.append((op, kw.get("document")))
        return actual(op, **kw)

    monkeypatch.setattr(cluster, "call", read)
    for _ in range(4):
        remaining = 1
        with pytest.raises(BudgetExhausted):
            Recovery(ActivityStore(sandbox / "incremental"), cluster).page("recovery", "r1")
    remaining = 1
    result = Recovery(store, cluster).page("recovery", "r1")
    assert result["coverage"] == "complete"
    assert dispatched == [
        ("campaign_read", None),
        ("campaign_read", "index.json"),
        ("campaign_read", "units/00000000.json"),
        ("campaign_ledger_read", None),
        ("campaign_changes", None),
    ]


def test_readonly_marker_recovery_normalizes_array_children(cluster, sandbox):
    import time

    job = cluster.submit("#!/bin/sh\ntrue\n", name="rsc-g-a", array="0-2", cwd=str(sandbox))
    before = set(json.loads((sandbox / ".fakeslurm.json").read_text())["jobs"])
    result = cluster.call("attempt_matches", marker="rsc-g-a", since=time.time())
    assert len(result["matches"]) == 1
    assert result["matches"][0]["job_id"] == job.job_id
    assert result["acceptance_time"] == "unknown"
    assert set(json.loads((sandbox / ".fakeslurm.json").read_text())["jobs"]) == before


def test_late_old_outcome_is_historical_and_keeps_replacement_current(cluster, sandbox):
    import copy

    from remoteslurm.campaigns.execution import plan_job_groups

    manager = cluster.campaigns
    manager.start(definition(sandbox), run_id="r1")
    snapshot = manager.store.read_complete_snapshot("recovery", "r1")
    group = plan_job_groups(snapshot, marker_root=str(sandbox / "markers"))[0]
    for unit in snapshot["units"]:
        old = {
            "attempt_id": "old",
            "group_id": group["group_id"],
            "job_id": "11_0",
            "state": "ACCEPTED",
        }
        new = {
            "attempt_id": "new",
            "group_id": group["group_id"],
            "job_id": "12_0",
            "state": "RUNNING",
        }
        unit["execution"].update(
            state="RUNNING", current_attempt=new, attempts=[copy.deepcopy(old), copy.deepcopy(new)]
        )
    manager._commit_snapshot(snapshot, [])
    record = {
        "state": "FAILED",
        "current_attempt": "old",
        "attempts": [{"attempt_id": "old", "state": "FAILED", "job_id": "11"}],
    }
    result = manager._merge_submission_outcomes(
        "recovery", "r1", [{"group": group, "record": record}]
    )
    assert all(
        u["execution"]["current_attempt"]["attempt_id"] == "new"
        and u["execution"]["state"] == "RUNNING"
        for u in result["units"]
    )
    assert all(u["execution"]["attempts"][0]["state"] == "FAILED" for u in result["units"])
