from types import SimpleNamespace

from remoteslurm import slurm
from remoteslurm.activity.collectors import sample
from remoteslurm.activity.monitor import enroll
from remoteslurm.activity.recovery import import_group
from remoteslurm.activity.store import ActivityStore


def seeded(cluster, sandbox, *, pack=False):
    subscription = enroll(cluster.host, "study", "run-1")
    root = sandbox / ".remoteslurm/campaigns/study/runs/run-1/markers"
    record = {
        "group_id": "g",
        "current_attempt": "a",
        "contract": {
            "mode": "pack" if pack else "single",
            "stage": "analysis",
            "cwd": str(sandbox),
            "marker_root": str(root),
            "mapping": [
                {"unit_id": f"u-{i}", "index": 0, **({"slot": i} if pack else {})}
                for i in range(2 if pack else 1)
            ],
        },
        "attempts": [
            {
                "attempt_id": "a",
                "job_id": "12",
                "state": "ACCEPTED",
                "created_at": 1,
                "marker": "rsc-g-a",
            }
        ],
    }
    store = ActivityStore()
    import_group(store, cluster, "study", "run-1", record)
    return subscription, store


def scheduler_reader(cluster, state="RUNNING", name="rsc-g-a"):
    calls = []

    def call(op, **kwargs):
        calls.append(op)
        assert op in {"squeue", "sacct"}
        assert len(kwargs["jobs"]) <= 1000
        jid = kwargs["jobs"][0]
        values = {"job_id": jid, "name": name, "state": state}
        return {
            "rc": 0,
            "stdout": "|".join(values.get(k, "") for k in slurm.SQUEUE_FIELDS)
            if op == "squeue"
            else "",
        }

    return SimpleNamespace(host=cluster.host, call=call), calls


def test_exact_scheduler_binding_and_wrong_marker_refuses_completion(cluster, sandbox):
    subscription, store = seeded(cluster, sandbox)
    reader, calls = scheduler_reader(cluster)
    sample(store, reader, subscription, "scheduler")
    assert store.documents("work")[0]["execution"] == "RUNNING"
    reader, calls = scheduler_reader(cluster, "COMPLETED", "a-different-attempt")
    sample(store, reader, subscription, "scheduler")
    work = store.documents("work")[0]
    assert work["execution"] == "UNKNOWN" and work["identity_conflict"]
    assert work["source_health"]["scheduler"]["freshness"] == "CONFLICT"
    assert calls == ["squeue", "sacct"]


def test_mixed_pack_completion_is_per_member_not_allocation(cluster, sandbox):
    import json

    subscription, store = seeded(cluster, sandbox, pack=True)
    for work in store.documents("work"):
        path = __import__("pathlib").Path(work["marker_dir"])
        path.mkdir(parents=True)
        code = 0 if work["unit_id"] == "u-0" else 3
        (path / "finished.json").write_text(
            json.dumps({"unit_id": work["unit_id"], "exit_code": code, "finished_at": 10})
        )
    result = sample(store, cluster, subscription, "artifacts")
    assert result["remote_calls"] == 1
    assert {w["unit_id"]: w["execution"] for w in store.documents("work")} == {
        "u-0": "COMPLETED",
        "u-1": "FAILED",
    }
    reader, calls = scheduler_reader(cluster, "RUNNING")
    sample(store, reader, subscription, "scheduler")
    assert {w["unit_id"]: w["execution"] for w in store.documents("work")} == {
        "u-0": "COMPLETED",
        "u-1": "FAILED",
    }
    assert calls == ["squeue", "sacct"]  # one shared child allocation


def test_output_change_invalidates_cached_validation_without_running_validator(
    cluster, sandbox, monkeypatch
):
    subscription, store = seeded(cluster, sandbox)
    output = sandbox / "result.txt"
    output.write_text("original")
    work = store.documents("work")[0]
    work["output_contracts"] = [
        {"name": "result", "path": str(output), "kind": "file", "min_bytes": 1}
    ]
    store.save("work", work["work_id"], work)
    sample(store, cluster, subscription, "artifacts")
    work = store.get("work", work["work_id"])
    assert work["artifacts"] == "PRESENT" and work["validation"] == "NOT_RUN"
    work.update(validation="PASSED", validation_artifact_signature=work["artifact_signature"])
    store.save("work", work["work_id"], work)
    output.write_text("changed output, much longer")
    actual, calls = cluster.call, []

    def readonly(op, **kw):
        assert op == "contract_observe"
        assert kw["max_hash_bytes"] == 64 * 1024 * 1024
        calls.append(op)
        return actual(op, **kw)

    monkeypatch.setattr(cluster, "call", readonly)
    sample(store, cluster, subscription, "artifacts")
    work = store.get("work", work["work_id"])
    assert work["artifacts"] == "CHANGED" and work["validation"] == "STALE"
    assert calls == ["contract_observe"]


def test_missing_usage_is_unavailable_not_zero(cluster, sandbox):
    subscription, store = seeded(cluster, sandbox)
    work = store.documents("work")[0]
    work["execution"] = "RUNNING"
    store.save("work", work["work_id"], work)
    reader = SimpleNamespace(host=cluster.host, call=lambda *a, **k: {"rc": 0, "stdout": ""})
    sample(store, reader, subscription, "usage")
    work = store.get("work", work["work_id"])
    assert work["usage"] is None
    assert work["source_health"]["resources"]["freshness"] == "UNAVAILABLE"


def test_contract_hash_budget_reports_gap_instead_of_reading_unbounded_file(cluster, sandbox):
    path = sandbox / "larger-than-budget"
    path.write_bytes(b"x" * 100)
    result = cluster.call(
        "contract_observe", items=[{"path": str(path), "sha256": True}], max_hash_bytes=64
    )
    assert result["items"][0]["matches"][0]["error"]["code"] == "too_large"
