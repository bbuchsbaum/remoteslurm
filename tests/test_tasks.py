"""Durable ensure contracts: identity, recovery, retries, and evidence invalidation."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from remoteslurm.errors import ExecutionMismatch, SessionDied
from remoteslurm.tasks import TaskSpec


def spec_for(sandbox: Path, *, script: str = "#!/bin/bash\necho fit\n") -> TaskSpec:
    output = sandbox / "result.txt"
    output.write_text("valid result\n")
    return TaskSpec(
        name="fit",
        script=script,
        cwd=str(sandbox),
        inputs=[str(sandbox / "proj" / "a.txt")],
        outputs=[str(output)],
        validate=["test", "-s", str(output)],
        environment={"container": "example@sha256:" + "a" * 64},
    )


def run_to_terminal(cluster, spec: TaskSpec) -> dict:
    result = {}
    for _ in range(10):
        result = cluster.ensure(spec)
        if result["state"] in {"VERIFIED", "INVALID", "FAILED", "UNKNOWN"}:
            return result
    raise AssertionError(f"task did not finish: {result}")


def fake_jobs(sandbox: Path) -> dict:
    return json.loads((sandbox / ".fakeslurm.json").read_text())["jobs"]


def drop_fake_jobs(sandbox: Path) -> None:
    path = sandbox / ".fakeslurm.json"
    state = json.loads(path.read_text())
    state["jobs"] = {}
    path.write_text(json.dumps(state))


def test_ensure_reuses_verified_job_and_detects_output_corruption(cluster, sandbox: Path) -> None:
    spec = spec_for(sandbox)
    first = run_to_terminal(cluster, spec)
    assert first["state"] == "VERIFIED"
    assert first["receipt"]["outputs"][0]["sha256"]
    job_id = first["job_id"]

    again = cluster.ensure(spec)
    assert again["state"] == "VERIFIED"
    assert again["job_id"] == job_id
    assert len(again["attempts"]) == 1

    (sandbox / "result.txt").write_text("different but still nonempty\n")
    invalid = cluster.ensure(spec)
    assert invalid["state"] == "INVALID"
    assert "fingerprints changed" in invalid["reason"]
    assert invalid["job_id"] == job_id
    assert len(fake_jobs(sandbox)) == 1


def test_changed_input_produces_a_different_task_identity(cluster, sandbox: Path) -> None:
    spec = spec_for(sandbox)
    original = cluster.ensure(spec)
    (sandbox / "proj" / "a.txt").write_text("changed input\n")
    changed = cluster.ensure(spec)
    assert changed["task_id"] != original["task_id"]
    assert changed["job_id"] != original["job_id"]
    assert len(fake_jobs(sandbox)) == 2


def test_verified_receipt_survives_scheduler_accounting_expiry(cluster, sandbox: Path) -> None:
    spec = spec_for(sandbox)
    verified = run_to_terminal(cluster, spec)
    drop_fake_jobs(sandbox)
    cluster.registry.forget(verified["job_id"])
    recovered = cluster.ensure(spec)
    assert recovered["state"] == "VERIFIED"
    assert recovered["job_id"] == verified["job_id"]
    assert fake_jobs(sandbox) == {}


def test_ensure_recovery_does_not_require_writable_local_registry(
    cluster, sandbox: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = spec_for(sandbox)
    first = cluster.ensure(spec)
    assert first["job_id"]

    def unavailable(*_args, **_kwargs):
        raise PermissionError("local registry unavailable")

    monkeypatch.setattr(cluster.registry, "get", unavailable)
    monkeypatch.setattr(cluster.registry, "put", unavailable)
    recovered = cluster.ensure(spec)

    assert recovered["job_id"] == first["job_id"]
    assert recovered["submitted"] is False


def test_failed_attempt_needs_explicit_retry(cluster, sandbox: Path) -> None:
    spec = spec_for(sandbox, script="#!/bin/bash\n# FAKESLURM_FAIL\nexit 1\n")
    failed = run_to_terminal(cluster, spec)
    assert failed["state"] == "FAILED"
    same = cluster.ensure(spec)
    assert same["job_id"] == failed["job_id"]
    assert len(same["attempts"]) == 1
    retried = cluster.ensure(spec, retry=True)
    assert retried["job_id"] != failed["job_id"]
    assert len(retried["attempts"]) == 2


def test_lost_reply_after_scheduler_acceptance_recovers_original_job(
    make_cluster, sandbox: Path
) -> None:
    spec = spec_for(sandbox)
    first = make_cluster({"REMOTESLURM_TEST_LOSE_AFTER_ACCEPT": "1"})
    with pytest.raises(SessionDied):
        first.ensure(spec)
    accepted_ids = set(fake_jobs(sandbox))
    assert len(accepted_ids) == 1

    recovered = make_cluster().ensure(spec)
    assert "job_id" in recovered, recovered
    assert recovered["job_id"] in accepted_ids
    assert recovered["submitted"] is False
    assert recovered["attempts"][0]["recovered"] is True
    assert set(fake_jobs(sandbox)) == accepted_ids


def test_concurrent_ensure_calls_create_one_attempt(cluster, sandbox: Path) -> None:
    spec = spec_for(sandbox)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: cluster.ensure(spec), range(2)))
    assert len({result["job_id"] for result in results}) == 1
    assert len(fake_jobs(sandbox)) == 1


def test_unknown_attempt_blocks_automatic_resubmission(cluster, sandbox: Path) -> None:
    spec = spec_for(sandbox)
    accepted = cluster.ensure(spec)
    drop_fake_jobs(sandbox)
    cluster.registry.forget(accepted["job_id"])
    unknown = cluster.ensure(spec)
    assert unknown["state"] == "UNKNOWN"
    assert len(unknown["attempts"]) == 1
    blocked = cluster.ensure(spec)
    assert blocked["state"] == "UNKNOWN"
    assert len(blocked["attempts"]) == 1
    retried = cluster.ensure(spec, retry_unknown=True)
    assert retried["state"] in {"PENDING", "RUNNING"}
    assert retried["job_id"] != accepted["job_id"]
    assert len(retried["attempts"]) == 2


def test_remote_stub_identity_must_match_before_submission(
    cluster, sandbox: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = spec_for(sandbox)
    info = cluster.info()
    monkeypatch.setattr(cluster, "info", lambda refresh=False: {**info, "stub_sha": "stale"})
    with pytest.raises(ExecutionMismatch):
        cluster.ensure(spec)
    assert not (sandbox / ".fakeslurm.json").exists()


def test_manifest_loads_script_relative_to_it(tmp_path: Path) -> None:
    (tmp_path / "fit.sh").write_text("echo fit\n")
    manifest = tmp_path / "fit.toml"
    manifest.write_text(
        'version = 1\nname = "fit"\nscript = "fit.sh"\n'
        'outputs = ["~/result.txt"]\nvalidate = ["test", "-s", "~/result.txt"]\n'
    )
    spec = TaskSpec.load(manifest)
    assert spec.script.startswith("#!/bin/bash\necho fit")
