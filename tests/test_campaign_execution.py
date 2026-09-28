from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from remoteslurm.campaigns.execution import plan_job_groups, render_group_script
from remoteslurm.campaigns.spec import compile_campaign
from remoteslurm.errors import InvalidArgument, SessionDied


def definition(tmp_path: Path, *, mode: str = "single", count: int = 4, fail: bool = False):
    marker = "# FAKESLURM_FAIL\n" if fail else ""
    (tmp_path / "run.sh").write_text(f'#!/bin/bash\n{marker}echo "$RS_UNIT_ID"\n')
    execution: dict[str, object] = {"mode": mode}
    if mode == "array":
        execution["max_array_size"] = 2
        execution["max_concurrent"] = 2
    elif mode == "pack":
        execution.update(
            max_array_size=2,
            max_concurrent=2,
            units_per_allocation=2,
            max_processes=2,
        )
    return compile_campaign(
        {
            "schema": 1,
            "name": "managed",
            "host": "local",
            "workspace": {
                "remote_root": str(tmp_path),
                "output_root": str(tmp_path / "outputs"),
            },
            "inventories": {
                "items": {
                    "key": ["subject"],
                    "rows": [{"subject": f"{index:03d}"} for index in range(count)],
                }
            },
            "stages": {
                "analysis": {
                    "foreach": "items",
                    "script": "run.sh",
                    "execution": execution,
                }
            },
        },
        base_dir=tmp_path,
    )


def fake_jobs(tmp_path: Path) -> dict:
    path = tmp_path / ".fakeslurm.json"
    return json.loads(path.read_text())["jobs"] if path.exists() else {}


def test_deterministic_plans_split_arrays_and_pack_units(cluster, sandbox: Path) -> None:
    array = definition(sandbox, mode="array", count=5)
    cluster.campaigns.start(array, run_id="array")
    snapshot = cluster.campaigns.store.read_complete_snapshot("managed", "array")
    first = plan_job_groups(snapshot, marker_root="/markers")
    second = plan_job_groups(snapshot, marker_root="/markers")
    assert first == second
    assert [len(group["mapping"]) for group in first] == [2, 2, 1]
    assert [[item["index"] for item in group["mapping"]] for group in first] == [
        [0, 1],
        [0, 1],
        [0],
    ]

    packed = definition(sandbox, mode="pack", count=5)
    cluster.campaigns.start(packed, run_id="pack")
    snapshot = cluster.campaigns.store.read_complete_snapshot("managed", "pack")
    groups = plan_job_groups(snapshot, marker_root="/markers")
    assert [len(group["mapping"]) for group in groups] == [4, 1]
    assert [(item["index"], item["slot"]) for item in groups[0]["mapping"]] == [
        (0, 0),
        (0, 1),
        (1, 0),
        (1, 1),
    ]
    script = render_group_script(
        groups[0], snapshot["stage_specs"]["analysis"], attempt_id="a" * 32
    )
    assert "started.json" in script and "finished.json" in script
    assert "stdout.log" in script and "stderr.log" in script


def test_wrapper_retains_script_bytes_without_indenting_heredocs(cluster, sandbox: Path) -> None:
    content = "#!/bin/bash\ncat <<'EOF'\nexact\nEOF\n"
    (sandbox / "run.sh").write_text(content)
    campaign = compile_campaign(
        {
            "schema": 1,
            "name": "managed",
            "host": "local",
            "workspace": {
                "remote_root": str(sandbox),
                "output_root": str(sandbox / "outputs"),
            },
            "inventories": {"items": {"key": ["subject"], "rows": [{"subject": "001"}]}},
            "stages": {
                "analysis": {
                    "foreach": "items",
                    "script": "run.sh",
                    "execution": {"mode": "single"},
                }
            },
        },
        base_dir=sandbox,
    )
    cluster.campaigns.start(campaign, run_id="heredoc")
    snapshot = cluster.campaigns.store.read_complete_snapshot("managed", "heredoc")
    group = plan_job_groups(snapshot, marker_root="/markers")[0]
    script = render_group_script(group, snapshot["stage_specs"]["analysis"], attempt_id="b" * 32)
    assert script.endswith(content)
    assert "\ncat <<'EOF'\nexact\nEOF\n" in script
    wrapper = sandbox / "heredoc-wrapper.sh"
    wrapper.write_text(script)
    result = subprocess.run(["bash", str(wrapper)], text=True, capture_output=True, check=False)
    assert result.returncode == 0
    assert result.stdout == "exact\n"


def test_packed_wrapper_emits_real_per_unit_markers(cluster, sandbox: Path) -> None:
    campaign = definition(sandbox, mode="pack", count=2)
    cluster.campaigns.start(campaign, run_id="pack-wrapper")
    snapshot = cluster.campaigns.store.read_complete_snapshot("managed", "pack-wrapper")
    marker_root = sandbox / "wrapper-markers"
    group = plan_job_groups(snapshot, marker_root=str(marker_root))[0]
    attempt_id = "c" * 32
    script = render_group_script(group, snapshot["stage_specs"]["analysis"], attempt_id=attempt_id)
    wrapper = sandbox / "pack-wrapper.sh"
    wrapper.write_text(script)
    environment = {**os.environ, "SLURM_ARRAY_TASK_ID": "0"}
    result = subprocess.run(
        ["bash", str(wrapper)], env=environment, text=True, capture_output=True, check=False
    )
    assert result.returncode == 0, result.stderr
    for item in group["mapping"]:
        marker_dir = marker_root / group["group_id"] / attempt_id / item["unit_id"]
        finished = json.loads((marker_dir / "finished.json").read_text())
        assert finished["exit_code"] == 0
        assert item["unit_id"] in (marker_dir / "stdout.log").read_text()


def test_apply_is_idempotent_and_concurrent(cluster, sandbox: Path) -> None:
    campaign = definition(sandbox, count=1)
    started = cluster.campaigns.start(campaign, run_id="once")
    assert "stage_specs" not in started
    assert started["stages"][0]["executor"] == "single"
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(lambda _: cluster.campaigns.apply("managed", run_id="once"), range(2))
        )
    job_ids = {
        group["attempt"]["job_id"]
        for result in results
        for group in result["groups"]
        if group["attempt"].get("job_id")
    }
    assert len(job_ids) == 1
    assert len(fake_jobs(sandbox)) == 1
    assert cluster.campaigns.apply("managed", run_id="once")["applied_groups"] == 0


def test_array_apply_preserves_exact_unit_index_maps(cluster, sandbox: Path) -> None:
    campaign = definition(sandbox, mode="array", count=5)
    cluster.campaigns.start(campaign, run_id="arrays")
    applied = cluster.campaigns.apply("managed", run_id="arrays")
    assert [group["units"] for group in applied["groups"]] == [2, 2, 1]
    snapshot = cluster.campaigns.store.read_complete_snapshot("managed", "arrays")
    bound = {
        unit["unit_id"]: unit["execution"]["current_attempt"]["job_id"]
        for unit in snapshot["units"]
    }
    assert len(bound) == 5
    assert len(set(bound.values())) == 5
    assert len(fake_jobs(sandbox)) == 5


def test_pack_refresh_requires_per_unit_markers(cluster, sandbox: Path) -> None:
    campaign = definition(sandbox, mode="pack", count=2)
    cluster.campaigns.start(campaign, run_id="packed")
    cluster.campaigns.apply("managed", run_id="packed")
    snapshot = cluster.campaigns.store.read_complete_snapshot("managed", "packed")
    first, second = snapshot["units"]
    first_dir = Path(first["execution"]["current_attempt"]["marker_dir"])
    second_dir = Path(second["execution"]["current_attempt"]["marker_dir"])
    first_dir.mkdir(parents=True)
    second_dir.mkdir(parents=True)
    (first_dir / "finished.json").write_text('{"exit_code":0}\n')
    (second_dir / "finished.json").write_text('{"exit_code":7}\n')
    refreshed = cluster.campaigns.status(
        "managed", run_id="packed", refresh=True, include_units=True
    )
    states = {unit["keys"]["subject"]: unit["execution"]["state"] for unit in refreshed["units"]}
    assert states == {"000": "COMPLETED", "001": "FAILED"}
    assert all(
        unit["execution"]["identity_confidence"] == "per_unit_marker" for unit in refreshed["units"]
    )


def test_finished_pack_allocation_without_markers_stays_unknown(cluster, sandbox: Path) -> None:
    cluster.campaigns.start(definition(sandbox, mode="pack", count=2), run_id="markerless")
    cluster.campaigns.apply("managed", run_id="markerless")
    status = None
    for _ in range(8):
        status = cluster.campaigns.status(
            "managed", run_id="markerless", refresh=True, include_units=True
        )
        if all(unit["execution"]["state"] == "UNKNOWN" for unit in status["units"]):
            break
    assert status is not None
    assert all(unit["execution"]["state"] == "UNKNOWN" for unit in status["units"])
    assert all(
        unit["execution"]["identity_confidence"] == "allocation_only" for unit in status["units"]
    )


def test_retry_is_explicit_and_closed_run_rejects_execution(cluster, sandbox: Path) -> None:
    campaign = definition(sandbox, count=1, fail=True)
    cluster.campaigns.start(campaign, run_id="retry")
    cluster.campaigns.apply("managed", run_id="retry")
    status = None
    for _ in range(8):
        status = cluster.campaigns.status(
            "managed", run_id="retry", refresh=True, include_units=True
        )
        if status["units"][0]["execution"]["state"] == "FAILED":
            break
    assert status is not None and status["units"][0]["execution"]["state"] == "FAILED"
    assert cluster.campaigns.apply("managed", run_id="retry")["applied_groups"] == 0
    preview = cluster.campaigns.retry(
        "managed",
        run_id="retry",
        stage="analysis",
        reason="fixed input",
        dry_run=True,
    )
    assert preview["selected"] == 1 and preview["authorized"] == 0
    before = cluster.campaigns.store.read_complete_snapshot("managed", "retry")
    assert "retry_authorizations" not in before["units"][0]["execution"]
    authorized = cluster.campaigns.retry(
        "managed", run_id="retry", stage="analysis", reason="fixed input", apply=True
    )
    assert authorized["apply"]["applied_groups"] == 1
    assert len(fake_jobs(sandbox)) == 2
    cluster.campaigns.close("managed", run_id="retry", allow_active=True, reason="stop")
    with pytest.raises(InvalidArgument, match="open run"):
        cluster.campaigns.apply("managed", run_id="retry")
    with pytest.raises(InvalidArgument, match="open run"):
        cluster.campaigns.retry("managed", run_id="retry", stage="analysis", reason="should fail")


@pytest.mark.parametrize("step", ["intent", "accepted"])
def test_interrupted_apply_recovers_without_duplicate(
    make_cluster, sandbox: Path, step: str
) -> None:
    setup = make_cluster()
    setup.campaigns.start(definition(sandbox, count=1), run_id="crash-" + step)
    setup.close()
    failing = make_cluster({"REMOTESLURM_TEST_CAMPAIGN_SUBMIT_FAIL_STEP": step})
    with pytest.raises(SessionDied):
        failing.campaigns.apply("managed", run_id="crash-" + step)
    accepted = set(fake_jobs(sandbox))
    assert len(accepted) == (1 if step == "accepted" else 0)

    recovered = make_cluster().campaigns.apply("managed", run_id="crash-" + step)
    assert recovered["groups"][0]["state"] == "ACCEPTED"
    assert len(fake_jobs(sandbox)) == 1
    if accepted:
        assert set(fake_jobs(sandbox)) == accepted
        assert recovered["groups"][0]["attempt"]["recovered"] is True


@pytest.mark.parametrize("count", [1, 2])
def test_array_acceptance_recovery_normalizes_scheduler_parent(
    make_cluster, sandbox: Path, count: int
) -> None:
    setup = make_cluster()
    setup.campaigns.start(definition(sandbox, mode="array", count=count), run_id="array-recovery")
    setup.close()
    failing = make_cluster({"REMOTESLURM_TEST_CAMPAIGN_SUBMIT_FAIL_STEP": "accepted"})
    with pytest.raises(SessionDied):
        failing.campaigns.apply("managed", run_id="array-recovery")

    cluster = make_cluster()
    result = cluster.campaigns.apply("managed", run_id="array-recovery")
    assert result["groups"][0]["state"] == "ACCEPTED"
    snapshot = cluster.campaigns.store.read_complete_snapshot("managed", "array-recovery")
    assert all(
        unit["execution"]["current_attempt"]["job_id"] in fake_jobs(sandbox)
        for unit in snapshot["units"]
    )


def test_crash_then_narrowed_selection_recovers_reserved_array(make_cluster, sandbox: Path) -> None:
    setup = make_cluster()
    setup.campaigns.start(definition(sandbox, mode="array", count=2), run_id="narrow-recovery")
    unit_id = setup.campaigns.store.read_complete_snapshot("managed", "narrow-recovery")["units"][
        0
    ]["unit_id"]
    setup.close()
    failing = make_cluster({"REMOTESLURM_TEST_CAMPAIGN_SUBMIT_FAIL_STEP": "accepted"})
    with pytest.raises(SessionDied):
        failing.campaigns.apply("managed", run_id="narrow-recovery")
    jobs_before = set(fake_jobs(sandbox))

    recovered = make_cluster().campaigns.apply("managed", run_id="narrow-recovery", unit_id=unit_id)
    assert set(fake_jobs(sandbox)) == jobs_before
    assert recovered["groups"][0]["units"] == 2


def test_started_pack_unit_becomes_unresolved_when_allocation_terminates(
    cluster, sandbox: Path
) -> None:
    cluster.campaigns.start(definition(sandbox, mode="pack", count=1), run_id="started")
    cluster.campaigns.apply("managed", run_id="started")
    snapshot = cluster.campaigns.store.read_complete_snapshot("managed", "started")
    marker = Path(snapshot["units"][0]["execution"]["current_attempt"]["marker_dir"])
    marker.mkdir(parents=True)
    (marker / "started.json").write_text('{"started_at":1}\n')
    for _ in range(8):
        snapshot = cluster.campaigns.status(
            "managed", run_id="started", refresh=True, include_units=True
        )
    execution = snapshot["units"][0]["execution"]
    assert execution["scheduler"]["terminal"]
    assert execution["state"] == "UNKNOWN"
    assert execution["marker_status"] == "started_without_finish"


def test_retry_starts_without_previous_attempt_scheduler_evidence(cluster, sandbox: Path) -> None:
    cluster.campaigns.start(definition(sandbox, count=1, fail=True), run_id="fresh-retry")
    cluster.campaigns.apply("managed", run_id="fresh-retry")
    for _ in range(8):
        snapshot = cluster.campaigns.status(
            "managed", run_id="fresh-retry", refresh=True, include_units=True
        )
        if snapshot["units"][0]["execution"]["state"] == "FAILED":
            break
    time.sleep(1.1)
    cluster.campaigns.retry(
        "managed", run_id="fresh-retry", stage="analysis", reason="retry", apply=True
    )
    snapshot = cluster.campaigns.status(
        "managed", run_id="fresh-retry", refresh=True, include_units=True
    )
    assert snapshot["units"][0]["execution"]["state"] in {"PENDING", "RUNNING"}


def test_concurrent_retry_authorization_is_consumed_once(
    cluster, sandbox: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import remoteslurm.campaigns.manager as manager

    cluster.campaigns.start(definition(sandbox, count=1, fail=True), run_id="retry-race")
    cluster.campaigns.apply("managed", run_id="retry-race")
    for _ in range(8):
        snapshot = cluster.campaigns.status(
            "managed", run_id="retry-race", refresh=True, include_units=True
        )
        if snapshot["units"][0]["execution"]["state"] == "FAILED":
            break
    cluster.campaigns.retry("managed", run_id="retry-race", stage="analysis", reason="retry")
    first_accepted = threading.Event()
    caller = threading.local()
    ensure = manager.ensure_campaign_attempt

    def signaled_ensure(*args, **kwargs):
        result = ensure(*args, **kwargs)
        if caller.name == "A":
            first_accepted.set()
        elif caller.name == "B":
            assert first_accepted.wait(timeout=10)
        return result

    monkeypatch.setattr(manager, "ensure_campaign_attempt", signaled_ensure)

    def apply(name: str):
        caller.name = name
        return cluster.campaigns.apply("managed", run_id="retry-race")

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(apply, ["A", "B"]))
    assert len(fake_jobs(sandbox)) == 2, results


def test_ambiguous_interruption_requires_duplicate_risk_authorization(
    make_cluster, sandbox: Path
) -> None:
    setup = make_cluster()
    setup.campaigns.start(definition(sandbox, count=1), run_id="ambiguous")
    setup.close()
    failing = make_cluster({"REMOTESLURM_TEST_CAMPAIGN_SUBMIT_FAIL_STEP": "submitting"})
    with pytest.raises(SessionDied):
        failing.campaigns.apply("managed", run_id="ambiguous")
    assert not fake_jobs(sandbox)

    cluster = make_cluster()
    recovered = cluster.campaigns.apply("managed", run_id="ambiguous")
    assert recovered["groups"][0]["state"] == "UNKNOWN"
    assert not fake_jobs(sandbox)
    with pytest.raises(InvalidArgument, match="duplicate-risk"):
        cluster.campaigns.retry(
            "managed", run_id="ambiguous", stage="analysis", reason="controller checked"
        )
    retried = cluster.campaigns.retry(
        "managed",
        run_id="ambiguous",
        stage="analysis",
        reason="controller checked",
        accept_duplicate_risk=True,
        apply=True,
    )
    assert retried["apply"]["groups"][0]["state"] == "ACCEPTED"
    assert len(fake_jobs(sandbox)) == 1


def test_verified_dependency_becomes_eligible_per_unit(cluster, sandbox: Path) -> None:
    (sandbox / "up.sh").write_text("#!/bin/bash\ntrue\n")
    (sandbox / "down.sh").write_text("#!/bin/bash\ntrue\n")
    campaign = compile_campaign(
        {
            "schema": 1,
            "name": "joined",
            "host": "local",
            "workspace": {
                "remote_root": str(sandbox),
                "output_root": str(sandbox / "outputs"),
            },
            "inventories": {
                "items": {
                    "key": ["subject"],
                    "rows": [{"subject": "001"}, {"subject": "002"}],
                }
            },
            "stages": {
                "up": {
                    "foreach": "items",
                    "script": "up.sh",
                    "outputs": [{"name": "result", "path": "up-{subject}.txt"}],
                },
                "down": {
                    "foreach": "items",
                    "script": "down.sh",
                    "needs": [{"stage": "up", "on": ["subject"], "require": "verified"}],
                },
            },
        },
        base_dir=sandbox,
    )
    cluster.campaigns.start(campaign, run_id="joined")
    snapshot = cluster.campaigns.store.read_complete_snapshot("joined", "joined")
    first = [unit for unit in snapshot["units"] if unit["stage"] == "up"][0]
    output = Path(first["artifacts"]["outputs"][0]["path"])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("valid\n")
    cluster.campaigns.verify("joined", run_id="joined", unit_id=first["unit_id"])

    applied = cluster.campaigns.apply("joined", run_id="joined", stage="down")
    assert applied["applied_groups"] == 1
    assert applied["groups"][0]["units"] == 1


def test_closed_run_can_cancel_previously_bound_active_job(make_cluster, sandbox: Path) -> None:
    cluster = make_cluster({"FAKESLURM_FREEZE": "1"})
    cluster.campaigns.start(definition(sandbox, count=1), run_id="closed-active")
    cluster.campaigns.apply("managed", run_id="closed-active")
    cluster.campaigns.close(
        "managed", run_id="closed-active", allow_active=True, reason="operator handoff"
    )
    plan = cluster.campaigns.cancel("managed", run_id="closed-active", all_active=True)
    assert len(plan["job_ids"]) == 1 and plan["applied"] is False
    cancelled = cluster.campaigns.cancel(
        "managed",
        run_id="closed-active",
        all_active=True,
        apply=True,
        confirm=True,
    )
    assert cancelled["applied"] is True
    status = cluster.campaigns.status("managed", run_id="closed-active", include_units=True)
    assert status["units"][0]["execution"]["state"] == "CANCELLED"


def test_cancel_remains_terminal_while_scheduler_accounting_lags(
    make_cluster, sandbox: Path
) -> None:
    cluster = make_cluster(
        {
            "FAKESLURM_FREEZE": "1",
            "FAKESLURM_ACCOUNTING_LAG": "1000",
            "FAKESLURM_SCONTROL_TTL": "0",
        }
    )
    cluster.campaigns.start(definition(sandbox, count=1), run_id="cancel-lag")
    cluster.campaigns.apply("managed", run_id="cancel-lag")
    cluster.campaigns.cancel(
        "managed", run_id="cancel-lag", all_active=True, apply=True, confirm=True
    )

    status = cluster.campaigns.status(
        "managed", run_id="cancel-lag", refresh=True, include_units=True
    )
    execution = status["units"][0]["execution"]
    assert execution["state"] == "CANCELLED"
    assert execution["scheduler_state"] == "CANCELLED"
    assert execution["scheduler_source"] == "registry"
    assert execution["current_attempt"]["scheduler_state"] == "CANCELLED"


def test_scheduler_confirmed_cancellation_can_be_retried(make_cluster, sandbox: Path) -> None:
    cluster = make_cluster({"FAKESLURM_FREEZE": "1"})
    cluster.campaigns.start(definition(sandbox, count=1), run_id="cancel-retry")
    cluster.campaigns.apply("managed", run_id="cancel-retry")
    cluster.campaigns.cancel(
        "managed", run_id="cancel-retry", all_active=True, apply=True, confirm=True
    )

    # Before a refresh, the only evidence is our own scancel request.
    preview = cluster.campaigns.retry(
        "managed", run_id="cancel-retry", stage="analysis", reason="resume", dry_run=True
    )
    assert preview["selected"] == 1
    assert preview["duplicate_risk_units"] == preview["unit_ids"]

    status = cluster.campaigns.status(
        "managed", run_id="cancel-retry", refresh=True, include_units=True
    )
    execution = status["units"][0]["execution"]
    assert execution["state"] == "CANCELLED"
    assert execution["scheduler_source"] in {"squeue", "sacct", "scontrol"}

    preview = cluster.campaigns.retry(
        "managed", run_id="cancel-retry", stage="analysis", reason="resume", dry_run=True
    )
    assert preview["selected"] == 1 and preview["duplicate_risk_units"] == []
    retried = cluster.campaigns.retry(
        "managed", run_id="cancel-retry", stage="analysis", reason="resume", apply=True
    )
    assert retried["authorized"] == 1
    assert retried["apply"]["applied_groups"] == 1
    assert len(fake_jobs(sandbox)) == 2

    snapshot = cluster.campaigns.store.read_complete_snapshot("managed", "cancel-retry")
    execution = snapshot["units"][0]["execution"]
    authorization = execution["retry_authorizations"][-1]
    assert authorization["execution_state"] == "CANCELLED"
    assert authorization["accept_duplicate_risk"] is False
    assert authorization["consumed_by"] == execution["current_attempt"]["attempt_id"]
    assert execution["state"] in {"PENDING", "RUNNING"}
    assert len(execution["attempts"]) == 2


def test_unconfirmed_cancellation_retry_requires_duplicate_risk(
    make_cluster, sandbox: Path
) -> None:
    cluster = make_cluster(
        {
            "FAKESLURM_FREEZE": "1",
            "FAKESLURM_ACCOUNTING_LAG": "1000",
            "FAKESLURM_SCONTROL_TTL": "0",
        }
    )
    cluster.campaigns.start(definition(sandbox, count=1), run_id="cancel-unconfirmed")
    cluster.campaigns.apply("managed", run_id="cancel-unconfirmed")
    cluster.campaigns.cancel(
        "managed", run_id="cancel-unconfirmed", all_active=True, apply=True, confirm=True
    )
    status = cluster.campaigns.status(
        "managed", run_id="cancel-unconfirmed", refresh=True, include_units=True
    )
    assert status["units"][0]["execution"]["scheduler_source"] == "registry"

    with pytest.raises(InvalidArgument, match="duplicate-risk"):
        cluster.campaigns.retry(
            "managed", run_id="cancel-unconfirmed", stage="analysis", reason="resume"
        )
    assert len(fake_jobs(sandbox)) == 1

    retried = cluster.campaigns.retry(
        "managed",
        run_id="cancel-unconfirmed",
        stage="analysis",
        reason="checked sacct by hand",
        accept_duplicate_risk=True,
        apply=True,
    )
    assert retried["apply"]["applied_groups"] == 1
    assert len(fake_jobs(sandbox)) == 2
    snapshot = cluster.campaigns.store.read_complete_snapshot("managed", "cancel-unconfirmed")
    authorization = snapshot["units"][0]["execution"]["retry_authorizations"][-1]
    assert authorization["accept_duplicate_risk"] is True


def test_pack_cancel_keeps_finished_sibling_terminal(make_cluster, sandbox: Path) -> None:
    cluster = make_cluster({"FAKESLURM_FREEZE": "1"})
    cluster.campaigns.start(definition(sandbox, mode="pack", count=2), run_id="pack-cancel")
    cluster.campaigns.apply("managed", run_id="pack-cancel")
    snapshot = cluster.campaigns.store.read_complete_snapshot("managed", "pack-cancel")
    finished, running = snapshot["units"]
    assert (
        finished["execution"]["current_attempt"]["job_id"]
        == running["execution"]["current_attempt"]["job_id"]
    )
    finished["execution"]["state"] = "COMPLETED"
    finished["validation"]["state"] = "PASSED"
    cluster.campaigns._commit_snapshot(snapshot, [])

    cancelled = cluster.campaigns.cancel(
        "managed", run_id="pack-cancel", all_active=True, apply=True, confirm=True
    )
    assert cancelled["affected_units"] == [running["unit_id"]]
    snapshot = cluster.campaigns.store.read_complete_snapshot("managed", "pack-cancel")
    states = {unit["unit_id"]: unit["execution"]["state"] for unit in snapshot["units"]}
    assert states == {finished["unit_id"]: "COMPLETED", running["unit_id"]: "CANCELLED"}

    preview = cluster.campaigns.retry(
        "managed", run_id="pack-cancel", stage="analysis", reason="resume", dry_run=True
    )
    assert preview["unit_ids"] == [running["unit_id"]]
    assert preview["duplicate_risk_units"] == [running["unit_id"]]

    # Per-unit markers decide packed outcomes: an unfinished unit in a terminal allocation is
    # UNKNOWN, so retrying it always requires duplicate-risk acceptance.
    status = cluster.campaigns.status(
        "managed", run_id="pack-cancel", refresh=True, include_units=True
    )
    by_id = {unit["unit_id"]: unit for unit in status["units"]}
    assert by_id[running["unit_id"]]["execution"]["state"] == "UNKNOWN"
    with pytest.raises(InvalidArgument, match="duplicate-risk"):
        cluster.campaigns.retry("managed", run_id="pack-cancel", stage="analysis", reason="resume")


def test_apply_defers_retry_when_refresh_shows_original_job_live(
    make_cluster, sandbox: Path
) -> None:
    cluster = make_cluster({"FAKESLURM_FREEZE": "1"})
    cluster.campaigns.start(definition(sandbox, count=1), run_id="live-retry")
    cluster.campaigns.apply("managed", run_id="live-retry")
    # Stored evidence claims a confirmed cancellation, but the job is still queued.
    snapshot = cluster.campaigns.store.read_complete_snapshot("managed", "live-retry")
    execution = snapshot["units"][0]["execution"]
    execution.update(state="CANCELLED", scheduler_source="sacct")
    execution.setdefault("scheduler", {})["terminal"] = True
    cluster.campaigns._commit_snapshot(snapshot, [])

    retried = cluster.campaigns.retry(
        "managed", run_id="live-retry", stage="analysis", reason="resume", apply=True
    )
    assert retried["authorized"] == 1
    assert retried["apply"]["applied_groups"] == 0
    assert retried["apply"]["voided_retry_units"] == retried["unit_ids"]
    assert len(fake_jobs(sandbox)) == 1
    snapshot = cluster.campaigns.store.read_complete_snapshot("managed", "live-retry")
    execution = snapshot["units"][0]["execution"]
    assert execution["state"] in {"PENDING", "RUNNING"}
    authorization = execution["retry_authorizations"][-1]
    assert "consumed_by" not in authorization
    assert authorization["voided_at"] and "execution" in authorization["voided_reason"]
    events = cluster.campaigns.events("managed", run_id="live-retry")["events"]
    assert any(event["type"] == "retry_authorizations_voided" for event in events)

    # If that original attempt later fails, the voided approval must not resubmit it.
    execution["state"] = "FAILED"
    assert plan_job_groups(snapshot, marker_root="/markers") == []


def test_apply_can_require_exact_current_preflight(cluster, sandbox: Path) -> None:
    campaign = definition(sandbox, count=1)
    receipt = cluster.campaigns.preflight(campaign)
    assert receipt["result"] == "PASSED"
    cluster.campaigns.start(campaign, run_id="qualified")
    applied = cluster.campaigns.apply("managed", run_id="qualified", require_preflight="")
    assert applied["applied_groups"] == 1
    assert applied["preflight_receipt"] == receipt["receipt_id"]


def test_drive_repeats_bounded_apply_and_refresh_without_retry(cluster, sandbox: Path) -> None:
    cluster.campaigns.start(definition(sandbox, count=1), run_id="driven")
    result = cluster.campaigns.drive("managed", run_id="driven", max_passes=5, interval=0)
    assert 1 <= result["passes"] <= 5
    assert len(fake_jobs(sandbox)) == 1
    assert all("validation" not in item for item in result["history"])
