from __future__ import annotations

import json
from pathlib import Path

import pytest

from remoteslurm.campaigns.spec import compile_campaign
from remoteslurm.errors import InvalidArgument


def definition(tmp_path: Path, *, output: bool = True):
    (tmp_path / "run.sh").write_text("#!/bin/sh\ntrue\n")
    stage: dict = {"foreach": "items", "script": "run.sh"}
    if output:
        stage["outputs"] = [
            {
                "name": "result",
                "kind": "file",
                "path": "sub-{subject}/result.txt",
                "min_bytes": 1,
            }
        ]
    return compile_campaign(
        {
            "schema": 1,
            "name": "study",
            "host": "local",
            "workspace": {
                "remote_root": str(tmp_path),
                "output_root": str(tmp_path / "outputs"),
            },
            "inventories": {"items": {"key": ["subject"], "rows": [{"subject": "001"}]}},
            "stages": {"analysis": stage},
        },
        base_dir=tmp_path,
    )


def test_output_only_adoption_preserves_independent_state_axes(cluster, sandbox: Path) -> None:
    campaign = definition(sandbox)
    started = cluster.campaigns.start(campaign, run_id="run-output")
    assert started["stages"][0]["ready"] == 1

    product = sandbox / "outputs" / "sub-001" / "result.txt"
    product.parent.mkdir(parents=True)
    product.write_text("result\n")
    cluster.campaigns.adopt("study", "analysis", run_id="run-output", output_only=True)
    status = cluster.campaigns.status(
        "study", run_id="run-output", refresh=True, include_units=True
    )
    unit = status["units"][0]
    assert unit["execution"]["state"] == "UNBOUND"
    assert unit["artifacts"]["state"] == "PRESENT"
    assert unit["validation"]["state"] == "NOT_RUN"


def test_adopted_job_reconciles_scheduler_without_claiming_outputs(cluster, sandbox: Path) -> None:
    campaign = definition(sandbox)
    cluster.campaigns.start(campaign, run_id="run-job")
    job = cluster.submit("#!/bin/sh\ntrue\n")
    cluster.campaigns.adopt("study", "analysis", run_id="run-job", job_id=job.job_id)

    status = {}
    for _ in range(8):
        status = cluster.campaigns.status(
            "study", run_id="run-job", refresh=True, include_units=True
        )
        if status["units"][0]["execution"]["state"] == "COMPLETED":
            break
    unit = status["units"][0]
    assert unit["execution"]["state"] == "COMPLETED"
    assert unit["artifacts"]["state"] == "MISSING"
    assert unit["validation"]["state"] == "NOT_RUN"
    assert unit["execution"]["scheduler"]["source"] in {"sacct", "scontrol", "registry"}


def test_explicit_retry_can_replace_an_adopted_failed_job(cluster, sandbox: Path) -> None:
    campaign = definition(sandbox, output=False)
    cluster.campaigns.start(campaign, run_id="run-adopted-retry")
    job = cluster.submit("#!/bin/sh\n# FAKESLURM_FAIL\nexit 1\n")
    cluster.campaigns.adopt("study", "analysis", run_id="run-adopted-retry", job_id=job.job_id)
    for _ in range(8):
        status = cluster.campaigns.status(
            "study", run_id="run-adopted-retry", refresh=True, include_units=True
        )
        if status["units"][0]["execution"]["state"] == "FAILED":
            break

    retried = cluster.campaigns.retry(
        "study",
        run_id="run-adopted-retry",
        stage="analysis",
        reason="replace adopted failure",
        apply=True,
    )
    assert retried["apply"]["submitted_groups"] == 1
    jobs = json.loads((sandbox / ".fakeslurm.json").read_text())["jobs"]
    assert len(jobs) == 2


def test_current_telemetry_reports_coverage_for_running_allocations(
    make_cluster, sandbox: Path
) -> None:
    cluster = make_cluster()
    campaign = definition(sandbox, output=False)
    cluster.campaigns.start(campaign, run_id="run-active")
    job = cluster.submit("#!/bin/sh\ntrue\n", cpus_per_task=4)
    cluster.campaigns.adopt("study", "analysis", run_id="run-active", job_id=job.job_id)
    cluster.campaigns.status("study", run_id="run-active", refresh=True)
    status = cluster.campaigns.status("study", run_id="run-active", refresh=True)

    assert status["telemetry"]["active_allocations"] == 1
    assert status["telemetry"]["cpu_coverage"]["eligible"] == 1
    assert status["stages"][0]["execution"]["RUNNING"] == 1


def test_close_archive_restore_semantics(cluster, sandbox: Path) -> None:
    campaign = definition(sandbox)
    cluster.campaigns.start(campaign, run_id="run-life")
    job = cluster.submit("#!/bin/sh\ntrue\n")
    cluster.campaigns.adopt("study", "analysis", run_id="run-life", job_id=job.job_id)
    with pytest.raises(InvalidArgument, match="active or unresolved"):
        cluster.campaigns.close("study", run_id="run-life")

    closed = cluster.campaigns.close(
        "study", run_id="run-life", allow_active=True, reason="handoff"
    )
    assert closed["lifecycle"] == "CLOSED"
    with pytest.raises(InvalidArgument, match="attempts remain active|unresolved"):
        cluster.campaigns.archive("study", run_id="run-life")

    idle = definition(sandbox)
    cluster.campaigns.start(idle, run_id="run-idle")
    cluster.campaigns.close("study", run_id="run-idle")
    archived = cluster.campaigns.archive("study", run_id="run-idle")
    assert archived["lifecycle"] == "ARCHIVED"
    restored = cluster.campaigns.restore("study", run_id="run-idle")
    assert restored["lifecycle"] == "CLOSED"


def test_campaign_name_resolution_requires_one_open_run(cluster, sandbox: Path) -> None:
    campaign = definition(sandbox)
    cluster.campaigns.start(campaign, run_id="run-a")
    assert cluster.campaigns.status("study")["run_id"] == "run-a"
    cluster.campaigns.start(campaign, run_id="run-b")
    with pytest.raises(InvalidArgument, match="2 open runs"):
        cluster.campaigns.status("study")
