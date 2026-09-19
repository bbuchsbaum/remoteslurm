"""Live WP5 campaign execution qualification for an explicitly configured cluster."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from remoteslurm import Cluster
from remoteslurm.campaigns.spec import compile_campaign
from remoteslurm.config import Config
from remoteslurm.errors import SessionDied
from remoteslurm.identity import control_identity
from remoteslurm.transport import SSHTransport

pytestmark = pytest.mark.skipif(
    not os.environ.get("REMOTESLURM_LIVE"), reason="REMOTESLURM_LIVE not set"
)
HOST = os.environ.get("REMOTESLURM_LIVE_HOST")
CWD = os.environ.get("REMOTESLURM_LIVE_CWD")
PARTITION = os.environ.get("REMOTESLURM_LIVE_PARTITION")
TIME = os.environ.get("REMOTESLURM_LIVE_TIME")
QOS = os.environ.get("REMOTESLURM_LIVE_QOS")
EVIDENCE_PATH = os.environ.get("REMOTESLURM_LIVE_EVIDENCE")


class _CampaignLostAcceptanceTransport(SSHTransport):
    def remote_bootstrap_script(self) -> str:
        script = super().remote_bootstrap_script()
        command = 'exec "$PY" -u "$P"'
        assert command in script
        return script.replace(
            command,
            'export REMOTESLURM_TEST_CAMPAIGN_SUBMIT_FAIL_STEP=accepted;exec "$PY" -u "$P"',
            1,
        )


@pytest.fixture(scope="module")
def cluster() -> Iterator[Cluster]:
    value = Cluster.connect(HOST)
    try:
        yield value
    finally:
        value.close()


def _resources() -> dict[str, Any]:
    result: dict[str, Any] = {
        "nodes": 1,
        "ntasks": 1,
        "cpus_per_task": 1,
    }
    if PARTITION:
        result["partition"] = PARTITION
    if TIME:
        result["time"] = TIME
    if QOS:
        result["qos"] = QOS
    return result


def _candidate_source_sha256() -> str:
    root = Path(__file__).resolve().parents[2]
    digest = hashlib.sha256()
    for path in sorted((root / "src" / "remoteslurm").rglob("*.py")):
        relative = path.relative_to(root).as_posix().encode()
        content = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _definition(
    tmp_path: Path,
    cluster: Cluster,
    *,
    name: str,
    remote_root: str,
    script: str,
    mode: str = "single",
    rows: tuple[str, ...] = ("one",),
):
    script_path = tmp_path / f"{name}.sh"
    script_path.write_text("#!/bin/bash\n" + script)
    execution: dict[str, Any] = {"mode": mode}
    if mode == "array":
        execution.update(max_array_size=8, max_concurrent=2)
    elif mode == "pack":
        execution.update(
            max_array_size=4,
            max_concurrent=1,
            units_per_allocation=2,
            max_processes=2,
        )
    return compile_campaign(
        {
            "schema": 1,
            "name": name,
            "host": cluster.host.name,
            "workspace": {
                "remote_root": remote_root,
                "output_root": f"{remote_root}/outputs",
                "deployment": "mutable",
            },
            "inventories": {
                "items": {
                    "key": ["subject"],
                    "rows": [{"subject": subject} for subject in rows],
                }
            },
            "stages": {
                "analysis": {
                    "foreach": "items",
                    "script": script_path.name,
                    "execution": execution,
                    "resources": _resources(),
                }
            },
        },
        base_dir=tmp_path,
        host_config=cluster.host,
    )


def _dependency_definition(
    tmp_path: Path,
    cluster: Cluster,
    *,
    name: str,
    remote_root: str,
):
    up_script = tmp_path / f"{name}-up.sh"
    up_script.write_text("#!/bin/bash\ntrue\n")
    down_script = tmp_path / f"{name}-down.sh"
    down_script.write_text("#!/bin/bash\ntrue\n")
    return compile_campaign(
        {
            "schema": 1,
            "name": name,
            "host": cluster.host.name,
            "workspace": {
                "remote_root": remote_root,
                "output_root": f"{remote_root}/outputs",
                "deployment": "mutable",
            },
            "inventories": {
                "items": {
                    "key": ["subject"],
                    "rows": [{"subject": "ready"}, {"subject": "blocked"}],
                }
            },
            "stages": {
                "up": {
                    "foreach": "items",
                    "script": up_script.name,
                    "outputs": [
                        {
                            "name": "result",
                            "kind": "file",
                            "path": "up-{subject}.txt",
                            "min_bytes": 1,
                        }
                    ],
                    "resources": _resources(),
                },
                "down": {
                    "foreach": "items",
                    "script": down_script.name,
                    "needs": [{"stage": "up", "on": ["subject"], "require": "verified"}],
                    "resources": _resources(),
                },
            },
        },
        base_dir=tmp_path,
        host_config=cluster.host,
    )


def _wait_for_states(
    cluster: Cluster,
    name: str,
    run_id: str,
    expected: set[str],
    *,
    timeout: float = 900,
    terminal_grace: float = 0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    terminal_mismatch_at: float | None = None
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = cluster.campaigns.status(name, run_id=run_id, refresh=True, include_units=True)
        states = {unit["execution"]["state"] for unit in last["units"]}
        if states == expected:
            return last
        if states <= {"COMPLETED", "FAILED", "CANCELLED", "UNKNOWN"} and not (
            states & {"PENDING", "RUNNING", "INTENDED"}
        ):
            terminal_mismatch_at = terminal_mismatch_at or time.monotonic()
            if time.monotonic() - terminal_mismatch_at >= terminal_grace:
                break
        else:
            terminal_mismatch_at = None
        time.sleep(5)
    raise AssertionError({"expected": sorted(expected), "observed": sorted(states), "status": last})


def _campaign_path(cluster: Cluster, name: str) -> str:
    root = cluster.call("expandpath", path=cluster.host.campaign_dir or "~/.remoteslurm/campaigns")[
        "path"
    ]
    return f"{str(root).rstrip('/')}/{name}"


def _ledger(cluster: Cluster, name: str, run_id: str) -> dict[str, Any]:
    path = f"{_campaign_path(cluster, name)}/runs/{run_id}/execution/ledger.json"
    return json.loads(cluster.read_text(path, max_bytes=8 * 1024 * 1024))


def _attempt_jobs(snapshot: dict[str, Any]) -> list[str]:
    return sorted(
        {
            str(attempt["group_job_id"])
            for unit in snapshot["units"]
            for attempt in unit["execution"].get("attempts", [])
            if attempt.get("group_job_id")
        }
    )


@pytest.mark.timeout(1800)
def test_live_wp5_campaign_execution_matrix(cluster: Cluster, tmp_path: Path) -> None:
    if CWD is None:
        pytest.skip("REMOTESLURM_LIVE_CWD is required for live campaign execution")

    suffix = uuid.uuid4().hex[:10]
    work_root = f"{CWD.rstrip('/')}/remoteslurm-wp5-{suffix}"
    run_id = "qualification"
    names: list[str] = []
    active_jobs: set[str] = set()
    info = cluster.info(refresh=True)
    evidence: dict[str, Any] = {
        "schema": 1,
        "qualification": "WP5 campaign execution",
        "result": "INCOMPLETE",
        "recorded_at": datetime.now(UTC).isoformat(),
        "host": info["hostname"],
        "slurm_version": info.get("slurm_version"),
        "client_stub_sha": control_identity()["stub_sha"],
        "remote_stub_sha": info.get("stub_sha"),
        "git_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "candidate": "working tree",
        "candidate_source_sha256": _candidate_source_sha256(),
        "qualification_test_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "resources": _resources(),
        "scenarios": {},
    }
    cluster.mkdir(work_root)
    try:
        # Lost response + array-parent recovery + selection-independent reservation.
        array_name = f"wp5array{suffix}"
        names.append(array_name)
        array_root = f"{work_root}/array"
        cluster.mkdir(array_root)
        array_definition = _definition(
            tmp_path,
            cluster,
            name=array_name,
            remote_root=array_root,
            mode="array",
            rows=("ok", "bad"),
            script='case "$RS_PARAMS_JSON" in *bad*) exit 7 ;; *) exit 0 ;; esac\n',
        )
        cluster.campaigns.start(array_definition, run_id=run_id)
        first_unit = cluster.campaigns.store.read_complete_snapshot(array_name, run_id)["units"][0][
            "unit_id"
        ]
        host = Config.load().host(HOST)
        fault_cluster = Cluster(
            host,
            _CampaignLostAcceptanceTransport(
                alias=host.ssh,
                mfa=host.mfa,
                python=host.python,
                install_dir=host.install_dir,
                control_path=host.control_path,
                control_persist=host.control_persist,
                extra_ssh_opts=list(host.ssh_opts),
            ),
        )
        try:
            try:
                unexpected = fault_cluster.campaigns.apply(array_name, run_id=run_id)
            except SessionDied:
                pass
            else:
                for group in unexpected.get("groups", []):
                    attempt = group.get("attempt") or {}
                    if attempt.get("job_id"):
                        active_jobs.add(str(attempt["job_id"]))
                raise AssertionError({"fault_injection_did_not_terminate_stub": unexpected})
        finally:
            fault_cluster.close()
        recovered = cluster.campaigns.apply(array_name, run_id=run_id, unit_id=first_unit)
        assert recovered["submitted_groups"] == 0
        assert recovered["groups"][0]["units"] == 2
        assert recovered["groups"][0]["attempt"]["recovered"] is True
        array_status = _wait_for_states(cluster, array_name, run_id, {"COMPLETED", "FAILED"})
        array_jobs = _attempt_jobs(array_status)
        active_jobs.update(array_jobs)
        array_ledger = _ledger(cluster, array_name, run_id)
        assert len(array_ledger["groups"]) == 1
        assert len(next(iter(array_ledger["groups"].values()))["attempts"]) == 1
        evidence["scenarios"]["array_lost_response"] = {
            "definition_id": array_status["definition_id"],
            "run_id": run_id,
            "job_ids": array_jobs,
            "states": sorted(unit["execution"]["state"] for unit in array_status["units"]),
            "recovered": True,
            "remote_attempts": 1,
        }

        # Packed allocation with decisive per-unit completion markers.
        pack_name = f"wp5pack{suffix}"
        names.append(pack_name)
        pack_root = f"{work_root}/pack"
        cluster.mkdir(pack_root)
        pack_definition = _definition(
            tmp_path,
            cluster,
            name=pack_name,
            remote_root=pack_root,
            mode="pack",
            rows=("one", "two"),
            script="sleep 2\nexit 0\n",
        )
        cluster.campaigns.start(pack_definition, run_id=run_id)
        pack_apply = cluster.campaigns.apply(pack_name, run_id=run_id)
        pack_status = _wait_for_states(cluster, pack_name, run_id, {"COMPLETED"}, terminal_grace=60)
        pack_jobs = _attempt_jobs(pack_status)
        active_jobs.update(pack_jobs)
        assert len(pack_apply["groups"]) == 1 and pack_apply["groups"][0]["units"] == 2
        assert all(
            unit["execution"].get("marker_status") == "finished"
            and unit["execution"].get("identity_confidence") == "per_unit_marker"
            for unit in pack_status["units"]
        )
        evidence["scenarios"]["packed_markers"] = {
            "definition_id": pack_status["definition_id"],
            "run_id": run_id,
            "job_ids": pack_jobs,
            "states": [unit["execution"]["state"] for unit in pack_status["units"]],
            "marker_statuses": [
                unit["execution"].get("marker_status") for unit in pack_status["units"]
            ],
        }

        # A single retry authorization under concurrent apply creates one replacement.
        retry_name = f"wp5retry{suffix}"
        names.append(retry_name)
        retry_root = f"{work_root}/retry"
        cluster.mkdir(retry_root)
        retry_definition = _definition(
            tmp_path,
            cluster,
            name=retry_name,
            remote_root=retry_root,
            script=('if mkdir "$PWD/retry-once.lock" 2>/dev/null; then exit 9; fi\nexit 0\n'),
        )
        cluster.campaigns.start(retry_definition, run_id=run_id)
        cluster.campaigns.apply(retry_name, run_id=run_id)
        _wait_for_states(cluster, retry_name, run_id, {"FAILED"})
        time.sleep(1.1)
        authorization = cluster.campaigns.retry(
            retry_name, run_id=run_id, stage="analysis", reason="live retry qualification"
        )
        with ThreadPoolExecutor(max_workers=2) as pool:
            apply_results = list(
                pool.map(lambda _: cluster.campaigns.apply(retry_name, run_id=run_id), range(2))
            )
        retry_status = _wait_for_states(cluster, retry_name, run_id, {"COMPLETED"})
        retry_jobs = _attempt_jobs(retry_status)
        active_jobs.update(retry_jobs)
        retry_attempts = retry_status["units"][0]["execution"]["attempts"]
        retry_ledger = _ledger(cluster, retry_name, run_id)
        assert len(retry_attempts) == 2
        assert len(retry_jobs) == 2
        assert len(retry_ledger["consumed_authorizations"]) == 1
        assert sum(result["submitted_groups"] for result in apply_results) == 1
        evidence["scenarios"]["concurrent_retry"] = {
            "definition_id": retry_status["definition_id"],
            "run_id": run_id,
            "authorization_id": authorization["authorization_id"],
            "job_ids": retry_jobs,
            "attempt_count": len(retry_attempts),
            "consumed_authorizations": len(retry_ledger["consumed_authorizations"]),
            "state": retry_status["units"][0]["execution"]["state"],
        }

        # Verified dependencies release matching units without coupling failed siblings.
        dependency_name = f"wp5dependency{suffix}"
        names.append(dependency_name)
        dependency_root = f"{work_root}/dependency"
        cluster.mkdir(dependency_root)
        dependency_definition = _dependency_definition(
            tmp_path,
            cluster,
            name=dependency_name,
            remote_root=dependency_root,
        )
        cluster.campaigns.start(dependency_definition, run_id=run_id)
        cluster.campaigns.apply(dependency_name, run_id=run_id, stage="up")
        _wait_for_states(cluster, dependency_name, run_id, {"COMPLETED", "UNBOUND"})
        dependency_snapshot = cluster.campaigns.store.read_complete_snapshot(
            dependency_name, run_id
        )
        ready_upstream = next(
            unit
            for unit in dependency_snapshot["units"]
            if unit["stage"] == "up" and unit["keys"]["subject"] == "ready"
        )
        cluster.write(ready_upstream["artifacts"]["outputs"][0]["path"], "ready\n")
        verification = cluster.campaigns.verify(dependency_name, run_id=run_id, stage="up")
        assert verification["counts"] == {"PASSED": 1, "FAILED": 1}
        dependency_apply = cluster.campaigns.apply(dependency_name, run_id=run_id, stage="down")
        assert dependency_apply["submitted_groups"] == 1
        assert dependency_apply["groups"][0]["units"] == 1
        dependency_status = _wait_for_states(
            cluster, dependency_name, run_id, {"COMPLETED", "UNBOUND"}
        )
        dependency_jobs = _attempt_jobs(dependency_status)
        active_jobs.update(dependency_jobs)
        down_units = {
            unit["keys"]["subject"]: unit
            for unit in dependency_status["units"]
            if unit["stage"] == "down"
        }
        assert down_units["ready"]["dependency"]["state"] == "SATISFIED"
        assert down_units["ready"]["execution"]["state"] == "COMPLETED"
        assert down_units["blocked"]["dependency"]["state"] == "BLOCKED"
        assert down_units["blocked"]["execution"]["state"] == "UNBOUND"
        evidence["scenarios"]["verified_dependency"] = {
            "definition_id": dependency_status["definition_id"],
            "run_id": run_id,
            "job_ids": dependency_jobs,
            "validation_counts": verification["counts"],
            "downstream": {
                subject: {
                    "dependency_state": unit["dependency"]["state"],
                    "execution_state": unit["execution"]["state"],
                }
                for subject, unit in sorted(down_units.items())
            },
        }

        # Bounded campaign cancellation records and terminates the accepted allocation.
        cancel_name = f"wp5cancel{suffix}"
        names.append(cancel_name)
        cancel_root = f"{work_root}/cancel"
        cluster.mkdir(cancel_root)
        cancel_definition = _definition(
            tmp_path,
            cluster,
            name=cancel_name,
            remote_root=cancel_root,
            script="sleep 300\n",
        )
        cluster.campaigns.start(cancel_definition, run_id=run_id)
        cluster.campaigns.apply(cancel_name, run_id=run_id)
        cancel_snapshot = cluster.campaigns.store.read_complete_snapshot(cancel_name, run_id)
        cancel_job = str(
            cancel_snapshot["units"][0]["execution"]["current_attempt"]["group_job_id"]
        )
        active_jobs.add(cancel_job)
        preview = cluster.campaigns.cancel(cancel_name, run_id=run_id, stage="analysis")
        assert preview["job_ids"] == [cancel_job] and preview["applied"] is False
        cancelled = cluster.campaigns.cancel(
            cancel_name, run_id=run_id, stage="analysis", apply=True, confirm=True
        )
        assert cancelled["applied"] is True
        deadline = time.monotonic() + 300
        scheduler_state = None
        while time.monotonic() < deadline:
            scheduler = cluster.job_status(cancel_job, refresh=True)
            scheduler_state = scheduler.state
            if scheduler.terminal:
                break
            time.sleep(5)
        assert scheduler_state == "CANCELLED"
        cancel_status = cluster.campaigns.status(
            cancel_name, run_id=run_id, refresh=True, include_units=True
        )
        assert cancel_status["units"][0]["execution"]["state"] == "CANCELLED"
        evidence["scenarios"]["cancellation"] = {
            "definition_id": cancel_status["definition_id"],
            "run_id": run_id,
            "job_ids": [cancel_job],
            "scheduler_state": scheduler_state,
            "campaign_state": cancel_status["units"][0]["execution"]["state"],
        }

        evidence["result"] = "PASSED"
    finally:
        # A lost response can precede the local merge. Recover accepted ids from each durable
        # ledger before cancelling, then remove only this test's isolated records and workspace.
        for name in names:
            try:
                ledger = _ledger(cluster, name, run_id)
                active_jobs.update(
                    str(attempt["job_id"])
                    for group in ledger.get("groups", {}).values()
                    for attempt in group.get("attempts", [])
                    if attempt.get("job_id")
                )
            except Exception:
                pass
        for job_id in sorted(active_jobs):
            try:
                status = cluster.job_status(job_id, refresh=True)
                if not status.terminal:
                    cluster.cancel(job_id, confirm=True)
            except Exception:
                pass
        for name in names:
            try:
                cluster.rm(_campaign_path(cluster, name), recursive=True)
            except Exception:
                pass
        try:
            cluster.rm(work_root, recursive=True)
        except Exception:
            pass
        if EVIDENCE_PATH:
            Path(EVIDENCE_PATH).write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
