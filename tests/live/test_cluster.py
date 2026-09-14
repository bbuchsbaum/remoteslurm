"""Live tests for any configured cluster with an established SSH session."""

from __future__ import annotations

import os
import time
import uuid

import pytest

from remoteslurm import Cluster, TaskSpec
from remoteslurm.config import Config
from remoteslurm.errors import SessionDied
from remoteslurm.transport import SSHTransport

pytestmark = pytest.mark.skipif(
    not os.environ.get("REMOTESLURM_LIVE"), reason="REMOTESLURM_LIVE not set"
)
HOST = os.environ.get("REMOTESLURM_LIVE_HOST")  # None -> default host
PARTITION = os.environ.get("REMOTESLURM_LIVE_PARTITION")
TIME = os.environ.get("REMOTESLURM_LIVE_TIME")
CWD = os.environ.get("REMOTESLURM_LIVE_CWD")
TEMPLATE = os.environ.get("REMOTESLURM_LIVE_TEMPLATE")
FAULTS = os.environ.get("REMOTESLURM_LIVE_FAULTS")


class _LostAcceptanceTransport(SSHTransport):
    def remote_bootstrap_script(self) -> str:
        script = super().remote_bootstrap_script()
        command = 'exec "$PY" -u "$P"'
        assert command in script
        return script.replace(
            command,
            'export REMOTESLURM_TEST_LOSE_AFTER_ACCEPT=1;exec "$PY" -u "$P"',
            1,
        )


@pytest.fixture(scope="module")
def cluster() -> Cluster:
    return Cluster.connect(HOST)


@pytest.fixture
def workdir(cluster: Cluster):
    if CWD is None:
        yield None
        return
    path = f"{CWD.rstrip('/')}/remoteslurm-live-{uuid.uuid4().hex[:12]}"
    cluster.mkdir(path)
    try:
        yield path
    finally:
        cluster.rm(path, recursive=True)


def test_basics(cluster: Cluster) -> None:
    assert cluster.ping()["protocol"] == 2
    info = cluster.info()
    assert info["slurm_tools"]["sbatch"]
    assert cluster.ls("~")["path"] == info["home"]


def test_job_lifecycle(cluster: Cluster, workdir: str | None) -> None:
    options = {
        key: value
        for key, value in {
            "partition": PARTITION,
            "time": TIME,
            "cwd": workdir,
            "template": TEMPLATE,
            "nodes": 1,
            "ntasks": 1,
        }.items()
        if value
    }
    job = cluster.submit(
        "#!/bin/bash\necho live-ok\n",
        name="rs_live",
        **options,
    )
    st = job.wait(poll=5, timeout=600)
    assert st.state == "COMPLETED" and st.exit_code == 0
    assert "live-ok" in job.output(tail=50)["content"]
    if st.script_path:
        cluster.rm(st.script_path)


def test_durable_task_reuse_and_output_validation(cluster: Cluster, workdir: str | None) -> None:
    if workdir is None:
        pytest.skip("REMOTESLURM_LIVE_CWD is required for a durable live task")
    resources = {
        key: value
        for key, value in {
            "partition": PARTITION,
            "time": TIME,
            "nodes": 1,
            "ntasks": 1,
        }.items()
        if value
    }
    spec = TaskSpec(
        name=f"rs_live_ensure_{uuid.uuid4().hex[:12]}",
        script="#!/bin/bash\nprintf 'live-ensure-ok\\n' > result.txt\n",
        cwd=workdir,
        outputs=["result.txt"],
        validate=["bash", "-lc", "grep -qx live-ensure-ok result.txt"],
        resources=resources,
    )
    task_dir = None
    try:
        result = cluster.ensure(spec)
        task_dir = result.get("task_dir")
        deadline = time.monotonic() + 600
        while result["state"] in {"PENDING", "RUNNING"} and time.monotonic() < deadline:
            time.sleep(5)
            result = cluster.ensure(spec)

        assert result["state"] == "VERIFIED"
        assert result["receipt"]["outputs"][0]["sha256"]
        job_id = result["job_id"]

        reused = cluster.ensure(spec)
        assert reused["state"] == "VERIFIED"
        assert reused["job_id"] == job_id
        assert len(reused["attempts"]) == 1

        changed = cluster.run(["bash", "-c", "printf 'corrupt\\n' > result.txt"], cwd=workdir)
        assert changed["rc"] == 0
        invalid = cluster.ensure(spec)
        assert invalid["state"] == "INVALID"
        assert invalid["job_id"] == job_id
        assert len(invalid["attempts"]) == 1
    finally:
        if task_dir:
            cluster.rm(task_dir, recursive=True)


def test_durable_task_recovers_after_lost_scheduler_acceptance(
    cluster: Cluster, workdir: str | None
) -> None:
    if not FAULTS:
        pytest.skip("REMOTESLURM_LIVE_FAULTS is required for live acceptance fault injection")
    if workdir is None:
        pytest.skip("REMOTESLURM_LIVE_CWD is required for a durable live task")
    host = Config.load().host(HOST)
    fault_cluster = Cluster(
        host,
        _LostAcceptanceTransport(
            alias=host.ssh,
            mfa=host.mfa,
            python=host.python,
            install_dir=host.install_dir,
            control_path=host.control_path,
            control_persist=host.control_persist,
            extra_ssh_opts=list(host.ssh_opts),
        ),
    )
    resources = {
        key: value
        for key, value in {
            "partition": PARTITION,
            "time": TIME,
            "nodes": 1,
            "ntasks": 1,
        }.items()
        if value
    }
    spec = TaskSpec(
        name=f"rs_live_recover_{uuid.uuid4().hex[:12]}",
        script="#!/bin/bash\nprintf 'live-recovered-ok\\n' > recovered.txt\n",
        cwd=workdir,
        outputs=["recovered.txt"],
        validate=["bash", "-lc", "grep -qx live-recovered-ok recovered.txt"],
        resources=resources,
    )
    task_dir = None
    try:
        with pytest.raises(SessionDied):
            fault_cluster.ensure(spec)

        result = cluster.ensure(spec)
        task_dir = result.get("task_dir")
        deadline = time.monotonic() + 600
        while result["state"] in {"PENDING", "RUNNING"} and time.monotonic() < deadline:
            time.sleep(5)
            result = cluster.ensure(spec)

        assert result["state"] == "VERIFIED"
        assert result["submitted"] is False
        assert len(result["attempts"]) == 1
        assert result["attempts"][0]["recovered"] is True
        assert result["attempts"][0]["recovered_from"] in {"squeue", "sacct"}
    finally:
        fault_cluster.close()
        if task_dir:
            cluster.rm(task_dir, recursive=True)
