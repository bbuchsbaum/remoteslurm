"""Live tests for any configured cluster with an established SSH session."""

from __future__ import annotations

import os
import time
import uuid

import pytest

from remoteslurm import Cluster, TaskSpec

pytestmark = pytest.mark.skipif(
    not os.environ.get("REMOTESLURM_LIVE"), reason="REMOTESLURM_LIVE not set"
)
HOST = os.environ.get("REMOTESLURM_LIVE_HOST")  # None -> default host
PARTITION = os.environ.get("REMOTESLURM_LIVE_PARTITION")
TIME = os.environ.get("REMOTESLURM_LIVE_TIME")
CWD = os.environ.get("REMOTESLURM_LIVE_CWD")
TEMPLATE = os.environ.get("REMOTESLURM_LIVE_TEMPLATE")


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
