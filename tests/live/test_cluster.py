"""Live tests for any configured cluster with an established SSH session."""

from __future__ import annotations

import os

import pytest

from remoteslurm import Cluster

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


def test_basics(cluster: Cluster) -> None:
    assert cluster.ping()["protocol"] == 2
    info = cluster.info()
    assert info["slurm_tools"]["sbatch"]
    assert cluster.ls("~")["path"] == info["home"]


def test_job_lifecycle(cluster: Cluster) -> None:
    options = {
        key: value
        for key, value in {
            "partition": PARTITION,
            "time": TIME,
            "cwd": CWD,
            "template": TEMPLATE,
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
