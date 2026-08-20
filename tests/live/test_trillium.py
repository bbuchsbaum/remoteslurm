"""Live tests against a real cluster. Run with REMOTESLURM_LIVE=1 (and an established master)."""

from __future__ import annotations

import os

import pytest

from remoteslurm import Cluster

pytestmark = pytest.mark.skipif(
    not os.environ.get("REMOTESLURM_LIVE"), reason="REMOTESLURM_LIVE not set"
)
HOST = os.environ.get("REMOTESLURM_LIVE_HOST")  # None -> default host
PARTITION = os.environ.get("REMOTESLURM_LIVE_PARTITION", "debug")


@pytest.fixture(scope="module")
def cluster() -> Cluster:
    return Cluster.connect(HOST)


def test_basics(cluster: Cluster) -> None:
    assert cluster.ping()["protocol"] == 1
    info = cluster.info()
    assert info["slurm_tools"]["sbatch"]
    assert cluster.ls("~")["path"] == info["home"]


def test_job_lifecycle(cluster: Cluster) -> None:
    job = cluster.submit(
        "#!/bin/bash\necho live-ok\n",
        name="rs_live",
        partition=PARTITION,
        time="00:02:00",
        cwd="$SCRATCH",
    )
    st = job.wait(poll=5, timeout=600)
    assert st.state == "COMPLETED" and st.exit_code == 0
    assert "live-ok" in job.output(tail=50)["content"]
    cluster.rm(st.script_path)  # type: ignore[arg-type]
