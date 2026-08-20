"""WP-D3: `run --compute` via srun (FakeSlurm `srun` shim runs the command locally)."""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from remoteslurm.cluster import Cluster
from remoteslurm.config import HostConfig, Template
from remoteslurm.session import Session


# --------------------------------------------------------------------------- helpers
def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # pragma: no cover
        return True
    return True


def _wait_for_file(path: Path, timeout: float = 10.0) -> int:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            txt = path.read_text().strip()
            if txt:
                return int(txt)
        except (OSError, ValueError):
            pass
        time.sleep(0.02)
    raise AssertionError(f"{path} never got a pid")


def _wait_dead(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _alive(pid):
            return True
        time.sleep(0.02)
    return False


# --------------------------------------------------------------------------- started / node
def test_compute_shell_string_started_returns_node(cluster: Cluster) -> None:
    r = cluster.run("echo hi", compute=True, time="00:01:00", queue_timeout=5)
    assert r["started"] is True
    assert r["rc"] == 0
    assert r["node"] == "fakenode1"
    assert r["stdout"].strip() == "hi"
    assert "RS_NODE" not in r["stderr"]  # sentinel stripped out of the returned stderr
    assert r["elapsed"] is not None


def test_compute_argv_form_no_shell(cluster: Cluster) -> None:
    # argv form runs the vector with no shell parsing of the arguments.
    r = cluster.run(["echo", "hello world"], compute=True, time="00:01:00", queue_timeout=5)
    assert r["started"] is True
    assert r["stdout"].strip() == "hello world"
    assert r["node"] == "fakenode1"


def test_compute_node_from_custom_nodename(make_cluster: Callable[..., Cluster]) -> None:
    c = make_cluster({"FAKESLURM_SRUN_NODE": "gpu042"})
    r = c.run("true", compute=True, time="00:01:00", queue_timeout=5)
    assert r["started"] is True and r["node"] == "gpu042"


def test_compute_nonzero_rc_still_started(cluster: Cluster) -> None:
    r = cluster.run(
        "echo out; echo err >&2; exit 7", compute=True, time="00:01:00", queue_timeout=5
    )
    assert r["started"] is True
    assert r["rc"] == 7
    assert r["stdout"].strip() == "out"
    assert "err" in r["stderr"]


# --------------------------------------------------------------------------- never allocated
def test_compute_queue_never_reports_not_started(make_cluster: Callable[..., Cluster]) -> None:
    c = make_cluster({"FAKESLURM_SRUN_QUEUE": "never"})
    r = c.run("echo hi", compute=True, time="00:01:00", queue_timeout=3)
    assert r["started"] is False
    assert "still queued after 3s" in r["reason"]
    assert r.get("node") is None


# --------------------------------------------------------------------------- resource resolution
def test_resolve_compute_resources_template_then_kwargs() -> None:
    tmpl = Template(
        name="cpu",
        options={"partition": "compute", "time": "01:00:00", "cpus_per_task": 4, "mem": "16G"},
    )
    host = HostConfig(name="h", ssh="local", mfa=False, account="rrg-host", templates={"cpu": tmpl})
    c = Cluster.__new__(Cluster)  # no session needed for the pure resolver
    c.host = host
    res = c._resolve_compute_resources(
        template="cpu",
        partition=None,
        time="00:10:00",  # explicit kwarg overrides the template's time
        cpus=None,
        mem=None,
        gpus=2,
        account=None,
    )
    assert res["partition"] == "compute"
    assert res["time"] == "00:10:00"  # kwarg won
    assert res["cpus"] == 4  # from cpus_per_task
    assert res["mem"] == "16G"
    assert res["gpus"] == 2
    assert res["account"] == "rrg-host"  # host default fills in


def test_compute_respects_allow_run_false(cluster: Cluster) -> None:
    from remoteslurm.errors import PermissionDenied

    cluster.host.allow_run = False
    with pytest.raises(PermissionDenied):
        cluster.run("echo hi", compute=True, time="00:01:00")


# --------------------------------------------------------------------------- cancel a compute run
def test_compute_cancel_kills_process_group(cluster: Cluster) -> None:
    """Cancelling an in-flight srun kills the whole step tree (like the E1 `run` cancel)."""
    marker = Path(cluster.info()["home"]) / "rs_pid"
    if marker.exists():
        marker.unlink()
    rid = Session.new_request_id()
    box: dict[str, object] = {}

    def run_it() -> None:
        box["v"] = cluster.session.call(
            "srun",
            {
                "cmd": "sleep 30 & echo $! > $HOME/rs_pid; wait",
                "time": "00:05:00",
                "queue_timeout": 5,
                "timeout": 120,
            },
            timeout=120,
            request_id=rid,
        )

    t = threading.Thread(target=run_it, daemon=True)
    t.start()
    child = _wait_for_file(marker)  # the step has really started
    assert _alive(child)
    deadline = time.time() + 5
    while time.time() < deadline and not cluster.session.cancel(rid).get("cancelled"):
        time.sleep(0.05)
    t.join(timeout=10)
    res = box["v"]
    assert isinstance(res, dict)
    assert res.get("cancelled") is True
    assert res.get("started") is True  # the RS_NODE sentinel was seen before the kill
    assert _wait_dead(child), "cancel did not kill the srun step's process group"
