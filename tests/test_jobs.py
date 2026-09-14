"""Job lifecycle tests against the FakeSlurm shims in tests/fakeslurm."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import remoteslurm.jobs as jobs_mod
from remoteslurm.errors import InvalidArgument, RegistryUnavailable, SlurmError

SCRIPT = "#!/bin/bash\n#SBATCH -J rs_test\necho hello\n"


@pytest.fixture(autouse=True)
def _isolated_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Never touch the user's real ~/.local/state/remoteslurm."""
    d = tmp_path / "rs-state"
    monkeypatch.setenv("REMOTESLURM_STATE_DIR", str(d))
    return d


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jobs_mod.time, "sleep", lambda *_: None)


def fake_state(sandbox: Path) -> dict:
    return json.loads((sandbox / ".fakeslurm.json").read_text())


def tick(sandbox: Path) -> int:
    return fake_state(sandbox)["tick"]


def burn_ticks(cluster, n: int) -> None:
    for _ in range(n):
        cluster.squeue(refresh=True)


def registry_jobs(state_dir: Path) -> dict[str, dict]:
    data = json.loads((state_dir / "local" / "jobs.json").read_text())
    return {j["job_id"]: j for j in data["jobs"]}


# -- submit --------------------------------------------------------------------------------------


def test_submit_returns_job_and_records_registry(cluster, sandbox, _isolated_state):
    job = cluster.submit(SCRIPT, name="rs_test")
    assert job.job_id.isdigit()
    assert int(job.job_id) >= 1000
    recs = registry_jobs(_isolated_state)
    rec = recs[job.job_id]
    assert rec["script_path"] and Path(rec["script_path"]).read_text() == SCRIPT
    assert rec["stdout_path"] and rec["stdout_path"].endswith(f"slurm-{job.job_id}.out")
    assert rec["workdir"]
    assert rec["last_state"] == "PENDING"
    # the stdout path lives in the sandbox, next to the generated script
    # jobs run from $HOME by default (not from the generated-script directory)
    assert Path(rec["stdout_path"]).parent == Path(cluster.home)


def test_submit_argument_validation(cluster):
    with pytest.raises(InvalidArgument):
        cluster.submit()
    with pytest.raises(InvalidArgument):
        cluster.submit(SCRIPT, path="/some/where.sh")


def test_submit_preflight_failure_creates_no_scheduler_job(cluster, sandbox, _isolated_state):
    _isolated_state.write_text("state path is a file\n")

    with pytest.raises(RegistryUnavailable) as exc:
        cluster.submit(SCRIPT)

    assert exc.value.code == "registry_unavailable"
    assert "REMOTESLURM_STATE_DIR" in str(exc.value.action)
    assert not (sandbox / ".fakeslurm.json").exists()


def test_post_submit_registry_failure_returns_recoverable_handle(
    cluster, sandbox, monkeypatch: pytest.MonkeyPatch
):
    def fail_put(_rec):
        raise PermissionError("registry became read-only")

    monkeypatch.setattr(cluster.registry, "put", fail_put)
    job = cluster.submit(SCRIPT)

    assert job.recorded is False
    assert job.submission() == {
        "submitted": True,
        "recorded": False,
        "job_id": job.job_id,
        "registry_error": "PermissionError: registry became read-only",
        "recovery": f"rslurm adopt {job.job_id}",
    }
    assert set(fake_state(sandbox)["jobs"]) == {job.job_id}


def test_adopt_reconstructs_a_missing_registry_record(cluster):
    job = cluster.submit(SCRIPT, name="recover-me")
    assert cluster.registry.forget(job.job_id)

    adopted = cluster.adopt(job.job_id)

    assert adopted.job_id == job.job_id and adopted.recorded is True
    rec = cluster.registry.get(job.job_id)
    assert rec is not None
    assert rec.name == "recover-me"
    assert rec.meta["adopted"] is True
    assert rec.stdout_path and rec.script_path


def test_status_and_jobs_degrade_when_registry_is_unreadable(
    cluster, monkeypatch: pytest.MonkeyPatch
):
    job = cluster.submit(SCRIPT)

    def unavailable(*_args, **_kwargs):
        raise PermissionError("registry cannot be read")

    for method in ("get", "all", "update", "prune"):
        monkeypatch.setattr(cluster.registry, method, unavailable)

    status = cluster.job_status(job.job_id, refresh=True)
    assert status.job_id == job.job_id
    assert status.source in {"squeue", "scontrol", "sacct"}
    assert status.registry_available is False
    assert "registry cannot be read" in str(status.registry_error)

    listed = cluster.jobs(refresh=True)
    assert [row.job_id for row in listed] == [job.job_id]
    assert listed[0].registry_available is False
    assert "registry cannot be read" in str(cluster.registry_error)


def test_pack_and_sweep_preflight_before_remote_support_files(
    cluster, sandbox, monkeypatch: pytest.MonkeyPatch
):
    error = RegistryUnavailable("registry unavailable")

    def fail_preflight() -> None:
        raise error

    monkeypatch.setattr(cluster.registry, "preflight", fail_preflight)

    with pytest.raises(RegistryUnavailable):
        cluster.pack(["echo one"])
    with pytest.raises(RegistryUnavailable):
        cluster.sweep({"x": [1]}, script=SCRIPT)

    assert not (sandbox / ".remoteslurm").exists()
    assert not (sandbox / ".fakeslurm.json").exists()


def test_sbatch_rejection_raises(cluster):
    with pytest.raises(SlurmError) as ei:
        cluster.submit("#!/bin/bash\n# FAKESLURM_REJECT\necho no\n")
    assert "Walltime" in str(ei.value)


def test_host_defaults_become_sbatch_flags(make_cluster, _isolated_state):
    c = make_cluster(account="acc", defaults={"time": "0:10:00"})
    j1 = c.submit(SCRIPT)
    j2 = c.submit(SCRIPT, time="1:00:00")
    recs = registry_jobs(_isolated_state)
    a1 = recs[j1.job_id]["sbatch_args"]
    assert "--account=acc" in a1
    assert "--time=0:10:00" in a1
    a2 = recs[j2.job_id]["sbatch_args"]
    assert "--time=1:00:00" in a2
    assert "--time=0:10:00" not in a2
    assert "--account=acc" in a2
    # the fake sbatch honoured the account flag
    assert c.job_status(j1.job_id, refresh=True).account == "acc"


# -- status ------------------------------------------------------------------------------------


def test_status_progression(cluster):
    job = cluster.submit(SCRIPT)
    seen = [cluster.job_status(job.job_id, refresh=True).state for _ in range(8)]
    assert seen[0] == "PENDING"
    assert "RUNNING" in seen
    assert seen[-1] == "COMPLETED"
    # monotone: P* R* C*
    order = {"PENDING": 0, "RUNNING": 1, "COMPLETED": 2}
    ranks = [order[s] for s in seen]
    assert ranks == sorted(ranks)


def test_squeue_cache_and_refresh(cluster, sandbox):
    cluster.submit(SCRIPT)  # invalidates the cache
    cluster.squeue()
    t1 = tick(sandbox)
    cluster.squeue()
    assert tick(sandbox) == t1  # served from cache, shim not invoked
    cluster.squeue(refresh=True)
    assert tick(sandbox) == t1 + 1


def test_terminal_status_from_sacct(cluster):
    job = cluster.submit(SCRIPT)
    st = job.wait(poll=5)
    assert st.terminal is True
    assert st.state == "COMPLETED"
    assert st.source == "sacct"
    assert st.exit_code == 0
    assert st.exit_code_raw == "0:0"
    assert st.max_rss == 15848 * 1024
    assert st.script_path and st.stdout_path
    steps = {s["step"] for s in st.extra["steps"]}
    assert steps == {"batch", "extern"}


def test_failed_script(cluster):
    job = cluster.submit("#!/bin/bash\n# FAKESLURM_FAIL\nexit 1\n")
    st = job.wait(poll=5)
    assert st.state == "FAILED"
    assert st.exit_code == 1
    assert st.terminal is True


def test_wait_returns_terminal(cluster):
    job = cluster.submit(SCRIPT)
    st = cluster.wait(job.job_id, poll=5)
    assert st.terminal
    assert st.state == "COMPLETED"


def test_accounting_lag_falls_back_to_scontrol_then_registry(make_cluster, sandbox):
    c = make_cluster({"FAKESLURM_ACCOUNTING_LAG": "1000"})
    job = c.submit(SCRIPT)
    # drive the fake clock until the job leaves squeue
    for _ in range(10):
        if not any(r["job_id"] == job.job_id for r in c.squeue(refresh=True)):
            break
    st = c.job_status(job.job_id, refresh=True)
    assert st.source == "scontrol"
    assert st.state == "COMPLETED"
    assert st.terminal is True
    # once scontrol forgets the job too, the registry's last state is reported as pending
    burn_ticks(c, 5)
    st = c.job_status(job.job_id, refresh=True)
    assert st.source == "registry"
    assert st.accounting_pending is True
    assert st.state == "COMPLETED"
    assert st.terminal is False


# -- cancel ----------------------------------------------------------------------------------


def test_cancel(cluster):
    job = cluster.submit(SCRIPT)
    res = cluster.cancel(job.job_id)
    assert job.job_id in res["cancelled"]
    assert res["skipped"] == []
    st = cluster.job_status(job.job_id, refresh=True)
    assert st.state == "CANCELLED"
    assert st.terminal is True


def test_cancel_unknown_job_is_skipped(cluster):
    res = cluster.cancel("424242")
    assert res["cancelled"] == []
    assert res["skipped"] == ["424242"]


# -- listing / output ------------------------------------------------------------------------


def test_jobs_lists_live_and_finished(cluster):
    done = cluster.submit(SCRIPT, name="done")
    done.wait(poll=5)
    live = cluster.submit(SCRIPT, name="live")
    listing = {s.job_id: s for s in cluster.jobs(refresh=True)}
    assert set(listing) == {done.job_id, live.job_id}
    assert listing[done.job_id].terminal
    assert listing[done.job_id].source == "sacct"
    assert listing[live.job_id].state in ("PENDING", "RUNNING")
    assert listing[live.job_id].source == "squeue"
    only_live = {s.job_id for s in cluster.jobs(include_finished=False)}
    assert only_live == {live.job_id}


def test_job_output_after_completion(cluster):
    job = cluster.submit(SCRIPT)
    job.wait(poll=5)
    out = job.output(tail=1)
    assert out["content"].strip() == "done"
    assert out["job_id"] == job.job_id
    assert out["state"] == "COMPLETED"
    full = cluster.job_output(job.job_id, tail=None)
    assert full["content"] == "hello\ndone\n"


def test_sinfo(cluster):
    parts = {p["partition"]: p for p in cluster.sinfo()}
    assert parts["compute"]["default"] is True
    assert parts["debug"]["nodes_idle"] == 3
