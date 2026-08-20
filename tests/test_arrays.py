"""Job arrays (WP-D1) and parameter sweeps (WP-D2).

The first test is the riskiest one from the plan: an array parent's ``job_status`` roll-up when
squeue shows some tasks pending/running and sacct holds the finished ones. It runs the real
:meth:`Cluster.job_status` path over the recorded ``array_*`` fixtures (via monkeypatched
squeue/sacct), then the rest cover the parser helpers, the FakeSlurm array lifecycle, output-path
expansion, cancel, the ``jobs`` TASKS column, and sweeps end to end.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

import remoteslurm.jobs as jobs_mod
from remoteslurm import cli, server, slurm
from remoteslurm.errors import InvalidArgument

FX = Path(__file__).parent / "fixtures" / "slurm"

ARRAY_SCRIPT = "#!/bin/bash\necho task $SLURM_ARRAY_TASK_ID\n"
SWEEP_BODY = "#!/bin/bash\necho hi\n"


def fx(name: str) -> str:
    return (FX / name).read_text()


@pytest.fixture(autouse=True)
def _isolated_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    d = tmp_path / "rs-state"
    monkeypatch.setenv("REMOTESLURM_STATE_DIR", str(d))
    return d


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jobs_mod.time, "sleep", lambda *_: None)


def drive_to_terminal(cluster, job_id: str, limit: int = 15) -> slurm.JobStatus:
    st = cluster.job_status(job_id, refresh=True)
    for _ in range(limit):
        if st.terminal:
            break
        st = cluster.job_status(job_id, refresh=True)
    return st


# --------------------------------------------------------------------------- the riskiest one
def test_array_job_status_aggregate_is_running(cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    """squeue: 4 RUNNING + 5-9 collapsed PENDING; sacct: 0-3 finished (1 FAILED) -> RUNNING."""
    sq = slurm.parse_squeue(fx("array_squeue.txt"))
    sa = slurm.parse_sacct(fx("array_sacct.txt"))
    monkeypatch.setattr(cluster, "squeue", lambda **kw: sq)
    monkeypatch.setattr(cluster, "sacct", lambda ids=None, **kw: sa)

    st = cluster.job_status("2200000")
    assert st.source == "array"
    assert st.state == "RUNNING"
    assert st.terminal is False
    assert st.extra["n_tasks"] == 10
    assert st.extra["tasks"] == {"COMPLETED": 3, "FAILED": 1, "RUNNING": 1, "PENDING": 5}
    assert st.extra["failed_tasks"] == [1]
    assert st.extra["task_states"][1] == "FAILED"
    assert st.extra["task_states"][4] == "RUNNING"
    assert st.extra["task_states"][7] == "PENDING"


# --------------------------------------------------------------------------- parser helpers
def test_expand_array_tasks() -> None:
    assert slurm.expand_array_tasks("[5-9%4]") == [5, 6, 7, 8, 9]
    assert slurm.expand_array_tasks("5-9%4") == [5, 6, 7, 8, 9]
    assert slurm.expand_array_tasks("5,7,9") == [5, 7, 9]
    assert slurm.expand_array_tasks("0-6:2") == [0, 2, 4, 6]
    assert slurm.expand_array_tasks("[3]") == [3]
    assert slurm.expand_array_tasks("1-3,7") == [1, 2, 3, 7]


def test_parse_squeue_expands_collapsed_array() -> None:
    rows = slurm.parse_squeue(fx("array_squeue.txt"))
    ids = [r["job_id"] for r in rows]
    assert "2200000_4" in ids
    pending_ids = [r["job_id"] for r in rows if r["state"] == "PENDING"]
    assert pending_ids == [f"2200000_{t}" for t in (5, 6, 7, 8, 9)]
    run_row = next(r for r in rows if r["job_id"] == "2200000_4")
    assert run_row["array_base"] == "2200000"
    assert run_row["array_task"] == "4"
    assert run_row["state"] == "RUNNING"
    pend = next(r for r in rows if r["job_id"] == "2200000_7")
    assert pend["array_base"] == "2200000" and pend["array_task"] == "7"


def test_parse_squeue_non_array_row_has_empty_array_fields() -> None:
    rows = slurm.parse_squeue(fx("squeue_running.txt"))
    assert rows[0]["array_base"] == "" and rows[0]["array_task"] == ""


def test_parse_sacct_array_per_task_records() -> None:
    jobs = slurm.parse_sacct(fx("array_sacct.txt"))
    assert set(jobs) == {f"2200000_{t}" for t in range(4)}
    assert jobs["2200000_1"]["state"] == "FAILED"
    assert jobs["2200000_1"]["exit_code"] == "1:0"
    assert {s["step"] for s in jobs["2200000_0"]["steps"]} == {"batch", "extern"}
    assert jobs["2200000_0"]["max_rss"] == 1024856 * 1024


def test_aggregate_array_from_fixtures() -> None:
    sq = slurm.parse_squeue(fx("array_squeue.txt"))
    sa = slurm.parse_sacct(fx("array_sacct.txt"))
    agg = slurm.aggregate_array("2200000", sq, sa)
    assert agg["state"] == "RUNNING"
    assert agg["terminal"] is False
    assert agg["failed_tasks"] == [1]
    assert agg["tasks"] == {"COMPLETED": 3, "FAILED": 1, "RUNNING": 1, "PENDING": 5}
    assert agg["task_states"][4] == "RUNNING"


def test_aggregate_array_state_rules() -> None:
    def sa(states: dict[int, str]) -> dict[str, dict]:
        return {f"9_{t}": {"state": s} for t, s in states.items()}

    assert slurm.aggregate_array("9", [], sa({0: "FAILED", 1: "PENDING"}))["state"] == "FAILED"
    assert slurm.aggregate_array("9", [], sa({0: "FAILED", 1: "RUNNING"}))["state"] == "RUNNING"
    done = slurm.aggregate_array("9", [], sa({0: "COMPLETED", 1: "COMPLETED"}))
    assert done["state"] == "COMPLETED" and done["terminal"] is True
    assert (
        slurm.aggregate_array("9", [], sa({0: "CANCELLED", 1: "CANCELLED"}))["state"] == "CANCELLED"
    )
    mixed = slurm.aggregate_array("9", [], sa({0: "COMPLETED", 1: "CANCELLED"}))
    assert mixed["state"] == "COMPLETED"
    assert slurm.aggregate_array("9", [], {})["state"] == "UNKNOWN"


# --------------------------------------------------------------------------- FakeSlurm lifecycle
def test_submit_array_records_flag_and_meta(make_cluster, _isolated_state) -> None:
    c = make_cluster()
    job = c.submit(ARRAY_SCRIPT, name="arr", array="0-9%4", dependency="afterok:1000")
    rec = c.registry.get(job.job_id)
    assert rec.meta["array"] == "0-9%4"
    assert rec.meta["dependency"] == "afterok:1000"
    assert "--array=0-9%4" in rec.sbatch_args
    assert "--dependency=afterok:1000" in rec.sbatch_args


def test_array_lifecycle_aggregates(make_cluster) -> None:
    c = make_cluster({"FAKESLURM_ARRAY_FAIL": "1,3"})
    job = c.submit(ARRAY_SCRIPT, name="arr", array="0-4")
    st = drive_to_terminal(c, job.job_id)
    assert st.terminal is True
    assert st.state == "FAILED"
    assert st.extra["failed_tasks"] == [1, 3]
    assert st.extra["tasks"]["COMPLETED"] == 3
    assert st.extra["tasks"]["FAILED"] == 2
    assert st.extra["n_tasks"] == 5
    # per-task status still works
    assert c.job_status(f"{job.job_id}_1", refresh=True).state == "FAILED"
    assert c.job_status(f"{job.job_id}_0", refresh=True).state == "COMPLETED"


def test_array_output_path_expansion(make_cluster) -> None:
    c = make_cluster()
    job = c.submit(ARRAY_SCRIPT, name="arr", array="0-2")
    drive_to_terminal(c, job.job_id)
    out = c.job_output(f"{job.job_id}_2", tail=None)
    assert out["content"] == "hello\ndone\n"
    assert out["job_id"] == f"{job.job_id}_2"
    # a bare array parent reads a representative task rather than erroring
    parent_out = c.job_output(job.job_id, tail=None)
    assert parent_out["content"] == "hello\ndone\n"


def test_expand_output_path_unit(cluster) -> None:
    st = slurm.JobStatus(job_id="1000_4", state="COMPLETED", source="sacct", name="grid")
    assert (
        cluster._expand_output_path("logs/slurm-%A_%a.out", "1000", "4", st)
        == "logs/slurm-1000_4.out"
    )
    assert cluster._expand_output_path("out-%x.log", "1000", "4", st) == "out-grid.log"


def test_cancel_array_whole_and_single_task(make_cluster) -> None:
    c = make_cluster({"FAKESLURM_FREEZE": "1"})  # keep tasks PENDING (cancellable)
    job = c.submit(ARRAY_SCRIPT, name="arr", array="0-3")
    r = c.cancel(f"{job.job_id}_2")
    assert r["cancelled"] == [f"{job.job_id}_2"]
    assert c.job_status(f"{job.job_id}_2", refresh=True).state == "CANCELLED"
    assert c.job_status(f"{job.job_id}_1", refresh=True).state == "PENDING"
    r2 = c.cancel(job.job_id)  # whole array via the base id
    assert job.job_id in r2["cancelled"]
    agg = c.job_status(job.job_id, refresh=True)
    assert agg.state == "CANCELLED" and agg.terminal is True


def test_jobs_lists_array_as_one_row(make_cluster) -> None:
    c = make_cluster({"FAKESLURM_FREEZE": "1"})
    job = c.submit(ARRAY_SCRIPT, name="arr", array="0-4")
    listing = {s.job_id: s for s in c.jobs(refresh=True)}
    assert job.job_id in listing
    st = listing[job.job_id]
    assert st.extra.get("array") is True
    assert st.extra["n_tasks"] == 5
    row = st.to_dict()
    assert cli.array_tasks_cell(row) == "5◦"  # five pending


def test_array_tasks_cell_glyphs() -> None:
    row = {"extra": {"array": True, "tasks": {"COMPLETED": 87, "RUNNING": 10, "FAILED": 3}}}
    assert cli.array_tasks_cell(row) == "87✓ 10▶ 3✗"
    assert cli.array_tasks_cell({"state": "RUNNING"}) == ""


def test_collapsed_pending_display(make_cluster) -> None:
    """With FAKESLURM_ARRAY_COLLAPSE the pending tasks come back as one bracketed row."""
    c = make_cluster({"FAKESLURM_FREEZE": "1", "FAKESLURM_ARRAY_COLLAPSE": "1"})
    job = c.submit(ARRAY_SCRIPT, name="arr", array="5-9%4")
    raw = c.call("squeue", format=slurm.SQUEUE_FORMAT, jobs=[job.job_id])
    assert f"{job.job_id}_[5-9%4]" in raw["stdout"]
    rows = c.squeue(refresh=True)
    task_ids = sorted(r["job_id"] for r in rows if r["array_base"] == job.job_id)
    assert task_ids == [f"{job.job_id}_{t}" for t in (5, 6, 7, 8, 9)]


# --------------------------------------------------------------------------- sweeps (pure)
def test_sweep_rows_cartesian() -> None:
    names, rows = jobs_mod.sweep_rows({"lr": ["0.1", "0.01"], "seed": [1, 2, 3]})
    assert names == ["lr", "seed"]
    assert len(rows) == 6
    assert rows[0] == {"lr": "0.1", "seed": 1}
    assert rows[-1] == {"lr": "0.01", "seed": 3}


def test_sweep_rows_list_of_dicts() -> None:
    names, rows = jobs_mod.sweep_rows([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    assert names == ["a", "b"] and len(rows) == 2


def test_sweep_rows_validation() -> None:
    with pytest.raises(InvalidArgument):
        jobs_mod.sweep_rows({"bad-name": [1]})  # not a shell identifier
    with pytest.raises(InvalidArgument):
        jobs_mod.sweep_rows([{"a": 1}, {"b": 2}])  # mismatched keys
    with pytest.raises(InvalidArgument):
        jobs_mod.sweep_rows({"a": ["x\ty"]})  # tab would corrupt the TSV


def test_params_tsv_and_wrapper_env(tmp_path: Path) -> None:
    names, rows = jobs_mod.sweep_rows({"lr": ["0.1", "0.01"], "seed": [1, 2, 3]})
    tsv = jobs_mod.params_tsv(names, rows)
    assert tsv.splitlines()[0] == "lr\tseed"
    assert tsv.splitlines()[1] == "0.1\t1"
    p = tmp_path / "params.tsv"
    p.write_text(tsv)
    body = 'echo "LR=$RS_PARAM_lr SEED=$RS_PARAM_seed"; echo "JSON=$RS_PARAMS_JSON"'
    wrapper = jobs_mod.sweep_wrapper(str(p), "demo", body=body)
    w = tmp_path / "w.sh"
    w.write_text(wrapper)
    r = subprocess.run(
        ["bash", str(w)],
        env={"SLURM_ARRAY_TASK_ID": "4", "PATH": "/usr/bin:/bin:/usr/local/bin"},
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stderr
    assert "LR=0.01 SEED=2" in r.stdout  # row 4 of the 2x3 product
    assert json.loads(r.stdout.split("JSON=", 1)[1].strip()) == {"lr": "0.01", "seed": "2"}


def test_sweep_wrapper_remote_path() -> None:
    w = jobs_mod.sweep_wrapper("/x/params.tsv", "s", remote_path="/remote/run.sh")
    assert "exec bash /remote/run.sh" in w
    with pytest.raises(InvalidArgument):
        jobs_mod.sweep_wrapper("/x/params.tsv", "s")  # neither body nor path


# --------------------------------------------------------------------------- sweeps (FakeSlurm)
def test_sweep_submits_array_and_records_meta(make_cluster) -> None:
    c = make_cluster()
    job = c.sweep(
        {"lr": ["0.1", "0.01"], "seed": [1, 2, 3]},
        script=SWEEP_BODY,
        name="grid",
        max_concurrent=2,
    )
    rec = c.registry.get(job.job_id)
    assert rec.meta["array"] == "0-5%2"
    assert rec.meta["sweep"]["n"] == 6
    assert rec.meta["sweep"]["names"] == ["lr", "seed"]
    tsv = c.read_text(rec.meta["sweep"]["params_path"])
    assert tsv.splitlines()[0] == "lr\tseed"
    assert len(tsv.splitlines()) == 7  # header + 6 rows


def test_sweep_failed_task_params_surface(make_cluster) -> None:
    c = make_cluster({"FAKESLURM_ARRAY_FAIL": "0"})  # task 0 fails
    job = c.sweep({"lr": ["0.1", "0.01"], "seed": [1, 2, 3]}, script=SWEEP_BODY, name="grid")
    st = drive_to_terminal(c, job.job_id)
    assert 0 in st.extra["failed_tasks"]
    fparams = st.extra["failed_task_params"]
    assert (fparams.get(0) or fparams.get("0")) == {"lr": "0.1", "seed": "1"}
    # a single failed task surfaces its own params
    st0 = c.job_status(f"{job.job_id}_0", refresh=True)
    assert st0.extra["params"] == {"lr": "0.1", "seed": "1"}
    # ... and diagnose carries them through
    d = c.diagnose(job.job_id)
    assert d["status"]["extra"]["failed_task_params"]


# --------------------------------------------------------------------------- MCP surface
def test_sweep_tool_registered_in_all_not_core() -> None:
    assert "sweep" in server.ALL_TOOLS
    assert "sweep" not in server.CORE_TOOLS
