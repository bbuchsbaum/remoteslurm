from __future__ import annotations

from pathlib import Path

import pytest

from remoteslurm import slurm
from remoteslurm.errors import SlurmError

FX = Path(__file__).parent / "fixtures" / "slurm"


def fx(name: str) -> str:
    return (FX / name).read_text()


def test_parse_sbatch() -> None:
    assert slurm.parse_sbatch_output(fx("sbatch_parsable.txt"), "", 0) == "2166954"
    assert slurm.parse_sbatch_output("123;cluster\n", "", 0) == "123"
    with pytest.raises(SlurmError) as ei:
        slurm.parse_sbatch_output("", fx("sbatch_error.txt"), 64)
    assert "Walltime" in ei.value.message
    with pytest.raises(SlurmError):
        slurm.parse_sbatch_output("garbage", "", 0)


def test_parse_job_id() -> None:
    assert slurm.parse_job_id("123") == ("123", None)
    assert slurm.parse_job_id("123_4") == ("123", "4")
    assert slurm.parse_job_id("123_[1-5]") == ("123", "[1-5]")
    with pytest.raises(SlurmError):
        slurm.parse_job_id("123+1")
    with pytest.raises(SlurmError):
        slurm.parse_job_id("abc")


def test_parse_squeue_running() -> None:
    rows = slurm.parse_squeue(fx("squeue_running.txt"))
    assert len(rows) == 1
    r = rows[0]
    assert r["job_id"] == "2166954" and r["state"] == "RUNNING" and r["partition"] == "debug"
    assert r["nodelist"] == "tri0002" and r["workdir"] == "/scratch/brad" and r["user"] == "brad"
    assert slurm.parse_squeue("") == []
    assert slurm.parse_squeue("\n  \n") == []


def test_parse_sacct_folds_steps() -> None:
    jobs = slurm.parse_sacct(fx("sacct_completed.txt"))
    assert list(jobs) == ["2166954"]
    j = jobs["2166954"]
    assert j["state"] == "COMPLETED" and j["exit_code"] == "0:0" and j["elapsed"] == "00:00:31"
    assert j["max_rss"] == 15848 * 1024  # from .batch step
    assert {s["step"] for s in j["steps"]} == {"batch", "extern"}
    assert j["batch_exit_code"] == "0:0"
    assert j["nodelist"] == "tri0002" and j["workdir"] == "/scratch/brad"


def test_parse_sacct_cancelled_by() -> None:
    txt = "42|x|CANCELLED by 1000|0:0|00:01:00||n1|a|b|c|p|acc|1|1G|/w|1:00|me\n"
    j = slurm.parse_sacct(txt)["42"]
    assert j["state"] == "CANCELLED" and j["state_raw"] == "CANCELLED by 1000"


def test_parse_scontrol_job() -> None:
    sc = slurm.parse_scontrol_job(fx("scontrol_job.txt"))
    assert sc["JobId"] == "2166954" and sc["JobState"] == "RUNNING"
    assert sc["StdOut"] == "/scratch/brad/slurm-2166954.out" and sc["WorkDir"] == "/scratch/brad"
    assert sc["StdErr"] == ""  # empty value followed by another key
    sc2 = slurm.parse_scontrol_job(fx("scontrol_job_finished.txt"))
    assert sc2["JobState"] == "COMPLETED"
    with pytest.raises(SlurmError):
        slurm.parse_scontrol_job("slurm_load_jobs error: Invalid job id specified")


def test_parse_sinfo() -> None:
    parts = {p["partition"]: p for p in slurm.parse_sinfo(fx("sinfo.txt"))}
    assert "debug" in parts and "compute" in parts
    assert parts["compute"]["default"] is True
    assert parts["debug"]["nodes_total"] == sum(parts["debug"]["nodes"].values())
    assert parts["debug"]["time_limit"] == "1:00:00"


def test_helpers() -> None:
    assert slurm.exit_code_int("0:0") == 0
    assert slurm.exit_code_int("1:0") == 1
    assert slurm.exit_code_int("0:9") == 137
    assert slurm.exit_code_int(None) is None
    assert slurm.normalize_state("CANCELLED by 1") == "CANCELLED"
    assert slurm.normalize_state("COMPLETING+") == "COMPLETING"
    assert slurm.is_terminal("FAILED") and not slurm.is_terminal("RUNNING")
    assert slurm.sbatch_args_from_options(
        {"time": "1:00", "gpus_per_node": 1, "exclusive": True, "x": None}
    ) == [
        "--time=1:00",
        "--gpus-per-node=1",
        "--exclusive",
    ]


def test_job_status_to_dict_drops_none() -> None:
    st = slurm.JobStatus(job_id="1", state="RUNNING", source="squeue")
    d = st.to_dict()
    assert d["job_id"] == "1" and "exit_code" not in d and "extra" not in d


def test_parse_live_sstat_usage() -> None:
    text = "2308782.batch|cpu=192,mem=767000M,node=1|1|1-03:33:11|66678456K|67906044K|101,102,103\n"
    usage = slurm.parse_sstat_usage(text, elapsed="34:30")

    assert usage is not None
    assert usage["source"] == "sstat"
    assert usage["allocated_cpus"] == 192
    assert usage["live_pids"] == 3
    assert usage["cpu_time_seconds"] == 99_191
    assert usage["effective_cpus"] == 47.92
    assert usage["cpu_utilization_percent"] == 25.0
    assert usage["estimated_total_rss_bytes"] == 66_678_456 * 1024
    assert usage["max_rss_bytes"] == 67_906_044 * 1024


def test_sstat_usage_accounts_for_multiple_slurm_tasks() -> None:
    text = "42.0|cpu=8,mem=32G|4|10:00.000|1G|2G|10,11\n"
    usage = slurm.parse_sstat_usage(text, elapsed="20:00")

    assert usage is not None
    assert usage["cpu_time_seconds"] == 2400
    assert usage["effective_cpus"] == 2.0
    assert usage["cpu_utilization_percent"] == 25.0
    assert usage["estimated_total_rss_bytes"] == 4 * 1024**3


def test_sstat_usage_accepts_scontrol_cpu_fallback() -> None:
    usage = slurm.parse_sstat_usage(
        "42.batch||1|10:00|1G|2G|10,11\n", elapsed="20:00", allocated_cpus=8
    )

    assert usage is not None
    assert usage["allocated_cpus"] == 8
    assert usage["cpu_utilization_percent"] == 6.2


def test_parse_terminal_sacct_usage() -> None:
    text = (
        "2308782|FAILED|192||00:40:29|1-10:38:11|||\n"
        "2308782.batch|FAILED|192|1|00:40:29|1-10:38:11|67906044K|67906044K\n"
        "2308782.extern|COMPLETED|192|1|00:40:29|00:00:00|||\n"
    )
    usage = slurm.parse_sacct_usage(text, "2308782")

    assert usage is not None
    assert usage["source"] == "sacct"
    assert usage["allocated_cpus"] == 192
    assert usage["cpu_time_seconds"] == 124_691
    assert usage["effective_cpus"] == 51.33
    assert usage["cpu_utilization_percent"] == 26.7
    assert "live_pids" not in usage
