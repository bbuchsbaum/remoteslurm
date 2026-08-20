"""WP-F4: parsers must produce correct structured output across Slurm versions.

The same delimited/`-o` invocations are parsed for both the trillium fixtures (Slurm 25.11, in
``tests/fixtures/slurm/``) and hand-synthesized older-Slurm fixtures (~20.02, in
``tests/fixtures/slurm/slurm20/`` — see that dir's README; they are NOT from a real cluster). Each
parser is parameterized over both sets so a version-specific format difference (e.g. the old
``ReqMem`` ``4Gc`` suffix) is caught here rather than live.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from remoteslurm import slurm

FX = Path(__file__).parent / "fixtures" / "slurm"

# One entry per Slurm generation. ``dir`` is where that generation's fixtures live.
CASES = {
    "trillium-25.11": {
        "dir": FX,
        "squeue": {
            "job_id": "2166954",
            "state": "RUNNING",
            "partition": "debug",
            "nodelist": "tri0002",
            "workdir": "/scratch/brad",
            "user": "brad",
        },
        "sacct": {
            "state": "COMPLETED",
            "exit_code": "0:0",
            "elapsed": "00:00:31",
            "max_rss": 15848 * 1024,
            "req_mem": "767000M",
            "steps": {"batch", "extern"},
            "batch_exit_code": "0:0",
            "nodelist": "tri0002",
            "workdir": "/scratch/brad",
        },
        "scontrol": {"JobId": "2166954", "JobState": "RUNNING", "WorkDir": "/scratch/brad"},
        "sinfo": {"default": "compute", "present": {"debug", "compute"}, "debug_tl": "1:00:00"},
    },
    "slurm20": {
        "dir": FX / "slurm20",
        "squeue": {
            "job_id": "1850042",
            "state": "RUNNING",
            "partition": "debug",
            "nodelist": "cdr552",
            "workdir": "/home/brad/scratch",
            "user": "brad",
        },
        "sacct": {
            "state": "COMPLETED",
            "exit_code": "0:0",
            "elapsed": "00:00:44",
            "max_rss": 1048576 * 1024,
            "req_mem": "4Gc",  # old per-core suffix form
            "steps": {"batch", "extern"},
            "batch_exit_code": "0:0",
            "nodelist": "cdr552",
            "workdir": "/home/brad/scratch",
        },
        "scontrol": {"JobId": "1850042", "JobState": "RUNNING", "WorkDir": "/home/brad/scratch"},
        "sinfo": {
            "default": "cpubase",
            "present": {"debug", "cpubase", "gpubase"},
            "debug_tl": "1:00:00",
        },
    },
}

VERSIONS = list(CASES)


def _fx(case: dict, name: str) -> str:
    return (case["dir"] / f"{name}.txt").read_text()


@pytest.mark.parametrize("version", VERSIONS)
def test_parse_squeue_running(version: str) -> None:
    case = CASES[version]
    rows = slurm.parse_squeue(_fx(case, "squeue_running"))
    assert len(rows) == 1
    r = rows[0]
    for k, v in case["squeue"].items():
        assert r[k] == v, f"{version} squeue field {k}"


@pytest.mark.parametrize("version", VERSIONS)
def test_parse_sacct_folds_steps(version: str) -> None:
    case = CASES[version]
    exp = case["sacct"]
    jobs = slurm.parse_sacct(_fx(case, "sacct_completed"))
    assert len(jobs) == 1
    j = next(iter(jobs.values()))
    assert j["state"] == exp["state"]
    assert j["exit_code"] == exp["exit_code"]
    assert j["elapsed"] == exp["elapsed"]
    assert j["max_rss"] == exp["max_rss"]
    assert j["req_mem"] == exp["req_mem"]
    assert {s["step"] for s in j["steps"]} == exp["steps"]
    assert j["batch_exit_code"] == exp["batch_exit_code"]
    assert j["nodelist"] == exp["nodelist"]
    assert j["workdir"] == exp["workdir"]
    # The parsed ReqMem string must yield sane bytes regardless of an n/c suffix.
    assert slurm._parse_mem(j["req_mem"]) is not None


@pytest.mark.parametrize("version", VERSIONS)
def test_parse_scontrol_job(version: str) -> None:
    case = CASES[version]
    exp = case["scontrol"]
    sc = slurm.parse_scontrol_job(_fx(case, "scontrol_job"))
    assert sc["JobId"] == exp["JobId"]
    assert sc["JobState"] == exp["JobState"]
    assert sc["WorkDir"] == exp["WorkDir"]
    # Empty StdErr immediately followed by another key must parse to "" (not the next key).
    assert sc["StdErr"] == ""
    assert sc["StdOut"].endswith(f"slurm-{exp['JobId']}.out")


@pytest.mark.parametrize("version", VERSIONS)
def test_parse_sinfo(version: str) -> None:
    case = CASES[version]
    exp = case["sinfo"]
    parts = {p["partition"]: p for p in slurm.parse_sinfo(_fx(case, "sinfo"))}
    assert exp["present"] <= set(parts)
    assert parts[exp["default"]]["default"] is True
    assert parts["debug"]["nodes_total"] == sum(parts["debug"]["nodes"].values())
    assert parts["debug"]["time_limit"] == exp["debug_tl"]


# --------------------------------------------------------------------------- ReqMem old vs new
@pytest.mark.parametrize(
    "text,expected",
    [
        ("767000M", 767000 * 1024**2),  # 25.11 plain form
        ("4Gn", 4 * 1024**3),  # old per-node suffix
        ("4Gc", 4 * 1024**3),  # old per-core suffix
        ("1024Mn", 1024 * 1024**2),  # old per-node, MB
        ("15848K", 15848 * 1024),
        ("", None),
        ("garbage", None),
    ],
)
def test_parse_mem_old_and_new_forms(text: str, expected) -> None:
    assert slurm._parse_mem(text) == expected


def test_oom_diagnosis_with_old_reqmem_suffix() -> None:
    """The OOM rule fires when an old-form ReqMem (`1024Mn`) parses to bytes and MaxRSS ~= it."""
    req = slurm._parse_mem("1024Mn")
    assert req == 1024 * 1024**2
    ctx = slurm.DiagContext(state="FAILED", exit_code=1, max_rss=int(req * 0.99), req_mem=req)
    verdict, hints = slurm.diagnose_job(ctx)
    assert "memory" in verdict.lower()
    assert any("%" in h or "mem" in h.lower() for h in hints)
