"""diagnose: the pure DIAGNOSTICS rules, the size cap, and end-to-end via FakeSlurm (WP-C C2)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

import remoteslurm.jobs as jobs_mod
from remoteslurm import slurm


@pytest.fixture(autouse=True)
def _isolated_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    d = tmp_path / "rs-state"
    monkeypatch.setenv("REMOTESLURM_STATE_DIR", str(d))
    return d


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jobs_mod.time, "sleep", lambda *_: None)


def dctx(**kw) -> slurm.DiagContext:
    return slurm.DiagContext(**kw)


# -- pure rules -----------------------------------------------------------------------------
def test_rule_oom_by_state() -> None:
    v, h = slurm.diagnose_job(dctx(state="OUT_OF_MEMORY"))
    assert "memory" in v.lower()
    assert any("mem" in x.lower() for x in h)


def test_rule_oom_by_maxrss_over_reqmem() -> None:
    v, h = slurm.diagnose_job(dctx(state="FAILED", exit_code=1, max_rss=1000, req_mem=1000))
    assert "memory" in v.lower()


def test_rule_oom_not_triggered_for_clean_completion() -> None:
    # a job that completed while using ~all its memory is NOT flagged OOM
    v, _ = slurm.diagnose_job(dctx(state="COMPLETED", exit_code=0, max_rss=1000, req_mem=1000))
    assert "success" in v.lower()


def test_rule_timeout_includes_last_stdout_line() -> None:
    v, h = slurm.diagnose_job(
        dctx(
            state="TIMEOUT",
            elapsed="01:00:00",
            time_limit="01:00:00",
            stdout_tail="warming up\nlast line before wall clock\n",
        )
    )
    assert "time" in v.lower()
    assert any("last line before wall clock" in x for x in h)


def test_rule_node_and_boot_fail() -> None:
    assert "node" in slurm.diagnose_job(dctx(state="NODE_FAIL"))[0].lower()
    assert "boot" in slurm.diagnose_job(dctx(state="BOOT_FAIL"))[0].lower()


def test_rule_cancelled_self_vs_other() -> None:
    v, _ = slurm.diagnose_job(dctx(state="CANCELLED", cancelled_by="alice", whoami="alice"))
    assert "you" in v.lower()
    v, _ = slurm.diagnose_job(dctx(state="CANCELLED", cancelled_by="bob", whoami="alice"))
    assert "another" in v.lower() or "admin" in v.lower()
    v, _ = slurm.diagnose_job(dctx(state="CANCELLED", cancelled_by="0", whoami="alice"))
    assert "system" in v.lower() or "scheduler" in v.lower()


@pytest.mark.parametrize(
    "tail,needle",
    [
        ("ModuleNotFoundError: No module named 'torch'", "import"),
        ("bash: frobnicate: command not found", "command"),
        ("OSError: [Errno 13] Permission denied: '/x'", "permission"),
        ("FileNotFoundError: No such file or directory: 'in.nii'", "missing"),
        ("RuntimeError: CUDA error: out of memory", "gpu"),
        ("/var/spool/slurm/job.sh: line 3: 12345 Killed  python train.py", "killed"),
    ],
)
def test_rule_failed_signatures(tail: str, needle: str) -> None:
    v, _ = slurm.diagnose_job(dctx(state="FAILED", exit_code=1, stderr_tail=tail))
    assert needle in v.lower()


@pytest.mark.parametrize(
    "reason,needle",
    [
        ("Priority", "priorit"),
        ("Resources", "resource"),
        ("QOSMaxJobsPerUserLimit", "qos"),
        ("AssocGrpBillingMinutes", "billing"),
        ("ReqNodeNotAvail", "avail"),
        ("Dependency", "dependen"),
    ],
)
def test_rule_pending_reasons(reason: str, needle: str) -> None:
    v, _ = slurm.diagnose_job(dctx(state="PENDING", reason=reason))
    assert needle in v.lower()


def test_generic_failed_fallback() -> None:
    v, _ = slurm.diagnose_job(dctx(state="FAILED", exit_code=42))
    assert "42" in v


# -- size cap ordering ----------------------------------------------------------------------
def test_cap_ordering_keeps_verdict_and_hints() -> None:
    big = "x" * 40000
    out = {
        "job_id": "1",
        "status": {"state": "OUT_OF_MEMORY"},
        "script": big,
        "stdout_tail": big,
        "stderr_tail": big,
        "steps": [{"step": "batch", "max_rss": 1} for _ in range(200)],
        "sync": None,
        "verdict": "Out of memory",
        "hints": ["raise --mem"],
        "truncated": False,
    }
    capped = slurm.cap_diagnostic_fields(dict(out), cap=64 * 1024)
    assert len(json.dumps(capped, default=str)) <= 64 * 1024
    assert capped["truncated"] is True
    # highest-priority fields are untouched, lowest (steps) sacrificed first
    assert capped["verdict"] == "Out of memory"
    assert capped["hints"] == ["raise --mem"]
    assert capped["steps"] == []


def test_cap_noop_when_small() -> None:
    out = {"verdict": "ok", "hints": [], "script": "short", "steps": [], "truncated": False}
    capped = slurm.cap_diagnostic_fields(dict(out))
    assert capped["truncated"] is False
    assert capped["script"] == "short"


# -- integration via FakeSlurm outcomes -----------------------------------------------------
def _submit_and_finish(c, script="#!/bin/bash\necho hi\n", **kw):
    job = c.submit(script, **kw)
    c.wait(job.job_id, poll=5)
    return job


def test_diagnose_oom_integration(make_cluster) -> None:
    c = make_cluster({"FAKESLURM_OUTCOME": "oom"})
    job = _submit_and_finish(c)
    d = c.diagnose(job.job_id)
    assert d["status"]["state"] == "OUT_OF_MEMORY"
    assert "memory" in d["verdict"].lower()
    # req_mem (1000M) and max_rss (1100M) were parsed -> the percentage hint is present
    assert any("peak memory" in h for h in d["hints"])


def test_diagnose_timeout_integration(make_cluster) -> None:
    c = make_cluster({"FAKESLURM_OUTCOME": "timeout"})
    job = _submit_and_finish(c)
    d = c.diagnose(job.job_id)
    assert d["status"]["state"] == "TIMEOUT"
    assert "time" in d["verdict"].lower()


def test_diagnose_nodefail_integration(make_cluster) -> None:
    c = make_cluster({"FAKESLURM_OUTCOME": "nodefail"})
    job = _submit_and_finish(c)
    d = c.diagnose(job.job_id)
    assert d["status"]["state"] == "NODE_FAIL"
    assert "node" in d["verdict"].lower()


def test_diagnose_fail_signature_in_stdout(make_cluster) -> None:
    c = make_cluster({"FAKESLURM_OUTCOME": "fail:ModuleNotFoundError: No module named 'torch'"})
    job = _submit_and_finish(c)
    d = c.diagnose(job.job_id)
    assert d["status"]["state"] == "FAILED"
    assert "import" in d["verdict"].lower() or "module" in d["verdict"].lower()
    assert "No module named" in (d["stdout_tail"] + d["stderr_tail"])


def test_diagnose_reads_distinct_stderr(make_cluster) -> None:
    c = make_cluster({"FAKESLURM_OUTCOME": "fail:OSError: Permission denied"})
    # a distinct --error file makes diagnose read the stderr tail, not stdout
    job = _submit_and_finish(c, error="err-%j.log")
    d = c.diagnose(job.job_id)
    assert "Permission denied" in d["stderr_tail"]
    assert "permission" in d["verdict"].lower()


def test_diagnose_includes_script_and_steps(make_cluster) -> None:
    c = make_cluster()
    job = _submit_and_finish(c, script="#!/bin/bash\necho MARKER_LINE\n")
    d = c.diagnose(job.job_id)
    assert "MARKER_LINE" in (d["script"] or "")
    assert d["status"]["state"] == "COMPLETED"
    assert "success" in d["verdict"].lower()
    assert {s["step"] for s in d["steps"]} == {"batch", "extern"}


def test_diagnose_surfaces_sync_marker(cluster, sandbox) -> None:
    job = cluster.submit("#!/bin/bash\necho hi\n")
    cluster.wait(job.job_id, poll=5)
    marker = {
        "pushed_at": "2026-08-20T00:00:00+00:00",
        "local_git_rev": "abc123def456789",
        "local_dirty": True,
        "files": 3,
        "bytes": 100,
        "project": "demo",
    }
    (sandbox / ".remoteslurm-sync.json").write_text(json.dumps(marker))
    d = cluster.diagnose(job.job_id)
    assert d["sync"] is not None
    assert d["sync"]["local_git_rev"] == "abc123def456789"
    assert any("abc123def456" in h for h in d["hints"])  # rev[:12] echoed in the hint


# -- CLI + MCP surfaces ---------------------------------------------------------------------
def test_cli_diagnose_json_and_human(make_cluster, monkeypatch, capsys) -> None:
    from remoteslurm import cli

    c = make_cluster({"FAKESLURM_OUTCOME": "oom"})
    job = _submit_and_finish(c)
    monkeypatch.setattr(cli, "get_cluster", lambda args, host=None: c)

    assert cli.main(["diagnose", job.job_id, "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert "memory" in out["verdict"].lower()

    assert cli.main(["diagnose", job.job_id]) == 0
    human = capsys.readouterr().out
    assert "memory" in human.lower()


def test_mcp_diagnose_is_core(make_cluster, monkeypatch) -> None:
    from remoteslurm import server

    c = make_cluster({"FAKESLURM_OUTCOME": "timeout"})
    job = _submit_and_finish(c)
    monkeypatch.setattr(server, "_get_cluster", lambda host: c)

    async def go() -> dict:
        # diagnose must be reachable on the *core* (default) server
        async with create_connected_server_and_client_session(server.mcp) as client:
            res = await client.call_tool("diagnose", {"job_id": job.job_id})
            assert not res.isError, res.content
            if res.structuredContent is not None:
                return dict(res.structuredContent)
            return dict(json.loads(res.content[0].text))

    d = asyncio.run(go())
    assert "time" in d["verdict"].lower()
