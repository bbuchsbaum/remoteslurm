"""F1 queue-intelligence tests: parsers, Cluster.queue_info/estimate_start/quota, MCP tools."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import remoteslurm.jobs as jobs_mod
from remoteslurm import server, slurm
from remoteslurm.cluster import Cluster

FIX = Path(__file__).parent / "fixtures" / "slurm"


@pytest.fixture(autouse=True)
def _isolated_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    d = tmp_path / "rs-state"
    monkeypatch.setenv("REMOTESLURM_STATE_DIR", str(d))
    return d


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jobs_mod.time, "sleep", lambda *_: None)


# --------------------------------------------------------------------------- pure parsers
def test_parse_squeue_start() -> None:
    rows = slurm.parse_squeue_start((FIX / "squeue_start.txt").read_text())
    assert rows[0] == {"job_id": "1234", "est_start": "2026-08-20T18:30:00", "reason": "Priority"}
    # N/A start collapses to None but the reason is kept.
    assert rows[1] == {"job_id": "1235", "est_start": None, "reason": "Resources"}
    assert rows[2]["est_start"] == "2026-08-21T02:00:00"


def test_parse_squeue_start_missing_columns() -> None:
    # A short line (only an id) must not raise; missing columns -> None.
    rows = slurm.parse_squeue_start("999\n\n1000|N/A\n")
    assert rows[0] == {"job_id": "999", "est_start": None, "reason": None}
    assert rows[1] == {"job_id": "1000", "est_start": None, "reason": None}


def test_parse_sshare() -> None:
    rows = slurm.parse_sshare((FIX / "sshare.txt").read_text())
    assert len(rows) == 2  # header skipped
    assert rows[0]["account"] == "def-brad"
    assert rows[0]["user"] == "brad"
    assert rows[0]["norm_shares"] == pytest.approx(0.25)
    assert rows[0]["raw_usage"] == 162
    assert rows[0]["fair_share"] == pytest.approx(0.439105)
    assert rows[1]["raw_usage"] == 17228145


def test_parse_sshare_tolerates_short_and_bad_numbers() -> None:
    rows = slurm.parse_sshare("acct|me|x|y\n")  # non-numeric + missing trailing columns
    assert rows[0]["account"] == "acct"
    assert rows[0]["raw_shares"] is None  # "x" not an int
    assert rows[0]["norm_shares"] is None  # "y" not a float
    assert rows[0]["fair_share"] is None  # column absent


def test_parse_qos() -> None:
    rows = slurm.parse_qos((FIX / "qos.txt").read_text())
    names = {r["name"]: r for r in rows}
    assert names["normal"]["max_jobs_pu"] == "150"
    assert names["debug"]["max_jobs_pu"] == "1"  # debug allows 1 job/user
    # Empty columns become None; a present "0" is kept as the string "0" (recorded output).
    assert names["normal"]["max_wall"] is None
    assert names["normal"]["priority"] == "0"
    assert names["debug"]["priority"] == "0"


def test_parse_assoc() -> None:
    rows = slurm.parse_assoc((FIX / "assoc.txt").read_text())
    assert rows[0]["account"] == "def-brad"
    assert rows[0]["qos"] == "normal"
    assert rows[1]["qos"] == "normal,debug"
    assert rows[0]["partition"] is None  # empty column


def test_parse_df_gnu_and_macos() -> None:
    gnu = (
        "Filesystem      Size  Used Avail Use% Mounted on\n"
        "/dev/sda1       100G   40G   60G  40% /home\n"
    )
    rows = slurm.parse_df(gnu)
    assert rows[0] == {
        "filesystem": "/dev/sda1",
        "size": "100G",
        "used": "40G",
        "avail": "60G",
        "use_pct": "40%",
        "mounted_on": "/home",
    }
    # macOS df -h has extra inode columns; the mount is still the last token, capacity the % one.
    mac = (
        "Filesystem     Size   Used  Avail Capacity iused ifree %iused  Mounted on\n"
        "/dev/disk3s5  926Gi  860Gi   11Gi    99%  1.0M  120M    1%   /System/Volumes/Data\n"
    )
    r = slurm.parse_df(mac)[0]
    assert r["filesystem"] == "/dev/disk3s5"
    assert r["use_pct"] == "99%"
    assert r["mounted_on"] == "/System/Volumes/Data"


def test_parse_diskusage_report() -> None:
    # Recorded fixed-width output: spaced slashes and an internal-space value ("0  B").
    rows = slurm.parse_quota_pairs((FIX / "diskusage.txt").read_text())
    assert len(rows) == 4  # header line dropped, 4 filesystems
    home = rows[0]
    assert home["description"] == "/home (user def-brad)"
    assert home["used"] == "88GiB"
    assert home["limit"] == "100GiB"
    assert home["files_used"] == "767K"
    assert home["files_limit"] == "1000K"
    # The "0  B/1024GiB   7 /2000K" row: internal space + slashes padded on either side.
    proj = rows[2]
    assert proj["description"] == "/project (project def-brad)"
    assert proj["used"] == "0 B"
    assert proj["limit"] == "1024GiB"
    assert proj["files_used"] == "7"
    assert proj["files_limit"] == "2000K"


def test_parse_diskusage_report_tolerates_freeform() -> None:
    # A line with no quota pair must still produce a row (raw preserved), never raise.
    rows = slurm.parse_diskusage_report("Some note with no numbers here\n")
    assert rows[0]["used"] is None and rows[0]["limit"] is None
    assert rows[0]["raw"] == "Some note with no numbers here"


# --------------------------------------------------------------------------- Cluster methods
def _frozen(make_cluster: Callable[..., Cluster], **kw: Any) -> Cluster:
    return make_cluster(extra_env={"FAKESLURM_FREEZE": "1"}, **kw)


def test_estimate_start(make_cluster: Callable[..., Cluster]) -> None:
    c = _frozen(make_cluster)
    job = c.submit(script="#!/bin/bash\necho hi\n", name="e1")
    est = c.estimate_start(job.job_id)
    assert est is not None
    assert est["job_id"] == job.job_id
    assert est["est_start"]  # the fake scheduler gives a pending job an estimate
    assert est["reason"] == "Priority"


def test_queue_info_shape(make_cluster: Callable[..., Cluster]) -> None:
    c = _frozen(make_cluster)
    job = c.submit(script="#!/bin/bash\necho hi\n", name="q1")
    q = c.queue_info()
    assert q["partitions"]  # from the sinfo fixture
    assert len(q["fairshare"]) == 2
    assert {r["name"] for r in q["qos"]} == {"normal", "debug"}
    assert len(q["accounts"]) == 2
    assert len(q["pending"]) == 1
    p = q["pending"][0]
    assert p["job_id"] == job.job_id
    assert p["est_start"]
    assert p["reason"] == "Priority"


def test_quota_via_command(make_cluster: Callable[..., Cluster]) -> None:
    c = make_cluster(quota_command="diskusage_report --per_user", quota_format="pairs")
    q = c.quota()
    assert q["available"] is True
    assert q["source"] == "command"
    assert q["format"] == "pairs"
    assert len(q["usage"]) == 4
    assert q["usage"][0]["used"] == "88GiB"
    assert q["usage"][0]["limit"] == "100GiB"


def test_quota_df_fallback(cluster: Cluster) -> None:
    q = cluster.quota()
    assert q["source"] == "df"
    assert q["available"] is True  # real df of the sandbox home
    assert q["usage"]
    assert "mounted_on" in q["usage"][0]


def test_quota_missing_command_is_unavailable(make_cluster: Callable[..., Cluster]) -> None:
    c = make_cluster(quota_command="rs_no_such_tool_xyz --per_user")
    q = c.quota()
    assert q["available"] is False
    assert q["usage"] == []


def test_quota_custom_command_is_raw_by_default(make_cluster: Callable[..., Cluster]) -> None:
    c = make_cluster(quota_command="printf 'site-specific output\\n'")
    q = c.quota()
    assert q["available"] is True
    assert q["format"] == "raw"
    assert q["usage"] == []
    assert q["raw"] == "site-specific output\n"


def test_queue_info_degrades_when_tools_absent(make_cluster: Callable[..., Cluster]) -> None:
    # A PATH without the FakeSlurm shims: every section degrades to empty, nothing raises.
    c = make_cluster(extra_env={"PATH": "/usr/bin:/bin"})
    q = c.queue_info()
    assert q["partitions"] == []
    assert q["fairshare"] == []
    assert q["qos"] == []
    assert q["accounts"] == []
    assert q["pending"] == []


def test_diagnose_pending_includes_estimate(make_cluster: Callable[..., Cluster]) -> None:
    c = _frozen(make_cluster)
    job = c.submit(script="#!/bin/bash\necho hi\n", name="d1")
    d = c.diagnose(job.job_id)
    assert d["status"]["state"] == "PENDING"
    assert any("estimated start" in h for h in d["hints"])


# --------------------------------------------------------------------------- MCP tools
def _mcp(cluster: Cluster, monkeypatch: pytest.MonkeyPatch, tool: str, **args: Any) -> dict:
    monkeypatch.setattr(server, "_get_cluster", lambda host: cluster)
    return asyncio.run(getattr(server, tool)(**args))


def test_mcp_queue_info(
    make_cluster: Callable[..., Cluster], monkeypatch: pytest.MonkeyPatch
) -> None:
    c = _frozen(make_cluster)
    c.submit(script="#!/bin/bash\necho hi\n", name="mq")
    r = _mcp(c, monkeypatch, "queue_info")
    assert "partitions" in r and "fairshare" in r and "pending" in r
    assert len(r["fairshare"]) == 2


def test_mcp_quota(make_cluster: Callable[..., Cluster], monkeypatch: pytest.MonkeyPatch) -> None:
    c = make_cluster(quota_command="diskusage_report --per_user", quota_format="pairs")
    r = _mcp(c, monkeypatch, "quota")
    assert r["available"] is True
    assert len(r["usage"]) == 4


def test_queue_info_and_quota_in_all_set() -> None:
    names = set(server.ALL_TOOLS)
    assert {"queue_info", "quota", "events"} <= names
    from remoteslurm.server import CORE_TOOLS

    assert not ({"queue_info", "quota", "events"} & CORE_TOOLS)  # `all` only
    assert "wait" in CORE_TOOLS


def test_quota_runs_in_login_shell(make_cluster, tmp_path):
    """quota_command runs via `bash -lc` so a login-defined site function is available."""
    bashrc = tmp_path / "login.sh"
    bashrc.write_text(
        "diskusage_report() { printf '%s\\n' "
        "'                  /home (user me)        88GiB/ 100GiB         5K/10K'; }\n"
    )
    # BASH_ENV makes non-interactive `bash -lc`/`bash -c` source the file defining the function
    c = make_cluster(
        quota_command="diskusage_report --per_user",
        quota_format="pairs",
        extra_env={"BASH_ENV": str(bashrc), "ENV": str(bashrc)},
    )
    q = c.quota()
    assert q["available"] and q["source"] == "command"
    assert q["usage"][0]["limit"] == "100GiB" and q["usage"][0]["used"] == "88GiB"


def test_diskusage_non_numeric_limit():
    """A project quota of `unlimited`/`inf` must keep the used value, not shift columns."""
    from remoteslurm.slurm import parse_diskusage_report

    raw = (
        "                            Description                Space         # of files\n"
        "               /project (project x)        88GiB/ unlimited        5K/10K\n"
    )
    r = parse_diskusage_report(raw)[0]
    assert r["used"] == "88GiB" and r["limit"] == "unlimited"
    assert r["files_used"] == "5K" and r["files_limit"] == "10K"


def test_events_since_bad_value_raises(tmp_path, monkeypatch):
    import pytest

    from remoteslurm import watch
    from remoteslurm.errors import InvalidArgument

    monkeypatch.setenv("REMOTESLURM_STATE_DIR", str(tmp_path))
    watch.append_event("h", {"t": 1.0, "job_id": "1", "state": "COMPLETED"})
    with pytest.raises(InvalidArgument):
        watch.drain_events("h", since="not-a-date")


def test_mcp_wait_bounds_wall_clock(monkeypatch):
    """MCP wait must bound elapsed wall-clock, not iteration count, even with slow job_status."""
    import time as _time

    from remoteslurm import server, slurm

    calls = {"n": 0}
    clock = {"t": 1000.0}

    def fake_status(self, job_id, refresh=False):
        calls["n"] += 1
        clock["t"] += 4.0  # each status "costs" 4s of wall-clock
        return slurm.JobStatus(job_id=job_id, state="PENDING", source="squeue", terminal=False)

    monkeypatch.setattr("remoteslurm.jobs.SlurmOps.job_status", fake_status, raising=False)
    monkeypatch.setattr(_time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(_time, "sleep", lambda s: clock.__setitem__("t", clock["t"] + s))

    import asyncio

    class FakeCluster:
        def job_status(self, job_id, refresh=False):
            return fake_status(self, job_id, refresh)

    monkeypatch.setattr(server, "_get_cluster", lambda host=None: FakeCluster())
    res = asyncio.run(server.wait("42", timeout=20))
    assert res["terminal"] is False
    # 20s cap / 4s-per-status ~ at most ~6 statuses, not unbounded
    assert calls["n"] <= 8


def test_pending_estimates_collapses_arrays(make_cluster, monkeypatch):
    """A pending array must yield ONE pending entry (base id), not one per task."""
    from remoteslurm import slurm

    c = make_cluster()
    rows = slurm.parse_squeue(
        "123_[0-999]|arr|PENDING|Priority|debug|acc|0:00|1:00|1|node|s|N/A|/w|me"
    )
    monkeypatch.setattr(type(c), "squeue", lambda self, refresh=False: rows)
    monkeypatch.setattr(
        type(c), "call", lambda self, op, **kw: {"rc": 1, "stdout": "", "stderr": ""}
    )
    pend = c._pending_estimates()
    assert len(pend) == 1 and pend[0]["job_id"] == "123"
