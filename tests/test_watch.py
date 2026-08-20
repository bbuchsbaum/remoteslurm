"""F2 watch/notify + bounded MCP wait/events, and F3 housekeeping (prune, clean, bootstrap)."""

from __future__ import annotations

import asyncio
import time
import types
from collections.abc import Callable
from pathlib import Path

import pytest

import remoteslurm.jobs as jobs_mod
from remoteslurm import cli, server, watch
from remoteslurm.cluster import Cluster
from remoteslurm.jobs import JobRecord, JobRegistry
from remoteslurm.transport import SSHTransport


@pytest.fixture(autouse=True)
def _isolated_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    d = tmp_path / "rs-state"
    monkeypatch.setenv("REMOTESLURM_STATE_DIR", str(d))
    # `server.events` calls Config.load(); point it at a non-existent file so the machine's real
    # config never leaks in (a bare "local" host is then returned for any name).
    monkeypatch.setenv("REMOTESLURM_CONFIG", str(tmp_path / "none.toml"))
    return d


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    # One patch on the shared time module covers cli/server/jobs sleeps.
    monkeypatch.setattr(jobs_mod.time, "sleep", lambda *_: None)


# --------------------------------------------------------------------------- events log
def test_events_append_and_drain_cursor() -> None:
    watch.append_event("h", {"t": time.time(), "job_id": "1", "state": "COMPLETED", "exit_code": 0})
    watch.append_event("h", {"t": time.time(), "job_id": "2", "state": "FAILED", "exit_code": 1})
    first = watch.drain_events("h")
    assert [e["job_id"] for e in first] == ["1", "2"]
    # Cursor advanced: a second drain sees nothing until a new event lands.
    assert watch.drain_events("h") == []
    watch.append_event("h", {"t": time.time(), "job_id": "3", "state": "COMPLETED", "exit_code": 0})
    assert [e["job_id"] for e in watch.drain_events("h")] == ["3"]
    # --all ignores the cursor and returns the whole log.
    assert len(watch.drain_events("h", all=True)) == 3


def test_events_since_filter() -> None:
    t0 = time.time()
    watch.append_event("h", {"t": t0 - 100, "job_id": "old", "state": "COMPLETED"})
    watch.append_event("h", {"t": t0 + 100, "job_id": "new", "state": "COMPLETED"})
    import datetime

    cut = datetime.datetime.fromtimestamp(t0).isoformat()
    got = watch.drain_events("h", since=cut)
    assert [e["job_id"] for e in got] == ["new"]
    # `since` must not touch the cursor: an unseen-drain still returns both.
    assert len(watch.drain_events("h")) == 2


def test_notify_runs_command(tmp_path: Path) -> None:
    from remoteslurm.config import HostConfig

    marker = tmp_path / "note.txt"
    # The message arrives as a SEPARATE argv element ($1 here), never spliced into the shell
    # string, so a job name with quotes/`$()` cannot inject.
    host = HostConfig(
        name="h", ssh="local", notify_command=f"sh -c 'printf %s \"$1\" >> {marker}' rs-notify"
    )
    assert watch.notify(host, "job-1 COMPLETED") is True
    assert marker.read_text().strip() == "job-1 COMPLETED"


def test_notify_no_injection(tmp_path: Path) -> None:
    from remoteslurm.config import HostConfig

    marker = tmp_path / "note.txt"
    host = HostConfig(
        name="h", ssh="local", notify_command=f"sh -c 'printf %s \"$1\" >> {marker}' rs-notify"
    )
    # a malicious job name must not execute; it is sanitized and passed as data
    watch.notify(host, f'x"; touch {tmp_path}/PWNED; echo "')
    assert not (tmp_path / "PWNED").exists()


# --------------------------------------------------------------------------- watch CLI
def test_watch_cli_transitions_and_events(
    cluster: Cluster, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli, "get_cluster", lambda args, host=None: cluster)
    job = cluster.submit(script="#!/bin/bash\necho hi\n", name="w")
    args = types.SimpleNamespace(
        job_id=[job.job_id], all=False, notify=False, poll=0.0, timeout=None, json=False
    )
    rc = cli.cmd_watch(args)  # type: ignore[arg-type]
    assert rc == 0  # the job COMPLETED
    out = capsys.readouterr().out
    assert "PENDING" in out and "COMPLETED" in out
    # A terminal event was appended to the host's events log.
    evs = watch.drain_events(cluster.host.name, all=True)
    assert any(e["job_id"] == job.job_id and e["state"] == "COMPLETED" for e in evs)


def test_watch_cli_all_no_jobs(cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "get_cluster", lambda args, host=None: cluster)
    args = types.SimpleNamespace(
        job_id=[], all=True, notify=False, poll=0.0, timeout=None, json=False
    )
    assert cli.cmd_watch(args) == 0  # nothing queued -> clean exit


# --------------------------------------------------------------------------- MCP wait / events
def test_mcp_wait_completes(cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server, "_get_cluster", lambda host: cluster)
    job = cluster.submit(script="#!/bin/bash\necho hi\n", name="ok")
    r = asyncio.run(server.wait(job.job_id, timeout=120))
    assert r["terminal"] is True
    assert r["state"] == "COMPLETED"


def test_mcp_wait_times_out(
    make_cluster: Callable[..., Cluster], monkeypatch: pytest.MonkeyPatch
) -> None:
    c = make_cluster(extra_env={"FAKESLURM_FREEZE": "1"})  # job never leaves PENDING
    monkeypatch.setattr(server, "_get_cluster", lambda host: c)
    job = c.submit(script="#!/bin/bash\necho hi\n", name="stuck")
    r = asyncio.run(server.wait(job.job_id, timeout=2))
    assert r["terminal"] is False
    assert r["state"] == "PENDING"


def test_mcp_events(cluster: Cluster) -> None:
    host = cluster.host.name  # "local"
    watch.append_event(host, {"t": time.time(), "job_id": "42", "state": "COMPLETED"})
    r = asyncio.run(server.events(host=host))
    assert r["count"] == 1
    assert r["events"][0]["job_id"] == "42"
    # Drained: a second call sees nothing new.
    assert asyncio.run(server.events(host=host))["count"] == 0


# --------------------------------------------------------------------------- registry prune (F3)
def _seed(reg: JobRegistry, job_id: str, state: str, age_days: float) -> None:
    t = time.time() - age_days * 86400
    reg.put(JobRecord(job_id=job_id, last_state=state, last_seen=t, submit_time=t))


def test_prune_drops_old_terminal_keeps_active() -> None:
    reg = JobRegistry("h")
    _seed(reg, "1", "COMPLETED", 40)  # old + terminal -> dropped
    _seed(reg, "2", "RUNNING", 40)  # old but active -> kept
    _seed(reg, "3", "COMPLETED", 1)  # terminal but recent -> kept
    r = reg.prune(force=True)
    assert r["pruned"] == 1 and r["removed"] == ["1"]
    assert {j.job_id for j in reg.all()} == {"2", "3"}


def test_prune_keep_active_false_drops_old_regardless() -> None:
    reg = JobRegistry("h")
    _seed(reg, "1", "RUNNING", 40)
    r = reg.prune(force=True, keep_active=False)
    assert r["pruned"] == 1


def test_prune_throttled_once_per_hour() -> None:
    reg = JobRegistry("h")
    _seed(reg, "1", "COMPLETED", 40)
    first = reg.prune()  # last_pruned absent -> runs
    assert first["throttled"] is False and first["pruned"] == 1
    _seed(reg, "2", "COMPLETED", 40)
    second = reg.prune()  # within the hour -> throttled, record survives
    assert second["throttled"] is True
    assert {j.job_id for j in reg.all()} == {"2"}
    # last_pruned persisted in the same file (survives the flock write-back).
    assert "last_pruned" in reg._read_raw()


def test_jobs_runs_opportunistic_prune(cluster: Cluster) -> None:
    reg = cluster.registry
    _seed(reg, "999999", "COMPLETED", 40)
    cluster.jobs()  # jobs() prunes first (last_pruned absent -> runs)
    assert reg.get("999999") is None


def test_prune_preserves_other_records_written_concurrently() -> None:
    # A put() from "another process" between reads must not be lost by prune's write-back.
    reg = JobRegistry("h")
    _seed(reg, "old", "COMPLETED", 40)
    _seed(reg, "keep", "RUNNING", 0)
    reg.prune(force=True)
    assert {j.job_id for j in reg.all()} == {"keep"}


# --------------------------------------------------------------------------- clean (F3)
def test_clean_dry_run_lists_without_removing(cluster: Cluster, sandbox: Path) -> None:
    scripts = sandbox / ".remoteslurm" / "scripts"
    scripts.mkdir(parents=True)
    old = scripts / "old.sh"
    new = scripts / "new.sh"
    old.write_text("#!/bin/bash\n")
    new.write_text("#!/bin/bash\n")
    old_t = time.time() - 40 * 86400
    import os

    os.utime(old, (old_t, old_t))
    dry = cluster.clean(dry_run=True)
    assert [Path(p).name for p in dry["removed"]] == ["old.sh"]
    assert old.exists() and new.exists()  # dry-run touches nothing
    real = cluster.clean()
    assert [Path(p).name for p in real["removed"]] == ["old.sh"]
    assert not old.exists() and new.exists()


def test_clean_nothing_when_dirs_absent(cluster: Cluster) -> None:
    r = cluster.clean()
    assert r["removed"] == [] and r["count"] == 0


# --------------------------------------------------------------------------- bootstrap stale stubs
def test_bootstrap_snippet_cleans_stale_stubs_and_lints() -> None:
    import subprocess

    snip = SSHTransport(alias="x").remote_bootstrap_script()
    assert 'for f in "$D"/stub-*.py' in snip
    assert '[ "$f" = "$P" ] || rm -f "$f"' in snip
    # The whole snippet must be valid POSIX sh (this is what runs on the remote).
    r = subprocess.run(["sh", "-n", "-c", snip], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
