"""Read-only discovery of every job the user owns, against FakeSlurm."""

from __future__ import annotations

import time

import pytest

from remoteslurm.activity.discovery import (
    DiscoveryObserver,
    lease,
    parse_squeue,
    parse_time,
)
from remoteslurm.activity.monitor import READS, ReadBudget
from remoteslurm.activity.store import ActivityStore


def observer(cluster) -> DiscoveryObserver:
    return DiscoveryObserver(lambda _: cluster, lambda _: cluster.host)


def kinds(store: ActivityStore, kind: str) -> list[dict]:
    return [e["payload"] for e in store.query(limit=1000)["events"] if e["kind"] == kind]


def submit(cluster, sandbox, **kw):
    (sandbox / "proj").mkdir(exist_ok=True)
    return cluster.submit(script="#!/bin/sh\ntrue", cwd=str(sandbox / "proj"), **kw)


def test_squeue_parse_keeps_extra_columns_and_array_throttle():
    out = (
        "4412702_[5-9%16]|fit|PENDING|JobArrayTaskLimit|compute|def|0:00|3:00:00|1|"
        "(JobArrayTaskLimit)|2026-11-05T10:00:00|N/A|/s|brad|192|750G|N/A\n"
        "4412702_1|fit|RUNNING|None|compute|def|12:01|3:00:00|1|c1|2026-11-05T10:00:00|"
        "2026-11-05T10:05:00|/s|brad|192|750G|2026-11-05T13:05:00\n"
    )
    rows, throttles = parse_squeue(out)
    assert throttles == {"4412702": 16}
    assert [r["array_task"] for r in rows] == ["5", "6", "7", "8", "9", "1"]
    assert rows[-1]["cpus"] == "192" and rows[-1]["min_memory"] == "750G"
    assert parse_time("N/A") is None and parse_time("2026-11-05T10:05:00") is not None


def test_scontrol_is_a_permitted_passive_read():
    assert "scontrol" in READS and "scancel" not in READS and "sbatch" not in READS


def test_discovers_untracked_jobs_and_the_first_scan_is_not_news(cluster, sandbox):
    store = ActivityStore()
    submit(cluster, sandbox)
    doc = observer(cluster).observe("local", cluster.host)
    assert doc["error"] is None and doc["accounting"] == "ok"
    assert set(doc["jobs"]) == {"1000"}
    assert kinds(store, "jobs.discovered") and not kinds(store, "job.state.changed")


def test_transitions_after_baseline_become_events(cluster, sandbox):
    store = ActivityStore()
    obs = observer(cluster)
    obs.observe("local", cluster.host)  # empty baseline
    submit(cluster, sandbox)
    now = time.time()
    for i in range(5):
        doc = obs.observe("local", cluster.host, now=now + 61 * (i + 1))
    assert doc["jobs"]["1000"]["state"] == "COMPLETED"
    assert doc["jobs"]["1000"]["exit"] == "0:0"
    changes = kinds(store, "job.state.changed")
    assert changes[0]["from"] is None and changes[-1]["state"] == "COMPLETED"
    # Peak memory comes from a stepped sacct for the finished job, never from squeue.
    assert doc["jobs"]["1000"]["maxrss"] is not None


def test_arrays_roll_up_and_new_failed_tasks_are_news(make_cluster, sandbox):
    cluster = make_cluster({"FAKESLURM_ARRAY_FAIL": "2"})
    store = ActivityStore()
    obs = observer(cluster)
    obs.observe("local", cluster.host)
    submit(cluster, sandbox, array="0-3")
    now = time.time()
    for i in range(8):
        doc = obs.observe("local", cluster.host, now=now + 61 * (i + 1))
    job = doc["jobs"]["1000"]
    assert job["array"] and set(job["tasks"]) == {"0", "1", "2", "3"}
    assert job["tasks"]["2"] == "FAILED" and job["failed_tasks"] == 1
    assert job["state"] == "FAILED" and "2" in job["task_info"]
    assert any(p.get("new_failed_tasks") for p in kinds(store, "job.state.changed"))


def test_sacct_failure_reports_accounting_unavailable_not_none(cluster, sandbox, monkeypatch):
    obs = observer(cluster)
    submit(cluster, sandbox)
    obs.observe("local", cluster.host)
    real = cluster.call

    def no_sacct(op, **args):
        if op == "sacct":
            return {"rc": 1, "stdout": "", "stderr": "slurmdbd down"}
        return real(op, **args)

    monkeypatch.setattr(cluster, "call", no_sacct)
    doc = obs.observe("local", cluster.host, now=time.time() + 120)
    assert doc["accounting"] == "unavailable" and doc["error"] is None
    assert doc["accounting_at"] is not None  # the last good read is kept for "as of"
    assert kinds(ActivityStore(), "discovery.accounting")[-1]["state"] == "unavailable"


def test_lost_connection_keeps_jobs_and_says_why(cluster, sandbox, monkeypatch):
    from remoteslurm.errors import NotConnected

    obs = observer(cluster)
    submit(cluster, sandbox)
    first = obs.observe("local", cluster.host)

    def down(op, **args):
        raise NotConnected("no ssh master")

    monkeypatch.setattr(cluster, "call", down)
    doc = obs.observe("local", cluster.host, now=time.time() + 40)
    assert doc["error"] == "not_connected" and doc["jobs"] == first["jobs"]
    assert doc["observed_at"] == first["observed_at"]  # never refreshed by a failed read


def test_budget_exhaustion_retries_soon_without_reporting_an_error(cluster, sandbox):
    store = ActivityStore()
    budgeted = ReadBudget(cluster, store)
    obs = DiscoveryObserver(lambda _: budgeted, lambda _: cluster.host)
    for _ in range(8):
        budgeted.call("squeue", format="%i")
    now = time.time()
    doc = obs.observe("local", cluster.host, now=now)
    assert doc["error"] is None and doc["next_poll"] == pytest.approx(now + 5)


def test_hosts_are_discovered_only_while_leased_or_attached(cluster):
    obs = observer(cluster)
    assert obs.hosts() == [] and not obs.active()
    lease([cluster.host], seconds=60)
    assert obs.hosts() == ["local"] and obs.active()
    lease([cluster.host], seconds=-1)
    assert obs.hosts() == []


def test_leased_job_log_tail_round_trips_through_the_observer(cluster, sandbox):
    from remoteslurm.config import Config
    from remoteslurm.web.provider import LiveProvider
    from remoteslurm.workspace.observer import WorkspaceObserver

    obs = observer(cluster)
    submit(cluster, sandbox)
    obs.observe("local", cluster.host)
    lease([cluster.host])
    (sandbox / "proj" / "slurm-1000.out").write_text("hello\n<b>done</b>\n")
    provider = LiveProvider(Config(hosts={"local": cluster.host}), [cluster.host])
    queued = provider.tail("local", "1000", "out", None)
    assert queued["state"] == "QUEUED", queued
    WorkspaceObserver(lambda _: cluster, lambda _: cluster.host).tick()
    done = provider.poll(queued["request"], "tail")
    assert done["state"] == "DONE", (done["error"], done["action"])
    assert done["result"]["lines"] == ["hello", "<b>done</b>"]


# ----------------------------------------------------------------------------- review regressions
def blank(baseline: bool = True) -> dict:
    return {"jobs": {}, "baseline": baseline, "baseline_at": 0.0}


def row(jid: str, state: str, **kw) -> tuple:
    line = "|".join(
        [
            jid,
            "sweep",
            state,
            kw.get("reason", "None"),
            "p",
            "a",
            "0:00",
            "1:00:00",
            "1",
            kw.get("node", "(Priority)"),
            "2026-11-05T10:00:00",
            "N/A",
            "/s",
            "u",
            "1",
            "1G",
            "N/A",
        ]
    )
    return parse_squeue(line + "\n")


def test_array_whose_tasks_leave_the_queue_is_unknown_never_running_and_settles():
    obs = DiscoveryObserver(None, None)
    doc = blank()
    rows, thr = row("123_[0-3%2]", "PENDING", reason="JobArrayTaskLimit")
    obs.merge(doc, rows, thr, None, 1000.0)
    assert doc["jobs"]["123"]["state"] == "PENDING"
    changes = obs.merge(doc, [], {}, None, 1030.0)  # cancelled: gone from the queue
    assert doc["jobs"]["123"]["state"] == "UNKNOWN"
    assert not [c for c in changes if c[2]["state"] == "RUNNING"]
    accounting = {"123_[0-3%2]": {"state": "CANCELLED", "end_time": "Unknown"}}
    obs.merge(doc, [], {}, accounting, 1060.0)
    assert doc["jobs"]["123"]["state"] == "CANCELLED"


def test_partly_finished_pending_array_is_pending_not_running():
    obs = DiscoveryObserver(None, None)
    doc = blank()
    rows, thr = row("7_[1-3]", "PENDING")
    accounting = {
        "7_0": {"state": "COMPLETED", "exit_code": "0:0", "end_time": "2026-11-05T10:30:00"}
    }
    obs.merge(doc, rows, thr, accounting, 1000.0)
    assert doc["jobs"]["7"]["state"] == "PENDING"


def test_fully_completed_array_records_its_end_and_ages_out():
    obs = DiscoveryObserver(None, None)
    doc = blank()
    accounting = {
        f"9_{t}": {
            "state": "COMPLETED",
            "exit_code": "0:0",
            "end_time": f"2026-11-0{t + 1}T10:00:00",
        }
        for t in range(3)
    }
    obs.merge(doc, [], {}, accounting, 1000.0)
    job = doc["jobs"]["9"]
    assert job["state"] == "COMPLETED" and job["end"] == parse_time("2026-11-03T10:00:00")
    DiscoveryObserver.prune(doc, job["end"] + 8 * 86400)
    assert doc["jobs"] == {}


def test_cancelled_tasks_are_not_reported_as_failures():
    from remoteslurm.web.state import Journal

    raw = {
        "id": "5",
        "array": True,
        "state": "FAILED",
        "tasks": {"0": "CANCELLED", "1": "FAILED"},
        "task_info": {"0": {"exit": "0:0"}, "1": {"exit": "1:0"}},
    }
    job = Journal.discovered("h", raw, {})
    assert job["array"]["exits"] == {"1": "1:0"}


def test_unstorable_document_backs_off_and_never_replays_events(cluster, sandbox, monkeypatch):
    from remoteslurm.errors import TooLarge

    store = ActivityStore()
    obs = observer(cluster)
    obs.observe("local", cluster.host)
    submit(cluster, sandbox)
    real = store.save

    def too_large(namespace, key, value):
        if namespace == "discovery":
            raise TooLarge("activity document exceeds 2 MiB")
        return real(namespace, key, value)

    monkeypatch.setattr(obs.store, "save", too_large)
    now = time.time() + 61
    with pytest.raises(TooLarge):
        obs.observe("local", cluster.host, now=now)  # the fallback save also fails here
    monkeypatch.setattr(obs.store, "save", real)
    before = len(kinds(store, "job.state.changed"))
    assert before == 0  # events are written only after the document is stored


def test_reopening_after_a_long_gap_catches_up_quietly(cluster, sandbox):
    store = ActivityStore()
    obs = observer(cluster)
    obs.observe("local", cluster.host)
    for _ in range(3):
        submit(cluster, sandbox)
    obs.observe("local", cluster.host, now=time.time() + 3 * 86400)
    assert kinds(store, "job.state.changed") == []
    assert kinds(store, "jobs.discovered")[-1]["resumed"] is True


def test_array_task_log_is_unknown_without_a_task_specific_path():
    from remoteslurm.web.files import task_path

    job = {"id": "4", "array": {"pathsTask": None}}
    assert task_path("/l/slurm-4_4294967294.out", job, 3) is None
    assert task_path("/l/out_%A_%a.log", job, 3) == "/l/out_4_3.log"
