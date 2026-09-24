import json
import time
from types import SimpleNamespace

import pytest

from remoteslurm import job_monitor as monitor
from remoteslurm.config import HostConfig


@pytest.fixture
def setup_monitor(tmp_path, monkeypatch):
    host = HostConfig(name="h", ssh="cluster", mfa=False)
    store = monitor.WatchStore(tmp_path)
    cluster = SimpleNamespace(host=host)
    engine = monitor.JobMonitor(store, lambda name: cluster, lambda name: host)
    snapshots = {}
    calls = []

    def collect(c, ids):
        assert c is cluster
        calls.append(ids)
        return {jid: snapshots.get(jid, state("UNKNOWN")) for jid in ids}

    monkeypatch.setattr(monitor, "collect", collect)
    return host, store, engine, snapshots, calls


def state(name):
    terminal = name in {"COMPLETED", "FAILED", "CANCELLED"}
    return {"state": name, "terminal": terminal, "failed": terminal and name != "COMPLETED"}


def ready(store):
    with store.connect() as db:
        db.execute("UPDATE watches SET next_poll=0")


def start(host, store, ids, **kwargs):
    return store.start(
        host,
        ids,
        condition=kwargs.get("condition", "all_terminal_or_any_failed"),
        poll=30,
        notify=kwargs.get("notify", False),
    )["watch_id"]


def test_shared_collection_and_idempotent_start(setup_monitor):
    host, store, engine, snapshots, calls = setup_monitor
    first = start(host, store, ["1", "2"])
    assert start(host, store, ["2", "1"]) == first
    second = start(host, store, ["2", "3"])
    engine.tick()
    assert calls == [["1", "2", "3"]]
    assert store.read(first, host="h")["status"] == "active"
    assert store.read(second, host="h")["counts"] == {"UNKNOWN": 2}


def test_persistent_failure_notification_once(setup_monitor, monkeypatch):
    host, store, engine, snapshots, calls = setup_monitor
    sent = []
    monkeypatch.setattr(
        "remoteslurm.watch.notify", lambda host, message: sent.append(message) or True
    )
    watch_id = start(host, store, ["1", "2"], notify=True)
    snapshots.update({"1": state("FAILED"), "2": state("RUNNING")})
    engine.tick()
    result = store.read(watch_id, host="h")
    assert result["result"]["reason"] == "any_failed"
    assert result["result"]["all_terminal"] is False
    assert result["delivery"] == "sent"
    restored = monitor.WatchStore(store.root)
    monitor.JobMonitor(restored, engine.get_cluster, engine.get_host).tick()
    assert len(sent) == 1
    assert restored.read(watch_id, host="h")["result"] == result["result"]


def test_restart_completes_saved_active_watch(setup_monitor):
    host, store, engine, snapshots, calls = setup_monitor
    watch_id = start(host, store, ["1", "2"], condition="all_terminal")
    snapshots.update({"1": state("FAILED"), "2": state("RUNNING")})
    engine.tick()
    assert store.active()
    ready(store)
    snapshots["2"] = state("COMPLETED")
    restored = monitor.WatchStore(store.root)
    monitor.JobMonitor(restored, engine.get_cluster, engine.get_host).tick()
    assert restored.read(watch_id, host="h")["result"]["all_terminal"]
    assert not restored.active()


def test_auth_gap_recovers_without_false_completion(setup_monitor, monkeypatch):
    host, store, engine, snapshots, calls = setup_monitor
    watch_id = start(host, store, ["1"])
    original = monitor.collect
    from remoteslurm.errors import AuthRequired

    def fail(*args):
        raise AuthRequired("reconnect manually")

    monkeypatch.setattr(monitor, "collect", fail)
    engine.tick()
    result = store.read(watch_id, host="h")
    assert result["status"] == "active" and result["result"] is None
    assert "AuthRequired" in result["error"]
    monkeypatch.setattr(monitor, "collect", original)
    snapshots["1"] = state("COMPLETED")
    ready(store)
    engine.tick()
    assert store.read(watch_id, host="h")["status"] == "finished"


def test_changed_target_is_not_queried(setup_monitor):
    host, store, engine, snapshots, calls = setup_monitor
    watch_id = start(host, store, ["1"])
    host.ssh = "different-cluster"
    engine.tick()
    assert not calls
    assert "target changed" in store.read(watch_id, host="h")["error"]


def test_stop_racing_observation_wins(setup_monitor, monkeypatch):
    host, store, engine, snapshots, calls = setup_monitor
    watch_id = start(host, store, ["1"])

    def collect(c, ids):
        store.stop(watch_id, host="h")
        return {"1": state("COMPLETED")}

    monkeypatch.setattr(monitor, "collect", collect)
    engine.tick()
    assert store.read(watch_id, host="h")["status"] == "stopped"


def test_array_accounting_gap_does_not_finish(setup_monitor):
    host, store, engine, snapshots, calls = setup_monitor
    watch_id = start(host, store, ["1"])
    snapshots["1"] = dict(state("RUNNING"), seen_tasks=["0", "1"])
    engine.tick()
    ready(store)
    snapshots["1"] = dict(state("COMPLETED"), seen_tasks=["0"])
    engine.tick()
    result = store.read(watch_id, host="h")
    assert result["status"] == "active" and result["counts"] == {"UNKNOWN": 1}


def test_array_gap_still_reports_observed_failure(setup_monitor):
    host, store, engine, snapshots, calls = setup_monitor
    watch_id = start(host, store, ["1"])
    snapshots["1"] = dict(state("RUNNING"), seen_tasks=["0", "1"])
    engine.tick()
    ready(store)
    snapshots["1"] = dict(state("FAILED"), seen_tasks=["0"])
    engine.tick()
    result = store.read(watch_id, host="h")
    assert result["result"]["reason"] == "any_failed"
    assert not result["result"]["all_terminal"]


def test_cached_connection_target_mismatch_blocks_observation(setup_monitor):
    host, store, engine, snapshots, calls = setup_monitor
    watch_id = start(host, store, ["1"])
    engine.get_cluster = lambda name: SimpleNamespace(host=HostConfig(name="h", ssh="old"))
    engine.tick()
    assert not calls
    assert "cached connection target changed" in store.read(watch_id, host="h")["error"]


def test_observer_lock_spans_socket_overrides(setup_monitor, monkeypatch):
    host, store, first, snapshots, calls = setup_monitor
    monkeypatch.setattr(first, "run", lambda: first.stop_event.wait(5))
    second = monitor.JobMonitor(store, first.get_cluster, first.get_host)
    try:
        first.start()
        second.start()
        assert first.thread is not None and second.thread is None
    finally:
        first.close()
        second.close()


def test_collector_reads_scheduler_only(cluster):
    job = cluster.submit(script="#!/bin/sh\ntrue\n")
    calls = []
    original = cluster.call

    def call(op, **kwargs):
        assert op in {"squeue", "sacct"}
        assert kwargs["jobs"] == [job.job_id]
        calls.append(op)
        return original(op, **kwargs)

    cluster.call = call
    assert monitor.collect(cluster, [job.job_id])[job.job_id]["state"] != "UNKNOWN"
    assert calls == ["squeue", "sacct"]


def test_local_read_has_no_destructive_cursor(setup_monitor):
    host, store, engine, snapshots, calls = setup_monitor
    watch_id = start(host, store, ["1"])
    snapshots["1"] = state("COMPLETED")
    engine.tick()
    first = store.read(watch_id, host="h")
    second = store.read(watch_id, host="h")
    assert first == second and len(json.dumps(first)) < 8192
    with store.connect() as db:
        db.execute(
            "INSERT OR REPLACE INTO health VALUES (1, ?)",
            (json.dumps({"heartbeat": time.time() - 100}),),
        )
    assert store.read(watch_id, host="h")["observer"]["stale"]
