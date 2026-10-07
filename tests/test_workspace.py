import time

import pytest

from remoteslurm.activity.store import ActivityStore
from remoteslurm.config import Config
from remoteslurm.errors import InvalidArgument
from remoteslurm.job_monitor import shared_collect
from remoteslurm.workspace import service
from remoteslurm.workspace.observer import WorkspaceObserver


def opened(cluster, sandbox):
    config = Config(hosts={"local": cluster.host}, default_host="local")
    workspace = service.select(config, "local", path=str(sandbox / "proj"), cluster=cluster)
    return service.subscribe(workspace), config


def test_offline_root_lookup_ignores_only_trailing_separators(cluster, sandbox, monkeypatch):
    config = Config(hosts={"local": cluster.host}, default_host="local")
    source = str(sandbox / "proj")
    opened = service.select(config, "local", path=source + "/", cluster=cluster)
    monkeypatch.setattr(cluster, "call", lambda *a, **kw: pytest.fail("offline lookup connected"))
    assert service.select(config, "local", path=source)["workspace_id"] == opened["workspace_id"]
    assert (
        service.select(config, "local", path=source + "///")["workspace_id"]
        == opened["workspace_id"]
    )
    # Do not normalize .. across unknown remote symlinks or invent an expansion offline.
    from remoteslurm.errors import NotFound

    with pytest.raises(NotFound):
        service.select(config, "local", path=source + "/../proj")


def test_snapshot_does_not_connect_and_shares_two_independent_readers(
    cluster, sandbox, monkeypatch
):
    workspace, config = opened(cluster, sandbox)
    job = cluster.submit(script="#!/bin/sh\ntrue", cwd=str(sandbox / "proj"))
    observer = WorkspaceObserver(lambda _: cluster, lambda _: cluster.host)
    observer.tick()

    def forbidden(*a, **k):
        pytest.fail("snapshot attempted remote access")

    monkeypatch.setattr(cluster, "call", forbidden)
    selected = service.select(config, "local", path=str(sandbox / "proj"))
    a, b = service.snapshot(selected), service.snapshot(selected)
    assert a["events"] == b["events"]
    assert a["work"][0]["job_id"] == job.job_id
    assert a["work"][0]["validation"] == "NOT_RUN"
    assert a["monitor"]["fresh"]
    ActivityStore().save("observer", "health", {"heartbeat": time.time() - 120})
    assert not service.snapshot(selected)["monitor"]["fresh"]


def test_file_requests_coalesce_and_pagination_is_cache_only(cluster, sandbox, monkeypatch):
    workspace, _ = opened(cluster, sandbox)
    a = service.inspect_request(workspace, "list")
    b = service.inspect_request(workspace, "list")
    assert a["request_id"] == b["request_id"]
    observer = WorkspaceObserver(lambda _: cluster, lambda _: cluster.host)
    observer.inspect(a)
    done = service.inspect_request(workspace, "list", request_id=a["request_id"])
    assert done["state"] == "DONE"
    names = {e["name"] for e in done["result"]["entries"]}
    assert {"a.txt", "b.log", "sub"} <= names
    monkeypatch.setattr(cluster, "call", lambda *a, **k: pytest.fail("page queried remote"))
    assert service.inspect_request(workspace, "list", token=done["result"]["token"])["entries"]
    with pytest.raises(InvalidArgument, match="expired"):
        service.inspect_request(workspace, "list", token="old-capture")


def test_stop_race_does_not_resurrect_subscription(cluster, sandbox, monkeypatch):
    workspace, _ = opened(cluster, sandbox)
    cluster.submit(script="#!/bin/sh\ntrue", cwd=str(sandbox / "proj"))
    from remoteslurm import job_monitor

    def collect(c, ids):
        service.subscribe(workspace, False)
        return {jid: {"state": "RUNNING"} for jid in ids}

    monkeypatch.setattr(job_monitor, "shared_collect", collect)
    observer = WorkspaceObserver(lambda _: cluster, lambda _: cluster.host)
    observer.observe(workspace)
    assert not ActivityStore().get("workspace", workspace["workspace_id"])["enabled"]
    with pytest.raises(InvalidArgument, match="disabled"):
        service.inspect_request(workspace, "preview", "a.txt")


def test_multiple_observers_share_scheduler_sample(cluster, sandbox, monkeypatch):
    workspace, _ = opened(cluster, sandbox)
    job = cluster.submit(script="#!/bin/sh\ntrue", cwd=str(sandbox / "proj"))
    from remoteslurm import job_monitor

    calls = []

    def collect(c, ids):
        calls.append(ids)
        return {jid: {"state": "RUNNING"} for jid in ids}

    monkeypatch.setattr(job_monitor, "collect", collect)
    shared_collect(cluster, [job.job_id], force=True)  # Existing watch owner.
    observer = WorkspaceObserver(lambda _: cluster, lambda _: cluster.host)
    observer.observe(workspace)
    observer.observe(workspace)
    assert calls == [[job.job_id]]


def test_changed_target_suspends_without_querying(cluster, sandbox, monkeypatch):
    workspace, _ = opened(cluster, sandbox)
    cluster.host.ssh = "another-host"
    observer = WorkspaceObserver(
        lambda _: pytest.fail("queried changed target"), lambda _: cluster.host
    )
    observer.observe(workspace)
    result = service.snapshot(workspace)
    assert result["monitor"]["state"] == "SUSPENDED" and not result["monitor"]["fresh"]


def test_monitor_stop_expires_freshness_immediately(cluster, sandbox):
    workspace, _ = opened(cluster, sandbox)
    WorkspaceObserver(lambda _: cluster, lambda _: cluster.host).tick()
    assert service.snapshot(workspace)["monitor"]["fresh"]
    service.subscribe(workspace, False)
    assert not service.snapshot(workspace)["monitor"]["fresh"]


def test_missing_array_rows_stay_unknown_and_terminal_rows_retire(cluster, sandbox, monkeypatch):
    from remoteslurm import job_monitor

    workspace, _ = opened(cluster, sandbox)
    job = cluster.submit(script="#!/bin/sh\ntrue", cwd=str(sandbox / "proj"))
    observations = [
        {"state": "RUNNING", "terminal": False, "seen_tasks": ["0", "1"]},
        {"state": "COMPLETED", "terminal": True, "seen_tasks": ["0"]},
        {"state": "COMPLETED", "terminal": True, "seen_tasks": ["0", "1"]},
    ]
    monkeypatch.setattr(
        job_monitor, "shared_collect", lambda c, ids: {job.job_id: observations.pop(0)}
    )
    observer = WorkspaceObserver(lambda _: cluster, lambda _: cluster.host)
    observer.observe(workspace)
    observer.observe(workspace)
    assert service.snapshot(workspace)["work"][0]["execution"] == "UNKNOWN"
    observer.observe(workspace)
    observed_at = service.snapshot(workspace)["work"][0]["observed_at"]
    observer.observe(workspace)  # No fourth scheduler sample is available or needed.
    result = service.snapshot(workspace)
    assert result["work"][0]["execution"] == "COMPLETED"
    assert result["work"][0]["observed_at"] == observed_at
    assert result["work"][0]["validation"] == "NOT_RUN"
    assert result["attention"]["kind"] != "complete"


def test_replaced_root_cannot_inherit_old_work(cluster, sandbox):
    workspace, config = opened(cluster, sandbox)
    cluster.submit(script="#!/bin/sh\ntrue", cwd=str(sandbox / "proj"))
    (sandbox / "proj").rename(sandbox / "original")
    (sandbox / "proj").mkdir()
    observer = WorkspaceObserver(lambda _: cluster, lambda _: cluster.host)
    observer.observe(workspace)
    assert service.snapshot(workspace)["monitor"]["state"] == "SUSPENDED"
    new = service.select(config, "local", path=str(sandbox / "proj"), cluster=cluster)
    assert new["workspace_id"] != workspace["workspace_id"]
    observer.observe(new)
    assert service.snapshot(new)["work"] == []


def test_replaced_root_file_cache_is_isolated(cluster, sandbox):
    from remoteslurm.errors import NotFound

    workspace, config = opened(cluster, sandbox)
    request = service.inspect_request(workspace, "preview", "a.txt")
    WorkspaceObserver(lambda _: cluster, lambda _: cluster.host).inspect(request)
    (sandbox / "proj").rename(sandbox / "original")
    (sandbox / "proj").mkdir()
    new = service.select(config, "local", path=str(sandbox / "proj"), cluster=cluster)
    with pytest.raises(NotFound, match="no cached"):
        service.inspect_request(new, "preview", "a.txt", cached_only=True)


def test_mcp_workspace_is_local_and_readers_do_not_consume(cluster, sandbox, monkeypatch):
    import asyncio

    from remoteslurm import server

    workspace, config = opened(cluster, sandbox)
    cluster.write(str(sandbox / "proj/a.txt"), "updated")
    monkeypatch.setattr(Config, "load", lambda *a: config)
    monkeypatch.setattr(server, "_get_cluster", lambda *a: pytest.fail("MCP opened SSH"))
    a = asyncio.run(server.workspace(path=workspace["source_root"]))
    b = asyncio.run(server.workspace(path=workspace["source_root"]))
    assert a["events"] == b["events"] and a["next_cursor"] == b["next_cursor"]
    assert a["attention"] == b["attention"]
