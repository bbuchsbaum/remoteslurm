import time
from types import SimpleNamespace

import pytest

from remoteslurm.activity.monitor import (
    BudgetExhausted,
    CampaignObserver,
    ReadBudget,
    control,
    enroll,
    policy,
    status,
)
from remoteslurm.activity.store import ActivityStore
from remoteslurm.config import HostConfig
from remoteslurm.errors import AuthRequired, InvalidArgument, RemoteSlurmError


def test_budget_persists_across_restart_and_refuses_mutations(monkeypatch):
    host = HostConfig(name="h", ssh="fixture", mfa=False)
    calls = []
    cluster = SimpleNamespace(
        host=host, call=lambda op, **kw: calls.append((op, kw)) or {"result": "x"}
    )
    store = ActivityStore()
    reader = ReadBudget(cluster, store)
    for _ in range(8):
        reader.call("squeue", _timeout=60)
    restored = ReadBudget(reader, ActivityStore())
    with pytest.raises(BudgetExhausted):
        restored.call("sacct")
    with pytest.raises(RemoteSlurmError, match="mutation"):
        restored.call("campaign_commit")
    assert len(calls) == 8 and all(kw["_timeout"] == 10 for _, kw in calls)
    assert status()["budgets"][0]["bytes"] > 0
    from remoteslurm.activity.producer import target_id

    budget = store.get("budget", target_id(host))
    budget["started"] -= 31
    store.save("budget", target_id(host), budget)
    restored.call("sacct")
    assert len(calls) == 9


def test_excerpt_budget_and_configurable_limits():
    host = HostConfig(name="h", ssh="fixture", mfa=False)
    reader = ReadBudget(SimpleNamespace(host=host, call=lambda *a, **k: {}), ActivityStore())
    for _ in range(3):
        reader.call("workspace_read")
    with pytest.raises(BudgetExhausted, match="excerpt"):
        reader.call("workspace_read")
    assert policy(scheduler_seconds=45, read_seconds=5)["scheduler_seconds"] == 45
    with pytest.raises(InvalidArgument):
        policy(max_calls=9)
    with pytest.raises(InvalidArgument):
        policy(scheduler_seconds=1)


def test_subscription_restart_disable_remove_and_auth_gap(monkeypatch):
    host = HostConfig(name="h", ssh="fixture", mfa=False)
    subscription = enroll(host, "study", "run-1")
    assert enroll(host, "study", "run-1")["id"] == subscription["id"]
    calls = []

    def disconnected(op, **kwargs):
        calls.append(op)
        raise AuthRequired("use foreground connect")

    reader = SimpleNamespace(host=host, call=disconnected)
    observer = CampaignObserver(lambda name: reader, lambda name: host)
    observer.tick()
    assert calls == ["campaign_read"]
    assert status()["sources"][0]["error"] == "auth_required"
    assert status()["sources"][0]["stale"]
    restored = CampaignObserver(lambda name: reader, lambda name: host)
    assert restored.active()
    control(False)
    restored.tick()
    assert len(calls) == 1 and not restored.active()
    control(True)
    enroll(host, "study", "run-1", enabled=False)
    assert not restored.active()


def test_collector_cadences_persist_independently(cluster, sandbox, monkeypatch):
    from remoteslurm.activity import collectors
    from remoteslurm.activity.recovery import Recovery
    from remoteslurm.activity.semantic import key

    subscription = enroll(cluster.host, "study", "run-1")
    observed = []
    monkeypatch.setattr(
        Recovery,
        "page",
        lambda *args: (
            observed.append("recovery") or {"coverage": "complete", "ledger_available": True}
        ),
    )
    monkeypatch.setattr(
        collectors,
        "sample",
        lambda store, c, sub, kind: observed.append(kind) or {"coverage": "complete"},
    )
    observer = CampaignObserver(lambda name: cluster, lambda name: cluster.host)
    for _ in range(4):
        observer.tick()
    assert observed == ["recovery", "scheduler", "artifacts", "usage"]
    observer.tick()
    assert len(observed) == 4
    sources = status()["sources"]
    assert {s["kind"]: s["cadence"] for s in sources} == {
        "recovery": 60,
        "scheduler": 30,
        "artifacts": 60,
        "usage": 60,
    }
    # Restore with the scheduler due alone, without replaying other sources.
    store = ActivityStore()
    skey = key(subscription["id"], "scheduler")
    source = store.get("source", skey)
    source["next_poll"] = time.time() - 1
    store.save("source", skey, source)
    subscription["next_poll"] = 0
    store.save("subscription", subscription["id"], subscription)
    CampaignObserver(lambda name: cluster, lambda name: cluster.host).tick()
    assert observed[-1] == "scheduler" and len(observed) == 5
