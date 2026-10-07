import asyncio
import copy
import json
from argparse import Namespace

from remoteslurm import cli, server
from remoteslurm.activity.recovery import import_group
from remoteslurm.activity.semantic import cache_snapshot
from remoteslurm.activity.store import ActivityStore


def cached(cluster, sandbox):
    snapshot = {
        "campaign": "study",
        "run_id": "r1",
        "store": {"transaction_id": "commit-1", "revision": 1},
        "workspace": {"remote_root": str(sandbox)},
        "stage_specs": {
            "fit": {
                "script": "fit.sh",
                "resources": {"cpus_per_task": 4},
                "script_content": "PRIVATE BODY",
            }
        },
        "units": [
            {
                "unit_id": "u1",
                "stage": "fit",
                "keys": {"subject": "001"},
                "contract": {
                    "outputs": [{"name": "result", "path": str(sandbox / "result"), "kind": "file"}]
                },
                "execution": {"state": "UNBOUND"},
                "dependency_requirements": [{"stage": "prepare"}],
            }
        ],
    }
    store = ActivityStore()
    with store.transaction() as db:
        for ns, key, value in cache_snapshot(store, cluster, snapshot):
            store.put(db, ns, key, value)
    record = {
        "group_id": "g",
        "current_attempt": "a",
        "contract": {
            "mode": "array",
            "stage": "fit",
            "mapping": [{"unit_id": "u1", "index": 2}],
            "cwd": str(sandbox),
        },
        "attempts": [
            {
                "attempt_id": "a",
                "job_id": "12",
                "state": "ACCEPTED",
                "created_at": 1,
                "accepted_at": 2,
            }
        ],
    }
    import_group(store, cluster, "study", "r1", record)
    return store, snapshot


def test_work_cards_are_exact_at_event_and_same_for_python_cli_mcp(
    cluster, sandbox, monkeypatch, capsys
):
    store, snapshot = cached(cluster, sandbox)
    monkeypatch.setattr(
        server,
        "_get_cluster",
        lambda *args: (_ for _ in ()).throw(AssertionError("reader connected")),
    )
    python = store.query()
    mcp = asyncio.run(server.activity())
    assert (
        cli.cmd_activity(
            Namespace(host=None, after=None, since=None, limit=100, follow=False, json=True)
        )
        == 0
    )
    command = json.loads(capsys.readouterr().out)
    assert python["events"] == mcp["events"] == command["events"]
    event = python["events"][0]
    card = event["work_card"]
    assert event["context"]["availability"] == "attached"
    assert card["semantic_ref"]["campaign_commit"] == "commit-1"
    assert card["attempt_members"][0] == {
        "attempt_id": "a",
        "group_id": "g",
        "job_id": "12",
        "array_task_id": "2",
        "packed_member": None,
    }
    assert card["resources"] == {"cpus_per_task": 4} and card["command_label"] == "fit.sh"
    assert "PRIVATE BODY" not in str(event)
    changed = copy.deepcopy(snapshot)
    changed["store"] = {"transaction_id": "commit-2", "revision": 2}
    changed["units"][0]["keys"] = {"subject": "renamed"}
    with store.transaction() as db:
        for ns, key, value in cache_snapshot(store, cluster, changed):
            store.put(db, ns, key, value)
    assert store.query()["events"][0]["work_card"]["label"] == {"subject": "001"}


def test_missing_semantic_context_and_bounded_attachments_are_explicit(cluster, sandbox):
    store, snapshot = cached(cluster, sandbox)
    with store.transaction() as db:
        db.execute("DELETE FROM documents WHERE namespace='semantic'")
    event = store.query()["events"][0]
    assert event["work_card"] is None and event["context"]["availability"] == "unavailable"
    assert event["subject"]["semantic_ref"]["campaign_commit"] == "commit-1"
    page = store.query(max_bytes=68 * 1024)
    assert len(json.dumps(page).encode()) <= 68 * 1024


def test_local_monitor_mcp_has_no_cluster_dependency(monkeypatch):
    monkeypatch.setattr(
        server,
        "_get_cluster",
        lambda *args: (_ for _ in ()).throw(AssertionError("health connected")),
    )
    result = asyncio.run(server.monitor_status())
    assert result["enabled"] and result["policy"]["max_calls"] == 8
