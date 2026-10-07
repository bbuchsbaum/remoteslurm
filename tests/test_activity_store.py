import json
import sqlite3
from concurrent.futures import ProcessPoolExecutor

import pytest

from remoteslurm.activity.model import Subject
from remoteslurm.activity.store import ActivityStore
from remoteslurm.errors import InvalidArgument, RegistryUnavailable, StoreConflict, TooLarge


def append(root, key):
    return ActivityStore(root).append(
        subject=Subject("target", "site"),
        kind="test.observed",
        payload={"value": key},
        source="fixture",
        source_key=key,
    )


def test_concurrent_replay_and_independent_readers(tmp_path):
    store = ActivityStore(tmp_path)
    cursor = store.query()["next_cursor"]
    with ProcessPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(append, [tmp_path] * 16, [str(i % 8) for i in range(16)]))
    assert len({event["event_id"] for event in results}) == 8
    a = store.query(after=cursor, limit=3)
    b = store.query(after=cursor, limit=3)
    assert a["events"] == b["events"] and a["has_more"]
    rest = store.query(after=a["next_cursor"])
    assert len(a["events"] + rest["events"]) == 8
    assert len({e["seq"] for e in a["events"] + rest["events"]}) == 8
    assert not store.query(after=rest["next_cursor"])["events"]


def test_event_checkpoint_projection_rollback(tmp_path):
    store = ActivityStore(tmp_path)
    with store.connection() as db:
        db.execute(
            "CREATE TRIGGER fail BEFORE INSERT ON documents WHEN NEW.namespace='checkpoint' "
            "BEGIN SELECT RAISE(ABORT, 'injected disk failure'); END"
        )
    with pytest.raises(RegistryUnavailable):
        store.append(
            subject=Subject("target", "site"),
            kind="job.accepted",
            payload={},
            source="fixture",
            checkpoint=("ledger", {"page": 1}),
            documents=(("job", "42", {"state": "RUNNING"}),),
        )
    assert store.query()["events"] == []
    assert store.get("job", "42") is None
    assert store.get("checkpoint", "ledger") is None


def test_replay_keeps_original_observation_and_does_not_revert_projection(tmp_path):
    store = ActivityStore(tmp_path)
    kwargs = dict(
        subject=Subject("t", "h"),
        kind="job.observed",
        payload={"state": "RUNNING"},
        source="fixture",
        source_key="captured-record",
    )
    first = store.append(**kwargs, now=100, documents=(("job", "1", {"state": "RUNNING"}),))
    store.save("job", "1", {"state": "COMPLETED"})
    replay = store.append(
        **kwargs,
        now=101,
        documents=(("job", "1", {"state": "RUNNING"}),),
        checkpoint=("source", {"page": 2}),
    )
    assert replay == first
    assert store.get("job", "1")["state"] == "COMPLETED"
    assert store.get("checkpoint", "source") == {"page": 2}
    with pytest.raises(StoreConflict):
        store.append(**{**kwargs, "payload": {"state": "FAILED"}})


def test_retention_gap_keeps_current_documents_and_monotonic_sequence(tmp_path):
    store = ActivityStore(tmp_path, max_age=10)
    start = store.query()["next_cursor"]
    first = store.append(
        subject=Subject("t", "h"),
        kind="one",
        payload={},
        source="test",
        now=1,
        documents=(("job", "1", {"state": "RUNNING"}),),
    )
    store.prune(now=20)
    assert store.query(after=start)["resync_required"]
    assert store.get("job", "1") == {"state": "RUNNING"}
    next_event = store.append(subject=Subject("t", "h"), kind="two", payload={}, source="test")
    assert next_event["seq"] > first["seq"]


def test_schema_reopen_future_version_and_private_file(tmp_path):
    append(tmp_path, "one")
    store = ActivityStore(tmp_path)
    assert len(store.query()["events"]) == 1
    assert store.path.stat().st_mode & 0o077 == 0
    with store.connection() as db:
        db.execute("PRAGMA user_version=999")
    with pytest.raises(RegistryUnavailable, match="schema"):
        ActivityStore(tmp_path)


def test_abandoned_transaction_is_rolled_back(tmp_path):
    store = ActivityStore(tmp_path)
    with pytest.raises(KeyboardInterrupt), store.transaction() as db:
        store.put(db, "checkpoint", "one", {"page": 3})
        raise KeyboardInterrupt
    assert store.get("checkpoint", "one") is None
    assert sqlite3.connect(store.path).execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_cursor_scope_tampering_bounds_and_byte_budget(tmp_path):
    store = ActivityStore(tmp_path)
    cursor = store.query(host="site")["next_cursor"]
    with pytest.raises(InvalidArgument):
        store.query(after=cursor, host="different")
    with pytest.raises(InvalidArgument):
        store.query(after="broken")
    with pytest.raises(InvalidArgument):
        store.query(limit=0)
    for _ in range(4):
        store.append(
            subject=Subject("t", "site"), kind="large", payload={"x": "a" * 35000}, source="test"
        )
    page = store.query(after=cursor, host="site", max_bytes=70000)
    assert len(json.dumps(page).encode()) <= 70000
    assert len(page["events"]) == 1 and page["has_more"]
    with pytest.raises(TooLarge):
        store.append(
            subject=Subject("t", "site"), kind="huge", payload={"x": "a" * 65536}, source="test"
        )


def test_equal_states_at_distinct_times_are_not_globally_deduplicated(tmp_path):
    store = ActivityStore(tmp_path)
    for state in ["RUNNING", "PENDING", "RUNNING"]:
        store.append(
            subject=Subject("t", "h"),
            kind="scheduler.observed",
            payload={"state": state},
            source="test",
        )
    assert [e["payload"]["state"] for e in store.query()["events"]] == [
        "RUNNING",
        "PENDING",
        "RUNNING",
    ]


def test_byte_retention_and_document_budget_are_reported(tmp_path, monkeypatch):
    store = ActivityStore(tmp_path, max_bytes=65536)
    cursor = store.query()["next_cursor"]
    for _ in range(4):
        store.append(
            subject=Subject("t", "h"), kind="large", source="test", payload={"value": "a" * 30000}
        )
    assert store.health()["event_bytes"] <= 65536
    assert store.query(after=cursor)["history_gap"]
    monkeypatch.setattr("remoteslurm.activity.store.MAX_DOCUMENT_STORAGE", 100)
    store.save("retained", "a", {"data": "a" * 50})
    with pytest.raises(TooLarge, match="storage budget"):
        store.save("retained", "b", {"data": "b" * 50})
    assert store.get("retained", "a") == {"data": "a" * 50}
    assert store.get("retained", "b") is None


def test_snapshot_and_event_boundary_are_read_together(tmp_path):
    store = ActivityStore(tmp_path)
    store.append(
        subject=Subject("t", "h"),
        kind="changed",
        payload={},
        source="test",
        documents=(("view", "one", {"state": "first"}),),
    )
    first = store.query(snapshot=("view", "one"))
    store.append(
        subject=Subject("t", "h"),
        kind="changed",
        payload={},
        source="test",
        documents=(("view", "one", {"state": "second"}),),
    )
    second = store.query(after=first["next_cursor"], snapshot=("view", "one"))
    assert first["snapshot"] == {"state": "first"}
    assert second["snapshot"] == {"state": "second"} and len(second["events"]) == 1
    store.save("view", "one", {"data": "a" * 120000})
    omitted = store.query(snapshot=("view", "one"))
    assert omitted["snapshot_omitted"] and omitted["snapshot"] is None
    assert len(json.dumps(omitted).encode()) < 128 * 1024
