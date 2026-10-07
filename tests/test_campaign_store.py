from __future__ import annotations

import pytest

from remoteslurm.campaigns.store import CampaignStore
from remoteslurm.errors import RemoteSlurmError, StoreConflict


def _put_run(store: CampaignStore, name: str = "study", run_id: str = "run-1") -> None:
    store.cluster.call(
        "campaign_put_immutable",
        campaign_dir=store.campaign_dir,
        name=name,
        kind="run",
        object_id=run_id,
        value={"schema": 1, "campaign": name, "run_id": run_id},
    )


def test_campaign_store_commit_is_bounded_and_idempotent(cluster) -> None:
    store = CampaignStore(cluster)
    _put_run(store)
    snapshot = {
        "campaign": "study",
        "run_id": "run-1",
        "lifecycle": "OPEN",
        "units": [{"unit_id": f"u-{index}"} for index in range(700)],
    }
    documents = store.snapshot_documents(snapshot)
    first = store.commit(
        "study",
        "run-1",
        expected_revision=0,
        transaction_id="tx-1",
        events=[{"type": "run_started"}],
        documents=documents,
    )
    second = store.commit(
        "study",
        "run-1",
        expected_revision=0,
        transaction_id="tx-1",
        events=[{"type": "run_started"}],
        documents=documents,
    )
    assert first["revision"] == 1
    assert second["revision"] == 1
    assert second["idempotent"] is True
    page = store.read_snapshot("study", "run-1", include_units=True, offset=490, limit=25)
    assert page["store"]["revision"] == 1
    assert [unit["unit_id"] for unit in page["units"]] == [f"u-{i}" for i in range(490, 515)]


def test_campaign_store_compare_and_swap_conflict(cluster) -> None:
    store = CampaignStore(cluster)
    _put_run(store)
    store.commit(
        "study",
        "run-1",
        expected_revision=0,
        transaction_id="tx-1",
        events=[],
        documents={"summary.json": {"revision": 1}},
    )
    with pytest.raises(StoreConflict):
        store.commit(
            "study",
            "run-1",
            expected_revision=0,
            transaction_id="tx-2",
            events=[],
            documents={"summary.json": {"revision": 2}},
        )


def test_snapshot_pages_stay_on_original_commit_during_concurrent_update(cluster, monkeypatch):
    store = CampaignStore(cluster)
    _put_run(store)
    first = {"campaign": "study", "run_id": "run-1", "units": [{"unit_id": "old"}]}
    second = {**first, "units": [{"unit_id": "new"}]}
    store.commit(
        "study",
        "run-1",
        expected_revision=0,
        transaction_id="old",
        events=[],
        documents=store.snapshot_documents(first),
    )
    real_read = store.read_document

    def read(name, run, document="summary.json", **kwargs):
        result = real_read(name, run, document, **kwargs)
        if document == "summary.json":
            store.commit(
                "study",
                "run-1",
                expected_revision=1,
                transaction_id="new",
                events=[],
                documents=store.snapshot_documents(second),
            )
        return result

    monkeypatch.setattr(store, "read_document", read)
    result = store.read_complete_snapshot("study", "run-1")
    assert result["store"]["transaction_id"] == "old"
    assert result["units"] == [{"unit_id": "old"}]


def test_forward_changes_bounded_and_legacy_cursor_unchanged(cluster):
    store = CampaignStore(cluster)
    _put_run(store)
    for i in range(10):
        store.commit(
            "study",
            "run-1",
            expected_revision=i,
            transaction_id=f"tx-{i}",
            events=[{"type": f"event-{i}"}],
            documents={"summary.json": {"i": i}},
        )
    base = dict(campaign_dir=store.campaign_dir, name="study", run_id="run-1")
    page = cluster.call("campaign_changes", **base)
    assert [c["revision"] for c in page["changes"]] == list(range(1, 9))
    assert page["has_more"] and page["coverage"] == "complete"
    tail = cluster.call("campaign_changes", **base, after=page["next_after"])
    assert [c["revision"] for c in tail["changes"]] == [9, 10]
    assert not tail["has_more"]
    assert store.events("study", "run-1", limit=1)["events"][0]["type"] == "event-9"
    gap = cluster.call("campaign_changes", **base, after="missing")
    assert gap["coverage"] == "gap"


def test_absent_canonical_ledger_read_creates_nothing(cluster, sandbox):
    store = CampaignStore(cluster)
    _put_run(store)
    path = sandbox / ".remoteslurm/campaigns/study/runs/run-1/execution"
    result = cluster.call(
        "campaign_ledger_read", campaign_dir=store.campaign_dir, name="study", run_id="run-1"
    )
    assert not result["available"] and result["groups"] == []
    assert not path.exists()


def test_campaign_events_follow_only_committed_head_history(cluster) -> None:
    store = CampaignStore(cluster)
    _put_run(store)
    store.commit(
        "study",
        "run-1",
        expected_revision=0,
        transaction_id="tx-1",
        events=[{"type": "one"}, {"type": "two"}],
        documents={"summary.json": {"revision": 1}},
    )
    store.commit(
        "study",
        "run-1",
        expected_revision=1,
        transaction_id="tx-2",
        events=[{"type": "three"}],
        documents={"summary.json": {"revision": 2}},
    )
    first = store.events("study", "run-1", limit=2)
    assert [event["type"] for event in first["events"]] == ["three", "one"]
    assert first["next_cursor"]
    second = store.events("study", "run-1", cursor=first["next_cursor"], limit=2)
    assert [event["type"] for event in second["events"]] == ["two"]
    assert second["next_cursor"] is None


def test_immutable_campaign_records_are_exact(cluster) -> None:
    store = CampaignStore(cluster)
    _put_run(store)
    _put_run(store)
    with pytest.raises(StoreConflict):
        store.cluster.call(
            "campaign_put_immutable",
            campaign_dir=store.campaign_dir,
            name="study",
            kind="run",
            object_id="run-1",
            value={"schema": 1, "campaign": "study", "run_id": "changed"},
        )


def test_incomplete_transaction_is_quarantined_before_replay(make_cluster) -> None:
    failing = make_cluster({"REMOTESLURM_TEST_CAMPAIGN_FAIL_STEP": "staged"})
    store = CampaignStore(failing)
    _put_run(store)
    with pytest.raises(RemoteSlurmError, match="injected campaign failure"):
        store.commit(
            "study",
            "run-1",
            expected_revision=0,
            transaction_id="tx-recover",
            events=[],
            documents={"summary.json": {"ok": True}},
        )
    failing.close()

    recovered_cluster = make_cluster()
    recovered = CampaignStore(recovered_cluster).commit(
        "study",
        "run-1",
        expected_revision=0,
        transaction_id="tx-recover",
        events=[],
        documents={"summary.json": {"ok": True}},
    )
    assert recovered["revision"] == 1
    orphaned = recovered_cluster.ls(
        "~/.remoteslurm/campaigns/study/runs/run-1/orphaned", hidden=True
    )
    assert orphaned["entries"]


def test_idempotency_result_recovers_from_reachable_older_revision(make_cluster) -> None:
    failing = make_cluster({"REMOTESLURM_TEST_CAMPAIGN_FAIL_STEP": "head"})
    store = CampaignStore(failing)
    _put_run(store)
    with pytest.raises(RemoteSlurmError, match="after HEAD publication"):
        store.commit(
            "study",
            "run-1",
            expected_revision=0,
            transaction_id="tx-first",
            events=[{"type": "first"}],
            documents={"summary.json": {"revision": 1}},
        )
    failing.close()

    cluster = make_cluster()
    store = CampaignStore(cluster)
    store.commit(
        "study",
        "run-1",
        expected_revision=1,
        transaction_id="tx-second",
        events=[{"type": "second"}],
        documents={"summary.json": {"revision": 2}},
    )
    recovered = store.commit(
        "study",
        "run-1",
        expected_revision=0,
        transaction_id="tx-first",
        events=[{"type": "first"}],
        documents={"summary.json": {"revision": 1}},
    )
    assert recovered["revision"] == 1
    assert recovered["idempotent"] is True
    assert store.read_document("study", "run-1")["head"]["revision"] == 2
