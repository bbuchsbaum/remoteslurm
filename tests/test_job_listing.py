import json

import pytest

from remoteslurm.job_listing import page
from remoteslurm.slurm import JobStatus


def test_large_array_does_not_consume_listing_budget():
    job = JobStatus(
        "1",
        "RUNNING",
        "squeue",
        extra={
            "array": True,
            "n_tasks": 100000,
            "tasks": {"RUNNING": 100000},
            "task_states": {str(i): "RUNNING" for i in range(100000)},
            "failed_tasks": list(range(10000)),
        },
    )
    result = page([job], max_bytes=2048)
    assert len(json.dumps(result).encode()) <= 2048
    assert result["jobs"][0]["extra"]["failed_tasks_count"] == 10000
    assert len(result["jobs"][0]["extra"]["failed_tasks_sample"]) == 10
    result = page([job], compact=False, max_bytes=2048)
    assert result["jobs"][0]["detail_omitted"]
    assert result["count"] == 1 and not result["has_more"]


def test_byte_pagination_makes_progress_without_losing_rows():
    jobs = [JobStatus(str(i), "PENDING", "squeue", name="🦕" * 1000) for i in range(200)]
    offset, ids = 0, []
    while True:
        result = page(jobs, offset=offset, max_bytes=2048)
        assert result["count"] > 0
        assert len(json.dumps(result).encode()) <= 2048
        ids.extend(row["job_id"] for row in result["jobs"])
        if not result["has_more"]:
            break
        assert result["next_offset"] > offset
        offset = result["next_offset"]
    assert ids == [str(i) for i in range(200)]


def test_fields_and_validation():
    st = JobStatus("1", "RUNNING", "squeue", stdout_path="/tmp/log")
    assert page([st], fields=["stdout_path"])["jobs"] == [
        {"job_id": "1", "state": "RUNNING", "stdout_path": "/tmp/log"}
    ]
    from remoteslurm.errors import InvalidArgument

    with pytest.raises(InvalidArgument):
        page([st], fields=["typo"])


def test_unicode_registry_error_preserves_budget_and_progress():
    result = page(
        [JobStatus("1", "RUNNING", "squeue", name="🦕" * 512)],
        max_bytes=2048,
        registry_error="🦕" * 512,
    )
    assert len(json.dumps(result).encode()) <= 2048
    assert result["count"] == 1 and result["next_offset"] is None
    assert result["registry_error_truncated"]


def test_listing_filters_before_detailed_lookup(cluster, monkeypatch):
    first = cluster.submit(script="#!/bin/sh\ntrue\n", name="cohort-a")
    cluster.submit(script="#!/bin/sh\ntrue\n", name="unrelated")
    calls = []
    original = cluster.job_status

    def status(jid, **kwargs):
        calls.append(jid)
        return original(jid, **kwargs)

    monkeypatch.setattr(cluster, "job_status", status)
    result = cluster.jobs_page(name="cohort-*", since="2000-01-01", refresh=True)
    assert [row["job_id"] for row in result["jobs"]] == [first.job_id]
    assert all(jid == first.job_id for jid in calls)
    assert cluster.jobs_page(since="2100-01-01")["count"] == 0
