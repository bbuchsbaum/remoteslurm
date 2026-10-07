import asyncio

import pytest

from remoteslurm.activity.producer import CLIENT, OPERATION
from remoteslurm.activity.store import ActivityStore
from remoteslurm.errors import InvalidArgument


def test_submit_records_acknowledgement_before_enrichment(cluster, sandbox, monkeypatch):
    def lost(*args, **kwargs):
        from remoteslurm.errors import SessionDied

        raise SessionDied("lost optional enrichment")

    monkeypatch.setattr(cluster, "scontrol_job", lost)
    job = cluster.submit(script="#!/bin/sh\necho fixture", cwd=str(sandbox / "proj"))
    events = ActivityStore().query()["events"]
    accepted = [e for e in events if e["kind"] == "submission.accepted"]
    assert len(accepted) == 1 and accepted[0]["subject"]["job_id"] == job.job_id
    assert accepted[0]["seq"] < next(e["seq"] for e in events if e["kind"] == "job.registered")
    assert len(ActivityStore().documents("work")) == 1
    assert ActivityStore().documents("work")[0]["workdir"] == str(sandbox / "proj")
    assert accepted[0]["evidence_refs"][0]["authority"] == "scheduler"
    assert all("#!/bin/sh" not in str(e) for e in events)
    assert OPERATION.get() is None


def test_recording_failure_does_not_reject_accepted_submission(cluster, monkeypatch):
    def broken(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr("remoteslurm.activity.producer.ActivityStore", broken)
    job = cluster.submit(script="#!/bin/sh\ntrue")
    assert job.job_id and job.submission()["submitted"]
    assert job.submission()["activity_recorded"] is False


def test_mcp_and_python_file_operations_share_records(cluster, sandbox, monkeypatch):
    from remoteslurm import server

    monkeypatch.setattr(server, "_get_cluster", lambda host: cluster)
    p = str(sandbox / "proj" / "observed.txt")
    cluster.write(p, "private fixture body")
    result = asyncio.run(server.write(path=p, content="second private body"))
    assert "error" not in result
    events = [e for e in ActivityStore().query()["events"] if e["kind"] == "file.write.returned"]
    assert [e["actor"] for e in events] == ["python", "mcp"]
    assert "private fixture body" not in str(events) and "second private body" not in str(events)
    assert CLIENT.get() == "python"


def test_preflight_failure_is_not_submission_acceptance(cluster):
    with pytest.raises(InvalidArgument):
        cluster.submit()
    assert not any(e["kind"] == "submission.accepted" for e in ActivityStore().query()["events"])


def test_rsync_partial_exit_is_one_correlated_operation(cluster, monkeypatch):
    from argparse import Namespace
    from types import SimpleNamespace

    from remoteslurm import cli, sync
    from remoteslurm.transport import SSHTransport

    monkeypatch.setattr(cluster, "transport", SSHTransport(alias="fixture", mfa=False))
    monkeypatch.setattr(sync, "find_rsync", lambda: ("/fixture/rsync", "3.2"))
    monkeypatch.setattr(sync, "expand_remote", lambda c, p: p)
    monkeypatch.setattr(cli.subprocess, "run", lambda argv: SimpleNamespace(returncode=23))
    assert (
        cli._rsync(cluster, "/local/src", "/remote/dest", to_remote=True, args=Namespace(json=True))
        == 23
    )
    events = ActivityStore().query()["events"]
    assert [e["kind"] for e in events] == ["transfer.rsync.requested", "transfer.rsync.returned"]
    assert events[0]["operation_id"] == events[1]["operation_id"]
    assert events[1]["payload"]["transfer_outcome"] == "partial_or_unknown"
    assert events[1]["payload"]["rc"] == 23


@pytest.mark.parametrize("actor", ["python", "mcp"])
def test_campaign_acceptance_precedes_projection_and_keeps_root_correlation(
    cluster, sandbox, monkeypatch, actor
):
    from remoteslurm import server
    from test_activity_recovery import definition

    cluster.campaigns.start(definition(sandbox), run_id="r1")
    monkeypatch.setattr(server, "_get_cluster", lambda host: cluster)
    if actor == "mcp":
        result = asyncio.run(server.campaign_apply("recovery", run_id="r1"))
        assert "error" not in result
    else:
        cluster.campaigns.apply("recovery", run_id="r1")
    events = ActivityStore().query(limit=1000)["events"]
    requested = next(e for e in events if e["kind"] == "campaign.apply.requested")
    accepted = [e for e in events if e["kind"] == "submission.recorded"]
    merged = next(
        e
        for e in events
        if e["kind"] == "campaign.changed" and "attempt_accepted" in e["payload"]["event_kinds"]
    )
    assert len(accepted) == 3 and max(e["seq"] for e in accepted) < merged["seq"]
    assert all(
        e["correlation_id"] == requested["operation_id"] and e["actor"] == actor
        for e in [*accepted, merged]
    )
    assert all(e["subject"]["attempt_id"] and e["work_card"] for e in accepted)
    refresh = next(e for e in events if e["kind"] == "campaign.refresh.requested")
    assert refresh["causation_id"] == requested["operation_id"]
    assert refresh["correlation_id"] == requested["operation_id"]
