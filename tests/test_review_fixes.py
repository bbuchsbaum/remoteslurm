"""Regression tests for issues found in the v1 review."""

from __future__ import annotations

import asyncio
import io
import subprocess
import sys
from pathlib import Path

import pytest

from remoteslurm import slurm
from remoteslurm.cluster import Cluster
from remoteslurm.errors import SlurmError
from remoteslurm.jobs import JobRecord, JobRegistry


def test_registry_merges_across_instances(tmp_path: Path) -> None:
    a = JobRegistry("h", base=tmp_path)
    b = JobRegistry("h", base=tmp_path)
    a.put(JobRecord(job_id="1", name="a"))
    b.put(JobRecord(job_id="2", name="b"))
    a.update("1", last_state="RUNNING")
    ids = {r.job_id for r in JobRegistry("h", base=tmp_path).all()}
    assert ids == {"1", "2"}
    assert b.get("1").last_state == "RUNNING"  # type: ignore[union-attr]
    assert a.forget("2") and not b.get("2")


def test_registry_concurrent_processes(tmp_path: Path) -> None:
    code = (
        "import sys; from pathlib import Path\n"
        "from remoteslurm.jobs import JobRegistry, JobRecord\n"
        f"r = JobRegistry('h', base=Path({str(tmp_path)!r}))\n"
        "for i in range(20): r.put(JobRecord(job_id=f'{sys.argv[1]}_{i}'))\n"
    )
    procs = [subprocess.Popen([sys.executable, "-c", code, str(k)]) for k in range(4)]
    assert all(p.wait() == 0 for p in procs)
    assert len(JobRegistry("h", base=tmp_path).all()) == 80


def test_wait_raises_on_unknown_job(cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("remoteslurm.jobs.time.sleep", lambda s: None)
    with pytest.raises(SlurmError) as ei:
        cluster.wait("424242", poll=5)
    assert "not known" in ei.value.message


def test_tail_bytes_window_on_newline_boundary() -> None:
    from remoteslurm.stub import _tail_bytes

    data = b"aaaa\nbbbb\ncccc\n"
    f = io.BytesIO(data)
    out, skipped = _tail_bytes(f, len(data), 10, 10)
    assert out == b"bbbb\ncccc\n" and skipped == 5
    f = io.BytesIO(b"x\nlast-no-newline")
    out, skipped = _tail_bytes(f, 17, 1, 100)
    assert out == b"last-no-newline" and skipped == 2
    f = io.BytesIO(data)
    out, skipped = _tail_bytes(f, len(data), 2, 1000)
    assert out == b"bbbb\ncccc\n" and skipped == 5


def test_mcp_cancel_reports_skipped(cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    from remoteslurm import server

    monkeypatch.setattr(server, "_get_cluster", lambda host=None: cluster)
    monkeypatch.setattr(
        Cluster, "cancel", lambda self, ids: {"cancelled": [], "skipped": list(ids), "rc": 0}
    )
    r = asyncio.run(server.cancel("999"))
    assert r["cancelled"] == [] and r["skipped"] == ["999"]


def test_sbatch_default_cwd_is_home(cluster: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    job = cluster.submit("#!/bin/bash\necho hi\n", name="cwdtest")
    rec = cluster.registry.get(job.job_id)
    assert rec is not None and rec.workdir == cluster.home


def test_session_timeout_pops_future_and_checks_master() -> None:
    from remoteslurm.errors import NotConnected, RemoteTimeout
    from remoteslurm.session import Session
    from remoteslurm.transport import LocalTransport

    s = Session(LocalTransport())
    s.start()
    try:
        with pytest.raises(RemoteTimeout):
            s.call("run", {"argv": ["sleep", "3"]}, timeout=0.5)
        assert not s._pending
        # now pretend the ssh master died: the next timeout must surface NotConnected
        s.transport.master_alive = lambda: False  # type: ignore[attr-defined]
        s.transport.alias = "fake"  # type: ignore[attr-defined]
        with pytest.raises(NotConnected) as ei:
            s.call("run", {"argv": ["sleep", "3"]}, timeout=0.5)
        assert "remoteslurm connect fake" in (ei.value.action or "")
    finally:
        s.close()


def test_exit_code_helpers_still_fine() -> None:
    assert slurm.is_terminal("CANCELLED by 1")


def test_edit_follows_symlink(cluster, sandbox):
    """Editing a symlinked path edits the target and keeps the link (review edit-1)."""
    import os

    real = sandbox / "real.txt"
    real.write_text("value = 1\n")
    link = sandbox / "link.txt"
    os.symlink(real, link)
    cluster.edit("~/link.txt", old="value = 1", new="value = 2")
    assert (sandbox / "link.txt").is_symlink()  # link preserved
    assert real.read_text() == "value = 2\n"  # target edited


def test_write_follows_symlink(cluster, sandbox):
    import os

    real = sandbox / "wr.txt"
    real.write_text("a\n")
    link = sandbox / "wr-link.txt"
    os.symlink(real, link)
    cluster.write("~/wr-link.txt", "b\n")
    assert (sandbox / "wr-link.txt").is_symlink()
    assert real.read_text() == "b\n"


def test_concurrent_edits_same_file_no_corruption(cluster, sandbox):
    """Two concurrent writes to one path must not interleave on a shared tmp name (edit-2)."""
    (sandbox / "conc.txt").write_text("x")
    futs = [
        cluster.session.submit("write", {"path": "~/conc.txt", "content": str(i) * 5000})
        for i in range(8)
    ]
    for f in futs:
        f.result(timeout=30)
    got = (sandbox / "conc.txt").read_text()
    # whichever writer won, the file is exactly one writer's content (no torn mix)
    assert len(set(got)) == 1 and len(got) == 5000


def test_expandpath_is_shell_free(cluster, sandbox):
    from remoteslurm.errors import InvalidArgument

    assert cluster.call("expandpath", path="$HOME/x")["path"] == f"{sandbox}/x"
    with pytest.raises(InvalidArgument):
        cluster.call("expandpath", path="$NOPE_XYZ/a")
