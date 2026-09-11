"""Bounded login-node `run`, detached runs (`run(detach=True)` + proc_*), and `wait_for`."""

from __future__ import annotations

import json
import os
import shlex
import signal
import socket
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from remoteslurm.cluster import Cluster
from remoteslurm.errors import InvalidArgument, NotFound, PermissionDenied, RemoteTimeout

PY = sys.executable


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # pragma: no cover - exists, not ours
        return True
    return True


def _wait_text(p: Path, timeout: float = 10.0) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if p.exists() and p.read_text().strip():
            return p.read_text().strip()
        time.sleep(0.05)
    raise AssertionError(f"{p} never got content")


def _wait_dead(pid: int, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _alive(pid):
            return True
        time.sleep(0.05)
    return False


def _kill(pid: int, group: bool = False) -> None:
    try:
        (os.killpg if group else os.kill)(pid, signal.SIGKILL)
    except OSError:
        pass


# --------------------------------------------------------------------------- bounded run
@pytest.mark.parametrize("stream", [False, True])
def test_run_returns_soon_after_exit_when_background_child_holds_pipe(
    cluster: Cluster, sandbox: Path, stream: bool
) -> None:
    marker = sandbox / f"bg-{stream}.pid"
    t0 = time.time()
    r = cluster.run(f"echo hi; sleep 30 & echo $! > {marker}", timeout=60, stream=stream)
    elapsed = time.time() - t0
    child = int(_wait_text(marker))
    try:
        assert r["rc"] == 0 and r["stdout"] == "hi\n"
        assert r["lingering"] is True and "detach" in r["note"]
        assert Path(r["lingering_log"]).parent == sandbox / ".cache" / "remoteslurm" / "procs"
        assert elapsed < 15, f"run was held open {elapsed:.1f}s by a background child"
        assert _alive(child)  # left running; only its output is gone
    finally:
        _kill(child)


@pytest.mark.parametrize("stream", [False, True])
def test_run_timeout_is_bounded_when_setsid_child_holds_pipe(
    cluster: Cluster, sandbox: Path, stream: bool
) -> None:
    """A setsid child survives the timeout's group kill but must not pin the worker after it."""
    marker = sandbox / f"setsid-{stream}.pid"
    code = (
        "import os, time; os.setsid(); "
        f"open({str(marker)!r}, 'w').write(str(os.getpid())); time.sleep(60)"
    )
    cmd = f"echo before; {shlex.quote(PY)} -c {shlex.quote(code)} & sleep 30"
    t0 = time.time()
    with pytest.raises(RemoteTimeout) as ei:
        cluster.run(cmd, timeout=1, stream=stream)
    elapsed = time.time() - t0
    child = int(_wait_text(marker))
    try:
        # The stub's own timeout (1 s + at most 2 s of draining), not the client's (16 s).
        assert elapsed < 10, f"timed-out run took {elapsed:.1f}s"
        assert ei.value.details.get("stdout") == "before\n"
        assert _alive(child)  # outside the killed group
        assert cluster.run(["echo", "ok"])["stdout"] == "ok\n"
    finally:
        _kill(child)


def test_run_large_stdin_with_capped_output(cluster: Cluster) -> None:
    data = "x" * (1 << 20)  # bigger than a pipe buffer in both directions
    r = cluster.run(["cat"], stdin=data, max_output=1000, timeout=30)
    assert r["rc"] == 0 and r["stdout_truncated"] and len(r["stdout"]) == 1000
    r = cluster.run(["true"], stdin=data, timeout=30)  # never reads stdin: EPIPE is not an error
    assert r["rc"] == 0 and "lingering" not in r


@pytest.mark.parametrize("stream", [False, True])
def test_lingering_child_keeps_running_and_its_output_goes_to_a_log(
    cluster: Cluster, sandbox: Path, stream: bool
) -> None:
    marker = sandbox / f"survived-{stream}"
    # The child must outlast the 2 s drain grace, or the run simply waits for it.
    r = cluster.run(
        f"echo hi; (sleep 3; echo late; echo survived > {marker}) &", timeout=30, stream=stream
    )
    assert r["lingering"] is True and r["stdout"] == "hi\n"
    # It wrote to stdout after the call returned and still reached its next command.
    assert _wait_text(marker) == "survived"
    w = cluster.wait_for(path=r["lingering_log"], pattern="^late$", timeout=10)
    assert w["met"] and w["line"] == "late"


def test_lingering_drain_is_capped(make_cluster: Callable[..., Cluster], sandbox: Path) -> None:
    c = make_cluster({"REMOTESLURM_LINGER_MAX_BYTES": "1000"})
    marker = sandbox / "flood.pid"
    r = c.run(f"sh -c 'echo $$ > {marker}; exec yes' &", timeout=30)
    flood = int(_wait_text(marker))
    try:
        assert r["lingering"] is True
        assert _wait_dead(flood), "the capped drain should close the pipe (SIGPIPE)"
        data = Path(r["lingering_log"]).read_bytes()
        assert data.endswith(b"cap reached; stopped reading]\n")
        assert b"y\ny\n" in data and len(data) < 1100
    finally:
        _kill(flood)


def test_run_exiting_inside_the_grace_before_its_deadline_is_not_a_timeout(
    cluster: Cluster, sandbox: Path
) -> None:
    marker = sandbox / "late.pid"
    # exits at ~1.5 s; its 2 s drain grace outlasts the 3 s deadline
    r = cluster.run(f"sleep 1.5; echo done; sleep 30 & echo $! > {marker}", timeout=3)
    child = int(_wait_text(marker))
    try:
        assert r["rc"] == 0 and r["stdout"] == "done\n" and r["lingering"] is True
    finally:
        _kill(child)


# --------------------------------------------------------------------------- detached runs
def test_detach_returns_at_once_and_records_exit(cluster: Cluster, sandbox: Path) -> None:
    t0 = time.time()
    r = cluster.run("echo started; sleep 2; echo finished; exit 3", detach=True)
    assert time.time() - t0 < 5
    pid = r["pid"]
    assert r["pgid"] == pid and r["host"] == socket.gethostname()
    assert Path(r["log"]).parent == sandbox / ".cache" / "remoteslurm" / "procs"
    assert cluster.proc_status(pid)["state"] == "running"
    w = cluster.wait_for(pid=pid, timeout=20)
    assert w["done"] and w["met"] and w["reason"] == "exited"
    assert w["process"]["state"] == "exited" and w["process"]["rc"] == 3
    assert cluster.proc_tail(pid)["content"] == "started\nfinished\n"
    listed = cluster.proc_status()
    assert listed["procs"][0]["pid"] == pid and listed["total"] >= 1


def test_detach_argv_env_cwd_and_explicit_log(cluster: Cluster, sandbox: Path) -> None:
    code = "import os; print(os.getcwd()); print(os.environ['RS_X'])"
    r = cluster.run(
        [PY, "-c", code], detach=True, cwd="~/proj", env={"RS_X": "42"}, log="~/logs/job.log"
    )
    log = sandbox / "logs" / "job.log"
    assert r["log"] == str(log)
    assert cluster.wait_for(pid=r["pid"], timeout=20)["process"]["rc"] == 0
    cwd, value = log.read_text().splitlines()
    assert Path(cwd).resolve() == (sandbox / "proj").resolve() and value == "42"


def test_detach_rejections(cluster: Cluster) -> None:
    with pytest.raises(NotFound):
        cluster.run(["definitely-not-a-command-xyz"], detach=True)
    with pytest.raises(InvalidArgument):
        cluster.run("echo", detach=True, compute=True)
    with pytest.raises(InvalidArgument):
        cluster.run("cat", detach=True, stdin="x")
    with pytest.raises(NotFound):
        cluster.proc_status(99999999)
    with pytest.raises(NotFound):
        cluster.proc_kill(99999999)
    with pytest.raises(InvalidArgument):
        cluster.wait_for()
    with pytest.raises(InvalidArgument):
        cluster.wait_for(path="~/proj/a.txt", pattern="(")


def test_detach_respects_safe_run_policy(cluster: Cluster) -> None:
    cluster.host.allow_run = "safe"
    try:
        with pytest.raises(PermissionDenied):
            cluster.run("sleep 1", detach=True)
    finally:
        cluster.host.allow_run = True


def test_proc_kill_term_stops_the_whole_group(cluster: Cluster, sandbox: Path) -> None:
    marker = sandbox / "grandchild.pid"
    r = cluster.run(f"sleep 300 & echo $! > {marker}; sleep 300", detach=True)
    grandchild = int(_wait_text(marker))
    try:
        k = cluster.proc_kill(r["pid"], grace=5)
        assert k["killed"] and k["signals"] == ["TERM"]
        assert k["state"] == "exited" and k["rc"] == 143
        assert _wait_dead(grandchild)
        again = cluster.proc_kill(r["pid"])
        assert again["killed"] is False and again["reason"] == "not running"
    finally:
        _kill(r["pid"], group=True)


def test_proc_kill_escalates_to_sigkill(cluster: Cluster, sandbox: Path) -> None:
    marker = sandbox / "ignoring.ready"
    r = cluster.run(f"trap '' TERM; echo ready > {marker}; sleep 300", detach=True)
    _wait_text(marker)  # the TERM-ignoring disposition is in place
    try:
        k = cluster.proc_kill(r["pid"], grace=1)
        assert k["killed"] and k["signals"] == ["TERM", "KILL"]
        assert k["state"] == "exited" and k["rc"] == 137  # recorded by the stub's reaper
    finally:
        _kill(r["pid"], group=True)


def test_detached_run_survives_the_stub(make_cluster: Callable[..., Cluster]) -> None:
    c1 = make_cluster()
    r = c1.run("sleep 300", detach=True)
    pid = r["pid"]
    c1.session.close()  # the stub (and, remotely, the ssh channel) goes away
    c2 = make_cluster()
    try:
        assert c2.proc_status(pid)["state"] == "running"
        k = c2.proc_kill(pid, grace=5)
        # No reaper survived the first stub: this status comes from the bash wrapper's trap.
        assert k["state"] == "exited" and k["rc"] == 143
    finally:
        _kill(pid, group=True)


def test_record_from_another_login_node(cluster: Cluster, sandbox: Path) -> None:
    procs = sandbox / ".cache" / "remoteslurm" / "procs"
    procs.mkdir(parents=True, exist_ok=True)
    rec = {
        "pid": 4242,
        "pgid": 4242,
        "host": "login-elsewhere",
        "cmd": "sleep 1",
        "log": str(procs / "x.log"),
        "rc_path": str(procs / "x.rc"),
        "started": time.time(),
    }
    (procs / "login-elsewhere.4242.json").write_text(json.dumps(rec))
    st = cluster.proc_status(4242)
    assert st["state"] == "unknown" and "login-elsewhere" in st["note"]
    with pytest.raises(InvalidArgument):
        cluster.proc_kill(4242)
    with pytest.raises(InvalidArgument):
        cluster.wait_for(pid=4242, timeout=1)  # no exit to observe from here, and no pattern
    (procs / "x.log").write_text("")
    # Finishing is visible from any node through the shared home: a pattern wait on its log
    # notices the rc file appearing instead of running out its timeout.
    timer = threading.Timer(0.5, lambda: (procs / "x.rc").write_text("0\n"))
    timer.start()
    try:
        w = cluster.wait_for(pid=4242, pattern="never printed", timeout=10)
    finally:
        timer.join()
    assert w["done"] and w["reason"] == "exited" and w["waited"] < 5
    assert cluster.proc_status(4242)["state"] == "exited"


# --------------------------------------------------------------------------- wait_for
def test_wait_for_pattern_in_detached_log(cluster: Cluster) -> None:
    r = cluster.run("echo setting up; sleep 0.5; echo 'SETUP DONE'; sleep 300", detach=True)
    try:
        w = cluster.wait_for(pid=r["pid"], pattern=r"SETUP\s+DONE", timeout=20)
        assert w["done"] and w["met"] and w["reason"] == "matched"
        assert w["line"] == "SETUP DONE" and w["process"]["state"] == "running"
    finally:
        cluster.proc_kill(r["pid"], grace=1)


def test_wait_for_pattern_ends_when_the_process_exits_first(cluster: Cluster) -> None:
    r = cluster.run("echo failed early; exit 2", detach=True)
    w = cluster.wait_for(pid=r["pid"], pattern="SETUP DONE", timeout=20)
    assert w["done"] and not w["met"] and w["reason"] == "exited"
    assert w["process"]["rc"] == 2


def test_wait_for_timeout_then_resume_from_offset(cluster: Cluster, sandbox: Path) -> None:
    log = sandbox / "progress.log"
    log.write_text("step 1\nstep 2\n")
    w = cluster.wait_for(path="~/progress.log", pattern="^done$", timeout=1)
    assert not w["done"] and w["reason"] == "timeout" and w["offset"] == len("step 1\nstep 2\n")
    with open(log, "a") as f:
        f.write("done\n")
    w2 = cluster.wait_for(path="~/progress.log", pattern="^done$", offset=w["offset"], timeout=10)
    assert w2["met"] and w2["line"] == "done" and w2["line_offset"] == w["offset"]
    # resuming after the match does not report the same line again
    w3 = cluster.wait_for(path="~/progress.log", pattern="^done$", offset=w2["offset"], timeout=1)
    assert not w3["done"]


def test_wait_for_path_to_appear_and_a_line_without_newline(
    cluster: Cluster, sandbox: Path
) -> None:
    target = sandbox / "later.txt"
    timer = threading.Timer(0.5, lambda: target.write_text("READY"))
    timer.start()
    try:
        w = cluster.wait_for(path="~/later.txt", timeout=10)
        assert w["done"] and w["met"] and w["reason"] == "exists"
    finally:
        timer.join()
    w = cluster.wait_for(path="~/later.txt", pattern="READY", timeout=5)
    assert w["met"] and w["line"] == "READY" and w["line_offset"] == 0


def test_wait_for_partial_line_matches_only_once_settled(cluster: Cluster, sandbox: Path) -> None:
    log = sandbox / "progress.txt"
    log.write_text("do")

    def finish_line() -> None:
        with open(log, "a") as f:
            f.write("ne\n")

    # "do" is half of "done": `^do$` must not fire on it before the rest of the line arrives
    timer = threading.Timer(0.2, finish_line)
    timer.start()
    try:
        w = cluster.wait_for(path="~/progress.txt", pattern="^do$", timeout=2)
    finally:
        timer.join()
    assert not w["done"] and w["reason"] == "timeout"


def test_wait_for_ignores_old_content_in_a_reused_log(cluster: Cluster, sandbox: Path) -> None:
    (sandbox / "server.log").write_text("old run\nREADY\n")
    r = cluster.run("sleep 1; echo READY; sleep 300", detach=True, log="~/server.log")
    try:
        w = cluster.wait_for(pid=r["pid"], pattern="^READY$", timeout=20)
        assert w["met"] and w["line_offset"] == len("old run\nREADY\n") and w["waited"] >= 0.5
    finally:
        cluster.proc_kill(r["pid"], grace=1)


def test_wait_for_scans_to_eof_after_a_large_burst_before_exit(cluster: Cluster) -> None:
    # 40 MB then the marker: more than a few 8 MiB per-check scans get through before the exit
    r = cluster.run("head -c 40000000 /dev/zero | tr '\\0' x; echo; echo DONE", detach=True)
    w = cluster.wait_for(pid=r["pid"], pattern="^DONE$", timeout=60)
    assert w["met"] and w["reason"] == "matched" and w["line"] == "DONE"


# --------------------------------------------------------------------------- cancellation
def test_cancel_of_a_queued_run_stops_it_when_it_starts(cluster: Cluster) -> None:
    from remoteslurm import stub as stub_mod

    s = cluster.session
    busy = [
        s.submit("run", {"argv": ["sleep", "2"]}) for _ in range(stub_mod.SLOW_WORKERS)
    ]  # fill the slow pool so the next run has to queue
    rid = s.new_request_id()
    queued = s.submit("run", {"argv": ["sleep", "60"], "timeout": 120}, request_id=rid)
    time.sleep(0.3)
    c = s.cancel(rid)
    assert c["cancelled"] is True and c.get("queued") is True  # not started: remembered
    r = queued.result(timeout=30)  # would take 60 s if the cancel had been lost
    assert r.get("cancelled") is True
    for f in busy:
        f.result(timeout=30)


def test_cancel_landing_while_the_request_is_sent_is_resent(
    cluster: Cluster, sandbox: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from remoteslurm.session import CURRENT_INFLIGHT, InFlight, Session

    tracker = InFlight()
    real_submit = Session.submit

    def submit_as_caller_gives_up(self, op, args=None, request_id=None):  # type: ignore[no-untyped-def]
        # The caller aborts while the run is being sent, and its cancel reaches nothing (e.g.
        # during a respawn). Other requests (the staleness ping, the cancel) pass untouched.
        if op == "run":
            tracker.cancelled.set()
        return real_submit(self, op, args, request_id)

    monkeypatch.setattr(Session, "submit", submit_as_caller_gives_up)
    token = CURRENT_INFLIGHT.set(tracker)
    try:
        t0 = time.time()
        r = cluster.run("exec sleep 60", timeout=120)
    finally:
        CURRENT_INFLIGHT.reset(token)
    assert r.get("cancelled") is True and time.time() - t0 < 10


def test_a_cancelled_call_still_records_its_submitted_job(cluster: Cluster) -> None:
    from remoteslurm.session import CURRENT_INFLIGHT, InFlight

    tracker = InFlight()
    tracker.cancel()  # the MCP call was abandoned; bookkeeping must still complete
    token = CURRENT_INFLIGHT.set(tracker)
    try:
        job = cluster.submit("#!/bin/bash\necho hi\n", name="cancelled-caller")
    finally:
        CURRENT_INFLIGHT.reset(token)
    assert cluster.registry.get(job.job_id) is not None


def test_old_lingering_logs_are_pruned(cluster: Cluster, sandbox: Path) -> None:
    procs = sandbox / ".cache" / "remoteslurm" / "procs"
    procs.mkdir(parents=True, exist_ok=True)
    old = procs / "lingering-20200101-000000-abcdef.log"
    old.write_text("stale\n")
    week_ago = time.time() - 8 * 86400
    os.utime(old, (week_ago, week_ago))
    marker = sandbox / "prune.pid"
    r = cluster.run(f"sleep 30 & echo $! > {marker}", timeout=30)
    try:
        assert r["lingering"] is True and Path(r["lingering_log"]).exists()
        assert not old.exists()
    finally:
        _kill(int(_wait_text(marker)))
