"""Shared state + tiny deterministic scheduler for the FakeSlurm shims.

State lives in a JSON file at ``$FAKESLURM_STATE`` (default ``$HOME/.fakeslurm.json``)::

    {"next_id": 1000, "tick": 0, "jobs": {"1000": {...}, ...}}

Time is a ``tick`` counter advanced by every ``squeue``/``sacct``/``scontrol`` invocation
(unless ``FAKESLURM_FREEZE=1``). A job sits in PENDING for ``PENDING_TICKS`` ticks after
submission and in RUNNING for ``RUNNING_TICKS`` more, then becomes COMPLETED (or FAILED when
the script contains ``FAKESLURM_FAIL``). On completion ``"hello\\ndone\\n"`` is written to the
job's stdout path.

Each query renders its output from the *current* state and advances the clock afterwards.
``PENDING_TICKS`` is 2 rather than 1 because ``Cluster.submit`` itself performs one
``scontrol show job`` lookup right after ``sbatch``; a client therefore still observes
PENDING on its first explicit status query, then RUNNING, then COMPLETED.

Environment knobs:

* ``FAKESLURM_FREEZE=1``           - never advance the clock / job states.
* ``FAKESLURM_ACCOUNTING_LAG=N``   - ``sacct`` omits jobs that finished < N ticks ago.
* ``FAKESLURM_SCONTROL_TTL=N``     - ``scontrol`` forgets finished jobs after N ticks (default 5).
"""

import fcntl
import getpass
import json
import os
import sys
import time
from contextlib import contextmanager

PENDING_TICKS = 2
RUNNING_TICKS = 2
DEFAULT_SCONTROL_TTL = 5

INVALID_JOB = "slurm_load_jobs error: Invalid job id specified"


def state_path():
    return os.environ.get("FAKESLURM_STATE") or os.path.join(
        os.path.expanduser("~"), ".fakeslurm.json"
    )


def user():
    try:
        return getpass.getuser()
    except Exception:  # pragma: no cover
        return "nobody"


def ts(t=None):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(t if t is not None else time.time()))


def hms(seconds):
    seconds = int(max(0, seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def mss(seconds):
    """squeue-style elapsed ``M:SS`` (``H:MM:SS`` past an hour)."""
    seconds = int(max(0, seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def _empty():
    return {"next_id": 1000, "tick": 0, "jobs": {}}


@contextmanager
def locked_state():
    """Load the state file under an exclusive lock; save it on normal exit."""
    path = state_path()
    lock_path = path + ".lock"
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(lock_path, "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            if os.path.exists(path):
                with open(path, encoding="utf-8") as f:
                    state = json.load(f)
            else:
                state = _empty()
            try:
                yield state
            finally:
                # save even when the shim exits via sys.exit() after an error
                tmp = path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(state, f, indent=1, sort_keys=True)
                os.replace(tmp, path)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def elapsed_seconds(job):
    if not job.get("start_time"):
        return 0
    end = job.get("end_time") or time.time()
    return end - job["start_time"]


def _write(path, text):
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
    except (OSError, TypeError):
        pass


def _finish(job, state_name, exit_code, tick, stdout_text=None, stderr_text=None):
    job["state"] = state_name
    job["state_raw"] = state_name
    job["exit_code"] = exit_code
    job["end_time"] = time.time()
    job["end_tick"] = tick
    if stdout_text is None:
        stdout_text = "hello\ndone\n"
    if job.get("stdout"):
        _write(job["stdout"], stdout_text)
    if stderr_text is not None and job.get("stderr"):
        _write(job["stderr"], stderr_text)


def _apply_outcome(job, tick):
    """Resolve a RUNNING job to a terminal state, honouring FAKESLURM_OUTCOME.

    ``FAKESLURM_OUTCOME`` (recorded on the job at submit time):
      * ``oom``       -> OUT_OF_MEMORY, batch-step MaxRSS >= ReqMem.
      * ``timeout``   -> TIMEOUT.
      * ``nodefail``  -> NODE_FAIL.
      * ``fail:TEXT`` -> FAILED, TEXT written to stderr (if a distinct stderr file exists)
                          else to stdout, so `diagnose` can match a signature.
    """
    outcome = job.get("outcome")
    if outcome == "oom":
        job["maxrss_batch"] = "1100M"
        _finish(job, "OUT_OF_MEMORY", "0:125", tick, stdout_text="hello\nallocating memory...\n")
    elif outcome == "timeout":
        _finish(job, "TIMEOUT", "0:15", tick, stdout_text="hello\nstill working...\n")
    elif outcome == "nodefail":
        _finish(job, "NODE_FAIL", "0:0", tick, stdout_text="hello\n")
    elif isinstance(outcome, str) and outcome.startswith("fail:"):
        text = outcome[len("fail:") :] or "error: something went wrong"
        if not text.endswith("\n"):
            text += "\n"
        if job.get("stderr"):
            _finish(
                job,
                "FAILED",
                "1:0",
                tick,
                stdout_text="job crashed; see stderr\n",
                stderr_text=text,
            )
        else:
            _finish(job, "FAILED", "1:0", tick, stdout_text=text)
    elif job.get("fail"):
        _finish(job, "FAILED", "1:0", tick)
    else:
        _finish(job, "COMPLETED", "0:0", tick)


def advance(state):
    """One scheduler tick (no-op when frozen)."""
    if os.environ.get("FAKESLURM_FREEZE") == "1":
        return
    state["tick"] += 1
    tick = state["tick"]
    for job in state["jobs"].values():
        if job["state"] == "PENDING" and tick - job["submit_tick"] >= PENDING_TICKS:
            job["state"] = job["state_raw"] = "RUNNING"
            job["start_time"] = time.time()
            job["start_tick"] = tick
            job["nodelist"] = "fake0001"
        if job["state"] == "RUNNING" and tick - job["start_tick"] >= RUNNING_TICKS:
            _apply_outcome(job, tick)


def is_terminal(job):
    return job["state"] not in ("PENDING", "RUNNING")


def accounting_visible(job, state):
    if not is_terminal(job):
        return True
    lag = int(os.environ.get("FAKESLURM_ACCOUNTING_LAG") or 0)
    return state["tick"] - job.get("end_tick", state["tick"]) >= lag


def scontrol_visible(job, state):
    if not is_terminal(job):
        return True
    ttl = int(os.environ.get("FAKESLURM_SCONTROL_TTL") or DEFAULT_SCONTROL_TTL)
    return state["tick"] - job.get("end_tick", state["tick"]) < ttl


def lookup_ids(arg):
    """``-j 1000,1001_2`` -> base ids as strings."""
    return [a.strip().split("_")[0].split(".")[0] for a in arg.split(",") if a.strip()]


def die(msg, rc=1):
    sys.stderr.write(msg + "\n")
    sys.exit(rc)
