"""MCP (stdio) server exposing a remote Slurm login node to coding agents.

Run as ``remoteslurm-mcp``. Every tool takes an optional ``host`` (a name from
``~/.config/remoteslurm/config.toml``), falling back to the configured ``default_host`` or
``$REMOTESLURM_DEFAULT_HOST``. Tools never raise: errors come back as
``{"error": <code>, "message": ..., "action": ...}`` so an agent can act on ``action``
(typically: ask the user to run ``remoteslurm connect <host>`` in a terminal).
"""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import threading
import time
from collections.abc import Callable
from typing import Any

from mcp.server.fastmcp import FastMCP

from .cluster import Cluster
from .config import ENV_DEFAULT_HOST
from .errors import RemoteSlurmError
from .session import CURRENT_INFLIGHT, InFlight
from .transport import SSHTransport, format_duration

ENV_MAX_CHARS = "REMOTESLURM_MCP_MAX_CHARS"
# Hard ceiling on any single string field in a tool result (agents have token caps).
MAX_CHARS = int(os.environ.get(ENV_MAX_CHARS, "200000"))

ENV_MAX_CALL = "REMOTESLURM_MCP_MAX_CALL"
# Longest a single tool call may block. Clients abort long silent calls (Claude Code: 1,800 s),
# so a login-node `run` timeout, and a `compute=True` run's queue wait + walltime, stay below.
MAX_CALL_SECONDS = 1500  # default; $REMOTESLURM_MCP_MAX_CALL overrides it (read per call)

ENV_MCP_TOOLS = "REMOTESLURM_MCP_TOOLS"
# The default tool set: the workflow-critical tools an agent needs, nothing more. Set
# REMOTESLURM_MCP_TOOLS=all to also expose glob/diff/job_output/sinfo/projects/sweep/
# queue_info/quota/events. (`wait` is core — agents want a bounded wait — and so are the
# proc_* tools, which are the only handle on a `run(detach=True)` process.)
CORE_TOOLS = {
    "info",
    "ls",
    "read",
    "edit",
    "grep",
    "write",
    "run",
    "proc_status",
    "proc_tail",
    "proc_kill",
    "submit",
    "adopt",
    "ensure",
    "pack",
    "jobs",
    "diagnose",
    "sync",
    "cancel",
    "connection",
    "wait",
}


# -- helpers ---------------------------------------------------------------------------------
def _get_cluster(host: str | None) -> Cluster:
    """Return the (cached) connected cluster for ``host``. Tests monkeypatch this."""
    return Cluster.connect(host)


def _max_chars() -> int:
    return int(os.environ.get(ENV_MAX_CHARS, str(MAX_CHARS)))


def _submitted_status(job: Any) -> dict[str, Any]:
    """Best-effort initial status that preserves a scheduler-accepted result."""
    try:
        return job.status().to_dict()
    except RemoteSlurmError as e:
        return {"job_id": job.job_id, "state": "UNKNOWN", "status_error": e.to_dict()}


def _cap(result: dict[str, Any]) -> dict[str, Any]:
    """Truncate any string field longer than the cap (recursively); flag when it happened."""
    cap = _max_chars()
    hit = False

    def walk(v: Any) -> Any:
        nonlocal hit
        if isinstance(v, str):
            if len(v) > cap:
                hit = True
                return v[:cap]
            return v
        if isinstance(v, dict):
            return {k: walk(x) for k, x in v.items()}
        if isinstance(v, list):
            return [walk(x) for x in v]
        return v

    out = walk(result)
    if hit:
        out["truncated_by_server"] = True
        out["truncated_note"] = f"string fields cut to {cap} chars (${ENV_MAX_CHARS})"
    return dict(out)


def _clamp(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(v)))


def _max_call() -> int:
    try:
        return max(1, int(os.environ.get(ENV_MAX_CALL) or MAX_CALL_SECONDS))
    except ValueError:
        return MAX_CALL_SECONDS


async def _in_worker(work: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    """Run ``work`` in a worker thread, tracking the stub requests it makes.

    If the MCP call is cancelled (the client timed out or gave up), those requests are cancelled
    on the stub, so a login-node run, an srun holding an allocation or a wait doesn't outlive
    the call. The worker thread itself finishes as soon as the stub answers.
    """
    tracker = InFlight()

    def tracked() -> dict[str, Any]:
        CURRENT_INFLIGHT.set(tracker)  # in the thread's copy of the context
        return work()

    try:
        return await asyncio.to_thread(tracked)
    except asyncio.CancelledError:
        # No awaiting here: anyio cancellation is level-triggered, so cancel from a thread.
        threading.Thread(target=tracker.cancel, name="rs-mcp-cancel", daemon=True).start()
        raise


async def _guard(host: str | None, fn: Callable[[Cluster], dict[str, Any]]) -> dict[str, Any]:
    """Run ``fn(cluster)`` in a worker thread, converting errors to dicts."""

    def work() -> dict[str, Any]:
        return fn(_get_cluster(host))

    try:
        return _cap(await _in_worker(work))
    except RemoteSlurmError as e:
        return e.to_dict()
    except Exception as e:  # noqa: BLE001 - never let an exception cross the MCP boundary
        return {"error": "internal", "message": f"{type(e).__name__}: {e}"}


async def _guard_confirmable(
    host: str | None, fn: Callable[[Cluster], dict[str, Any]]
) -> dict[str, Any]:
    """Like :func:`_guard`, but turns a ``ConfirmationRequired`` into a plain
    ``{needs_confirmation: true, what: ...}`` reply (agents re-call with ``confirm=True``)."""
    from .errors import ConfirmationRequired

    def work() -> dict[str, Any]:
        return fn(_get_cluster(host))

    try:
        return _cap(await _in_worker(work))
    except ConfirmationRequired as e:
        return {"needs_confirmation": True, "what": e.what}
    except RemoteSlurmError as e:
        return e.to_dict()
    except Exception as e:  # noqa: BLE001
        return {"error": "internal", "message": f"{type(e).__name__}: {e}"}


def _ls_summary(r: dict[str, Any]) -> str:
    entries = r.get("entries", [])
    ndirs = sum(1 for e in entries if e.get("type") == "dir")
    nfiles = len(entries) - ndirs
    s = f"{r.get('path')}: {ndirs} dirs, {nfiles} files"
    if r.get("truncated"):
        s += f" (showing {len(entries)} of {r.get('total')}; next_token={r.get('next_token')})"
    return s


# -- filesystem tools ------------------------------------------------------------------------
async def ls(
    path: str = "~",
    limit: int = 200,
    token: str | None = None,
    hidden: bool = True,
    host: str | None = None,
) -> dict[str, Any]:
    """List a remote directory (or stat a single file).

    Returns ``entries`` (name/type/size/mtime/mode), ``total``, ``truncated`` and a
    ``next_token`` to pass back as ``token`` for the next page (``limit`` per page, max ~2000).
    ``~`` expands to the remote home. On ``error: not_connected`` call ``connection`` and
    have the user run ``remoteslurm connect <host>`` in a terminal.
    """

    def f(c: Cluster) -> dict[str, Any]:
        r = c.ls(path, limit=limit, token=token, hidden=hidden)
        r["summary"] = _ls_summary(r)
        return r

    return await _guard(host, f)


async def read(
    path: str,
    max_bytes: int = 65536,
    offset: int = 0,
    head: int | None = None,
    tail: int | None = None,
    host: str | None = None,
) -> dict[str, Any]:
    """Read a bounded slice of a remote file.

    Text files return ``content`` (plus ``truncated``/``size``); use ``offset``+``max_bytes``
    (clamped to 1..1,000,000) to page, or ``head``/``tail`` for the first/last N lines
    (``tail`` is the cheap way to peek at a log). Binary files return ``content_b64``.
    """
    mb = _clamp(max_bytes, 1, 1_000_000)

    def f(c: Cluster) -> dict[str, Any]:
        r = c.read(path, max_bytes=mb, offset=offset, head=head, tail=tail)
        if r.get("binary"):
            r["note"] = "binary file: content_b64 is base64-encoded bytes"
        return r

    return await _guard(host, f)


async def grep(
    pattern: str,
    path: str,
    glob: str | None = None,
    max_matches: int = 200,
    ignore_case: bool = False,
    context: int = 0,
    max_depth: int = 10,
    host: str | None = None,
) -> dict[str, Any]:
    """Search remote files for a regular expression (runs remotely; only matches come back).

    ``path`` may be a file or a directory (searched recursively to ``max_depth``); ``glob``
    filters file names (e.g. ``*.log``). Stops after ``max_matches`` (``truncated`` is set).
    ``context`` adds N lines around each match.
    """

    def f(c: Cluster) -> dict[str, Any]:
        return c.grep(
            pattern,
            path,
            glob=glob,
            max_matches=max_matches,
            ignore_case=ignore_case,
            context=context,
            max_depth=max_depth,
        )

    return await _guard(host, f)


async def glob(
    path: str,
    pattern: str = "*",
    limit: int = 500,
    type: str | None = None,
    max_depth: int = 10,
    host: str | None = None,
) -> dict[str, Any]:
    """Find files under a remote directory matching a glob (``**`` allowed), a.k.a. find.

    ``type`` may be ``"file"`` or ``"dir"``. At most ``limit`` results; ``truncated`` is set
    when more exist. Hidden entries are skipped.
    """

    def f(c: Cluster) -> dict[str, Any]:
        return c.glob(path, pattern, limit=limit, type=type, max_depth=max_depth)

    return await _guard(host, f)


async def write(
    path: str,
    content: str,
    append: bool = False,
    mode: int | None = None,
    force: bool = False,
    host: str | None = None,
) -> dict[str, Any]:
    """Write (or append) text to a remote file, creating parent directories as needed.

    ``mode`` is an octal permission as an int (e.g. 493 == 0o755). Returns path and bytes
    written. Use ``submit`` with ``script=`` instead of writing job scripts by hand. A
    configured protected path (``error: permission``) is refused unless ``force=True``.
    """

    def f(c: Cluster) -> dict[str, Any]:
        return c.write(path, content, append=append, mode=mode, force=force)

    return await _guard(host, f)


async def edit(
    path: str,
    old: str,
    new: str,
    expect: int = 1,
    all: bool = False,
    force: bool = False,
    host: str | None = None,
) -> dict[str, Any]:
    """Replace an exact string in a remote text file — use ``edit`` instead of read+write
    for small changes.

    The occurrence count of ``old`` must equal ``expect`` (default 1) unless ``all`` is
    true, which replaces every occurrence. On ``error: not_found`` check ``closest`` for
    near-matching lines; on too many matches the error lists their line numbers — make
    ``old`` more specific (include surrounding lines) or pass ``all=true``. Returns
    ``replacements``, ``first_line`` and a unified-diff ``preview``. Line endings and
    file mode are preserved; the replace is atomic.
    """

    def f(c: Cluster) -> dict[str, Any]:
        return c.edit(path, old, new, expect=expect, all=all, force=force)

    return await _guard(host, f)


async def diff(
    path: str,
    content: str,
    context: int = 3,
    max_lines: int = 500,
    host: str | None = None,
) -> dict[str, Any]:
    """Unified diff of a remote text file against ``content`` (what you expect it to say).

    Returns ``identical`` (bool), ``diff`` (unified diff text, ``context`` lines of
    context, at most ``max_lines`` lines — ``truncated`` flags the cut). Cheap way to
    check whether a remote file matches a local version before overwriting it.
    """

    def f(c: Cluster) -> dict[str, Any]:
        return c.diff(path, content, context=context, max_lines=max_lines)

    return await _guard(host, f)


# -- commands ---------------------------------------------------------------------------------
async def run(
    cmd: str | list[str],
    cwd: str | None = None,
    timeout: int = 60,
    login: bool = False,
    max_output: int = 65536,
    compute: bool = False,
    template: str | None = None,
    partition: str | None = None,
    time: str | None = None,
    cpus: int | None = None,
    mem: str | None = None,
    gpus: int | None = None,
    queue_timeout: int = 600,
    detach: bool = False,
    log: str | None = None,
    host: str | None = None,
) -> dict[str, Any]:
    """Run a shell command on the *login node* and return rc/stdout/stderr — or, with
    ``compute=True``, on a *compute node* via srun.

    Login-node runs cap output at ``max_output`` bytes/stream and clamp ``timeout`` to the
    per-call limit below;
    ``login=True`` loads modules/profile. Keep login-node work light — heavy or long work belongs
    in ``submit`` or ``compute=True``.

    ``detach=True`` starts the command in the background, detached from this connection, and
    returns at once with ``{pid, pgid, log, host}``; stdout+stderr go to ``log`` (default: a new
    file under ``~/.cache/remoteslurm/procs``). Use it for anything that must keep running after
    the call (a server, an install, a setup script), then ``proc_status``/``proc_tail``/
    ``proc_kill`` and ``wait(pid=..., pattern=...)``. Don't background with ``&`` in a normal
    run: it returns ~2 s after the command exits with ``lingering: true``, and the background
    process's further output goes to ``lingering_log`` only for as long as this session lasts.

    Calls stay under ~25 min (``$REMOTESLURM_MCP_MAX_CALL``, default 1500 s, at most 3600)
    because clients abort long silent calls; cancelling the call kills its remote process. A
    ``compute=True`` run
    whose worst case (``queue_timeout`` + walltime + 30 s) exceeds that is refused
    (``error: invalid_arg``) — pass a ``time`` and ``queue_timeout`` that fit, or use ``submit``.

    ``compute=True`` queues for a node and runs the command there. Resources come from
    ``template`` then ``partition``/``time``/``cpus``/``mem``/``gpus`` (account from the host
    default). ``queue_timeout`` bounds the wait for an allocation; the result is
    ``{started: true, rc, stdout, stderr, node, elapsed}`` when a node was granted, else
    ``{started: false, reason}``. ``cmd`` may be a shell string or an argv list. The argv form is
    required when the host uses ``allow_run = "safe"``. Returns ``error: permission`` when the
    configured run policy forbids the command.

    Live streaming of output (``run --stream``) and ``tail -f`` are CLI-only: this MCP ``run``
    tool always returns the complete result in one response.
    """
    limit = _max_call()
    to = _clamp(timeout, 1, min(3600, limit))

    def f(c: Cluster) -> dict[str, Any]:
        if detach:
            return c.run(cmd, cwd=cwd, login=login, detach=True, log=log, compute=compute)
        if compute:
            return c.run(
                cmd,
                compute=True,
                template=template,
                partition=partition,
                time=time,
                cpus=cpus,
                mem=mem,
                gpus=gpus,
                queue_timeout=queue_timeout,
                login=login,
                max_output=max_output,
                cwd=cwd,
                max_seconds=limit,
            )
        return c.run(cmd, cwd=cwd, timeout=to, login=login, max_output=max_output)

    return await _guard(host, f)


async def proc_status(pid: int | None = None, host: str | None = None) -> dict[str, Any]:
    """State of a process started with ``run(detach=True)``.

    ``state`` is ``running``, ``exited`` (with ``rc``), ``gone`` (killed with no exit status
    recorded) or ``unknown`` (started on another login node); also ``log``, ``cmd``, ``started``,
    ``elapsed``. Without ``pid``: the most recent detached runs, newest first. Never blocks —
    use ``wait(pid=...)`` to wait for the exit or for a line in its log.
    """

    def f(c: Cluster) -> dict[str, Any]:
        return c.proc_status(pid)

    return await _guard(host, f)


async def proc_tail(
    pid: int, lines: int = 50, max_bytes: int = 65536, host: str | None = None
) -> dict[str, Any]:
    """The last ``lines`` lines of a detached run's log (stdout+stderr) as ``content``, plus its
    current ``state``. ``max_bytes`` caps the read (1..1,000,000)."""
    mb = _clamp(max_bytes, 1, 1_000_000)

    def f(c: Cluster) -> dict[str, Any]:
        return c.proc_tail(pid, lines=lines, max_bytes=mb)

    return await _guard(host, f)


async def proc_kill(
    pid: int,
    signal: str = "TERM",
    grace: int = 5,
    confirm: bool = False,
    host: str | None = None,
) -> dict[str, Any]:
    """Stop a detached run: signal its whole process group (``TERM``, ``INT``, ``HUP`` or
    ``KILL``), escalating to ``KILL`` after ``grace`` seconds (0..60).

    Returns the final state with ``killed`` and the ``signals`` sent (``rc`` 143 = TERM, 137 =
    KILL). Only runs started on the current login node can be signalled. If the host requires
    confirmation for ``proc_kill`` and ``confirm`` is not ``true``, nothing is sent and the reply
    is ``{needs_confirmation: true, what}`` — re-call with ``confirm=true``.
    """
    g = _clamp(grace, 0, 60)

    def f(c: Cluster) -> dict[str, Any]:
        return c.proc_kill(pid, signal=signal, grace=g, confirm=confirm)

    return await _guard_confirmable(host, f)


# -- slurm ------------------------------------------------------------------------------------
async def submit(
    script: str | None = None,
    path: str | None = None,
    name: str | None = None,
    cwd: str | None = None,
    options: dict[str, Any] | None = None,
    args: list[str] | None = None,
    template: str | None = None,
    force_preamble: bool = False,
    host: str | None = None,
) -> dict[str, Any]:
    """Submit a batch job with sbatch. Give exactly one of ``script`` (content) or ``path``.

    ``options`` are sbatch long options without dashes, e.g.
    ``{"time": "1:00:00", "partition": "debug", "gpus_per_node": 1, "mem": "8G"}``; ``args``
    are raw extra flags. Host defaults (account/partition) are filled in automatically.
    ``template`` names a config template (see ``info``'s ``templates``): its options merge in
    (host defaults < template < your ``options``) and, for ``script=``, its preamble wraps the
    body. A template with a preamble refuses a ``path=`` submission unless ``force_preamble``.
    Returns job_id, script_path, stdout_path, stderr_path, workdir and initial state; poll
    with ``jobs(job_id=...)`` and explain finished/stuck jobs with ``diagnose``.
    """

    def f(c: Cluster) -> dict[str, Any]:
        job = c.submit(
            script,
            path=path,
            name=name,
            cwd=cwd,
            args=args,
            template=template,
            force_preamble=force_preamble,
            **(options or {}),
        )
        status = _submitted_status(job)
        result = {
            "job_id": job.job_id,
            "script_path": status.get("script_path"),
            "stdout_path": status.get("stdout_path"),
            "stderr_path": status.get("stderr_path"),
            "workdir": status.get("workdir"),
            "state": status.get("state"),
        }
        if status.get("status_error"):
            result["status_error"] = status["status_error"]
        result.update(job.submission())
        return result

    return await _guard(host, f)


async def adopt(job_id: str, host: str | None = None) -> dict[str, Any]:
    """Recover local history for a Slurm job that was accepted but not recorded locally.

    The job must belong to the configured remote user and still be visible through ``scontrol``
    or ``sacct``. Returns the reconstructed status with ``adopted=true``.
    """

    def f(c: Cluster) -> dict[str, Any]:
        job = c.adopt(job_id)
        result = job.status(refresh=True).to_dict()
        result.update({"adopted": True, "recorded": True})
        return result

    return await _guard(host, f)


async def ensure(
    manifest: dict[str, Any],
    retry: bool = False,
    retry_unknown: bool = False,
    host: str | None = None,
) -> dict[str, Any]:
    """Recover, submit, or verify a durable single-job contract.

    ``manifest`` uses the same fields as ``rslurm ensure``. MCP callers should normally provide
    ``script_inline`` because ``script`` is resolved on the MCP server's local filesystem.
    ``retry`` creates another visible attempt after FAILED/INVALID/REJECTED. An UNKNOWN attempt
    remains blocked unless ``retry_unknown`` explicitly accepts possible duplicate execution.
    """
    from .tasks import TaskSpec

    def f(c: Cluster) -> dict[str, Any]:
        return c.ensure(TaskSpec.from_mapping(manifest), retry=retry, retry_unknown=retry_unknown)

    selected_host = host or manifest.get("host")
    return await _guard(selected_host, f)


async def sweep(
    params: dict[str, list[Any]] | list[dict[str, Any]],
    script: str | None = None,
    path: str | None = None,
    template: str | None = None,
    name: str = "sweep",
    max_concurrent: int | None = None,
    host: str | None = None,
) -> dict[str, Any]:
    """Submit a parameter sweep as one job array.

    ``params`` is either ``{name: [values]}`` (the Cartesian product becomes the tasks) or a
    list of explicit row dicts. Give exactly one of ``script`` (content) or ``path`` (an
    existing remote script). A ``params.tsv`` and a wrapper are written remotely; each task
    reads its row and gets ``RS_PARAM_<NAME>`` env vars plus ``RS_PARAMS_JSON``. ``template``
    and ``max_concurrent`` (the ``%N`` array throttle) behave as for ``submit``. Returns the
    array ``job_id``, task count ``n`` and the ``params_path``; poll with ``jobs(job_id=...)``
    and explain a failed task with ``diagnose`` (it surfaces that task's parameters).
    """

    def f(c: Cluster) -> dict[str, Any]:
        job = c.sweep(
            params,
            script=script,
            path=path,
            template=template,
            name=name,
            max_concurrent=max_concurrent,
        )
        rec = c.registry.get(job.job_id) if job.recorded else None
        sweep_meta = (rec.meta.get("sweep") if rec else None) or job.metadata.get("sweep", {})
        status = _submitted_status(job)
        result = {
            "job_id": job.job_id,
            "n": sweep_meta.get("n"),
            "names": sweep_meta.get("names"),
            "params_path": sweep_meta.get("params_path"),
            "array": rec.meta.get("array") if rec else None,
            "state": status.get("state"),
        }
        if status.get("status_error"):
            result["status_error"] = status["status_error"]
        result.update(job.submission())
        return result

    return await _guard(host, f)


async def pack(
    commands: list[str],
    max_processes: int = 1,
    batches: int = 1,
    max_concurrent: int | None = None,
    dependency: str | None = None,
    template: str | None = None,
    name: str = "pack",
    cwd: str | None = None,
    options: dict[str, Any] | None = None,
    host: str | None = None,
) -> dict[str, Any]:
    """Submit independent shell commands packed onto one-node allocations with GNU Parallel.

    ``commands`` contains one shell command per item. Each of the ``batches`` Slurm array tasks
    receives a contiguous slice and runs at most ``max_processes`` commands concurrently on its
    node. ``max_concurrent`` separately caps simultaneously running array tasks. Resources come
    from the host/template/``options``; when CPUs are unspecified, CPUs per task defaults to
    ``max_processes``. Returns the job id and persistent remote command-file metadata.
    """

    def f(c: Cluster) -> dict[str, Any]:
        job = c.pack(
            commands,
            max_processes=max_processes,
            batches=batches,
            max_concurrent=max_concurrent,
            dependency=dependency,
            template=template,
            name=name,
            cwd=cwd,
            **(options or {}),
        )
        rec = c.registry.get(job.job_id) if job.recorded else None
        meta = (rec.meta.get("pack") if rec else None) or job.metadata.get("pack", {})
        status = _submitted_status(job)
        result = {
            "job_id": job.job_id,
            "state": status.get("state"),
            "script_path": status.get("script_path"),
            "stdout_path": status.get("stdout_path"),
            "commands_path": meta.get("commands_path"),
            "n": meta.get("n"),
            "batches": meta.get("batches"),
            "max_processes": meta.get("max_processes"),
        }
        if status.get("status_error"):
            result["status_error"] = status["status_error"]
        result.update(job.submission())
        return result

    return await _guard(host, f)


async def jobs(
    job_id: str | None = None,
    refresh: bool = False,
    include_finished: bool = True,
    host: str | None = None,
) -> dict[str, Any]:
    """Job status. With ``job_id``: one merged record (squeue/sacct/scontrol/local registry).

    Without it: ``{"jobs": [...], "count": n}`` covering your live queue plus jobs submitted
    through remoteslurm (finished ones too unless ``include_finished=False``). Each record has
    ``state``, ``terminal`` (done?), ``exit_code``, ``reason``, ``elapsed``, paths.
    squeue is cached ~10 s; ``refresh=True`` bypasses the cache. A job array is one record
    keyed by its base id, with an ``extra`` block (``tasks`` counts, ``failed_tasks``,
    ``task_states``); pass a task id (``123_4``) for a single task.
    """

    def f(c: Cluster) -> dict[str, Any]:
        if job_id:
            return c.job_status(job_id, refresh=refresh).to_dict()
        lst = c.jobs(include_finished=include_finished, refresh=refresh)
        result: dict[str, Any] = {
            "jobs": [s.to_dict() for s in lst],
            "count": len(lst),
            "registry_available": c.registry_error is None,
        }
        if c.registry_error:
            result["registry_error"] = c.registry_error
        return result

    return await _guard(host, f)


async def diagnose(job_id: str, tail: int = 60, host: str | None = None) -> dict[str, Any]:
    """Explain a job in one call — use this after a failure or when a job won't start.

    Returns a plain-English ``verdict`` (out-of-memory, timeout, missing module, permission,
    cancelled-by-whom, or the pending reason), actionable ``hints``, the ``stderr_tail`` /
    ``stdout_tail``, the sacct ``steps``, the merged ``status``, the submit ``script`` and any
    project ``sync`` marker. Prefer this over reading logs by hand. ``tail`` sets how many log
    lines to include; the whole payload is capped (~64 KB, ``truncated`` flags a cut).
    """

    def f(c: Cluster) -> dict[str, Any]:
        return c.diagnose(job_id, tail=tail)

    return await _guard(host, f)


async def job_output(
    job_id: str,
    tail: int = 100,
    max_bytes: int = 65536,
    stream: str = "stdout",
    host: str | None = None,
) -> dict[str, Any]:
    """Read the last ``tail`` lines of a job's stdout (or ``stream="stderr"``) log.

    The log path is resolved from Slurm/the local registry. ``max_bytes`` caps the read
    (1..1,000,000). Includes the job's current ``state``.
    """
    mb = _clamp(max_bytes, 1, 1_000_000)

    def f(c: Cluster) -> dict[str, Any]:
        return c.job_output(job_id, tail=tail, max_bytes=mb, stream=stream)

    return await _guard(host, f)


async def cancel(job_id: str, confirm: bool = False, host: str | None = None) -> dict[str, Any]:
    """Cancel one job or several (``"123,124"`` or ``"123_4"`` for an array task).

    If the host requires confirmation for ``cancel`` and ``confirm`` is not ``true``, no job is
    cancelled and the reply is ``{needs_confirmation: true, what: "..."}`` — re-call with
    ``confirm=true`` to proceed.
    """
    ids = [j.strip() for j in job_id.split(",") if j.strip()]

    def f(c: Cluster) -> dict[str, Any]:
        return c.cancel(ids, confirm=confirm)

    return await _guard_confirmable(host, f)


async def sinfo(host: str | None = None) -> dict[str, Any]:
    """Partition summary (sinfo): partitions, node states, time limits. Cheap."""

    def f(c: Cluster) -> dict[str, Any]:
        parts = c.sinfo()
        return {"partitions": parts, "count": len(parts)}

    return await _guard(host, f)


async def info(refresh: bool = False, host: str | None = None) -> dict[str, Any]:
    """Remote facts plus local policy: notes, templates, projects, and selected env vars."""

    def f(c: Cluster) -> dict[str, Any]:
        return dict(c.info(refresh=refresh))

    return await _guard(host, f)


async def connection(host: str | None = None) -> dict[str, Any]:
    """Check connectivity without hanging: is the ssh master alive, is the remote stub up?

    Use this first after any ``not_connected``/``auth_required`` error. When not alive,
    ``action`` holds the exact command the *user* must run in a terminal (MFA hosts cannot be
    authenticated by an agent), e.g. ``remoteslurm connect mycluster``.

    When alive it also reports the master's ``connected_at``/``age``. If the host sets
    ``session_lifetime`` (the site cuts connections after a fixed time) it adds ``expires_at``/
    ``remaining_seconds``, and a ``warning`` plus ``action`` once less than an hour remains.
    Check this before starting long or unattended work, so the user can reconnect first.
    """

    def work() -> dict[str, Any]:
        from .config import Config

        cfg = Config.load()
        hc = cfg.host(host)
        from .cluster import _clusters

        c = _clusters.get(hc.name)
        transport = c.transport if c is not None else Cluster._transport_for(hc)
        out: dict[str, Any] = {
            "host": hc.name,
            "ssh_alias": hc.ssh,
            "mfa": hc.mfa,
            "master_alive": None,
            "stub_alive": bool(c is not None and c.session.alive),
            "action": None,
        }
        if isinstance(transport, SSHTransport):
            out["master_alive"] = transport.master_alive()
            if not out["master_alive"]:
                out["action"] = f"run in a terminal: remoteslurm connect {hc.name}"
                out["connect_cmd"] = shlex.join(transport.connect_cmd())
            else:
                lt = transport.master_lifetime(hc.session_lifetime)
                out.update(lt)
                if lt.get("expiring"):
                    left = int(lt.get("remaining_seconds") or 0)
                    eta = f"in {format_duration(left)}" if left > 0 else "at any moment"
                    out["warning"] = (
                        f"the ssh connection to {hc.name} is {lt['age']} old and the site limit "
                        f"is {hc.session_lifetime}; expect it to drop {eta}. Ask the user to "
                        "reconnect before starting long or unattended work."
                    )
                    out["action"] = f"run in a terminal: remoteslurm connect --force {hc.name}"
        else:
            out["transport"] = "local"
            out["master_alive"] = True
        if out["master_alive"] and not out["stub_alive"]:
            out["note"] = "stub not started yet; it starts on the first tool call"
        return out

    try:
        return await asyncio.to_thread(work)
    except RemoteSlurmError as e:
        return e.to_dict()
    except Exception as e:  # noqa: BLE001
        return {"error": "internal", "message": f"{type(e).__name__}: {e}"}


# -- project sync -----------------------------------------------------------------------------
async def sync(
    project: str | None = None,
    direction: str = "push",
    dry_run: bool = False,
    delete: bool = False,
    force: bool = False,
    host: str | None = None,
) -> dict[str, Any]:
    """rsync a configured project between the local machine and the cluster.

    Projects come from ``[hosts.X.projects.NAME]`` in the config (``projects`` lists them).
    Omit ``project`` to pick the one containing the current directory. ``direction`` is
    ``"push"`` (default) or ``"pull"``. ``dry_run=True`` shows what would change without
    touching anything. ``delete=True`` removes remote files missing locally, and only works
    when the project also sets ``delete = true`` (double opt-in). Oversized pushes are
    refused (``error: too_large``) unless ``force=True``. rsync runs locally in the server
    process; a ``.remoteslurm-sync.json`` marker is written after each successful push. The
    rsync is bounded by the per-call limit (``$REMOTESLURM_MCP_MAX_CALL``, default 1500 s).
    """
    from pathlib import Path

    from . import sync as sync_mod
    from .errors import InvalidArgument

    def f(c: Cluster) -> dict[str, Any]:
        if direction not in ("push", "pull"):
            raise InvalidArgument(
                f"direction must be 'push' or 'pull', not {direction!r}",
            )
        p = sync_mod.resolve_project(c.host, project, Path.cwd())
        return sync_mod.sync(
            c,
            p,
            pull=direction == "pull",
            dry_run=dry_run,
            delete=delete,
            force=force,
            force_protected=force,
            timeout=_max_call(),
        )

    return await _guard(host, f)


async def projects(host: str | None = None) -> dict[str, Any]:
    """List the host's configured sync projects (name, local, remote, excludes)."""

    def f(c: Cluster) -> dict[str, Any]:
        rows = [
            {
                "name": name,
                "local": p.local,
                "remote": p.remote,
                "exclude": p.exclude,
                "delete": p.delete,
            }
            for name, p in sorted(c.host.projects.items())
        ]
        return {"projects": rows, "count": len(rows), "host": c.host.name}

    return await _guard(host, f)


# -- queue intelligence (F1) ------------------------------------------------------------------
async def queue_info(host: str | None = None) -> dict[str, Any]:
    """One-call queue picture: ``partitions`` (with idle-node counts), my ``accounts``/``qos``
    limits, ``fairshare``, and my ``pending`` jobs with the scheduler's start estimates.

    Use this to pick a partition/account and to explain why a job hasn't started. Each section
    is empty when its Slurm tool is missing on the cluster (site formats vary) — never an error.
    """

    def f(c: Cluster) -> dict[str, Any]:
        return c.queue_info()

    return await _guard(host, f)


async def quota(host: str | None = None) -> dict[str, Any]:
    """Disk usage / quota for the user's filesystems.

    Uses the host's optional ``quota_command`` when set, else ``df -h`` over ``quota_paths``.
    Custom output remains raw unless ``quota_format`` selects the generic ``pairs`` or ``df``
    parser. Returns ``available`` (false when the tool is missing), parsed ``usage`` rows when
    configured, and the ``raw`` text.
    """

    def f(c: Cluster) -> dict[str, Any]:
        return c.quota()

    return await _guard(host, f)


# -- watch / wait (F2) ------------------------------------------------------------------------
def _wait_job(c: Cluster, job_id: str, cap: int) -> dict[str, Any]:
    interval = 5.0
    tracker = CURRENT_INFLIGHT.get()
    t0 = time.monotonic()
    st = c.job_status(job_id, refresh=True)
    # Bound WALL-CLOCK, not iteration count: each job_status can itself take seconds over a slow
    # ssh link, so sleep only for the time left in the cap.
    while not st.terminal:
        elapsed = time.monotonic() - t0
        if elapsed >= cap:
            break
        pause = min(interval, cap - elapsed)
        if tracker is not None:
            if tracker.cancelled.wait(pause):
                break  # the MCP call was cancelled: stop polling
        else:
            time.sleep(pause)
        st = c.job_status(job_id, refresh=True)
    d = st.to_dict()
    d["terminal"] = bool(st.terminal)
    return d


async def wait(
    job_id: str | None = None,
    timeout: int = 120,
    host: str | None = None,
    *,
    pid: int | None = None,
    path: str | None = None,
    pattern: str | None = None,
    offset: int | None = None,
) -> dict[str, Any]:
    """Bounded wait — up to ``timeout`` seconds (capped at 300), then return. Give one target:

    * ``job_id``: a Slurm job. Returns its status with ``terminal: bool`` (``state``/``exit_code``
      once finished).
    * ``pid``: a ``run(detach=True)`` process. Returns when it stops running (``process`` holds
      its final state and ``rc``).
    * ``path``: returns when the remote file exists; with ``pattern`` (a Python regex), when a
      line of the file matches. ``pid`` + ``pattern`` watches that process's log and also
      returns if the process exits first.

    Non-job waits return ``{done, met, reason, waited, ...}``: ``done`` = stop waiting
    (``reason``: ``matched``/``exists``/``exited``), ``met`` = the condition you asked for held; a
    match carries ``line``. Most MCP clients cap how long one call may run, so on timeout it
    returns ``terminal: false`` / ``done: false`` — call again to keep waiting, passing back
    ``offset`` for a pattern wait so the scan resumes rather than restarts. Prefer this over
    polling ``jobs``/``proc_status`` or a hand-written sleep loop in ``run``.
    """
    cap = _clamp(timeout, 1, 300)

    def f(c: Cluster) -> dict[str, Any]:
        from .errors import InvalidArgument

        if job_id is not None:
            if pid is not None or path is not None or pattern is not None:
                raise InvalidArgument("give either job_id or pid/path/pattern, not both")
            return _wait_job(c, job_id, cap)
        if pid is None and path is None:
            raise InvalidArgument(
                "wait needs a target: job_id, pid (a detached run) or path (+ optional pattern)"
            )
        return c.wait_for(pid=pid, path=path, pattern=pattern, offset=offset, timeout=cap)

    return await _guard(host, f)


async def events(since: str | None = None, host: str | None = None) -> dict[str, Any]:
    """Job-finish events recorded by ``rslurm watch`` — "did anything finish while I worked?".

    With no ``since``, returns only events not yet seen (a read cursor advances so the next call
    won't repeat them). With ``since`` (an ISO timestamp or epoch), returns every event at/after
    that time and leaves the cursor untouched. Returns ``{events: [...], count}``; never blocks.
    """

    def work() -> dict[str, Any]:
        from . import watch
        from .config import Config

        cfg = Config.load()
        hc = cfg.host(host)
        evs = watch.drain_events(hc.name, since=since, all=False)
        return {"events": evs, "count": len(evs), "host": hc.name}

    try:
        return await asyncio.to_thread(work)
    except RemoteSlurmError as e:
        return e.to_dict()
    except Exception as e:  # noqa: BLE001
        return {"error": "internal", "message": f"{type(e).__name__}: {e}"}


# -- resources ---------------------------------------------------------------------------------
def guide_resource() -> str:
    """The agent guide (also `rslurm agent-guide`)."""
    from .guide import AGENT_GUIDE

    return AGENT_GUIDE


# -- server assembly ---------------------------------------------------------------------------
# Every tool, in a stable order. Only the CORE_TOOLS subset is registered unless
# REMOTESLURM_MCP_TOOLS=all.
ALL_TOOLS: dict[str, Any] = {
    "info": info,
    "ls": ls,
    "read": read,
    "edit": edit,
    "grep": grep,
    "glob": glob,
    "write": write,
    "diff": diff,
    "run": run,
    "proc_status": proc_status,
    "proc_tail": proc_tail,
    "proc_kill": proc_kill,
    "submit": submit,
    "adopt": adopt,
    "ensure": ensure,
    "pack": pack,
    "sweep": sweep,
    "jobs": jobs,
    "diagnose": diagnose,
    "job_output": job_output,
    "cancel": cancel,
    "sinfo": sinfo,
    "connection": connection,
    "sync": sync,
    "projects": projects,
    "wait": wait,
    "queue_info": queue_info,
    "quota": quota,
    "events": events,
}


def make_mcp(tool_set: str | None = None) -> FastMCP:
    """Build the FastMCP app exposing ``core`` (default) or ``all`` tools."""
    tool_set = (tool_set or os.environ.get(ENV_MCP_TOOLS, "core")).strip().lower()
    m = FastMCP("remoteslurm")
    for name, fn in ALL_TOOLS.items():
        if tool_set == "all" or name in CORE_TOOLS:
            m.tool(name=name)(fn)
    m.resource("remoteslurm://guide")(guide_resource)
    return m


mcp = make_mcp()


# -- entry points ------------------------------------------------------------------------------
def mcp_config_snippet(host: str | None = None) -> str:
    """JSON snippet for a client's ``mcpServers`` config (printed by the CLI)."""
    return json.dumps(
        {
            "mcpServers": {
                "remoteslurm": {
                    "command": "remoteslurm-mcp",
                    "args": [],
                    "env": {
                        ENV_DEFAULT_HOST: host or "<host>",
                        # tool set: "core" (default) or "all" (adds glob/diff/job_output/
                        # sinfo/projects/sweep/queue_info/quota/events). Remove to keep core.
                        ENV_MCP_TOOLS: "core",
                    },
                }
            }
        },
        indent=2,
    )


def main() -> None:
    mcp.run()


if __name__ == "__main__":  # pragma: no cover
    main()
