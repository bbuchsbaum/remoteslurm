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
from collections.abc import Callable
from typing import Any

from mcp.server.fastmcp import FastMCP

from .cluster import Cluster
from .config import ENV_DEFAULT_HOST
from .errors import RemoteSlurmError
from .transport import SSHTransport

ENV_MAX_CHARS = "REMOTESLURM_MCP_MAX_CHARS"
# Hard ceiling on any single string field in a tool result (agents have token caps).
MAX_CHARS = int(os.environ.get(ENV_MAX_CHARS, "200000"))

ENV_MCP_TOOLS = "REMOTESLURM_MCP_TOOLS"
# The default tool set: the workflow-critical tools an agent needs, nothing more. Set
# REMOTESLURM_MCP_TOOLS=all to also expose glob/diff/job_output/sinfo/projects/sweep.
CORE_TOOLS = {
    "info",
    "ls",
    "read",
    "edit",
    "grep",
    "write",
    "run",
    "submit",
    "jobs",
    "diagnose",
    "sync",
    "cancel",
    "connection",
}


# -- helpers ---------------------------------------------------------------------------------
def _get_cluster(host: str | None) -> Cluster:
    """Return the (cached) connected cluster for ``host``. Tests monkeypatch this."""
    return Cluster.connect(host)


def _max_chars() -> int:
    return int(os.environ.get(ENV_MAX_CHARS, str(MAX_CHARS)))


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


async def _guard(host: str | None, fn: Callable[[Cluster], dict[str, Any]]) -> dict[str, Any]:
    """Run ``fn(cluster)`` in a worker thread, converting errors to dicts."""

    def work() -> dict[str, Any]:
        return fn(_get_cluster(host))

    try:
        return _cap(await asyncio.to_thread(work))
    except RemoteSlurmError as e:
        return e.to_dict()
    except Exception as e:  # noqa: BLE001 - never let an exception cross the MCP boundary
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
    host: str | None = None,
) -> dict[str, Any]:
    """Write (or append) text to a remote file, creating parent directories as needed.

    ``mode`` is an octal permission as an int (e.g. 493 == 0o755). Returns path and bytes
    written. Use ``submit`` with ``script=`` instead of writing job scripts by hand.
    """

    def f(c: Cluster) -> dict[str, Any]:
        return c.write(path, content, append=append, mode=mode)

    return await _guard(host, f)


async def edit(
    path: str,
    old: str,
    new: str,
    expect: int = 1,
    all: bool = False,
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
        return c.edit(path, old, new, expect=expect, all=all)

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
    cmd: str,
    cwd: str | None = None,
    timeout: int = 60,
    login: bool = False,
    max_output: int = 65536,
    host: str | None = None,
) -> dict[str, Any]:
    """Run a shell command on the *login node* (not a compute node) and return rc/stdout/stderr.

    Output is capped at ``max_output`` bytes per stream; ``timeout`` is clamped to 1..3600 s.
    ``login=True`` runs through a login shell (loads modules/profile). Keep it light: heavy
    work belongs in ``submit``. Returns ``error: permission`` when the host config has
    ``allow_run = false``.
    """
    to = _clamp(timeout, 1, 3600)

    def f(c: Cluster) -> dict[str, Any]:
        return c.run(cmd, cwd=cwd, timeout=to, login=login, max_output=max_output)

    return await _guard(host, f)


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
        st = job.status()
        return {
            "job_id": job.job_id,
            "script_path": st.script_path,
            "stdout_path": st.stdout_path,
            "stderr_path": st.stderr_path,
            "workdir": st.workdir,
            "state": st.state,
        }

    return await _guard(host, f)


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
        rec = c.registry.get(job.job_id)
        sweep_meta = (rec.meta.get("sweep") if rec else None) or {}
        st = job.status()
        return {
            "job_id": job.job_id,
            "n": sweep_meta.get("n"),
            "names": sweep_meta.get("names"),
            "params_path": sweep_meta.get("params_path"),
            "array": rec.meta.get("array") if rec else None,
            "state": st.state,
        }

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
        return {"jobs": [s.to_dict() for s in lst], "count": len(lst)}

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


async def cancel(job_id: str, host: str | None = None) -> dict[str, Any]:
    """Cancel one job or several (``"123,124"`` or ``"123_4"`` for an array task)."""
    ids = [j.strip() for j in job_id.split(",") if j.strip()]

    def f(c: Cluster) -> dict[str, Any]:
        r = c.cancel(ids)
        return r

    return await _guard(host, f)


async def sinfo(host: str | None = None) -> dict[str, Any]:
    """Partition summary (sinfo): partitions, node states, time limits. Cheap."""

    def f(c: Cluster) -> dict[str, Any]:
        parts = c.sinfo()
        return {"partitions": parts, "count": len(parts)}

    return await _guard(host, f)


async def info(refresh: bool = False, host: str | None = None) -> dict[str, Any]:
    """Remote facts: user, home, hostname, Slurm version, selected env vars (cached)."""

    def f(c: Cluster) -> dict[str, Any]:
        return dict(c.info(refresh=refresh))

    return await _guard(host, f)


async def connection(host: str | None = None) -> dict[str, Any]:
    """Check connectivity without hanging: is the ssh master alive, is the remote stub up?

    Use this first after any ``not_connected``/``auth_required`` error. When not alive,
    ``action`` holds the exact command the *user* must run in a terminal (MFA hosts cannot be
    authenticated by an agent), e.g. ``remoteslurm connect trillium``.
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
    process; a ``.remoteslurm-sync.json`` marker is written after each successful push.
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
    "submit": submit,
    "sweep": sweep,
    "jobs": jobs,
    "diagnose": diagnose,
    "job_output": job_output,
    "cancel": cancel,
    "sinfo": sinfo,
    "connection": connection,
    "sync": sync,
    "projects": projects,
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
                        # sinfo/projects/sweep). Remove this line to keep the default core set.
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
