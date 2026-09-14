"""Job handles, the local job registry, and the Slurm operations mixed into ``Cluster``."""

from __future__ import annotations

import fcntl
import itertools
import json
import os
import re
import shlex
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import slurm
from .config import Template, state_dir
from .errors import (
    ConfirmationRequired,
    InvalidArgument,
    PermissionDenied,
    RegistryUnavailable,
    RemoteSlurmError,
    RemoteTimeout,
    SlurmError,
)

if TYPE_CHECKING:
    from .cluster import Cluster

SQUEUE_CACHE_SECONDS = 10.0
ACCOUNTING_GRACE_SECONDS = 600.0  # how long after last sighting we report "accounting_pending"
MAX_SWEEP_TASKS = 1000  # guard: refuse sweeps larger than a typical Slurm MaxArraySize
MAX_PACK_BATCHES = 1000  # same scheduler-facing guard for packed command arrays
LEARNED_NOTES_CAP = 50


def _slurm_timestamp(value: str | None) -> float:
    if value and value not in {"Unknown", "N/A", "None"}:
        try:
            return datetime.fromisoformat(value).timestamp()
        except ValueError:
            pass
    return time.time()


# --------------------------------------------------------------------------- learned notes
def learned_notes_path(host: str) -> Path:
    return state_dir() / host / "learned_notes.txt"


def read_learned_notes(host: str) -> list[str]:
    """Lines remembered from prior policy rejections on ``host`` (client-side state)."""
    p = learned_notes_path(host)
    try:
        if p.exists():
            return [ln.rstrip("\n") for ln in p.read_text("utf-8").splitlines() if ln.strip()]
    except OSError:
        pass
    return []


def append_learned_notes(host: str, lines: list[str], *, cap: int = LEARNED_NOTES_CAP) -> None:
    """Append policy-rejection ``lines`` (deduped, most-recent-``cap`` kept). Best effort."""
    if not lines:
        return
    p = learned_notes_path(host)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        existing = read_learned_notes(host)
        seen = set(existing)
        for line in lines:
            if line not in seen:
                existing.append(line)
                seen.add(line)
        existing = existing[-cap:]
        p.write_text("\n".join(existing) + "\n", "utf-8")
    except OSError:
        pass


# --------------------------------------------------------------------------- template scripts
def _opts_repr(opts: dict[str, Any]) -> str:
    return "{" + ", ".join(f"{k}={v}" for k, v in sorted(opts.items())) + "}"


def compose_templated_script(
    script: str, template: Template, *, name: str, merged_options: dict[str, Any]
) -> str:
    """Wrap ``script`` with the template preamble/epilogue and a documentation header.

    Layout: ``#!`` shebang (if any) -> ``# remoteslurm: template=… options=…`` ->
    preamble -> original body -> epilogue. Options remain command-line sbatch flags; the
    header is documentation only (read back by ``diagnose``).
    """
    header = f"# remoteslurm: template={template.name} options={_opts_repr(merged_options)}"
    body = script
    shebang: str | None = None
    if body.startswith("#!"):
        nl = body.find("\n")
        if nl == -1:
            shebang, body = body, ""
        else:
            shebang, body = body[:nl], body[nl + 1 :]
    chunks: list[str] = []
    if shebang is not None:
        chunks.append(shebang + "\n")
    chunks.append(header + "\n")
    if template.preamble:
        chunks.append(
            template.preamble if template.preamble.endswith("\n") else template.preamble + "\n"
        )
    if body:
        chunks.append(body if body.endswith("\n") else body + "\n")
    if template.epilogue:
        chunks.append(
            template.epilogue if template.epilogue.endswith("\n") else template.epilogue + "\n"
        )
    return "".join(chunks)


# --------------------------------------------------------------------------- parameter sweeps
_PARAM_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def sweep_rows(
    params: dict[str, list[Any]] | list[dict[str, Any]],
) -> tuple[list[str], list[dict[str, Any]]]:
    """Turn ``params`` into ``(names, rows)`` — the columns and per-task parameter dicts.

    A ``dict`` of ``{name: [values]}`` becomes the Cartesian product (column order preserved);
    a ``list`` of dicts is taken as explicit rows (all must share the same keys in the same
    order). Parameter names must be valid shell identifiers (they become ``RS_PARAM_<NAME>``).
    """
    if isinstance(params, dict):
        names = list(params.keys())
        for n in names:
            if not isinstance(params[n], (list, tuple)) or not params[n]:
                raise InvalidArgument(f"sweep values for {n!r} must be a non-empty list")
        rows = [
            dict(zip(names, combo, strict=False))
            for combo in itertools.product(*(params[n] for n in names))
        ]
    elif isinstance(params, list):
        if not params:
            raise InvalidArgument("sweep params list is empty")
        names = list(params[0].keys())
        rows = []
        for r in params:
            if list(r.keys()) != names:
                raise InvalidArgument(
                    "every sweep row must have the same keys in the same order",
                    first=names,
                    offending=list(r.keys()),
                )
            rows.append(dict(r))
    else:
        raise InvalidArgument("sweep params must be a dict of lists or a list of dicts")
    if not names:
        raise InvalidArgument("sweep has no parameters")
    for n in names:
        if not _PARAM_NAME_RE.match(n):
            raise InvalidArgument(
                f"invalid sweep parameter name {n!r}",
                action="parameter names become RS_PARAM_<NAME>: use letters, digits, underscore",
            )
    if not rows:
        raise InvalidArgument("sweep produced no parameter combinations")
    for r in rows:  # values go into a TSV; tabs/newlines would corrupt it
        for n in names:
            v = str(r[n])
            if "\t" in v or "\n" in v:
                raise InvalidArgument(f"sweep value for {n!r} may not contain a tab or newline")
    return names, rows


def params_tsv(names: list[str], rows: list[dict[str, Any]]) -> str:
    """Render the ``params.tsv`` table (header row + one data row per task)."""
    lines = ["\t".join(names)]
    lines.extend("\t".join(str(r[n]) for n in names) for r in rows)
    return "\n".join(lines) + "\n"


_SWEEP_WRAPPER_TMPL = r"""#!/bin/bash
# remoteslurm sweep wrapper (name=__NAME__); reads params.tsv by $SLURM_ARRAY_TASK_ID and
# exports RS_PARAM_<NAME> for each column plus RS_PARAMS_JSON for the whole row.
RS_PARAMS_TSV=__PARAMS_Q__
RS_TASK_ID="${SLURM_ARRAY_TASK_ID:-0}"
IFS=$'\t' read -r -a __rs_names < <(sed -n '1p' "$RS_PARAMS_TSV")
IFS=$'\t' read -r -a __rs_vals < <(sed -n "$((RS_TASK_ID + 2))p" "$RS_PARAMS_TSV")
__i=0
while [ "$__i" -lt "${#__rs_names[@]}" ]; do
  export "RS_PARAM_${__rs_names[$__i]}=${__rs_vals[$__i]}"
  __i=$((__i + 1))
done
export RS_PARAMS_JSON="$(python3 -c 'import sys,json,csv
tsv=sys.argv[1]
idx=int(sys.argv[2])
with open(tsv) as f:
    rows=list(csv.reader(f,delimiter="\t",quoting=csv.QUOTE_NONE))
if 0 <= idx+1 < len(rows):
    print(json.dumps(dict(zip(rows[0],rows[idx+1]))))
else:
    print("{}")' "$RS_PARAMS_TSV" "$RS_TASK_ID")"
# ---- user body ----
__BODY__
"""


def sweep_wrapper(
    params_path: str, name: str, *, body: str | None = None, remote_path: str | None = None
) -> str:
    """Build the sweep wrapper script embedding the (absolute) ``params_path``.

    Exactly one of ``body`` (inline user script content, appended) or ``remote_path`` (an
    existing remote script, run via ``exec bash``) is used.
    """
    if (body is None) == (remote_path is None):
        raise InvalidArgument("provide exactly one of body= or remote_path=")
    user = body if body is not None else f"exec bash {shlex.quote(remote_path or '')}"
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)[:64] or "sweep"
    return (
        _SWEEP_WRAPPER_TMPL.replace("__NAME__", safe_name)
        .replace("__PARAMS_Q__", shlex.quote(params_path))
        .replace("__BODY__", user)
    )


# --------------------------------------------------------------------------- packed commands
def packed_commands(commands: list[str]) -> list[str]:
    """Validate one-command-per-line input while preserving shell syntax and whitespace."""
    if not isinstance(commands, (list, tuple)):
        raise InvalidArgument("packed commands must be a list of shell command strings")
    clean: list[str] = []
    for command in commands:
        if not isinstance(command, str):
            raise InvalidArgument("every packed command must be a string")
        if "\x00" in command or "\n" in command:
            raise InvalidArgument("a packed command may not contain NUL or newline characters")
        command = command.rstrip("\r")
        if command.strip():
            clean.append(command)
    if not clean:
        raise InvalidArgument("packed command list is empty")
    return clean


_PACK_WRAPPER_TMPL = r"""#!/bin/bash
# remoteslurm packed-command wrapper (name=__NAME__). Each Slurm array task selects a
# contiguous slice of the command file and runs at most RS_MAX_PROCESSES through GNU Parallel.
set -o pipefail
RS_COMMANDS=__COMMANDS_Q__
RS_TOTAL=__TOTAL__
RS_BATCHES=__BATCHES__
RS_MAX_PROCESSES=__MAX_PROCESSES__
RS_BATCH_ID="${SLURM_ARRAY_TASK_ID:-0}"
case "$RS_BATCH_ID" in
  ''|*[!0-9]*) echo "invalid SLURM_ARRAY_TASK_ID: $RS_BATCH_ID" >&2; exit 2 ;;
esac
if [ "$RS_BATCH_ID" -ge "$RS_BATCHES" ]; then
  echo "SLURM_ARRAY_TASK_ID $RS_BATCH_ID is outside 0-$((RS_BATCHES - 1))" >&2
  exit 2
fi
if ! command -v parallel >/dev/null 2>&1; then
  echo "GNU Parallel is required for a remoteslurm packed job" >&2
  exit 127
fi
RS_PER_BATCH=$(( (RS_TOTAL + RS_BATCHES - 1) / RS_BATCHES ))
RS_START=$(( RS_BATCH_ID * RS_PER_BATCH + 1 ))
RS_END=$(( RS_START + RS_PER_BATCH - 1 ))
if [ "$RS_END" -gt "$RS_TOTAL" ]; then RS_END="$RS_TOTAL"; fi
if [ "$RS_START" -gt "$RS_TOTAL" ]; then exit 0; fi
sed -n "${RS_START},${RS_END}p" "$RS_COMMANDS" | parallel --jobs "$RS_MAX_PROCESSES"
"""


def pack_wrapper(
    commands_path: str, name: str, *, total: int, batches: int, max_processes: int
) -> str:
    """Build a one-node packed-command array wrapper around a remote command file."""
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)[:64] or "pack"
    return (
        _PACK_WRAPPER_TMPL.replace("__NAME__", safe_name)
        .replace("__COMMANDS_Q__", shlex.quote(commands_path))
        .replace("__TOTAL__", str(total))
        .replace("__BATCHES__", str(batches))
        .replace("__MAX_PROCESSES__", str(max_processes))
    )


@dataclass
class JobRecord:
    job_id: str
    name: str | None = None
    script_path: str | None = None
    stdout_path: str | None = None
    stderr_path: str | None = None
    workdir: str | None = None
    submit_time: float = 0.0
    last_state: str | None = None
    last_seen: float = 0.0
    sbatch_args: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)


class JobRegistry:
    """Per-host JSON file of jobs submitted through remoteslurm (survives agent restarts).

    Several processes (CLI invocations, the MCP server, the daemon) may write the same file,
    so every mutation re-reads the file under an exclusive ``flock`` and merges by job id.
    """

    def __init__(self, host: str, base: Path | None = None) -> None:
        self.path = (base or state_dir()) / host / "jobs.json"
        self._lock = threading.Lock()

    def _read_raw(self, *, strict_io: bool = False) -> dict[str, Any]:
        """The whole registry file as a dict (``{"jobs": [...], "last_pruned": ...}``)."""
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text("utf-8"))
                if isinstance(data, dict):
                    return data
            except OSError:
                if strict_io:
                    raise
            except ValueError:
                pass
        return {}

    @staticmethod
    def _jobs_from_raw(raw: dict[str, Any]) -> dict[str, JobRecord]:
        jobs: dict[str, JobRecord] = {}
        for d in raw.get("jobs", []):
            try:
                known = {k: v for k, v in d.items() if k in JobRecord.__dataclass_fields__}
                jobs[known["job_id"]] = JobRecord(**known)
            except (TypeError, KeyError):
                continue
        return jobs

    def _read_file(self) -> dict[str, JobRecord]:
        return self._jobs_from_raw(self._read_raw(strict_io=True))

    def _write_raw(self, raw: dict[str, Any]) -> None:
        tmp = self.path.with_name(f"jobs.{os.getpid()}.{threading.get_ident()}.tmp")
        tmp.write_text(json.dumps(raw, indent=1), "utf-8")
        os.replace(tmp, self.path)

    def preflight(self) -> None:
        """Prove that the registry lock and atomic replacement are writable.

        This deliberately exercises the same path as a later ``put`` instead of relying on
        ``os.access``, which cannot establish that locking and replacement will work.
        """
        try:
            with self._locked_raw():
                pass
        except OSError as e:
            raise RegistryUnavailable(
                f"local job registry is not writable: {self.path}",
                action=(
                    "set REMOTESLURM_STATE_DIR to a writable directory, for example "
                    "`REMOTESLURM_STATE_DIR=/tmp/remoteslurm-state rslurm ...`"
                ),
                path=str(self.path),
                operation="preflight",
                cause=str(e),
            ) from e

    @contextmanager
    def _locked_raw(self) -> Iterator[dict[str, Any]]:
        """Yield the whole registry dict under an exclusive lock; write it back on exit.

        Top-level keys other than ``jobs`` (e.g. ``last_pruned``) are preserved across writes.
        """
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path.with_suffix(".lock"), "w") as lf:
                fcntl.flock(lf, fcntl.LOCK_EX)
                try:
                    raw = self._read_raw(strict_io=True)
                    yield raw
                    self._write_raw(raw)
                finally:
                    fcntl.flock(lf, fcntl.LOCK_UN)

    @contextmanager
    def _locked(self) -> Iterator[dict[str, JobRecord]]:
        """Yield the on-disk records under an exclusive lock; write back on exit."""
        with self._locked_raw() as raw:
            jobs = self._jobs_from_raw(raw)
            yield jobs
            raw["jobs"] = [asdict(j) for j in jobs.values()]

    def get(self, job_id: str) -> JobRecord | None:
        with self._lock:
            return self._read_file().get(job_id)

    def all(self) -> list[JobRecord]:
        with self._lock:
            return sorted(self._read_file().values(), key=lambda j: j.submit_time)

    def put(self, rec: JobRecord) -> None:
        with self._locked() as jobs:
            jobs[rec.job_id] = rec

    def update(self, job_id: str, **fields: Any) -> None:
        with self._locked() as jobs:
            rec = jobs.get(job_id)
            if rec is None:
                return
            for k, v in fields.items():
                setattr(rec, k, v)

    def forget(self, job_id: str) -> bool:
        with self._locked() as jobs:
            return jobs.pop(job_id, None) is not None

    def prune(
        self,
        older_than_days: int = 30,
        *,
        keep_active: bool = True,
        force: bool = False,
        interval: float = 3600.0,
    ) -> dict[str, Any]:
        """Drop stale records: ``last_seen`` older than the cutoff AND a terminal ``last_state``.

        ``keep_active`` (default) never drops a record whose last state is active/unknown, only
        the finished ones. Throttled to once per ``interval`` seconds via a ``last_pruned``
        timestamp stored in the registry file (under the same flock) — pass ``force=True`` to
        prune now regardless. Returns ``{pruned, removed, kept, throttled}``.
        """
        now = time.time()
        cutoff = now - older_than_days * 86400
        with self._locked_raw() as raw:
            last = raw.get("last_pruned") or 0
            jobs = self._jobs_from_raw(raw)
            if not force and (now - float(last)) < interval:
                # Preserve the file as-is (write back what we read) and report the throttle.
                raw["jobs"] = [asdict(j) for j in jobs.values()]
                return {"pruned": 0, "removed": [], "kept": len(jobs), "throttled": True}
            removed: list[str] = []
            for jid, rec in list(jobs.items()):
                if not rec.last_seen or rec.last_seen >= cutoff:
                    continue
                terminal = bool(rec.last_state) and slurm.is_terminal(rec.last_state or "")
                if keep_active and not terminal:
                    continue
                del jobs[jid]
                removed.append(jid)
            raw["jobs"] = [asdict(j) for j in jobs.values()]
            raw["last_pruned"] = now
            return {
                "pruned": len(removed),
                "removed": removed,
                "kept": len(jobs),
                "throttled": False,
            }

    def audit(self, event: str, **data: Any) -> None:
        """Append-only audit log next to the registry (run/write/cancel/submit)."""
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path.parent / "audit.log", "a", encoding="utf-8") as f:
                f.write(json.dumps({"t": time.time(), "event": event, **data}) + "\n")
        except OSError:
            pass


class Job:
    """A lightweight handle; all state lives on the cluster / in the registry."""

    def __init__(
        self,
        cluster: Cluster,
        job_id: str,
        *,
        recorded: bool = True,
        registry_error: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.cluster = cluster
        self.job_id = job_id
        self.recorded = recorded
        self.registry_error = registry_error
        self.metadata = metadata or {}

    def submission(self) -> dict[str, Any]:
        """Machine-readable scheduler/local-registry outcome for this handle."""
        out: dict[str, Any] = {
            "submitted": True,
            "recorded": self.recorded,
            "job_id": self.job_id,
        }
        if not self.recorded:
            out.update(
                registry_error=self.registry_error,
                recovery=f"rslurm adopt {self.job_id}",
            )
        return out

    def __repr__(self) -> str:
        return f"Job({self.job_id!r} on {self.cluster.host.name!r})"

    def status(self, refresh: bool = False) -> slurm.JobStatus:
        return self.cluster.job_status(self.job_id, refresh=refresh)

    def wait(self, *, poll: float = 15.0, timeout: float | None = None) -> slurm.JobStatus:
        return self.cluster.wait(self.job_id, poll=poll, timeout=timeout)

    def cancel(self) -> dict[str, Any]:
        return self.cluster.cancel(self.job_id)

    def output(self, *, tail: int | None = 100, max_bytes: int = 65536) -> dict[str, Any]:
        return self.cluster.job_output(self.job_id, tail=tail, max_bytes=max_bytes)


class SlurmOps:
    """Mixin providing Slurm operations; expects ``self.call``, ``self.host``, ``self.info``."""

    host: Any
    _squeue_cache: tuple[float, list[dict[str, str]]] | None = None
    _squeue_lock = threading.Lock()
    _registry: JobRegistry | None = None
    _registry_error: str | None = None
    _sweep_params_cache: dict[str, list[dict[str, Any]]] | None = None

    def call(
        self, op: str, *, _timeout: float | None = 60.0, **args: Any
    ) -> Any:  # pragma: no cover
        raise NotImplementedError

    def read(  # pragma: no cover
        self,
        path: str,
        *,
        max_bytes: int = 65536,
        offset: int = 0,
        head: int | None = None,
        tail: int | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def write(  # pragma: no cover
        self,
        path: str,
        content: str | bytes,
        *,
        append: bool = False,
        mkdirs: bool = True,
        mode: int | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @property
    def registry(self) -> JobRegistry:
        if self._registry is None:
            self._registry = JobRegistry(self.host.name)
        return self._registry

    @property
    def registry_error(self) -> str | None:
        return self._registry_error

    def _note_registry_error(self, error: OSError) -> None:
        self._registry_error = f"{type(error).__name__}: {error}"

    def _registry_get(self, job_id: str) -> JobRecord | None:
        try:
            return self.registry.get(job_id)
        except OSError as e:
            self._note_registry_error(e)
            return None

    def _registry_all(self) -> list[JobRecord]:
        try:
            return self.registry.all()
        except OSError as e:
            self._note_registry_error(e)
            return []

    def _registry_update(self, job_id: str, **fields: Any) -> bool:
        try:
            self.registry.update(job_id, **fields)
            return True
        except OSError as e:
            self._note_registry_error(e)
            return False

    def _mark_registry_status(self, status: slurm.JobStatus) -> slurm.JobStatus:
        if self._registry_error:
            status.registry_available = False
            status.registry_error = self._registry_error
        return status

    def _probe_registry(self) -> None:
        """Record local-state failure without turning a scheduler status query into an error."""
        try:
            self.registry.preflight()
        except RegistryUnavailable as e:
            cause = e.details.get("cause")
            self._registry_error = f"{e.message}: {cause}" if cause else e.message

    # -- submit ---------------------------------------------------------------------------
    def submit(
        self,
        script: str | None = None,
        *,
        path: str | None = None,
        name: str | None = None,
        cwd: str | None = None,
        args: list[str] | None = None,
        template: str | None = None,
        force_preamble: bool = False,
        array: str | None = None,
        dependency: str | None = None,
        **options: Any,
    ) -> Job:
        """Submit a batch job.

        ``script`` is the script *content* (written remotely to ``script_dir``), or ``path`` an
        existing remote script. ``options`` become ``--key=value`` sbatch flags
        (``time="1:00:00"``, ``gpus_per_node=1``, ``exclusive=True``). Host defaults
        (``account``, ``partition``, ``defaults`` table) are applied when not overridden.

        ``template`` names a ``[hosts.X.templates.NAME]`` bundle: its options are merged
        (host defaults < template < explicit ``options``) and, for ``script=`` submissions,
        its preamble/epilogue wrap the body. A template with a non-empty preamble refuses a
        ``path=`` submission (existing remote script) unless ``force_preamble=True``, in which
        case only the options are applied.

        ``array`` (e.g. ``"0-9%4"``) submits a job array (``--array=`` flag) and marks the
        registry record so :meth:`job_status` rolls the tasks up; ``dependency`` (e.g.
        ``"afterok:123"``) becomes ``--dependency=``.
        """
        if (script is None) == (path is None):
            raise InvalidArgument("provide exactly one of script= or path=")
        # Refuse before the remote side effect if the local recovery handle cannot be persisted.
        self.registry.preflight()
        opts: dict[str, Any] = dict(self.host.defaults or {})
        if self.host.account:
            opts.setdefault("account", self.host.account)
        if self.host.partition:
            opts.setdefault("partition", self.host.partition)
        tmpl: Template | None = None
        if template is not None:
            tmpl = self.host.resolve_template(template)  # ConfigError if unknown/cyclic
            opts.update(tmpl.options)
        opts.update(options)
        if array is not None:
            opts["array"] = array
        if dependency is not None:
            opts["dependency"] = dependency
        if name:
            opts.setdefault("job_name", name)
        if tmpl is not None and path is not None and tmpl.preamble and not force_preamble:
            raise InvalidArgument(
                f"template {template!r} has a preamble but path= submissions cannot inject it",
                action="pass force_preamble=True to apply the template's options only, "
                "or submit the script content with script= instead",
            )
        if tmpl is not None and script is not None:
            script = compose_templated_script(
                script, tmpl, name=template or tmpl.name, merged_options=opts
            )
        flags = slurm.sbatch_args_from_options(opts) + list(args or [])
        res = self.call(
            "sbatch",
            _timeout=180,
            script=script,
            path=path,
            args=flags,
            cwd=cwd,
            name=name or opts.get("job_name"),
            script_dir=self.host.script_dir,
        )
        try:
            job_id = slurm.parse_sbatch_output(res["stdout"], res["stderr"], res["rc"])
        except SlurmError:
            # Remember durable policy rejections (walltime/account/QOS/…); transient errors
            # never match the allow-list, so nothing is written for them.
            append_learned_notes(self.host.name, slurm.match_policy_lines(res.get("stderr", "")))
            raise
        rec = JobRecord(
            job_id=job_id,
            name=name or opts.get("job_name"),
            script_path=res.get("script_path"),
            submit_time=time.time(),
            last_state="PENDING",
            last_seen=time.time(),
            sbatch_args=flags,
            meta={
                "template": template,
                "options": dict(opts),
                "array": array,
                "dependency": dependency,
            },
        )
        # Fill in stdout/stderr/workdir from scontrol (best effort; job may be gone already).
        try:
            sc = self.scontrol_job(job_id)
            rec.stdout_path = sc.get("StdOut") or None
            rec.stderr_path = sc.get("StdErr") or None
            rec.workdir = sc.get("WorkDir") or None
            rec.name = rec.name or sc.get("JobName")
        except RemoteSlurmError:
            pass
        recorded = True
        registry_error = None
        try:
            self.registry.put(rec)
        except OSError as e:
            # Slurm already accepted the job. Preserve that success and make recovery explicit;
            # raising here invites callers to repeat the submission and create a duplicate.
            recorded = False
            self._note_registry_error(e)
            registry_error = self.registry_error
        self.registry.audit("submit", job_id=job_id, script=rec.script_path, args=flags)
        self._invalidate_squeue()
        return Job(
            self,  # type: ignore[arg-type]
            job_id,
            recorded=recorded,
            registry_error=registry_error,
        )

    # -- raw queries -----------------------------------------------------------------------
    def _invalidate_squeue(self) -> None:
        with self._squeue_lock:
            self._squeue_cache = None

    def adopt(self, job_id: str) -> Job:
        """Reconstruct a local registry record for an existing scheduler job."""
        base, task = slurm.parse_job_id(job_id)
        self.registry.preflight()
        accounting = self.sacct([job_id], all_steps=True)
        acct = accounting.get(job_id) or accounting.get(base)
        control: dict[str, str] = {}
        try:
            control = self.scontrol_job(job_id)
        except SlurmError:
            pass
        if acct is None and not control:
            raise SlurmError(
                f"job {job_id} is not known to scontrol or sacct",
                job_id=job_id,
                action="check the job id and scheduler accounting retention",
            )
        owner = str(acct.get("user") or "") if acct else ""
        if control.get("UserId"):
            owner = control["UserId"].split("(", 1)[0]
        expected_owner = str(self.user)  # type: ignore[attr-defined]
        if owner and owner != expected_owner:
            raise PermissionDenied(
                f"job {job_id} belongs to {owner}, not {expected_owner}",
                job_id=job_id,
                owner=owner,
            )

        status = self.job_status(job_id, refresh=True)
        if status.source == "unknown":
            raise SlurmError(
                f"job {job_id} has no usable scheduler status",
                job_id=job_id,
                action="check scheduler accounting retention",
            )
        array = task is None and any(
            key.startswith(base + "_") for key in accounting if "." not in key
        )
        rec = JobRecord(
            job_id=job_id,
            name=status.name or control.get("JobName") or None,
            script_path=control.get("Command") or status.script_path,
            stdout_path=control.get("StdOut") or status.stdout_path,
            stderr_path=control.get("StdErr") or status.stderr_path,
            workdir=control.get("WorkDir") or status.workdir,
            submit_time=_slurm_timestamp(status.submit_time),
            last_state=status.state,
            last_seen=time.time(),
            meta={"adopted": True, "array": "adopted" if array else None},
        )
        self.registry.put(rec)
        self.registry.audit("adopt", job_id=job_id, source=status.source)
        self._registry_error = None
        return Job(self, job_id)  # type: ignore[arg-type]

    def squeue(self, *, refresh: bool = False, user: str | None = None) -> list[dict[str, str]]:
        """My queued/running jobs (cached ~10 s unless ``refresh``)."""
        if user is None:
            with self._squeue_lock:
                c = self._squeue_cache
                if c and not refresh and time.time() - c[0] < SQUEUE_CACHE_SECONDS:
                    return c[1]
        res = self.call("squeue", format=slurm.SQUEUE_FORMAT, user=user)
        if res["rc"] != 0:
            raise SlurmError("squeue failed: " + res["stderr"].strip(), rc=res["rc"])
        rows = slurm.parse_squeue(res["stdout"])
        if user is None:
            with self._squeue_lock:
                self._squeue_cache = (time.time(), rows)
        return rows

    def squeue_jobs(self, job_ids: list[str]) -> list[dict[str, str]]:
        res = self.call("squeue", format=slurm.SQUEUE_FORMAT, jobs=job_ids)
        if res["rc"] != 0:
            if "Invalid job id" in res["stderr"]:
                return []
            raise SlurmError("squeue failed: " + res["stderr"].strip(), rc=res["rc"])
        return slurm.parse_squeue(res["stdout"])

    def sacct(
        self,
        job_ids: list[str] | None = None,
        *,
        since: str | None = None,
        all_steps: bool = False,
    ) -> dict[str, dict[str, Any]]:
        """Accounting records keyed by job id (array tasks keyed ``<base>_<task>``).

        ``all_steps=True`` includes the per-step ``.batch``/``.extern`` rows, needed only to
        fold MaxRSS for a single job / ``diagnose``; the default (allocations only) is cheaper.
        """
        if not job_ids and not since:
            since = "now-7days"
        res = self.call(
            "sacct",
            _timeout=180,
            fields=slurm.SACCT_FIELDS,
            jobs=job_ids,
            since=since,
            all_steps=all_steps,
        )
        if res["rc"] != 0:
            raise SlurmError("sacct failed: " + res["stderr"].strip(), rc=res["rc"])
        return slurm.parse_sacct(res["stdout"])

    def scontrol_job(self, job_id: str) -> dict[str, str]:
        base, _ = slurm.parse_job_id(job_id)
        res = self.call("scontrol", what="job", id=job_id if "_" in job_id else base)
        if res["rc"] != 0:
            raise SlurmError(
                "scontrol failed: " + res["stderr"].strip(), rc=res["rc"], job_id=job_id
            )
        return slurm.parse_scontrol_job(res["stdout"])

    def sinfo(self) -> list[dict[str, Any]]:
        res = self.call("sinfo")
        if res["rc"] != 0:
            raise SlurmError("sinfo failed: " + res["stderr"].strip(), rc=res["rc"])
        return slurm.parse_sinfo(res["stdout"])

    # -- status -------------------------------------------------------------------------------
    def job_status(
        self,
        job_id: str,
        *,
        refresh: bool = False,
        _reset_registry_error: bool = True,
    ) -> slurm.JobStatus:
        """Merge squeue -> scontrol -> sacct -> registry into one status record.

        A bare array id (``123``) is rolled up across its tasks (see :meth:`_array_status`);
        a task id (``123_4``) is reported on its own.
        """
        if _reset_registry_error:
            self._registry_error = None
            self._probe_registry()
        base, task = slurm.parse_job_id(job_id)
        rec = self._registry_get(job_id)
        st: slurm.JobStatus | None = None

        all_rows = self.squeue(refresh=refresh)
        if task is None and self._is_array(base, all_rows, rec):
            return self._mark_registry_status(self._array_status(base, all_rows))

        rows = [r for r in all_rows if r["job_id"] == job_id]
        if not rows and refresh:
            rows = self.squeue_jobs([job_id])
        if rows:
            r = rows[0]
            st = slurm.JobStatus(
                job_id=job_id,
                state=r["state"],
                source="squeue",
                name=r["name"] or None,
                reason=(r["reason"] if r["reason"] not in ("None", "") else None),
                elapsed=r["time_used"] or None,
                time_limit=r["time_limit"] or None,
                nodelist=(
                    r["nodelist"] if r["nodelist"] and not r["nodelist"].startswith("(") else None
                ),
                partition=r["partition"] or None,
                account=r["account"] or None,
                submit_time=r["submit_time"] or None,
                start_time=(r["start_time"] if r["start_time"] not in ("N/A", "") else None),
                workdir=r["workdir"] or None,
            )
        if st is None:
            found = self.sacct([job_id], all_steps=True)
            acct = found.get(job_id.split("_")[0]) or found.get(job_id)
            if acct:
                st = slurm.JobStatus(
                    job_id=job_id,
                    state=acct["state"],
                    source="sacct",
                    name=acct.get("name") or None,
                    exit_code=slurm.exit_code_int(acct.get("exit_code")),
                    exit_code_raw=acct.get("exit_code"),
                    elapsed=acct.get("elapsed") or None,
                    time_limit=acct.get("time_limit") or None,
                    nodelist=acct.get("nodelist") or None,
                    partition=acct.get("partition") or None,
                    account=acct.get("account") or None,
                    submit_time=acct.get("submit_time") or None,
                    start_time=acct.get("start_time") or None,
                    end_time=acct.get("end_time") or None,
                    max_rss=acct.get("max_rss"),
                    workdir=acct.get("workdir") or None,
                    extra={"steps": acct.get("steps", [])},
                )
        if st is None:
            try:
                sc = self.scontrol_job(job_id)
                st = slurm.JobStatus(
                    job_id=job_id,
                    state=slurm.normalize_state(sc.get("JobState", "UNKNOWN")),
                    source="scontrol",
                    name=sc.get("JobName"),
                    reason=(sc.get("Reason") if sc.get("Reason") not in ("None", "") else None),
                    exit_code=slurm.exit_code_int(sc.get("ExitCode")),
                    exit_code_raw=sc.get("ExitCode"),
                    elapsed=sc.get("RunTime"),
                    time_limit=sc.get("TimeLimit"),
                    nodelist=sc.get("NodeList")
                    if sc.get("NodeList") not in ("(null)", "")
                    else None,
                    partition=sc.get("Partition"),
                    account=sc.get("Account"),
                    submit_time=sc.get("SubmitTime"),
                    start_time=sc.get("StartTime") if sc.get("StartTime") != "Unknown" else None,
                    end_time=sc.get("EndTime") if sc.get("EndTime") != "Unknown" else None,
                    workdir=sc.get("WorkDir"),
                    stdout_path=sc.get("StdOut") or None,
                    stderr_path=sc.get("StdErr") or None,
                )
            except SlurmError:
                pass
        if st is None:
            # Not visible anywhere. Within the grace window after last sighting, report pending
            # accounting rather than "unknown".
            pending = bool(rec) and (time.time() - rec.last_seen) < ACCOUNTING_GRACE_SECONDS  # type: ignore[union-attr]
            st = slurm.JobStatus(
                job_id=job_id,
                state=(rec.last_state if rec and pending else "UNKNOWN") or "UNKNOWN",
                source="registry" if rec else "unknown",
                accounting_pending=pending,
            )
        if rec:
            st.stdout_path = st.stdout_path or rec.stdout_path
            st.stderr_path = st.stderr_path or rec.stderr_path
            st.script_path = rec.script_path
            st.workdir = st.workdir or rec.workdir
            st.name = st.name or rec.name
            if st.source != "registry":
                self._registry_update(job_id, last_state=st.state, last_seen=time.time())
        # An array task inherits the parent's registered paths (the ``%A_%a`` output template,
        # script, workdir) and, for sweeps, surfaces its row of parameters.
        if task is not None and task.isdigit():
            parent = self._registry_get(base)
            if parent is not None:
                st.stdout_path = st.stdout_path or parent.stdout_path
                st.stderr_path = st.stderr_path or parent.stderr_path
                st.script_path = st.script_path or parent.script_path
                st.workdir = st.workdir or parent.workdir
                st.name = st.name or parent.name
                if parent.meta.get("sweep"):
                    params = self._sweep_params_for(base, int(task))
                    if params is not None:
                        st.extra["params"] = params
        st.terminal = slurm.is_terminal(st.state) and not st.accounting_pending
        return self._mark_registry_status(st)

    # -- arrays -------------------------------------------------------------------------------
    def _is_array(
        self,
        base: str,
        squeue_rows: list[dict[str, str]],
        rec: JobRecord | None = None,
    ) -> bool:
        """Is ``base`` a job array? True if the registry marked it, or squeue shows tasks."""
        if rec is not None and rec.meta.get("array"):
            return True
        if any(r.get("array_base") == base for r in squeue_rows):
            return True
        return False

    def _array_status(self, base: str, squeue_rows: list[dict[str, str]]) -> slurm.JobStatus:
        """Roll an array's tasks up into one :class:`slurm.JobStatus` (state + ``extra``)."""
        sacct_jobs = self.sacct([base])  # per-task allocations (states); steps not needed
        agg = slurm.aggregate_array(base, squeue_rows, sacct_jobs)
        # No task visible in squeue or sacct: the array has aged out of accounting. Report it as
        # unknown (not a perpetual non-terminal "array") so wait() stops instead of looping.
        source = "array" if agg["task_states"] else "unknown"
        terminal = agg["terminal"] or source == "unknown"
        st = slurm.JobStatus(
            job_id=base,
            state=agg["state"],
            source=source,
            terminal=terminal,
            extra={
                "array": True,
                "n_tasks": len(agg["task_states"]),
                "tasks": agg["tasks"],
                "failed_tasks": agg["failed_tasks"],
                "task_states": agg["task_states"],
            },
        )
        rec = self._registry_get(base)
        if rec:
            st.name = rec.name
            st.stdout_path = rec.stdout_path
            st.stderr_path = rec.stderr_path
            st.script_path = rec.script_path
            st.workdir = rec.workdir
            if rec.meta.get("sweep"):
                st.extra["sweep"] = rec.meta["sweep"]
                failed_params: dict[int, dict[str, Any]] = {}
                for t in agg["failed_tasks"]:
                    p = self._sweep_params_for(base, t)
                    if p is not None:
                        failed_params[t] = p
                if failed_params:
                    st.extra["failed_task_params"] = failed_params
            self._registry_update(base, last_state=agg["state"], last_seen=time.time())
        return st

    def _sweep_params_for(self, base: str, task: int) -> dict[str, Any] | None:
        """Return the sweep parameters for array ``base`` task ``task`` (cached read of the TSV)."""
        rec = self._registry_get(base)
        sweep = rec.meta.get("sweep") if rec else None
        if not sweep:
            return None
        path = sweep.get("params_path")
        if not path:
            return None
        if self._sweep_params_cache is None:
            self._sweep_params_cache = {}
        rowlist = self._sweep_params_cache.get(path)
        if rowlist is None:
            try:
                text = self.read(path, max_bytes=8 * 1024 * 1024).get("content", "")
            except SlurmError:
                return None
            lines = [ln for ln in str(text).splitlines() if ln != ""]
            if not lines:
                return None
            header = lines[0].split("\t")
            rowlist = [dict(zip(header, ln.split("\t"), strict=False)) for ln in lines[1:]]
            self._sweep_params_cache[path] = rowlist
        if 0 <= task < len(rowlist):
            return rowlist[task]
        return None

    def jobs(
        self, *, include_finished: bool = True, refresh: bool = False
    ) -> list[slurm.JobStatus]:
        """All jobs: my live queue plus registry-known jobs (with their last/terminal state).

        A job array shows up as one row keyed by its base id (rolled up across tasks).
        """
        # Opportunistic housekeeping: drop long-finished records (throttled to once/hour via a
        # timestamp in the registry file). Never let it break a listing.
        self._registry_error = None
        try:
            self.registry.prune()
        except OSError as e:
            self._note_registry_error(e)
        out: dict[str, slurm.JobStatus] = {}
        for r in self.squeue(refresh=refresh):
            jid = r["array_base"] or r["job_id"]  # arrays roll up under the base id
            if jid not in out:
                out[jid] = self.job_status(jid, _reset_registry_error=False)
        if include_finished:
            known = [r.job_id for r in self._registry_all() if r.job_id not in out]
            if known:
                acct = self.sacct(known) if known else {}
                for jid in known:
                    rec = self._registry_get(jid)
                    a = acct.get(jid)
                    if a:
                        st = slurm.JobStatus(
                            job_id=jid,
                            state=a["state"],
                            source="sacct",
                            name=a.get("name") or (rec.name if rec else None),
                            exit_code=slurm.exit_code_int(a.get("exit_code")),
                            exit_code_raw=a.get("exit_code"),
                            elapsed=a.get("elapsed") or None,
                            nodelist=a.get("nodelist") or None,
                            partition=a.get("partition") or None,
                            start_time=a.get("start_time") or None,
                            end_time=a.get("end_time") or None,
                            max_rss=a.get("max_rss"),
                            workdir=a.get("workdir") or None,
                            stdout_path=rec.stdout_path if rec else None,
                            stderr_path=rec.stderr_path if rec else None,
                            script_path=rec.script_path if rec else None,
                        )
                        st.terminal = slurm.is_terminal(st.state)
                        self._registry_update(jid, last_state=st.state, last_seen=time.time())
                    else:
                        st = self.job_status(jid, _reset_registry_error=False)
                    out[jid] = st
        rows = sorted(out.values(), key=lambda s: int(s.job_id.split("_")[0]))
        if self._registry_error:
            for status in rows:
                self._mark_registry_status(status)
        return rows

    def wait(
        self,
        job_id: str,
        *,
        poll: float = 15.0,
        timeout: float | None = None,
        callback: Any = None,
    ) -> slurm.JobStatus:
        """Block until the job reaches a terminal state (polling with the squeue cache)."""
        t0 = time.time()
        poll = max(5.0, poll)
        while True:
            st = self.job_status(job_id, refresh=True)
            if callback:
                callback(st)
            if st.terminal:
                return st
            if st.source == "unknown":
                raise SlurmError(
                    f"job {job_id} is not known to squeue, scontrol or sacct",
                    job_id=job_id,
                    action="check the job id; if it just finished, accounting may be delayed",
                )
            if timeout is not None and time.time() - t0 > timeout:
                raise RemoteTimeout(
                    f"job {job_id} not finished after {timeout}s (state {st.state})",
                    job_id=job_id,
                    state=st.state,
                )
            time.sleep(poll)

    def cancel(self, job_id: str | list[str], *, confirm: bool = False) -> dict[str, Any]:
        ids = [job_id] if isinstance(job_id, str) else list(job_id)
        for j in ids:
            slurm.parse_job_id(j)
        if "cancel" in self.host.confirm and not confirm:
            raise ConfirmationRequired(
                f"cancel needs confirmation on host {self.host.name}: cancel {', '.join(ids)}",
                what=f"cancel {', '.join(ids)}",
                op="cancel",
            )
        res = self.call("scancel", jobs=ids)
        self.registry.audit("cancel", jobs=ids, result=res)
        self._invalidate_squeue()
        if res.get("rc", 0) != 0:
            raise SlurmError("scancel failed: " + str(res.get("stderr", "")).strip(), **res)
        return res

    def job_output(
        self, job_id: str, *, tail: int | None = 100, max_bytes: int = 65536, stream: str = "stdout"
    ) -> dict[str, Any]:
        """Read the job's stdout (or stderr) file, resolving its path via status/registry.

        Slurm output patterns are expanded (``%A`` array job id, ``%a`` task id, ``%j``/``%J``
        job id, ``%x`` name, ``%u`` user, ``%N`` node). For a bare array id the output is read
        from a representative task (the first failed one, else the lowest task id).
        """
        base, task = slurm.parse_job_id(job_id)
        st = self.job_status(job_id)
        # A bare array parent: pick a representative task to read.
        if task is None and st.extra.get("array"):
            failed = st.extra.get("failed_tasks") or []
            states = st.extra.get("task_states") or {}
            pick = failed[0] if failed else (min(states) if states else 0)
            task = str(pick)
        path = st.stdout_path if stream == "stdout" else (st.stderr_path or st.stdout_path)
        # Fall back to scontrol for a task file when the merged status has no resolved path.
        if (not path or "%a" in path) and task is not None:
            try:
                sc = self.scontrol_job(f"{base}_{task}")
                p = (
                    sc.get("StdOut")
                    if stream == "stdout"
                    else (sc.get("StdErr") or sc.get("StdOut"))
                )
                if p:
                    path = p
            except SlurmError:
                pass
        if not path:
            raise SlurmError(
                f"no {stream} path known for job {job_id}", job_id=job_id, state=st.state
            )
        path = self._expand_output_path(path, base, task, st)
        if "%a" in path:  # an array file we could not pin to a task
            raise SlurmError(
                f"job {job_id} is an array; specify a task id (e.g. {base}_0)",
                job_id=job_id,
                state=st.state,
            )
        r = self.read(path, tail=tail, max_bytes=max_bytes)
        r["job_id"] = job_id
        r["state"] = st.state
        return r

    def _expand_output_path(
        self, path: str, base: str, task: str | None, st: slurm.JobStatus
    ) -> str:
        """Expand Slurm output specifiers in ``path`` (``%A %a %j %J %x %u %N``)."""
        node = (st.nodelist or "").split(",")[0]
        try:
            user = str(self.user)  # type: ignore[attr-defined]
        except (RemoteSlurmError, AttributeError):
            user = ""
        # A job name may itself contain "%" or "/"; sanitize so it cannot inject another
        # specifier or redirect the read to a different path.
        jobname = re.sub(r"[%/]", "_", st.name or "")
        mapping = {
            "A": base,
            "J": f"{base}_{task}" if task is not None else base,
            "j": base,
            "x": jobname,
            "u": user,
            "N": node,
            "a": task if task is not None else "%a",
        }
        # single pass: each %X is replaced exactly once, from the original string
        return re.sub(r"%([AaJjxuN])", lambda m: mapping[m.group(1)], path)

    # -- packed jobs / sweeps -----------------------------------------------------------------
    def pack(
        self,
        commands: list[str],
        *,
        max_processes: int = 1,
        batches: int = 1,
        max_concurrent: int | None = None,
        dependency: str | None = None,
        template: str | None = None,
        name: str = "pack",
        cwd: str | None = None,
        **options: Any,
    ) -> Job:
        """Submit independent shell commands packed onto one-node allocations.

        Commands are stored remotely one per line. The job is an array of ``batches`` one-node,
        one-task allocations; each array task takes a contiguous slice and runs GNU Parallel
        with at most ``max_processes`` children. ``max_concurrent`` separately throttles how
        many packed allocations Slurm may run at once. Unless host defaults, a template, or an
        explicit option specifies CPUs per task, the allocation requests one CPU per concurrent
        child. GNU Parallel must be available in the job environment.
        """
        clean = packed_commands(commands)
        for label, value in (("max_processes", max_processes), ("batches", batches)):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise InvalidArgument(f"{label} must be a positive integer")
        if batches > len(clean):
            raise InvalidArgument(
                f"batches ({batches}) exceeds the number of commands ({len(clean)})",
                action="reduce batches so every allocation receives at least one command",
            )
        if batches > MAX_PACK_BATCHES:
            raise InvalidArgument(
                f"packed job would create {batches} array tasks (limit {MAX_PACK_BATCHES})",
                action="reduce batches or split the command list",
            )
        if max_concurrent is not None and (
            isinstance(max_concurrent, bool)
            or not isinstance(max_concurrent, int)
            or max_concurrent <= 0
        ):
            raise InvalidArgument("max_concurrent must be a positive integer")

        # Do not create remote support files unless the later scheduler handle can be saved.
        self.registry.preflight()

        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)[:48] or "pack"
        stamp = time.strftime("%Y%m%d-%H%M%S")
        base_dir = (self.host.script_dir or "~/.remoteslurm/packs").rstrip("/")
        pack_dir = f"{base_dir}/{safe}-{stamp}-{os.getpid()}"
        written = self.write(f"{pack_dir}/commands.txt", "\n".join(clean) + "\n")
        commands_path = written["path"]
        wrapper = pack_wrapper(
            commands_path,
            name,
            total=len(clean),
            batches=batches,
            max_processes=max_processes,
        )
        spec = f"0-{batches - 1}"
        if max_concurrent is not None:
            spec += f"%{max_concurrent}"

        pack_options = dict(options)
        known_options: dict[str, Any] = dict(self.host.defaults or {})
        if template is not None:
            known_options.update(self.host.resolve_template(template).options)
        known_options.update(pack_options)
        if "cpus_per_task" not in known_options and "cpus" not in known_options:
            pack_options["cpus_per_task"] = max_processes
        # Each array element is one allocation on one node. GNU Parallel, not Slurm task
        # fan-out, owns concurrency inside it.
        pack_options["nodes"] = 1
        pack_options["ntasks"] = 1

        job = self.submit(
            script=wrapper,
            name=name,
            cwd=cwd,
            template=template,
            array=spec,
            dependency=dependency,
            **pack_options,
        )
        pack_meta = {
            "commands_path": commands_path,
            "n": len(clean),
            "batches": batches,
            "max_processes": max_processes,
        }
        job.metadata["pack"] = pack_meta
        rec = self._registry_get(job.job_id) if job.recorded else None
        if rec is not None:
            meta = dict(rec.meta)
            meta["pack"] = pack_meta
            if not self._registry_update(job.job_id, meta=meta):
                job.recorded = False
                job.registry_error = self.registry_error
        self.registry.audit(
            "pack",
            job_id=job.job_id,
            n=len(clean),
            batches=batches,
            max_processes=max_processes,
            commands_path=commands_path,
        )
        return job

    def sweep(
        self,
        params: dict[str, list[Any]] | list[dict[str, Any]],
        *,
        script: str | None = None,
        path: str | None = None,
        template: str | None = None,
        name: str = "sweep",
        max_concurrent: int | None = None,
        cwd: str | None = None,
        **options: Any,
    ) -> Job:
        """Submit a parameter sweep as one job array.

        ``params`` is a dict ``{name: [values]}`` (Cartesian product) or a list of explicit
        row dicts. A ``params.tsv`` and a wrapper script are written remotely; the wrapper
        reads its ``$SLURM_ARRAY_TASK_ID`` row, exports ``RS_PARAM_<NAME>`` for each column and
        ``RS_PARAMS_JSON`` for the whole row, then runs ``script`` (content) or ``path`` (an
        existing remote script, via ``exec bash``). Submitted as ``--array=0-(n-1)[%N]``; the
        registry records ``meta["sweep"]`` so ``status``/``diagnose`` surface each task's params.
        """
        if (script is None) == (path is None):
            raise InvalidArgument("provide exactly one of script= or path=")
        names, rows = sweep_rows(params)
        n = len(rows)
        if n == 0:
            raise InvalidArgument("sweep has no parameter rows")
        if n > MAX_SWEEP_TASKS:
            raise InvalidArgument(
                f"sweep would create {n} tasks (limit {MAX_SWEEP_TASKS}); "
                "Slurm's MaxArraySize is typically ~1000",
                action="reduce the parameter grid or split the sweep",
            )
        # The parameter file is part of the submission. Check local recovery state first.
        self.registry.preflight()
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)[:48] or "sweep"
        stamp = time.strftime("%Y%m%d-%H%M%S")
        base_dir = (self.host.script_dir or "~/.remoteslurm/sweeps").rstrip("/")
        sweep_dir = f"{base_dir}/{safe}-{stamp}-{os.getpid()}"
        w = self.write(f"{sweep_dir}/params.tsv", params_tsv(names, rows))
        params_path = w["path"]
        wrapper = sweep_wrapper(params_path, name, body=script, remote_path=path)
        spec = f"0-{n - 1}"
        if max_concurrent:
            spec += f"%{int(max_concurrent)}"
        job = self.submit(
            script=wrapper,
            name=name,
            cwd=cwd,
            template=template,
            array=spec,
            **options,
        )
        sweep_meta = {"params_path": params_path, "n": n, "names": names}
        job.metadata["sweep"] = sweep_meta
        rec = self._registry_get(job.job_id) if job.recorded else None
        if rec is not None:
            meta = dict(rec.meta)
            meta["sweep"] = sweep_meta
            if not self._registry_update(job.job_id, meta=meta):
                job.recorded = False
                job.registry_error = self.registry_error
        self.registry.audit("sweep", job_id=job.job_id, n=n, params_path=params_path, names=names)
        return job
