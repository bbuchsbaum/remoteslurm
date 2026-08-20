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
from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import slurm
from .config import Template, state_dir
from .errors import InvalidArgument, RemoteSlurmError, RemoteTimeout, SlurmError

if TYPE_CHECKING:
    from .cluster import Cluster

SQUEUE_CACHE_SECONDS = 10.0
ACCOUNTING_GRACE_SECONDS = 600.0  # how long after last sighting we report "accounting_pending"
MAX_SWEEP_TASKS = 1000  # guard: refuse sweeps larger than a typical Slurm MaxArraySize
LEARNED_NOTES_CAP = 50


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

    def _read_file(self) -> dict[str, JobRecord]:
        jobs: dict[str, JobRecord] = {}
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text("utf-8"))
                for d in data.get("jobs", []):
                    known = {k: v for k, v in d.items() if k in JobRecord.__dataclass_fields__}
                    jobs[known["job_id"]] = JobRecord(**known)
            except (OSError, ValueError, TypeError, KeyError):
                pass
        return jobs

    def _write_file(self, jobs: dict[str, JobRecord]) -> None:
        tmp = self.path.with_name(f"jobs.{os.getpid()}.{threading.get_ident()}.tmp")
        payload = {"jobs": [asdict(j) for j in jobs.values()]}
        tmp.write_text(json.dumps(payload, indent=1), "utf-8")
        os.replace(tmp, self.path)

    @contextmanager
    def _locked(self) -> Iterator[dict[str, JobRecord]]:
        """Yield the on-disk records under an exclusive lock; write back on exit."""
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path.with_suffix(".lock"), "w") as lf:
                fcntl.flock(lf, fcntl.LOCK_EX)
                try:
                    jobs = self._read_file()
                    yield jobs
                    self._write_file(jobs)
                finally:
                    fcntl.flock(lf, fcntl.LOCK_UN)

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

    def __init__(self, cluster: Cluster, job_id: str) -> None:
        self.cluster = cluster
        self.job_id = job_id

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
        except SlurmError:
            pass
        self.registry.put(rec)
        self.registry.audit("submit", job_id=job_id, script=rec.script_path, args=flags)
        self._invalidate_squeue()
        return Job(self, job_id)  # type: ignore[arg-type]

    # -- raw queries -----------------------------------------------------------------------
    def _invalidate_squeue(self) -> None:
        with self._squeue_lock:
            self._squeue_cache = None

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
    def job_status(self, job_id: str, *, refresh: bool = False) -> slurm.JobStatus:
        """Merge squeue -> scontrol -> sacct -> registry into one status record.

        A bare array id (``123``) is rolled up across its tasks (see :meth:`_array_status`);
        a task id (``123_4``) is reported on its own.
        """
        base, task = slurm.parse_job_id(job_id)
        rec = self.registry.get(job_id)
        st: slurm.JobStatus | None = None

        all_rows = self.squeue(refresh=refresh)
        if task is None and self._is_array(base, all_rows, rec):
            return self._array_status(base, all_rows)

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
                self.registry.update(job_id, last_state=st.state, last_seen=time.time())
        # An array task inherits the parent's registered paths (the ``%A_%a`` output template,
        # script, workdir) and, for sweeps, surfaces its row of parameters.
        if task is not None and task.isdigit():
            parent = self.registry.get(base)
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
        return st

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
        rec = self.registry.get(base)
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
            self.registry.update(base, last_state=agg["state"], last_seen=time.time())
        return st

    def _sweep_params_for(self, base: str, task: int) -> dict[str, Any] | None:
        """Return the sweep parameters for array ``base`` task ``task`` (cached read of the TSV)."""
        rec = self.registry.get(base)
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
        out: dict[str, slurm.JobStatus] = {}
        for r in self.squeue(refresh=refresh):
            jid = r["array_base"] or r["job_id"]  # arrays roll up under the base id
            if jid not in out:
                out[jid] = self.job_status(jid)
        if include_finished:
            known = [r.job_id for r in self.registry.all() if r.job_id not in out]
            if known:
                acct = self.sacct(known) if known else {}
                for jid in known:
                    rec = self.registry.get(jid)
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
                        self.registry.update(jid, last_state=st.state, last_seen=time.time())
                    else:
                        st = self.job_status(jid)
                    out[jid] = st
        return sorted(out.values(), key=lambda s: int(s.job_id.split("_")[0]))

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

    def cancel(self, job_id: str | list[str]) -> dict[str, Any]:
        ids = [job_id] if isinstance(job_id, str) else list(job_id)
        for j in ids:
            slurm.parse_job_id(j)
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

    # -- sweeps -------------------------------------------------------------------------------
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
        rec = self.registry.get(job.job_id)
        if rec is not None:
            meta = dict(rec.meta)
            meta["sweep"] = {"params_path": params_path, "n": n, "names": names}
            self.registry.update(job.job_id, meta=meta)
        self.registry.audit("sweep", job_id=job.job_id, n=n, params_path=params_path, names=names)
        return job
