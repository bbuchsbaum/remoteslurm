"""Job handles, the local job registry, and the Slurm operations mixed into ``Cluster``."""

from __future__ import annotations

import fcntl
import json
import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import slurm
from .config import Template, state_dir
from .errors import InvalidArgument, RemoteTimeout, SlurmError

if TYPE_CHECKING:
    from .cluster import Cluster

SQUEUE_CACHE_SECONDS = 10.0
ACCOUNTING_GRACE_SECONDS = 600.0  # how long after last sighting we report "accounting_pending"
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
            meta={"template": template, "options": dict(opts)},
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
        self, job_ids: list[str] | None = None, *, since: str | None = None
    ) -> dict[str, dict[str, Any]]:
        if not job_ids and not since:
            since = "now-7days"
        res = self.call("sacct", _timeout=180, fields=slurm.SACCT_FIELDS, jobs=job_ids, since=since)
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
        """Merge squeue -> scontrol -> sacct -> registry into one status record."""
        slurm.parse_job_id(job_id)
        rec = self.registry.get(job_id)
        st: slurm.JobStatus | None = None

        rows = [r for r in self.squeue(refresh=refresh) if r["job_id"] == job_id]
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
            found = self.sacct([job_id])
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
        st.terminal = slurm.is_terminal(st.state) and not st.accounting_pending
        return st

    def jobs(
        self, *, include_finished: bool = True, refresh: bool = False
    ) -> list[slurm.JobStatus]:
        """All jobs: my live queue plus registry-known jobs (with their last/terminal state)."""
        out: dict[str, slurm.JobStatus] = {}
        for jid in [r["job_id"] for r in self.squeue(refresh=refresh)]:
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
        """Read the job's stdout (or stderr) file, resolving its path via status/registry."""
        st = self.job_status(job_id)
        path = st.stdout_path if stream == "stdout" else (st.stderr_path or st.stdout_path)
        if not path:
            raise SlurmError(
                f"no {stream} path known for job {job_id}", job_id=job_id, state=st.state
            )
        path = path.replace("%j", job_id.split("_")[0]).replace("%J", job_id)
        r = self.read(path, tail=tail, max_bytes=max_bytes)
        r["job_id"] = job_id
        r["state"] = st.state
        return r
