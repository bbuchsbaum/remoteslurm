"""Slurm command formats and output parsers (pure functions; tested against fixtures)."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

from .errors import SlurmError

# squeue --format fields (order matters; parsed positionally). %Z = WorkDir, %V = SubmitTime.
SQUEUE_FIELDS = [
    "job_id",
    "name",
    "state",
    "reason",
    "partition",
    "account",
    "time_used",
    "time_limit",
    "nodes",
    "nodelist",
    "submit_time",
    "start_time",
    "workdir",
    "user",
]
SQUEUE_FORMAT = "%i|%j|%T|%r|%P|%a|%M|%l|%D|%R|%V|%S|%Z|%u"

SACCT_FIELDS = [
    "JobID",
    "JobName",
    "State",
    "ExitCode",
    "Elapsed",
    "MaxRSS",
    "NodeList",
    "Start",
    "End",
    "Submit",
    "Partition",
    "Account",
    "AllocCPUS",
    "ReqMem",
    "WorkDir",
    "Timelimit",
    "User",
]

ACTIVE_STATES = {
    "PENDING",
    "RUNNING",
    "SUSPENDED",
    "COMPLETING",
    "CONFIGURING",
    "RESIZING",
    "REQUEUED",
    "REQUEUE_HOLD",
    "REQUEUE_FED",
    "SIGNALING",
    "STAGE_OUT",
    "STOPPED",
    "RESV_DEL_HOLD",
    "SPECIAL_EXIT",
    "PREEMPTED_HOLD",
}
TERMINAL_STATES = {
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "NODE_FAIL",
    "PREEMPTED",
    "BOOT_FAIL",
    "DEADLINE",
    "OUT_OF_MEMORY",
    "REVOKED",
}

JOB_ID_RE = re.compile(r"^(\d+)(?:_(\d+|\[[\d,\-%:]+\]))?(?:\+(\d+))?$")

# sbatch stderr lines matching one of these are *policy* rejections worth remembering
# (appended to the host's learned_notes.txt). Transient controller/network errors never match.
LEARNED_NOTE_PATTERNS = [
    r"Walltime must be",
    r"Invalid account",
    r"Invalid partition",
    r"QOS",
    r"exceeds .* limit",
    r"not permitted",
    r"Requested node configuration is not available",
]


def match_policy_lines(stderr: str) -> list[str]:
    """Return the stripped stderr lines that look like a durable Slurm *policy* rejection."""
    out: list[str] = []
    for line in (stderr or "").splitlines():
        line = line.strip()
        if line and any(re.search(p, line) for p in LEARNED_NOTE_PATTERNS):
            out.append(line)
    return out


def normalize_state(s: str) -> str:
    """``CANCELLED by 12345`` -> ``CANCELLED``; strip trailing ``+`` etc."""
    s = s.strip()
    if not s:
        return "UNKNOWN"
    return s.split()[0].rstrip("+").upper()


def is_terminal(state: str) -> bool:
    return normalize_state(state) in TERMINAL_STATES


def parse_job_id(s: str) -> tuple[str, str | None]:
    """Return ``(base_id, array_task)``; raises SlurmError on malformed ids."""
    m = JOB_ID_RE.match(s.strip())
    if not m:
        raise SlurmError(f"malformed job id: {s!r}")
    if m.group(3):
        raise SlurmError(f"heterogeneous job components are not supported: {s!r}")
    return m.group(1), m.group(2)


def parse_sbatch_output(stdout: str, stderr: str, rc: int) -> str:
    if rc != 0:
        raise SlurmError(
            "sbatch failed: " + (stderr.strip() or stdout.strip() or f"exit code {rc}"),
            rc=rc,
            stderr=stderr.strip(),
        )
    line = stdout.strip().splitlines()[-1] if stdout.strip() else ""
    jid = line.split(";")[0].strip()  # --parsable: "<jobid>[;<cluster>]"
    if not jid or not JOB_ID_RE.match(jid):
        raise SlurmError(f"could not parse job id from sbatch output: {stdout!r}", stderr=stderr)
    return jid


def parse_squeue(stdout: str) -> list[dict[str, str]]:
    rows = []
    for line in stdout.splitlines():
        line = line.rstrip("\n")
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) < len(SQUEUE_FIELDS):
            parts += [""] * (len(SQUEUE_FIELDS) - len(parts))
        row = dict(zip(SQUEUE_FIELDS, parts, strict=False))
        row["state"] = normalize_state(row["state"])
        rows.append(row)
    return rows


def _parse_mem(s: str) -> int | None:
    """'15848K' -> bytes; '' -> None."""
    s = s.strip()
    if not s:
        return None
    m = re.match(r"^([\d.]+)([KMGTP]?)[nc]?$", s, re.I)
    if not m:
        return None
    mult = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5}
    return int(float(m.group(1)) * mult[m.group(2).upper()])


def parse_sacct(stdout: str) -> dict[str, dict[str, Any]]:
    """Parse ``sacct -n -P`` output into ``{job_id: record}`` folding job steps.

    Step rows (``123.batch``, ``123.extern``, ``123.0``) contribute ``max_rss`` (max across
    steps) and, for the batch step, ``batch_exit_code``; the main row provides the rest.
    """
    jobs: dict[str, dict[str, Any]] = {}
    for line in stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) < len(SACCT_FIELDS):
            parts += [""] * (len(SACCT_FIELDS) - len(parts))
        rec = dict(zip([f.lower() for f in SACCT_FIELDS], parts, strict=False))
        jid = rec["jobid"]
        base, _, step = jid.partition(".")
        rss = _parse_mem(rec["maxrss"])
        if step:
            job = jobs.setdefault(base, {"job_id": base, "steps": []})
            job["steps"].append(
                {"step": step, "state": normalize_state(rec["state"]), "max_rss": rss}
            )
            if rss is not None and (job.get("max_rss") is None or rss > job["max_rss"]):
                job["max_rss"] = rss
            if step == "batch":
                job["batch_exit_code"] = rec["exitcode"]
            continue
        job = jobs.setdefault(base, {"job_id": base, "steps": []})
        job.update(
            {
                "name": rec["jobname"],
                "state": normalize_state(rec["state"]),
                "state_raw": rec["state"],
                "exit_code": rec["exitcode"],
                "elapsed": rec["elapsed"],
                "nodelist": rec["nodelist"],
                "start_time": rec["start"],
                "end_time": rec["end"],
                "submit_time": rec["submit"],
                "partition": rec["partition"],
                "account": rec["account"],
                "alloc_cpus": rec["alloccpus"],
                "req_mem": rec["reqmem"],
                "workdir": rec["workdir"],
                "time_limit": rec["timelimit"],
                "user": rec["user"],
            }
        )
        if rss is not None and (job.get("max_rss") is None or rss > job["max_rss"]):
            job["max_rss"] = rss
    for job in jobs.values():
        job.setdefault("max_rss", None)
    return jobs


_SCONTROL_KV = re.compile(r"(\S+?)=(.*?)(?=\s+\S+?=|$)")


def parse_scontrol_job(stdout: str) -> dict[str, str]:
    """Parse ``scontrol -o show job`` (one line of ``Key=Value`` pairs)."""
    text = " ".join(stdout.split())
    out: dict[str, str] = {}
    for m in _SCONTROL_KV.finditer(text):
        out[m.group(1)] = m.group(2)
    if "JobId" not in out:
        raise SlurmError("could not parse scontrol output", raw=stdout[:500])
    return out


def parse_sinfo(stdout: str) -> list[dict[str, Any]]:
    """``sinfo -h -o "%P|%a|%l|%D|%T|%c|%m|%G"`` -> per partition summary."""
    parts: dict[str, dict[str, Any]] = {}
    for line in stdout.splitlines():
        if not line.strip():
            continue
        f = line.split("|")
        if len(f) < 5:
            continue
        name = f[0].rstrip("*")
        p = parts.setdefault(
            name,
            {
                "partition": name,
                "default": f[0].endswith("*"),
                "avail": f[1],
                "time_limit": f[2],
                "nodes": {},
                "cpus_per_node": f[5] if len(f) > 5 else None,
                "mem_per_node_mb": f[6] if len(f) > 6 else None,
                "gres": (f[7] if len(f) > 7 and f[7] not in ("(null)", "") else None),
            },
        )
        state = f[4].rstrip("*$~#!%@^-")
        try:
            n = int(f[3])
        except ValueError:
            n = 0
        p["nodes"][state] = p["nodes"].get(state, 0) + n
    for p in parts.values():
        p["nodes_total"] = sum(p["nodes"].values())
        p["nodes_idle"] = p["nodes"].get("idle", 0)
    return list(parts.values())


def exit_code_int(code: str | None) -> int | None:
    """'0:0' -> 0 ; '1:0' -> 1 ; '0:9' (signal) -> 128+9."""
    if not code:
        return None
    try:
        rc, sig = (int(x) for x in code.split(":")[:2])
    except ValueError:
        return None
    return rc if sig == 0 else 128 + sig


def sbatch_args_from_options(options: dict[str, Any]) -> list[str]:
    """``{"time": "1:00:00", "gpus_per_node": 1, "exclusive": True}`` -> sbatch argv flags."""
    out: list[str] = []
    for k, v in options.items():
        if v is None or v is False:
            continue
        flag = "--" + k.replace("_", "-")
        if v is True:
            out.append(flag)
        else:
            out.append(f"{flag}={v}")
    return out


@dataclass
class JobStatus:
    job_id: str
    state: str
    source: str  # squeue | scontrol | sacct | registry | unknown
    name: str | None = None
    reason: str | None = None
    exit_code: int | None = None
    exit_code_raw: str | None = None
    elapsed: str | None = None
    time_limit: str | None = None
    nodelist: str | None = None
    partition: str | None = None
    account: str | None = None
    submit_time: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    max_rss: int | None = None
    workdir: str | None = None
    stdout_path: str | None = None
    stderr_path: str | None = None
    script_path: str | None = None
    accounting_pending: bool = False
    terminal: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if not d["extra"]:
            d.pop("extra")
        return {k: v for k, v in d.items() if v is not None or k in ("state", "job_id")}


# --------------------------------------------------------------------------- diagnostics


def _fmt_bytes(n: int | None) -> str:
    if n is None:
        return "?"
    f = float(n)
    for unit in ("B", "K", "M", "G", "T"):
        if f < 1024 or unit == "T":
            return f"{f:.0f}{unit}" if unit == "B" else f"{f:.1f}{unit}"
        f /= 1024
    return str(n)


@dataclass
class DiagContext:
    """Everything the pure diagnosis rules need; built by :meth:`Cluster.diagnose`."""

    state: str
    exit_code: int | None = None
    exit_code_raw: str | None = None
    reason: str | None = None
    max_rss: int | None = None  # bytes
    req_mem: int | None = None  # bytes
    stdout_tail: str = ""
    stderr_tail: str = ""
    cancelled_by: str | None = None  # uid/user from "CANCELLED by <x>"
    whoami: str | None = None
    elapsed: str | None = None
    time_limit: str | None = None

    def tails(self) -> str:
        return (self.stderr_tail or "") + "\n" + (self.stdout_tail or "")

    def last_stdout_line(self) -> str | None:
        for ln in reversed((self.stdout_tail or "").splitlines()):
            if ln.strip():
                return ln.strip()
        return None


# stderr/stdout signatures for a FAILED job: (regex, verdict, hints)
_FAILED_SIGNATURES: list[tuple[str, str, list[str]]] = [
    (
        r"No module named",
        "A Python import failed: a required module is missing",
        [
            "activate the right environment before running "
            "(e.g. `module load python`, then `source .../venv/bin/activate`)",
            "the batch environment differs from your login shell; set it up inside the script",
        ],
    ),
    (
        r"command not found",
        "A command was not found on PATH",
        [
            "load the module that provides it, or call it by absolute path",
            "batch jobs don't source your interactive profile; `module load` in the script",
        ],
    ),
    (
        r"Permission denied",
        "Permission denied",
        [
            "check the file permissions and that scripts are executable (`chmod +x`)",
            "verify you own the target path and it's on a writable filesystem",
        ],
    ),
    (
        r"No such file or directory",
        "A file or directory was missing at run time",
        [
            "paths must be correct *on the cluster*, not your laptop",
            "make sure inputs were synced/created before the job ran (see the sync marker)",
        ],
    ),
    (
        r"CUDA (?:error|out of memory)|No CUDA-capable device|torch\.cuda|CUDA_ERROR",
        "A GPU/CUDA error occurred",
        [
            "request a GPU (`--gpus-per-node=…`) and load the matching CUDA/toolkit module",
            "for CUDA out-of-memory, reduce batch size or model size",
        ],
    ),
    (
        r"\bKilled\b",
        "The process was killed (often the cgroup OOM killer or a signal)",
        [
            "check memory use vs `--mem`; a cgroup OOM shows up as `Killed`",
            "inspect `seff <jobid>` / the job's MaxRSS",
        ],
    ),
]

# PENDING reason -> plain-English explanation.
_PENDING_REASONS: list[tuple[str, str]] = [
    (
        "QOSMaxJobsPerUserLimit",
        "You've hit the max running jobs for your QOS; earlier jobs "
        "must finish before this one starts.",
    ),
    ("QOSMax", "A QOS resource limit is holding the job (jobs/CPUs/GPUs per user)."),
    (
        "AssocGrpBillingMinutes",
        "Your allocation is out of billing minutes (compute budget exhausted for the period).",
    ),
    ("AssocGrp", "An account/allocation group limit is holding the job."),
    (
        "ReqNodeNotAvail",
        "The requested nodes/features aren't currently available "
        "(often a reservation or maintenance window).",
    ),
    ("Resources", "Waiting for enough free resources to start."),
    ("Priority", "Queued behind higher-priority jobs; this is normal, just wait."),
    ("Dependency", "Waiting on a job dependency to finish first."),
    ("PartitionTimeLimit", "The requested time exceeds the partition's limit; lower `--time`."),
    ("PartitionNodeLimit", "The request exceeds the partition's node limit."),
]


def _rule_oom(ctx: DiagContext) -> tuple[str, list[str]] | None:
    near = (
        ctx.max_rss is not None
        and ctx.req_mem is not None
        and ctx.req_mem > 0
        and ctx.max_rss >= 0.95 * ctx.req_mem
    )
    if ctx.state == "OUT_OF_MEMORY" or (ctx.state == "FAILED" and near):
        hints: list[str] = []
        if ctx.max_rss is not None and ctx.req_mem:
            pct = round(100 * ctx.max_rss / ctx.req_mem)
            hints.append(
                f"peak memory {_fmt_bytes(ctx.max_rss)} reached ~{pct}% of the "
                f"{_fmt_bytes(ctx.req_mem)} requested"
            )
        hints.append("raise `--mem` (or `--mem-per-cpu`), or lower the job's memory footprint")
        return "Out of memory: the job exceeded its memory allocation", hints
    return None


def _rule_timeout(ctx: DiagContext) -> tuple[str, list[str]] | None:
    if ctx.state != "TIMEOUT":
        return None
    hints = [
        f"increase `--time` (ran {ctx.elapsed or '?'} of a {ctx.time_limit or '?'} limit) "
        "or checkpoint and resume",
    ]
    last = ctx.last_stdout_line()
    if last:
        hints.append(f"last stdout line before the wall clock: {last!r}")
    return "Hit the wall-clock time limit", hints


def _rule_node_fail(ctx: DiagContext) -> tuple[str, list[str]] | None:
    if ctx.state == "NODE_FAIL":
        return (
            "Node failure (infrastructure, not your job)",
            ["resubmit; the compute node died mid-run — usually transient"],
        )
    if ctx.state == "BOOT_FAIL":
        return (
            "Node boot failure (infrastructure, not your job)",
            ["resubmit; the allocated node failed to boot"],
        )
    return None


def _rule_cancelled(ctx: DiagContext) -> tuple[str, list[str]] | None:
    if ctx.state != "CANCELLED":
        return None
    by = ctx.cancelled_by
    if by and ctx.whoami and (by == ctx.whoami):
        return "Cancelled by you (or one of your own sessions)", []
    if by and by not in ("0",) and ctx.whoami and by != ctx.whoami:
        return (
            f"Cancelled by another user/admin (uid {by})",
            ["an administrator or a different session cancelled it; ask why before resubmitting"],
        )
    if by == "0":
        return (
            "Cancelled by the system/scheduler (uid 0)",
            ["often a time/QOS enforcement or a node drain; check the partition/reservation state"],
        )
    return "Cancelled", []


def _rule_failed_signature(ctx: DiagContext) -> tuple[str, list[str]] | None:
    if ctx.state not in ("FAILED", "COMPLETED"):
        return None
    if ctx.state == "COMPLETED" and not ctx.exit_code:
        return None
    for stream in (ctx.stderr_tail, ctx.stdout_tail):
        for pat, verdict, hints in _FAILED_SIGNATURES:
            if stream and re.search(pat, stream):
                return verdict, list(hints)
    return None


def _rule_failed_generic(ctx: DiagContext) -> tuple[str, list[str]] | None:
    if ctx.state == "FAILED" or (ctx.state == "COMPLETED" and ctx.exit_code):
        code = ctx.exit_code if ctx.exit_code is not None else "?"
        return (
            f"Job failed (exit code {code})",
            ["read the stderr tail below for the underlying error"],
        )
    return None


def _rule_pending(ctx: DiagContext) -> tuple[str, list[str]] | None:
    if ctx.state != "PENDING":
        return None
    reason = ctx.reason or ""
    for key, text in _PENDING_REASONS:
        if reason.startswith(key):
            return f"Pending: {text}", (
                [] if key in ("Priority", "Resources") else [f"scheduler reason: {reason}"]
            )
    if reason:
        return f"Pending (scheduler reason: {reason})", []
    return "Pending in the queue", []


def _rule_running(ctx: DiagContext) -> tuple[str, list[str]] | None:
    if ctx.state == "RUNNING":
        return "Running", []
    return None


def _rule_completed(ctx: DiagContext) -> tuple[str, list[str]] | None:
    if ctx.state == "COMPLETED" and not ctx.exit_code:
        return "Completed successfully (exit 0)", []
    return None


# order matters: most specific / highest-signal first
DIAGNOSTICS = [
    _rule_oom,
    _rule_timeout,
    _rule_node_fail,
    _rule_cancelled,
    _rule_failed_signature,
    _rule_pending,
    _rule_completed,
    _rule_running,
    _rule_failed_generic,
]


def diagnose_job(ctx: DiagContext) -> tuple[str, list[str]]:
    """Return ``(verdict, hints)`` for a job context (pure; rule table above)."""
    for rule in DIAGNOSTICS:
        r = rule(ctx)
        if r is not None:
            return r
    if ctx.exit_code:
        return (
            f"Exit code {ctx.exit_code}; see stderr",
            ["no specific pattern matched — inspect the stderr tail below"],
        )
    return f"No diagnosis available (state {ctx.state})", []


DIAGNOSE_CAP = 64 * 1024


def _jsize(obj: Any) -> int:
    import json

    return len(json.dumps(obj, default=str))


def cap_diagnostic_fields(out: dict[str, Any], cap: int = DIAGNOSE_CAP) -> dict[str, Any]:
    """Trim a diagnose payload to ``cap`` bytes, sacrificing low-priority fields first.

    Priority (never trimmed first): ``verdict``/``hints`` > tails > ``script`` > ``steps``.
    Sets ``truncated=True`` when anything was dropped or shortened.
    """
    out.setdefault("truncated", False)
    if _jsize(out) <= cap:
        return out
    # 1. drop steps entirely
    if _jsize(out) > cap and out.get("steps"):
        out["steps"] = []
        out["truncated"] = True
    # 2. shrink the script, then the tails (stdout before stderr — stderr is the error)
    for f in ("script", "stdout_tail", "stderr_tail"):
        if _jsize(out) <= cap:
            break
        if out.get(f):
            excess = _jsize(out) - cap
            val = out[f]
            out[f] = val[: max(0, len(val) - excess - 64)]
            while out[f] and _jsize(out) > cap:
                out[f] = out[f][:-256]
            out["truncated"] = True
    return out
