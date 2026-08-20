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
