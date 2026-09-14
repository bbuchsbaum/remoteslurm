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

SSTAT_USAGE_FIELDS = ["JobID", "AllocTRES", "NTasks", "AveCPU", "AveRSS", "MaxRSS", "Pids"]
SACCT_USAGE_FIELDS = [
    "JobID",
    "State",
    "AllocCPUS",
    "NTasks",
    "Elapsed",
    "TotalCPU",
    "AveRSS",
    "MaxRSS",
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
# Terminal states that count as a *failure* when rolling up an array (CANCELLED and COMPLETED
# are handled separately). Used by :func:`aggregate_array` and the ``jobs`` TASKS column.
ARRAY_FAILED_STATES = {
    "FAILED",
    "TIMEOUT",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "BOOT_FAIL",
    "DEADLINE",
    "PREEMPTED",
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


_COLLAPSED_ARRAY_RE = re.compile(r"^(\d+)_\[([\d,\-:%]+)\]$")
_TASK_ARRAY_RE = re.compile(r"^(\d+)_(\d+)$")


MAX_ARRAY_EXPAND = 100_000  # cap task ids materialized from one bracket (guards a huge squeue line)


def expand_array_tasks(spec: str) -> list[int]:
    """Expand a Slurm array bracket spec into task ids (order-preserving, deduplicated).

    Accepts the bracket form with or without the surrounding ``[...]`` and strips the
    ``%N`` concurrency suffix: ``"[5-9%4]"`` -> ``[5,6,7,8,9]``, ``"5,7,9"`` -> ``[5,7,9]``,
    ``"0-6:2"`` (step) -> ``[0,2,4,6]``. The total is capped at ``MAX_ARRAY_EXPAND`` so a
    pathological ``0-100000000`` in one squeue row cannot exhaust memory.
    """
    s = spec.strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    if "%" in s:  # trailing concurrency throttle, e.g. 5-9%4
        s = s.split("%", 1)[0]
    seen: set[int] = set()
    uniq: list[int] = []

    def add(t: int) -> bool:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
        return len(uniq) < MAX_ARRAY_EXPAND

    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        step = 1
        if ":" in part:
            part, _, step_s = part.partition(":")
            if step_s.isdigit() and int(step_s) > 0:
                step = int(step_s)
        if "-" in part.lstrip("-"):  # a range lo-hi (task ids are non-negative)
            lo_s, _, hi_s = part.partition("-")
            lo, hi = int(lo_s), int(hi_s)
            hi = min(hi, lo + MAX_ARRAY_EXPAND * step)  # bound the range before iterating
            for t in range(lo, hi + 1, step):
                if not add(t):
                    return uniq
        elif not add(int(part)):
            return uniq
    return uniq


def parse_squeue(stdout: str) -> list[dict[str, str]]:
    """Parse ``squeue`` output into per-job rows.

    A collapsed *pending* array row (``123_[5-9%4]``) is expanded into one pseudo-row per
    task id (each carries the collapsed row's state, normally ``PENDING``); a running/single
    task row (``123_4``) is kept as-is. Every row gains ``array_base`` and ``array_task``
    (both ``""`` for a non-array job), so array aggregation is a simple tally over rows.
    """
    rows: list[dict[str, str]] = []
    for line in stdout.splitlines():
        line = line.rstrip("\n")
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) < len(SQUEUE_FIELDS):
            parts += [""] * (len(SQUEUE_FIELDS) - len(parts))
        row = dict(zip(SQUEUE_FIELDS, parts, strict=False))
        row["state"] = normalize_state(row["state"])
        jid = row["job_id"]
        m = _COLLAPSED_ARRAY_RE.match(jid)
        if m:
            base = m.group(1)
            for t in expand_array_tasks(m.group(2)):
                pseudo = dict(row)
                pseudo["job_id"] = f"{base}_{t}"
                pseudo["array_base"] = base
                pseudo["array_task"] = str(t)
                pseudo["array_collapsed"] = "1"
                rows.append(pseudo)
            continue
        row["array_collapsed"] = ""
        m2 = _TASK_ARRAY_RE.match(jid)
        if m2:
            row["array_base"] = m2.group(1)
            row["array_task"] = m2.group(2)
        else:
            row["array_base"] = ""
            row["array_task"] = ""
        rows.append(row)
    return rows


def aggregate_array(
    base_id: str,
    squeue_rows: list[dict[str, str]],
    sacct_jobs: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Roll up an array's per-task states into one summary.

    ``squeue_rows`` are :func:`parse_squeue` rows (currently active tasks win, as squeue drops
    finished ones); ``sacct_jobs`` are :func:`parse_sacct` records keyed ``<base>_<task>`` for
    finished tasks. Returns ``{state, tasks, failed_tasks, task_states, terminal}`` where the
    aggregate ``state`` is FAILED if any task failed and none is running, else RUNNING if any
    is running, else PENDING if any is pending, else CANCELLED/COMPLETED; ``terminal`` is true
    only when every task has reached a terminal state.
    """
    task_states: dict[int, str] = {}
    for key, rec in sacct_jobs.items():  # sacct: authoritative for finished tasks
        b, sep, t = key.partition("_")
        if sep and b == base_id and t.isdigit():
            task_states[int(t)] = normalize_state(rec.get("state", "UNKNOWN"))

    # squeue overrides sacct for active tasks. Apply explicit rows first, then collapsed-pending
    # pseudo-rows only for tasks not already seen, so a `123_[4-9]` pending bracket cannot
    # downgrade a `123_4` that squeue also lists as RUNNING.
    def apply(rows: list[dict[str, str]], collapsed: bool) -> None:
        for r in rows:
            if r.get("array_base") != base_id or not r.get("array_task"):
                continue
            try:
                t = int(r["array_task"])
            except (TypeError, ValueError):
                continue
            if collapsed and t in task_states:
                continue
            task_states[t] = normalize_state(r["state"])

    apply([r for r in squeue_rows if r.get("array_collapsed") != "1"], collapsed=False)
    apply([r for r in squeue_rows if r.get("array_collapsed") == "1"], collapsed=True)
    tasks: dict[str, int] = {}
    for s in task_states.values():
        tasks[s] = tasks.get(s, 0) + 1
    failed_tasks = sorted(t for t, s in task_states.items() if s in ARRAY_FAILED_STATES)
    any_running = any(s not in TERMINAL_STATES and s != "PENDING" for s in task_states.values())
    any_pending = any(s == "PENDING" for s in task_states.values())
    any_completed = any(s == "COMPLETED" for s in task_states.values())
    any_cancelled = any(s == "CANCELLED" for s in task_states.values())
    if not task_states:
        state = "UNKNOWN"
    elif failed_tasks and not any_running:
        state = "FAILED"
    elif any_running:
        state = "RUNNING"
    elif any_pending:
        state = "PENDING"
    elif any_cancelled and not any_completed:
        state = "CANCELLED"
    else:
        state = "COMPLETED"
    terminal = bool(task_states) and all(s in TERMINAL_STATES for s in task_states.values())
    return {
        "state": state,
        "tasks": tasks,
        "failed_tasks": failed_tasks,
        "task_states": task_states,
        "terminal": terminal,
    }


def walltime_to_seconds(spec: str | int | None) -> int | None:
    """Parse a Slurm walltime spec into seconds.

    Accepts the Slurm forms ``minutes``, ``minutes:seconds``, ``hours:minutes:seconds``,
    ``days-hours``, ``days-hours:minutes`` and ``days-hours:minutes:seconds``. Returns ``None``
    for an unset / unparseable value (the caller then falls back to a default budget).
    """
    if spec is None:
        return None
    if isinstance(spec, int):
        return spec * 60 if spec >= 0 else None  # a bare int is minutes, matching Slurm
    s = str(spec).strip()
    if not s:
        return None
    days = 0
    if "-" in s:
        d, _, s = s.partition("-")
        if not d.isdigit():
            return None
        days = int(d)
    parts = s.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return None
    if days and len(parts) == 1:  # days-hours
        h, m, sec = nums[0], 0, 0
    elif days and len(parts) == 2:  # days-hours:minutes
        h, m, sec = nums[0], nums[1], 0
    elif days:  # days-hours:minutes:seconds
        h, m, sec = nums[0], nums[1], nums[2]
    elif len(parts) == 1:  # minutes
        return days * 86400 + nums[0] * 60
    elif len(parts) == 2:  # minutes:seconds
        return nums[0] * 60 + nums[1]
    else:  # hours:minutes:seconds
        h, m, sec = nums[0], nums[1], nums[2]
    return days * 86400 + h * 3600 + m * 60 + sec


def accounting_time_to_seconds(spec: str | None) -> float | None:
    """Parse an ``sstat``/``sacct`` CPU-time value, including fractional seconds."""
    if spec is None:
        return None
    text = str(spec).strip()
    if not text:
        return None
    days = 0
    if "-" in text:
        day_text, _, text = text.partition("-")
        if not day_text.isdigit():
            return None
        days = int(day_text)
    parts = text.split(":")
    try:
        values = [float(part) for part in parts]
    except ValueError:
        return None
    if len(values) == 3:
        hours, minutes, seconds = values
    elif len(values) == 2:
        hours, minutes, seconds = 0.0, values[0], values[1]
    elif len(values) == 1:
        hours, minutes, seconds = 0.0, 0.0, values[0]
    else:
        return None
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


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


def _parse_int(value: str | None) -> int | None:
    try:
        return int(str(value)) if value not in (None, "") else None
    except ValueError:
        return None


def _allocated_cpus(tres: str) -> int | None:
    match = re.search(r"(?:^|,)cpu=(\d+)(?:,|$)", tres or "", re.I)
    return int(match.group(1)) if match else None


def _usage_derived(
    *,
    source: str,
    job_step: str,
    allocated_cpus: int | None,
    slurm_tasks: int | None,
    elapsed_raw: str | None,
    cpu_time_raw: str | None,
    cpu_time_is_average: bool,
    ave_rss_raw: str | None,
    max_rss_raw: str | None,
    pids_raw: str | None = None,
) -> dict[str, Any]:
    """Normalize live and terminal accounting rows into one usage schema."""
    tasks = slurm_tasks or 1
    elapsed_seconds = accounting_time_to_seconds(elapsed_raw)
    cpu_value = accounting_time_to_seconds(cpu_time_raw)
    cpu_seconds = cpu_value * tasks if cpu_value is not None and cpu_time_is_average else cpu_value
    effective_cpus = None
    utilization = None
    if cpu_seconds is not None and elapsed_seconds and elapsed_seconds > 0:
        effective_cpus = cpu_seconds / elapsed_seconds
        if allocated_cpus and allocated_cpus > 0:
            utilization = 100.0 * effective_cpus / allocated_cpus
    pids = {
        pid.strip() for pid in (pids_raw or "").split(",") if pid.strip() and pid.strip().isdigit()
    }
    ave_rss = _parse_mem(ave_rss_raw or "")
    result: dict[str, Any] = {
        "available": True,
        "source": source,
        "job_step": job_step,
        "allocated_cpus": allocated_cpus,
        "slurm_tasks": slurm_tasks,
        "elapsed": elapsed_raw,
        "elapsed_seconds": elapsed_seconds,
        "cpu_time": cpu_time_raw,
        "cpu_time_seconds": cpu_seconds,
        "effective_cpus": round(effective_cpus, 2) if effective_cpus is not None else None,
        "cpu_utilization_percent": round(utilization, 1) if utilization is not None else None,
        "ave_rss_bytes": ave_rss,
        "estimated_total_rss_bytes": ave_rss * tasks if ave_rss is not None else None,
        "max_rss_bytes": _parse_mem(max_rss_raw or ""),
    }
    if source == "sstat" and pids_raw not in (None, ""):
        result["live_pids"] = len(pids)
    return {key: value for key, value in result.items() if value is not None}


def parse_sstat_usage(
    stdout: str, *, elapsed: str | None = None, allocated_cpus: int | None = None
) -> dict[str, Any] | None:
    """Parse one live batch-step sample produced with :data:`SSTAT_USAGE_FIELDS`."""
    rows = []
    for line in stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) < len(SSTAT_USAGE_FIELDS):
            parts += [""] * (len(SSTAT_USAGE_FIELDS) - len(parts))
        rows.append(dict(zip([f.lower() for f in SSTAT_USAGE_FIELDS], parts, strict=False)))
    if not rows:
        return None
    row = next((item for item in rows if item["jobid"].endswith(".batch")), rows[0])
    return _usage_derived(
        source="sstat",
        job_step=row["jobid"],
        allocated_cpus=_allocated_cpus(row["alloctres"]) or allocated_cpus,
        slurm_tasks=_parse_int(row["ntasks"]),
        elapsed_raw=elapsed,
        cpu_time_raw=row["avecpu"] or None,
        cpu_time_is_average=True,
        ave_rss_raw=row["averss"] or None,
        max_rss_raw=row["maxrss"] or None,
        pids_raw=row["pids"],
    )


def parse_sacct_usage(stdout: str, job_id: str) -> dict[str, Any] | None:
    """Parse terminal allocation and batch-step accounting into the live-usage schema."""
    rows: list[dict[str, str]] = []
    fields = [f.lower() for f in SACCT_USAGE_FIELDS]
    for line in stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) < len(fields):
            parts += [""] * (len(fields) - len(parts))
        rows.append(dict(zip(fields, parts, strict=False)))
    if not rows:
        return None
    allocation = next((row for row in rows if row["jobid"] == job_id), rows[0])
    batch = next((row for row in rows if row["jobid"] == job_id + ".batch"), allocation)
    return _usage_derived(
        source="sacct",
        job_step=batch["jobid"],
        allocated_cpus=_parse_int(allocation["alloccpus"] or batch["alloccpus"]),
        slurm_tasks=_parse_int(batch["ntasks"]),
        elapsed_raw=allocation["elapsed"] or batch["elapsed"] or None,
        cpu_time_raw=allocation["totalcpu"] or batch["totalcpu"] or None,
        cpu_time_is_average=False,
        ave_rss_raw=batch["averss"] or allocation["averss"] or None,
        max_rss_raw=batch["maxrss"] or allocation["maxrss"] or None,
    )


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


# --------------------------------------------------------------------------- queue intelligence
# Every parser here is deliberately tolerant: site formats and Slurm versions vary, so an
# unknown/short/missing column becomes ``None`` rather than raising.


def parse_squeue_start(stdout: str) -> list[dict[str, Any]]:
    """``squeue --start -o "%i|%S|%r"`` -> ``[{job_id, est_start, reason}]``.

    ``est_start`` is ``None`` for ``N/A``/unknown (the scheduler has no estimate yet).
    """
    out: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        if not line.strip():
            continue
        parts = [p.strip() for p in line.split("|")]
        jid = parts[0] if parts else ""
        if not jid or jid.lower() == "job_id":  # skip a stray header, if any
            continue
        start = parts[1] if len(parts) > 1 else ""
        reason = parts[2] if len(parts) > 2 else ""
        out.append(
            {
                "job_id": jid,
                "est_start": start if start and start not in ("N/A", "Unknown", "") else None,
                "reason": reason or None,
            }
        )
    return out


SSHARE_FIELDS = [
    "account",
    "user",
    "raw_shares",
    "norm_shares",
    "raw_usage",
    "effective_usage",
    "fair_share",
]
_SSHARE_INT = {"raw_shares", "raw_usage"}
_SSHARE_FLOAT = {"norm_shares", "effective_usage", "fair_share"}


def parse_sshare(stdout: str) -> list[dict[str, Any]]:
    """``sshare -U -P`` -> per-account fair-share rows.

    Columns: ``Account|User|RawShares|NormShares|RawUsage|EffectvUsage|FairShare``. Numeric
    fields are typed (int/float) when parseable, else ``None``; a header row is skipped.
    """
    out: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        if not line.strip():
            continue
        parts = [p.strip() for p in line.split("|")]
        if parts and parts[0].lower() == "account":  # header
            continue
        rec: dict[str, Any] = {}
        for i, name in enumerate(SSHARE_FIELDS):
            raw = parts[i] if i < len(parts) else ""
            if not raw:
                rec[name] = None
            elif name in _SSHARE_INT:
                try:
                    rec[name] = int(raw)
                except ValueError:
                    rec[name] = None
            elif name in _SSHARE_FLOAT:
                try:
                    rec[name] = float(raw)
                except ValueError:
                    rec[name] = None
            else:
                rec[name] = raw
        out.append(rec)
    return out


QOS_FIELDS = ["name", "max_wall", "max_jobs_pu", "max_tres_pu", "priority"]


def parse_qos(stdout: str) -> list[dict[str, Any]]:
    """``sacctmgr -P -n show qos format=name,maxwall,maxjobspu,maxtresperuser,priority`` -> rows.

    All values are kept as strings (``None`` when empty); the format is site-dependent so no
    field is required. ``max_wall`` empty means "no QOS wall limit".
    """
    out: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        if not line.strip():
            continue
        parts = [p.strip() for p in line.split("|")]
        if parts and parts[0].lower() == "name":  # header, if the site emits one
            continue
        rec: dict[str, Any] = {}
        for i, name in enumerate(QOS_FIELDS):
            raw = parts[i] if i < len(parts) else ""
            rec[name] = raw or None
        out.append(rec)
    return out


ASSOC_FIELDS = ["account", "partition", "qos", "grp_tres", "max_jobs"]


def parse_assoc(stdout: str) -> list[dict[str, Any]]:
    """``sacctmgr show assoc user=<me> format=account,partition,qos,grptres,maxjobs`` -> rows."""
    out: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        if not line.strip():
            continue
        parts = [p.strip() for p in line.split("|")]
        if parts and parts[0].lower() == "account":  # header
            continue
        rec: dict[str, Any] = {}
        for i, name in enumerate(ASSOC_FIELDS):
            raw = parts[i] if i < len(parts) else ""
            rec[name] = raw or None
        out.append(rec)
    return out


def parse_df(stdout: str) -> list[dict[str, Any]]:
    """Parse ``df -h`` output into per-filesystem rows (tolerant of GNU vs BSD/macOS columns).

    Uses positional columns for filesystem/size/used/avail, the first ``%`` token for the
    capacity, and the last token for the mount point — so macOS's extra inode columns don't
    shift the mount off the end. Short/wrapped lines are skipped.
    """
    out: list[dict[str, Any]] = []
    for i, line in enumerate([ln for ln in stdout.splitlines() if ln.strip()]):
        parts = line.split()
        if i == 0 and parts and parts[0].lower() == "filesystem":  # header
            continue
        if len(parts) < 5:
            continue
        pct = next((p for p in parts[4:] if p.endswith("%")), None)
        out.append(
            {
                "filesystem": parts[0],
                "size": parts[1],
                "used": parts[2],
                "avail": parts[3],
                "use_pct": pct,
                "mounted_on": parts[-1],
            }
        )
    return out


# A ``<used>/<limit>`` quota pair from a fixed-width site quota command. A value may carry an
# internal space (``0  B``) or spaces around
# the slash (``88GiB/ 100GiB``, ``7 /2000K``) — hence the ``\s*`` inside and around each value.
# used side must start with a digit (so it never grabs a word from the description); the limit
# side may be non-numeric (``unlimited``, ``inf``), as some sites report for project quotas.
_DU_PAIR = re.compile(r"([\d.]+\s*[A-Za-z]*)\s*/\s*([\d.]+\s*[A-Za-z]*|unlimited|inf|N/A)")


def _du_norm(v: str | None) -> str | None:
    """Collapse internal whitespace in a quota value (``0  B`` -> ``0 B``); ``""`` -> ``None``."""
    if v is None:
        return None
    s = " ".join(v.split())
    return s or None


def parse_quota_pairs(stdout: str) -> list[dict[str, Any]]:
    """Parse a fixed-width site quota table containing ``<used>/<limit>`` pairs.

    Columns are ``Description``, ``Space`` (``<used>/<limit>``), ``# of files``
    (``<used>/<limit>``). The description ends at its ``)`` (e.g. ``/home (user brad)``); the two
    quota pairs follow. Values may contain spaces (``0  B``) and the slash may be padded
    (``88GiB/ 100GiB``). Tolerant: the header is skipped, missing pieces become ``None``, and a
    line that doesn't fit still yields a row with ``raw`` — it never raises. ``df -h`` is the
    fallback when the tool is absent.
    """
    out: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        s = line.strip()
        if not s:
            continue
        low = s.lower()
        if low.startswith("description") or set(s) <= set("-= "):  # header / rule line
            continue
        rp = s.rfind(")")
        if rp != -1:
            desc, rest = s[: rp + 1].strip(), s[rp + 1 :]
        else:  # no parenthetical: description is the text before the first number
            m0 = re.search(r"\d", s)
            desc = (s[: m0.start()].strip() or s) if m0 else s
            rest = s[m0.start() :] if m0 else ""
        pairs = _DU_PAIR.findall(rest)
        used = limit = files_used = files_limit = None
        if len(pairs) >= 1:
            used, limit = _du_norm(pairs[0][0]), _du_norm(pairs[0][1])
        if len(pairs) >= 2:
            files_used, files_limit = _du_norm(pairs[1][0]), _du_norm(pairs[1][1])
        out.append(
            {
                "description": desc,
                "used": used,
                "limit": limit,
                "files_used": files_used,
                "files_limit": files_limit,
                "raw": s,
            }
        )
    return out


def parse_diskusage_report(stdout: str) -> list[dict[str, Any]]:
    """Backward-compatible alias for the generic :func:`parse_quota_pairs` parser."""
    return parse_quota_pairs(stdout)


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
    registry_available: bool | None = None
    registry_error: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    allocated_cpus: int | None = None
    usage: dict[str, Any] | None = None
    progress: dict[str, Any] | None = None

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
