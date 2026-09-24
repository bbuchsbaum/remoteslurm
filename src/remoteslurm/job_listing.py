"""Bounded presentation of job listings; detailed status remains a separate lookup."""

from __future__ import annotations

import datetime as dt
import json
import math
from dataclasses import fields as dataclass_fields
from typing import Any

from .errors import InvalidArgument
from .slurm import JobStatus

SUMMARY_FIELDS = (
    "job_id",
    "name",
    "state",
    "terminal",
    "exit_code",
    "reason",
    "elapsed",
    "source",
    "accounting_pending",
    "submit_time",
)


def since_epoch(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        stamp = float(value)
        if not math.isfinite(stamp):
            raise ValueError("nonfinite timestamp")
        return stamp
    except ValueError:
        try:
            return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError as exc:
            raise InvalidArgument(
                "since must be an ISO timestamp or Unix epoch (submission time)"
            ) from exc


def summary(status: JobStatus) -> dict[str, Any]:
    out = {k: getattr(status, k) for k in SUMMARY_FIELDS if getattr(status, k) is not None}
    extra = status.extra
    if extra.get("array"):
        failed = extra.get("failed_tasks", [])
        out["extra"] = {
            "array": True,
            "n_tasks": extra.get("n_tasks"),
            "tasks": extra.get("tasks", {}),
            "failed_tasks_count": len(failed),
            "failed_tasks_sample": failed[:10],
        }
    clipped = []
    for key, value in list(out.items()):
        if isinstance(value, str) and len(value) > 512:
            out[key] = value[:512]
            clipped.append(key)
    if clipped:
        out["truncated_fields"] = clipped
    return out


def page(
    statuses: list[JobStatus],
    *,
    compact: bool = True,
    fields: list[str] | None = None,
    limit: int = 50,
    offset: int = 0,
    max_bytes: int = 32768,
    registry_error: str | None = None,
) -> dict[str, Any]:
    if not 1 <= limit <= 1000 or offset < 0 or not 2048 <= max_bytes <= 200000:
        raise InvalidArgument("limit must be 1..1000, offset >= 0, max_bytes 2048..200000")
    allowed = {f.name for f in dataclass_fields(JobStatus)}
    if fields is not None and (not fields or set(fields) - allowed):
        raise InvalidArgument(
            "fields must be nonempty JobStatus field names", fields=sorted(allowed)
        )
    rows: list[dict[str, Any]] = []
    out: dict[str, Any] = {
        "jobs": rows,
        "count": 0,
        "total": len(statuses),
        "next_offset": None,
        "has_more": False,
        "registry_available": registry_error is None,
    }
    if registry_error:
        # Reserve room for at least one record even when JSON escapes every code point.
        out["registry_error"] = registry_error[:64]
        out["registry_error_truncated"] = len(registry_error) > 64
    for status in statuses[offset : offset + limit]:
        row = summary(status) if compact else status.to_dict()
        if fields is not None:
            # Field selection is explicit detail access, subject to the same byte budget.
            row = {k: getattr(status, k) for k in set(fields) | {"job_id", "state"}}
        size = len(json.dumps(row).encode())
        if size > max_bytes - 1024:
            row = summary(status)
            row["detail_omitted"] = True
            row["action"] = "request this job_id separately for full detail"
            if len(json.dumps(row).encode()) > max_bytes - 1024:
                row = {"job_id": status.job_id, "state": status.state, "detail_omitted": True}
        rows.append(row)
        if len(json.dumps(out).encode()) > max_bytes - 256:
            if len(rows) == 1:
                rows[0] = {"job_id": status.job_id, "state": status.state, "detail_omitted": True}
            else:
                rows.pop()
            break
    out["count"] = len(rows)
    end = offset + len(rows)
    out["has_more"] = end < len(statuses)
    out["next_offset"] = end if out["has_more"] else None
    return out
