"""Local journal readers; following never contacts the scheduler."""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from ..config import parse_duration
from ..errors import InvalidArgument
from .store import ActivityStore


def safe_text(value: Any) -> str:
    """Escape terminal controls, including OSC/CSI, without interpreting markup."""
    return "".join(c if c.isprintable() or c == "\n" else f"\\x{ord(c):02x}" for c in str(value))


def parse_since(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        if "T" in value or ":" in value:
            date = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if date.tzinfo is None:
                raise ValueError("timezone required")
            return date.timestamp()
        return time.time() - parse_duration(value)
    except (ValueError, TypeError) as exc:
        raise InvalidArgument("since needs a duration (30m) or timestamp with timezone") from exc


def event_line(event: dict) -> str:
    subject, payload = event["subject"], event["payload"]
    fields = [event["recorded_at"], subject["host_alias"], event["kind"]]
    fields += [
        str(v)
        for v in (
            subject.get("job_id"),
            payload.get("name"),
            payload.get("state"),
            payload.get("path"),
            payload.get("summary"),
        )
        if v is not None
    ]
    return safe_text("  ".join(fields)).replace("\n", "\\n")


def follow(store: ActivityStore, **query: Any) -> Iterator[dict]:
    """Each yielded page carries its own resume cursor; heartbeat at most every 30s."""
    last_emit = 0.0
    while True:
        result = store.query(**query)
        if result["events"] or result["history_gap"] or time.monotonic() - last_emit >= 30:
            yield result
            last_emit = time.monotonic()
        query.pop("since", None)
        if result["resync_required"]:
            query.pop("after", None)
        else:
            query["after"] = result["next_cursor"]
        if not result["has_more"]:
            time.sleep(1)
