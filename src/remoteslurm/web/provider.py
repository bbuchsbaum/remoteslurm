"""Data providers for the dashboard: the live journal, or the bundled example data."""

from __future__ import annotations

import copy
import json
import time
from pathlib import Path
from typing import Any

from . import files as jobfiles
from .state import Journal

DEMO = Path(__file__).parent / "demo.json"
OBSERVER_TTL = 5.0


class LiveProvider:
    def __init__(self, config: Any, hosts: list[Any]) -> None:
        self.config, self.hosts = config, hosts
        self.journal = Journal(config, hosts)
        self.by_name = {h.name: h for h in hosts}
        self._observer: tuple[float, dict] | None = None
        observer = self.journal.observer

        def cached() -> dict:
            now = time.monotonic()
            if self._observer is None or now - self._observer[0] > OBSERVER_TTL:
                self._observer = (now, observer())
            return self._observer[1]

        self.journal.observer = cached  # type: ignore[method-assign]

    def state(self) -> dict:
        return self.journal.state()

    def activity(self, after: str | None, limit: int) -> dict:
        return self.journal.activity(after, limit)

    def keepalive(self) -> None:
        from ..activity.discovery import lease

        lease(self.hosts)

    def job(self, host: str, jid: str) -> dict | None:
        return next((j for j in self.state()["jobs"] if j["host"] == host and j["id"] == jid), None)

    def tail(self, host: str, jid: str, which: str, task: int | None) -> dict:
        hc, job = self.by_name.get(host), self.job(host, jid)
        if hc is None or job is None:
            return jobfiles.unavailable("unknown_job", "This job is no longer listed.")
        path = job["stdout"] if which == "out" else job["stderr"]
        guessed = job.get("pathsGuessed", False) and which == "out"
        if which == "err" and not path:
            return jobfiles.unavailable(
                "no_stderr", "This job writes its errors to the output log.", None
            )
        if not path:
            return jobfiles.unavailable("no_path", "Slurm did not report a log path for this job.")
        if job.get("array"):
            if task is None:
                return jobfiles.unavailable("task_required", "Pick a task to read its log.")
            resolved = jobfiles.task_path(path, job, task)
            if resolved is None:
                return jobfiles.unavailable(
                    "path_unknown",
                    "The log name for this task can't be worked out from what Slurm reported.",
                    path,
                )
            path = resolved
        elif "%" in path:
            resolved = jobfiles.expand(path, job, None)
            if resolved is None:
                return jobfiles.unavailable(
                    "path_unknown", "The log name uses a pattern that can't be resolved.", path
                )
            path = resolved
        if not path.startswith("/"):
            return jobfiles.unavailable("path_unknown", "The log path is not absolute.", path)
        return jobfiles.queue(hc, job, "tail", path, guessed=guessed)

    def files(self, host: str, jid: str) -> dict:
        hc, job = self.by_name.get(host), self.job(host, jid)
        if hc is None or job is None:
            return jobfiles.unavailable("unknown_job", "This job is no longer listed.")
        if not job.get("workdir") or not job["workdir"].startswith("/"):
            return jobfiles.unavailable("no_workdir", "Slurm did not report a work directory.")
        return jobfiles.queue(hc, job, "list", job["workdir"], guessed=False)

    def poll(self, rid: str, kind: str) -> dict:
        return jobfiles.poll(rid, kind=kind)


class DemoProvider:
    """Example data with relative times anchored to now; reads answer at once."""

    TIME_KEYS = {
        "observedAt",
        "accountingSince",
        "submit",
        "start",
        "end",
        "estStart",
        "t",
        "heartbeat",
        "vanishedAt",
        "mtime",
    }

    def __init__(self, path: Path = DEMO) -> None:
        self.data = json.loads(path.read_text())
        self.t0 = time.time()

    def shift(self, value: Any, now: float) -> Any:
        if isinstance(value, dict):
            return {
                k: (
                    now + v
                    if k in self.TIME_KEYS and isinstance(v, (int, float))
                    else self.shift(v, now)
                )
                for k, v in value.items()
            }
        if isinstance(value, list):
            return [self.shift(v, now) for v in value]
        return value

    def state(self) -> dict:
        state = self.shift(copy.deepcopy(self.data["state"]), self.t0)
        state.update(now=time.time(), demo=True)
        # Example hosts stay as fresh as they were when the page opened.
        for host in state.get("hosts", []):
            if host.get("observedAt") is not None:
                host["observedAt"] += time.time() - self.t0
        return state

    def activity(self, after: str | None, limit: int) -> dict:
        if after is not None:
            return {"events": [], "cursor": "demo", "resync": False, "more": False}
        items = self.shift(copy.deepcopy(self.data.get("activity", [])), self.t0)
        return {"events": items[-limit:], "cursor": "demo", "resync": False, "more": False}

    def keepalive(self) -> None:
        return None

    def tail(self, host: str, jid: str, which: str, task: int | None) -> dict:
        logs = self.data.get("logs", {}).get(f"{host}:{jid}", {})
        lines = logs.get(which)
        job: dict = next((j for j in self.data["state"]["jobs"] if j["key"] == f"{host}:{jid}"), {})
        path = job.get("stdout" if which == "out" else "stderr")
        if lines is None:
            return jobfiles.unavailable(
                "no_example", "The example data has no log for this job.", path
            )
        return {
            "request": None,
            "state": "DONE",
            "path": path,
            "pathGuessed": False,
            "error": None,
            "action": None,
            "result": {
                "lines": lines,
                "observedAt": time.time() - 5,
                "size": sum(len(x) + 1 for x in lines),
                "truncated": False,
                "gap": None,
                "binary": False,
            },
            "cached": None,
        }

    def files(self, host: str, jid: str) -> dict:
        entries = self.data.get("files", {}).get(f"{host}:{jid}")
        job: dict = next((j for j in self.data["state"]["jobs"] if j["key"] == f"{host}:{jid}"), {})
        if entries is None:
            return jobfiles.unavailable(
                "no_example", "The example data has no listing for this job.", job.get("workdir")
            )
        return {
            "request": None,
            "state": "DONE",
            "path": job.get("workdir"),
            "pathGuessed": False,
            "error": None,
            "action": None,
            "result": {"observedAt": time.time() - 5, "entries": entries, "partial": False},
            "cached": None,
        }

    def poll(self, rid: str, kind: str) -> dict:
        return jobfiles.unavailable("expired", "Example data answers immediately.")
