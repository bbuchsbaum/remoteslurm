"""Explicit job subscriptions owned by the local daemon, independent of tool-call lifetime.

This narrow store does not replace legacy events or the planned campaign activity journal.
Observations never submit, cancel, retry or validate work. Desktop delivery is best effort;
the persisted result is authoritative for whether a watch condition was observed.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import threading
import time
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

from . import slurm
from .cluster import Cluster
from .config import Config, HostConfig, state_dir
from .errors import InvalidArgument, NotFound, RemoteSlurmError

CONDITIONS = {"all_terminal", "any_failed", "all_terminal_or_any_failed"}


def target_identity(host: HostConfig) -> str:
    raw = json.dumps([host.ssh, host.ssh_opts, host.control_path], sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


class WatchStore:
    def __init__(self, root: Path | None = None):
        self.root = root or state_dir()
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / "job-watches.sqlite3"
        with self.connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS watches (id TEXT PRIMARY KEY, host TEXT, "
                "active INTEGER, next_poll REAL, data TEXT)"
            )
            db.execute("CREATE TABLE IF NOT EXISTS health (id INTEGER PRIMARY KEY, data TEXT)")

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=5)

    def start(
        self, host: HostConfig, job_ids: list[str], *, condition: str, poll: float, notify: bool
    ) -> dict[str, Any]:
        ids = sorted(set(job_ids))
        poll = float(poll)
        if not ids or len(ids) > 1000 or not 30 <= poll <= 3600 or condition not in CONDITIONS:
            raise InvalidArgument(
                "watch needs 1..1000 job IDs, poll 30..3600, and a valid condition"
            )
        for jid in ids:
            slurm.parse_job_id(jid)
            if "[" in jid or "+" in jid:
                raise InvalidArgument("watch accepts numeric job IDs or individual array task IDs")
        spec = {
            "host": host.name,
            "target": target_identity(host),
            "job_ids": ids,
            "condition": condition,
            "poll": poll,
            "notify": notify,
        }
        watch_id = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()[:24]
        data = dict(
            spec,
            watch_id=watch_id,
            status="active",
            created=time.time(),
            observed_at=None,
            observations={},
            result=None,
            error=None,
            delivery="pending" if notify else "disabled",
        )
        with self.connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO watches VALUES (?, ?, 1, 0, ?)",
                (watch_id, host.name, json.dumps(data)),
            )
        return self.read(watch_id, host=host.name)

    def read(self, watch_id: str, *, host: str) -> dict[str, Any]:
        with self.connect() as db:
            row = db.execute(
                "SELECT data FROM watches WHERE id=? AND host=?", (watch_id, host)
            ).fetchone()
            health = db.execute("SELECT data FROM health WHERE id=1").fetchone()
        if row is None:
            raise NotFound("unknown watch_id", watch_id=watch_id)
        data = json.loads(row[0])
        observations = data.pop("observations", {})
        data["counts"] = dict(Counter(o["state"] for o in observations.values()))
        ids = data.pop("job_ids")
        data["job_count"] = len(ids)
        data["job_ids_sample"] = ids[:20]
        data["observer"] = json.loads(health[0]) if health else {"heartbeat": None}
        heartbeat = data["observer"].get("heartbeat")
        data["observer"]["stale"] = not heartbeat or time.time() - heartbeat > 90
        data["observation_stale"] = (
            bool(data["error"])
            or not data["observed_at"]
            or (data["status"] == "active" and time.time() - data["observed_at"] > 2 * data["poll"])
        )
        data["agent_wakeup"] = "requires client integration; desktop notification only"
        return dict(data)

    def stop(self, watch_id: str, *, host: str) -> dict[str, Any]:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT data FROM watches WHERE id=? AND host=?", (watch_id, host)
            ).fetchone()
            if row is None:
                raise NotFound("unknown watch_id", watch_id=watch_id)
            data = json.loads(row[0])
            if data["status"] == "active":
                data["status"] = "stopped"
                db.execute(
                    "UPDATE watches SET active=0, data=? WHERE id=?", (json.dumps(data), watch_id)
                )
        return self.read(watch_id, host=host)

    def active(self) -> bool:
        with self.connect() as db:
            return db.execute("SELECT 1 FROM watches WHERE active=1 LIMIT 1").fetchone() is not None

    def due(self, now: float) -> list[dict[str, Any]]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT data FROM watches WHERE active=1 AND next_poll<=? "
                "ORDER BY next_poll, id LIMIT 32",
                (now,),
            ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def save(self, data: dict[str, Any], now: float) -> bool:
        with self.connect() as db:
            # A stop racing collection wins; the worker must not resurrect it.
            changed = db.execute(
                "UPDATE watches SET active=?, next_poll=?, data=? WHERE id=? AND active=1",
                (
                    data["status"] == "active",
                    now + data["poll"],
                    json.dumps(data),
                    data["watch_id"],
                ),
            ).rowcount
        return bool(changed)

    def delivered(self, watch_id: str, delivered: bool) -> None:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT data FROM watches WHERE id=?", (watch_id,)).fetchone()
            if row:
                data = json.loads(row[0])
                data["delivery"] = "sent" if delivered else "failed"
                db.execute("UPDATE watches SET data=? WHERE id=?", (json.dumps(data), watch_id))

    def heartbeat(self, error: str | None = None) -> None:
        with self.connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO health VALUES (1, ?)",
                (json.dumps({"heartbeat": time.time(), "pid": os.getpid(), "error": error}),),
            )


def collect(cluster: Cluster, ids: list[str]) -> dict[str, dict[str, Any]]:
    """Two bounded scheduler queries for at most 1,000 IDs, shared by overlapping watches."""
    queue = cluster.call("squeue", jobs=ids, format=slurm.SQUEUE_FORMAT, _timeout=25)
    acct = cluster.call("sacct", jobs=ids, fields=slurm.SACCT_FIELDS, all_steps=False, _timeout=25)
    for result in (queue, acct):
        if result.get("rc") or result.get("stdout_truncated"):
            raise RemoteSlurmError("scheduler observation failed or was truncated")
    rows, accounting = slurm.parse_squeue(queue["stdout"]), slurm.parse_sacct(acct["stdout"])
    out = {}
    for jid in ids:
        children = [r for r in rows if r.get("array_base") == jid]
        is_array = bool(children) or any(key.startswith(jid + "_") for key in accounting)
        if is_array:
            aggregate = slurm.aggregate_array(jid, rows, accounting)
            states = aggregate["task_states"]
            failed = any(s in slurm.TERMINAL_STATES and s != "COMPLETED" for s in states.values())
            failed = failed or any(
                rec.get("state") in slurm.TERMINAL_STATES
                and slurm.exit_code_int(rec.get("exit_code")) not in (None, 0)
                for key, rec in accounting.items()
                if key.startswith(jid + "_")
            )
            out[jid] = {
                "state": aggregate["state"],
                "terminal": aggregate["terminal"],
                "failed": failed,
                "seen_tasks": sorted(str(t) for t in states),
            }
        else:
            live = next((r for r in rows if r["job_id"] == jid), None)
            rec = live or accounting.get(jid) or {}
            state = slurm.normalize_state(rec.get("state", "UNKNOWN"))
            code = slurm.exit_code_int(rec.get("exit_code"))
            terminal = state in slurm.TERMINAL_STATES
            out[jid] = {
                "state": state,
                "terminal": terminal,
                "failed": terminal and (state != "COMPLETED" or code not in (None, 0)),
                "exit_code": code,
            }
    return out


class JobMonitor:
    def __init__(
        self,
        store: WatchStore,
        get_cluster: Callable[[str], Cluster],
        get_host: Callable[[str], HostConfig],
    ):
        self.store, self.get_cluster, self.get_host = store, get_cluster, get_host
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.lockfile: Any = None

    def start(self) -> None:
        self.lockfile = open(self.store.root / "job-monitor.lock", "a")
        try:
            fcntl.flock(self.lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lockfile.close()
            self.lockfile = None
            return
        self.thread = threading.Thread(target=self.run, name="rs-job-monitor", daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=55)
        # Never release ownership while a worker may still be observing/notifying.
        if self.lockfile and (self.thread is None or not self.thread.is_alive()):
            self.lockfile.close()

    def run(self) -> None:
        heartbeat_at = 0.0
        while not self.stop_event.is_set():
            try:
                if time.monotonic() - heartbeat_at >= 15:
                    self.store.heartbeat()
                    heartbeat_at = time.monotonic()
                self.tick()
            except Exception as exc:
                try:
                    self.store.heartbeat(type(exc).__name__ + ": " + str(exc)[:512])
                except Exception:
                    pass
            self.stop_event.wait(1)

    def tick(self) -> None:
        now = time.time()
        due = self.store.due(now)
        if not due:
            return
        host_name = due[0]["host"]
        selected: list[dict[str, Any]] = []
        ids: set[str] = set()
        for watch in due:
            merged = ids | set(watch["job_ids"])
            if watch["host"] == host_name and len(merged) <= 1000:
                selected.append(watch)
                ids = merged
        try:
            host = self.get_host(host_name)
            matching = [w for w in selected if w["target"] == target_identity(host)]
            matching_ids = sorted({jid for watch in matching for jid in watch["job_ids"]})
            observed = {}
            if matching:
                cluster = self.get_cluster(host_name)
                if target_identity(cluster.host) != target_identity(host):
                    raise RemoteSlurmError("cached connection target changed; reconnect the host")
                observed = collect(cluster, matching_ids)
            error = None
        except Exception as exc:
            error = type(exc).__name__ + ": " + str(exc)[:512]
            observed = {}
            host = None
        for watch in selected:
            if host is not None and watch["target"] != target_identity(host):
                watch["error"] = "configured host target changed; register a new watch"
            elif error:
                watch["error"] = error
            else:
                previous = watch["observations"]
                current = {jid: dict(observed[jid]) for jid in watch["job_ids"]}
                for jid, item in current.items():
                    before = previous.get(jid, {})
                    if item["state"] == "UNKNOWN" and before.get("terminal"):
                        current[jid] = dict(before, retained=True)
                    elif set(before.get("seen_tasks", [])) - set(item.get("seen_tasks", [])):
                        item.update(
                            state="UNKNOWN",
                            terminal=False,
                            seen_tasks=sorted(
                                set(before["seen_tasks"]) | set(item.get("seen_tasks", []))
                            ),
                        )
                watch.update(observations=current, observed_at=now, error=None)
                failed = [jid for jid, value in current.items() if value["failed"]]
                complete = all(value["terminal"] for value in current.values())
                condition = watch["condition"]
                triggered = bool(failed) and condition != "all_terminal"
                if complete or triggered:
                    watch["status"] = "finished"
                    watch["result"] = {
                        "event_id": watch["watch_id"],
                        "observed_at": now,
                        "reason": "any_failed" if triggered else "all_terminal",
                        "condition_met": triggered or condition != "any_failed",
                        "failed_count": len(failed),
                        "failed_job_ids_sample": failed[:20],
                        "all_terminal": complete,
                    }
                    # Persist before delivery. A crash here is visible as uncertain delivery;
                    # no automatic replay that might generate duplicate notifications.
                    if watch["notify"]:
                        watch["delivery"] = "unknown"
            saved = self.store.save(watch, now)
            if saved and watch["status"] == "finished" and watch["notify"] and host is not None:
                from .watch import notify

                ok = notify(
                    host,
                    f"watch {watch['watch_id']}: {watch['result']['reason']} "
                    f"({len(watch['job_ids'])} jobs)",
                )
                self.store.delivered(watch["watch_id"], ok)


def watch_jobs(
    *,
    host: str | None = None,
    job_ids: list[str] | None = None,
    watch_id: str | None = None,
    action: str = "start",
    condition: str = "all_terminal_or_any_failed",
    poll: float = 60,
    notify: bool = True,
) -> dict[str, Any]:
    config = Config.load()
    hc = config.host(host)
    store = WatchStore()
    if action == "status" and watch_id:
        return store.read(watch_id, host=hc.name)
    if action == "stop" and watch_id:
        return store.stop(watch_id, host=hc.name)
    if action != "start" or not job_ids or watch_id is not None:
        raise InvalidArgument("start needs job_ids; status/stop need watch_id")
    if os.environ.get("REMOTESLURM_NO_DAEMON") or hc.ssh == "local":
        raise InvalidArgument(
            "persistent watches require the local daemon and a configured SSH host"
        )
    result = store.start(hc, job_ids, condition=condition, poll=poll, notify=notify)
    from .daemon import connect_via_daemon

    try:
        connection = connect_via_daemon(hc.name, config)
        result["observer_start"] = "requested" if connection is not None else "disabled"
        if connection is None:
            result["action"] = "enable the local daemon to observe this saved subscription"
    except RemoteSlurmError as exc:
        result["observer_start"] = "unavailable"
        result["action"] = str(exc)
    return result
