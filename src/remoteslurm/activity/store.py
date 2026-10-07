"""Local presentation journal. No remote operations or execution decisions live here."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sqlite3
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..config import state_dir
from ..errors import InvalidArgument, RegistryUnavailable, StoreConflict, TooLarge
from .model import ActivityEvent, EvidenceRef, Subject

VERSION = 1
MAX_EVENT_BYTES = 64 * 1024
MAX_DOCUMENT_BYTES = 2 * 1024 * 1024
MAX_DOCUMENT_STORAGE = 128 * 1024 * 1024


def encoded(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )


def stamp(now: float | None = None) -> str:
    return datetime.fromtimestamp(time.time() if now is None else now, UTC).isoformat()


class ActivityStore:
    """Short-lived connections permit independent CLI, MCP, daemon and UI processes.

    Retention bounds retained event payloads. SQLite reuses freed pages; physical disk
    allocation and document storage are reported separately, never called an event cap.
    """

    def __init__(
        self,
        root: Path | None = None,
        *,
        max_bytes: int = 256 * 1024 * 1024,
        max_age: float = 30 * 86400,
    ) -> None:
        self.root = (root or state_dir()) / "activity"
        self.path = self.root / "journal.sqlite3"
        self.max_bytes, self.max_age = max_bytes, max_age
        if max_bytes < MAX_EVENT_BYTES or max_age < 0:
            raise InvalidArgument("invalid activity retention bounds")
        try:
            self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
            # Create privately before SQLite creates the WAL/SHM beside it.
            fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
            os.close(fd)
            with self.connection() as db:
                db.execute("PRAGMA journal_mode=WAL")
                db.execute("BEGIN IMMEDIATE")
                version = db.execute("PRAGMA user_version").fetchone()[0]
                if version not in (0, VERSION):
                    raise RegistryUnavailable(f"unsupported activity schema {version}")
                if version == 0:
                    for sql in (
                        "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
                        "CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT, "
                        "event_id TEXT UNIQUE NOT NULL, source_key TEXT UNIQUE NOT NULL, "
                        "digest TEXT NOT NULL, target TEXT NOT NULL, host TEXT NOT NULL, "
                        "workspace TEXT, recorded REAL NOT NULL, data TEXT NOT NULL, "
                        "bytes INTEGER)",
                        "CREATE INDEX event_scope ON events(target, workspace, seq)",
                        "CREATE TABLE documents (namespace TEXT, key TEXT, data TEXT NOT NULL, "
                        "updated REAL NOT NULL, PRIMARY KEY(namespace,key))",
                    ):
                        db.execute(sql)
                    db.executemany(
                        "INSERT INTO meta VALUES (?,?)",
                        [
                            ("journal", uuid.uuid4().hex),
                            ("secret", uuid.uuid4().hex),
                            ("pruned", "0"),
                        ],
                    )
                    db.execute(f"PRAGMA user_version={VERSION}")
                db.commit()
        except (OSError, sqlite3.Error) as exc:
            raise RegistryUnavailable("activity journal unavailable", path=str(self.path)) from exc

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=2, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA synchronous=FULL")
            yield db
        finally:
            db.close()  # Rolls back any incomplete transaction, including BaseException.

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        try:
            with self.connection() as db:
                db.execute("BEGIN IMMEDIATE")
                yield db
                db.commit()
        except sqlite3.Error as exc:
            raise RegistryUnavailable("activity transaction failed", path=str(self.path)) from exc

    @staticmethod
    def put(db: sqlite3.Connection, namespace: str, key: str, value: dict[str, Any]) -> None:
        data = encoded(value)
        if len(data.encode()) > MAX_DOCUMENT_BYTES:
            raise TooLarge("activity document exceeds 2 MiB")
        total = db.execute(
            "SELECT coalesce(sum(length(CAST(data AS BLOB))),0) FROM documents "
            "WHERE NOT(namespace=? AND key=?)",
            (namespace, key),
        ).fetchone()[0]
        if total + len(data.encode()) > MAX_DOCUMENT_STORAGE:
            raise TooLarge("activity documents exhausted the 128 MiB storage budget")
        db.execute(
            "INSERT INTO documents VALUES (?,?,?,?) ON CONFLICT(namespace,key) DO UPDATE "
            "SET data=excluded.data, updated=excluded.updated",
            (namespace, key, data, time.time()),
        )

    def save(self, namespace: str, key: str, value: dict[str, Any]) -> None:
        with self.transaction() as db:
            self.put(db, namespace, key, value)

    @staticmethod
    def get_in(db: sqlite3.Connection, namespace: str, key: str) -> dict[str, Any] | None:
        row = db.execute(
            "SELECT data FROM documents WHERE namespace=? AND key=?", (namespace, key)
        ).fetchone()
        return json.loads(row[0]) if row else None

    def get(self, namespace: str, key: str) -> dict[str, Any] | None:
        with self.connection() as db:
            return self.get_in(db, namespace, key)

    def documents(self, namespace: str, *, limit: int = 100, offset: int = 0) -> list[dict]:
        if not 1 <= limit <= 1000 or offset < 0:
            raise InvalidArgument("invalid document page")
        with self.connection() as db:
            return [
                json.loads(row[0])
                for row in db.execute(
                    "SELECT data FROM documents WHERE namespace=? ORDER BY key LIMIT ? OFFSET ?",
                    (namespace, limit, offset),
                )
            ]

    def append(
        self,
        *,
        subject: Subject,
        kind: str,
        payload: dict[str, Any],
        source: str,
        provenance: Any = "observation",
        freshness: Any = "FRESH",
        observed_at: str | None = None,
        source_key: str | None = None,
        workspace: str | None = None,
        evidence: tuple[EvidenceRef, ...] = (),
        actor: str | None = None,
        operation_id: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        projection_version: int | None = None,
        occurred_at: str | None = None,
        checkpoint: tuple[str, dict] | None = None,
        documents: tuple[tuple[str, str, dict], ...] = (),
        now: float | None = None,
    ) -> dict:
        now = time.time() if now is None else now
        event_id = uuid.uuid4().hex
        key = source_key or event_id
        payload_json = encoded(payload)
        # Identity is based on source facts, not the time we happened to ingest them.
        digest = hashlib.sha256(
            encoded(
                [
                    asdict(subject),
                    kind,
                    payload,
                    source,
                    workspace,
                    provenance,
                    freshness,
                    observed_at,
                    occurred_at,
                    [asdict(e) for e in evidence],
                ]
            ).encode()
        ).hexdigest()
        with self.transaction() as db:
            previous = db.execute(
                "SELECT digest,data FROM events WHERE source_key=?", (key,)
            ).fetchone()
            if previous:
                if previous[0] != digest:
                    raise StoreConflict("activity source key reused for different evidence")
                result = json.loads(previous[1])
            else:
                journal = db.execute("SELECT value FROM meta WHERE key='journal'").fetchone()[0]
                seq = db.execute(
                    "INSERT INTO events(event_id,source_key,digest,target,host,workspace,recorded,"
                    "data,bytes) VALUES (?,?,?,?,?,?,?,'',0)",
                    (event_id, key, digest, subject.target_id, subject.host_alias, workspace, now),
                ).lastrowid
                event = ActivityEvent(
                    event_id,
                    journal,
                    int(seq or 0),
                    kind,
                    subject,
                    provenance,
                    source,
                    stamp(now),
                    observed_at or (stamp(now) if freshness == "FRESH" else None),
                    evidence,
                    payload_json,
                    freshness,
                    actor=actor,
                    operation_id=operation_id,
                    occurred_at=occurred_at,
                    correlation_id=correlation_id or operation_id,
                    causation_id=causation_id,
                    projection_version=projection_version,
                )
                result = event.to_dict()
                result["workspace_id"] = workspace
                data = encoded(result)
                result = json.loads(data)
                size = len(data.encode())
                if size > MAX_EVENT_BYTES:
                    raise TooLarge("activity event exceeds 64 KiB")
                db.execute("UPDATE events SET data=?,bytes=? WHERE seq=?", (data, size, seq))
                for namespace, doc_key, value in documents:
                    self.put(db, namespace, doc_key, value)
            if checkpoint:
                self.put(db, "checkpoint", *checkpoint)
            self._prune(db, now)
            return result

    def _prune(self, db: sqlite3.Connection, now: float) -> None:
        total = db.execute("SELECT coalesce(sum(bytes),0) FROM events").fetchone()[0]
        last = int(db.execute("SELECT value FROM meta WHERE key='pruned'").fetchone()[0])
        # Delete a contiguous prefix; projections/checkpoints retain current facts.
        for row in db.execute("SELECT seq,recorded,bytes FROM events ORDER BY seq"):
            if total <= self.max_bytes and row[1] >= now - self.max_age:
                break
            total -= row[2]
            last = row[0]
        db.execute("DELETE FROM events WHERE seq<=?", (last,))
        db.execute("UPDATE meta SET value=? WHERE key='pruned'", (str(last),))

    def prune(self, *, now: float | None = None) -> None:
        with self.transaction() as db:
            self._prune(db, time.time() if now is None else now)

    @staticmethod
    def _cursor(meta: dict, seq: int, scope: dict) -> str:
        data = encoded({"journal": meta["journal"], "seq": seq, "scope": scope}).encode()
        signature = hmac.new(meta["secret"].encode(), data, hashlib.sha256).hexdigest().encode()
        return base64.urlsafe_b64encode(data + b"." + signature).decode()

    def query(
        self,
        *,
        target: str | None = None,
        host: str | None = None,
        workspace: str | None = None,
        after: str | None = None,
        limit: int = 100,
        max_bytes: int = 128 * 1024,
        since: float | None = None,
        snapshot: tuple[str, str] | None = None,
    ) -> dict:
        if not 1 <= limit <= 1000 or not MAX_EVENT_BYTES + 4096 <= max_bytes <= 1024 * 1024:
            raise InvalidArgument("invalid activity query bounds")
        if after and since is not None:
            raise InvalidArgument("use either after or since")
        scope = {"target": target, "host": host, "workspace": workspace}
        with self.connection() as db:
            db.execute("BEGIN")
            meta = dict(db.execute("SELECT key,value FROM meta"))
            high = max(
                int(meta["pruned"]),
                db.execute("SELECT coalesce(max(seq),0) FROM events").fetchone()[0],
            )
            start = 0
            if after:
                try:
                    raw, signature = base64.urlsafe_b64decode(after.encode()).rsplit(b".", 1)
                    wanted = hmac.new(meta["secret"].encode(), raw, hashlib.sha256).hexdigest()
                    cursor = json.loads(raw)
                    if not hmac.compare_digest(signature.decode(), wanted):
                        raise ValueError("signature")
                    if cursor["journal"] != meta["journal"] or cursor["scope"] != scope:
                        raise ValueError("scope")
                    start = cursor["seq"]
                    if type(start) is not int or not 0 <= start <= high:
                        raise ValueError("sequence")
                except (ValueError, KeyError, TypeError, UnicodeError) as exc:
                    raise InvalidArgument("invalid activity cursor or changed filter") from exc
            gap = bool(after and start < int(meta["pruned"]))
            clauses = ["seq>?"]
            params: list[Any] = [start]
            for column, value in scope.items():
                if value is not None:
                    clauses.append(f"{column}=?")
                    params.append(value)
            if since is not None:
                clauses.append("recorded>=?")
                params.append(since)
            direction = "ASC" if after or since is not None else "DESC"
            rows = list(
                db.execute(
                    "SELECT seq,data,bytes FROM events WHERE "
                    + " AND ".join(clauses)
                    + f" ORDER BY seq {direction} LIMIT ?",
                    [*params, limit + 1],
                )
            )
            captured = self.get_in(db, *snapshot) if snapshot else None
            snapshot_bytes = len(encoded(captured).encode())
            snapshot_omitted = snapshot_bytes > max_bytes - MAX_EVENT_BYTES - 4096
            if snapshot_omitted:
                captured, snapshot_bytes = None, 4
            events, used, last = [], snapshot_bytes, start
            for row in rows[:limit]:
                if used + row[2] > max_bytes - 4096:
                    break
                events.append(json.loads(row[1]))
                used += row[2]
                last = row[0]
            more = len(rows) > len(events)
            if direction == "DESC":
                events.reverse()
                last = high
            elif not more:
                last = high
            from .semantic import attach_events

            return attach_events(
                self,
                {
                    "schema_version": VERSION,
                    "journal_id": meta["journal"],
                    "events": events,
                    "next_cursor": self._cursor(meta, last, scope),
                    "has_more": more if direction == "ASC" else False,
                    "history_truncated": more if direction == "DESC" else False,
                    "history_gap": gap,
                    "resync_required": gap,
                    "resync_cursor": self._cursor(meta, high, scope) if gap else None,
                    "retained_after_seq": int(meta["pruned"]),
                    "observed_at": stamp(),
                    "snapshot": captured,
                    "snapshot_omitted": snapshot_omitted,
                },
                max_bytes=max_bytes,
            )

    def health(self) -> dict:
        with self.connection() as db:
            return {
                "path": str(self.path),
                "schema_version": VERSION,
                "event_count": db.execute("SELECT count(*) FROM events").fetchone()[0],
                "event_bytes": db.execute("SELECT coalesce(sum(bytes),0) FROM events").fetchone()[
                    0
                ],
                "event_budget_bytes": self.max_bytes,
                "document_bytes": db.execute(
                    "SELECT coalesce(sum(length(CAST(data AS BLOB))),0) FROM documents"
                ).fetchone()[0],
                "document_budget_bytes": MAX_DOCUMENT_STORAGE,
                "allocated_bytes": sum(
                    p.stat().st_size for p in self.root.glob("journal.sqlite3*")
                ),
                "retained_after_seq": int(
                    db.execute("SELECT value FROM meta WHERE key='pruned'").fetchone()[0]
                ),
            }
