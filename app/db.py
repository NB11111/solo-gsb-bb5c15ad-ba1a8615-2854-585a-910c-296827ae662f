"""SQLite persistence layer for the webhook inbox.

A single connection guarded by a lock keeps the insert-or-get critical
section atomic inside the process; WAL mode + busy_timeout keep the file
safe for concurrent readers and external processes.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id     TEXT PRIMARY KEY,
    type         TEXT NOT NULL,
    payload      TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'acknowledged')),
    received_at  TEXT NOT NULL,
    acked_at     TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_received ON events (received_at, event_id);
CREATE INDEX IF NOT EXISTS idx_events_status ON events (status);
"""


def utcnow_iso() -> str:
    """Fixed-width UTC ISO-8601 (microseconds, 'Z' suffix).

    Fixed width makes lexicographic string order equal chronological order,
    which the time-cursor pagination relies on.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def canonical_payload(payload: Any) -> str:
    """Deterministic JSON serialization (sorted keys, compact separators)."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def content_hash(event_type: str, payload: Any) -> str:
    """Fingerprint of the event content, excluding the event_id."""
    return sha256(f"{event_type}\n{canonical_payload(payload)}".encode("utf-8")).hexdigest()


class Database:
    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout = 5000")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._lock = threading.Lock()
        with self._lock, self._conn:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def insert_event(self, event_id: str, event_type: str, payload: Any) -> tuple[sqlite3.Row, bool]:
        """Insert or get. Returns (stored_row, created).

        INSERT OR IGNORE is atomic at the SQLite level; the lock additionally
        serializes the insert/read-back pair within this process.
        """
        digest = content_hash(event_type, payload)
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO events (event_id, type, payload, content_hash, received_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (event_id, event_type, canonical_payload(payload), digest, utcnow_iso()),
            )
            created = cur.rowcount == 1
            row = self._conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
        return row, created

    def get_event(self, event_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()

    def list_events(
        self,
        *,
        limit: int,
        cursor: tuple[str, str] | None = None,
        status: str | None = None,
    ) -> list[sqlite3.Row]:
        """Return up to limit+1 rows after the (received_at, event_id) cursor."""
        conds: list[str] = []
        params: list[Any] = []
        if status is not None:
            conds.append("status = ?")
            params.append(status)
        if cursor is not None:
            conds.append("(received_at > ? OR (received_at = ? AND event_id > ?))")
            params.extend([cursor[0], cursor[0], cursor[1]])
        sql = "SELECT * FROM events"
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY received_at, event_id LIMIT ?"
        params.append(limit + 1)
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def ack_event(self, event_id: str) -> sqlite3.Row | None:
        """Mark acknowledged (idempotent). Returns the row, or None if missing."""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE events SET status = 'acknowledged', acked_at = ?"
                " WHERE event_id = ? AND status = 'pending'",
                (utcnow_iso(), event_id),
            )
            return self._conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
