from __future__ import annotations

import base64
import binascii
import json
import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response

from .db import Database, content_hash
from .schemas import EventIn, EventOut, EventPage

DEFAULT_DB_PATH = "./data/events.db"


def row_to_event(row: sqlite3.Row) -> EventOut:
    return EventOut(
        event_id=row["event_id"],
        type=row["type"],
        payload=json.loads(row["payload"]),
        status=row["status"],
        received_at=row["received_at"],
        acked_at=row["acked_at"],
    )


def encode_cursor(row: sqlite3.Row) -> str:
    raw = json.dumps(
        {"received_at": row["received_at"], "event_id": row["event_id"]},
        separators=(",", ":"),
    )
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(token: str) -> tuple[str, str]:
    try:
        padded = token + "=" * (-len(token) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded.encode()))
        return str(data["received_at"]), str(data["event_id"])
    except (binascii.Error, ValueError, KeyError, TypeError):
        raise HTTPException(status_code=400, detail="invalid cursor")


def get_db(request: Request) -> Database:
    return request.app.state.db


@asynccontextmanager
async def lifespan(app: FastAPI):
    db_path = os.environ.get("DATABASE_PATH", DEFAULT_DB_PATH)
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    app.state.db = Database(db_path)  # creates tables on startup
    yield
    app.state.db.close()


app = FastAPI(title="Webhook Inbox", version="1.0.0", lifespan=lifespan)


@app.post("/events", response_model=EventOut, status_code=201)
def create_event(event: EventIn, response: Response, db: Database = Depends(get_db)):
    row, created = db.insert_event(event.event_id, event.type, event.payload)
    if created:
        return row_to_event(row)  # 201
    if row["content_hash"] != content_hash(event.type, event.payload):
        raise HTTPException(
            status_code=409,
            detail=f"event_id '{event.event_id}' already exists with different content",
        )
    # Idempotent replay: return the stored first version, byte-identical body.
    response.status_code = 200
    response.headers["X-Idempotent-Replay"] = "true"
    return row_to_event(row)


@app.get("/events", response_model=EventPage)
def list_events(
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = Query(default=None),
    status: Literal["pending", "acknowledged"] | None = Query(default=None),
    db: Database = Depends(get_db),
):
    position = decode_cursor(cursor) if cursor else None
    rows = db.list_events(limit=limit, cursor=position, status=status)
    page = rows[:limit]
    next_cursor = encode_cursor(page[-1]) if len(rows) > limit else None
    return EventPage(items=[row_to_event(r) for r in page], next_cursor=next_cursor)


@app.get("/events/{event_id}", response_model=EventOut)
def get_event(event_id: str, db: Database = Depends(get_db)):
    row = db.get_event(event_id)
    if row is None:
        raise HTTPException(status_code=404, detail="event not found")
    return row_to_event(row)


@app.post("/events/{event_id}/ack", response_model=EventOut)
def ack_event(event_id: str, db: Database = Depends(get_db)):
    row = db.ack_event(event_id)
    if row is None:
        raise HTTPException(status_code=404, detail="event not found")
    return row_to_event(row)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
