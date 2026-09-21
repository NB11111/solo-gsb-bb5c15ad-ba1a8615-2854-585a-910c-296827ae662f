import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def make_event(i=1, **overrides):
    body = {"event_id": f"evt-{i}", "type": "payment.succeeded", "payload": {"n": i}}
    body.update(overrides)
    return body


def test_create_event(client):
    r = client.post("/events", json=make_event())
    assert r.status_code == 201
    body = r.json()
    assert body["event_id"] == "evt-1"
    assert body["status"] == "pending"
    assert body["acked_at"] is None
    assert body["received_at"].endswith("Z")


def test_duplicate_same_content_returns_first_version(client):
    first = client.post("/events", json=make_event())
    replay = client.post("/events", json=make_event())
    assert first.status_code == 201
    assert replay.status_code == 200
    assert replay.headers["X-Idempotent-Replay"] == "true"
    assert replay.json() == first.json()  # consistent result, first version kept


def test_same_id_different_content_conflicts(client):
    client.post("/events", json=make_event())
    r = client.post("/events", json=make_event(payload={"n": 999}))
    assert r.status_code == 409
    r2 = client.post("/events", json=make_event(type="payment.failed"))
    assert r2.status_code == 409
    # original version is untouched
    assert client.get("/events/evt-1").json()["payload"] == {"n": 1}


def test_payload_key_order_does_not_matter(client):
    client.post("/events", json=make_event(payload={"a": 1, "b": 2}))
    r = client.post("/events", json=make_event(payload={"b": 2, "a": 1}))
    assert r.status_code == 200


def test_get_single_and_404(client):
    client.post("/events", json=make_event())
    assert client.get("/events/evt-1").status_code == 200
    assert client.get("/events/nope").status_code == 404


def test_ack_flow(client):
    client.post("/events", json=make_event())
    r = client.post("/events/evt-1/ack")
    assert r.status_code == 200
    assert r.json()["status"] == "acknowledged"
    assert r.json()["acked_at"] is not None
    # ack is idempotent
    again = client.post("/events/evt-1/ack")
    assert again.status_code == 200
    assert again.json() == r.json()
    assert client.post("/events/nope/ack").status_code == 404


def test_pagination_with_cursor(client):
    for i in range(7):
        client.post("/events", json=make_event(i))

    seen, cursor = [], None
    for _ in range(10):  # safety bound
        params = {"limit": 3}
        if cursor:
            params["cursor"] = cursor
        page = client.get("/events", params=params).json()
        seen.extend(e["event_id"] for e in page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert seen == [f"evt-{i}" for i in range(7)]  # no dups, no gaps, time order


def test_status_filter_and_invalid_cursor(client):
    for i in range(4):
        client.post("/events", json=make_event(i))
    client.post("/events/evt-0/ack")
    client.post("/events/evt-2/ack")

    pending = client.get("/events", params={"status": "pending"}).json()
    assert [e["event_id"] for e in pending["items"]] == ["evt-1", "evt-3"]
    acked = client.get("/events", params={"status": "acknowledged"}).json()
    assert [e["event_id"] for e in acked["items"]] == ["evt-0", "evt-2"]

    assert client.get("/events", params={"cursor": "!!not-a-cursor!!"}).status_code == 400
    assert client.get("/events", params={"limit": 0}).status_code == 422


def test_validation_errors(client):
    assert client.post("/events", json={"event_id": "x"}).status_code == 422
    assert client.post("/events", json=make_event(event_id="")).status_code == 422


@pytest.fixture()
def live_server(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "live.db"))
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config("app.main:app", host="127.0.0.1", port=port, log_level="error")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    for _ in range(200):
        try:
            httpx.get(f"{base}/healthz", timeout=0.2)
            break
        except httpx.TransportError:
            time.sleep(0.05)
    else:
        raise RuntimeError("server did not start")
    yield base
    server.should_exit = True
    thread.join(timeout=5)


def test_concurrent_duplicate_submissions(live_server):
    event = make_event(42)

    def submit(_):
        r = httpx.post(f"{live_server}/events", json=event, timeout=10)
        return r.status_code, r.json()

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(submit, range(32)))

    codes = [c for c, _ in results]
    assert codes.count(201) == 1
    assert codes.count(200) == 31
    bodies = {str(b) for _, b in results}
    assert len(bodies) == 1  # every client sees the same stored version
    assert httpx.get(f"{live_server}/events").json()["items"].__len__() == 1


def test_concurrent_conflicting_submissions(live_server):
    def submit(i):
        r = httpx.post(
            f"{live_server}/events",
            json=make_event(7, payload={"n": i}),  # same id, different content
            timeout=10,
        )
        return r.status_code

    with ThreadPoolExecutor(max_workers=16) as pool:
        codes = list(pool.map(submit, range(32)))

    assert codes.count(201) == 1
    assert codes.count(409) == 31
    items = httpx.get(f"{live_server}/events").json()["items"]
    assert len(items) == 1  # only the first version was kept
