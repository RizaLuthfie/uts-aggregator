"""
test_api.py — Test endpoint API
"""
import asyncio
import uuid
import os
import pytest
from httpx import AsyncClient, ASGITransport
from datetime import datetime, timezone

os.environ["DEDUP_DB_PATH"] = "/tmp/test_api.db"

from src.main import app, consumer
from src.main import dedup_store, event_store, stats


def buat_event(topic="test-topic", event_id=None, source="test"):
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "payload": {"level": "INFO"}
    }


@pytest.fixture(autouse=True)
def reset():
    dedup_store.clear()
    event_store._store.clear()
    stats.received = 0
    stats.unique_processed = 0
    stats.duplicate_dropped = 0
    yield


@pytest.fixture
async def client():
    consumer.queue = asyncio.Queue()
    consumer._running = False
    task = asyncio.create_task(consumer.start())
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test"
    ) as ac:
        yield ac
    consumer.stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def test_health_check(client):
    """GET /health harus return status ok."""
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


async def test_publish_single_event(client):
    """POST /publish dengan 1 event harus return accepted=1."""
    resp = await client.post(
        "/publish",
        json={"events": [buat_event()]}
    )
    assert resp.status_code == 202
    assert resp.json()["accepted"] == 1


async def test_publish_batch_event(client):
    """POST /publish dengan 50 event sekaligus."""
    events = [buat_event() for _ in range(50)]
    resp = await client.post("/publish", json={"events": events})
    assert resp.status_code == 202
    assert resp.json()["accepted"] == 50


async def test_get_events_by_topic(client):
    """GET /events?topic=X hanya mengembalikan event dari topic X."""
    await client.post("/publish", json={
        "events": [buat_event("topic-A") for _ in range(5)]
    })
    await client.post("/publish", json={
        "events": [buat_event("topic-B") for _ in range(3)]
    })
    await asyncio.sleep(0.5)

    resp_a = await client.get("/events?topic=topic-A")
    resp_b = await client.get("/events?topic=topic-B")

    assert resp_a.json()["count"] == 5
    assert resp_b.json()["count"] == 3


async def test_get_events_semua_topic(client):
    """GET /events tanpa filter mengembalikan semua topic."""
    await client.post("/publish", json={
        "events": [buat_event("topik-1"), buat_event("topik-2")]
    })
    await asyncio.sleep(0.3)

    resp = await client.get("/events")
    data = resp.json()
    assert "total_unique" in data
    assert "topics" in data