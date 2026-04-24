"""
test_dedup.py — Test deduplication
"""
import asyncio
import uuid
import os
import pytest
from httpx import AsyncClient, ASGITransport
from datetime import datetime, timezone

os.environ["DEDUP_DB_PATH"] = "/tmp/test_dedup.db"

from src.main import app, consumer
from src.main import dedup_store, event_store, stats


def buat_event(topic="dedup-topic", event_id=None):
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test-dedup",
        "payload": {}
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


async def test_duplikat_hanya_diproses_sekali(client):
    """Kirim event yang sama 5x → unique=1, dropped=4."""
    event = buat_event(event_id="duplikat-001")
    for _ in range(5):
        await client.post("/publish", json={"events": [event]})
    await asyncio.sleep(0.5)

    data = (await client.get("/stats")).json()
    assert data["received"] == 5
    assert data["unique_processed"] == 1
    assert data["duplicate_dropped"] == 4


async def test_event_id_sama_topic_beda_bukan_duplikat(client):
    """event_id sama tapi topic beda = BUKAN duplikat."""
    eid = str(uuid.uuid4())
    event_a = buat_event(topic="topic-A", event_id=eid)
    event_b = buat_event(topic="topic-B", event_id=eid)

    await client.post("/publish", json={"events": [event_a, event_b]})
    await asyncio.sleep(0.5)

    data = (await client.get("/stats")).json()
    assert data["unique_processed"] == 2
    assert data["duplicate_dropped"] == 0


async def test_batch_dengan_duplikat_internal(client):
    """Batch berisi 7 unik + 3 duplikat → unique=7, dropped=3."""
    events_unik = [buat_event() for _ in range(7)]
    events_dup = [
        buat_event(event_id=events_unik[0]["event_id"]),
        buat_event(event_id=events_unik[1]["event_id"]),
        buat_event(event_id=events_unik[2]["event_id"]),
    ]
    await client.post("/publish", json={"events": events_unik + events_dup})
    await asyncio.sleep(0.5)

    data = (await client.get("/stats")).json()
    assert data["unique_processed"] == 7
    assert data["duplicate_dropped"] == 3