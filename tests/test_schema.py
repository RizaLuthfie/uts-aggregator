"""
test_schema.py — Test validasi skema event
"""
import asyncio
import uuid
import os
import pytest
from httpx import AsyncClient, ASGITransport
from datetime import datetime, timezone

os.environ["DEDUP_DB_PATH"] = "/tmp/test_schema.db"

from src.main import app, consumer
from src.main import dedup_store, event_store, stats


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


async def test_event_valid_diterima(client):
    """Event dengan semua field benar harus diterima (202)."""
    event = {
        "topic": "valid-topic",
        "event_id": str(uuid.uuid4()),
        "timestamp": "2024-01-01T10:00:00Z",
        "source": "service-test",
        "payload": {"key": "value"}
    }
    resp = await client.post("/publish", json={"events": [event]})
    assert resp.status_code == 202


async def test_topic_kosong_ditolak(client):
    """Event dengan topic kosong harus ditolak (422)."""
    event = {
        "topic": "",
        "event_id": str(uuid.uuid4()),
        "timestamp": "2024-01-01T10:00:00Z",
        "source": "test",
        "payload": {}
    }
    resp = await client.post("/publish", json={"events": [event]})
    assert resp.status_code == 422


async def test_event_id_kosong_ditolak(client):
    """Event dengan event_id kosong harus ditolak (422)."""
    event = {
        "topic": "test-topic",
        "event_id": "",
        "timestamp": "2024-01-01T10:00:00Z",
        "source": "test",
        "payload": {}
    }
    resp = await client.post("/publish", json={"events": [event]})
    assert resp.status_code == 422


async def test_timestamp_salah_format_ditolak(client):
    """Timestamp bukan ISO 8601 harus ditolak (422)."""
    event = {
        "topic": "test-topic",
        "event_id": str(uuid.uuid4()),
        "timestamp": "24-04-2026 07:00:00",
        "source": "test",
        "payload": {}
    }
    resp = await client.post("/publish", json={"events": [event]})
    assert resp.status_code == 422


async def test_field_topic_tidak_ada_ditolak(client):
    """Event tanpa field topic harus ditolak (422)."""
    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": "2024-01-01T10:00:00Z",
        "source": "test",
        "payload": {}
    }
    resp = await client.post("/publish", json={"events": [event]})
    assert resp.status_code == 422