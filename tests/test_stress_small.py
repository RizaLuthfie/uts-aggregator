"""
test_stress_small.py — Stress test 5000 event
"""
import asyncio
import uuid
import os
import time
import pytest
from httpx import AsyncClient, ASGITransport
from datetime import datetime, timezone

os.environ["DEDUP_DB_PATH"] = "/tmp/test_stress.db"

from src.main import app, consumer
from src.main import dedup_store, event_store, stats


def buat_event(topic="stress-topic", event_id=None):
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "stress-test",
        "payload": {"data": "test"}
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


async def test_5000_event_dengan_25_persen_duplikat(client):
    """5000 event (3750 unik + 1250 duplikat) dalam < 30 detik."""
    TOTAL_UNIK = 3750
    TOTAL_DUPLIKAT = 1250
    BATCH_SIZE = 500

    event_unik = [buat_event() for _ in range(TOTAL_UNIK)]
    event_duplikat = [
        buat_event(event_id=event_unik[i % TOTAL_UNIK]["event_id"])
        for i in range(TOTAL_DUPLIKAT)
    ]
    semua_event = event_unik + event_duplikat

    waktu_mulai = time.time()

    for i in range(0, len(semua_event), BATCH_SIZE):
        batch = semua_event[i:i + BATCH_SIZE]
        resp = await client.post("/publish", json={"events": batch})
        assert resp.status_code == 202

    await asyncio.wait_for(consumer.queue.join(), timeout=25)

    durasi = time.time() - waktu_mulai
    data = (await client.get("/stats")).json()

    print(f"\n  Unik diproses    : {data['unique_processed']}")
    print(f"  Duplikat di-drop : {data['duplicate_dropped']}")
    print(f"  Durasi           : {durasi:.2f} detik")

    assert data["unique_processed"] == TOTAL_UNIK
    assert data["duplicate_dropped"] == TOTAL_DUPLIKAT
    assert durasi < 30