"""
routes.py
---------
Semua endpoint REST API aggregator.

DAFTAR ENDPOINT:
  POST /publish          → terima event (batch atau single)
  GET  /events?topic=X   → lihat event unik per topic
  GET  /stats            → statistik aggregator
  GET  /health           → cek sistem hidup atau tidak
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from src.models import PublishRequest
from src.consumer import QueueConsumer, EventStore, AggregatorStats

logger = logging.getLogger(__name__)
router = APIRouter()

# Variabel global, akan diisi oleh main.py saat startup
_consumer: Optional[QueueConsumer] = None
_event_store: Optional[EventStore] = None
_stats: Optional[AggregatorStats] = None


def init_routes(
    consumer: QueueConsumer,
    event_store: EventStore,
    stats: AggregatorStats
):
    """Dipanggil dari main.py untuk inject dependency."""
    global _consumer, _event_store, _stats
    _consumer = consumer
    _event_store = event_store
    _stats = stats


@router.post("/publish", status_code=202)
async def publish(request: PublishRequest):
    """
    Terima batch atau single event dari publisher.

    - Validasi skema dilakukan otomatis oleh Pydantic
    - Event yang tidak valid → HTTP 422 otomatis
    - Event valid → masuk asyncio.Queue untuk diproses
    - Return HTTP 202 (Accepted): event diterima tapi
      belum tentu selesai diproses (async)
    """
    if not _consumer:
        raise HTTPException(status_code=503, detail="Service belum siap")

    jumlah = len(request.events)
    for event in request.events:
        await _consumer.enqueue(event)

    logger.info(f"[PUBLISH] {jumlah} event diterima, masuk ke queue.")
    return {
        "accepted": jumlah,
        "message": f"{jumlah} event diterima dan sedang diproses."
    }


@router.get("/events")
async def get_events(
    topic: Optional[str] = Query(
        None,
        description="Filter event berdasarkan nama topic"
    )
):
    """
    Kembalikan daftar event unik yang sudah diproses.

    Contoh:
      GET /events              → semua event, semua topic
      GET /events?topic=app-logs → hanya event dari topic app-logs
    """
    if not _event_store:
        raise HTTPException(status_code=503, detail="Service belum siap")

    if topic:
        events = _event_store.get_by_topic(topic)
        return {
            "topic": topic,
            "count": len(events),
            "events": events
        }
    else:
        semua_topic = _event_store.get_all_topics()
        hasil = {t: _event_store.get_by_topic(t) for t in semua_topic}
        return {
            "total_unique": _event_store.total_unique(),
            "topics": hasil
        }


@router.get("/stats")
async def get_stats():
    """
    Statistik aggregator:
    - received         : total event diterima (termasuk duplikat)
    - unique_processed : event unik yang berhasil diproses
    - duplicate_dropped: event duplikat yang di-drop
    - topics           : daftar topic yang dikenal sistem
    - uptime_seconds   : berapa lama sistem sudah berjalan
    """
    if not _stats or not _event_store:
        raise HTTPException(status_code=503, detail="Service belum siap")

    return {
        "received": _stats.received,
        "unique_processed": _stats.unique_processed,
        "duplicate_dropped": _stats.duplicate_dropped,
        "topics": _event_store.get_all_topics(),
        "uptime_seconds": _stats.uptime_seconds()
    }


@router.get("/health")
async def health():
    """Cek apakah service berjalan normal."""
    return {"status": "ok"}