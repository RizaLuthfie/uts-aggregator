"""
main.py
-------
Entry point aplikasi Pub-Sub Log Aggregator.

URUTAN INISIALISASI SAAT STARTUP:
1. DedupStore  → koneksi ke SQLite
2. EventStore  → siapkan penyimpanan in-memory
3. AggregatorStats → mulai hitung uptime
4. QueueConsumer → siap menerima event
5. FastAPI app → mulai consumer worker sebagai background task
6. Routes → endpoint siap menerima request

CARA JALANKAN (lokal):
  python -m src.main

CARA JALANKAN (Docker):
  docker build -t uts-aggregator .
  docker run -p 8080:8080 -v uts-data:/app/data uts-aggregator
"""

import asyncio
import logging
import os
import sys

import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI

from src.storage import DedupStore
from src.consumer import QueueConsumer, EventStore, AggregatorStats
from src.routes import router, init_routes

# ── Setup logging ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ── Inisialisasi komponen global ───────────────────────────────
dedup_store = DedupStore()
event_store = EventStore()
stats       = AggregatorStats()
consumer    = QueueConsumer(
    dedup_store=dedup_store,
    event_store=event_store,
    stats=stats
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle manager FastAPI:
    - STARTUP  : jalankan consumer worker sebagai background task
    - SHUTDOWN : hentikan consumer dengan bersih
    """
    logger.info("=" * 50)
    logger.info("  Pub-Sub Log Aggregator — STARTING UP")
    logger.info("=" * 50)

    task = asyncio.create_task(consumer.start())
    logger.info("[MAIN] Consumer worker berjalan di background.")

    yield  # aplikasi berjalan di sini

    logger.info("[MAIN] Shutting down...")
    consumer.stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("[MAIN] Consumer worker dihentikan. Bye!")


# ── Buat aplikasi FastAPI ──────────────────────────────────────
app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description=(
        "UTS Sistem Terdistribusi dan Parallel — "
        "Idempotent Consumer dengan Persistent Deduplication"
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)

# ── Inisialisasi routes di level module ────────────────────────
# Dipanggil di sini (bukan di dalam lifespan) supaya saat testing
# tanpa lifespan, routes sudah punya dependency yang di-inject.
init_routes(consumer=consumer, event_store=event_store, stats=stats)


# ── Jalankan dengan uvicorn ────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info"
    )