"""
consumer.py
-----------
Tiga komponen utama sistem:

1. EventStore
   Menyimpan event unik yang sudah diproses di memori (RAM).
   Diorganisir per topic untuk query GET /events?topic=...

2. AggregatorStats
   Menghitung statistik: received, unique_processed,
   duplicate_dropped, uptime.

3. QueueConsumer
   Worker yang berjalan di background (asyncio task).
   Membaca event dari asyncio.Queue satu per satu,
   lalu memproses dengan cek deduplication.
   Bersifat IDEMPOTEN: event yang sama diproses berkali-kali
   akan menghasilkan state yang sama (hanya disimpan 1x).
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List

from src.models import Event
from src.storage import DedupStore

logger = logging.getLogger(__name__)


class EventStore:
    """Penyimpanan in-memory event unik, dikelompokkan per topic."""

    def __init__(self):
        self._store: Dict[str, List[dict]] = {}

    def add(self, event: Event):
        if event.topic not in self._store:
            self._store[event.topic] = []
        self._store[event.topic].append(event.model_dump())

    def get_by_topic(self, topic: str) -> List[dict]:
        return self._store.get(topic, [])

    def get_all_topics(self) -> List[str]:
        return list(self._store.keys())

    def total_unique(self) -> int:
        return sum(len(v) for v in self._store.values())


class AggregatorStats:
    """Statistik aggregator untuk endpoint GET /stats."""

    def __init__(self):
        self.received: int = 0
        self.unique_processed: int = 0
        self.duplicate_dropped: int = 0
        self.start_time: float = time.time()

    def uptime_seconds(self) -> float:
        return round(time.time() - self.start_time, 2)


class QueueConsumer:
    """
    Async consumer yang memproses event dari asyncio.Queue.

    ALUR KERJA:
    Publisher kirim event → enqueue() → asyncio.Queue
    → _process() → cek DedupStore
        → DUPLIKAT : drop + log warning
        → UNIK     : simpan ke EventStore + catat di DedupStore
    """

    def __init__(
        self,
        dedup_store: DedupStore,
        event_store: EventStore,
        stats: AggregatorStats
    ):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.dedup_store = dedup_store
        self.event_store = event_store
        self.stats = stats
        self._running = False

    async def enqueue(self, event: Event):
        """
        Masukkan event ke queue.
        Dipanggil oleh endpoint POST /publish.
        """
        self.stats.received += 1
        await self.queue.put(event)

    async def start(self):
        """
        Jalankan loop consumer di background.
        Berjalan terus sampai stop() dipanggil.
        """
        self._running = True
        logger.info("[CONSUMER] Worker mulai berjalan.")
        while self._running:
            try:
                event: Event = await asyncio.wait_for(
                    self.queue.get(), timeout=1.0
                )
                await self._process(event)
                self.queue.task_done()
            except asyncio.TimeoutError:
                # Tidak ada event di queue, tunggu lagi
                continue

    async def _process(self, event: Event):
        """
        Proses satu event:
        1. Coba tandai sebagai diproses di DedupStore
        2. Jika berhasil (unik) → simpan ke EventStore
        3. Jika gagal (duplikat) → drop, tambah counter
        """
        received_at = datetime.now(timezone.utc).isoformat()

        is_new = self.dedup_store.mark_processed(
            topic=event.topic,
            event_id=event.event_id,
            received_at=received_at
        )

        if is_new:
            self.event_store.add(event)
            self.stats.unique_processed += 1
            logger.info(
                f"[DIPROSES] topic={event.topic!r} "
                f"event_id={event.event_id!r}"
            )
        else:
            self.stats.duplicate_dropped += 1

    def stop(self):
        """Hentikan consumer loop."""
        self._running = False
        logger.info("[CONSUMER] Worker dihentikan.")