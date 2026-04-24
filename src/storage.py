"""
storage.py
----------
Persistent deduplication store menggunakan SQLite.

KENAPA SQLite?
- Lokal, tidak butuh server database eksternal
- Data tersimpan di file disk, tahan restart container
- Mendukung transaksi ACID (atomic, tidak korup saat crash)
- Lookup cepat dengan index pada kolom (topic, event_id)

CARA KERJA:
- Setiap event yang diproses dicatat di tabel 'processed_events'
- Primary Key adalah pasangan (topic, event_id)
- Jika event yang sama dikirim lagi, INSERT akan gagal
  karena Primary Key sudah ada → event dianggap duplikat → di-drop
"""

import sqlite3
import logging
import os
from threading import Lock

logger = logging.getLogger(__name__)

# Path database bisa diubah via environment variable
# Default: /app/data/dedup.db (di dalam container Docker)
DB_PATH = os.environ.get("DEDUP_DB_PATH", "/app/data/dedup.db")


class DedupStore:

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._lock = Lock()  # mencegah konflik saat banyak thread
        # Buat folder jika belum ada
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        return self._conn

    def _init_db(self):
        """Buat tabel dan index jika belum ada."""
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
                    topic       TEXT NOT NULL,
                    event_id    TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    PRIMARY KEY (topic, event_id)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_topic_event
                ON processed_events (topic, event_id)
            """)
            conn.commit()
        logger.info(f"[STORAGE] DedupStore siap di: {self.db_path}")

    def is_duplicate(self, topic: str, event_id: str) -> bool:
        """
        Cek apakah event ini sudah pernah diproses.
        Return True = duplikat, False = baru/unik.
        """
        with self._lock:
            with self._get_conn() as conn:
                cur = conn.execute(
                    "SELECT 1 FROM processed_events "
                    "WHERE topic=? AND event_id=?",
                    (topic, event_id)
                )
                return cur.fetchone() is not None

    def mark_processed(
        self, topic: str, event_id: str, received_at: str
    ) -> bool:
        """
        Tandai event sebagai sudah diproses.
        Return True  = berhasil (event baru/unik)
        Return False = gagal karena sudah ada (duplikat)
        """
        with self._lock:
            try:
                with self._get_conn() as conn:
                    conn.execute(
                        "INSERT INTO processed_events "
                        "(topic, event_id, received_at) "
                        "VALUES (?, ?, ?)",
                        (topic, event_id, received_at)
                    )
                    conn.commit()
                return True
            except sqlite3.IntegrityError:
                # Primary Key sudah ada = duplikat
                logger.warning(
                    f"[DUPLIKAT TERDETEKSI] "
                    f"topic={topic!r} event_id={event_id!r} "
                    f"— event ini sudah pernah diproses, di-drop."
                )
                return False

    def clear(self):
        """Hapus semua data. Dipakai saat testing."""
        with self._lock:
            with self._get_conn() as conn:
                conn.execute("DELETE FROM processed_events")
                conn.commit()
        logger.info("[STORAGE] Semua data dedup dihapus (testing).")