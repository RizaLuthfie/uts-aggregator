"""
test_persistence.py
-------------------
Test persistensi dedup store:
- Setelah restart (simulasi), event yang sudah diproses
  tetap dikenali sebagai duplikat
- Data di SQLite tidak hilang saat instance baru dibuat
"""

import os
import pytest
from datetime import datetime, timezone

os.environ["DEDUP_DB_PATH"] = "/tmp/test_persistence.db"

from src.storage import DedupStore


@pytest.fixture(autouse=True)
def reset():
    store = DedupStore(db_path="/tmp/test_persistence.db")
    store.clear()
    yield


def test_data_tetap_ada_setelah_restart():
    """
    Simulasi restart: buat DedupStore baru dengan DB path sama.
    Data yang sudah ada harus tetap terbaca.
    """
    db_path = "/tmp/test_persistence.db"
    waktu = datetime.now(timezone.utc).isoformat()

    # Instance pertama: tandai event sebagai diproses
    store1 = DedupStore(db_path=db_path)
    hasil = store1.mark_processed("topic-A", "event-restart-001", waktu)
    assert hasil is True  # berhasil disimpan

    # Instance BARU (simulasi restart container)
    store2 = DedupStore(db_path=db_path)

    # Store baru harus mengenali event sebagai duplikat
    assert store2.is_duplicate("topic-A", "event-restart-001") is True


def test_event_baru_tidak_dianggap_duplikat_setelah_restart():
    """
    Setelah restart, event yang BELUM pernah diproses
    tidak boleh dianggap duplikat.
    """
    db_path = "/tmp/test_persistence.db"

    store1 = DedupStore(db_path=db_path)
    store1.mark_processed(
        "topic-B", "event-lama-001",
        datetime.now(timezone.utc).isoformat()
    )

    # Instance baru
    store2 = DedupStore(db_path=db_path)

    # Event lama = duplikat
    assert store2.is_duplicate("topic-B", "event-lama-001") is True
    # Event baru = bukan duplikat
    assert store2.is_duplicate("topic-B", "event-baru-999") is False


def test_mark_processed_dua_kali_return_false():
    """
    mark_processed event yang sama dua kali:
    - Pertama  → True (berhasil)
    - Kedua    → False (duplikat)
    """
    db_path = "/tmp/test_persistence.db"
    waktu = datetime.now(timezone.utc).isoformat()
    store = DedupStore(db_path=db_path)

    hasil1 = store.mark_processed("topic-C", "event-double-001", waktu)
    hasil2 = store.mark_processed("topic-C", "event-double-001", waktu)

    assert hasil1 is True
    assert hasil2 is False