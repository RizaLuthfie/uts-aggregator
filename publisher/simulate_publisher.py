"""
simulate_publisher.py
---------------------
Script simulasi publisher untuk demo dan testing manual.

YANG DILAKUKAN SCRIPT INI:
1. Kirim 20 event unik ke topic 'app-logs'
2. Kirim ulang 5 event yang SAMA (simulasi at-least-once delivery)
3. Kirim 10 event unik ke topic 'error-logs'
4. Tampilkan GET /stats → buktikan deduplication bekerja
5. Tampilkan GET /events?topic=app-logs

CARA PAKAI:
  Pastikan aggregator sudah berjalan dulu:
    python -m src.main
  Lalu di terminal lain:
    python publisher/simulate_publisher.py
"""

import httpx
import uuid
import json
import os
import time
from datetime import datetime, timezone

# URL aggregator — bisa diubah via environment variable
# (dipakai saat Docker Compose: AGGREGATOR_URL=http://aggregator:8080)
BASE_URL = os.environ.get("AGGREGATOR_URL", "http://localhost:8080")


def buat_event(
    topic: str,
    event_id: str = None,
    source: str = "simulate-publisher"
) -> dict:
    """Helper untuk membuat satu event valid."""
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "payload": {
            "level": "INFO",
            "message": f"Event dari {source}",
            "service_version": "1.0.0"
        }
    }


def cetak_separator(judul: str):
    print("\n" + "=" * 55)
    print(f"  {judul}")
    print("=" * 55)


def main():
    cetak_separator("Pub-Sub Log Aggregator — Simulasi Publisher")
    print(f"  Target URL: {BASE_URL}")

    # ── LANGKAH 1: Kirim 20 event UNIK ──────────────────────────
    cetak_separator("LANGKAH 1: Kirim 20 event unik ke 'app-logs'")
    event_unik = [buat_event("app-logs") for _ in range(20)]
    resp = httpx.post(
        f"{BASE_URL}/publish",
        json={"events": event_unik}
    )
    print(f"  Status  : {resp.status_code}")
    print(f"  Response: {resp.json()}")

    # ── LANGKAH 2: Kirim 5 event DUPLIKAT ───────────────────────
    cetak_separator("LANGKAH 2: Kirim ULANG 5 event yang SAMA (duplikat!)")
    print("  (Ini simulasi at-least-once delivery)")
    event_duplikat = [
        buat_event("app-logs", event_id=event_unik[i]["event_id"])
        for i in range(5)
    ]
    resp = httpx.post(
        f"{BASE_URL}/publish",
        json={"events": event_duplikat}
    )
    print(f"  Status  : {resp.status_code}")
    print(f"  Response: {resp.json()}")

    # ── LANGKAH 3: Kirim 10 event ke topic berbeda ───────────────
    cetak_separator("LANGKAH 3: Kirim 10 event ke topic 'error-logs'")
    event_error = [
        buat_event("error-logs", source="service-B")
        for _ in range(10)
    ]
    resp = httpx.post(
        f"{BASE_URL}/publish",
        json={"events": event_error}
    )
    print(f"  Status  : {resp.status_code}")
    print(f"  Response: {resp.json()}")

    # Tunggu consumer selesai memproses semua event
    print("\n  Menunggu consumer memproses semua event...")
    time.sleep(1.5)

    # ── LANGKAH 4: Cek statistik ─────────────────────────────────
    cetak_separator("LANGKAH 4: Hasil GET /stats")
    stats = httpx.get(f"{BASE_URL}/stats").json()
    print(json.dumps(stats, indent=2))
    print()
    print("  PENJELASAN HASIL:")
    print(f"  - received         : {stats['received']}"
          " (20 unik + 5 duplikat + 10 unik = 35)")
    print(f"  - unique_processed : {stats['unique_processed']}"
          " (harusnya 30)")
    print(f"  - duplicate_dropped: {stats['duplicate_dropped']}"
          " (harusnya 5)")

    # ── LANGKAH 5: Cek event per topic ───────────────────────────
    cetak_separator("LANGKAH 5: Hasil GET /events?topic=app-logs")
    events = httpx.get(f"{BASE_URL}/events?topic=app-logs").json()
    print(f"  Jumlah event unik di 'app-logs': {events['count']}"
          " (harusnya 20)")

    cetak_separator("SELESAI")
    print("  Deduplication BERHASIL jika:")
    print("  unique_processed=30 dan duplicate_dropped=5")
    print()


if __name__ == "__main__":
    main()