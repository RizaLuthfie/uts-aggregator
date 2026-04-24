# Pub-Sub Log Aggregator

UTS Mata Kuliah Sistem Paralel dan Terdistribusi  
**Tema:** Pub-Sub Log Aggregator dengan Idempotent Consumer dan Deduplication

---

## Cara Build dan Run

### Menggunakan Docker (Wajib)

```bash
# 1. Build image
docker build -t uts-aggregator .

# 2. Jalankan container
docker run -p 8080:8080 -v uts-data:/app/data uts-aggregator
```

### Menggunakan Docker Compose (Bonus +10%)

```bash
# Jalankan aggregator + publisher sekaligus
docker-compose up --build

# Hanya aggregator saja
docker-compose up aggregator
```

### Menjalankan Unit Tests (Lokal)

```bash
# Aktifkan virtual environment dulu
.venv\Scripts\activate

# Jalankan semua tests
pytest tests/ -v
```

---

## Endpoint API

| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| POST | `/publish` | Kirim batch atau single event |
| GET | `/events?topic=X` | Daftar event unik per topic |
| GET | `/stats` | Statistik aggregator |
| GET | `/health` | Health check |

---

## Contoh Penggunaan

### Kirim Event

```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d "{\"events\": [{\"topic\": \"app-logs\", \"event_id\": \"abc-123\", \"timestamp\": \"2024-01-01T10:00:00Z\", \"source\": \"service-A\", \"payload\": {}}]}"
```

### Lihat Event

```bash
curl http://localhost:8080/events?topic=app-logs
```

### Lihat Statistik

```bash
curl http://localhost:8080/stats
```

---

## Asumsi Desain

1. **Ordering** — Total ordering tidak diperlukan untuk log aggregator
2. **Dedup Store** — SQLite untuk persistensi tahan restart
3. **At-least-once** — Disimulasikan dengan mengirim ulang event_id yang sama
4. **Semua lokal** — Tidak ada dependency ke layanan eksternal publik

---

## Video Demo

[Link YouTube — akan diisi setelah rekam demo]