# Distributed Pub-Sub Log Aggregator

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/Framework-FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)
![Docker](https://img.shields.io/badge/Deployment-Docker%20Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![SQLite](https://img.shields.io/badge/Storage-aiosqlite-003B57?style=flat-square&logo=sqlite&logoColor=white)
![Testing](https://img.shields.io/badge/Tests-pytest-0A9EDC?style=flat-square&logo=pytest&logoColor=white)

Sistem log aggregator berbasis pola arsitektur **Publish-Subscribe** yang menerima event dari berbagai publisher, memproses event melalui consumer yang bersifat **idempotent**, dan melakukan **deduplication** terhadap duplikasi event secara persisten.

> 📺 **Video Demonstrasi**: [Tonton Demonstrasi Sistem di YouTube](https://youtu.be/HzasxlvjN5Q)

---

## Deskripsi Sistem

Sistem ini dirancang untuk menangani aliran event log terdistribusi dengan keandalan pemrosesan tinggi:
- **Idempotent Consumer**: Memastikan setiap event hanya diproses satu kali meskipun terjadi pengiriman ulang (at-least-once delivery).
- **Persistent Deduplication**: Menggunakan penyimpanan SQLite asinkron (`aiosqlite`) sehingga data identitas event tetap bertahan saat terjadi restart container.
- **Containerized Orchestration**: Seluruh komponen dikemas rapi dalam Docker Compose, lengkap dengan simulator publisher untuk pengujian beban.

---

## Arsitektur

```text
┌──────────────┐     ┌──────────────────┐     ┌────────────────┐     ┌──────────────────┐
│   Publisher  │────▶│   FastAPI API    │────▶│  asyncio.Queue │────▶│   Consumer       │
│  (HTTP POST) │     │  POST /publish   │     │  (in-memory)   │     │  (Idempotent)    │
└──────────────┘     └──────────────────┘     └────────────────┘     └────────┬─────────┘
                                                                               │
                      ┌──────────────────┐                              ┌──────▼─────────┐
                      │  GET /events     │◀─────── in-memory store ◀────│  DedupStore     │
                      │  GET /stats      │                              │  (SQLite)       │
                      └──────────────────┘                              └────────────────┘
```

**Lapisan Arsitektur (Clean Architecture):**

| Lapisan | Direktori | Tanggung Jawab |
|:---|:---|:---|
| **API** | `src/api/` | Route handlers FastAPI |
| **Service** | `src/services/` | Logika publisher, consumer, dan kalkulasi statistik |
| **Storage** | `src/storage/` | Persistensi deduplikasi berbasis SQLite |
| **Model** | `src/models/` | Skema validasi event Pydantic |
| **Core** | `src/core/` | Konfigurasi aplikasi dan konstanta |

---

## Teknologi

- **Bahasa & Framework**: Python 3.11, FastAPI (async/await)
- **Persistensi Dedup**: SQLite via `aiosqlite`
- **Antrean Internal**: `asyncio.Queue` (simulasi pub-sub in-process)
- **Containerization**: Docker & Docker Compose
- **Pengujian**: pytest & pytest-asyncio

---

## Cara Build & Run

### 1. Docker (Rekomendasi)

```bash
# Build image
docker build -t uts-aggregator .

# Jalankan container
docker run -p 8080:8080 uts-aggregator
```

### 2. Docker Compose (Dengan Publisher Simulator)

```bash
# Build dan jalankan seluruh service
docker compose up --build

# Publisher simulator otomatis mengirim 5000 event dengan 25% duplikasi buatan
```

### 3. Eksekusi Lokal (Development)

```bash
# Pasang dependensi
pip install -r requirements.txt

# Jalankan server
python -m src.main
```

---

## Dokumentasi API Endpoints

### `POST /publish`: Menerbitkan Event

Mendukung penerimaan single event maupun batch events.

**Contoh Single Event:**
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": {
      "topic": "auth.login",
      "event_id": "evt-001",
      "timestamp": "2026-01-15T10:30:00Z",
      "source": "auth-service",
      "payload": {"user_id": "u123", "action": "login"}
    }
  }'
```

**Response (202 Accepted):**
```json
{
  "status": "accepted",
  "received": 1,
  "message": "Event accepted"
}
```

### `GET /events?topic=...`: Mengambil Event yang Telah Diproses

```bash
# Mengambil seluruh event
curl http://localhost:8080/events

# Filter berdasarkan topik tertentu
curl "http://localhost:8080/events?topic=auth.login"
```

### `GET /stats`: Statistik Sistem Realtime

```bash
curl http://localhost:8080/stats
```

**Contoh Response:**
```json
{
  "received": 5000,
  "unique_processed": 3750,
  "duplicate_dropped": 1250,
  "topics": ["auth.login", "payment.processed", "user.signup"],
  "uptime": "0h 2m 15s",
  "uptime_seconds": 135.42
}
```

### `GET /health`: Health Check Endpoint

```bash
curl http://localhost:8080/health
```

---

## Pengujian Otomatis (Testing)

```bash
# Pasang dependensi pengujian
pip install -r requirements.txt

# Jalankan seluruh unit test suite
pytest tests/ -v

# Jalankan dengan laporan ringkas
pytest tests/ -v --tb=short
```

---

## Struktur Direktori Proyek

```text
distributed-pubsub-aggregator/
├── Dockerfile                  # Definisi container image
├── docker-compose.yml          # Orkestrasi multi-service
├── pyproject.toml              # Konfigurasi pengujian pytest
├── requirements.txt            # Dependensi Python
├── src/
│   ├── main.py                 # Titik masuk utama aplikasi FastAPI
│   ├── simulator.py            # Simulator generator event dan stress test
│   ├── api/routes.py           # Endpoint router HTTP
│   ├── core/config.py          # Konfigurasi sistem
│   ├── models/event.py         # Skema data model Pydantic
│   ├── services/
│   │   ├── consumer.py         # Idempotent consumer worker
│   │   ├── publisher.py        # Publisher service
│   │   └── stats.py            # Kalkulator metrik dan statistik
│   └── storage/dedup_store.py  # Penyimpanan state deduplikasi SQLite
└── tests/                      # Rangkaian pengujian unit dan edge case
```

---

## Konteks Akademik
Proyek ini dikembangkan sebagai Ujian Tengah Semester (UTS) mata kuliah Sistem Paralel dan Terdistribusi, Program Studi Informatika, Institut Teknologi Kalimantan.
