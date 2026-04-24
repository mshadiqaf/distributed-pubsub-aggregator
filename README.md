# Pub-Sub Log Aggregator

> **UTS Sistem Paralel dan Terdistribusi** — Pub-Sub Log Aggregator dengan Idempotent Consumer dan Deduplication

## Deskripsi

Sistem log aggregator berbasis **Publish-Subscribe** yang menerima event dari publisher, memproses event melalui consumer yang bersifat **idempotent**, dan melakukan **deduplication** terhadap event duplikat. Seluruh komponen berjalan lokal di dalam container Docker.

## Arsitektur

```
┌──────────────┐     ┌──────────────────┐     ┌────────────────┐     ┌──────────────────┐
│   Publisher   │────▶│   FastAPI API     │────▶│  asyncio.Queue │────▶│   Consumer       │
│  (HTTP POST)  │     │  POST /publish    │     │  (in-memory)   │     │  (Idempotent)    │
└──────────────┘     └──────────────────┘     └────────────────┘     └────────┬─────────┘
                                                                               │
                      ┌──────────────────┐                              ┌──────▼─────────┐
                      │  GET /events     │◀─────── in-memory store ◀────│  DedupStore     │
                      │  GET /stats      │                              │  (SQLite)       │
                      └──────────────────┘                              └────────────────┘
```

**Clean Architecture Layers:**

| Layer | Path | Responsibility |
|-------|------|----------------|
| API | `src/api/` | FastAPI route handlers |
| Service | `src/services/` | Publisher, Consumer, Stats logic |
| Storage | `src/storage/` | SQLite dedup persistence |
| Model | `src/models/` | Pydantic event schemas |
| Core | `src/core/` | Configuration & constants |

## Teknologi

- **Python 3.11** + **FastAPI** (async/await)
- **SQLite** via `aiosqlite` (dedup persistence)
- **asyncio.Queue** (internal pub-sub simulation)
- **Docker** + **Docker Compose**
- **pytest** + **pytest-asyncio** (unit testing)

## Cara Build & Run

### 1. Docker (Recommended)

```bash
# Build image
docker build -t uts-aggregator .

# Run container
docker run -p 8080:8080 uts-aggregator
```

### 2. Docker Compose (dengan Publisher Simulator)

```bash
# Build dan run semua services
docker compose up --build

# Publisher simulator akan otomatis mengirim 5000 event dengan 25% duplikasi
```

### 3. Lokal (Development)

```bash
# Install dependencies
pip install -r requirements.txt

# Run server
python -m src.main
```

## API Endpoints

### `POST /publish` — Publish Events

Menerima single event atau batch events.

**Single Event:**
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

**Batch Events:**
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "topic": "auth.login",
        "event_id": "evt-001",
        "timestamp": "2026-01-15T10:30:00Z",
        "source": "auth-service",
        "payload": {"user_id": "u123"}
      },
      {
        "topic": "payment.processed",
        "event_id": "pay-001",
        "timestamp": "2026-01-15T10:31:00Z",
        "source": "payment-gateway",
        "payload": {"amount": 99.99}
      }
    ]
  }'
```

**Response (202 Accepted):**
```json
{
  "status": "accepted",
  "received": 2,
  "message": "Batch of 2 events accepted"
}
```

### `GET /events?topic=...` — Get Processed Events

```bash
# Semua events
curl http://localhost:8080/events

# Filter by topic
curl "http://localhost:8080/events?topic=auth.login"
```

### `GET /stats` — System Statistics

```bash
curl http://localhost:8080/stats
```

**Response:**
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

### `GET /health` — Health Check

```bash
curl http://localhost:8080/health
```

## Demo: Deduplication & Idempotency

```bash
# 1. Kirim event pertama kali
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"events":{"topic":"demo","event_id":"d1","timestamp":"2026-01-01T00:00:00Z","source":"test","payload":{}}}'

# 2. Kirim event yang SAMA (duplicate)
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"events":{"topic":"demo","event_id":"d1","timestamp":"2026-01-01T00:00:00Z","source":"test","payload":{}}}'

# 3. Cek stats — harus 2 received, 1 unique, 1 duplicate
curl http://localhost:8080/stats

# 4. Restart container lalu kirim event yang sama lagi
docker restart <container_id>
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"events":{"topic":"demo","event_id":"d1","timestamp":"2026-01-01T00:00:00Z","source":"test","payload":{}}}'

# 5. Cek stats — duplicate_dropped harus bertambah (persistence works!)
curl http://localhost:8080/stats
```

## Running Tests

```bash
# Install dependencies
pip install -r requirements.txt

# Run all 10 tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --tb=short
```

## Struktur Folder

```
pub-sub-log-aggregator/
├── BRIEF.md                    # Brief tugas UTS
├── README.md                   # Dokumentasi ini
├── report.md                   # Laporan desain & teori (Bab 1-7)
├── requirements.txt            # Python dependencies
├── pyproject.toml              # Pytest configuration
├── Dockerfile                  # Docker image definition
├── docker-compose.yml          # Multi-service orchestration
├── .gitignore
├── src/
│   ├── __init__.py
│   ├── main.py                 # FastAPI app entry point
│   ├── simulator.py            # Publisher stress test simulator
│   ├── core/
│   │   ├── __init__.py
│   │   └── config.py           # Application configuration
│   ├── models/
│   │   ├── __init__.py
│   │   └── event.py            # Pydantic event schemas
│   ├── storage/
│   │   ├── __init__.py
│   │   └── dedup_store.py      # SQLite dedup persistence
│   ├── services/
│   │   ├── __init__.py
│   │   ├── publisher.py        # Publisher service
│   │   ├── consumer.py         # Idempotent consumer service
│   │   └── stats.py            # Statistics tracking
│   └── api/
│       ├── __init__.py
│       └── routes.py           # FastAPI route handlers
└── tests/
    ├── __init__.py
    ├── conftest.py             # Shared test fixtures
    ├── test_dedup_store.py     # Tests 1-3: Dedup & persistence
    ├── test_models.py          # Test 4: Schema validation
    ├── test_api.py             # Tests 5-8: Endpoints & stress
    ├── test_consumer.py        # Test 9: Queue processing
    └── test_edge_cases.py      # Test 10: Edge cases
```

## Asumsi & Catatan

1. **Internal queue only**: Komunikasi pub-sub menggunakan `asyncio.Queue` in-process (bukan message broker eksternal).
2. **SQLite untuk dedup**: Ringan, embedded, dan persist tanpa dependency eksternal.
3. **At-least-once simulation**: Publisher simulator mengirim ulang event untuk mensimulasikan duplikasi.
4. **Ordering**: Menggunakan timestamp + monotonic sequence counter. Total ordering tidak dijamin antar topics.
5. **Tidak ada external service**: Semua berjalan lokal dalam container.

## Video Demo

> Link video demo YouTube: *(tambahkan link di sini)*
