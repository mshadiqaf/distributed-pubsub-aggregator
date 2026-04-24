# UTS Sistem Terdistribusi dan Parallel (Take-Home)

## Tema: Pub-Sub Log Aggregator dengan Idempotent Consumer dan Deduplication

Buatlah sebuah layanan Pub-Sub log aggregator yang menerima event/log dari publisher, kemudian dilanjutkan untuk diproses melalui subscriber/consumer yang bersifat idempotent (tidak memproses ulang event yang sama), serta melakukan deduplication terhadap duplikasi event. Seluruh komponen berjalan lokal di dalam container (Docker). Docker Compose bersifat opsional sebagai bonus.

---

## Ketentuan Umum

- Tugas individu, take-home.
- Bahasa Indonesia, gunakan istilah teknis dalam Bahasa Inggris bila relevan.
- Cakupan teori: Bab 1–7 dari buku utama (Distributed Systems: Principles and Paradigms | Andrew S. Tanenbaum, Maarten Van Steen).
- Submit via link GitHub + laporan PDF atau MD yang proper.
- Base bahasa pemrograman: Python (disarankan).
- Eksekusi dalam Docker (Windows atau Linux image bebas). Koneksi jaringan hanya lokal di dalam container.
- Sertakan 5–10 unit tests.
- Docker Compose opsional untuk bonus (+10%).
- Sitasi wajib menggunakan format APA edisi ke-7 (Bahasa Indonesia) untuk buku utama; cantumkan judul, penulis, dan tahun.

---

## Tujuan Pembelajaran (Bab 1–7)

- Memahami karakteristik sistem terdistribusi (Bab 1) dan arsitektur (Bab 2) terutama pattern publish-subscribe.
- Menerapkan komunikasi antar komponen (Bab 3) dan penamaan (Bab 4) untuk event/topic.
- Mempertimbangkan waktu, ordering, dan clock (Bab 5) pada pemrosesan event.
- Mengelola toleransi kegagalan (Bab 6) dan implikasinya terhadap duplikasi/retries.
- Mencapai konsistensi (Bab 7) melalui idempotency dan deduplication.

---

## Struktur Tugas

### 1. Bagian Teori (40%)

Jawab ringkas-padat (sekitar 150–250 kata per poin) dan sertakan sitasi ke bab terkait di buku utama.

- **T1 (Bab 1):** Jelaskan karakteristik utama sistem terdistribusi dan trade-off yang umum pada desain Pub-Sub log aggregator.
- **T2 (Bab 2):** Bandingkan arsitektur client-server vs publish-subscribe untuk aggregator. Kapan memilih Pub-Sub? Berikan alasan teknis.
- **T3 (Bab 3):** Uraikan at-least-once vs exactly-once delivery semantics. Mengapa idempotent consumer krusial pada presence of retries?
- **T4 (Bab 4):** Rancang skema penamaan untuk topic dan event_id (unik, collision-resistant). Jelaskan dampaknya terhadap dedup.
- **T5 (Bab 5):** Bahas ordering: kapan total ordering tidak diperlukan? Usulkan pendekatan praktis (mis. event timestamp + monotonic counter) dan batasannya.
- **T6 (Bab 6):** Identifikasi failure modes (duplikasi, out-of-order, crash). Jelaskan strategi mitigasi (retry, backoff, durable dedup store).
- **T7 (Bab 7):** Definisikan eventual consistency pada aggregator, jelaskan bagaimana idempotency + dedup membantu mencapai konsistensi.
- **T8 (Bab 1–7):** Rumuskan metrik evaluasi sistem (throughput, latency, duplicate rate) dan kaitkan ke keputusan desain.

---

### 2. Bagian Implementasi (60%)

Bangun layanan aggregator berbasis Python (disarankan FastAPI/Flask + asyncio) dengan spesifikasi berikut:

#### a. Model Event & API

Format JSON:

```json
{
  "topic": "string",
  "event_id": "string-unik",
  "timestamp": "ISO8601",
  "source": "string",
  "payload": { }
}
```

- Endpoint `POST /publish` menerima batch atau single event, validasi skema.
- Consumer (subscriber) memproses event dari internal queue (in-memory) dan melakukan dedup berdasarkan `(topic, event_id)`.
- Endpoint `GET /events?topic=...` mengembalikan daftar event unik yang telah diproses.
- Endpoint `GET /stats` menampilkan: `received`, `unique_processed`, `duplicate_dropped`, `topics`, `uptime`.

---

#### b. Idempotency & Deduplication

- Implementasikan dedup store yang tahan terhadap restart (contoh: SQLite atau file-based key-value) dan local-only.
- Idempotency: satu event dengan `(topic, event_id)` yang sama hanya diproses sekali meski diterima berkali-kali.
- Logging yang jelas untuk setiap duplikasi yang terdeteksi.

---

#### c. Reliability & Ordering

- At-least-once delivery: simulasi duplicate delivery di publisher (mengirim beberapa event dengan event_id sama).
- Toleransi crash: setelah restart container, dedup store tetap mencegah reprocessing event yang sama.
- Ordering: jelaskan di laporan apakah total ordering dibutuhkan atau tidak dalam konteks aggregator Anda.

---

#### d. Performa Minimum

- Skala uji proses ≥ 5.000 event (dengan ≥ 20% duplikasi).
- Sistem harus tetap responsif.

---

#### e. Docker

- Wajib: `Dockerfile` untuk membangun image yang menjalankan layanan.

Contoh (sesuaikan):

```dockerfile
FROM python:3.11-slim
WORKDIR /app
RUN adduser --disabled-password --gecos '' appuser && chown -R appuser:appuser /app
USER appuser
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
EXPOSE 8080
CMD ["python", "-m", "src.main"]
```

---

#### f. Docker Compose (Opsional, +10%)

- Pisahkan publisher dan aggregator dalam dua service, jaringan internal default Compose.
- Tidak boleh menggunakan layanan eksternal publik.

---

#### g. Unit Tests (Wajib, 5–10 tests)

Gunakan `pytest` (atau framework pilihan) dengan cakupan minimum:

- Validasi dedup: kirim duplikat, pastikan hanya sekali diproses.
- Persistensi dedup store: setelah restart (simulasi), dedup tetap efektif.
- Validasi skema event `(topic, event_id, timestamp)`.
- Endpoint `GET /stats` dan `GET /events` konsisten dengan data.
- Stress kecil: masukan batch event, ukur waktu eksekusi (assert dalam batas wajar).

---

## Deliverables (GitHub + Laporan)

- Link repository GitHub (public atau akses disediakan) berisi:
  - `src/` kode aplikasi
  - `tests/` unit tests
  - `requirements.txt` (atau `pyproject.toml`)
  - `Dockerfile` (wajib), `docker-compose.yml` (opsional)
  - `README.md` berisi cara build/run, asumsi, dan endpoint
  - `report.md` atau `report.pdf` berisi penjelasan desain (hubungkan ke Bab 1–7) dan sitasi
  - Link video demo YouTube (5–8 menit)

- Sertakan instruksi run singkat:
  - Build: `docker build -t uts-aggregator .`
  - Run: `docker run -p 8080:8080 uts-aggregator`

---

## Video Demo (Wajib)

- Unggah video demo ke YouTube dengan visibilitas publik; cantumkan link di README.md atau laporan.
- Durasi 5–8 menit, fokus pada demonstrasi teknis berikut:
  - Build image dan menjalankan container.
  - Mengirim event duplikat (simulasi at-least-once) dan menunjukkan idempotency + dedup bekerja.
  - Memeriksa `GET /events` dan `GET /stats` sebelum/sesudah pengiriman duplikat.
  - Restart container dan tunjukkan dedup store persisten mencegah reprocessing.
  - Ringkas arsitektur dan keputusan desain (singkat, 30–60 detik).

---

## Format Laporan (MD/PDF)

- Ringkasan sistem dan arsitektur (diagram sederhana).
- Keputusan desain: idempotency, dedup store, ordering, retry.
- Analisis performa dan metrik.
- Keterkaitan ke Bab 1–7 (cantumkan referensi per bagian).
- Sitasi buku utama: gunakan APA edisi ke-7 (Bahasa Indonesia).
  - Format buku: Nama Belakang, Inisial. (Tahun). Judul buku. Penerbit.
  - Jika ada DOI/URL: tambahkan setelah penerbit.
  - Contoh sitasi dalam teks: (Nama Belakang, Tahun) atau Nama Belakang (Tahun).
- Sesuaikan detail dengan metadata di `docs/buku-utama.pdf`.

---

## Rubrik Penilaian (Total 100 + Bonus 10)

- **Teori (40 poin):**
  - T1–T8: kedalaman, akurasi, dan sitasi tepat (5 poin x 8 = 40).

- **Implementasi (60 poin):**
  - Arsitektur & Correctness (13): memenuhi spesifikasi API dan perilaku.
  - Idempotency & Dedup (13): dedup akurat, tahan restart, logging jelas.
  - Dockerfile & Reproducibility (9): image minimal, non-root, build/run mulus.
  - Unit Tests (9): 5–10 tests, cakupan fungsional inti, dapat dijalankan.
  - Observability & Stats (4): endpoint `/stats`, logging informatif.
  - Dokumentasi (2): README & laporan jelas, instruksi run, asumsi.
  - Video Demo (10): demonstrasi build/run, API, dedup & persistensi, jelas dan terstruktur.

- **Bonus Docker Compose (optional +10):**
  - dua service terpisah berjalan lokal dengan jaringan internal.

---

## Kebijakan & Tenggat

- Durasi: sesuai deadline di LMS; dosen berhak menetapkan penalti (-10%/hari).
- Individual work: nyatakan sumber yang digunakan, dilarang plagiarisme.

---

## Saran Teknis

- Gunakan `asyncio.Queue` untuk pipeline sederhana publisher→consumer di dalam proses.
- Dedup store: SQLite embedded atau file JSON/LMDB; pastikan atomicity pada write.
- Pertimbangkan Bloom filter (opsional) untuk mengurangi overhead lookup pada duplikasi.

---

## Catatan Penting

- Tidak menggunakan layanan eksternal; semua lokal dalam container.
- Jika memilih menggunakan Rust, spesifikasi tetap sama; sesuaikan toolchain dan base image (mis. `rust:1.72-slim`).
