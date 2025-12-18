# UAS Sistem Terdistribusi – Pub-Sub Log Aggregator

> Sistem multi-service (Python + Docker Compose) untuk mengumpulkan log secara at-least-once, melakukan deduplikasi/idempotensi persisten, dan memastikan transaksi ACID dengan kontrol konkurensi berbasis database.

## Ringkasan Stack

- **aggregator** – FastAPI + worker async, menulis ke Postgres dengan constraint unik `(topic, event_id)` dan statistik transaksional.
- **broker** – Redis 7 (list + BLPOP) sehingga antrean event bertahan walau aggregator restart.
- **storage** – Postgres 16 dengan named volume `pg_data` untuk persistensi.
- **publisher** – client Python yang memompa ≥20.000 event (±35% duplikasi) untuk demonstrasi reliabilitas.

```
publisher --> HTTP POST /publish --> aggregator (FastAPI)
                                    |-> enqueue -> Redis list (broker)
                                    |-> workers -> Postgres (storage)
                                                   |-> unique constraint + stats

observability: /stats, structured logging, readiness via /healthz
```

## Fitur Utama

- Idempotent consumer: `INSERT ... ON CONFLICT DO NOTHING` + transaksi tunggal per event menjaga konsistensi walau ada retry atau crash.
- Dedup store persisten: Postgres berada pada volume bernama, sehingga data aman setelah container dihapus.
- Multi-worker concurrency: jumlah worker dikendalikan env `WORKER_COUNT`, setiap worker menarik data dari Redis secara blocking.
- Batch-aware API: `POST /publish` menerima objek tunggal maupun array, memvalidasi skema memakai Pydantic, dan menerapkan back-pressure (503) bila antrean penuh.
- Observability: `GET /stats` menampilkan `received`, `unique_processed`, `duplicate_dropped`, daftar topic beserta jumlah event, serta `uptime_seconds` dan `worker_count`.
- Testing 12 kasus (pytest) yang mencakup deduplikasi, race condition, validasi skema, overflow antrean, health endpoint, serta integrasi end-to-end.

## Menjalankan dengan Docker Compose (disarankan)

1. **Build & start**
   ```bash
   docker compose up --build
   ```
   Compose akan memulai Postgres (`storage`), Redis (`broker`), lalu `aggregator` dan `publisher`. Publisher menunggu 5 detik sebelum mengirim batch.
2. **Monitoring**
   - Aggregator API: http://localhost:8080
   - Stats: `curl http://localhost:8080/stats`
   - Event sample: `curl "http://localhost:8080/events?topic=topic_1"`
3. **Graceful stop** – tekan `Ctrl+C`. Volume `pg_data` dan `redis_data` menjaga data agar tetap ada jika Anda menjalankan kembali `docker compose up`.

> **Catatan jaringan:** Seluruh service berada di network default Compose tanpa akses eksternal; publisher hanya berbicara dengan aggregator melalui hostname internal.

## Jalankan Aggregator Saja (mode dev)

```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
set DATABASE_URL=sqlite:///./data/dev.db
python -m uvicorn src.main:app --reload
```

SQLite disediakan untuk kemudahan pengembangan; lingkungan produksi/demo selalu menggunakan Postgres.

## Konfigurasi Penting

| Variabel | Default | Deskripsi |
| --- | --- | --- |
| `DATABASE_URL` | `sqlite:///./data/dedup.db` | URL SQLAlchemy; Compose mengisinya dengan Postgres. |
| `BROKER_URL` | `memory://local` | Gunakan `redis://broker:6379/0` agar antrean berada di Redis. |
| `BROKER_QUEUE_KEY` | `aggregator:events` | Nama list Redis. |
| `WORKER_COUNT` | `4` | Banyaknya worker konsumen event. |
| `QUEUE_MAXSIZE` | `5000` | Kapasitas antrean in-memory (fallback non-Redis). |
| `QUEUE_POLL_INTERVAL` | `2` | Timeout BLPOP / polling. |

## API Surface

| Endpoint | Deskripsi | Contoh |
| --- | --- | --- |
| `POST /publish` | Terima event tunggal / batch, validasi, enqueue ke Redis/memory queue. | `curl -X POST http://localhost:8080/publish -H "Content-Type: application/json" -d "{...}"` |
| `GET /events?topic=` | List event unik; filter opsional `topic`. | `curl http://localhost:8080/events?topic=topic_3` |
| `GET /stats` | Statistik sistem + daftar topic + uptime. | `curl http://localhost:8080/stats` |
| `GET /healthz` | Cek koneksi DB + siap pakai (untuk readiness). | `curl http://localhost:8080/healthz` |

Response `POST /publish` (contoh):

```json
{
  "accepted": 500,
  "worker_count": 4
}
```

Response `GET /stats` (contoh setelah publisher mengirim 20k event dengan 35% duplikasi):

```json
{
  "received": 20000,
  "unique_processed": 13019,
  "duplicate_dropped": 6981,
  "topics": [{"topic": "topic_0", "count": 1305}, ...],
  "uptime_seconds": 184.2,
  "worker_count": 4
}
```

## Testing & Benchmark

1. **Unit/integration tests (12 kasus):**
   ```bash
   python -m pytest
   ```
   Suite mencakup: dedup batch, race-condition dengan ThreadPool, validasi skema, overflow antrean (mengembalikan 503), health check, serta verifikasi statistik.

2. **Load demo via publisher:** Jalankan Compose; `publisher` otomatis mengirim ≥20.000 event dengan 35% duplikasi. Statistik throughput bisa diambil via `GET /stats` atau script `tools/collect_stats.py`.

3. **Optional k6:** Template skrip ada pada `tools/run_benchmark.ps1` untuk memicu k6 GitHub Action / lokal (tidak wajib dieksekusi pada asisten ini).

## Observability & Recovery Checklist

- **Log struktur**: Aggregator menulis log worker (`processed`, `duplicate`) untuk setiap event sehingga audit mudah dilakukan.
- **Crash tolerance**: Redis menyimpan antrean dengan `appendonly yes`, Postgres memakai volume `pg_data`. Setelah `docker compose down` + `docker compose up`, `GET /events` menunjukkan state yang sama dan duplikasi tetap ditolak.
- **Back-pressure**: Jika queue penuh, API mengembalikan 503 agar publisher dapat melakukan retry dengan exponential backoff.

## Dokumen Pendukung

- **Laporan teori & analisis Bab 1–13**: lihat [report.md](report.md)
- **Script utilitas**: `tools/activate_venv.ps1`, `tools/run_benchmark.ps1`, `tools/collect_stats.py`
- **Video demo (maks 25 menit)**: https://youtu.be/yWEU48rka2M (akan diperbarui bila ada rekaman terbaru)

## Lisensi & Kontribusi

Proyek ini dibuat sebagai tugas individu. Silakan gunakan referensi ini untuk studi, namun jangan menyalin kode secara literal tanpa atribusi sesuai etika akademik.
