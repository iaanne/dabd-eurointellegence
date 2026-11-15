# Bukti Increment 5: Redpanda dan Trino di Compose

Tanggal: 2026-11-13

Increment ini melengkapi F-01 (Deployment dan isolasi service) dengan dua
service yang belum ada: broker stream Redpanda dan query engine Trino. Trino
sekaligus membuktikan bahwa tabel Bronze di Garage dapat dibaca dan ditulis
melalui jalur SQL, bukan hanya melalui PyIceberg.

## Lingkungan

- Podman 6.1.4 dengan `podman-compose` 1.6.0.
- Redpanda `redpandadata/redpanda:v26.2.7`, satu node, mode dev.
- Trino `trinodb/trino:484`, satu node koordinator dan worker.
- Kredensial Garage tetap dibaca dari `.env`, tidak ada secret baru di repositori.

## Service baru di compose.yaml

| Service | Port host | Catatan |
| --- | --- | --- |
| `redpanda` | 9092, 8080, 9644 (9644 hanya localhost) | Kafka API, Pandaproxy, Admin API |
| `redpanda-topics` | tidak ada | one-shot, membuat topic saat stack dinyalakan |
| `trino` | 8081 | katalog `iceberg`, koneksi ke Iceberg REST dan Garage |

Volume baru: `redpanda-data` dan `trino-data`.

Redpanda memakai dua listener Kafka karena alamat yang diiklankan berbeda:

- `PLAINTEXT` di dalam jaringan compose, diiklankan sebagai `redpanda:9092`.
- `OUTSIDE` untuk host, diiklankan sebagai `localhost:9092` dengan pemetaan
  port `9092:39092`.

Trino dikonfigurasi di `infra/trino/etc/`:

- `config.properties`: satu node, port 8081.
- `node.properties`: environment `eurogrid`, data dir `/data/trino`.
- `jvm.config`: heap 2 GB. Opsi `-XX:MaxBytecodeSize` dan
  `-XX:PerMethodRecompilationCutoff` dari contoh lama tidak dikenal JVM image
  484 dan membuat JVM gagal start, jadi dihapus.
- `catalog/iceberg.properties`: connector `iceberg` dengan `iceberg.catalog.type=rest`
  ke `http://iceberg-rest:8181`, file system S3 native ke `http://garage:3900`
  dengan path-style access. Kredensial dibaca dengan `${ENV:VAR}` dari
  lingkungan container, yang diisi dari `.env`.

Worker Prefect kini juga mendapat override `REDPANDA_BROKERS=redpanda:9092`
dan `TRINO_URL=http://trino:8081`, mengikuti pola override `PREFECT_API_URL`
yang sudah ada.

## Topic Redpanda

Service `redpanda-topics` menjalankan `rpk topic create --if-not-exists`
dengan percobaan ulang sampai 30 kali, sehingga aman saat broker belum siap.
Hasil:

```text
NAME                 PARTITIONS  REPLICAS
eurogrid.events      3           1
eurogrid.events.dlq  3           1
```

Diverifikasi dari dua arah:

- Dalam jaringan compose: `make redpanda-list` (`rpk topic list` di dalam
  container broker).
- Dari host: `podman run --rm --network host --entrypoint /usr/bin/rpk
  docker.io/redpandadata/redpanda:v26.2.7 topic list -X brokers=localhost:9092`.

Catatan versi rpk: flag yang benar adalah `-X brokers=<addr>`. Entry point image
juga sudah memanggil `rpk`, sehingga pemanggilan dengan `--entrypoint` perlu
tanpa awalan `rpk`.

## Trino membaca tabel Bronze

Schemas yang terlihat dari katalog `iceberg`: `bronze`, `smoke`,
`information_schema`, `system`. Jumlah baris per tabel Bronze saat pemeriksaan
(2026-11-13 14:17 UTC):

| Tabel | Baris |
| --- | --- |
| `bronze.energy_charts_public_power` | 22.176 |
| `bronze.energy_charts_price` | 1.056 |
| `bronze.open_meteo_weather_hourly` | 1.056 |
| `bronze.entsoe_actual_load` | 960 |

Rentang `bronze.energy_charts_price`: 2026-11-01 23:00 UTC sampai
2026-11-12 22:45 UTC. Angka lebih besar dari README sebelumnya karena run
Prefect mengejar jadwal yang ketinggalan saat server dimatikan tiga hari; rincian
ada di bagian catatan operasional.

Contoh query yang dijalankan:

```text
SELECT count(*) FROM iceberg.bronze.energy_charts_price  ->  "1056"
```

## Trino menulis ke Garage

Jalur tulis diuji dengan tabel sementara di namespace `smoke`:

```text
CREATE TABLE iceberg.smoke.trino_write_test AS SELECT 1 AS v, 'tes-trino' AS label
CREATE TABLE: 1 row
SELECT * FROM iceberg.smoke.trino_write_test  ->  ("1","tes-trino")
DROP TABLE iceberg.smoke.trino_write_test
```

CTAS, baca kembali, dan drop berhasil, sehingga kompatibilitas Garage dengan
tulis Iceberg lewat Trino terbukti untuk tabel kecil tanpa partisi besar.

## Target Makefile baru

| Target | Isi |
| --- | --- |
| `redpanda-up` | `podman-compose up -d redpanda redpanda-topics` |
| `redpanda-topics` | buat topic manual dengan `rpk topic create` |
| `redpanda-list` | daftar topic |
| `trino-up` | `podman-compose up -d trino` |
| `trino-query Q="..."` | jalankan SQL lewat Trino CLI |

`make test` tetap `29 passed`, tidak ada perubahan kode Python pada increment ini.

## Catatan operasional

- Run jadwal harian untuk 2026-11-11, 2026-11-12, dan 2026-11-13 (jadwal
  05:00 `Europe/Berlin`) tertunda selama server Prefect mati. Ketika stack
  dinyalakan, run yang ketinggalan dijalankan, tetapi tiga run pertama gagal
  karena worker Prefect hidup lebih dulu daripada Garage dan Iceberg REST,
  sehingga task publish gagal dengan `Failed to resolve 'iceberg-rest'`.
  Retry task (2 kali) menghabiskan batas pada tiga run itu. Satu run
  berikutnya (`unique-slug`) selesai 14:16 UTC setelah dependensi sehat, dan
  tabel Bronze terisi data sampai 2026-11-12.
  Saran tindak lanjut untuk Platform: tambahkan `depends_on` worker Prefect ke
  `garage` dan `iceberg-rest`, atau tunda penandatanganan run yang ketinggalan.
- Ingest pertama pada rentang sama tetap idempoten; jumlah baris tabel bergerak
  karena gabungan rentang ingest dari beberapa run, bukan duplikasi.
- Producer dan consumer stream (F-04 dan F-05), dedup, dan DLQ handling belum
  dikerjakan. Redpanda dan topiknya sudah siap dipakai.
