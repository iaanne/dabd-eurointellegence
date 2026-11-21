# Bukti F-09: Katalog metadata dan lineage

Tanggal: 2026-11-18
Branch: `nad`
Lingkungan: Docker Compose `eurogrid`, Python 3.12.12.

## 1. Katalog dataset

Berkas: `docs/catalog/datasets.yaml` (8 dataset, 4 Bronze + 4 Silver).

Isi tiap entri: `layer`, `source`, `owner`, `description`, `grain`,
`business_key`, `retention_days`, `refresh`, `license`, dan `quality_gate`
untuk Silver.

| Layer | Pemilik | Jumlah | Retensi |
| --- | --- | ---: | --- |
| Bronze | Ian | 4 | selama proyek |
| Silver | Nad | 4 | 365 hari |

Pembacaan dan pemeriksaan ada di `src/eurogrid/catalog.py`. Hasil pencocokan
dengan katalog Iceberg yang sebenarnya:

```text
katalog: 8 dataset | iceberg: 16 tabel
hilang di Iceberg: tidak ada
tanpa metadata: tidak ada
pemilik: {'Ian': 4, 'Nad': 4}
```

Enam belas tabel itu termasuk tabel bantu `_quarantine` dan `_quality_report`
yang mengikuti metadata dataset induknya.

## 2. Layanan Marquez

Ditambahkan ke `compose.yaml`:

| Layanan | Image | Port host |
| --- | --- | --- |
| `marquez-db` | `postgres:16` | tidak diekspos |
| `marquez` | `marquezproject/marquez:0.51.1` | 127.0.0.1:5001 |
| `marquez-web` | `marquezproject/marquez-web:0.51.1` | 127.0.0.1:3002 |

API: `http://localhost:5001/api/v1/...`, UI: `http://localhost:3002`.

## 3. Event OpenLineage

`src/eurogrid/lineage.py` mengirim `START` dan `COMPLETE` ke
`POST /api/v1/lineage`. Bila `OPENLINEAGE_URL` kosong, pengiriman menjadi
no-op sehingga flow tetap jalan tanpa Marquez.

* Flow Bronze mengirim job `bronze.publish` dengan masukan
  `source.entsoe-api.actual_load`, `source.energy-charts-api.price`,
  `source.energy-charts-api.public_power`, `source.open-meteo-api.weather_hourly`
  dan keluaran empat tabel Bronze.
* Flow Silver mengirim satu job per dataset, misalnya
  `silver.entsoe_actual_load`, dengan masukan `bronze.entsoe_actual_load` dan
  keluaran tabel Silver beserta karantina dan laporan mutunya.

Worker Prefect memakai `OPENLINEAGE_URL=http://marquez:5000`; host memakai
`http://localhost:5001` dari `.env`.

## 4. Bukti jalur lineage

Kueri `GET /api/v1/lineage?nodeId=dataset:eurogrid:silver.entsoe_actual_load&depth=3`
menghasilkan rantai:

```text
dataset:source.entsoe-api.actual_load
  -> job:bronze.publish
  -> dataset:bronze.entsoe_actual_load
  -> job:silver.entsoe_actual_load
  -> dataset:silver.entsoe_actual_load
  -> dataset:silver.entsoe_actual_load_quarantine
  -> dataset:silver.entsoe_actual_load_quality_report
```

Job yang terdaftar di namespace `eurogrid`: `bronze.publish`,
`silver.entsoe_actual_load`, `silver.energy_charts_price`,
`silver.energy_charts_public_power`, `silver.open_meteo_weather_hourly`.

## 5. Kesalahan yang ditemukan dan diperbaiki

Percobaan pertama memakai satu `runId` untuk keempat job Silver. Marquez
mengaitkan seluruh output run itu ke setiap job, sehingga
`silver.energy_charts_price` tampak memproduksi `silver.entsoe_actual_load`.
Perbaikannya: `runId` lineage diturunkan per dataset
(`{quality_run_id}-{nama dataset}`).

Percobaan pertama juga ditolak Marquez (HTTP 422) karena facet `jobType`
tidak memuat `integration` dan `processingType`. Facet itu dihapus.

## 6. Tes

```text
.\.venv\Scripts\python.exe -m pytest -q
60 passed
```

Tes baru di `tests/test_catalog_lineage.py` (13):
katalog lengkap, retensi sesuai arsitektur, pemilik per dataset, katalog ganda,
pemilik kosong, layer tidak dikenal, kolom wajib kurang, pencocokan dengan tabel
Iceberg, bentuk event OpenLineage, `runId` deterministik, lineage mati tanpa
URL, pengiriman ke Marquez (mock), dan kegagalan Marquez yang tidak melempar.

Tambahan di `tests/test_prefect_yaml.py`: parameter pada `prefect.yaml` harus
benar-benar diterima fungsi flow-nya.

## 7. Berkas yang diubah atau ditambahkan

* `compose.yaml` — tiga layanan Marquez, volume `marquez-db-data`, env lineage di worker
* `.env`, `.env.example` — `OPENLINEAGE_URL`, `OPENLINEAGE_NAMESPACE`
* `docs/catalog/datasets.yaml` — katalog 8 dataset
* `src/eurogrid/catalog.py` — pembaca dan pemeriksa katalog
* `src/eurogrid/lineage.py` — pengirim event OpenLineage
* `src/eurogrid/silver/warehouse.py` — kirim lineage per dataset
* `flows/bronze_daily.py` — task `emit-bronze-lineage`
* `tests/test_catalog_lineage.py`, `tests/test_prefect_yaml.py`

## 8. Yang belum

* Gold belum ada, jadi rantai lineage berhenti di Silver. Edge Gold ditambahkan
  saat F-03/F-04 dikerjakan Jim.
* Kolom `columns` di katalog baru diisi untuk layer Bronze; tabel Silver memakai
  kolom Bronze ditambah `quality_status`, `quality_run_id`, `validated_at`,
  `unit_source`.
* Marquez belum diamankan dengan autentikasi karena hanya mendengarkan di
  `127.0.0.1`.
