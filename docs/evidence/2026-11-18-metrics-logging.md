# F-14: metrik, log JSON, dan dashboard

Tanggal: 18 November 2026. Dijalankan di mesin pengembang, Docker Compose proyek `eurogrid`.

## Yang dibangun

| Bagian | Berkas | Isi |
| --- | --- | --- |
| Exporter metrik | `src/eurogrid/observability/exporter.py` | Proses kecil yang membaca katalog Iceberg dan API Prefect, lalu menyajikan `/metrics` di port 9101 |
| Ringkasan metrik | `src/eurogrid/observability/metrics.py` | Fungsi murni: hitung status flow, durasi, jumlah baris dari snapshot, kesegaran, dan laporan mutu terakhir |
| Log JSON | `src/eurogrid/observability/logging.py` | Formatter satu baris dengan `timestamp`, `severity`, `service`, `run_id` |
| Prometheus | `infra/prometheus/prometheus.yml` | Menjaring `metrics-exporter`, `node-exporter`, dan dirinya sendiri tiap 30 detik |
| Grafana | `infra/grafana/provisioning`, `infra/grafana/dashboards/eurogrid.json` | Datasource otomatis dan dashboard `EuroGrid Pipeline` (10 panel) |
| Compose | `compose.yaml` | Layanan `metrics-exporter`, `prometheus`, `node-exporter`, `grafana` |
| Flow | `flows/bronze_daily.py`, `flows/silver_daily.py` | Memasang log JSON sejak modul dimuat, lalu menambahkan `run_id` saat flow mulai |

## Metrik yang tersedia

| Metrik | Label | Arti |
| --- | --- | --- |
| `eurogrid_table_rows` | `table`, `layer` | Jumlah baris tiap tabel Bronze/Silver |
| `eurogrid_table_freshness_seconds` | `table`, `layer` | Umur snapshot terakhir |
| `eurogrid_quarantine_rows` | `dataset` | Baris karantina hasil run Silver |
| `eurogrid_silver_rows_failed` | `dataset` | Baris gagal validasi pada laporan mutu terakhir |
| `eurogrid_silver_gate_blocked` | `dataset` | 1 bila quality gate memblokir Gold |
| `eurogrid_flow_runs` | `flow`, `state` | Jumlah run yang terlihat di API Prefect |
| `eurogrid_flow_last_success_timestamp_seconds` | `flow` | Waktu sukses terakhir |
| `eurogrid_flow_last_duration_seconds` | `flow` | Durasi run terakhir |
| `eurogrid_flow_last_run_state` | `flow`, `state` | Status run terakhir |
| `eurogrid_exporter_scrape_errors_total` | `source` | Siklus pengambilan yang gagal (`catalog`, `quality`, `prefect`) |
| `eurogrid_exporter_last_scrape_timestamp_seconds` | - | Waktu siklus terakhir berhasil |

## Bukti

Target Prometheus, semuanya `up`:

```text
eurogrid   up
node       up
prometheus up
```

Kueri `eurogrid_table_rows` mengembalikan 16 seri, di antaranya:

```text
bronze.energy_charts_price             8644
bronze.energy_charts_public_power    181524
bronze.entsoe_actual_load              8640
bronze.open_meteo_weather_hourly       8640
silver.entsoe_actual_load              8640
silver.entsoe_actual_load_quarantine      0
```

`eurogrid_silver_gate_blocked` = 0 untuk keempat dataset, sesuai laporan mutu terakhir.

Grafana menyajikan datasource `Prometheus` (`http://prometheus:9090`) dan dashboard
`EuroGrid Pipeline` (uid `eurogrid-pipeline`) tanpa perlu login, karena mode anonim
viewer dinyalakan.

Contoh log JSON dari flow:

```json
{"timestamp": "2026-11-18T16:45:03.364Z", "severity": "INFO", "service": "eurogrid-demo", "logger": "httpx", "message": "HTTP Request: POST ...", "run_id": "45fa8c74-9a80-4bc1-a0fd-d2ba13116b90"}
```

Logger `httpx`, `httpcore`, dan `urllib3` diturunkan ke WARNING supaya log tidak
dipenuhi permintaan HTTP rutin.

## Cara memeriksa sendiri

```powershell
docker compose -p eurogrid ps
curl.exe -s http://127.0.0.1:9101/metrics | Select-String eurogrid_table_rows
curl.exe -s "http://127.0.0.1:9090/api/v1/targets"
```

Buka http://127.0.0.1:3000 lalu pilih dashboard `EuroGrid Pipeline`.

## Batasan

- Belum ada aturan alert. Itu F-15 dan memakai metrik di atas sebagai sumber.
- `node-exporter` memakai mount `/:/host:ro` tanpa propagasi `rslave`, karena Docker
  Desktop di WSL2 menolak `rslave` dengan galat "not a shared or slave mount".
- Dashboard memakai data yang tersedia saat ini; panel karantina akan mendatar di nol
  selama data Bronze tetap bersih.
- Metrik jumlah baris memakai ringkasan snapshot Iceberg. Bila ringkasan tidak memuat
  `total-records`, exporter menghitung langsung dengan memindai tabel.
