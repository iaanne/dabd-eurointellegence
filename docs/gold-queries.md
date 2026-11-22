# Lapisan Gold dan 10 Query Analitik (F-03, F-10 SQL, F-11, F-12)

Pemilik: **Jim** (Analitik dan Machine Learning).

## 1. Tabel Gold di Katalog Iceberg (`gold.*`)

| Tabel | Grain | Business Key | Partisi |
| --- | --- | --- | --- |
| `gold.mart_hourly_mix` | 1 jam UTC per negara per `series_id` | `(country, hour_utc, series_id)` | `event_date` |
| `gold.mart_daily_price` | 1 hari UTC per bidding zone | `(bzn, event_date)` | `event_date` |
| `gold.mart_forecast_features` | 1 jam UTC per wilayah dan zona | `(country, bzn, timestamp_utc)` | `event_date` |
| `gold.ml_load_predictions` | 1 record input per versi model | `(input_record_id, model_version)` | — |

Aturan quality gate (`F-08`): `publish_gold()` memeriksa tabel `silver.*_quality_report` terlebih dahulu dan menolak publikasi bila ada dataset Silver berstatus `blocked`.

## 2. Daftar 10 Query Trino (`sql/gold_queries.sql`)

| No | Nama Query | Tabel Sumber | Parameter |
| --- | --- | --- | --- |
| 1 | `hourly_generation_mix` | `gold.mart_hourly_mix` | `country`, `start_date`, `end_date` |
| 2 | `hourly_renewable_share` | `gold.mart_hourly_mix` | `country`, `start_date`, `end_date` |
| 3 | `daily_peak_load_hours` | `gold.mart_hourly_mix` | `country`, `start_date`, `end_date` |
| 4 | `daily_price_summary` | `gold.mart_daily_price` | `bzn`, `start_date`, `end_date` |
| 5 | `negative_price_intervals` | `gold.mart_daily_price` | `bzn`, `start_date`, `end_date` |
| 6 | `peak_vs_offpeak_price` | `gold.mart_daily_price` | `bzn`, `start_date`, `end_date` |
| 7 | `load_source_reconciliation` | `gold.mart_forecast_features` | `country`, `bzn`, `start_date`, `end_date` |
| 8 | `weather_load_sensitivity` | `gold.mart_forecast_features` | `country`, `start_date`, `end_date` |
| 9 | `load_forecast_vs_actual` | `gold.ml_load_predictions` | `country`, `start_date`, `end_date` |
| 10 | `gold_mart_freshness` | seluruh tabel `gold.*` | — |

Validasi parameter tanggal dijalankan oleh `eurogrid.gold.queries.render_query(...)`: format tanggal selain `YYYY-MM-DD` atau `start_date > end_date` akan menghasilkan `ValueError`.

## 3. Perintah CLI & Prefect

```bash
uv run --extra iceberg eurogrid publish-gold
uv run --extra iceberg eurogrid publish-gold --start 2026-11-01 --end 2026-11-07
uv run --extra iceberg eurogrid train-ml
uv run --extra iceberg eurogrid predict-ml
uv run eurogrid gold-query --name daily_price_summary --start 2026-11-01 --end 2026-11-07
```
