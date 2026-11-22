-- 10 Query Analitik Gold untuk Trino (F-10 SQL)
-- Katalog default: iceberg, Schema default: gold
-- Parameter tanggal menggunakan format ISO YYYY-MM-DD (misal '2026-11-01' s.d. '2026-11-07').

-- 1. hourly_generation_mix: Bauran pembangkitan per jam per jenis energi
SELECT
    hour_utc,
    country,
    series_id,
    series_name,
    energy_group,
    avg_mw,
    share_of_load_pct
FROM iceberg.gold.mart_hourly_mix
WHERE country = '{{country}}'
  AND event_date BETWEEN DATE '{{start_date}}' AND DATE '{{end_date}}'
ORDER BY hour_utc, series_id;

-- 2. hourly_renewable_share: Tren porsi energi terbarukan vs fosil per jam
SELECT
    hour_utc,
    country,
    SUM(CASE WHEN energy_group = 'renewable' THEN avg_mw ELSE 0.0 END) AS renewable_mw,
    SUM(CASE WHEN energy_group = 'fossil' THEN avg_mw ELSE 0.0 END) AS fossil_mw,
    MAX(CASE WHEN series_id = 'load' THEN avg_mw END) AS load_mw,
    CASE
        WHEN MAX(CASE WHEN series_id = 'load' THEN avg_mw END) > 0
        THEN (SUM(CASE WHEN energy_group = 'renewable' THEN avg_mw ELSE 0.0 END)
              / MAX(CASE WHEN series_id = 'load' THEN avg_mw END)) * 100.0
        ELSE NULL
    END AS renewable_share_of_load_pct
FROM iceberg.gold.mart_hourly_mix
WHERE country = '{{country}}'
  AND event_date BETWEEN DATE '{{start_date}}' AND DATE '{{end_date}}'
GROUP BY hour_utc, country
ORDER BY hour_utc;

-- 3. daily_peak_load_hours: Beban puncak harian dan jam kejadiannya
WITH ranked AS (
    SELECT
        event_date,
        country,
        hour_utc,
        avg_mw AS load_mw,
        ROW_NUMBER() OVER (PARTITION BY country, event_date ORDER BY avg_mw DESC, hour_utc ASC) AS rn
    FROM iceberg.gold.mart_hourly_mix
    WHERE country = '{{country}}'
      AND series_id = 'load'
      AND event_date BETWEEN DATE '{{start_date}}' AND DATE '{{end_date}}'
)
SELECT event_date, country, hour_utc AS peak_hour_utc, load_mw AS peak_load_mw
FROM ranked
WHERE rn = 1
ORDER BY event_date;

-- 4. daily_price_summary: Ringkasan harga day-ahead harian DE-LU
SELECT
    event_date,
    bzn,
    avg_price_eur_mwh,
    min_price_eur_mwh,
    max_price_eur_mwh,
    stddev_price_eur_mwh,
    interval_count,
    completeness_pct
FROM iceberg.gold.mart_daily_price
WHERE bzn = '{{bzn}}'
  AND event_date BETWEEN DATE '{{start_date}}' AND DATE '{{end_date}}'
ORDER BY event_date;

-- 5. negative_price_intervals: Hari dengan interval harga negatif
SELECT
    event_date,
    bzn,
    negative_interval_count,
    min_price_eur_mwh,
    avg_price_eur_mwh
FROM iceberg.gold.mart_daily_price
WHERE bzn = '{{bzn}}'
  AND negative_interval_count > 0
  AND event_date BETWEEN DATE '{{start_date}}' AND DATE '{{end_date}}'
ORDER BY negative_interval_count DESC, event_date;

-- 6. peak_vs_offpeak_price: Perbandingan harga jam puncak vs luar jam puncak
SELECT
    event_date,
    bzn,
    peak_avg_price_eur_mwh,
    offpeak_avg_price_eur_mwh,
    (peak_avg_price_eur_mwh - offpeak_avg_price_eur_mwh) AS peak_spread_eur_mwh
FROM iceberg.gold.mart_daily_price
WHERE bzn = '{{bzn}}'
  AND event_date BETWEEN DATE '{{start_date}}' AND DATE '{{end_date}}'
ORDER BY event_date;

-- 7. load_source_reconciliation: Rekonsiliasi beban Energy-Charts vs ENTSO-E
SELECT
    timestamp_utc,
    country,
    bzn,
    target_load_mw AS energy_charts_load_mw,
    entsoe_load_mw,
    (target_load_mw - entsoe_load_mw) AS diff_mw,
    CASE
        WHEN entsoe_load_mw > 0
        THEN ABS(target_load_mw - entsoe_load_mw) / entsoe_load_mw * 100.0
        ELSE NULL
    END AS abs_diff_pct
FROM iceberg.gold.mart_forecast_features
WHERE country = '{{country}}'
  AND bzn = '{{bzn}}'
  AND event_date BETWEEN DATE '{{start_date}}' AND DATE '{{end_date}}'
ORDER BY timestamp_utc;

-- 8. weather_load_sensitivity: Sensitivitas beban & harga terhadap suhu dan radiasi
SELECT
    event_date,
    country,
    AVG(temperature_2m_c) AS avg_temp_c,
    AVG(wind_speed_10m_kmh) AS avg_wind_kmh,
    AVG(shortwave_radiation_wm2) AS avg_radiation_wm2,
    AVG(target_load_mw) AS avg_load_mw,
    AVG(day_ahead_price_eur_mwh) AS avg_price_eur_mwh
FROM iceberg.gold.mart_forecast_features
WHERE country = '{{country}}'
  AND event_date BETWEEN DATE '{{start_date}}' AND DATE '{{end_date}}'
GROUP BY event_date, country
ORDER BY event_date;

-- 9. load_forecast_vs_actual: Perbandingan prediksi model ML vs beban aktual
SELECT
    p.timestamp_utc,
    p.country,
    p.model_version,
    p.predicted_load_mw,
    p.actual_load_mw,
    ABS(p.predicted_load_mw - p.actual_load_mw) AS abs_error_mw,
    p.error_status,
    p.predicted_at
FROM iceberg.gold.ml_load_predictions AS p
WHERE p.country = '{{country}}'
  AND DATE(p.timestamp_utc) BETWEEN DATE '{{start_date}}' AND DATE '{{end_date}}'
ORDER BY p.timestamp_utc;

-- 10. gold_mart_freshness: Indikator freshness dan kelengkapan seluruh tabel Gold
SELECT 'mart_hourly_mix' AS table_name, COUNT(*) AS row_count, MAX(updated_at) AS last_updated_at
FROM iceberg.gold.mart_hourly_mix
UNION ALL
SELECT 'mart_daily_price' AS table_name, COUNT(*) AS row_count, MAX(updated_at) AS last_updated_at
FROM iceberg.gold.mart_daily_price
UNION ALL
SELECT 'mart_forecast_features' AS table_name, COUNT(*) AS row_count, MAX(updated_at) AS last_updated_at
FROM iceberg.gold.mart_forecast_features
UNION ALL
SELECT 'ml_load_predictions' AS table_name, COUNT(*) AS row_count, MAX(predicted_at) AS last_updated_at
FROM iceberg.gold.ml_load_predictions;
