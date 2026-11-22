"""Registri 10 query analitik Gold untuk Trino dan validasi parameter tanggal (F-10)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

_SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class GoldQuery:
    name: str
    description: str
    sql: str
    uses_date_range: bool = True


GOLD_QUERIES: dict[str, GoldQuery] = {
    "hourly_generation_mix": GoldQuery(
        name="hourly_generation_mix",
        description="Bauran pembangkitan per jam per jenis energi.",
        sql="""
SELECT
    hour_utc,
    country,
    series_id,
    series_name,
    energy_group,
    avg_mw,
    share_of_load_pct
FROM iceberg.gold.mart_hourly_mix
WHERE country = '{country}'
  AND event_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
ORDER BY hour_utc, series_id
""".strip(),
    ),
    "hourly_renewable_share": GoldQuery(
        name="hourly_renewable_share",
        description="Tren porsi energi terbarukan vs fosil per jam.",
        sql="""
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
WHERE country = '{country}'
  AND event_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
GROUP BY hour_utc, country
ORDER BY hour_utc
""".strip(),
    ),
    "daily_peak_load_hours": GoldQuery(
        name="daily_peak_load_hours",
        description="Beban puncak harian dan jam kejadiannya.",
        sql="""
WITH ranked AS (
    SELECT
        event_date,
        country,
        hour_utc,
        avg_mw AS load_mw,
        ROW_NUMBER() OVER (PARTITION BY country, event_date ORDER BY avg_mw DESC, hour_utc ASC) AS rn
    FROM iceberg.gold.mart_hourly_mix
    WHERE country = '{country}'
      AND series_id = 'load'
      AND event_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
)
SELECT event_date, country, hour_utc AS peak_hour_utc, load_mw AS peak_load_mw
FROM ranked
WHERE rn = 1
ORDER BY event_date
""".strip(),
    ),
    "daily_price_summary": GoldQuery(
        name="daily_price_summary",
        description="Ringkasan harga day-ahead harian DE-LU.",
        sql="""
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
WHERE bzn = '{bzn}'
  AND event_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
ORDER BY event_date
""".strip(),
    ),
    "negative_price_intervals": GoldQuery(
        name="negative_price_intervals",
        description="Hari dengan interval harga negatif.",
        sql="""
SELECT
    event_date,
    bzn,
    negative_interval_count,
    min_price_eur_mwh,
    avg_price_eur_mwh
FROM iceberg.gold.mart_daily_price
WHERE bzn = '{bzn}'
  AND negative_interval_count > 0
  AND event_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
ORDER BY negative_interval_count DESC, event_date
""".strip(),
    ),
    "peak_vs_offpeak_price": GoldQuery(
        name="peak_vs_offpeak_price",
        description="Perbandingan harga jam puncak vs luar jam puncak.",
        sql="""
SELECT
    event_date,
    bzn,
    peak_avg_price_eur_mwh,
    offpeak_avg_price_eur_mwh,
    (peak_avg_price_eur_mwh - offpeak_avg_price_eur_mwh) AS peak_spread_eur_mwh
FROM iceberg.gold.mart_daily_price
WHERE bzn = '{bzn}'
  AND event_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
ORDER BY event_date
""".strip(),
    ),
    "load_source_reconciliation": GoldQuery(
        name="load_source_reconciliation",
        description="Rekonsiliasi beban Energy-Charts vs ENTSO-E.",
        sql="""
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
WHERE country = '{country}'
  AND bzn = '{bzn}'
  AND event_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
ORDER BY timestamp_utc
""".strip(),
    ),
    "weather_load_sensitivity": GoldQuery(
        name="weather_load_sensitivity",
        description="Sensitivitas beban dan harga terhadap suhu serta radiasi.",
        sql="""
SELECT
    event_date,
    country,
    AVG(temperature_2m_c) AS avg_temp_c,
    AVG(wind_speed_10m_kmh) AS avg_wind_kmh,
    AVG(shortwave_radiation_wm2) AS avg_radiation_wm2,
    AVG(target_load_mw) AS avg_load_mw,
    AVG(day_ahead_price_eur_mwh) AS avg_price_eur_mwh
FROM iceberg.gold.mart_forecast_features
WHERE country = '{country}'
  AND event_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
GROUP BY event_date, country
ORDER BY event_date
""".strip(),
    ),
    "load_forecast_vs_actual": GoldQuery(
        name="load_forecast_vs_actual",
        description="Perbandingan prediksi model ML vs beban aktual.",
        sql="""
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
WHERE p.country = '{country}'
  AND DATE(p.timestamp_utc) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
ORDER BY p.timestamp_utc
""".strip(),
    ),
    "gold_mart_freshness": GoldQuery(
        name="gold_mart_freshness",
        description="Indikator freshness dan jumlah baris seluruh tabel Gold.",
        uses_date_range=False,
        sql="""
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
FROM iceberg.gold.ml_load_predictions
""".strip(),
    ),
}


def validate_date_range(start_date: str, end_date: str) -> tuple[str, str]:
    """Pastikan format tanggal YYYY-MM-DD valid dan ``start_date <= end_date``."""
    try:
        start = date.fromisoformat(str(start_date).strip())
    except ValueError as exc:
        raise ValueError(
            f"Tanggal mulai tidak valid: {start_date!r}. Gunakan format YYYY-MM-DD."
        ) from exc

    try:
        end = date.fromisoformat(str(end_date).strip())
    except ValueError as exc:
        raise ValueError(
            f"Tanggal akhir tidak valid: {end_date!r}. Gunakan format YYYY-MM-DD."
        ) from exc

    if start > end:
        raise ValueError(
            f"Rentang tanggal tidak valid: start_date ({start}) lebih besar dari end_date ({end})."
        )
    return start.isoformat(), end.isoformat()


def _validate_identifier(value: str, label: str) -> str:
    cleaned = str(value).strip()
    if not cleaned or not _SAFE_TOKEN_RE.match(cleaned):
        raise ValueError(f"Parameter {label} tidak valid: {value!r}")
    return cleaned


def render_query(
    name: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    country: str = "de",
    bzn: str = "DE-LU",
) -> str:
    """Bangun SQL siap eksekusi dengan validasi parameter ketat."""
    if name not in GOLD_QUERIES:
        tersedia = ", ".join(sorted(GOLD_QUERIES))
        raise ValueError(f"Query Gold tidak dikenal: {name!r}. Pilihan: {tersedia}")

    query = GOLD_QUERIES[name]
    if not query.uses_date_range:
        return query.sql

    if not start_date or not end_date:
        raise ValueError("Parameter start_date dan end_date wajib diisi untuk query ini.")

    safe_start, safe_end = validate_date_range(start_date, end_date)
    safe_country = _validate_identifier(country, "country")
    safe_bzn = _validate_identifier(bzn, "bzn")
    return query.sql.format(
        start_date=safe_start,
        end_date=safe_end,
        country=safe_country,
        bzn=safe_bzn,
    )
