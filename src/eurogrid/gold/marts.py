"""Transformasi Silver -> Gold untuk tiga mart analitik dan fitur ML.

Semua fungsi di modul ini murni mengolah DataFrame tanpa menyentuh jaringan
atau katalog, sehingga dapat diuji deterministik dengan data fixture.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Sequence

import numpy as np
import pandas as pd

GOLD_MART_KEYS: dict[str, tuple[str, ...]] = {
    "mart_hourly_mix": ("country", "hour_utc", "series_id"),
    "mart_daily_price": ("bzn", "event_date"),
    "mart_forecast_features": ("country", "bzn", "timestamp_utc"),
    "ml_load_predictions": ("input_record_id", "model_version"),
}

_RENEWABLE_SERIES = frozenset(
    {
        "biomass",
        "geothermal",
        "hydro_pumped_storage",
        "hydro_run_of_river",
        "hydro_water_reservoir",
        "solar",
        "wind_offshore",
        "wind_onshore",
    }
)
_FOSSIL_SERIES = frozenset(
    {
        "fossil_brown_coal_lignite",
        "fossil_coal_derived_gas",
        "fossil_gas",
        "fossil_hard_coal",
        "fossil_oil",
    }
)
_NUCLEAR_SERIES = frozenset({"nuclear"})
_LOAD_SERIES = frozenset({"load", "residual_load"})


def energy_group(series_id: str) -> str:
    """Kelompokkan ``series_id`` pembangkitan menjadi kategori bisnis."""
    sid = str(series_id).strip().lower()
    if sid in _RENEWABLE_SERIES:
        return "renewable"
    if sid in _FOSSIL_SERIES:
        return "fossil"
    if sid in _NUCLEAR_SERIES:
        return "nuclear"
    if sid in _LOAD_SERIES:
        return "load"
    return "other"


def _now_utc(updated_at: datetime | pd.Timestamp | None = None) -> pd.Timestamp:
    ts = pd.Timestamp(updated_at or datetime.now(timezone.utc))
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _ensure_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True)


def _join_licenses(values: pd.Series | list[str]) -> str:
    unik = sorted({str(v).strip() for v in values if pd.notna(v) and str(v).strip()})
    return " | ".join(unik) if unik else ""


def build_mart_hourly_mix(
    public_power: pd.DataFrame,
    *,
    updated_at: datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Agregasi ``silver.energy_charts_public_power`` (15 menit) ke grain 1 jam."""
    kolom = [
        "event_date",
        "hour_utc",
        "country",
        "series_id",
        "series_name",
        "energy_group",
        "avg_mw",
        "min_mw",
        "max_mw",
        "sample_count",
        "share_of_load_pct",
        "license",
        "updated_at",
    ]
    if public_power.empty:
        return pd.DataFrame(columns=kolom)

    work = public_power.copy()
    work["timestamp_utc"] = _ensure_utc(work["timestamp_utc"])
    work["value"] = pd.to_numeric(work["value"], errors="coerce")
    work = work.dropna(subset=["timestamp_utc", "value", "country", "series_id"])
    if work.empty:
        return pd.DataFrame(columns=kolom)

    work["hour_utc"] = work["timestamp_utc"].dt.floor("h")
    if "series_name" not in work.columns:
        work["series_name"] = work["series_id"]
    if "license" not in work.columns:
        work["license"] = ""

    grouped = (
        work.groupby(["country", "hour_utc", "series_id"], as_index=False)
        .agg(
            series_name=("series_name", "last"),
            avg_mw=("value", "mean"),
            min_mw=("value", "min"),
            max_mw=("value", "max"),
            sample_count=("value", "count"),
            license=("license", _join_licenses),
        )
    )
    grouped["event_date"] = grouped["hour_utc"].dt.date
    grouped["energy_group"] = grouped["series_id"].map(energy_group)

    load_ref = (
        grouped[grouped["series_id"] == "load"][["country", "hour_utc", "avg_mw"]]
        .rename(columns={"avg_mw": "_load_mw"})
    )
    grouped = grouped.merge(load_ref, on=["country", "hour_utc"], how="left")

    valid_load = grouped["_load_mw"].notna() & (grouped["_load_mw"] > 0) & (grouped["series_id"] != "load")
    grouped["share_of_load_pct"] = np.where(
        valid_load,
        (grouped["avg_mw"] / grouped["_load_mw"]) * 100.0,
        np.nan,
    )
    grouped = grouped.drop(columns=["_load_mw"])
    grouped["updated_at"] = _now_utc(updated_at)

    grouped = grouped[kolom].sort_values(["country", "hour_utc", "series_id"], ignore_index=True)
    return grouped


def build_mart_daily_price(
    price: pd.DataFrame,
    *,
    updated_at: datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Agregasi ``silver.energy_charts_price`` (15 menit) ke ringkasan harian per zona."""
    kolom = [
        "event_date",
        "bzn",
        "avg_price_eur_mwh",
        "min_price_eur_mwh",
        "max_price_eur_mwh",
        "stddev_price_eur_mwh",
        "peak_avg_price_eur_mwh",
        "offpeak_avg_price_eur_mwh",
        "negative_interval_count",
        "interval_count",
        "completeness_pct",
        "license",
        "updated_at",
    ]
    if price.empty:
        return pd.DataFrame(columns=kolom)

    work = price.copy()
    work["timestamp_utc"] = _ensure_utc(work["timestamp_utc"])
    work["value"] = pd.to_numeric(work["value"], errors="coerce")
    work = work.dropna(subset=["timestamp_utc", "value", "bidding_zone"])
    if work.empty:
        return pd.DataFrame(columns=kolom)

    # Aturan lisensi PRD: harga di luar DE-LU tidak dipakai untuk artefak publik.
    work = work[work["bidding_zone"].astype(str) == "DE-LU"].copy()
    if work.empty:
        return pd.DataFrame(columns=kolom)

    if "license" not in work.columns:
        work["license"] = ""

    work["bzn"] = work["bidding_zone"].astype(str)
    work["event_date"] = work["timestamp_utc"].dt.date
    local_ts = work["timestamp_utc"].dt.tz_convert("Europe/Berlin")
    local_hour = local_ts.dt.hour
    work["is_peak"] = (local_hour >= 8) & (local_hour < 20)
    work["is_negative"] = work["value"] < 0.0
    work["peak_value"] = np.where(work["is_peak"], work["value"], np.nan)
    work["offpeak_value"] = np.where(~work["is_peak"], work["value"], np.nan)

    grouped = (
        work.groupby(["bzn", "event_date"], as_index=False)
        .agg(
            avg_price_eur_mwh=("value", "mean"),
            min_price_eur_mwh=("value", "min"),
            max_price_eur_mwh=("value", "max"),
            stddev_price_eur_mwh=("value", lambda s: float(s.std(ddof=0)) if len(s) > 0 else 0.0),
            peak_avg_price_eur_mwh=("peak_value", "mean"),
            offpeak_avg_price_eur_mwh=("offpeak_value", "mean"),
            negative_interval_count=("is_negative", "sum"),
            interval_count=("value", "count"),
            license=("license", _join_licenses),
        )
    )
    grouped["negative_interval_count"] = grouped["negative_interval_count"].astype("int64")
    grouped["interval_count"] = grouped["interval_count"].astype("int64")
    grouped["completeness_pct"] = np.minimum((grouped["interval_count"] / 96.0) * 100.0, 100.0)
    grouped["updated_at"] = _now_utc(updated_at)

    return grouped[kolom].sort_values(["bzn", "event_date"], ignore_index=True)


def build_mart_forecast_features(
    public_power: pd.DataFrame,
    price: pd.DataFrame,
    weather: pd.DataFrame,
    actual_load: pd.DataFrame | None = None,
    *,
    country: str = "de",
    bzn: str = "DE-LU",
    location: str = "berlin",
    updated_at: datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Bangun tabel fitur bergrain 1 jam untuk peramalan beban tanpa data leakage."""
    kolom = [
        "event_date",
        "timestamp_utc",
        "country",
        "bzn",
        "target_load_mw",
        "entsoe_load_mw",
        "day_ahead_price_eur_mwh",
        "temperature_2m_c",
        "wind_speed_10m_kmh",
        "shortwave_radiation_wm2",
        "precipitation_mm",
        "hour_of_day_local",
        "day_of_week",
        "is_weekend",
        "load_lag_1h_mw",
        "load_lag_24h_mw",
        "load_lag_168h_mw",
        "load_rolling_mean_24h_mw",
        "is_complete_features",
        "license",
        "updated_at",
    ]
    if public_power.empty:
        return pd.DataFrame(columns=kolom)

    pp = public_power.copy()
    pp["timestamp_utc"] = _ensure_utc(pp["timestamp_utc"])
    pp["value"] = pd.to_numeric(pp["value"], errors="coerce")
    pp = pp[(pp["country"].astype(str) == country) & (pp["series_id"].astype(str) == "load")]
    pp = pp.dropna(subset=["timestamp_utc", "value"])
    if pp.empty:
        return pd.DataFrame(columns=kolom)

    pp["hour_utc"] = pp["timestamp_utc"].dt.floor("h")
    base = (
        pp.groupby("hour_utc", as_index=False)
        .agg(target_load_mw=("value", "mean"), _pp_lic=("license", _join_licenses))
        .rename(columns={"hour_utc": "timestamp_utc"})
    )

    # Harga day-ahead rata-rata per jam
    if not price.empty:
        pr = price.copy()
        pr["timestamp_utc"] = _ensure_utc(pr["timestamp_utc"])
        pr["value"] = pd.to_numeric(pr["value"], errors="coerce")
        pr = pr[pr["bidding_zone"].astype(str) == bzn].dropna(subset=["timestamp_utc", "value"])
        if not pr.empty:
            pr["hour_utc"] = pr["timestamp_utc"].dt.floor("h")
            pr_hourly = (
                pr.groupby("hour_utc", as_index=False)
                .agg(day_ahead_price_eur_mwh=("value", "mean"), _pr_lic=("license", _join_licenses))
                .rename(columns={"hour_utc": "timestamp_utc"})
            )
            base = base.merge(pr_hourly, on="timestamp_utc", how="left")
        else:
            base["day_ahead_price_eur_mwh"] = np.nan
            base["_pr_lic"] = ""
    else:
        base["day_ahead_price_eur_mwh"] = np.nan
        base["_pr_lic"] = ""

    # Beban aktual ENTSO-E (opsional, untuk rekonsiliasi)
    if actual_load is not None and not actual_load.empty:
        el = actual_load.copy()
        el["timestamp_utc"] = _ensure_utc(el["timestamp_utc"])
        el["value"] = pd.to_numeric(el["value"], errors="coerce")
        el = el.dropna(subset=["timestamp_utc", "value"])
        if not el.empty:
            el["hour_utc"] = el["timestamp_utc"].dt.floor("h")
            el_hourly = (
                el.groupby("hour_utc", as_index=False)
                .agg(entsoe_load_mw=("value", "mean"))
                .rename(columns={"hour_utc": "timestamp_utc"})
            )
            base = base.merge(el_hourly, on="timestamp_utc", how="left")
        else:
            base["entsoe_load_mw"] = np.nan
    else:
        base["entsoe_load_mw"] = np.nan

    # Cuaca Open-Meteo (sudah 1 jam)
    weather_cols = {
        "temperature_2m": "temperature_2m_c",
        "wind_speed_10m": "wind_speed_10m_kmh",
        "shortwave_radiation": "shortwave_radiation_wm2",
        "precipitation": "precipitation_mm",
    }
    if not weather.empty:
        wt = weather.copy()
        wt["timestamp_utc"] = _ensure_utc(wt["timestamp_utc"])
        wt["value"] = pd.to_numeric(wt["value"], errors="coerce")
        if "location" in wt.columns:
            wt = wt[wt["location"].astype(str) == location]
        wt["hour_utc"] = wt["timestamp_utc"].dt.floor("h")
        wt_piv = (
            wt.pivot_table(index="hour_utc", columns="variable", values="value", aggfunc="mean")
            .rename(columns=weather_cols)
            .reset_index()
            .rename(columns={"hour_utc": "timestamp_utc"})
        )
        for target_col in weather_cols.values():
            if target_col not in wt_piv.columns:
                wt_piv[target_col] = np.nan
        wt_lic = _join_licenses(wt["license"]) if "license" in wt.columns else ""
        base = base.merge(wt_piv[["timestamp_utc", *weather_cols.values()]], on="timestamp_utc", how="left")
        base["_wt_lic"] = wt_lic
    else:
        for target_col in weather_cols.values():
            base[target_col] = np.nan
        base["_wt_lic"] = ""

    # Urutkan berdasarkan waktu dan reindex per jam agar lag berbasis waktu tepat (anti-leakage)
    base = base.sort_values("timestamp_utc", ignore_index=True)
    full_index = pd.date_range(
        base["timestamp_utc"].min(),
        base["timestamp_utc"].max(),
        freq="1h",
        tz="UTC",
    )
    hourly_series = base.set_index("timestamp_utc")["target_load_mw"].reindex(full_index)
    lag_1h = hourly_series.shift(1)
    lag_24h = hourly_series.shift(24)
    lag_168h = hourly_series.shift(168)
    # shift(1) sebelum rolling memastikan jam target t tidak ikut masuk ke rata-rata masa lalu
    rolling_24h = hourly_series.shift(1).rolling(window=24, min_periods=12).mean()

    base["load_lag_1h_mw"] = base["timestamp_utc"].map(lag_1h)
    base["load_lag_24h_mw"] = base["timestamp_utc"].map(lag_24h)
    base["load_lag_168h_mw"] = base["timestamp_utc"].map(lag_168h)
    base["load_rolling_mean_24h_mw"] = base["timestamp_utc"].map(rolling_24h)

    local_ts = base["timestamp_utc"].dt.tz_convert("Europe/Berlin")
    base["event_date"] = base["timestamp_utc"].dt.date
    base["country"] = country
    base["bzn"] = bzn
    base["hour_of_day_local"] = local_ts.dt.hour.astype("int64")
    base["day_of_week"] = (local_ts.dt.day_of_week + 1).astype("int64")
    base["is_weekend"] = local_ts.dt.day_of_week >= 5

    required_feature_cols = [
        "target_load_mw",
        "temperature_2m_c",
        "wind_speed_10m_kmh",
        "load_lag_1h_mw",
        "load_lag_24h_mw",
    ]
    base["is_complete_features"] = base[required_feature_cols].notna().all(axis=1)
    base["license"] = [
        _join_licenses([a, b, c])
        for a, b, c in zip(base["_pp_lic"], base["_pr_lic"], base["_wt_lic"])
    ]
    base["updated_at"] = _now_utc(updated_at)

    return base[kolom].sort_values(["country", "bzn", "timestamp_utc"], ignore_index=True)


def _parse_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip())


def merge_incremental(
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
    *,
    business_key: Sequence[str],
    date_column: str = "event_date",
    start_date: str | date | None = None,
    end_date: str | date | None = None,
) -> pd.DataFrame:
    """Gabungkan data baru secara idempoten untuk backfill atau pembaruan harian (F-03).

    Menjalankan ulang rentang ``[start_date, end_date]`` yang sama akan mengganti
    baris pada rentang itu secara bersih tanpa menghasilkan duplikasi.
    """
    start_d = _parse_date(start_date)
    end_d = _parse_date(end_date)

    inc = incoming.copy()
    if not inc.empty and date_column in inc.columns and (start_d or end_d):
        inc_dates = pd.to_datetime(inc[date_column]).dt.date
        mask = pd.Series(True, index=inc.index)
        if start_d is not None:
            mask &= inc_dates >= start_d
        if end_d is not None:
            mask &= inc_dates <= end_d
        inc = inc.loc[mask].copy()

    if existing is None or existing.empty:
        if inc.empty:
            return inc
        return inc.drop_duplicates(subset=list(business_key), keep="last").sort_values(
            list(business_key), ignore_index=True
        )

    keep_existing = existing.copy()
    if date_column in keep_existing.columns and (start_d or end_d):
        ex_dates = pd.to_datetime(keep_existing[date_column]).dt.date
        in_window = pd.Series(True, index=keep_existing.index)
        if start_d is not None:
            in_window &= ex_dates >= start_d
        if end_d is not None:
            in_window &= ex_dates <= end_d
        keep_existing = keep_existing.loc[~in_window].copy()

    if inc.empty:
        return keep_existing.sort_values(list(business_key), ignore_index=True)

    combined = pd.concat([keep_existing, inc], ignore_index=True, sort=False)
    combined = combined.drop_duplicates(subset=list(business_key), keep="last")
    return combined.sort_values(list(business_key), ignore_index=True)
