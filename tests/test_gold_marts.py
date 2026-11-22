from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from eurogrid.gold.marts import (
    GOLD_MART_KEYS,
    build_mart_daily_price,
    build_mart_forecast_features,
    build_mart_hourly_mix,
    merge_incremental,
)
from eurogrid.gold.warehouse import check_silver_quality_gate


def _ts_15m(start: str, periods: int) -> list[pd.Timestamp]:
    base = pd.Timestamp(start, tz="UTC")
    return [base + pd.Timedelta(minutes=15 * i) for i in range(periods)]


def test_mart_hourly_mix_agregasi_dan_share_of_load() -> None:
    ts = _ts_15m("2026-11-05T00:00:00", 8)
    rows = []
    for t in ts:
        rows.append(
            {
                "timestamp_utc": t,
                "country": "de",
                "series_id": "solar",
                "series_name": "Solar",
                "value": 10000.0,
                "license": "CC BY 4.0",
            }
        )
        rows.append(
            {
                "timestamp_utc": t,
                "country": "de",
                "series_id": "load",
                "series_name": "Load",
                "value": 50000.0,
                "license": "CC BY 4.0",
            }
        )
    df = pd.DataFrame(rows)
    mart = build_mart_hourly_mix(df)

    assert len(mart) == 4  # 2 jam x 2 seri
    solar_row = mart[mart["series_id"] == "solar"].iloc[0]
    load_row = mart[mart["series_id"] == "load"].iloc[0]

    assert solar_row["energy_group"] == "renewable"
    assert solar_row["sample_count"] == 4
    assert solar_row["avg_mw"] == pytest.approx(10000.0)
    assert solar_row["share_of_load_pct"] == pytest.approx(20.0)
    assert pd.isna(load_row["share_of_load_pct"])


def test_mart_daily_price_hitung_negatif_dan_filter_zona() -> None:
    ts = _ts_15m("2026-11-05T00:00:00", 4)
    df = pd.DataFrame(
        {
            "timestamp_utc": ts + [ts[0]],
            "bidding_zone": ["DE-LU", "DE-LU", "DE-LU", "DE-LU", "FR"],
            "series_id": ["day_ahead_price"] * 5,
            "value": [-10.0, 20.0, 30.0, 40.0, 999.0],
            "license": ["CC BY 4.0"] * 4 + ["private"],
        }
    )
    mart = build_mart_daily_price(df)

    assert len(mart) == 1
    assert mart["bzn"].iloc[0] == "DE-LU"
    assert mart["negative_interval_count"].iloc[0] == 1
    assert mart["avg_price_eur_mwh"].iloc[0] == pytest.approx(20.0)
    assert mart["interval_count"].iloc[0] == 4


def test_mart_forecast_features_gabungan_dan_lag_tanpa_leakage() -> None:
    hours = pd.date_range("2026-11-01T00:00:00Z", periods=30, freq="1h")
    pp = pd.DataFrame(
        {
            "timestamp_utc": hours,
            "country": ["de"] * len(hours),
            "series_id": ["load"] * len(hours),
            "value": [40000.0 + i * 100.0 for i in range(len(hours))],
            "license": ["CC BY 4.0"] * len(hours),
        }
    )
    pr = pd.DataFrame(
        {
            "timestamp_utc": hours,
            "bidding_zone": ["DE-LU"] * len(hours),
            "value": [80.0 + i for i in range(len(hours))],
            "license": ["CC BY 4.0"] * len(hours),
        }
    )
    wt = pd.DataFrame(
        {
            "timestamp_utc": list(hours) * 2,
            "location": ["berlin"] * (len(hours) * 2),
            "variable": ["temperature_2m"] * len(hours) + ["wind_speed_10m"] * len(hours),
            "value": [10.0] * len(hours) + [15.0] * len(hours),
            "license": ["CC BY 4.0"] * (len(hours) * 2),
        }
    )
    features = build_mart_forecast_features(pp, pr, wt)

    assert len(features) == 30
    # Baris ke-1 (indeks 1) punya lag 1h dari baris ke-0 (40000.0)
    assert features["load_lag_1h_mw"].iloc[1] == pytest.approx(40000.0)
    # Baris ke-24 punya lag 24h dari baris ke-0 (40000.0)
    assert features["load_lag_24h_mw"].iloc[24] == pytest.approx(40000.0)
    assert bool(features["is_complete_features"].iloc[24]) is True
    assert bool(features["is_complete_features"].iloc[0]) is False


def test_backfill_idempoten_dua_kali_sama() -> None:
    df1 = pd.DataFrame(
        {
            "event_date": [date(2026, 11, 5), date(2026, 11, 6)],
            "bzn": ["DE-LU", "DE-LU"],
            "avg_price_eur_mwh": [50.0, 60.0],
        }
    )
    df2 = pd.DataFrame(
        {
            "event_date": [date(2026, 11, 6)],
            "bzn": ["DE-LU"],
            "avg_price_eur_mwh": [65.0],
        }
    )
    run_1 = merge_incremental(
        df1,
        df2,
        business_key=GOLD_MART_KEYS["mart_daily_price"],
        start_date="2026-11-06",
        end_date="2026-11-06",
    )
    run_2 = merge_incremental(
        run_1,
        df2,
        business_key=GOLD_MART_KEYS["mart_daily_price"],
        start_date="2026-11-06",
        end_date="2026-11-06",
    )

    pd.testing.assert_frame_equal(run_1, run_2)
    assert len(run_2) == 2
    assert run_2.loc[run_2["event_date"] == date(2026, 11, 6), "avg_price_eur_mwh"].iloc[0] == 65.0


class _FakeScan:
    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame

    def to_pandas(self) -> pd.DataFrame:
        return self._frame


class _FakeTable:
    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame

    def scan(self) -> _FakeScan:
        return _FakeScan(self._frame)


class _FakeCatalog:
    def __init__(self, tables: dict[str, pd.DataFrame]) -> None:
        self._tables = tables

    def table_exists(self, identifier: str) -> bool:
        return identifier in self._tables

    def load_table(self, identifier: str) -> _FakeTable:
        return _FakeTable(self._tables[identifier])


def test_quality_gate_blocked_menolak_publish_gold() -> None:
    catalog = _FakeCatalog(
        {
            "silver.energy_charts_public_power_quality_report": pd.DataFrame(
                [{"status": "passed", "block_reasons": ""}]
            ),
            "silver.energy_charts_price_quality_report": pd.DataFrame(
                [{"status": "blocked", "block_reasons": "ambang_gagal"}]
            ),
            "silver.open_meteo_weather_hourly_quality_report": pd.DataFrame(
                [{"status": "passed", "block_reasons": ""}]
            ),
            "silver.entsoe_actual_load_quality_report": pd.DataFrame(
                [{"status": "passed", "block_reasons": ""}]
            ),
        }
    )

    with pytest.raises(RuntimeError, match="diblokir"):
        check_silver_quality_gate(catalog, silver_ns="silver")
