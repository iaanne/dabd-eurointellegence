"""Publikasi lapisan Gold ke katalog Iceberg di Garage S3.

Sebelum menulis Gold, modul ini memeriksa laporan mutu Silver (``*_quality_report``).
Bila ada dataset sumber berstatus ``blocked``, publikasi Gold dibatalkan agar data
cacat tidak lolos ke lapisan serving dan ML (F-08).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence
from uuid import uuid4

import pandas as pd

from ..bronze.warehouse import build_catalog
from ..config import Settings, load_settings
from ..lineage import emit_job
from ..silver.warehouse import silver_namespace
from .marts import (
    GOLD_MART_KEYS,
    build_mart_daily_price,
    build_mart_forecast_features,
    build_mart_hourly_mix,
    merge_incremental,
)

REQUIRED_SILVER_DATASETS: tuple[str, ...] = (
    "energy_charts_public_power",
    "energy_charts_price",
    "open_meteo_weather_hourly",
    "entsoe_actual_load",
)


def gold_namespace() -> str:
    return os.getenv("ICEBERG_GOLD_NAMESPACE", "gold").strip() or "gold"


@dataclass(frozen=True)
class GoldResult:
    table: str
    rows: int
    created: bool
    location: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "rows": self.rows,
            "created": self.created,
            "location": self.location,
        }


def to_gold_arrow(frame: pd.DataFrame):
    """Konversi DataFrame Gold ke PyArrow Table dengan tipe DATE untuk ``event_date``."""
    import pyarrow as pa

    arrays = []
    fields = []
    for name in frame.columns:
        series = frame[name]
        if pd.api.types.is_datetime64_any_dtype(series):
            if getattr(series.dt, "tz", None) is None:
                series = series.dt.tz_localize("UTC")
            else:
                series = series.dt.tz_convert("UTC")
            arrow_type = pa.timestamp("us", tz="UTC")
            arrays.append(pa.array(series, type=arrow_type))
        elif pd.api.types.is_bool_dtype(series):
            arrow_type = pa.bool_()
            arrays.append(pa.array(series, type=arrow_type))
        elif pd.api.types.is_integer_dtype(series):
            arrow_type = pa.int64()
            arrays.append(pa.array(series, type=arrow_type))
        elif pd.api.types.is_float_dtype(series):
            arrow_type = pa.float64()
            arrays.append(pa.array(series, type=arrow_type))
        elif name == "event_date" or _looks_like_date_series(series):
            arrow_type = pa.date32()
            converted = pd.to_datetime(series, errors="coerce").dt.date
            arrays.append(pa.array(converted, type=arrow_type))
        else:
            arrow_type = pa.string()
            arrays.append(pa.array(series.astype("string"), type=arrow_type))
        fields.append(pa.field(name, arrow_type, nullable=True))

    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _looks_like_date_series(series: pd.Series) -> bool:
    non_null = series.dropna()
    if non_null.empty:
        return False
    first = non_null.iloc[0]
    return isinstance(first, date)


def write_gold_table(catalog, identifier: str, frame: pd.DataFrame) -> tuple[bool, str | None]:
    """Tulis atau timpa tabel Gold di katalog Iceberg."""
    arrow_table = to_gold_arrow(frame)
    created = False
    if catalog.table_exists(identifier):
        table = catalog.load_table(identifier)
        existing_columns = [field.name for field in table.schema().fields]
        if existing_columns != arrow_table.column_names:
            catalog.drop_table(identifier)
            table = catalog.create_table(identifier, schema=arrow_table.schema)
            created = True
            table.append(arrow_table)
        else:
            table.overwrite(arrow_table)
    else:
        table = catalog.create_table(identifier, schema=arrow_table.schema)
        created = True
        table.append(arrow_table)
    return created, table.location()


def check_silver_quality_gate(
    catalog,
    *,
    silver_ns: str | None = None,
    datasets: Sequence[str] = REQUIRED_SILVER_DATASETS,
) -> dict[str, str]:
    """Pastikan seluruh dataset Silver lolos quality gate sebelum menulis Gold."""
    ns = silver_ns or silver_namespace()
    statuses: dict[str, str] = {}
    blocked: list[str] = []

    for name in datasets:
        report_id = f"{ns}.{name}_quality_report"
        if not catalog.table_exists(report_id):
            blocked.append(f"{name} (laporan mutu belum ada)")
            continue
        report_df = catalog.load_table(report_id).scan().to_pandas()
        if report_df.empty:
            blocked.append(f"{name} (laporan mutu kosong)")
            continue
        if "validated_at" in report_df.columns:
            report_df = report_df.sort_values("validated_at")
        latest = report_df.iloc[-1]
        status = str(latest.get("status", "unknown"))
        statuses[name] = status
        if status == "blocked":
            reasons = str(latest.get("block_reasons") or "ambang_gagal")
            blocked.append(f"{name} ({reasons})")

    if blocked:
        raise RuntimeError(
            "Publikasi Gold diblokir oleh quality gate Silver: " + "; ".join(blocked)
        )
    return statuses


def _read_silver_table(catalog, silver_ns: str, dataset: str) -> pd.DataFrame:
    identifier = f"{silver_ns}.{dataset}"
    if not catalog.table_exists(identifier):
        return pd.DataFrame()
    return catalog.load_table(identifier).scan().to_pandas()


def publish_gold(
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    run_id: str | None = None,
    settings: Settings | None = None,
    skip_quality_gate: bool = False,
) -> list[GoldResult]:
    """Bangun dan terbitkan ketiga mart Gold ke katalog Iceberg."""
    settings = settings or load_settings()
    gold_run_id = run_id or uuid4().hex
    silver_ns = silver_namespace()
    gold_ns = gold_namespace()

    catalog = build_catalog(settings)
    if not skip_quality_gate:
        check_silver_quality_gate(catalog, silver_ns=silver_ns)

    catalog.create_namespace_if_not_exists(gold_ns)

    public_power = _read_silver_table(catalog, silver_ns, "energy_charts_public_power")
    price = _read_silver_table(catalog, silver_ns, "energy_charts_price")
    weather = _read_silver_table(catalog, silver_ns, "open_meteo_weather_hourly")
    actual_load = _read_silver_table(catalog, silver_ns, "entsoe_actual_load")

    marts = {
        "mart_hourly_mix": build_mart_hourly_mix(public_power),
        "mart_daily_price": build_mart_daily_price(price),
        "mart_forecast_features": build_mart_forecast_features(
            public_power, price, weather, actual_load
        ),
    }

    results: list[GoldResult] = []
    for mart_name, incoming in marts.items():
        identifier = f"{gold_ns}.{mart_name}"
        existing = (
            catalog.load_table(identifier).scan().to_pandas()
            if catalog.table_exists(identifier)
            else None
        )
        merged = merge_incremental(
            existing,
            incoming,
            business_key=GOLD_MART_KEYS[mart_name],
            start_date=start_date,
            end_date=end_date,
        )
        created, location = write_gold_table(catalog, identifier, merged)
        results.append(
            GoldResult(
                table=identifier,
                rows=len(merged),
                created=created,
                location=location,
            )
        )

    emit_job(
        job_name="gold.publish",
        run_id=gold_run_id,
        inputs=[f"{silver_ns}.{name}" for name in REQUIRED_SILVER_DATASETS],
        outputs=[item.table for item in results],
    )
    return results
