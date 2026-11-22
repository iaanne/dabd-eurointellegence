"""Scheduled batch inference dan pencatatan tabel prediksi ``gold.ml_load_predictions`` (F-12)."""

from __future__ import annotations

import hashlib
import json
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd

from ..bronze.warehouse import build_catalog
from ..config import Settings, load_settings
from ..gold.marts import GOLD_MART_KEYS, merge_incremental
from ..gold.warehouse import gold_namespace, write_gold_table
from ..lineage import emit_job
from .experiment import FEATURE_COLUMNS, TARGET_COLUMN

PREDICTION_COLUMNS: tuple[str, ...] = (
    "input_record_id",
    "timestamp_utc",
    "country",
    "bzn",
    "model_version",
    "predicted_at",
    "predicted_load_mw",
    "actual_load_mw",
    "error_status",
)

CORE_INFERENCE_FEATURES: tuple[str, ...] = (
    "temperature_2m_c",
    "wind_speed_10m_kmh",
    "load_lag_1h_mw",
    "load_lag_24h_mw",
)


def _record_id(country: Any, bzn: Any, ts: Any) -> str:
    raw = f"{country}|{bzn}|{ts}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def run_batch_inference(
    features_df: pd.DataFrame,
    *,
    model: Any,
    model_version: str,
    predicted_at: datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Jalankan inference batch; baris rusak/tidak lengkap ditandai ``MALFORMED_INPUT``."""
    if features_df.empty:
        return pd.DataFrame(columns=list(PREDICTION_COLUMNS))

    pred_ts = pd.Timestamp(predicted_at or datetime.now(timezone.utc))
    if pred_ts.tzinfo is None:
        pred_ts = pred_ts.tz_localize("UTC")
    else:
        pred_ts = pred_ts.tz_convert("UTC")

    work = features_df.copy()
    parsed_ts = pd.to_datetime(work.get("timestamp_utc"), utc=True, errors="coerce")

    rows: list[dict[str, Any]] = []
    valid_indices: list[int] = []
    valid_matrix_rows: list[dict[str, float]] = []

    for idx, row in work.iterrows():
        ts_val = parsed_ts.loc[idx]
        country = row.get("country")
        bzn = row.get("bzn")
        reasons: list[str] = []

        if pd.isna(ts_val):
            reasons.append("timestamp_invalid")
        if pd.isna(country) or not str(country).strip():
            reasons.append("country_kosong")
        if pd.isna(bzn) or not str(bzn).strip():
            reasons.append("bzn_kosong")

        feature_vals: dict[str, float] = {}
        for col in FEATURE_COLUMNS:
            raw_val = row.get(col)
            try:
                num = float(raw_val) if pd.notna(raw_val) else np.nan
            except (TypeError, ValueError):
                num = np.nan
                if col in CORE_INFERENCE_FEATURES:
                    reasons.append(f"{col}_bukan_angka")
            if not np.isnan(num) and not np.isfinite(num):
                reasons.append(f"{col}_tak_berhingga")
                num = np.nan
            if col in CORE_INFERENCE_FEATURES and np.isnan(num):
                reasons.append(f"{col}_kosong")
            feature_vals[col] = num

        actual_raw = row.get(TARGET_COLUMN)
        try:
            actual_val = float(actual_raw) if pd.notna(actual_raw) else np.nan
        except (TypeError, ValueError):
            actual_val = np.nan

        rec_id = _record_id(country, bzn, ts_val.isoformat() if pd.notna(ts_val) else f"row-{idx}")

        if reasons:
            rows.append(
                {
                    "input_record_id": rec_id,
                    "timestamp_utc": ts_val,
                    "country": str(country) if pd.notna(country) else "",
                    "bzn": str(bzn) if pd.notna(bzn) else "",
                    "model_version": model_version,
                    "predicted_at": pred_ts,
                    "predicted_load_mw": np.nan,
                    "actual_load_mw": actual_val,
                    "error_status": "MALFORMED_INPUT:" + ",".join(sorted(set(reasons))),
                }
            )
        else:
            valid_indices.append(len(rows))
            valid_matrix_rows.append(feature_vals)
            rows.append(
                {
                    "input_record_id": rec_id,
                    "timestamp_utc": ts_val,
                    "country": str(country),
                    "bzn": str(bzn),
                    "model_version": model_version,
                    "predicted_at": pred_ts,
                    "predicted_load_mw": np.nan,
                    "actual_load_mw": actual_val,
                    "error_status": "OK",
                }
            )

    if valid_matrix_rows:
        x_pred = pd.DataFrame(valid_matrix_rows, columns=list(FEATURE_COLUMNS))
        preds = model.predict(x_pred)
        for pos, pred_val in zip(valid_indices, preds):
            rows[pos]["predicted_load_mw"] = float(pred_val)

    result = pd.DataFrame(rows, columns=list(PREDICTION_COLUMNS))
    result["timestamp_utc"] = pd.to_datetime(result["timestamp_utc"], utc=True)
    result["predicted_at"] = pd.to_datetime(result["predicted_at"], utc=True)
    return result


def load_latest_model(artifact_dir: str | Path = "data/models") -> tuple[Any, str]:
    """Muat model terbaik terbaru dari ``latest_report.json``."""
    report_path = Path(artifact_dir) / "latest_report.json"
    if not report_path.is_file():
        raise FileNotFoundError(
            f"Laporan model tidak ditemukan di {report_path}. Jalankan `eurogrid train-ml` dulu."
        )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    best_version = report["best_model_version"]
    model_path = Path(artifact_dir) / best_version / "model.pkl"
    with model_path.open("rb") as f:
        model = pickle.load(f)
    return model, best_version


def publish_predictions(
    *,
    artifact_dir: str | Path = "data/models",
    run_id: str | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Baca ``gold.mart_forecast_features``, jalankan inference, dan tulis ``gold.ml_load_predictions``."""
    settings = settings or load_settings()
    gold_ns = gold_namespace()
    catalog = build_catalog(settings)

    features_id = f"{gold_ns}.mart_forecast_features"
    if not catalog.table_exists(features_id):
        raise RuntimeError(f"Tabel fitur {features_id} belum tersedia. Jalankan publish-gold dulu.")

    features_df = catalog.load_table(features_id).scan().to_pandas()
    model, model_version = load_latest_model(artifact_dir)
    incoming = run_batch_inference(features_df, model=model, model_version=model_version)

    pred_id = f"{gold_ns}.ml_load_predictions"
    existing = (
        catalog.load_table(pred_id).scan().to_pandas()
        if catalog.table_exists(pred_id)
        else None
    )
    merged = merge_incremental(
        existing,
        incoming,
        business_key=GOLD_MART_KEYS["ml_load_predictions"],
    )
    write_gold_table(catalog, pred_id, merged)

    emit_job(
        job_name="ml.batch_inference",
        run_id=run_id or uuid4().hex,
        inputs=[features_id],
        outputs=[pred_id],
    )
    return {
        "table": pred_id,
        "model_version": model_version,
        "rows_total": len(merged),
        "rows_ok": int((incoming["error_status"] == "OK").sum()),
        "rows_malformed": int((incoming["error_status"] != "OK").sum()),
    }
