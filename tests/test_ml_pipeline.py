from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from eurogrid.ml.experiment import run_experiments, time_based_split
from eurogrid.ml.inference import run_batch_inference


def _sample_features(n: int = 60) -> pd.DataFrame:
    hours = pd.date_range("2026-10-01T00:00:00Z", periods=n, freq="1h")
    rng = np.random.default_rng(42)
    base_load = 50000.0 + 5000.0 * np.sin(np.arange(n) * (2 * np.pi / 24.0))
    return pd.DataFrame(
        {
            "event_date": hours.date,
            "timestamp_utc": hours,
            "country": ["de"] * n,
            "bzn": ["DE-LU"] * n,
            "target_load_mw": base_load + rng.normal(0, 100, size=n),
            "day_ahead_price_eur_mwh": 80.0 + rng.normal(0, 5, size=n),
            "temperature_2m_c": 12.0 + rng.normal(0, 2, size=n),
            "wind_speed_10m_kmh": 15.0 + rng.normal(0, 3, size=n),
            "shortwave_radiation_wm2": np.maximum(0.0, 200.0 * np.sin(np.arange(n) * 0.2)),
            "precipitation_mm": np.zeros(n),
            "hour_of_day_local": hours.hour,
            "day_of_week": (hours.day_of_week + 1),
            "is_weekend": hours.day_of_week >= 5,
            "load_lag_1h_mw": np.roll(base_load, 1),
            "load_lag_24h_mw": np.roll(base_load, 24),
            "load_lag_168h_mw": base_load,
            "load_rolling_mean_24h_mw": base_load,
        }
    )


def test_time_based_split_tanpa_leakage() -> None:
    df = _sample_features(50).sample(frac=1.0, random_state=7)  # acak urutan masuk
    train_df, test_df = time_based_split(df, test_ratio=0.2)

    assert len(train_df) == 40
    assert len(test_df) == 10
    assert train_df["timestamp_utc"].max() < test_df["timestamp_utc"].min()


def test_dua_eksperimen_pada_test_set_sama(tmp_path: Path) -> None:
    df = _sample_features(60)
    report = run_experiments(df, artifact_dir=tmp_path / "models", seed=42)

    assert len(report.runs) == 2
    assert report.train_rows == 48
    assert report.test_rows == 12
    assert report.best_model_name in {r.model_name for r in report.runs}
    for run in report.runs:
        assert Path(run.artifact_path).is_file()
        assert run.dataset_version == report.dataset_version
        assert "mae" in run.metrics and "rmse" in run.metrics


def test_batch_inference_tandai_payload_rusak(tmp_path: Path) -> None:
    df = _sample_features(60)
    report = run_experiments(df, artifact_dir=tmp_path / "models", seed=42)
    best_run = next(r for r in report.runs if r.model_version == report.best_model_version)
    with Path(best_run.artifact_path).open("rb") as f:
        model = pickle.load(f)

    test_batch = df.tail(3).copy().reset_index(drop=True)
    # Suntikkan payload rusak pada baris terakhir
    test_batch["load_lag_1h_mw"] = test_batch["load_lag_1h_mw"].astype(object)
    test_batch.loc[2, "temperature_2m_c"] = np.nan
    test_batch.loc[2, "load_lag_1h_mw"] = "rusak"

    preds = run_batch_inference(
        test_batch,
        model=model,
        model_version=report.best_model_version,
    )

    assert len(preds) == 3
    assert list(preds["error_status"].iloc[:2]) == ["OK", "OK"]
    assert preds["predicted_load_mw"].iloc[:2].notna().all()
    assert preds["error_status"].iloc[2].startswith("MALFORMED_INPUT:")
    assert pd.isna(preds["predicted_load_mw"].iloc[2])
    assert preds["model_version"].nunique() == 1
