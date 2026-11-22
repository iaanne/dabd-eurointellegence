"""Eksperimen ML peramalan beban listrik dengan time-based split dan tracking MLflow (F-11)."""

from __future__ import annotations

import hashlib
import json
import os
import pickle
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

DEFAULT_SEED = 42
TARGET_COLUMN = "target_load_mw"
FEATURE_COLUMNS: tuple[str, ...] = (
    "temperature_2m_c",
    "wind_speed_10m_kmh",
    "shortwave_radiation_wm2",
    "precipitation_mm",
    "day_ahead_price_eur_mwh",
    "hour_of_day_local",
    "day_of_week",
    "is_weekend",
    "load_lag_1h_mw",
    "load_lag_24h_mw",
    "load_lag_168h_mw",
    "load_rolling_mean_24h_mw",
)


@dataclass(frozen=True)
class ExperimentRun:
    model_name: str
    model_version: str
    params: dict[str, Any]
    metrics: dict[str, float]
    artifact_path: str
    dataset_version: str
    code_version: str
    mlflow_run_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExperimentReport:
    runs: tuple[ExperimentRun, ...]
    best_model_name: str
    best_model_version: str
    train_rows: int
    test_rows: int
    dataset_version: str
    code_version: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "runs": [run.to_dict() for run in self.runs],
            "best_model_name": self.best_model_name,
            "best_model_version": self.best_model_version,
            "train_rows": self.train_rows,
            "test_rows": self.test_rows,
            "dataset_version": self.dataset_version,
            "code_version": self.code_version,
        }


def compute_dataset_version(frame: pd.DataFrame) -> str:
    """Hash SHA-256 deterministik dari isi dataset fitur."""
    if frame.empty:
        return hashlib.sha256(b"empty").hexdigest()[:12]
    ordered = frame.sort_values("timestamp_utc", ignore_index=True)
    payload = ordered.to_json(date_format="iso", orient="records").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:12]


def get_code_version() -> str:
    """Ambil commit hash git singkat bila tersedia."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=3,
        )
        return out.strip() or "unknown"
    except Exception:
        return "unknown"


def time_based_split(
    frame: pd.DataFrame, *, test_ratio: float = 0.2
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Bagi train/test secara kronologis untuk mencegah data leakage."""
    if not 0.05 <= test_ratio <= 0.5:
        raise ValueError(f"test_ratio harus di antara 0.05 dan 0.5, diterima: {test_ratio}")

    clean = frame.dropna(subset=["timestamp_utc", TARGET_COLUMN]).copy()
    clean["timestamp_utc"] = pd.to_datetime(clean["timestamp_utc"], utc=True)
    clean = clean.sort_values("timestamp_utc", ignore_index=True)

    if len(clean) < 20:
        raise ValueError(
            f"Data latih terlalu sedikit ({len(clean)} baris); minimal 20 baris."
        )

    split_idx = int(len(clean) * (1.0 - test_ratio))
    split_idx = max(10, min(split_idx, len(clean) - 5))
    return clean.iloc[:split_idx].copy(), clean.iloc[split_idx:].copy()


def _prepare_matrix(
    frame: pd.DataFrame, feature_cols: Sequence[str]
) -> pd.DataFrame:
    mat = pd.DataFrame(index=frame.index)
    for col in feature_cols:
        if col in frame.columns:
            series = frame[col]
            if series.dtype == bool:
                mat[col] = series.astype(float)
            else:
                mat[col] = pd.to_numeric(series, errors="coerce")
        else:
            mat[col] = np.nan
    return mat


def _evaluate(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    non_zero = y_true != 0
    mape = (
        float(np.mean(np.abs((y_true[non_zero] - y_pred[non_zero]) / y_true[non_zero])) * 100.0)
        if bool(non_zero.any())
        else 0.0
    )
    r2 = float(r2_score(y_true, y_pred))
    return {
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "mape_pct": round(mape, 4),
        "r2": round(r2, 4),
    }


def _log_to_mlflow(
    tracking_uri: str,
    *,
    experiment_name: str,
    run_name: str,
    params: dict[str, Any],
    metrics: dict[str, float],
    tags: dict[str, str],
) -> str | None:
    """Catat run ke MLflow REST API bila server tersedia."""
    import httpx

    base = tracking_uri.rstrip("/")
    try:
        resp = httpx.get(
            f"{base}/api/2.0/mlflow/experiments/get-by-name",
            params={"experiment_name": experiment_name},
            timeout=5.0,
        )
        if resp.status_code == 200:
            exp_id = resp.json()["experiment"]["experiment_id"]
        else:
            create_exp = httpx.post(
                f"{base}/api/2.0/mlflow/experiments/create",
                json={"name": experiment_name},
                timeout=5.0,
            )
            create_exp.raise_for_status()
            exp_id = create_exp.json()["experiment_id"]

        run_resp = httpx.post(
            f"{base}/api/2.0/mlflow/runs/create",
            json={
                "experiment_id": exp_id,
                "run_name": run_name,
                "start_time": int(datetime.now(timezone.utc).timestamp() * 1000),
                "tags": [{"key": k, "value": str(v)} for k, v in tags.items()],
            },
            timeout=5.0,
        )
        run_resp.raise_for_status()
        run_id = run_resp.json()["run"]["info"]["run_id"]

        httpx.post(
            f"{base}/api/2.0/mlflow/runs/log-batch",
            json={
                "run_id": run_id,
                "params": [{"key": k, "value": str(v)} for k, v in params.items()],
                "metrics": [
                    {
                        "key": k,
                        "value": float(v),
                        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                        "step": 0,
                    }
                    for k, v in metrics.items()
                ],
            },
            timeout=5.0,
        ).raise_for_status()

        httpx.post(
            f"{base}/api/2.0/mlflow/runs/update",
            json={
                "run_id": run_id,
                "status": "FINISHED",
                "end_time": int(datetime.now(timezone.utc).timestamp() * 1000),
            },
            timeout=5.0,
        )
        return run_id
    except Exception:
        return None


def run_experiments(
    features_df: pd.DataFrame,
    *,
    artifact_dir: str | Path = "data/models",
    mlflow_tracking_uri: str | None = None,
    experiment_name: str = "eurogrid-load-forecast",
    test_ratio: float = 0.2,
    seed: int = DEFAULT_SEED,
) -> ExperimentReport:
    """Latih dua model (baseline Ridge vs HistGradientBoosting) pada test set yang sama."""
    train_df, test_df = time_based_split(features_df, test_ratio=test_ratio)
    dataset_version = compute_dataset_version(features_df)
    code_version = get_code_version()

    x_train = _prepare_matrix(train_df, FEATURE_COLUMNS)
    y_train = train_df[TARGET_COLUMN].to_numpy(dtype=float)
    x_test = _prepare_matrix(test_df, FEATURE_COLUMNS)
    y_test = test_df[TARGET_COLUMN].to_numpy(dtype=float)

    candidates: list[tuple[str, Any, dict[str, Any]]] = [
        (
            "baseline_ridge",
            Pipeline(
                [
                    ("imputer", SimpleImputer(strategy="median")),
                    ("scaler", StandardScaler()),
                    ("model", Ridge(alpha=1.0, random_state=seed)),
                ]
            ),
            {"model_type": "Ridge", "alpha": 1.0, "seed": seed, "test_ratio": test_ratio},
        ),
        (
            "candidate_hist_gradient_boosting",
            Pipeline(
                [
                    ("imputer", SimpleImputer(strategy="median")),
                    (
                        "model",
                        HistGradientBoostingRegressor(
                            max_iter=200,
                            learning_rate=0.05,
                            max_depth=6,
                            random_state=seed,
                        ),
                    ),
                ]
            ),
            {
                "model_type": "HistGradientBoostingRegressor",
                "max_iter": 200,
                "learning_rate": 0.05,
                "max_depth": 6,
                "seed": seed,
                "test_ratio": test_ratio,
            },
        ),
    ]

    out_root = Path(artifact_dir)
    out_root.mkdir(parents=True, exist_ok=True)
    tracking_uri = mlflow_tracking_uri or os.getenv("MLFLOW_TRACKING_URI", "").strip() or None

    runs: list[ExperimentRun] = []
    for model_name, pipeline, params in candidates:
        pipeline.fit(x_train, y_train)
        preds = pipeline.predict(x_test)
        metrics = _evaluate(y_test, preds)
        model_version = f"{model_name}-{dataset_version[:8]}-s{seed}"

        model_dir = out_root / model_version
        model_dir.mkdir(parents=True, exist_ok=True)
        model_path = model_dir / "model.pkl"
        with model_path.open("wb") as f:
            pickle.dump(pipeline, f)

        meta_path = model_dir / "metadata.json"
        meta_path.write_text(
            json.dumps(
                {
                    "model_name": model_name,
                    "model_version": model_version,
                    "params": params,
                    "metrics": metrics,
                    "dataset_version": dataset_version,
                    "code_version": code_version,
                    "features": list(FEATURE_COLUMNS),
                    "target": TARGET_COLUMN,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        mlflow_run_id = None
        if tracking_uri:
            mlflow_run_id = _log_to_mlflow(
                tracking_uri,
                experiment_name=experiment_name,
                run_name=model_version,
                params=params,
                metrics=metrics,
                tags={
                    "dataset_version": dataset_version,
                    "code_version": code_version,
                    "model_version": model_version,
                },
            )

        runs.append(
            ExperimentRun(
                model_name=model_name,
                model_version=model_version,
                params=params,
                metrics=metrics,
                artifact_path=str(model_path),
                dataset_version=dataset_version,
                code_version=code_version,
                mlflow_run_id=mlflow_run_id,
            )
        )

    best = min(runs, key=lambda r: r.metrics["mae"])
    report = ExperimentReport(
        runs=tuple(runs),
        best_model_name=best.model_name,
        best_model_version=best.model_version,
        train_rows=len(train_df),
        test_rows=len(test_df),
        dataset_version=dataset_version,
        code_version=code_version,
    )
    (out_root / "latest_report.json").write_text(
        json.dumps(report.to_dict(), indent=2), encoding="utf-8"
    )
    return report
