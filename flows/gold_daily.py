"""Flow harian dan backfill yang membangun lapisan Gold dan menjalankan batch inference.

Dipicu otomatis oleh ``silver-daily`` bila tidak ada dataset Silver yang berstatus
``blocked``. Dapat juga dijalankan manual dengan parameter ``start_date`` dan
``end_date`` untuk backfill idempoten (F-03, F-11, F-12).
"""

from __future__ import annotations

from typing import Any

from prefect import flow, task

from eurogrid.observability.logging import configure_json_logging, flow_run_id

SERVICE = "eurogrid-gold-daily"

configure_json_logging(SERVICE)

RETRY_DELAYS = [30, 120]
TASK_TIMEOUT_SECONDS = 1800


@task(
    name="publish-gold-marts",
    retries=2,
    retry_delay_seconds=RETRY_DELAYS,
    timeout_seconds=TASK_TIMEOUT_SECONDS,
    log_prints=True,
)
def publish_gold_task(
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, Any]]:
    from eurogrid.gold.warehouse import publish_gold

    results = publish_gold(start_date=start_date, end_date=end_date)
    for item in results:
        print(f"Tabel {item.table}: {item.rows} baris")
    return [item.to_dict() for item in results]


@task(
    name="batch-inference-ml",
    retries=1,
    retry_delay_seconds=[30],
    timeout_seconds=TASK_TIMEOUT_SECONDS,
    log_prints=True,
)
def batch_inference_task() -> dict[str, Any]:
    from pathlib import Path

    from eurogrid.bronze.warehouse import build_catalog
    from eurogrid.gold.warehouse import gold_namespace
    from eurogrid.ml.experiment import run_experiments
    from eurogrid.ml.inference import publish_predictions

    artifact_dir = Path("data/models")
    if not (artifact_dir / "latest_report.json").is_file():
        catalog = build_catalog()
        features_df = catalog.load_table(f"{gold_namespace()}.mart_forecast_features").scan().to_pandas()
        report = run_experiments(features_df, artifact_dir=artifact_dir)
        print(
            f"Model dilatih: terbaik={report.best_model_version} "
            f"(train={report.train_rows}, test={report.test_rows})"
        )

    summary = publish_predictions(artifact_dir=artifact_dir)
    print(
        f"Prediksi {summary['table']}: {summary['rows_ok']} OK, "
        f"{summary['rows_malformed']} malformed"
    )
    return summary


@flow(name="gold-daily", log_prints=True)
def gold_daily(
    start_date: str | None = None,
    end_date: str | None = None,
    run_ml: bool = True,
) -> dict[str, Any]:
    """Bangun tiga mart Gold dan jalankan batch inference bila diaktifkan."""
    configure_json_logging(SERVICE, run_id=flow_run_id())
    marts = publish_gold_task(start_date=start_date, end_date=end_date)
    ml_summary: dict[str, Any] | None = None
    if run_ml:
        ml_summary = batch_inference_task()
    return {
        "marts": marts,
        "ml": ml_summary,
    }


if __name__ == "__main__":
    gold_daily()
