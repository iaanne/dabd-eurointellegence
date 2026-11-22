"""Flow harian yang mengubah Bronze menjadi Silver tervalidasi.

Dijalankan setelah ``bronze-daily``. Setiap dataset melewati quality gate:
baris lolos masuk Silver, baris gagal masuk karantina, dan satu baris laporan
mutu dicatat per run. Bila tingkat kegagalan atau celah waktu melewati ambang,
status run menjadi ``blocked`` sehingga Gold tidak boleh dipublikasikan.
"""

from __future__ import annotations

from typing import Any

from prefect import flow, task

from eurogrid.observability.logging import configure_json_logging, flow_run_id

SERVICE = "eurogrid-silver-daily"

configure_json_logging(SERVICE)

RETRY_DELAYS = [30, 120]
TASK_TIMEOUT_SECONDS = 1800


@task(
    name="validate-silver",
    retries=2,
    retry_delay_seconds=RETRY_DELAYS,
    timeout_seconds=TASK_TIMEOUT_SECONDS,
    log_prints=True,
)
def validate_silver_task(datasets: list[str] | None = None) -> list[dict[str, Any]]:
    from eurogrid.silver.warehouse import publish_silver

    hasil = publish_silver(datasets=datasets)
    for item in hasil:
        print(
            f"Tabel {item.silver_table}: {item.rows_passed} lolos, "
            f"{item.rows_failed} karantina, status {item.status}"
        )
        if item.block_reasons:
            print(f"  diblokir: {item.block_reasons}")
    return [item.to_dict() for item in hasil]


@task(
    name="trigger-gold",
    retries=2,
    retry_delay_seconds=RETRY_DELAYS,
    timeout_seconds=120,
    log_prints=True,
)
def trigger_gold_task(silver_run_id: str | None) -> dict[str, Any]:
    """Picu run Gold setelah Silver lolos tanpa status blocked."""
    from prefect.deployments import run_deployment

    try:
        flow_run = run_deployment(
            name="gold-daily/production",
            parameters={},
            timeout=0,
            as_subflow=False,
            tags=["dipicu-silver"],
            idempotency_key=f"gold-after-silver-{silver_run_id or 'manual'}",
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Gagal memicu Gold: {exc}")
        return {"triggered": False, "error": str(exc)}

    print(f"Gold dipicu setelah Silver: {flow_run.id}")
    return {"triggered": True, "flow_run_id": str(flow_run.id)}


@flow(name="silver-daily", log_prints=True)
def silver_daily(datasets: list[str] | None = None) -> dict[str, Any]:
    """Validasi Bronze menjadi Silver untuk dataset terpilih."""
    run_id = flow_run_id()
    configure_json_logging(SERVICE, run_id=run_id)
    hasil = validate_silver_task(datasets=datasets)
    total_lolos = sum(item["rows_passed"] for item in hasil)
    total_gagal = sum(item["rows_failed"] for item in hasil)
    diblokir = [item["dataset"] for item in hasil if item["status"] == "blocked"]

    print(
        f"Selesai: {total_lolos} baris lolos, {total_gagal} baris karantina, "
        f"{len(diblokir)} dataset diblokir"
    )
    gold_trigger: dict[str, Any] = {"triggered": False, "reason": "blocked"}
    if not diblokir:
        gold_trigger = trigger_gold_task(run_id)
    else:
        print(f"Gold tidak dipicu karena Silver diblokir: {', '.join(diblokir)}")

    return {
        "datasets": hasil,
        "rows_passed": total_lolos,
        "rows_failed": total_gagal,
        "blocked": diblokir,
        "gold_trigger": gold_trigger,
    }


if __name__ == "__main__":
    silver_daily()
