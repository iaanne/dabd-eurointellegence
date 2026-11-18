"""Flow harian yang mengubah Bronze menjadi Silver tervalidasi.

Dijalankan setelah ``bronze-daily``. Setiap dataset melewati quality gate:
baris lolos masuk Silver, baris gagal masuk karantina, dan satu baris laporan
mutu dicatat per run. Bila tingkat kegagalan atau celah waktu melewati ambang,
status run menjadi ``blocked`` sehingga Gold tidak boleh dipublikasikan.
"""

from __future__ import annotations

from typing import Any

from prefect import flow, task

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


@flow(name="silver-daily", log_prints=True)
def silver_daily(datasets: list[str] | None = None) -> dict[str, Any]:
    """Validasi Bronze menjadi Silver untuk dataset terpilih."""
    hasil = validate_silver_task(datasets=datasets)
    total_lolos = sum(item["rows_passed"] for item in hasil)
    total_gagal = sum(item["rows_failed"] for item in hasil)
    diblokir = [item["dataset"] for item in hasil if item["status"] == "blocked"]

    print(
        f"Selesai: {total_lolos} baris lolos, {total_gagal} baris karantina, "
        f"{len(diblokir)} dataset diblokir"
    )
    return {
        "datasets": hasil,
        "rows_passed": total_lolos,
        "rows_failed": total_gagal,
        "blocked": diblokir,
    }


if __name__ == "__main__":
    silver_daily()
