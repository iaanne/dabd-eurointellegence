"""Publikasi Silver: data tervalidasi, karantina, dan laporan mutu.

Alur satu run:

1. Baca tabel Bronze dari katalog Iceberg.
2. Jalankan :func:`eurogrid.silver.quality.validate_frame`.
3. Tulis ulang tabel Silver untuk dataset itu (penulisan penuh, sehingga run
   ulang tidak menambah baris).
4. Tulis ulang tabel karantina dari run tersebut.
5. Tambahkan satu baris laporan mutu per ``quality_run_id``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import pandas as pd

from ..bronze.warehouse import _to_arrow, build_catalog
from ..config import Settings, load_settings
from ..lineage import emit_job
from .quality import validate_frame
from .rules import RULES, DatasetRule, get_rule


def silver_namespace() -> str:
    return os.getenv("ICEBERG_SILVER_NAMESPACE", "silver").strip() or "silver"


@dataclass(frozen=True)
class SilverResult:
    dataset: str
    silver_table: str
    quarantine_table: str
    report_table: str
    rows_passed: int
    rows_failed: int
    duplicates_dropped: int
    status: str
    block_reasons: str

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset": self.dataset,
            "silver_table": self.silver_table,
            "quarantine_table": self.quarantine_table,
            "report_table": self.report_table,
            "rows_passed": self.rows_passed,
            "rows_failed": self.rows_failed,
            "duplicates_dropped": self.duplicates_dropped,
            "status": self.status,
            "block_reasons": self.block_reasons,
        }


def publish_silver(
    *,
    datasets: list[str] | None = None,
    run_id: str | None = None,
    settings: Settings | None = None,
) -> list[SilverResult]:
    """Validasi seluruh dataset terpilih dan tulis hasilnya ke Iceberg."""
    settings = settings or load_settings()
    quality_run_id = run_id or uuid4().hex
    namespace = silver_namespace()
    selected = [get_rule(nama) for nama in datasets] if datasets else list(RULES.values())

    catalog = build_catalog(settings)
    catalog.create_namespace_if_not_exists(namespace)

    hasil: list[SilverResult] = []
    for rule in selected:
        frame = catalog.load_table(rule.bronze_table).scan().to_pandas()
        outcome = validate_frame(frame, rule, run_id=quality_run_id)

        _write_table(catalog, _qualified(rule, namespace, ""), outcome.passed)
        _write_table(catalog, _qualified(rule, namespace, "_quarantine"), outcome.quarantine)
        _append_report(
            catalog,
            _qualified(rule, namespace, "_quality_report"),
            outcome.report,
        )

        emit_job(
            job_name=f"silver.{rule.name}",
            run_id=f"{quality_run_id}-{rule.name}",
            inputs=[rule.bronze_table],
            outputs=[
                _qualified(rule, namespace, ""),
                _qualified(rule, namespace, "_quarantine"),
                _qualified(rule, namespace, "_quality_report"),
            ],
        )

        hasil.append(
            SilverResult(
                dataset=rule.name,
                silver_table=_qualified(rule, namespace, ""),
                quarantine_table=_qualified(rule, namespace, "_quarantine"),
                report_table=_qualified(rule, namespace, "_quality_report"),
                rows_passed=outcome.report["rows_passed"],
                rows_failed=outcome.report["rows_failed"],
                duplicates_dropped=outcome.report["duplicates_dropped"],
                status=outcome.report["status"],
                block_reasons=outcome.report["block_reasons"],
            )
        )
    return hasil


def _qualified(rule: DatasetRule, namespace: str, suffix: str) -> str:
    """Nama tabel Silver lengkap, misalnya ``silver.entsoe_actual_load``."""
    return f"{namespace}.{rule.name}{suffix}"


def _write_table(catalog, identifier: str, frame: pd.DataFrame) -> None:
    """Tulis ulang tabel; buat ulang bila susunan kolomnya berubah."""
    arrow_table = _to_arrow(frame)
    if catalog.table_exists(identifier):
        table = catalog.load_table(identifier)
        existing = [field.name for field in table.schema().fields]
        if existing != arrow_table.column_names:
            catalog.drop_table(identifier)
            catalog.create_table(identifier, schema=arrow_table.schema).append(arrow_table)
        else:
            table.overwrite(arrow_table)
        return
    catalog.create_table(identifier, schema=arrow_table.schema).append(arrow_table)


def _append_report(catalog, identifier: str, report: dict) -> None:
    """Simpan laporan mutu; satu baris per ``quality_run_id``."""
    baru = pd.DataFrame([report])
    if catalog.table_exists(identifier):
        lama = catalog.load_table(identifier).scan().to_pandas()
        gabungan = pd.concat([lama, baru], ignore_index=True)
        gabungan = gabungan.drop_duplicates(
            subset=["dataset", "quality_run_id"], keep="last"
        )
        _write_table(catalog, identifier, gabungan)
        return
    _write_table(catalog, identifier, baru)


def bronze_available(settings: Settings | None = None) -> list[str]:
    """Daftar tabel Bronze yang ada di katalog, untuk dipakai flow."""
    settings = settings or load_settings()
    catalog = build_catalog(settings)
    return [nama for _, nama in catalog.list_tables("bronze")]


def local_archive_path(settings: Settings | None = None) -> Path:
    """Folder arsip laporan mutu lokal, dipakai untuk bukti F-18."""
    settings = settings or load_settings()
    return Path(settings.data_dir) / "silver" / "quality"
