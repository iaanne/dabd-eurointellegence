"""Quality gate Bronze ke Silver.

Satu frame Bronze masuk, tiga keluaran keluar:

* ``passed``     -- baris lolos, siap ditulis ke tabel Silver;
* ``quarantine`` -- baris asli ditambah alasan kegagalan dan jejak run;
* ``report``     -- ringkasan satu run untuk laporan mutu.

Aturan per dataset ada di :mod:`eurogrid.silver.rules`. Modul ini sengaja tidak
menyentuh jaringan atau katalog agar bisa diuji dengan frame sintetis.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

import numpy as np
import pandas as pd

from ..revisions import beyond_available_until, collapse_revisions
from .rules import DatasetRule, SeriesRule, ValueBounds


@dataclass
class QualityResult:
    """Hasil satu run validasi untuk satu dataset."""

    passed: pd.DataFrame
    quarantine: pd.DataFrame
    report: dict[str, Any]

    @property
    def blocked(self) -> bool:
        return self.report["status"] == "blocked"


def validate_frame(
    frame: pd.DataFrame,
    rule: DatasetRule,
    *,
    run_id: str,
    processed_at: datetime | pd.Timestamp | None = None,
    dataset_label: str | None = None,
) -> QualityResult:
    """Jalankan seluruh pemeriksaan untuk satu dataset.

    ``dataset_label`` dipakai laporan bila nama dataset perlu dibedakan dari
    nama aturan, misalnya saat menguji satu aturan dengan data contoh.
    """
    validated_at = pd.Timestamp(processed_at or datetime.now(timezone.utc))
    if validated_at.tzinfo is None:
        validated_at = validated_at.tz_localize("UTC")
    dataset = dataset_label or rule.name

    work = frame.copy()
    rows_in = len(work)
    duplicates_dropped, work = collapse_revisions(work, rule.key)

    violations: dict[str, pd.Series] = {}
    warnings: dict[str, pd.Series] = {}

    def add(bucket: dict[str, pd.Series], name: str, mask: pd.Series) -> None:
        if bool(mask.any()):
            bucket[name] = mask

    # 1. Kolom wajib dan nilai kosong.
    for column in rule.required:
        if column not in work.columns:
            add(
                violations,
                f"kolom_hilang:{column}",
                pd.Series(True, index=work.index),
            )
            continue
        kosong = _blank_mask(work[column])
        add(violations, f"kolom_wajib_kosong:{column}", kosong)

    # 2. Timestamp: bisa dibaca dan punya zona waktu.
    if "timestamp_utc" in work.columns:
        raw_ts = work["timestamp_utc"]
        parsed_ts = pd.to_datetime(raw_ts, utc=True, errors="coerce")
        add(violations, "timestamp_tidak_valid", parsed_ts.isna())
        naive = pd.Series(False, index=work.index)
        if pd.api.types.is_datetime64_any_dtype(raw_ts):
            if getattr(raw_ts.dt, "tz", None) is None:
                naive = raw_ts.notna()
        add(violations, "timestamp_tanpa_zona", naive)
    else:
        parsed_ts = pd.Series(pd.NaT, index=work.index)

    # 2b. Revisi sumber: data setelah ``available_until`` belum final.
    add(violations, "melewati_available_until", beyond_available_until(work))

    # 3. Nilai: numerik dan berhingga.
    if "value" in work.columns:
        raw_value = work["value"]
        values = pd.to_numeric(raw_value, errors="coerce")
        add(violations, "nilai_bukan_angka", values.isna() & raw_value.notna())
        tak_berhingga = values.notna() & ~np.isfinite(values.astype("float64"))
        add(violations, "nilai_tak_berhingga", tak_berhingga)
    else:
        values = pd.Series(np.nan, index=work.index)

    # 4. Satuan dan resolusi sesuai kontrak dataset.
    add(violations, "satuan_tidak_sesuai", _unit_violation(work, rule))
    if rule.expected_resolution and "resolution" in work.columns:
        resolusi = work["resolution"].astype("string").str.strip()
        add(
            violations,
            "resolusi_tidak_sesuai",
            resolusi.notna() & (resolusi != rule.expected_resolution),
        )

    # 5. Batas nilai, batas peringatan, dan larangan negatif per seri.
    add(violations, "nilai_terlalu_kecil", _bounds_violation(work, values, rule, "low"))
    add(violations, "nilai_terlalu_besar", _bounds_violation(work, values, rule, "high"))
    add(violations, "nilai_negatif", _negative_violation(work, values, rule))
    add(warnings, "nilai_mencurigakan_kecil", _warn_violation(work, values, rule, "low"))
    add(warnings, "nilai_mencurigakan_besar", _warn_violation(work, values, rule, "high"))

    # 6. Susun karantina.
    violation_count = pd.Series(0, index=work.index)
    for mask in violations.values():
        violation_count = violation_count + mask.astype("int64")

    failed = violation_count > 0
    reason_text = _reason_text(work.index, violations)

    # Peringatan hanya bermakna untuk baris yang lolos.
    warnings = {
        nama: (mask & ~failed) for nama, mask in warnings.items() if bool((mask & ~failed).any())
    }

    passed = work.loc[~failed].copy()
    quarantine = work.loc[failed].copy()
    quarantine["reasons"] = reason_text.loc[failed]
    quarantine["violation_count"] = violation_count.loc[failed].astype("int64")
    quarantine["quality_run_id"] = run_id
    quarantine["bronze_table"] = rule.bronze_table
    quarantine["validated_at"] = validated_at

    # 7. Normalisasi satuan pada baris yang lolos, simpan aslinya untuk audit.
    if rule.unit_aliases and "unit" in passed.columns:
        asli = passed["unit"].astype("string")
        dinormalkan = asli.map(lambda u: rule.unit_aliases.get(str(u).strip(), u))
        passed["unit_source"] = asli
        passed["unit"] = dinormalkan
    passed["quality_status"] = "valid"
    passed["quality_run_id"] = run_id
    passed["validated_at"] = validated_at
    if "timestamp_utc" in passed.columns:
        passed = passed.sort_values("timestamp_utc", ignore_index=True)
    else:  # pragma: no cover - hanya bila kolom kunci hilang total
        passed = passed.reset_index(drop=True)

    # 8. Celah waktu dihitung dari timestamp baris yang lolos.
    if "timestamp_utc" in passed.columns and not passed.empty:
        unique_ts = (
            pd.to_datetime(passed["timestamp_utc"], utc=True, errors="coerce")
            .dropna()
            .drop_duplicates()
            .sort_values()
        )
    else:
        unique_ts = pd.Series(dtype="datetime64[ns, UTC]")
    gap_count, missing_intervals, expected_total = _gap_stats(unique_ts, rule)
    missing_rate = (
        missing_intervals / expected_total if expected_total else 0.0
    )

    rows_failed = int(failed.sum())
    failure_rate = rows_failed / rows_in if rows_in else 0.0

    block_reasons: list[str] = []
    if failure_rate > rule.max_failure_rate:
        block_reasons.append(
            f"ambang_gagal: {failure_rate:.4%} > {rule.max_failure_rate:.2%}"
        )
    if missing_rate > rule.max_missing_rate:
        block_reasons.append(
            f"celah_waktu: {missing_rate:.4%} > {rule.max_missing_rate:.2%}"
        )

    report: dict[str, Any] = {
        "dataset": dataset,
        "bronze_table": rule.bronze_table,
        "quality_run_id": run_id,
        "processed_at": validated_at,
        "rows_in": rows_in,
        "rows_passed": int(len(passed)),
        "rows_failed": rows_failed,
        "duplicates_dropped": duplicates_dropped,
        "revision_not_available": int(beyond_available_until(work).sum()),
        "failure_rate": round(failure_rate, 6),
        "max_failure_rate": rule.max_failure_rate,
        "gap_count": gap_count,
        "missing_intervals": missing_intervals,
        "missing_rate": round(float(missing_rate), 6),
        "max_missing_rate": rule.max_missing_rate,
        "violations": _summary(violations),
        "warnings": _summary(warnings),
        "status": "blocked" if block_reasons else "passed",
        "block_reasons": "; ".join(block_reasons),
        "rules_note": rule.notes,
    }
    return QualityResult(passed=passed, quarantine=quarantine, report=report)


def _blank_mask(column: pd.Series) -> pd.Series:
    kosong = column.isna()
    if column.dtype == object or pd.api.types.is_string_dtype(column):
        teks = column.astype("string").str.strip()
        kosong = kosong | (teks.fillna("") == "")
    return kosong.fillna(False)


def _series_column(work: pd.DataFrame, rule: DatasetRule) -> pd.Series:
    if rule.series_column in work.columns:
        return work[rule.series_column].astype("string")
    return pd.Series(pd.NA, index=work.index, dtype="string")


def _series_rule(rule: DatasetRule, nama: Any) -> SeriesRule | None:
    if nama is None or pd.isna(nama):
        return None
    return rule.series.get(str(nama))


def _unit_violation(work: pd.DataFrame, rule: DatasetRule) -> pd.Series:
    gagal = pd.Series(False, index=work.index)
    if "unit" not in work.columns:
        return gagal

    satuan = work["unit"].astype("string").str.strip()
    if rule.units:
        gagal = gagal | ~satuan.isin(list(rule.units))
    seri = _series_column(work, rule)
    for nama, series_rule in rule.series.items():
        if not series_rule.unit:
            continue
        dipilih = seri == nama
        gagal = gagal | (dipilih & (satuan != series_rule.unit))
    return gagal.fillna(False)


def _bounds_plan(rule: DatasetRule, work: pd.DataFrame):
    """Pasangan (pemilih baris, batas) yang berlaku untuk tiap baris."""
    seri = _series_column(work, rule)
    semua = pd.Series(True, index=work.index)
    rencana = [(semua, rule.bounds)]
    for nama, series_rule in rule.series.items():
        if series_rule.bounds is not None:
            rencana.append((seri == nama, series_rule.bounds))
    return rencana


def _bounds_violation(
    work: pd.DataFrame, values: pd.Series, rule: DatasetRule, sisi: str
) -> pd.Series:
    gagal = pd.Series(False, index=work.index)
    for dipilih, batas in _bounds_plan(rule, work):
        if sisi == "low" and batas.reject_below is not None:
            gagal = gagal | (dipilih & (values < batas.reject_below))
        if sisi == "high" and batas.reject_above is not None:
            gagal = gagal | (dipilih & (values > batas.reject_above))
    return gagal.fillna(False)


def _warn_violation(
    work: pd.DataFrame, values: pd.Series, rule: DatasetRule, sisi: str
) -> pd.Series:
    peringatan = pd.Series(False, index=work.index)
    for dipilih, batas in _bounds_plan(rule, work):
        if sisi == "low" and batas.warn_below is not None:
            peringatan = peringatan | (dipilih & (values < batas.warn_below))
        if sisi == "high" and batas.warn_above is not None:
            peringatan = peringatan | (dipilih & (values > batas.warn_above))
    return peringatan.fillna(False)


def _negative_violation(
    work: pd.DataFrame, values: pd.Series, rule: DatasetRule
) -> pd.Series:
    gagal = pd.Series(False, index=work.index)
    seri = _series_column(work, rule)
    for nama, series_rule in rule.series.items():
        if series_rule.allow_negative:
            continue
        gagal = gagal | ((seri == nama) & (values < 0))
    return gagal.fillna(False)


def _reason_text(index: pd.Index, violations: Mapping[str, pd.Series]) -> pd.Series:
    teks = np.full(len(index), "", dtype=object)
    for nama, mask in violations.items():
        aktif = mask.fillna(False).to_numpy(dtype=bool)
        teks = np.where(
            aktif,
            np.where(teks == "", nama, np.char.add(np.char.add(teks, "|"), nama)),
            teks,
        )
    return pd.Series(teks, index=index, dtype=object)


def _summary(bucket: Mapping[str, pd.Series]) -> str:
    if not bucket:
        return ""
    return "; ".join(f"{nama}={int(mask.sum())}" for nama, mask in sorted(bucket.items()))


def _gap_stats(
    unique_ts: pd.Series, rule: DatasetRule
) -> tuple[int, int, int]:
    """Hitung jumlah celah dan interval yang hilang pada sumbu waktu."""
    if rule.expected_interval_minutes is None or len(unique_ts) < 2:
        return 0, 0, int(len(unique_ts))

    expected = pd.Timedelta(minutes=rule.expected_interval_minutes)
    jarak = unique_ts.diff().dropna()
    gap_count = int((jarak > expected).sum())
    rentang = (unique_ts.iloc[-1] - unique_ts.iloc[0]) / expected + 1
    expected_total = int(round(float(rentang)))
    missing = max(0, expected_total - int(len(unique_ts)))
    return gap_count, missing, max(expected_total, int(len(unique_ts)))
