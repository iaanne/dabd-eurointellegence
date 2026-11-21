"""Ringkas status Prefect dan katalog Iceberg menjadi angka yang bisa dipantau.

Fungsi di sini sengaja tidak menyentuh jaringan atau penyimpanan, supaya bisa
diuji dengan data contoh. Pengambilan data ada di :mod:`eurogrid.observability.exporter`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence


def parse_time(value: Any) -> datetime | None:
    """Baca waktu ISO 8601 dari Prefect; kembalikan None bila kosong/rusak."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    teks = str(value).strip()
    if not teks:
        return None
    if teks.endswith("Z"):
        teks = teks[:-1] + "+00:00"
    try:
        waktu = datetime.fromisoformat(teks)
    except ValueError:
        return None
    return waktu if waktu.tzinfo else waktu.replace(tzinfo=timezone.utc)


def epoch(value: Any) -> float | None:
    """Waktu ISO Prefect menjadi detik epoch."""
    waktu = parse_time(value)
    return waktu.timestamp() if waktu else None


@dataclass(frozen=True)
class FlowStats:
    """Ringkasan flow run yang diambil dari API Prefect."""

    runs: dict[tuple[str, str], int] = field(default_factory=dict)
    last_success: dict[str, float] = field(default_factory=dict)
    last_start: dict[str, float] = field(default_factory=dict)
    last_duration: dict[str, float] = field(default_factory=dict)
    last_state: dict[str, str] = field(default_factory=dict)


def flow_run_stats(
    flows: Iterable[Mapping[str, Any]],
    runs: Iterable[Mapping[str, Any]],
) -> FlowStats:
    """Hitung jumlah run per state, waktu sukses terakhir, dan durasi terakhir.

    ``flows`` adalah daftar objek Prefect dengan ``id`` dan ``name``;
    ``runs`` adalah flow run dengan ``flow_id``, ``state_type``,
    ``start_time``, dan ``end_time``.
    """
    nama_flow = {
        str(item.get("id")): str(item.get("name"))
        for item in flows
        if item.get("id") and item.get("name")
    }

    runs_count: dict[tuple[str, str], int] = {}
    last_success: dict[str, float] = {}
    last_start: dict[str, float] = {}
    last_duration: dict[str, float] = {}
    last_state: dict[str, str] = {}

    terbaru: dict[str, tuple[float, str]] = {}
    for run in runs:
        flow = nama_flow.get(str(run.get("flow_id")))
        if not flow:
            continue
        state = str(run.get("state_type") or "UNKNOWN")
        runs_count[(flow, state)] = runs_count.get((flow, state), 0) + 1

        mulai = epoch(run.get("start_time"))
        if mulai is None:
            continue

        if mulai > last_start.get(flow, float("-inf")):
            last_start[flow] = mulai
            last_state[flow] = state
            selesai = epoch(run.get("end_time"))
            if selesai is not None and selesai >= mulai:
                last_duration[flow] = selesai - mulai
            else:
                last_duration.pop(flow, None)

        if state == "COMPLETED":
            sukses = epoch(run.get("end_time")) or mulai
            if sukses > last_success.get(flow, float("-inf")):
                last_success[flow] = sukses

        if state in ("FAILED", "CRASHED") and flow not in terbaru:
            terbaru[flow] = (mulai, state)

    return FlowStats(
        runs=runs_count,
        last_success=last_success,
        last_start=last_start,
        last_duration=last_duration,
        last_state=last_state,
    )


def rows_from_summary(summary: Mapping[str, Any] | None) -> int | None:
    """Ambil jumlah baris dari ringkasan snapshot Iceberg."""
    if not summary:
        return None
    for kunci in ("total-records", "total_records"):
        nilai = summary.get(kunci)
        if nilai is None:
            continue
        try:
            return int(nilai)
        except (TypeError, ValueError):
            return None
    return None


def freshness_seconds(timestamp_ms: Any, now: float | None = None) -> float | None:
    """Umur snapshot dalam detik, yaitu selisih waktu sekarang dan waktu snapshot."""
    if timestamp_ms is None:
        return None
    try:
        dibuat = float(timestamp_ms) / 1000.0
    except (TypeError, ValueError):
        return None
    sekarang = now if now is not None else datetime.now(timezone.utc).timestamp()
    return max(0.0, sekarang - dibuat)


ARTIFACT_SUFFIXES = ("_quarantine", "_quality_report")


def dataset_of(table: str) -> tuple[str, str | None]:
    """Pisahkan nama tabel dari akhiran bantu Silver.

    ``silver.entsoe_actual_load_quarantine`` menjadi
    ``("silver.entsoe_actual_load", "quarantine")``.
    """
    for akhiran in ARTIFACT_SUFFIXES:
        if table.endswith(akhiran):
            return table[: -len(akhiran)], akhiran.lstrip("_")
    return table, None


def latest_report(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    """Baris laporan mutu dengan ``processed_at`` paling baru."""
    terpilih: Mapping[str, Any] | None = None
    waktu_terpilih = float("-inf")
    for row in rows:
        waktu = parse_time(row.get("processed_at"))
        if waktu is None:
            continue
        if waktu.timestamp() > waktu_terpilih:
            waktu_terpilih = waktu.timestamp()
            terpilih = row
    if terpilih is not None:
        return terpilih
    return rows[-1] if rows else None
