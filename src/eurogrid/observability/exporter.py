"""Exporter metrik Prometheus untuk EuroGrid.

Satu proses kecil yang membaca dua sumber:

* katalog Iceberg -- jumlah baris dan kesegaran snapshot tiap tabel Bronze/Silver,
  termasuk tabel karantina dan laporan mutu;
* API Prefect -- jumlah run per status, waktu sukses terakhir, dan durasi run.

Nilai diperbarui berkala, sedangkan endpoint ``/metrics`` melayani permintaan
Prometheus kapan pun. Kalau satu sumber gagal, ekspor tetap jalan dan kegagalan
dihitung di ``eurogrid_exporter_scrape_errors_total``.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Iterable

from prometheus_client import Counter, Gauge, start_http_server

from .logging import configure_json_logging
from .metrics import (
    dataset_of,
    flow_run_stats,
    freshness_seconds,
    latest_report,
    rows_from_summary,
)

LOG = logging.getLogger("eurogrid.observability.exporter")

TABLE_ROWS = Gauge(
    "eurogrid_table_rows",
    "Jumlah baris terakhir sebuah tabel Iceberg.",
    ["table", "layer"],
)
TABLE_FRESHNESS = Gauge(
    "eurogrid_table_freshness_seconds",
    "Umur snapshot terakhir tabel Iceberg dalam detik.",
    ["table", "layer"],
)
QUARANTINE_ROWS = Gauge(
    "eurogrid_quarantine_rows",
    "Baris karantina pada run Silver terakhir.",
    ["dataset"],
)
SILVER_FAILED = Gauge(
    "eurogrid_silver_rows_failed",
    "Baris gagal validasi pada run Silver terakhir.",
    ["dataset"],
)
SILVER_BLOCKED = Gauge(
    "eurogrid_silver_gate_blocked",
    "1 bila quality gate dataset memblokir publikasi Gold.",
    ["dataset"],
)
FLOW_RUNS = Gauge(
    "eurogrid_flow_runs",
    "Jumlah flow run yang terlihat di API Prefect, per status.",
    ["flow", "state"],
)
FLOW_LAST_SUCCESS = Gauge(
    "eurogrid_flow_last_success_timestamp_seconds",
    "Waktu selesai sukses terakhir sebuah flow, dalam detik epoch.",
    ["flow"],
)
FLOW_LAST_DURATION = Gauge(
    "eurogrid_flow_last_duration_seconds",
    "Durasi run terakhir sebuah flow, dalam detik.",
    ["flow"],
)
FLOW_LAST_STATE = Gauge(
    "eurogrid_flow_last_run_state",
    "Status run terakhir sebuah flow (1 pada status yang berlaku).",
    ["flow", "state"],
)
SCRAPE_ERRORS = Counter(
    "eurogrid_exporter_scrape_errors_total",
    "Jumlah siklus pengambilan metrik yang gagal.",
    ["source"],
)
LAST_SCRAPE = Gauge(
    "eurogrid_exporter_last_scrape_timestamp_seconds",
    "Waktu siklus pengambilan metrik terakhir berhasil, dalam detik epoch.",
)
UP = Gauge("eurogrid_exporter_up", "1 bila exporter metrik hidup.")


def prefect_api_url() -> str:
    return os.getenv("PREFECT_API_URL", "http://localhost:4200/api").rstrip("/")


def fetch_flow_runs(
    api_url: str,
    *,
    limit: int = 200,
    timeout: float = 10.0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Ambil daftar flow dan flow run terbaru dari API Prefect."""
    import httpx

    with httpx.Client(timeout=timeout) as client:
        flows = client.post(
            f"{api_url}/flows/filter",
            json={"limit": 100, "sort": "NAME_ASC"},
        )
        flows.raise_for_status()
        runs = client.post(
            f"{api_url}/flow_runs/filter",
            json={"limit": limit, "sort": "START_TIME_DESC"},
        )
        runs.raise_for_status()
    return list(flows.json()), list(runs.json())


def _snapshot(table) -> Any:
    snapshots = getattr(table.metadata, "snapshots", None)
    if not snapshots:
        return None
    return snapshots[-1]


def _count_rows(table, snapshot) -> int | None:
    """Jumlah baris dari ringkasan snapshot, atau hitung langsung bila tidak ada."""
    jumlah = rows_from_summary(getattr(snapshot, "summary", None))
    if jumlah is not None:
        return jumlah
    try:
        return int(table.scan().to_arrow().num_rows)
    except Exception as exc:  # noqa: BLE001 - hitungan langsung hanya cadangan
        LOG.warning("Gagal menghitung baris %s: %s", table.name(), exc)
        return None


def collect_tables(catalog, *, now: float | None = None) -> list[dict[str, Any]]:
    """Kumpulkan statistik tiap tabel di namespace Bronze dan Silver."""
    waktu = now if now is not None else time.time()
    hasil: list[dict[str, Any]] = []
    for namespace in ("bronze", "silver"):
        try:
            identifiers: Iterable[Any] = catalog.list_tables(namespace)
        except Exception as exc:  # noqa: BLE001 - namespace belum ada
            LOG.warning("Namespace %s belum bisa dibaca: %s", namespace, exc)
            continue

        for identifier in identifiers:
            nama = ".".join(identifier) if isinstance(identifier, tuple) else str(identifier)
            try:
                table = catalog.load_table(nama)
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Tabel %s tidak bisa dimuat: %s", nama, exc)
                continue

            snapshot = _snapshot(table)
            hasil.append(
                {
                    "table": nama,
                    "layer": namespace,
                    "rows": _count_rows(table, snapshot),
                    "freshness": freshness_seconds(
                        getattr(snapshot, "timestamp_ms", None), waktu
                    ),
                    "object": table,
                }
            )
    return hasil


def collect_quality(catalog, namespace: str = "silver") -> list[dict[str, Any]]:
    """Baca laporan mutu terakhir tiap dataset Silver."""
    hasil: list[dict[str, Any]] = []
    try:
        identifiers = list(catalog.list_tables(namespace))
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Laporan mutu tidak terbaca: %s", exc)
        return hasil

    for identifier in identifiers:
        nama = ".".join(identifier) if isinstance(identifier, tuple) else str(identifier)
        if not nama.endswith("_quality_report"):
            continue
        try:
            rows = catalog.load_table(nama).scan().to_pandas().to_dict("records")
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Laporan %s tidak terbaca: %s", nama, exc)
            continue
        terakhir = latest_report(rows)
        if not terakhir:
            continue
        hasil.append(
            {
                "dataset": dataset_of(nama)[0],
                "rows_failed": int(terakhir.get("rows_failed") or 0),
                "blocked": 1 if terakhir.get("status") == "blocked" else 0,
            }
        )
    return hasil


def publish_tables(stats: Iterable[dict[str, Any]]) -> None:
    TABLE_ROWS.clear()
    TABLE_FRESHNESS.clear()
    QUARANTINE_ROWS.clear()
    for item in stats:
        table = item["table"]
        layer = item["layer"]
        if item["rows"] is not None:
            TABLE_ROWS.labels(table=table, layer=layer).set(item["rows"])
        if item["freshness"] is not None:
            TABLE_FRESHNESS.labels(table=table, layer=layer).set(item["freshness"])
        dataset, artefak = dataset_of(table)
        if artefak == "quarantine" and item["rows"] is not None:
            QUARANTINE_ROWS.labels(dataset=dataset).set(item["rows"])


def publish_quality(items: Iterable[dict[str, Any]]) -> None:
    SILVER_FAILED.clear()
    SILVER_BLOCKED.clear()
    for item in items:
        SILVER_FAILED.labels(dataset=item["dataset"]).set(item["rows_failed"])
        SILVER_BLOCKED.labels(dataset=item["dataset"]).set(item["blocked"])


def publish_flows(stats) -> None:
    FLOW_RUNS.clear()
    FLOW_LAST_SUCCESS.clear()
    FLOW_LAST_DURATION.clear()
    FLOW_LAST_STATE.clear()
    for (flow, state), jumlah in stats.runs.items():
        FLOW_RUNS.labels(flow=flow, state=state).set(jumlah)
    for flow, waktu in stats.last_success.items():
        FLOW_LAST_SUCCESS.labels(flow=flow).set(waktu)
    for flow, durasi in stats.last_duration.items():
        FLOW_LAST_DURATION.labels(flow=flow).set(durasi)
    for flow, state in stats.last_state.items():
        FLOW_LAST_STATE.labels(flow=flow, state=state).set(1)


def refresh(state: dict[str, Any]) -> None:
    """Satu siklus pengambilan metrik."""
    from eurogrid.bronze.warehouse import build_catalog

    if state.get("catalog") is None:
        try:
            state["catalog"] = build_catalog()
        except Exception as exc:  # noqa: BLE001
            SCRAPE_ERRORS.labels(source="catalog").inc()
            LOG.warning("Katalog belum siap: %s", exc)
            return

    try:
        tables = collect_tables(state["catalog"])
        publish_tables(tables)
    except Exception as exc:  # noqa: BLE001
        SCRAPE_ERRORS.labels(source="catalog").inc()
        LOG.warning("Gagal membaca katalog: %s", exc)
        state["catalog"] = None
        return

    try:
        publish_quality(collect_quality(state["catalog"]))
    except Exception as exc:  # noqa: BLE001
        SCRAPE_ERRORS.labels(source="quality").inc()
        LOG.warning("Gagal membaca laporan mutu: %s", exc)

    try:
        flows, runs = fetch_flow_runs(prefect_api_url())
        publish_flows(flow_run_stats(flows, runs))
    except Exception as exc:  # noqa: BLE001 - Prefect boleh sedang restart
        SCRAPE_ERRORS.labels(source="prefect").inc()
        LOG.warning("Gagal membaca Prefect: %s", exc)

    LAST_SCRAPE.set(time.time())


def main() -> None:
    configure_json_logging("eurogrid-metrics")
    port = int(os.getenv("METRICS_PORT", "9101"))
    interval = float(os.getenv("METRICS_INTERVAL_SECONDS", "60"))
    start_http_server(port, addr="0.0.0.0")
    UP.set(1)
    LOG.info("Exporter metrik mendengarkan di :%s, interval %ss", port, interval)

    state: dict[str, Any] = {"catalog": None}
    while True:
        refresh(state)
        time.sleep(interval)


if __name__ == "__main__":
    main()
