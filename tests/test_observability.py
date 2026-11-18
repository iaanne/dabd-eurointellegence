"""Tes log JSON, ringkasan metrik, dan konfigurasi pemantauan."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import yaml

from eurogrid.observability.logging import WAJIB, JsonFormatter
from eurogrid.observability.metrics import (
    dataset_of,
    flow_run_stats,
    freshness_seconds,
    latest_report,
    rows_from_summary,
)

ROOT = Path(__file__).resolve().parents[1]
ISO = "2026-11-18T05:00:00+00:00"


def _record(message: str = "halo", **extra) -> logging.LogRecord:
    record = logging.LogRecord(
        name="prefect.flow_runs",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )
    for kunci, nilai in extra.items():
        setattr(record, kunci, nilai)
    return record


def test_log_json_memuat_kolom_wajib():
    formatter = JsonFormatter("eurogrid-bronze-daily", run_id="abc-123")
    payload = json.loads(formatter.format(_record()))

    for kolom in WAJIB:
        assert kolom in payload, kolom
    assert payload["severity"] == "INFO"
    assert payload["service"] == "eurogrid-bronze-daily"
    assert payload["run_id"] == "abc-123"
    assert payload["message"] == "halo"
    assert payload["timestamp"].endswith("Z")


def test_log_json_satu_baris_dan_run_id_dari_record():
    formatter = JsonFormatter("eurogrid-silver-daily")
    teks = formatter.format(_record("baris\nkedua", run_id="run-9"))

    assert "\n" not in teks
    payload = json.loads(teks)
    assert payload["run_id"] == "run-9"
    assert payload["message"] == "baris\nkedua"


def test_flow_run_stats_menghitung_status_dan_durasi():
    flows = [{"id": "f1", "name": "bronze-daily"}, {"id": "f2", "name": "silver-daily"}]
    runs = [
        {
            "flow_id": "f1",
            "state_type": "COMPLETED",
            "start_time": "2026-11-18T05:00:00+00:00",
            "end_time": "2026-11-18T05:07:00+00:00",
        },
        {
            "flow_id": "f1",
            "state_type": "FAILED",
            "start_time": "2026-11-18T06:00:00+00:00",
            "end_time": "2026-11-18T06:01:30+00:00",
        },
        {
            "flow_id": "f2",
            "state_type": "COMPLETED",
            "start_time": "2026-11-18T05:30:00+00:00",
            "end_time": "2026-11-18T05:30:20+00:00",
        },
        {"flow_id": "tidak-dikenal", "state_type": "COMPLETED", "start_time": ISO},
    ]

    stats = flow_run_stats(flows, runs)

    assert stats.runs[("bronze-daily", "COMPLETED")] == 1
    assert stats.runs[("bronze-daily", "FAILED")] == 1
    assert stats.runs[("silver-daily", "COMPLETED")] == 1
    assert all(flow != "tidak-dikenal" for flow, _ in stats.runs)
    assert stats.last_state["bronze-daily"] == "FAILED"
    assert stats.last_duration["bronze-daily"] == 90.0
    sukses = datetime(2026, 11, 18, 5, 7, tzinfo=timezone.utc).timestamp()
    assert stats.last_success["bronze-daily"] == sukses


def test_flow_run_stats_tanpa_run():
    stats = flow_run_stats([{"id": "f1", "name": "bronze-daily"}], [])

    assert stats.runs == {}
    assert stats.last_success == {}
    assert stats.last_duration == {}


def test_rows_dari_ringkasan_snapshot():
    assert rows_from_summary({"total-records": "8640"}) == 8640
    assert rows_from_summary({"total_records": 12}) == 12
    assert rows_from_summary({"added-records": 5}) is None
    assert rows_from_summary(None) is None
    assert rows_from_summary({"total-records": "banyak"}) is None


def test_freshness_dihitung_dari_milidetik():
    sekarang = 1_700_000_000.0
    assert freshness_seconds((sekarang - 600) * 1000, sekarang) == 600.0
    assert freshness_seconds(sekarang * 1000 + 5000, sekarang) == 0.0
    assert freshness_seconds(None, sekarang) is None
    assert freshness_seconds("rusak", sekarang) is None


def test_dataset_of_memisahkan_tabel_bantu():
    assert dataset_of("silver.entsoe_actual_load") == (
        "silver.entsoe_actual_load",
        None,
    )
    assert dataset_of("silver.entsoe_actual_load_quarantine") == (
        "silver.entsoe_actual_load",
        "quarantine",
    )
    assert dataset_of("silver.entsoe_actual_load_quality_report") == (
        "silver.entsoe_actual_load",
        "quality_report",
    )


def test_latest_report_memilih_yang_paling_baru():
    rows = [
        {"processed_at": "2026-11-17T05:00:00+00:00", "status": "passed", "rows_failed": 0},
        {"processed_at": "2026-11-18T05:00:00+00:00", "status": "blocked", "rows_failed": 87},
    ]

    assert latest_report(rows)["status"] == "blocked"
    assert latest_report(list(reversed(rows)))["rows_failed"] == 87
    assert latest_report([]) is None


def test_dashboard_grafana_valid_dan_punya_metrik_inti():
    dashboard = json.loads(
        (ROOT / "infra" / "grafana" / "dashboards" / "eurogrid.json").read_text(
            encoding="utf-8"
        )
    )

    assert dashboard["uid"] == "eurogrid-pipeline"
    assert dashboard["panels"], "dashboard tanpa panel"
    ekspresi = " ".join(
        target["expr"] for panel in dashboard["panels"] for target in panel["targets"]
    )
    for metrik in (
        "eurogrid_table_rows",
        "eurogrid_quarantine_rows",
        "eurogrid_flow_last_success_timestamp_seconds",
        "node_cpu_seconds_total",
    ):
        assert metrik in ekspresi, metrik


def test_prometheus_menjaring_exporter_node_dan_diri_sendiri():
    config = yaml.safe_load(
        (ROOT / "infra" / "prometheus" / "prometheus.yml").read_text(encoding="utf-8")
    )
    jobs = {job["job_name"]: job for job in config["scrape_configs"]}

    assert jobs["eurogrid"]["static_configs"][0]["targets"] == ["metrics-exporter:9101"]
    assert jobs["node"]["static_configs"][0]["targets"] == ["node-exporter:9100"]
    assert "prometheus" in jobs


def test_compose_menyambung_exporter_ke_prometheus():
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    services = compose["services"]

    for nama in ("metrics-exporter", "prometheus", "node-exporter", "grafana"):
        assert nama in services, nama
    assert "9101:9101" in services["metrics-exporter"]["ports"][-1]
    assert services["grafana"]["environment"]["GF_AUTH_ANONYMOUS_ENABLED"] == "true"
