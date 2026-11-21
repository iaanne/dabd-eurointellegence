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
    assert config["rule_files"] == ["/etc/prometheus/alerts.yml"]
    assert config["alerting"]["alertmanagers"][0]["static_configs"][0]["targets"] == [
        "alertmanager:9093"
    ]


def _alert_rules() -> dict[str, dict]:
    aturan = yaml.safe_load(
        (ROOT / "infra" / "prometheus" / "alerts.yml").read_text(encoding="utf-8")
    )
    return {
        rule["alert"]: rule
        for grup in aturan["groups"]
        for rule in grup["rules"]
    }


def test_tiga_alert_wajib_punya_ambang_jendela_pemilik_dan_kanal():
    rules = _alert_rules()

    for nama in ("EuroGridFlowRunFailed", "EuroGridDataStale", "EuroGridNodeCpuHigh"):
        rule = rules[nama]
        assert rule["expr"].strip(), nama
        assert rule["for"], nama
        assert rule["labels"]["severity"] in ("warning", "critical")
        assert rule["labels"]["owner"], nama
        assert rule["labels"]["channel"], nama
        assert rule["annotations"]["summary"], nama


def test_alert_job_gagal_melihat_status_run_terakhir():
    rules = _alert_rules()

    assert "FAILED" in rules["EuroGridFlowRunFailed"]["expr"]
    assert rules["EuroGridFlowRunFailed"]["labels"]["severity"] == "critical"
    assert "eurogrid_table_freshness_seconds" in rules["EuroGridDataStale"]["expr"]
    assert "node_cpu_seconds_total" in rules["EuroGridNodeCpuHigh"]["expr"]


def test_alertmanager_mengirim_ke_penerima_lokal():
    config = yaml.safe_load(
        (ROOT / "infra" / "alertmanager" / "alertmanager.yml").read_text(encoding="utf-8")
    )
    penerima = {item["name"]: item for item in config["receivers"]}

    assert config["route"]["receiver"] == "webhook-lokal"
    webhook = penerima["webhook-lokal"]["webhook_configs"][0]
    assert webhook["url"] == "http://alert-sink:8080/alerts"
    assert webhook["send_resolved"] is True


def test_penerima_alert_menyimpan_satu_baris_per_alert(tmp_path, monkeypatch):
    import importlib.util

    jalur = ROOT / "infra" / "alerts" / "sink.py"
    spesifikasi = importlib.util.spec_from_file_location("eurogrid_alert_sink", jalur)
    assert spesifikasi and spesifikasi.loader
    sink = importlib.util.module_from_spec(spesifikasi)
    spesifikasi.loader.exec_module(sink)

    berkas = tmp_path / "alerts.jsonl"
    monkeypatch.setattr(sink, "OUTPUT", berkas)
    payload = {
        "status": "firing",
        "alerts": [
            {
                "status": "firing",
                "labels": {
                    "alertname": "EuroGridFlowRunFailed",
                    "severity": "critical",
                    "owner": "nad",
                    "channel": "webhook-lokal",
                },
                "annotations": {"summary": "flow gagal"},
                "startsAt": "2026-11-19T01:00:00Z",
                "endsAt": "0001-01-01T00:00:00Z",
                "fingerprint": "abc",
            }
        ],
    }

    tersimpan = sink.simpan(payload)

    assert len(tersimpan) == 1
    baris = berkas.read_text(encoding="utf-8").splitlines()
    assert len(baris) == 1
    assert json.loads(baris[0])["alertname"] == "EuroGridFlowRunFailed"
    assert sink.terakhir()[0]["severity"] == "critical"


def test_compose_menyertakan_pemantauan_dan_alert():
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    services = compose["services"]

    for nama in (
        "metrics-exporter",
        "prometheus",
        "node-exporter",
        "grafana",
        "alertmanager",
        "alert-sink",
    ):
        assert nama in services, nama
    assert "9093:9093" in services["alertmanager"]["ports"][-1]
    assert "8080:8080" in services["alert-sink"]["ports"][-1]
