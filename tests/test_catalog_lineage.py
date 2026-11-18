from __future__ import annotations

from pathlib import Path
from uuid import UUID

import httpx
import pytest
import respx
import yaml

from eurogrid.catalog import check_against_tables, load_catalog, owners
from eurogrid.lineage import build_run_event, emit_job, emit_run_event, runtime_run_id

ROOT = Path(__file__).resolve().parent.parent
KATALOG = ROOT / "docs" / "catalog" / "datasets.yaml"


def _tulis(tmp_path: Path, isi: dict) -> Path:
    path = tmp_path / "datasets.yaml"
    path.write_text(yaml.safe_dump(isi), encoding="utf-8")
    return path


def _entri(**ubah) -> dict:
    dasar = {
        "name": "bronze.contoh",
        "layer": "bronze",
        "source": "sumber",
        "owner": "Nad",
        "description": "contoh",
        "grain": "15 menit",
        "business_key": ["timestamp_utc"],
        "retention_days": None,
        "refresh": "0 5 * * * Europe/Berlin",
    }
    dasar.update(ubah)
    return dasar


def test_katalog_repo_lengkap() -> None:
    katalog = load_catalog(KATALOG)

    assert len(katalog) == 8
    assert {entri.layer for entri in katalog.values()} == {"bronze", "silver"}
    for nama, entri in katalog.items():
        assert entri.owner, nama
        assert entri.business_key, nama
        assert entri.refresh, nama

    bronze = {nama for nama, entri in katalog.items() if entri.layer == "bronze"}
    silver = {nama for nama, entri in katalog.items() if entri.layer == "silver"}
    assert len(bronze) == 4
    assert len(silver) == 4
    assert {nama.replace("silver.", "bronze.") for nama in silver} == bronze


def test_retensi_sesuai_arsitektur() -> None:
    katalog = load_catalog(KATALOG)

    for nama, entri in katalog.items():
        if entri.layer == "bronze":
            assert entri.retention_days is None, nama
        else:
            assert entri.retention_days == 365, nama


def test_pemilik_per_dataset() -> None:
    daftar = owners(load_catalog(KATALOG))

    assert daftar["Nad"] == sorted(
        [
            "silver.energy_charts_price",
            "silver.energy_charts_public_power",
            "silver.entsoe_actual_load",
            "silver.open_meteo_weather_hourly",
        ]
    )


def test_katalog_ganda_ditolak(tmp_path: Path) -> None:
    path = _tulis(tmp_path, {"version": 1, "datasets": [_entri(), _entri()]})

    with pytest.raises(ValueError, match="ganda"):
        load_catalog(path)


def test_pemilik_kosong_ditolak(tmp_path: Path) -> None:
    path = _tulis(tmp_path, {"version": 1, "datasets": [_entri(owner="  ")]})

    with pytest.raises(ValueError, match="Pemilik kosong"):
        load_catalog(path)


def test_layer_tidak_dikenal_ditolak(tmp_path: Path) -> None:
    path = _tulis(tmp_path, {"version": 1, "datasets": [_entri(layer="platinum")]})

    with pytest.raises(ValueError, match="Layer tidak dikenal"):
        load_catalog(path)


def test_kolom_wajib_kurang_ditolak(tmp_path: Path) -> None:
    entri = _entri()
    del entri["refresh"]
    path = _tulis(tmp_path, {"version": 1, "datasets": [entri]})

    with pytest.raises(ValueError, match="kurang kolom"):
        load_catalog(path)


def test_bandingkan_dengan_tabel_iceberg() -> None:
    katalog = load_catalog(KATALOG)
    tersedia = [
        "bronze.entsoe_actual_load",
        "bronze.energy_charts_price",
        "silver.entsoe_actual_load",
        "silver.entsoe_actual_load_quarantine",
        "silver.entsoe_actual_load_quality_report",
        "tabel.tanpa_metadata",
    ]

    hasil = check_against_tables(katalog, tersedia)

    assert "bronze.energy_charts_public_power" in hasil["hilang"]
    assert "silver.energy_charts_price" in hasil["hilang"]
    # Tabel bantu mengikuti metadata induknya, bukan dianggap tanpa metadata.
    assert hasil["tanpa_metadata"] == ["tabel.tanpa_metadata"]


def test_event_lineage_berbentuk_sah() -> None:
    event = build_run_event(
        event_type="COMPLETE",
        job_name="silver.entsoe_actual_load",
        run_id="8feec042f5f349a9bbb26fde2effdb96",
        inputs=["bronze.entsoe_actual_load"],
        outputs=["silver.entsoe_actual_load"],
        namespace="eurogrid",
    )

    assert event["eventType"] == "COMPLETE"
    assert event["job"]["namespace"] == "eurogrid"
    assert event["job"]["name"] == "silver.entsoe_actual_load"
    assert event["inputs"] == [
        {"namespace": "eurogrid", "name": "bronze.entsoe_actual_load"}
    ]
    assert event["outputs"] == [
        {"namespace": "eurogrid", "name": "silver.entsoe_actual_load"}
    ]
    UUID(event["run"]["runId"])  # harus UUID yang sah
    assert event["schemaURL"].startswith("https://openlineage.io/spec/")


def test_run_id_deterministik() -> None:
    pertama = runtime_run_id("8feec042f5f349a9bbb26fde2effdb96")
    kedua = runtime_run_id("8feec042f5f349a9bbb26fde2effdb96")

    assert pertama == kedua
    UUID(pertama)


def test_lineage_mati_tanpa_url(monkeypatch) -> None:
    monkeypatch.delenv("OPENLINEAGE_URL", raising=False)

    assert emit_job(job_name="uji", run_id="uji", outputs=["silver.uji"]) is False


def test_kirim_event_ke_marquez(monkeypatch) -> None:
    monkeypatch.delenv("OPENLINEAGE_URL", raising=False)
    event = build_run_event(
        event_type="START",
        job_name="uji",
        run_id="uji",
        outputs=["silver.uji"],
        namespace="eurogrid",
    )

    with respx.mock(base_url="http://lineage.test") as mock:
        route = mock.post("/api/v1/lineage").mock(return_value=httpx.Response(201))
        assert emit_run_event(event, url="http://lineage.test") is True
        assert route.called
        assert route.calls[0].request.headers["content-type"] == "application/json"


def test_kegagalan_marquez_tidak_melempar(monkeypatch) -> None:
    monkeypatch.delenv("OPENLINEAGE_URL", raising=False)
    event = build_run_event(event_type="START", job_name="uji", run_id="uji")

    with respx.mock(base_url="http://lineage.test") as mock:
        mock.post("/api/v1/lineage").mock(return_value=httpx.Response(500))
        assert emit_run_event(event, url="http://lineage.test") is False
