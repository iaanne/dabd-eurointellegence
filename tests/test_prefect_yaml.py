from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent


def _deployments() -> list[dict]:
    konfigurasi = yaml.safe_load((ROOT / "prefect.yaml").read_text(encoding="utf-8"))
    return konfigurasi["deployments"]


def _cari(entrypoint: str) -> dict:
    for deployment in _deployments():
        if deployment["entrypoint"] == entrypoint:
            return deployment
    raise AssertionError(f"deployment {entrypoint} tidak ditemukan")


def test_deployment_bronze_daily() -> None:
    deployment = _cari("flows/bronze_daily.py:bronze_daily")

    assert deployment["name"] == "production"
    assert deployment["work_pool"]["name"] == "eurogrid-pool"
    assert deployment["parameters"]["days"] == 7

    jadwal = deployment["schedules"][0]
    assert jadwal["cron"] == "0 5 * * *"
    assert jadwal["timezone"] == "Europe/Berlin"
    assert jadwal["active"] is True


def test_deployment_silver_daily() -> None:
    deployment = _cari("flows/silver_daily.py:silver_daily")

    assert deployment["name"] == "production"
    assert deployment["work_pool"]["name"] == "eurogrid-pool"

    jadwal = deployment["schedules"][0]
    assert jadwal["cron"] == "30 5 * * *"
    assert jadwal["timezone"] == "Europe/Berlin"
    assert jadwal["active"] is True


def test_entrypoint_deployment_ada() -> None:
    for deployment in _deployments():
        berkas, fungsi = deployment["entrypoint"].split(":")

        path = ROOT / berkas
        assert path.is_file()
        assert f"def {fungsi}(" in path.read_text(encoding="utf-8")


def test_parameter_deployment_ada_di_fungsi_flow() -> None:
    """Parameter di prefect.yaml harus benar-benar diterima fungsi flow."""
    import importlib.util
    import inspect

    for deployment in _deployments():
        berkas, fungsi = deployment["entrypoint"].split(":")
        spec = importlib.util.spec_from_file_location(
            f"flow_{fungsi}", ROOT / berkas
        )
        modul = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(modul)
        flow = getattr(modul, fungsi)
        parameter = inspect.signature(flow.fn).parameters

        for nama in deployment.get("parameters") or {}:
            assert nama in parameter, f"{fungsi} tidak menerima parameter {nama}"
