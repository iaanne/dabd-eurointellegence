from __future__ import annotations

import pandas as pd
import pytest

from eurogrid.silver.quality import validate_frame
from eurogrid.silver.rules import ACTUAL_LOAD, PRICE, PUBLIC_POWER, WEATHER, get_rule

RUN = "test-run"


def _timestamps(n: int, menit: int = 15) -> list[pd.Timestamp]:
    awal = pd.Timestamp("2026-11-03T00:00:00Z")
    return [awal + pd.Timedelta(minutes=menit * i) for i in range(n)]


def _load_frame(
    values: list,
    *,
    units: list | None = None,
    resolutions: list | None = None,
    timestamps: list | None = None,
) -> pd.DataFrame:
    n = len(values)
    return pd.DataFrame(
        {
            "timestamp_utc": timestamps or _timestamps(n),
            "dataset": ["actual_load"] * n,
            "series_id": ["1"] * n,
            "series_name": ["actual_load"] * n,
            "value": values,
            "unit": units or ["MAW"] * n,
            "resolution": resolutions or ["PT15M"] * n,
            "bidding_zone": ["10Y1001A1001A82H"] * n,
            "country": ["de"] * n,
        }
    )


def test_baris_valid_lolos_dan_satuan_dinormalkan() -> None:
    hasil = validate_frame(_load_frame([50_000.0, 51_000.0]), ACTUAL_LOAD, run_id=RUN)

    assert len(hasil.passed) == 2
    assert hasil.quarantine.empty
    assert hasil.report["rows_in"] == 2
    assert hasil.report["rows_passed"] == 2
    assert hasil.report["rows_failed"] == 0
    assert hasil.report["status"] == "passed"
    assert set(hasil.passed["unit"]) == {"MW"}
    assert set(hasil.passed["unit_source"]) == {"MAW"}
    assert set(hasil.passed["quality_status"]) == {"valid"}
    assert set(hasil.passed["quality_run_id"]) == {RUN}


def test_nilai_kosong_masuk_karantina() -> None:
    hasil = validate_frame(_load_frame([50_000.0, None]), ACTUAL_LOAD, run_id=RUN)

    assert len(hasil.passed) == 1
    assert hasil.quarantine["reasons"].iloc[0] == "kolom_wajib_kosong:value"
    assert hasil.report["violations"] == "kolom_wajib_kosong:value=1"


def test_timestamp_tidak_valid_masuk_karantina() -> None:
    timestamps = _timestamps(2)
    timestamps[1] = "bukan-tanggal"
    hasil = validate_frame(_load_frame([50_000.0, 51_000.0], timestamps=timestamps), ACTUAL_LOAD, run_id=RUN)

    assert len(hasil.quarantine) == 1
    assert "timestamp_tidak_valid" in hasil.quarantine["reasons"].iloc[0]


def test_nilai_bukan_angka_masuk_karantina() -> None:
    hasil = validate_frame(_load_frame([50_000.0, "abc"]), ACTUAL_LOAD, run_id=RUN)

    assert len(hasil.quarantine) == 1
    assert "nilai_bukan_angka" in hasil.quarantine["reasons"].iloc[0]


def test_satuan_dan_resolusi_harus_sesuai() -> None:
    frame = _load_frame(
        [50_000.0, 50_000.0],
        units=["MAW", "kW"],
        resolutions=["PT15M", "PT1H"],
    )
    hasil = validate_frame(frame, ACTUAL_LOAD, run_id=RUN)

    assert len(hasil.quarantine) == 1
    alasan = hasil.quarantine["reasons"].iloc[0]
    assert "satuan_tidak_sesuai" in alasan
    assert "resolusi_tidak_sesuai" in alasan
    assert hasil.quarantine["violation_count"].iloc[0] == 2


def test_satu_baris_gagal_beberapa_aturan() -> None:
    frame = _load_frame([50_000.0, -10.0], units=["MAW", "kW"])
    hasil = validate_frame(frame, ACTUAL_LOAD, run_id=RUN)

    alasan = set(hasil.quarantine["reasons"].iloc[0].split("|"))
    assert {"nilai_terlalu_kecil", "satuan_tidak_sesuai"} <= alasan
    assert hasil.report["violations"].count("nilai_terlalu_kecil=1") == 1


def test_batas_atas_beban_dan_pita_peringatan() -> None:
    hasil = validate_frame(
        _load_frame([50_000.0, 120_000.0, 82_000.0]),
        ACTUAL_LOAD,
        run_id=RUN,
    )

    assert len(hasil.quarantine) == 1
    assert "nilai_terlalu_besar" in hasil.quarantine["reasons"].iloc[0]
    # 82.000 MW masih di atas batas peringatan 80.600, tetapi tetap lolos.
    assert len(hasil.passed) == 2
    assert hasil.report["warnings"] == "nilai_mencurigakan_besar=1"


def test_duplikat_kunci_dibuang_dan_dihitung() -> None:
    waktu = _timestamps(2)
    waktu[1] = waktu[0]  # kunci bisnis sama
    frame = _load_frame([50_000.0, 55_000.0], timestamps=waktu)

    hasil = validate_frame(frame, ACTUAL_LOAD, run_id=RUN)

    assert hasil.report["rows_in"] == 2
    assert hasil.report["duplicates_dropped"] == 1
    assert len(hasil.passed) == 1
    assert hasil.passed["value"].iloc[0] == 55_000.0


def test_celah_waktu_dihitung_dan_memblokir() -> None:
    timestamps = _timestamps(4)
    del timestamps[1]  # satu interval 15 menit hilang
    frame = _load_frame([50_000.0, 51_000.0, 52_000.0], timestamps=timestamps)

    hasil = validate_frame(frame, ACTUAL_LOAD, run_id=RUN)

    assert hasil.report["gap_count"] == 1
    assert hasil.report["missing_intervals"] == 1
    assert hasil.report["status"] == "blocked"
    assert "celah_waktu" in hasil.report["block_reasons"]


def test_ambang_gagal_memblokir_status() -> None:
    hasil = validate_frame(_load_frame([50_000.0, None, -5.0]), ACTUAL_LOAD, run_id=RUN)

    assert hasil.report["rows_failed"] == 2
    assert hasil.report["status"] == "blocked"
    assert "ambang_gagal" in hasil.report["block_reasons"]


def test_harga_negatif_tetap_valid() -> None:
    frame = pd.DataFrame(
        {
            "timestamp_utc": _timestamps(2),
            "dataset": ["price"] * 2,
            "series_id": ["day_ahead_price"] * 2,
            "series_name": ["day_ahead_price"] * 2,
            "value": [-19.0, 740.0],
            "unit": ["EUR / MWh"] * 2,
            "resolution": ["PT15M"] * 2,
            "bidding_zone": ["DE-LU"] * 2,
        }
    )
    hasil = validate_frame(frame, PRICE, run_id=RUN)

    assert len(hasil.passed) == 2
    assert hasil.report["rows_failed"] == 0


def test_public_power_negatif_hanya_untuk_seri_tertentu() -> None:
    frame = pd.DataFrame(
        {
            "timestamp_utc": _timestamps(3),
            "dataset": ["public_power"] * 3,
            "series_id": ["solar", "cross_border_electricity_trading", "load"],
            "series_name": ["solar", "trading", "load"],
            "value": [-10.0, -19_623.0, 71_714.4],
            "unit": ["MW"] * 3,
            "resolution": ["PT15M"] * 3,
            "country": ["de"] * 3,
        }
    )
    hasil = validate_frame(frame, PUBLIC_POWER, run_id=RUN)

    assert len(hasil.quarantine) == 1
    assert hasil.quarantine["series_id"].iloc[0] == "solar"
    assert "nilai_negatif" in hasil.quarantine["reasons"].iloc[0]
    assert len(hasil.passed) == 2


def test_cuaca_memakai_aturan_per_variabel() -> None:
    frame = pd.DataFrame(
        {
            "timestamp_utc": [
                pd.Timestamp("2026-11-03T00:00:00Z"),
                pd.Timestamp("2026-11-03T00:00:00Z"),
            ],
            "location": ["berlin", "berlin"],
            "variable": ["temperature_2m", "wind_speed_10m"],
            "value": [-100.0, 20.0],
            "unit": ["°C", "km/h"],
        }
    )
    hasil = validate_frame(frame, WEATHER, run_id=RUN)

    assert len(hasil.quarantine) == 1
    assert hasil.quarantine["variable"].iloc[0] == "temperature_2m"
    assert "nilai_terlalu_kecil" in hasil.quarantine["reasons"].iloc[0]
    assert len(hasil.passed) == 1


def test_satuan_salah_pada_variabel_cuaca() -> None:
    frame = pd.DataFrame(
        {
            "timestamp_utc": [pd.Timestamp("2026-11-03T00:00:00Z")],
            "location": ["berlin"],
            "variable": ["temperature_2m"],
            "value": [12.0],
            "unit": ["km/h"],
        }
    )
    hasil = validate_frame(frame, WEATHER, run_id=RUN)

    assert "satuan_tidak_sesuai" in hasil.quarantine["reasons"].iloc[0]


def test_kolom_hilang_mengarantina_semua_baris() -> None:
    frame = _load_frame([50_000.0]).drop(columns=["unit"])
    hasil = validate_frame(frame, ACTUAL_LOAD, run_id=RUN)

    assert hasil.passed.empty
    assert "kolom_hilang:unit" in hasil.quarantine["reasons"].iloc[0]


def test_nama_dataset_tidak_dikenal() -> None:
    with pytest.raises(ValueError, match="tidak dikenal"):
        get_rule("tidak_ada")
