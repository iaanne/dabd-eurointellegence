from __future__ import annotations

import pandas as pd

from eurogrid.bronze.warehouse import _normalize
from eurogrid.revisions import beyond_available_until, collapse_revisions
from eurogrid.silver.quality import validate_frame
from eurogrid.silver.rules import PRICE

RUN = "test-revisi"
TS = pd.Timestamp("2026-11-03T00:00:00Z")


def _price_frame(
    values: list,
    *,
    generated_at: list | None = None,
    available_until: list | None = None,
    timestamps: list | None = None,
) -> pd.DataFrame:
    n = len(values)
    data = {
        "timestamp_utc": timestamps or [TS] * n,
        "series_id": ["price"] * n,
        "value": values,
        "unit": ["EUR / MWh"] * n,
        "bidding_zone": ["DE-LU"] * n,
    }
    if generated_at is not None:
        data["generated_at"] = generated_at
    if available_until is not None:
        data["available_until"] = available_until
    return pd.DataFrame(data)


def test_versi_dengan_generated_at_terbaru_menang() -> None:
    frame = _price_frame(
        [10.0, 20.0, 30.0],
        generated_at=[
            "2026-11-01T00:00:00+00:00",
            "2026-11-03T00:00:00+00:00",
            "2026-11-02T00:00:00+00:00",
        ],
    )

    dropped, hasil = collapse_revisions(frame, PRICE.key)

    assert dropped == 2
    assert len(hasil) == 1
    assert hasil["value"].iloc[0] == 20.0


def test_generated_at_sama_memakai_available_until_terakhir() -> None:
    frame = _price_frame(
        [10.0, 20.0],
        generated_at=["2026-11-03T00:00:00+00:00"] * 2,
        available_until=[
            "2026-11-02T00:00:00+00:00",
            "2026-11-04T00:00:00+00:00",
        ],
    )

    _, hasil = collapse_revisions(frame, PRICE.key)

    assert hasil["value"].iloc[0] == 20.0


def test_tanpa_kolom_revisi_tetap_memakai_baris_terakhir() -> None:
    frame = _price_frame([10.0, 20.0])

    dropped, hasil = collapse_revisions(frame, PRICE.key)

    assert dropped == 1
    assert hasil["value"].iloc[0] == 20.0


def test_kunci_kosong_tidak_dibuang_supaya_masuk_karantina() -> None:
    frame = _price_frame([10.0, 20.0])
    frame.loc[1, "series_id"] = None

    dropped, hasil = collapse_revisions(frame, PRICE.key)

    assert dropped == 0
    assert len(hasil) == 2


def test_baris_setelah_available_until_masuk_karantina() -> None:
    frame = _price_frame(
        [50.0],
        generated_at=["2026-11-18T00:00:00+00:00"],
        available_until=["2026-11-17T00:00:00+00:00"],
        timestamps=[pd.Timestamp("2026-11-18T00:00:00Z")],
    )

    hasil = validate_frame(frame, PRICE, run_id=RUN)

    assert hasil.passed.empty
    assert hasil.quarantine["reasons"].iloc[0] == "melewati_available_until"
    assert hasil.report["revision_not_available"] == 1
    assert hasil.report["rows_failed"] == 1


def test_baris_dalam_available_until_tetap_lolos() -> None:
    frame = _price_frame(
        [50.0],
        generated_at=["2026-11-18T00:00:00+00:00"],
        available_until=["2026-11-18T00:00:00+00:00"],
    )

    hasil = validate_frame(frame, PRICE, run_id=RUN)

    assert len(hasil.passed) == 1
    assert hasil.report["revision_not_available"] == 0


def test_beyond_available_until_aman_tanpa_kolom() -> None:
    frame = _price_frame([10.0])

    assert not beyond_available_until(frame).any()


def test_normalize_bronze_memilih_generated_at_terbaru() -> None:
    frame = pd.concat(
        [
            _price_frame(
                [10.0],
                generated_at=["2026-11-01T00:00:00+00:00"],
                available_until=["2026-11-02T00:00:00+00:00"],
            ),
            _price_frame(
                [20.0],
                generated_at=["2026-11-03T00:00:00+00:00"],
                available_until=["2026-11-02T00:00:00+00:00"],
            ),
        ],
        ignore_index=True,
    )

    hasil = _normalize(frame)

    assert len(hasil) == 1
    assert hasil["value"].iloc[0] == 20.0
