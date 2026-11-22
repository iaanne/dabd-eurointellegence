from __future__ import annotations

from pathlib import Path

import pytest

from eurogrid.gold.queries import GOLD_QUERIES, render_query, validate_date_range

ROOT = Path(__file__).resolve().parent.parent


def test_sepuluh_query_gold_terdaftar_dan_ada_di_sql() -> None:
    assert len(GOLD_QUERIES) == 10
    sql_file = (ROOT / "sql" / "gold_queries.sql").read_text(encoding="utf-8")
    for name in GOLD_QUERIES:
        assert name in sql_file


def test_render_query_valid() -> None:
    sql = render_query(
        "daily_price_summary",
        start_date="2026-11-01",
        end_date="2026-11-07",
        bzn="DE-LU",
    )
    assert "DATE '2026-11-01'" in sql
    assert "DATE '2026-11-07'" in sql
    assert "bzn = 'DE-LU'" in sql


def test_render_query_tanggal_invalid_ditolak() -> None:
    with pytest.raises(ValueError, match="tidak valid"):
        validate_date_range("2026-13-45", "2026-11-07")

    with pytest.raises(ValueError, match="lebih besar"):
        validate_date_range("2026-11-10", "2026-11-01")


def test_render_query_parameter_berbahaya_ditolak() -> None:
    with pytest.raises(ValueError, match="tidak valid"):
        render_query(
            "hourly_generation_mix",
            start_date="2026-11-01",
            end_date="2026-11-07",
            country="de'; DROP TABLE x; --",
        )
