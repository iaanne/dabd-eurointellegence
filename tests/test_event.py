from __future__ import annotations

import json
from eurogrid.streaming.common.event import (
    build_event,
    generate_event_id,
    serialize_event,
)


def test_generate_event_id_deterministik() -> None:
    id1 = generate_event_id("entsoe", "actual_load", "DE_LU", "2026-11-14T10:00:00Z")
    id2 = generate_event_id("entsoe", "actual_load", "DE_LU", "2026-11-14T10:00:00Z")
    id_lain = generate_event_id("entsoe", "actual_load", "NL", "2026-11-14T10:00:00Z")

    assert id1 == id2
    assert id1 != id_lain


def test_build_event_struktur_lengkap() -> None:
    event = build_event(
        source="entsoe",
        dataset="actual_load",
        country_code="DE_LU",
        event_time="2026-11-14T10:00:00Z",
        value=52340.5,
        unit="MW",
        metadata={"bidding_zone": "10Y1001A1001A82H"},
    )

    assert event["source"] == "entsoe"
    assert event["dataset"] == "actual_load"
    assert event["country_code"] == "DE_LU"
    assert event["value"] == 52340.5
    assert event["unit"] == "MW"
    assert event["metadata"]["bidding_zone"] == "10Y1001A1001A82H"
    assert len(event["event_id"]) == 64  # SHA256 hex string
    assert "ingestion_time" in event


def test_serialize_event_menghasilkan_json_bytes() -> None:
    event = build_event(
        source="entsoe",
        dataset="actual_load",
        country_code="DE_LU",
        event_time="2026-11-14T10:00:00Z",
        value=50000.0,
        unit="MW",
    )
    payload = serialize_event(event)
    assert isinstance(payload, bytes)
    parsed = json.loads(payload.decode("utf-8"))
    assert parsed["event_id"] == event["event_id"]

