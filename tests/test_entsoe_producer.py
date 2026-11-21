from __future__ import annotations

from unittest.mock import MagicMock
from eurogrid.streaming.producers.entsoe_producer import publish_records, TOPIC


def test_publish_records_mengirim_ke_redpanda() -> None:
    mock_producer = MagicMock()

    records = [
        {
            "event_time": "2026-11-14T10:00:00Z",
            "value": 52340.5,
            "unit": "MW",
            "metadata": {"bidding_zone": "10Y1001A1001A82H"},
        },
        {
            "event_time": "2026-11-14T10:15:00Z",
            "value": 52100.0,
            "unit": "MW",
            "metadata": {"bidding_zone": "10Y1001A1001A82H"},
        },
    ]

    count = publish_records(
        producer=mock_producer,
        country_code="DE_LU",
        records=records,
    )

    assert count == 2
    assert mock_producer.send.call_count == 2

    # Verifikasi parameter send pertama
    first_call_args, first_call_kwargs = mock_producer.send.call_args_list[0]
    assert first_call_args[0] == TOPIC
    assert first_call_kwargs["key"] == "DE_LU"
    event_data = first_call_kwargs["value"]
    assert event_data["source"] == "entsoe"
    assert event_data["dataset"] == "actual_load"
    assert event_data["value"] == 52340.5

