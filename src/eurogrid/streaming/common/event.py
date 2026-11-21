
import hashlib
import json

from datetime import datetime, timezone
from typing import Any


def generate_event_id(
    source: str,
    dataset: str,
    country_code: str,
    event_time: str,
) -> str:
    """
    Generate deterministic event ID.

    Event dengan source, dataset, negara, dan waktu
    pengamatan yang sama akan menghasilkan ID yang sama.
    """

    raw_key = (
        f"{source}|{dataset}|{country_code}|{event_time}"
    )

    return hashlib.sha256(
        raw_key.encode("utf-8")
    ).hexdigest()


def build_event(
    source: str,
    dataset: str,
    country_code: str,
    event_time: str,
    value: float,
    unit: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Bangun event standar untuk Redpanda.
    """

    ingestion_time = datetime.now(
        timezone.utc
    ).isoformat()

    event = {
        "event_id": generate_event_id(
            source=source,
            dataset=dataset,
            country_code=country_code,
            event_time=event_time,
        ),
        "source": source,
        "dataset": dataset,
        "country_code": country_code,
        "event_time": event_time,
        "ingestion_time": ingestion_time,
        "value": value,
        "unit": unit,
        "metadata": metadata or {},
    }

    return event


def serialize_event(event: dict[str, Any]) -> bytes:
    """
    Serialisasi event menjadi bytes JSON untuk Redpanda.
    """

    return json.dumps(
        event,
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
