"""Klien producer Redpanda untuk streaming EuroGrid.

Redpanda memakai protokol Kafka, jadi pustaka kliennya tetap `kafka-python-ng`.
"""

import json
import os

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None  # type: ignore


def create_redpanda_producer() -> "KafkaProducer":
    """Buat producer Redpanda dari konfigurasi lingkungan."""

    if KafkaProducer is None:
        raise ImportError(
            "Pustaka 'kafka-python-ng' belum terpasang. Pasang dengan "
            "'uv sync --extra streaming' atau jalankan di dalam container streaming."
        )

    bootstrap_servers = os.getenv("REDPANDA_BROKERS", "redpanda:9092")

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=5,
        compression_type="gzip",
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
            default=str,
        ).encode("utf-8"),
    )
