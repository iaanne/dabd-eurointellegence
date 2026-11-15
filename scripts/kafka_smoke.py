"""Smoke test untuk Kafka broker dan streaming topics.

Jalankan dengan:
    uv run --extra streaming python scripts/kafka_smoke.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

from eurogrid.streaming.common.event import build_event
from eurogrid.streaming.common.kafka import create_kafka_producer


def main() -> int:
    parser = argparse.ArgumentParser(description="EuroGrid Kafka Smoke Test")
    parser.add_argument(
        "--bootstrap-server",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default="entsoe.actual_load",
        help="Topic uji (default: entsoe.actual_load)",
    )
    args = parser.parse_args()

    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = args.bootstrap_server
    print(f"Connecting to Kafka at {args.bootstrap_server}...")

    try:
        producer = create_kafka_producer()
    except Exception as exc:
        print(f"FAILED to initialize Kafka Producer: {exc}", file=sys.stderr)
        return 1

    test_event = build_event(
        source="smoke_test",
        dataset="actual_load",
        country_code="DE_LU",
        event_time=datetime.now(timezone.utc).isoformat(),
        value=50000.0,
        unit="MAW",
        metadata={"smoke_test": True, "created_at": time.time()},
    )

    print(f"Publishing test event to topic '{args.topic}'...")
    try:
        future = producer.send(
            args.topic,
            key="DE_LU",
            value=test_event,
        )
        record_metadata = future.get(timeout=10)
        producer.flush()
        print(f"SUCCESS: Event published to {record_metadata.topic} [partition {record_metadata.partition}] at offset {record_metadata.offset}")
    except Exception as exc:
        print(f"FAILED to publish event: {exc}", file=sys.stderr)
        return 1
    finally:
        producer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

