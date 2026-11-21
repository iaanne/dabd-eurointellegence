"""Smoke test untuk broker Redpanda dan topik streaming.

Jalankan dengan:
    uv run --extra streaming python scripts/redpanda_smoke.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

from eurogrid.streaming.common.event import build_event
from eurogrid.streaming.common.redpanda import create_redpanda_producer


def main() -> int:
    parser = argparse.ArgumentParser(description="EuroGrid Redpanda Smoke Test")
    parser.add_argument(
        "--bootstrap-server",
        default=os.getenv("REDPANDA_BROKERS", "localhost:9092"),
        help="Redpanda brokers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default="entsoe.actual_load",
        help="Topic uji (default: entsoe.actual_load)",
    )
    args = parser.parse_args()

    os.environ["REDPANDA_BROKERS"] = args.bootstrap_server
    print(f"Menghubungi Redpanda di {args.bootstrap_server}...")

    try:
        producer = create_redpanda_producer()
    except Exception as exc:
        print(f"Gagal membuat producer Redpanda: {exc}", file=sys.stderr)
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

