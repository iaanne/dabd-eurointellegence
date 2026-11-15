"""
ENTSO-E Kafka Consumer — Pure Python implementation.

Reads JSON events from Kafka using kafka-python-ng,
validates and deduplicates them, then writes partitioned
Parquet files to the Bronze layer using PyArrow.

No PySpark / JVM dependency — avoids spark-sql-kafka JAR
compatibility issues with PySpark 4.x.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config from environment (same vars as before)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "entsoe.actual_load")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "eurogrid-bronze-writer")

BRONZE_PATH = Path(os.getenv("ENTSOE_BRONZE_PATH", "/data/bronze/entsoe_actual_load"))
CHECKPOINT_PATH = Path(os.getenv("ENTSOE_CHECKPOINT_PATH", "/data/checkpoints/entsoe_actual_load"))

VALID_COUNTRIES = {"DE_LU", "NL", "GB"}

# Flush a Parquet file after accumulating this many records
BATCH_SIZE = int(os.getenv("ENTSOE_BATCH_SIZE", "50"))

# How long (seconds) to wait before flushing even if batch not full
FLUSH_INTERVAL = int(os.getenv("ENTSOE_FLUSH_INTERVAL", "30"))

# ---------------------------------------------------------------------------
# PyArrow schema
# ---------------------------------------------------------------------------
ARROW_SCHEMA = pa.schema([
    pa.field("event_id",       pa.string()),
    pa.field("source",         pa.string()),
    pa.field("dataset",        pa.string()),
    pa.field("country_code",   pa.string()),
    pa.field("event_time",     pa.timestamp("us", tz="UTC")),
    pa.field("ingestion_time", pa.timestamp("us", tz="UTC")),
    pa.field("value",          pa.float64()),
    pa.field("unit",           pa.string()),
    pa.field("metadata",       pa.map_(pa.string(), pa.string())),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_timestamp(value: str | None) -> datetime | None:
    """Try to parse an ISO timestamp string; return None on failure."""
    if not value:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S+00:00",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f+00:00",
    ):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def validate(record: dict) -> bool:
    """Return True if the record has all required fields and valid country."""
    return (
        bool(record.get("event_id"))
        and record.get("country_code") in VALID_COUNTRIES
        and record.get("event_time") is not None
        and record.get("value") is not None
        and record.get("unit")
    )


def to_arrow_row(record: dict) -> dict:
    """Convert a parsed JSON dict to a dict ready for PyArrow."""
    event_time = parse_timestamp(record.get("event_time")) or datetime.now(timezone.utc)
    ingestion_time = parse_timestamp(record.get("ingestion_time")) or datetime.now(timezone.utc)
    metadata = record.get("metadata") or {}
    return {
        "event_id":       record.get("event_id"),
        "source":         record.get("source", ""),
        "dataset":        record.get("dataset", ""),
        "country_code":   record.get("country_code"),
        "event_time":     event_time,
        "ingestion_time": ingestion_time,
        "value":          float(record.get("value", 0.0)),
        "unit":           record.get("unit", ""),
        "metadata":       list(metadata.items()),  # pa.map_ expects list of (k,v) tuples
    }


def flush_batch(rows: list[dict]) -> None:
    """Write a list of rows to partitioned Parquet under BRONZE_PATH."""
    if not rows:
        return

    # Group rows by country_code for partitioned write
    by_country: dict[str, list[dict]] = {}
    for row in rows:
        cc = row["country_code"]
        by_country.setdefault(cc, []).append(row)

    for country_code, country_rows in by_country.items():
        partition_dir = BRONZE_PATH / f"country_code={country_code}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Build PyArrow table
        columns: dict[str, list] = {field.name: [] for field in ARROW_SCHEMA}
        for row in country_rows:
            for field in ARROW_SCHEMA:
                columns[field.name].append(row[field.name])

        table = pa.table(columns, schema=ARROW_SCHEMA)

        # Unique filename per flush to avoid overwrites
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        out_file = partition_dir / f"part-{ts}.parquet"
        pq.write_table(table, out_file)
        logger.info(
            "Flushed %d rows → %s", len(country_rows), out_file
        )


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def create_consumer() -> KafkaConsumer:
    """Create a KafkaConsumer that reads from earliest offset."""
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=5_000,   # StopIteration after 5 s idle
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logger.info("Starting EuroGrid ENTSO-E Consumer (pure Python)")
    logger.info("Kafka        : %s  topic=%s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    logger.info("Bronze path  : %s", BRONZE_PATH)
    logger.info("Checkpoint   : %s", CHECKPOINT_PATH)

    BRONZE_PATH.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)

    # In-memory dedup set (event_id); cleared per flush cycle to bound memory
    seen_ids: set[str] = set()
    batch: list[dict] = []
    last_flush = time.monotonic()

    consumer = create_consumer()
    logger.info("Connected to Kafka. Waiting for messages…")

    try:
        while True:
            try:
                for message in consumer:
                    record = message.value
                    event_id = record.get("event_id")

                    if not validate(record):
                        logger.debug("Skipping invalid record: %s", event_id)
                        continue

                    if event_id in seen_ids:
                        logger.debug("Duplicate skipped: %s", event_id)
                        continue

                    seen_ids.add(event_id)
                    batch.append(to_arrow_row(record))

                    # Flush when batch is full
                    if len(batch) >= BATCH_SIZE:
                        flush_batch(batch)
                        batch.clear()
                        seen_ids.clear()
                        last_flush = time.monotonic()

            except StopIteration:
                # consumer_timeout_ms elapsed — no new messages
                pass

            # Flush on time interval even if batch not full
            if batch and (time.monotonic() - last_flush) >= FLUSH_INTERVAL:
                flush_batch(batch)
                batch.clear()
                seen_ids.clear()
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        logger.info("Interrupted — flushing remaining %d records", len(batch))
        flush_batch(batch)
    finally:
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    main()
