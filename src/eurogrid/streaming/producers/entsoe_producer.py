
import logging
import os
import time

from typing import Any, Callable

from eurogrid.streaming.common.event import build_event
from eurogrid.streaming.common.redpanda import create_redpanda_producer
from eurogrid.streaming.common.rate_limiter import RateLimiter


logger = logging.getLogger(__name__)

COUNTRIES = ["DE_LU", "NL", "GB"]

TOPIC = "entsoe.actual_load"

POLLING_INTERVAL = int(
    os.getenv("ENTSOE_POLLING_INTERVAL", "900")
)

RATE_LIMIT = int(
    os.getenv("ENTSOE_RATE_LIMIT", "200")
)


def publish_records(
    producer,
    country_code: str,
    records: list[dict[str, Any]],
) -> int:
    """
    Convert fetched records into standardized events
    and publish them to Redpanda.

    Expected record format:
    {
        "event_time": "2026-11-14T10:00:00Z",
        "value": 52340.5,
        "unit": "MW",
        "metadata": {}
    }
    """

    published = 0

    for record in records:
        event = build_event(
            source="entsoe",
            dataset="actual_load",
            country_code=country_code,
            event_time=record["event_time"],
            value=float(record["value"]),
            unit=record.get("unit", "MW"),
            metadata=record.get("metadata", {}),
        )

        producer.send(
            TOPIC,
            key=country_code,
            value=event,
        )

        published += 1

    return published


def run_producer(
    fetcher: Callable[[str], list[dict[str, Any]]],
) -> None:
    """
    Poll ENTSO-E and publish records to Redpanda.

    fetcher must accept a country code and return
    normalized records.
    """

    producer = create_redpanda_producer()

    limiter = RateLimiter(
        max_requests=RATE_LIMIT,
        period=60,
    )

    logger.info("Starting ENTSO-E Redpanda Producer")

    try:
        while True:
            cycle_start = time.monotonic()

            for country_code in COUNTRIES:
                try:
                    logger.info(
                        "Fetching actual load for %s",
                        country_code,
                    )

                    limiter.wait()

                    records = fetcher(country_code)

                    published = publish_records(
                        producer=producer,
                        country_code=country_code,
                        records=records,
                    )

                    producer.flush()

                    logger.info(
                        "Published %s records for %s",
                        published,
                        country_code,
                    )

                except Exception:
                    logger.exception(
                        "Failed to process country %s",
                        country_code,
                    )

            elapsed = time.monotonic() - cycle_start

            sleep_time = max(
                0,
                POLLING_INTERVAL - elapsed,
            )

            logger.info(
                "Polling cycle completed. "
                "Next cycle in %.2f seconds",
                sleep_time,
            )

            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")

    finally:
        producer.flush()
        producer.close()


ZONE_MAP = {
    "DE_LU": "10Y1001A1001A82H",
    "NL": "10YNL----------L",
    "GB": "10YGB----------A",
}


def fetch_entsoe_records(country_code: str) -> list[dict[str, Any]]:
    """
    Fetch actual load records from ENTSO-E API, or generate fallback records
    if ENTSOE_API_TOKEN is not configured yet.
    """
    from datetime import date, datetime, timedelta, timezone
    from eurogrid.config import load_settings
    from eurogrid.http import build_client
    from eurogrid.sources import entsoe

    settings = load_settings()
    bzn = ZONE_MAP.get(country_code, "10Y1001A1001A82H")

    if settings.entsoe_api_token:
        try:
            today = date.today()
            start = (today - timedelta(days=1)).isoformat()
            end = start

            with build_client(settings) as client:
                xml_text, _ = entsoe.fetch_document(
                    client,
                    settings.entsoe_api_url,
                    security_token=settings.entsoe_api_token,
                    dataset="actual_load",
                    bidding_zone=bzn,
                    start=start,
                    end=end,
                )
            envelope = entsoe.parse_document(xml_text)
            frame = entsoe.to_long_frame(
                envelope,
                dataset="actual_load",
                country=country_code.lower(),
            )
            records: list[dict[str, Any]] = []
            for _, row in frame.iterrows():
                records.append({
                    "event_time": row["timestamp_utc"].isoformat(),
                    "value": float(row["value"]),
                    "unit": str(row["unit"]),
                    "metadata": {
                        "bidding_zone": str(row["bidding_zone"]),
                        "series_id": str(row["series_id"]),
                    },
                })
            return records
        except Exception as exc:
            logger.warning(
                "Gagal mengambil data ENTSO-E untuk %s: %s. Menggunakan data demo fallback.",
                country_code,
                exc,
            )

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    base_values = {"DE_LU": 52000.0, "NL": 14000.0, "GB": 28000.0}
    simulated_val = base_values.get(country_code, 30000.0) + (hash(country_code + now_iso) % 500)
    return [
        {
            "event_time": now_iso,
            "value": float(simulated_val),
            "unit": "MAW",
            "metadata": {
                "bidding_zone": str(bzn),
                "simulated": "true",
            },
        }
    ]


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        force=True,
    )

    run_producer(fetch_entsoe_records)
