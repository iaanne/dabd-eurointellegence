
import json
import os

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None  # type: ignore


def create_kafka_producer() -> KafkaProducer:
    """
    Create Kafka Producer using environment configuration.
    """
    if KafkaProducer is None:
        raise ImportError(
            "Library 'kafka' belum terpasang. Pasang dengan 'uv add kafka-python-ng' "
            "atau jalankan di dalam container streaming."
        )

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    )

    producer = KafkaProducer(
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

    return producer
