#!/usr/bin/env bash

set -euo pipefail

# Kafka configuration
BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
KAFKA_BIN="/opt/kafka/bin/kafka-topics.sh"

# Topics to create
TOPICS=(
  "entsoe.actual_load"
  "entsoe.actual_generation"
  "entsoe.day_ahead_prices"
  "entsoe.cross_border"
)

echo "======================================"
echo " EuroGrid - Kafka Topic Initialization"
echo "======================================"
echo "Bootstrap server : ${BOOTSTRAP_SERVER}"
echo "Partitions       : ${PARTITIONS}"
echo "Replication      : ${REPLICATION_FACTOR}"
echo

# Wait until Kafka is ready
echo "Waiting for Kafka broker at ${BOOTSTRAP_SERVER}..."

while true; do
  if ERR_OUTPUT="$($KAFKA_BIN --bootstrap-server "${BOOTSTRAP_SERVER}" --list 2>&1)"; then
    break
  else
    ERR_LINE=$(echo "${ERR_OUTPUT}" | tail -n 2 | tr '\n' ' ')
    echo "Kafka is not ready yet (${ERR_LINE}). Retrying in 5 seconds..."
    sleep 5
  fi
done

echo "Kafka broker is ready."
echo

# Create topics
for TOPIC in "${TOPICS[@]}"; do
  echo "Checking topic: ${TOPIC}"

  if "$KAFKA_BIN" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --list | grep -Fxq "${TOPIC}"; then

    echo "Topic already exists: ${TOPIC}"

  else
    "$KAFKA_BIN" \
      --bootstrap-server "${BOOTSTRAP_SERVER}" \
      --create \
      --topic "${TOPIC}" \
      --partitions "${PARTITIONS}" \
      --replication-factor "${REPLICATION_FACTOR}"

    echo "Topic created: ${TOPIC}"
  fi
done

echo
echo "======================================"
echo " Kafka topics initialization complete"
echo "======================================"

# Display all topics
echo "Available topics:"
"$KAFKA_BIN" \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --list