FROM docker.io/python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

# Kafka client untuk Redpanda. Consumer menulis parquet dengan PyArrow,
# jadi JRE dan PySpark tidak diperlukan.
RUN pip install --no-cache-dir --timeout 120 --retries 10 -e ".[streaming]"

ENV PYTHONPATH=/app/src
