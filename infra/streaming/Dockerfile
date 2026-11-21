FROM docker.io/python:3.12-slim

# Install Headless JRE untuk PySpark Structured Streaming dan utilitas sistem
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    procps \
    curl \
    bash \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

# Install dependensi eurogrid dan paket streaming (kafka-python-ng, pyspark)
RUN pip install --no-cache-dir -e ".[streaming]"

ENV PYTHONPATH=/app/src

