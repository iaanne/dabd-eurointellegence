FROM docker.io/library/python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir -e ".[iceberg]"

EXPOSE 9101

CMD ["python", "-m", "eurogrid.observability.exporter"]
