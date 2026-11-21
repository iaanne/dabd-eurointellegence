START ?= $(shell date -d '7 days ago' +%F)
END ?= $(shell date -d 'yesterday' +%F)

.PHONY: setup test demo demo-power demo-price demo-weather demo-entsoe list show docs clean prefect-up prefect-down prefect-deploy prefect-run prefect-logs publish-iceberg sync-bronze redpanda-up redpanda-topics redpanda-list trino-up trino-query mlflow-up metabase-up monitoring-up monitoring-down backup restore-verify streaming-up streaming-down streaming-logs
.PHONY: setup test demo demo-power demo-price demo-weather demo-entsoe list show docs clean prefect-up prefect-down prefect-deploy prefect-run prefect-logs publish-iceberg sync-bronze redpanda-up redpanda-topics redpanda-list trino-up trino-query mlflow-up metabase-up monitoring-up monitoring-down backup restore-test

setup:
	uv sync --extra dev

test:
	uv run pytest

docs:
	uv run --with markdown --with weasyprint python scripts/build_docs.py

demo-power:
	uv run eurogrid ingest-energy-charts --dataset public_power --country de --start $(START) --end $(END)

demo-price:
	uv run eurogrid ingest-energy-charts --dataset price --bzn DE-LU --start $(START) --end $(END)

demo-weather:
	uv run eurogrid ingest-weather --location berlin --latitude 52.52 --longitude 13.41 --start $(START) --end $(END)

demo-entsoe:
	uv run eurogrid ingest-entsoe --start $(START) --end $(END)

demo: demo-power demo-price demo-weather demo-entsoe

list:
	uv run eurogrid list

show:
	uv run eurogrid show --dataset $(or $(DATASET),public_power)

publish-iceberg:
	uv run --extra iceberg eurogrid publish-iceberg

sync-bronze:
	uv run eurogrid sync-bronze

clean:
	rm -rf data .pytest_cache

prefect-up:
	podman-compose -p eurogrid up -d prefect-server prefect-worker

prefect-down:
	podman-compose -p eurogrid stop prefect-server prefect-worker

prefect-deploy:
	podman exec eurogrid-prefect-worker prefect deploy --all --prefect-file /app/prefect.yaml

prefect-run:
	podman exec eurogrid-prefect-worker prefect deployment run 'bronze-daily/production'

prefect-logs:
	podman logs -f eurogrid-prefect-worker

streaming-up:
	podman-compose -p eurogrid up -d --build redpanda redpanda-topics streaming-producer streaming-consumer

streaming-down:
	podman-compose -p eurogrid stop streaming-producer streaming-consumer

streaming-logs:
	podman logs -f eurogrid-streaming-producer eurogrid-streaming-consumer
redpanda-up:
	podman-compose -p eurogrid up -d redpanda redpanda-topics

redpanda-topics:
	podman exec eurogrid-redpanda rpk topic create -X brokers=localhost:9092 --if-not-exists --partitions 3 --replicas 1 eurogrid.events eurogrid.events.dlq entsoe.actual_load

redpanda-list:
	podman exec eurogrid-redpanda rpk topic list -X brokers=localhost:9092

trino-up:
	podman-compose -p eurogrid up -d trino

trino-query:
	podman exec eurogrid-trino trino --server http://localhost:8081 --catalog iceberg --schema bronze --execute "$(Q)"
mlflow-up:
	podman-compose -p eurogrid up -d mlflow

metabase-up:
	podman-compose -p eurogrid up -d metabase

monitoring-up:
	podman-compose -p eurogrid up -d prometheus grafana

monitoring-down:
	podman-compose -p eurogrid stop prometheus grafana
backup:
	scripts/backup.sh

restore-verify:
	scripts/restore.sh $(SRC)
