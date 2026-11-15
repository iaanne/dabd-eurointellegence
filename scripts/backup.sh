#!/usr/bin/env bash
# Backup EuroGrid Intelligence ke direktori berbeda (F-17).
# Isi: pg_dump ketiga database Postgres (katalog Iceberg, MLflow, Metabase)
# dan tar volume Garage (meta + data) serta volume artefak MLflow.
# Penulis dihentikan sejenak agar salinan konsisten: metadata Iceberg ada di
# Postgres, data file ada di Garage, keduanya harus cocok satu sama lain.
#
# Pakai:
#   scripts/backup.sh              # backup dengan nama timestamp UTC
#   BACKUP_ROOT=/path scripts/backup.sh
set -euo pipefail

PREFIX="${COMPOSE_PROJECT:-eurogrid}"

BACKUP_ROOT="${BACKUP_ROOT:-$HOME/Backups/eurogrid}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="$BACKUP_ROOT/$STAMP"
mkdir -p "$DEST"

echo "== backup ke $DEST"
echo "-- waktu (UTC): $(date -u +%FT%TZ)"

WRITERS=(eurogrid-prefect-worker eurogrid-iceberg-rest eurogrid-trino eurogrid-mlflow eurogrid-metabase)
echo "-- hentikan penulis: ${WRITERS[*]}"
podman stop --time 30 "${WRITERS[@]}" >/dev/null
podman stop --time 30 eurogrid-garage >/dev/null

cleanup() {
  echo "-- nyalakan lagi"
  podman start eurogrid-garage >/dev/null || true
  n=0
  until [ "$(podman inspect --format '{{.State.Health.Status}}' eurogrid-garage 2>/dev/null)" = "healthy" ] || [ "$n" -ge 60 ]; do
    sleep 2
    n=$((n + 1))
  done
  podman start "${WRITERS[@]}" >/dev/null || true
}
trap cleanup EXIT

echo "-- pg_dump (format custom)"
for db in iceberg mlflow metabase; do
  podman exec eurogrid-postgres pg_dump -U eurogrid --format=custom -f "/tmp/$db.dump" "$db"
  podman cp "eurogrid-postgres:/tmp/$db.dump" "$DEST/$db.dump"
  podman exec eurogrid-postgres rm -f "/tmp/$db.dump"
done

echo "-- tar volume garage-meta, garage-data, mlflow-data"
podman run --rm \
  -v "${PREFIX}_garage-meta":/src/garage-meta:ro,Z \
  -v "${PREFIX}_garage-data":/src/garage-data:ro,Z \
  -v "$DEST:/out:Z" \
  docker.io/library/postgres:16 \
  tar -C /src -cf /out/garage.tar garage-meta garage-data
podman run --rm \
  -v "${PREFIX}_mlflow-data":/src/mlflow-data:ro,Z \
  -v "$DEST:/out:Z" \
  docker.io/library/postgres:16 \
  tar -C /src -cf /out/mlflow.tar mlflow-data

echo "-- checksum"
( cd "$DEST" && md5sum ./*.dump ./*.tar > manifest.md5 )

cleanup
trap - EXIT

{
  echo "stamp=$STAMP"
  echo "created_utc=$(date -u +%FT%TZ)"
  echo "files=$(ls -1 "$DEST" | tr '\n' ' ')"
} > "$DEST/backup.info"

echo "== selesai. isi:"
du -sh "$DEST"/*
echo "manifest: $DEST/manifest.md5"
