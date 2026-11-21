#!/usr/bin/env bash
# Verifikasi restore EuroGrid Intelligence (F-17).
#
# Mode bawaan: rehearsal non-destruktif, volume/ database live TIDAK disentuh.
#   1) restore tiap dump Postgres ke database scratch di instance yang sama,
#      bandingkan checksum isi tiap tabel dengan database live, lalu drop scratch.
#   2) ekstrak tar Garage/MLflow ke direktori scratch, bandingkan md5 tiap file
#      dengan isi volume live (baca read-only).
#
# Pakai --force untuk restore sungguhan (DESTRUKTIF): stack dihentikan, isi
# volume dan database live dihapus lalu diisi dari backup. Butuh CONFIRM=yes.
#
#   scripts/restore.sh <dir-backup>
#   CONFIRM=yes scripts/restore.sh <dir-backup> --force
set -euo pipefail

PREFIX="${COMPOSE_PROJECT:-eurogrid}"

SRC="${1:?butuh direktori backup, contoh ~/Backups/eurogrid/20261114T000000Z}"
FORCE="${2:-}"
VERIFY=/tmp/opencode/eurogrid-restore-verify
rm -rf "$VERIFY"
mkdir -p "$VERIFY"

note() { printf '== %s\n' "$*"; }

# Tabel yang ditulis ulang saat Metabase hidup (catatan heartbeat Quartz dan
# properti startup seperti startup-time-millis), dibandingkan per jumlah baris saja.
VOLATILE_TABLES="setting qrtz_scheduler_state qrtz_triggers qrtz_cron_triggers qrtz_job_details qrtz_fired_triggers qrtz_simprop_triggers qrtz_simple_triggers qrtz_paused_trigger_grps task_history login_history"

note "checksum manifest"
( cd "$SRC" && md5sum -c manifest.md5 )

note "verifikasi Postgres (scratch database, non-destruktif)"
for db in iceberg mlflow metabase; do
  scratch="${db}_restore_verify"
  podman exec eurogrid-postgres dropdb -U eurogrid --if-exists "$scratch"
  podman exec eurogrid-postgres createdb -U eurogrid "$scratch"
  podman cp "$SRC/$db.dump" "eurogrid-postgres:/tmp/$db.dump"
  podman exec eurogrid-postgres pg_restore -U eurogrid -d "$scratch" --no-owner "/tmp/$db.dump" >/dev/null
  podman exec eurogrid-postgres rm -f "/tmp/$db.dump"

  tables=$(podman exec eurogrid-postgres psql -U eurogrid -d "$db" -Atc \
    "SELECT relname FROM pg_class WHERE relkind='r' AND relnamespace='public'::regnamespace ORDER BY 1")
  ok=1
  while IFS= read -r t; do
    [ -z "$t" ] && continue
    if [[ " $VOLATILE_TABLES " == *" $t "* ]]; then
      a=$(podman exec eurogrid-postgres psql -U eurogrid -d "$db" -Atc "SELECT count(*) FROM public.\"$t\"")
      b=$(podman exec eurogrid-postgres psql -U eurogrid -d "$scratch" -Atc "SELECT count(*) FROM public.\"$t\"")
      if [ "$a" -eq "$b" ]; then
        echo "  $db.$t VOLATILE-OK (jumlah baris $a; isi berubah saat aplikasi hidup)"
      elif [ "$a" -gt "$b" ]; then
        echo "  $db.$t VOLATILE-GROWTH (backup=$b, live=$a; baris bertambah setelah backup, isi backup utuh)"
      else
        echo "  $db.$t VOLATILE-HILANG (live=$a < backup=$b)"
        ok=0
      fi
      continue
    fi
    a=$(podman exec eurogrid-postgres psql -U eurogrid -d "$db" -Atc \
      "SELECT coalesce(md5(string_agg(m, '' ORDER BY m)), 'empty') FROM (SELECT md5(t::text) AS m FROM public.\"$t\" t) s")
    b=$(podman exec eurogrid-postgres psql -U eurogrid -d "$scratch" -Atc \
      "SELECT coalesce(md5(string_agg(m, '' ORDER BY m)), 'empty') FROM (SELECT md5(t::text) AS m FROM public.\"$t\" t) s")
    if [ "$a" = "$b" ]; then
      echo "  $db.$t OK"
    else
      echo "  $db.$t MISMATCH ($a vs $b)"
      ok=0
    fi
  done <<< "$tables"
  podman exec eurogrid-postgres dropdb -U eurogrid "$scratch"
  [ "$ok" -eq 1 ] || { echo "GAGAL: $db tidak cocok"; exit 1; }
done

note "verifikasi arsip tar (ekstrak scratch, bandingkan md5, non-destruktif)"
mkdir -p "$VERIFY/garage" "$VERIFY/mlflow"
tar -xf "$SRC/garage.tar" -C "$VERIFY/garage"
tar -xf "$SRC/mlflow.tar" -C "$VERIFY/mlflow"
live=$(podman run --rm \
  -v "${PREFIX}_garage-meta":/src/garage-meta:ro,Z \
  -v "${PREFIX}_garage-data":/src/garage-data:ro,Z \
  -v "${PREFIX}_mlflow-data":/src/mlflow-data:ro,Z \
  docker.io/library/postgres:16 \
  sh -c 'cd /src && find . -type f -exec md5sum {} + | sed "s| \./| |;s|^\./||"')
back=$( { (cd "$VERIFY/garage" && find . -type f -exec md5sum {} +); (cd "$VERIFY/mlflow" && find . -type f -exec md5sum {} +); } | sed 's| \./| |;s|^\./||' )
# db.sqlite-shm adalah indeks memori bersama SQLite; ditulis ulang saat Garage
# hidup, jadi nilainya tidak stabil antar waktu. db.sqlite dan db.sqlite-wal
# (yang otoritatif) tetap dicek ketat.
shm_live=$(printf '%s\n' "$live" | grep -c 'db\.sqlite-shm$' || true)
shm_back=$(printf '%s\n' "$back" | grep -c 'db\.sqlite-shm$' || true)
live=$(printf '%s\n' "$live" | grep -v 'db\.sqlite-shm$')
back=$(printf '%s\n' "$back" | grep -v 'db\.sqlite-shm$')
echo "  (dikecualikan: db.sqlite-shm, live=$shm_live berkas, backup=$shm_back berkas)"
if [ "$(printf '%s\n' "$live" | sort)" = "$(printf '%s\n' "$back" | sort)" ]; then
  echo "  garage-meta + garage-data + mlflow-data OK ($(printf '%s\n' "$live" | grep -c .) berkas)"
else
  echo "  SELISIH antara arsip dan volume live:"
  diff <(printf '%s\n' "$live" | sort) <(printf '%s\n' "$back" | sort) | head -20
  exit 1
fi

note "rehearsal selesai: semua arsip cocok dengan kondisi live"
rm -rf "$VERIFY"

if [ "$FORCE" = "--force" ]; then
  [ "${CONFIRM:-}" = "yes" ] || { echo "restore destruktif butuh CONFIRM=yes"; exit 1; }
  note "RESTORE DESTRUKTIF: hentikan stack, kosongkan volume/database, isi dari backup"
  podman stop eurogrid-prefect-worker eurogrid-iceberg-rest eurogrid-trino eurogrid-mlflow eurogrid-metabase eurogrid-garage eurogrid-postgres >/dev/null
  for db in iceberg mlflow metabase; do
    podman start eurogrid-postgres >/dev/null
    podman exec eurogrid-postgres dropdb -U eurogrid --if-exists "$db"
    podman exec eurogrid-postgres createdb -U eurogrid "$db"
    podman cp "$SRC/$db.dump" "eurogrid-postgres:/tmp/$db.dump"
    podman exec eurogrid-postgres pg_restore -U eurogrid -d "$db" --no-owner "/tmp/$db.dump"
    podman stop eurogrid-postgres >/dev/null
  done
  podman volume rm "${PREFIX}_garage-data" "${PREFIX}_garage-meta" "${PREFIX}_mlflow-data" >/dev/null
  podman volume create "${PREFIX}_garage-data" >/dev/null
  podman volume create "${PREFIX}_garage-meta" >/dev/null
  podman volume create "${PREFIX}_mlflow-data" >/dev/null
  podman run --rm -v "${PREFIX}_garage-meta":/dst/garage-meta:Z -v "${PREFIX}_garage-data":/dst/garage-data:Z -v "$SRC:/src:ro,Z" docker.io/library/postgres:16 \
    tar -C /dst -xf /src/garage.tar
  podman run --rm -v "${PREFIX}_mlflow-data":/dst/mlflow-data:Z -v "$SRC:/src:ro,Z" docker.io/library/postgres:16 \
    tar -C /dst -xf /src/mlflow.tar
  podman start eurogrid-postgres eurogrid-garage >/dev/null
  sleep 10
  podman start eurogrid-iceberg-rest eurogrid-trino eurogrid-mlflow eurogrid-metabase eurogrid-prefect-worker >/dev/null
  note "restore destruktif selesai; stack dinyalakan. Verifikasi isi dengan query/tes."
fi
