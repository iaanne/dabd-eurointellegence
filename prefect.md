# Prefect: tab UI dan kegunaan

## Tab utama

- Dashboard: ringkasan global, jumlah flow run, status, grafik aktivitas.
- Runs (Flow Runs): daftar eksekusi flow, status (Scheduled/Running/Completed/Failed), log, timeline, retry.
- Flows: daftar flow yang terdaftar (nama, entrypoint).
- Deployments: daftar deployment + jadwal, parameter, work pool, status READY/NOT_READY.
- Work Pools: pool eksekusi (process/docker/kubernetes) dan status worker yang terhubung.
- Work Queues: antrian kerja per pool, concurrency, filter tag.
- Blocks: konfigurasi reusable (storage, secret, infra) yang dipakai deployment.
- Variables: pasangan key-value global yang bisa dibaca flow.
- Automations: aturan trigger, misalnya kirim notifikasi saat flow gagal.
- Event Feed: stream event sistem (run started/completed, deployment created).
- Concurrency (limits): batas jumlah run/task paralel per tag atau global.

## Untuk proyek DABD (EuroGrid)

- Runs: status run terakhir; satu flow run berisi beberapa task run.
- Deployments: `bronze-daily/production`, jadwal cron `0 5 * * *` zona
  `Europe/Berlin`, pool `eurogrid-pool`, status READY.
- Work Pools: `eurogrid-pool` beserta worker yang terhubung.
- Blocks: `telegram-alert`, webhook ke Telegram Bot API.
- Automations: `telegram-bronze-daily-completed` dan
  `telegram-bronze-daily-failed`, mengirim pesan saat run selesai atau gagal.
- Variables: belum dipakai.

## Perintah cepat

- Nyalakan server + worker: `make prefect-up`
- Hentikan server + worker: `make prefect-down`
- Daftarkan deployment: `make prefect-deploy`
- Jalankan satu run sekarang: `make prefect-run`
- Ikuti log worker: `make prefect-logs`

## Cek cepat lewat API

```bash
curl -s http://localhost:4200/api/health
curl -s http://localhost:4200/api/deployments/filter \
  -H 'Content-Type: application/json' -d '{"limit":5}'
curl -s http://localhost:4200/api/flow_runs/filter \
  -H 'Content-Type: application/json' -d '{"sort":"START_TIME_DESC","limit":5}'
```
