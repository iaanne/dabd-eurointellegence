# Bukti: tes jalan dari clone bersih

Tanggal: 2026-11-19. Pemilik: Nad (F-18, bagian yang tidak bergantung pada Gold,
Trino, Redpanda, Metabase, atau MLflow).

## Langkah yang diuji

```powershell
git clone --branch nad <repo> C:\Users\ASUS\AppData\Local\Temp\opencode\clone-nad
cd C:\Users\ASUS\AppData\Local\Temp\opencode\clone-nad
uv sync --extra dev
uv run pytest
```

Clone tidak membawa `.env`, `data/`, atau `.venv`. Semua tes berjalan tanpa
container dan tanpa jaringan.

## Hasil

```text
........................................................................ [ 85%]
............                                                             [100%]
84 passed
```

## Masalah yang ditemukan dan diperbaiki

Percobaan pertama gagal:

```text
flows\bronze_daily.py:13: ModuleNotFoundError: No module named 'prefect'
FAILED tests/test_prefect_yaml.py::test_parameter_deployment_ada_di_fungsi_flow
```

`dev` extra tidak memasang Prefect, padahal tes membaca `flows/`. Sekarang
`pyproject.toml` menaruh `prefect>=3` di extra `dev`; setelah itu clone bersih
lulus 84 tes.

## Catatan

- `make test` tetap `uv run pytest`; bila `make` tidak ada di Windows,
  perintahnya dijalankan langsung. README memuat keduanya.
- Makefile tetap memakai Podman sesuai keputusan tim; tidak diubah ke Docker.
- Yang belum bisa dibuktikan: satu perintah compose untuk seluruh stack, karena
  Trino, Redpanda, Metabase, FastAPI, dan MLflow belum ada di `compose.yaml`.
