# Bukti F-08: Quality gate Bronze ke Silver

Tanggal: 2026-11-18
Branch: `nad`
Lingkungan: Docker Compose `eurogrid` (Garage, Iceberg REST, Postgres, Prefect), Python 3.12.12 di `.venv`.

## 1. Hasil

| Dataset Silver | Baris lolos | Karantina | Status |
| --- | ---: | ---: | --- |
| `silver.entsoe_actual_load` | 8.640 | 0 | passed |
| `silver.energy_charts_price` | 8.644 | 0 | passed |
| `silver.energy_charts_public_power` | 181.524 | 0 | passed |
| `silver.open_meteo_weather_hourly` | 8.640 | 0 | passed |

Total 207.448 baris lolos dalam satu run flow `silver-daily`, durasi 14 detik.
Karantina kosong karena data Bronze yang masuk memang bersih: 0 nilai kosong,
0 duplikat, 0 celah waktu, satu satuan dan satu resolusi per dataset.

Tabel laporan mutu menyimpan satu baris per `quality_run_id`:
`silver.<dataset>_quality_report`.

## 2. Ambang dan dasar angkanya

Angka diturunkan dari 90 hari data Bronze (2026-08-20 s/d 2026-11-17),
bukan dari perkiraan.

| Dataset | Tolak (`reject`) | Tandai (`warn`) | Dasar |
| --- | --- | --- | --- |
| `entsoe_actual_load` | `< 0`; `> 100.000 MW` | `< 25.600`; `> 80.600` | Pagar 3xIQR = 101.300 MW; 1,5xIQR = 25.600 / 80.600 MW. Maksimum observasi 72.456 MW |
| `energy_charts_price` | `< -1.000`; `> 10.000 EUR/MWh` | `< -26,2`; `> 368,8` | Harga negatif sah secara pasar (minimum observasi -19). Batas reject hanya menangkap salah satuan/skala |
| `energy_charts_public_power` | `< -30.000`; `> 120.000 MW`; negatif pada 18 seri yang tidak pernah negatif | — | Tiga seri sah negatif: `cross_border_electricity_trading`, `hydro_pumped_storage_consumption`, `residual_load` |
| `open_meteo_weather_hourly` | per variabel: suhu `-60..60 °C`, angin `0..350 km/jam`, radiasi `0..1.400 W/m²`, presipitasi `0..500 mm` | suhu `< -25`/`> 42`, angin `> 150`, presipitasi `> 60` | Rentang fisik tiap variabel |

Pemeriksaan struktural untuk semua dataset: kolom wajib terisi, timestamp bisa
dibaca dan berzona waktu, nilai numerik dan berhingga, satuan sesuai kontrak,
resolusi sesuai (`PT15M`, cuaca `PT1H`), kunci bisnis unik.

Ambang gagal: `> 1%` baris gagal **atau** `> 1%` interval waktu hilang →
status `blocked`, sehingga Gold tidak boleh dipublikasikan.

Batas ini masih pagar statistik. Batas resmi perlu disepakati dengan Jim
sebelum dijadikan kontrak permanen.

## 3. Uji injeksi data rusak

Enam baris Bronze diambil, lima di antaranya dirusak sengaja:

```text
timestamp_utc                  value        unit  alasan
2026-08-20 00:15:00+00:00           NaN     MAW  kolom_wajib_kosong:value
2026-08-20 00:30:00+00:00     -500.00000     MAW  nilai_terlalu_kecil
2026-08-20 00:45:00+00:00   999999.00000     MAW  nilai_terlalu_besar
2026-08-20 01:00:00+00:00    36853.99512      kW  satuan_tidak_sesuai
bukan-tanggal                36925.67447     MAW  timestamp_tidak_valid
```

Hasil: 1 baris lolos, 5 masuk karantina, status `blocked` karena
`ambang_gagal: 83,3333% > 1,00%`. Data uji ini tidak ditulis ke tabel.

## 4. Idempotensi

`publish_silver(datasets=["entsoe_actual_load"], run_id="idempotency-check")`
dijalankan dua kali:

* jumlah baris `silver.entsoe_actual_load` tetap 8.640;
* tabel laporan tetap 2 baris (satu run awal, satu `idempotency-check`),
  bukan bertambah setiap kali run.

Penulisan tabel Silver memakai penulisan ulang penuh, sehingga run ulang untuk
rentang yang sama menghasilkan isi yang sama.

## 5. Tes otomatis

```text
.\.venv\Scripts\python.exe -m pytest -q
46 passed
```

Belasan tes baru ada di `tests/test_silver_quality.py`: baris valid, nilai
kosong, timestamp rusak, nilai bukan angka, satuan salah, resolusi salah,
satu baris gagal beberapa aturan, batas atas, pita peringatan, duplikat kunci,
celah waktu, ambang gagal, harga negatif, aturan per seri `public_power`,
aturan per variabel cuaca, kolom hilang, dan nama dataset tidak dikenal.

## 6. Berkas yang ditambahkan

* `src/eurogrid/silver/__init__.py`
* `src/eurogrid/silver/rules.py` — aturan per dataset
* `src/eurogrid/silver/quality.py` — mesin validasi, karantina, laporan
* `src/eurogrid/silver/warehouse.py` — publikasi ke namespace `silver`
* `flows/silver_daily.py` — flow `silver-daily`
* `tests/test_silver_quality.py` — 16 tes baru
* `tests/test_prefect_yaml.py` — disesuaikan untuk dua deployment
* `prefect.yaml` — deployment `silver-daily/production` (cron 05:30 Europe/Berlin)

## 7. Cara menjalankan ulang

```powershell
# langsung, tanpa Prefect
.\.venv\Scripts\python.exe -c "import sys; sys.path.insert(0,'flows'); from silver_daily import silver_daily; silver_daily()"

# lewat Prefect
docker compose -p eurogrid exec prefect-worker prefect deployment run "silver-daily/production"
```

## 8. Yang belum

* Gold belum ada, jadi "ambang gagal memblokir Gold" baru berupa status
  `blocked` pada laporan mutu dan nilai balik flow, belum berupa penolakan
  tulis ke tabel Gold.
* Kebijakan revisi sumber (`generated_at` / `available_until`) belum
  diterapkan; Silver saat ini memakai baris terakhir per kunci dari tabel
  Bronze.
* Batas resmi menunggu persetujuan Jim.
* `make test` masih memakai Podman sehingga belum jalan di Windows/Docker
  (bagian F-18).
