# Bukti: kebijakan revisi sumber dan pemicu Silver setelah Bronze

Tanggal: 2026-11-19. Pemilik: Nad (F-08).

Dokumen ini menutup dua sisa F-08 yang tidak bergantung pada Gold:

1. Silver memakai versi data terbaru menurut `generated_at` dan menghormati
   `available_until`.
2. Silver dipicu oleh flow Bronze setelah tabel Bronze terbit, bukan oleh cron
   terpisah 30 menit kemudian.

## 1. Kebijakan revisi sumber

Modul baru `src/eurogrid/revisions.py`, dipakai dua lapisan:

| Tempat | Perilaku |
| --- | --- |
| `bronze/warehouse.py:_normalize` | Duplikat pada kunci bisnis disisakan dari `generated_at` terbaru, bukan dari urutan folder batch (nama folder adalah checksum, jadi urutannya acak) |
| `silver/quality.py` | Sebelum validasi, baris dikelompokkan per kunci dan versi terbaru yang dipakai; baris setelah `available_until` masuk karantina dengan alasan `melewati_available_until` |

Aturan pemilihan:

- `generated_at` terbesar menang.
- Bila `generated_at` sama, `available_until` yang lebih akhir menang.
- Nilai revisi kosong dianggap paling tua, sehingga tidak pernah menimpa baris
  yang punya metadata revisi.
- Dataset tanpa kolom revisi (ENTSO-E, Open-Meteo) tetap memakai perilaku lama:
  baris terakhir pada kunci yang sama.

Laporan mutu mendapat kolom baru `revision_not_available`, berisi jumlah baris
yang melewati `available_until`.

### Bukti pada data nyata

Empat tabel Bronze dibandingkan sebelum dan sesudah perubahan, memakai seluruh
batch lokal (Agustus-November 2026):

| Tabel | Baris sebelum | Baris sesudah | Nilai berubah |
| --- | ---: | ---: | ---: |
| `energy_charts_price` | 8.644 | 8.644 | 0 |
| `energy_charts_public_power` | 181.524 | 181.524 | 0 |
| `entsoe_actual_load` | 8.640 | 8.640 | 0 |
| `open_meteo_weather_hourly` | 8.640 | 8.640 | 0 |

Tidak ada angka yang bergeser karena batch terbaru memang juga menang pada
urutan lama. Yang berubah adalah jaminannya: pemenang sekarang ditentukan
`generated_at`, bukan urutan nama folder.

Pada data nyata `available_until` sama dengan timestamp terakhir tiap batch,
sehingga baris yang dikarantina karena aturan ini nol. Aturan itu diuji dengan
data sintetis: satu baris setelah `available_until` masuk karantina dengan alasan
`melewati_available_until` dan laporan mencatat `revision_not_available = 1`.

Run Silver setelah perubahan:

```text
silver.entsoe_actual_load          8.640 lolos, 0 karantina, passed
silver.energy_charts_price         8.644 lolos, 0 karantina, passed
silver.energy_charts_public_power  181.524 lolos, 0 karantina, passed
silver.open_meteo_weather_hourly   8.640 lolos, 0 karantina, passed
```

## 2. Silver dipicu setelah Bronze

Perubahan:

- `flows/bronze_daily.py` mendapat task `trigger-silver`. Task ini memanggil
  `run_deployment("silver-daily/production", timeout=0)`, jadi run Silver
  dibuat lalu Bronze selesai tanpa menunggu Silver.
- Kunci idempotensi `silver-after-bronze-<flow_run_id>` mencegah dua run Silver
  saat task diulang.
- Kegagalan memicu Silver tidak menggagalkan Bronze; alasannya dicatat di
  keluaran flow pada `silver_trigger`.
- `prefect.yaml`: cron `30 5 * * *` milik Silver dihapus; deployment Silver
  sekarang hanya dipicu.

### Bukti run

```text
silver-daily 2c355949 Completed tags ['daily', 'dipicu-bronze', 'silver']
Selesai: 207.448 baris lolos, 0 baris karantina, 0 dataset diblokir
```

Jadwal Silver di server benar-benar kosong setelah deployment diperbarui:

```text
bronze-daily ba12220c jadwal tersisa [('0 5 * * *', True)]
silver-daily 70fb72b2 jadwal tersisa []
```

Catatan: `prefect deploy` memperbarui deployment tetapi tidak menghapus jadwal
lama. Jadwal 30 5 dihapus lewat API (`DELETE /deployments/{id}/schedules/{id}`,
HTTP 204). Bila anggota lain men-deploy ulang dari `prefect.yaml` yang sama,
jadwal itu tidak muncul lagi karena sudah tidak ada di berkas.

## 3. Kendala yang tidak bisa diselesaikan

Energy-Charts tidak bisa dihubungi dari jaringan ini saat pengujian:

```text
host      : ConnectTimeout [WinError 10060]
container : ConnectError [Errno 111] Connection refused
```

Akibatnya run Bronze `4a93cef8` berakhir `Failed` di task `ingest-public-power`
setelah tiga percobaan ulang, sehingga pemicu Silver tidak bisa dibuktikan lewat
run Bronze yang sukses hari ini. Pemicu dibuktikan dengan flow uji
`cek-trigger-silver` di worker yang memanggil task `trigger_silver_task` yang
sama; hasilnya satu run Silver dengan tag `dipicu-bronze` seperti di atas.

## 4. Tes

`pytest` lulus 84 tes, 9 di antaranya baru di `tests/test_revisions.py` dan
`tests/test_prefect_yaml.py`:

- versi dengan `generated_at` terbaru menang;
- `available_until` menjadi penentu bila `generated_at` sama;
- dataset tanpa kolom revisi tetap seperti sebelumnya;
- baris setelah `available_until` masuk karantina;
- `_normalize` Bronze memilih `generated_at` terbaru;
- deployment Silver tidak punya jadwal;
- flow Bronze benar-benar memanggil `silver-daily/production` dengan `timeout=0`.

## 5. Sisa F-08

- Blokir tulis Gold yang nyata: menunggu tabel Gold dari Jim.
- Persetujuan ambang oleh Jim.
