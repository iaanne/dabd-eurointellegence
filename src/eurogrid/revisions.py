"""Kebijakan revisi sumber untuk Bronze dan Silver.

Sumber seperti Energy-Charts menerbitkan ulang rentang waktu yang sama. Setiap
batch membawa ``generated_at`` (kapan sumber membuat respons) dan
``available_until`` (sampai kapan datanya dianggap final). Dua akibatnya:

* satu kunci bisnis bisa punya beberapa versi, dan yang benar adalah versi
  dengan ``generated_at`` terbaru, bukan baris yang kebetulan terakhir dibaca;
* baris setelah ``available_until`` belum final sehingga tidak boleh masuk
  Silver.

Modul ini tidak menyentuh jaringan atau katalog supaya mudah diuji.
"""

from __future__ import annotations

import pandas as pd

GENERATED_AT = "generated_at"
AVAILABLE_UNTIL = "available_until"

_ORDER_GENERATED = "_revisi_generated_at"
_ORDER_AVAILABLE = "_revisi_available_until"


def parse_utc(series: pd.Series) -> pd.Series:
    """Ubah kolom waktu apa pun menjadi ``datetime64[ns, UTC]``; gagal jadi NaT."""
    if series.empty:
        return pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns, UTC]")
    return pd.to_datetime(series, utc=True, errors="coerce")


def sort_by_revision(frame: pd.DataFrame) -> pd.DataFrame:
    """Urutkan baris dari versi tertua ke terbaru.

    ``generated_at`` menentukan versi. Bila sama, ``available_until`` yang lebih
    akhir dianggap lebih lengkap. Nilai kosong dianggap paling tua sehingga
    baris tanpa metadata revisi tidak pernah menimpa baris yang punya metadata.
    """
    kolom = [k for k in (GENERATED_AT, AVAILABLE_UNTIL) if k in frame.columns]
    if not kolom:
        return frame

    siap = frame.copy()
    if GENERATED_AT in frame.columns:
        siap[_ORDER_GENERATED] = parse_utc(frame[GENERATED_AT])
    if AVAILABLE_UNTIL in frame.columns:
        siap[_ORDER_AVAILABLE] = parse_utc(frame[AVAILABLE_UNTIL])

    urut = [k for k in (_ORDER_GENERATED, _ORDER_AVAILABLE) if k in siap.columns]
    siap = siap.sort_values(urut, kind="stable", na_position="first")
    return siap.drop(columns=[k for k in urut])


def collapse_revisions(
    frame: pd.DataFrame, keys: list[str] | tuple[str, ...]
) -> tuple[int, pd.DataFrame]:
    """Sisakan satu baris per kunci bisnis, yaitu versi terbaru.

    Baris dengan kunci belum lengkap tidak dibuang di sini supaya tetap masuk
    karantina dengan alasan kolom wajib kosong. Urutan indeks asli
    dipertahankan agar hasilnya deterministik.
    """
    key_columns = [column for column in keys if column in frame.columns]
    if not key_columns or frame.empty:
        return 0, frame

    lengkap = frame[key_columns].notna().all(axis=1)
    utama = sort_by_revision(frame.loc[lengkap])
    sisa = frame.loc[~lengkap]

    unik = utama.drop_duplicates(subset=key_columns, keep="last")
    gabungan = pd.concat([unik, sisa]).sort_index()
    return int(len(frame) - len(gabungan)), gabungan


def beyond_available_until(frame: pd.DataFrame) -> pd.Series:
    """Tandai baris yang waktunya melewati ``available_until`` sumber.

    Dataset tanpa kolom itu, atau tanpa nilai yang bisa dibaca, dianggap tidak
    melanggar apa pun.
    """
    if AVAILABLE_UNTIL not in frame.columns or "timestamp_utc" not in frame.columns:
        return pd.Series(False, index=frame.index)

    batas = parse_utc(frame[AVAILABLE_UNTIL])
    waktu = parse_utc(frame["timestamp_utc"])
    return (batas.notna() & waktu.notna() & (waktu > batas)).fillna(False)
