"""Katalog dataset: pemilik, retensi, jadwal, dan kunci bisnis.

Berkas katalog ada di ``docs/catalog/datasets.yaml``. Modul ini membacanya,
memeriksa kelengkapan, dan mencocokkannya dengan tabel yang benar-benar ada di
katalog Iceberg agar metadata tidak menyimpang dari kenyataan.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

LAYERS = ("bronze", "silver", "gold")
WAJIB = (
    "name",
    "layer",
    "source",
    "owner",
    "description",
    "grain",
    "business_key",
    "retention_days",
    "refresh",
)
DEFAULT_PATH = Path("docs/catalog/datasets.yaml")


@dataclass(frozen=True)
class DatasetEntry:
    name: str
    layer: str
    source: str
    owner: str
    description: str
    grain: str
    business_key: tuple[str, ...]
    retention_days: int | None
    refresh: str
    license: str | None = None
    quality_gate: str | None = None
    columns: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "layer": self.layer,
            "source": self.source,
            "owner": self.owner,
            "description": self.description,
            "grain": self.grain,
            "business_key": list(self.business_key),
            "retention_days": self.retention_days,
            "refresh": self.refresh,
            "license": self.license,
            "quality_gate": self.quality_gate,
            "columns": list(self.columns),
        }


def load_catalog(path: str | Path | None = None) -> dict[str, DatasetEntry]:
    """Baca katalog YAML menjadi kamus ``nama -> DatasetEntry``."""
    berkas = Path(path or DEFAULT_PATH)
    if not berkas.is_file():
        raise FileNotFoundError(f"Katalog tidak ditemukan: {berkas}")

    isi = yaml.safe_load(berkas.read_text(encoding="utf-8")) or {}
    versi = isi.get("version")
    if versi != 1:
        raise ValueError(f"Versi katalog tidak didukung: {versi!r}")

    entri: dict[str, DatasetEntry] = {}
    for item in isi.get("datasets") or []:
        _periksa_item(item)
        dataset = DatasetEntry(
            name=item["name"],
            layer=item["layer"],
            source=item["source"],
            owner=item["owner"],
            description=item["description"],
            grain=item["grain"],
            business_key=tuple(item["business_key"]),
            retention_days=item["retention_days"],
            refresh=item["refresh"],
            license=item.get("license"),
            quality_gate=item.get("quality_gate"),
            columns=tuple(item.get("columns") or ()),
        )
        if dataset.name in entri:
            raise ValueError(f"Dataset ganda di katalog: {dataset.name}")
        entri[dataset.name] = dataset

    if not entri:
        raise ValueError("Katalog tidak memuat satu dataset pun.")
    return entri


def _periksa_item(item: Mapping[str, Any]) -> None:
    kurang = [kunci for kunci in WAJIB if kunci not in item]
    if kurang:
        raise ValueError(
            f"Entri katalog {item.get('name', '<tanpa nama>')} kurang kolom: "
            + ", ".join(kurang)
        )
    if item["layer"] not in LAYERS:
        raise ValueError(f"Layer tidak dikenal: {item['layer']} pada {item['name']}")
    if not str(item["owner"]).strip():
        raise ValueError(f"Pemilik kosong pada {item['name']}")
    if not item["business_key"]:
        raise ValueError(f"Kunci bisnis kosong pada {item['name']}")
    retensi = item["retention_days"]
    if retensi is not None and (not isinstance(retensi, int) or retensi <= 0):
        raise ValueError(f"Retensi tidak sah pada {item['name']}: {retensi!r}")


def check_against_tables(
    katalog: Mapping[str, DatasetEntry], tabel_tersedia: Iterable[str]
) -> dict[str, list[str]]:
    """Bandingkan katalog dengan tabel Iceberg yang ada.

    ``tabel_tersedia`` memakai nama ber-namespace, misalnya
    ``bronze.entsoe_actual_load``. Tabel bantu Silver (``_quarantine`` dan
    ``_quality_report``) mengikuti metadata dataset induknya, jadi tidak
    dihitung sebagai tabel tanpa metadata.
    """
    tersedia = set(tabel_tersedia)
    hilang = sorted(nama for nama in katalog if nama not in tersedia)
    tanpa_metadata = sorted(
        nama
        for nama in tersedia
        if nama not in katalog and not _tabel_bantu(nama, katalog)
    )
    return {"hilang": hilang, "tanpa_metadata": tanpa_metadata}


ARTIFACT_SUFFIXES = ("_quarantine", "_quality_report")


def _tabel_bantu(nama: str, katalog: Mapping[str, DatasetEntry]) -> bool:
    for akhiran in ARTIFACT_SUFFIXES:
        if nama.endswith(akhiran):
            return nama[: -len(akhiran)] in katalog
    return False


def owners(katalog: Mapping[str, DatasetEntry]) -> dict[str, list[str]]:
    """Pemilik beserta dataset yang dipegangnya, untuk laporan."""
    hasil: dict[str, list[str]] = {}
    for dataset in katalog.values():
        hasil.setdefault(dataset.owner, []).append(dataset.name)
    return {nama: sorted(dataset) for nama, dataset in sorted(hasil.items())}
