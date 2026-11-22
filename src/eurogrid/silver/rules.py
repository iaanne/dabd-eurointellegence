"""Aturan kualitas per dataset untuk lapisan Silver.

Ambang di bawah diturunkan dari data Bronze yang benar-benar ada di katalog,
bukan angka karangan. Rentang dasarnya 2026-08-20 sampai 2026-11-17 (90 hari)
untuk keempat dataset:

* ``entsoe_actual_load``: 8.640 baris, min 34.934 MW, maks 72.456 MW.
* ``energy_charts_price``: 8.644 baris, min -19 EUR/MWh, maks 740 EUR/MWh.
* ``energy_charts_public_power``: 181.524 baris, 21 seri, min -19.623 MW,
  maks 71.714 MW.
* ``open_meteo_weather_hourly``: 8.640 baris per variabel.

Dua jenis batas dipakai:

* ``reject_*`` -- pelanggaran berat, baris masuk karantina.
* ``warn_*``   -- mencurigakan tetapi masih wajar, baris tetap lolos dan
  dicatat sebagai peringatan.

Batas atas ``actual_load`` dan ``price`` masih pagar kewarasan statistik.
Batas resmi harus disepakati dengan pemilik domain (Jim) sebelum dipakai
sebagai kontrak permanen; lihat ``docs/data-contract.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True)
class ValueBounds:
    """Batas nilai untuk satu dataset atau satu seri."""

    reject_below: float | None = None
    reject_above: float | None = None
    warn_below: float | None = None
    warn_above: float | None = None


@dataclass(frozen=True)
class SeriesRule:
    """Aturan tambahan untuk satu seri di dalam dataset."""

    unit: str | None = None
    bounds: ValueBounds | None = None
    allow_negative: bool = True


@dataclass(frozen=True)
class DatasetRule:
    """Kontrak validasi satu dataset Bronze menuju Silver."""

    name: str
    bronze_table: str
    key: tuple[str, ...]
    required: tuple[str, ...]
    units: tuple[str, ...] = ()
    series_column: str = "series_id"
    expected_resolution: str | None = "PT15M"
    expected_interval_minutes: int | None = 15
    bounds: ValueBounds = ValueBounds()
    series: Mapping[str, SeriesRule] = field(default_factory=dict)
    unit_aliases: Mapping[str, str] = field(default_factory=dict)
    max_failure_rate: float = 0.01
    max_missing_rate: float = 0.01
    notes: str = ""

    @property
    def silver_table(self) -> str:
        return f"silver.{self.name}"

    @property
    def quarantine_table(self) -> str:
        return f"silver.{self.name}_quarantine"

    @property
    def report_table(self) -> str:
        return f"silver.{self.name}_quality_report"


# Seri public_power yang belum pernah negatif pada 90 hari data. Tiga seri lain
# (cross_border_electricity_trading, hydro_pumped_storage_consumption,
# residual_load) memang sah negatif, jadi tidak masuk daftar ini.
_NON_NEGATIVE_POWER_SERIES = (
    "biomass",
    "fossil_brown_coal_lignite",
    "fossil_coal_derived_gas",
    "fossil_gas",
    "fossil_hard_coal",
    "fossil_oil",
    "geothermal",
    "hydro_pumped_storage",
    "hydro_run_of_river",
    "hydro_water_reservoir",
    "load",
    "others",
    "renewable_share_of_generation",
    "renewable_share_of_load",
    "solar",
    "waste",
    "wind_offshore",
    "wind_onshore",
)

_PUBLIC_POWER_SERIES: Mapping[str, SeriesRule] = MappingProxyType(
    {
        nama: SeriesRule(allow_negative=False)
        for nama in _NON_NEGATIVE_POWER_SERIES
    }
)

_WEATHER_SERIES: Mapping[str, SeriesRule] = MappingProxyType(
    {
        "temperature_2m": SeriesRule(
            unit="°C",
            bounds=ValueBounds(
                reject_below=-60.0,
                reject_above=60.0,
                warn_below=-25.0,
                warn_above=42.0,
            ),
        ),
        "wind_speed_10m": SeriesRule(
            unit="km/h",
            bounds=ValueBounds(
                reject_below=0.0,
                reject_above=350.0,
                warn_above=150.0,
            ),
        ),
        "shortwave_radiation": SeriesRule(
            unit="W/m²",
            bounds=ValueBounds(
                reject_below=0.0,
                reject_above=1400.0,
            ),
        ),
        "precipitation": SeriesRule(
            unit="mm",
            bounds=ValueBounds(
                reject_below=0.0,
                reject_above=500.0,
                warn_above=60.0,
            ),
        ),
    }
)


ACTUAL_LOAD = DatasetRule(
    name="entsoe_actual_load",
    bronze_table="bronze.entsoe_actual_load",
    key=("bidding_zone", "series_id", "timestamp_utc"),
    required=(
        "timestamp_utc",
        "series_id",
        "value",
        "unit",
        "bidding_zone",
    ),
    units=("MAW", "MW"),
    unit_aliases={"MAW": "MW"},
    bounds=ValueBounds(
        reject_below=0.0,
        reject_above=100_000.0,
        warn_below=25_600.0,
        warn_above=80_600.0,
    ),
    notes=(
        "Pagar 3xIQR = 101.300 MW dan 1,5xIQR = 80.600 / 25.600 MW dari "
        "90 hari data. Nilai negatif tidak mungkin untuk beban aktual."
    ),
)

PRICE = DatasetRule(
    name="energy_charts_price",
    bronze_table="bronze.energy_charts_price",
    key=("bidding_zone", "series_id", "timestamp_utc"),
    required=(
        "timestamp_utc",
        "series_id",
        "value",
        "unit",
        "bidding_zone",
    ),
    units=("EUR / MWh",),
    expected_resolution="PT15M",
    bounds=ValueBounds(
        reject_below=-500.0,
        reject_above=5_000.0,
        warn_below=-26.2,
        warn_above=368.8,
    ),
    notes=(
        "Harga negatif sah secara pasar dan tidak boleh dikarantina. Batas "
        "reject mengikuti kontrak PRD (-500 sampai 5.000 EUR/MWh). Peringatan "
        "memakai pagar 1,5xIQR (-26,2 dan 368,8 EUR/MWh)."
    ),
)

PUBLIC_POWER = DatasetRule(
    name="energy_charts_public_power",
    bronze_table="bronze.energy_charts_public_power",
    key=("country", "series_id", "timestamp_utc"),
    required=(
        "timestamp_utc",
        "series_id",
        "value",
        "unit",
        "country",
    ),
    units=("MW",),
    bounds=ValueBounds(
        reject_below=-30_000.0,
        reject_above=120_000.0,
    ),
    series=_PUBLIC_POWER_SERIES,
    notes=(
        "Batas lebar karena tiap seri berbeda skala. Tiga seri yang sah "
        "negatif: cross_border_electricity_trading, "
        "hydro_pumped_storage_consumption, residual_load."
    ),
)

WEATHER = DatasetRule(
    name="open_meteo_weather_hourly",
    bronze_table="bronze.open_meteo_weather_hourly",
    key=("location", "variable", "timestamp_utc"),
    required=(
        "timestamp_utc",
        "variable",
        "value",
        "unit",
        "location",
    ),
    series_column="variable",
    expected_resolution="PT1H",
    expected_interval_minutes=60,
    series=_WEATHER_SERIES,
    notes="Satuan dan batas berbeda per variabel cuaca.",
)

RULES: Mapping[str, DatasetRule] = MappingProxyType(
    {
        rule.name: rule
        for rule in (ACTUAL_LOAD, PRICE, PUBLIC_POWER, WEATHER)
    }
)


def get_rule(name: str) -> DatasetRule:
    """Ambil aturan berdasarkan nama dataset Silver."""
    try:
        return RULES[name]
    except KeyError:  # pragma: no cover - pesan galat saja
        tersedia = ", ".join(sorted(RULES))
        raise ValueError(f"Dataset Silver tidak dikenal: {name}. Pilihan: {tersedia}") from None
