"""Lapisan Silver: data Bronze yang sudah divalidasi.

Modul utama:

* :mod:`eurogrid.silver.rules` -- aturan kualitas per dataset.
* :mod:`eurogrid.silver.quality` -- mesin validasi, karantina, laporan mutu.
* :mod:`eurogrid.silver.warehouse` -- publikasi hasil ke namespace Iceberg.
"""

from .quality import QualityResult, validate_frame
from .rules import RULES, DatasetRule, get_rule
from .warehouse import SilverResult, publish_silver

__all__ = [
    "DatasetRule",
    "QualityResult",
    "RULES",
    "SilverResult",
    "get_rule",
    "publish_silver",
    "validate_frame",
]
