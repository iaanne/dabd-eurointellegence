"""Skema event streaming ENTSO-E."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EntsoeEvent:
    event_id: str
    source: str
    dataset: str
    country_code: str
    event_time: str
    ingestion_time: str
    value: float
    unit: str
    metadata: dict[str, Any]

