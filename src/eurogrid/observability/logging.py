"""Log terstruktur satu baris per kejadian.

Setiap baris memuat ``timestamp``, ``severity``, ``service``, dan ``run_id``
sehingga bisa dicari dan dihitung mesin, bukan dibaca manusia saja. Formatter
dipasang ke root logger dan ke logger Prefect, jadi pesan bawaan Prefect
("Beginning flow run ...") juga keluar sebagai JSON.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

WAJIB = ("timestamp", "severity", "service", "run_id")


def _timestamp(created: float) -> str:
    """Waktu log dalam ISO 8601 UTC dengan akhiran Z."""
    return (
        datetime.fromtimestamp(created, timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class JsonFormatter(logging.Formatter):
    """Ubah LogRecord menjadi satu objek JSON tanpa baris baru di dalamnya."""

    def __init__(self, service: str, run_id: str | None = None) -> None:
        super().__init__()
        self.service = service
        self.run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": _timestamp(record.created),
            "severity": record.levelname,
            "service": self.service,
            "logger": record.name,
            "message": record.getMessage(),
        }
        run_id = getattr(record, "run_id", None) or self.run_id
        if run_id:
            payload["run_id"] = str(run_id)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_json_logging(
    service: str,
    *,
    run_id: str | None = None,
    stream=None,
    level: int = logging.INFO,
) -> JsonFormatter:
    """Pasang formatter JSON ke root dan seluruh logger Prefect.

    Handler lama Prefect tidak dihapus supaya log tetap sampai ke UI Prefect;
    hanya formatternya yang diganti.
    """
    formatter = JsonFormatter(service, run_id)
    target = stream or sys.stdout

    root = logging.getLogger()
    for existing in list(root.handlers):
        if isinstance(existing, logging.StreamHandler):
            root.removeHandler(existing)
    handler = logging.StreamHandler(target)
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(level)

    for name, logger in list(logging.Logger.manager.loggerDict.items()):
        if not isinstance(logger, logging.Logger):
            continue
        if name == "prefect" or name.startswith("prefect."):
            for existing in logger.handlers:
                existing.setFormatter(formatter)
            logger.propagate = False

    # Klien HTTP bawaan terlalu ramai untuk log produksi.
    for berisik in ("httpx", "httpcore", "urllib3"):
        logging.getLogger(berisik).setLevel(logging.WARNING)
    return formatter


def flow_run_id() -> str | None:
    """Id flow run Prefect yang sedang berjalan, bila ada."""
    try:
        from prefect.runtime import flow_run

        return flow_run.id
    except Exception:  # noqa: BLE001 - di luar Prefect tidak ada id
        return None
