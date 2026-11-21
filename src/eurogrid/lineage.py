"""Kirim event OpenLineage ke Marquez.

Tanpa ``OPENLINEAGE_URL`` di environment, seluruh fungsi di sini menjadi no-op
sehingga flow tetap jalan di mesin yang tidak punya Marquez.

Event minimal yang dikirim adalah ``START`` dan ``COMPLETE`` per job dengan
daftar dataset masukan dan keluaran. Marquez memakai itu untuk menggambar
lineage Bronze -> Silver -> Gold.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Iterable, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

PRODUCER = "https://github.com/eurogrid/intelligence"
SCHEMA_URL = (
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
)
JOB_TYPE_SCHEMA = (
    "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json#/$defs/JobTypeJobFacet"
)
DEFAULT_NAMESPACE = "eurogrid"


def lineage_url() -> str | None:
    """URL Marquez, misalnya ``http://localhost:5001``."""
    nilai = os.getenv("OPENLINEAGE_URL", "").strip()
    return nilai.rstrip("/") or None


def lineage_namespace() -> str:
    return os.getenv("OPENLINEAGE_NAMESPACE", DEFAULT_NAMESPACE).strip() or DEFAULT_NAMESPACE


def runtime_run_id(seed: str) -> str:
    """Ubah id run apa pun menjadi UUID yang sah dan deterministik."""
    try:
        return str(UUID(str(seed)))
    except (ValueError, AttributeError, TypeError):
        return str(uuid5(NAMESPACE_URL, str(seed)))


def _dataset(nama: str, namespace: str) -> dict:
    return {"namespace": namespace, "name": nama}


def build_run_event(
    *,
    event_type: str,
    job_name: str,
    run_id: str,
    inputs: Iterable[str] = (),
    outputs: Iterable[str] = (),
    namespace: str | None = None,
    event_time: datetime | None = None,
) -> dict:
    """Susun satu RunEvent OpenLineage tanpa menyentuh jaringan."""
    ns = namespace or lineage_namespace()
    waktu = event_time or datetime.now(timezone.utc)
    if waktu.tzinfo is None:
        waktu = waktu.replace(tzinfo=timezone.utc)

    return {
        "eventType": event_type,
        "eventTime": waktu.isoformat(),
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
        "run": {"runId": runtime_run_id(run_id)},
        "job": {
            "namespace": ns,
            "name": job_name,
            "facets": {},
        },
        "inputs": [_dataset(nama, ns) for nama in inputs],
        "outputs": [_dataset(nama, ns) for nama in outputs],
    }


def emit_run_event(
    event: dict, *, url: str | None = None, timeout: float = 10.0
) -> bool:
    """Kirim satu event ke Marquez. Kembalikan True bila diterima."""
    tujuan = (url or lineage_url())
    if not tujuan:
        return False

    import httpx

    try:
        respons = httpx.post(f"{tujuan}/api/v1/lineage", json=event, timeout=timeout)
        respons.raise_for_status()
    except Exception as exc:  # noqa: BLE001 - lineage tidak boleh mematikan flow
        print(f"Lineage gagal dikirim: {exc}")
        return False
    return True


def emit_job(
    *,
    job_name: str,
    run_id: str,
    inputs: Sequence[str] = (),
    outputs: Sequence[str] = (),
    namespace: str | None = None,
    url: str | None = None,
    event_types: Sequence[str] = ("START", "COMPLETE"),
) -> bool:
    """Kirim START lalu event akhir untuk satu job."""
    if not (url or lineage_url()):
        return False

    berhasil = True
    for event_type in event_types:
        event = build_run_event(
            event_type=event_type,
            job_name=job_name,
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
            namespace=namespace,
        )
        berhasil = emit_run_event(event, url=url) and berhasil
    return berhasil
