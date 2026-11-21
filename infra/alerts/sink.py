"""Penerima webhook Alertmanager untuk lingkungan lokal.

Tanpa email atau Slack, alert tetap harus bisa dibuktikan sampai. Layanan ini
menerima POST dari Alertmanager, menyimpan tiap alert sebagai satu baris JSON,
dan menyediakannya kembali lewat GET supaya mudah diperiksa.

Hanya memakai pustaka standar, jadi tidak perlu proses build.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

OUTPUT = Path(os.getenv("ALERT_SINK_FILE", "/data/alerts.jsonl"))
PORT = int(os.getenv("ALERT_SINK_PORT", "8080"))
MAKS_BACA = 50
KUNCI = threading.Lock()


def _waktu() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def simpan(payload: dict) -> list[dict]:
    """Tulis tiap alert pada payload webhook sebagai satu baris JSON."""
    tersimpan: list[dict] = []
    for alert in payload.get("alerts") or []:
        labels = alert.get("labels") or {}
        record = {
            "received_at": _waktu(),
            "status": alert.get("status"),
            "alertname": labels.get("alertname"),
            "severity": labels.get("severity"),
            "owner": labels.get("owner"),
            "channel": labels.get("channel"),
            "summary": (alert.get("annotations") or {}).get("summary"),
            "starts_at": alert.get("startsAt"),
            "ends_at": alert.get("endsAt"),
            "fingerprint": alert.get("fingerprint"),
        }
        tersimpan.append(record)

    if tersimpan:
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        with KUNCI, OUTPUT.open("a", encoding="utf-8") as berkas:
            for record in tersimpan:
                berkas.write(json.dumps(record, ensure_ascii=False) + "\n")
    return tersimpan


def terakhir(limit: int = MAKS_BACA) -> list[dict]:
    if not OUTPUT.is_file():
        return []
    with KUNCI:
        baris = OUTPUT.read_text(encoding="utf-8").splitlines()
    hasil: list[dict] = []
    for teks in baris[-limit:]:
        try:
            hasil.append(json.loads(teks))
        except json.JSONDecodeError:
            continue
    return hasil


class Handler(BaseHTTPRequestHandler):
    server_version = "eurogrid-alert-sink/1.0"

    def _kirim(self, kode: int, payload: object) -> None:
        isi = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(kode)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(isi)))
        self.end_headers()
        self.wfile.write(isi)

    def do_POST(self) -> None:  # noqa: N802 - nama wajib dari BaseHTTPRequestHandler
        if self.path.rstrip("/") != "/alerts":
            self._kirim(404, {"error": "rute tidak dikenal"})
            return
        panjang = int(self.headers.get("Content-Length") or 0)
        mentah = self.rfile.read(panjang) if panjang else b"{}"
        try:
            payload = json.loads(mentah or b"{}")
        except json.JSONDecodeError:
            self._kirim(400, {"error": "payload bukan JSON"})
            return

        tersimpan = simpan(payload)
        print(
            json.dumps(
                {
                    "timestamp": _waktu(),
                    "severity": "INFO",
                    "service": "eurogrid-alert-sink",
                    "message": f"menerima {len(tersimpan)} alert",
                    "alerts": [
                        f"{item['status']}:{item['alertname']}" for item in tersimpan
                    ],
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        self._kirim(200, {"accepted": len(tersimpan)})

    def do_GET(self) -> None:  # noqa: N802
        jalur = self.path.split("?")[0].rstrip("/")
        if jalur == "/alerts":
            self._kirim(200, {"alerts": terakhir()})
        elif jalur in ("/health", ""):
            self._kirim(200, {"status": "ok", "file": str(OUTPUT)})
        else:
            self._kirim(404, {"error": "rute tidak dikenal"})

    def log_message(self, format: str, *args) -> None:  # noqa: A002
        """Matikan log bawaan; setiap permintaan sudah dicatat sebagai JSON."""
        return


def main() -> None:
    print(
        json.dumps(
            {
                "timestamp": _waktu(),
                "severity": "INFO",
                "service": "eurogrid-alert-sink",
                "message": f"mendengarkan di :{PORT}, menyimpan ke {OUTPUT}",
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
