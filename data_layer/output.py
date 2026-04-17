"""
Escritura atómica de docs/data.json y docs/update_status.json.

Escritura atómica = escribe a *.tmp y rename al final. Evita que el dashboard
lea un archivo a medio escribir si el proceso se interrumpe.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import DATA_OUT, DOCS_DIR, SCHEMA_VERSION, STATUS_OUT, METHOD_LABEL

log = logging.getLogger(__name__)


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".tmp",
        dir=path.parent, delete=False,
    ) as tmp:
        json.dump(payload, tmp, indent=2, ensure_ascii=False)
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)  # rename atómico en el mismo volumen


def write_data_json(
    market_timing: dict,
    portfolio: dict,
    actions: dict,
    signals: dict,
    monitor: dict,
    data_date: str,
) -> None:
    payload = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "data_date":    data_date,
            "version":      SCHEMA_VERSION,
            "method":       METHOD_LABEL,
        },
        "market_timing":       market_timing,
        "portfolio":           portfolio,
        "actions_this_week":   actions,
        "signals":             signals,
        "live_signal_monitor": monitor,
    }
    _atomic_write_json(DATA_OUT, payload)
    log.info("[OK] %s actualizado", DATA_OUT.name)


def write_status(
    status: str,
    last_data_date: str | None = None,
    errors: list[str] | None = None,
    log_url: str | None = None,
    next_run_at: str | None = None,
) -> None:
    """
    status: "OK" | "EJECUTANDO" | "PENDIENTE" | "ERROR"
    """
    payload = {
        "status":         status,
        "last_run_at":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "next_run_at":    next_run_at,
        "last_data_date": last_data_date,
        "errors":         errors or [],
        "log_url":        log_url,
    }
    _atomic_write_json(STATUS_OUT, payload)
    log.info("[OK] %s: %s", STATUS_OUT.name, status)
