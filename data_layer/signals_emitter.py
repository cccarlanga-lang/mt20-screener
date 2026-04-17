"""
Emisor + validador de signals_auto.json.

Este módulo es el ÚNICO lugar que debe escribir signals_auto.json.
Uso típico desde el motor semanal:

    from data_layer.signals_emitter import write_signals_auto

    write_signals_auto(
        signals=[...],            # lista de dicts (ver SIGNAL_REQUIRED)
        market_status={...},      # dict con mt_ok, regime, summary
    )

Schema v1.0:

    {
      "generated_at": "2026-04-16T22:00:00+02:00",   # Madrid TZ
      "market_status": {
        "mt_ok": bool,
        "regime": "GREEN" | "AMBER" | "RED",
        "summary": str,
        # extras OPCIONALES consumidos por el pipeline:
        "nh_nl_ok": bool?,
        "qqq_above_wma_ok": bool?,
        "nh_nl_value": float?,
        "qqq_close": float?,
        "qqq_wma30": float?
      },
      "signals": [
        {
          "ticker": str,
          "action": "BUY" | "SELL" | "EXIT" | "WATCHLIST",
          "setup": str,              # p.ej. WEINSTEIN_FIRST_GEN, WEINSTEIN_CONTINUATION
          "timeframe": "W",
          "price": float,
          "date": "YYYY-MM-DD",
          "score": int,
          "stop_reference": float,
          "distance_wma30_pct": float?,
          "mansfield": float?,
          "momentum": float?,
          "industry_mansfield": float?,
          "subindustry_mansfield": float?,
          "notes": str?,
          # extras OPCIONALES, usados por el pipeline si están:
          "sizing": "100%" | "50%" | null,
          "atr_distance": float?,
          "name": str?,
          "subindustry": str?
        }
      ],
      "meta": { "source": str, "version": "1.0" }
    }
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    _MADRID = ZoneInfo("Europe/Madrid")
except Exception:  # pragma: no cover — sistemas sin tzdata
    _MADRID = timezone.utc

from .config import SIGNALS_IN

log = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0"

# Valores permitidos (para futuro SELL / EXIT / WATCHLIST)
ACTION_VALUES = {"BUY", "SELL", "EXIT", "WATCHLIST"}
REGIME_VALUES = {"GREEN", "AMBER", "RED"}
TIMEFRAME_VALUES = {"W"}

# Campos obligatorios por señal
SIGNAL_REQUIRED = (
    "ticker", "action", "setup", "timeframe",
    "price", "date", "score", "stop_reference",
)


# =====================================================================
# Validación
# =====================================================================
def validate_signals_schema(doc: dict) -> tuple[bool, list[str]]:
    """
    Mini-validator. Devuelve (ok, errors). No lanza excepciones.
    """
    errs: list[str] = []

    if not isinstance(doc, dict):
        return False, ["root no es un objeto"]

    # Top-level
    for key in ("generated_at", "market_status", "signals", "meta"):
        if key not in doc:
            errs.append(f"falta key top-level: {key}")

    ms = doc.get("market_status")
    if not isinstance(ms, dict):
        errs.append("market_status debe ser objeto")
    else:
        if "mt_ok" not in ms or not isinstance(ms.get("mt_ok"), bool):
            errs.append("market_status.mt_ok debe ser bool")
        if ms.get("regime") not in REGIME_VALUES:
            errs.append(f"market_status.regime debe ser uno de {sorted(REGIME_VALUES)}")
        if not isinstance(ms.get("summary", ""), str):
            errs.append("market_status.summary debe ser str")

    sigs = doc.get("signals")
    if not isinstance(sigs, list):
        errs.append("signals debe ser lista")
    else:
        for i, s in enumerate(sigs):
            if not isinstance(s, dict):
                errs.append(f"signals[{i}] no es objeto")
                continue
            for k in SIGNAL_REQUIRED:
                if k not in s:
                    errs.append(f"signals[{i}].{k} falta")
            if s.get("action") not in ACTION_VALUES:
                errs.append(f"signals[{i}].action inválido: {s.get('action')}")
            if s.get("timeframe") not in TIMEFRAME_VALUES:
                errs.append(f"signals[{i}].timeframe inválido: {s.get('timeframe')}")
            # Tipos numéricos básicos
            for num_key in ("price", "stop_reference"):
                v = s.get(num_key)
                if v is None or not isinstance(v, (int, float)) or v != v:  # nan-safe
                    errs.append(f"signals[{i}].{num_key} debe ser número no nulo")

    meta = doc.get("meta", {})
    if not isinstance(meta, dict) or "source" not in meta or "version" not in meta:
        errs.append("meta.source y meta.version son obligatorios")

    return (len(errs) == 0), errs


# =====================================================================
# Escritura atómica
# =====================================================================
def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".tmp",
        dir=path.parent, delete=False,
    ) as tmp:
        json.dump(payload, tmp, indent=2, ensure_ascii=False, default=_json_default)
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)


def _json_default(o: Any) -> Any:
    # pandas/numpy → native
    if hasattr(o, "item"):
        try:
            return o.item()
        except Exception:
            pass
    if hasattr(o, "isoformat"):
        return o.isoformat()
    return str(o)


def _madrid_now_iso() -> str:
    return datetime.now(_MADRID).strftime("%Y-%m-%dT%H:%M:%S%z")


# =====================================================================
# API pública
# =====================================================================
def write_signals_auto(
    signals: Iterable[dict],
    market_status: dict,
    *,
    source: str = "weekly_engine",
    path: Optional[Path] = None,
) -> Path:
    """
    Escribe signals_auto.json de forma atómica y validada.

    Si la validación falla, lanza ValueError sin tocar el archivo anterior
    (importante para robustez: NO pisar un signals_auto válido con uno roto).
    """
    path = Path(path) if path else SIGNALS_IN

    doc = {
        "generated_at":  _madrid_now_iso(),
        "market_status": _normalize_market_status(market_status),
        "signals":       [_normalize_signal(s) for s in signals],
        "meta":          {"source": source, "version": SCHEMA_VERSION},
    }

    ok, errs = validate_signals_schema(doc)
    if not ok:
        msg = "; ".join(errs[:5]) + (f" ... (+{len(errs)-5} más)" if len(errs) > 5 else "")
        raise ValueError(f"signals_auto inválido, no se escribe: {msg}")

    _atomic_write_json(path, doc)
    log.info("[OK] signals_auto.json escrito: %d signals, regime=%s",
             len(doc["signals"]), doc["market_status"]["regime"])
    return path


def _normalize_market_status(ms: dict) -> dict:
    """Asegura que mt_ok, regime y summary están presentes y tipados."""
    regime = (ms.get("regime") or "RED").upper()
    if regime not in REGIME_VALUES:
        regime = "RED"
    mt_ok = bool(ms.get("mt_ok", regime == "GREEN"))
    out = {
        "mt_ok":   mt_ok,
        "regime":  regime,
        "summary": str(ms.get("summary", "")),
    }
    # Preservar extras opcionales sin exigirlos
    for k in ("nh_nl_ok", "qqq_above_wma_ok", "nh_nl_value", "qqq_close", "qqq_wma30"):
        if k in ms:
            out[k] = ms[k]
    return out


def _normalize_signal(s: dict) -> dict:
    """Asegura tipos básicos y pone valores por defecto donde aplica."""
    def _f(v):  # cast seguro a float o None
        if v is None:
            return None
        try:
            f = float(v)
            return f if f == f else None
        except Exception:
            return None

    out = {
        "ticker":                str(s["ticker"]).upper().strip(),
        "action":                str(s.get("action", "BUY")).upper(),
        "setup":                 str(s.get("setup", "WEINSTEIN_CORE")),
        "timeframe":             str(s.get("timeframe", "W")),
        "price":                 float(s["price"]),
        "date":                  str(s["date"]),
        "score":                 int(s["score"]),
        "stop_reference":        float(s["stop_reference"]),
        "distance_wma30_pct":    _f(s.get("distance_wma30_pct")),
        "mansfield":             _f(s.get("mansfield")),
        "momentum":              _f(s.get("momentum")),
        "industry_mansfield":    _f(s.get("industry_mansfield")),
        "subindustry_mansfield": _f(s.get("subindustry_mansfield")),
        "notes":                 s.get("notes") or "",
    }
    # Extras opcionales que el pipeline sabe leer
    for k in ("sizing", "atr_distance", "name", "subindustry"):
        if k in s and s[k] is not None:
            out[k] = s[k]
    return out
