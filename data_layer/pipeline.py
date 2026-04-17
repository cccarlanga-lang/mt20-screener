"""
Pipeline: orquesta los 5 pasos y produce docs/data.json + docs/update_status.json.

  1. Lee portfolio_manual.json
  2. Lee signals_auto.json (output del motor semanal)
  3. Toma market_timing del propio signals_auto.json
  4. Enriquece cartera con yfinance + WMA30/Mansfield (data_layer.market)
  5. Deriva actions_this_week
  6. Escribe docs/*.json (atómico)

Si algo falla a nivel pipeline, escribe update_status.json con status=ERROR.
"""
from __future__ import annotations

import json
import logging
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional

from .config import PORTFOLIO_IN, SIGNALS_IN
from .output import write_data_json, write_status
from .portfolio import ManualPosition, derive_actions, enrich_portfolio

log = logging.getLogger(__name__)


# =====================================================================
# Readers de inputs
# =====================================================================
def _read_manual_portfolio() -> tuple[float, list[ManualPosition]]:
    if not PORTFOLIO_IN.exists():
        raise FileNotFoundError(f"Falta {PORTFOLIO_IN}. Crea portfolio_manual.json.")
    with PORTFOLIO_IN.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    # Compat: soporta lista plana o dict con "positions"
    if isinstance(raw, list):
        positions_raw = raw
        capital = 100_000.0
    else:
        positions_raw = raw.get("positions", [])
        capital = float(raw.get("capital_total", 100_000.0))

    positions: list[ManualPosition] = []
    for i, p in enumerate(positions_raw):
        if "ticker" not in p or "precio_compra" not in p:
            raise ValueError(f"Posición #{i} inválida: faltan ticker/precio_compra")
        positions.append(ManualPosition(
            ticker=p["ticker"].upper().strip(),
            precio_compra=float(p["precio_compra"]),
            fecha_compra=p.get("fecha_compra"),
            partials_done=p.get("partials_done") or [],
            note=p.get("note"),
        ))
    return capital, positions


def _read_signals() -> dict:
    """Si signals_auto.json no existe o es inválido, devuelve estructura vacía (no rompe)."""
    if not SIGNALS_IN.exists():
        log.warning("signals_auto.json no encontrado — dashboard se genera sin señales")
        return _empty_signals()
    try:
        with SIGNALS_IN.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.error("signals_auto.json inválido: %s — continúo sin señales", e)
        return _empty_signals()


def _empty_signals() -> dict:
    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "market_status": {
            "mt_ok": False,
            "regime": "RED",
            "summary": "Sin datos de motor semanal",
        },
        "signals": [],
        "meta": {"source": "empty_fallback", "version": "1.0"},
    }


# =====================================================================
# Transformadores
# =====================================================================
def _setup_to_type(setup: str) -> str:
    """WEINSTEIN_FIRST_GEN → FIRST_GEN, WEINSTEIN_CONTINUATION → CONTINUATION."""
    if "FIRST_GEN" in setup:
        return "FIRST_GEN"
    if "CONTINUATION" in setup:
        return "CONTINUATION"
    return setup


def _signals_to_buys(signals_doc: dict) -> list[dict]:
    """
    Convierte signals_auto.json (v1.0) → actions_this_week.buys[].
    Top 3 señales BUY ordenadas por Mansfield RS (ya vienen ordenadas del motor).
    FG y CT se mezclan; el ranking es por Mansfield, no por tipo.
    """
    sigs = [s for s in signals_doc.get("signals", []) if s.get("action") == "BUY"]

    out = []
    for s in sigs[:3]:   # como mucho 3 compras sugeridas/semana
        entry = s["price"]
        stop = s["stop_reference"]
        r_val = entry - stop
        sig_type = _setup_to_type(s.get("setup", ""))
        ms_val = s.get("mansfield")
        ms_str = f"{ms_val:+.2f}" if ms_val is not None else "N/A"

        out.append({
            "symbol":           s["ticker"],
            "name":             s.get("name", s["ticker"]),
            "type":             sig_type,
            "entry_price":      entry,
            "stop_price":       stop,
            "stop_distance_pct": round((stop / entry - 1) * 100, 2),
            "atr_distance":     s.get("atr_distance"),
            "sizing":           s.get("sizing", "100%"),
            "level_2r":         round(entry + 2 * r_val, 2),
            "level_3r":         round(entry + 3 * r_val, 2),
            "mansfield":        ms_val,
            "subindustry":      s.get("subindustry"),
            "rationale":        f"{sig_type} con Mansfield {ms_str}",
        })
    return out


def _signals_to_panel(signals_doc: dict) -> dict:
    """Convierte signals_auto.json (v1.0) → data.json.signals (panel de referencia)."""
    sigs = [s for s in signals_doc.get("signals", []) if s.get("action") == "BUY"]

    def _row(s):
        stop = s.get("stop_reference")
        return {
            "symbol":        s["ticker"],
            "name":          s.get("name", s["ticker"]),
            "type":          _setup_to_type(s.get("setup", "")),
            "current_price": s["price"],
            "wma30":         round(stop / 0.97, 2) if stop else None,
            "stop_price":    stop,
            "mansfield":     s.get("mansfield"),
            "subind_ms":     s.get("subindustry_mansfield"),
            "atr_pct":       s.get("atr_distance"),
            "subindustry":   s.get("subindustry"),
            "core_score":    s.get("score", 5),
        }

    return {
        "first_gen":    [_row(s) for s in sigs if "FIRST_GEN" in s.get("setup", "")],
        "continuation": [_row(s) for s in sigs if "CONTINUATION" in s.get("setup", "")],
    }


def _monitor_from_signals(signals_doc: dict) -> dict:
    """
    Live signal monitor: señales con action=WATCHLIST (futuro) o bloque
    watchlist legacy.
    """
    sigs = signals_doc.get("signals", [])
    wl = [s for s in sigs if s.get("action") == "WATCHLIST"]
    if not wl:
        wl = signals_doc.get("watchlist") or []
    return {"watchlist": wl}


def _next_friday_2200_utc(now: Optional[datetime] = None) -> str:
    """Próximo viernes 22:00 GMT+1 → 21:00 UTC."""
    now = now or datetime.now(timezone.utc)
    days_ahead = (4 - now.weekday()) % 7  # 4 = viernes
    if days_ahead == 0 and now.hour >= 21:
        days_ahead = 7
    nxt = (now + timedelta(days=days_ahead)).replace(hour=21, minute=0, second=0, microsecond=0)
    return nxt.strftime("%Y-%m-%dT%H:%M:%SZ")


# =====================================================================
# Entry point
# =====================================================================
def build_dashboard_data(log_url: Optional[str] = None) -> bool:
    """
    Orquesta el pipeline. Devuelve True si OK, False si ERROR.
    """
    # Marcamos "EJECUTANDO" para que el dashboard muestre spinner si se recarga
    # durante el proceso.
    try:
        write_status("EJECUTANDO", log_url=log_url, next_run_at=_next_friday_2200_utc())
    except Exception as e:
        log.warning("No pude escribir status EJECUTANDO: %s", e)

    try:
        capital, manual = _read_manual_portfolio()
        log.info("  cartera manual: %d posiciones, capital $%.0f", len(manual), capital)

        signals_doc = _read_signals()
        ms = signals_doc.get("market_status", {})
        # Traducir regime (GREEN/AMBER/RED) → VERDE/ROJO para dashboard
        mt_ok = ms.get("mt_ok", False)
        mt_status = "VERDE" if mt_ok else "ROJO"
        log.info("  signals_auto: %d signals, regime=%s (MT=%s)",
                 len(signals_doc.get("signals", [])), ms.get("regime", "?"), mt_status)

        # Enriquecer cartera
        portfolio = enrich_portfolio(manual, capital)

        # Buys desde signals + acciones derivadas
        buys = _signals_to_buys(signals_doc)
        actions = derive_actions(portfolio, buys, mt_status)

        signals_panel = _signals_to_panel(signals_doc)
        monitor = _monitor_from_signals(signals_doc)

        # data_date: tomar del primer signal o de generated_at
        sigs_list = signals_doc.get("signals", [])
        data_date = sigs_list[0].get("date", "") if sigs_list else ""
        if not data_date:
            gen = signals_doc.get("generated_at", "")
            data_date = gen[:10] if gen else datetime.now(timezone.utc).strftime("%Y-%m-%d")

        write_data_json(
            market_timing=_clean_market_timing(ms),
            portfolio=portfolio,
            actions=actions,
            signals=signals_panel,
            monitor=monitor,
            data_date=data_date,
        )
        write_status("OK", last_data_date=data_date, log_url=log_url, next_run_at=_next_friday_2200_utc())
        log.info("[OK] pipeline completo")
        return True

    except Exception as e:
        tb = traceback.format_exc()
        log.error("Pipeline falló: %s\n%s", e, tb)
        try:
            write_status(
                "ERROR",
                errors=[f"{type(e).__name__}: {e}"],
                log_url=log_url,
                next_run_at=_next_friday_2200_utc(),
            )
        except Exception as e2:
            log.error("Además, no pude escribir status ERROR: %s", e2)
        return False


def _clean_market_timing(ms: dict) -> dict:
    """
    Traduce market_status (esquema v1.0) al formato que el dashboard espera.
    regime GREEN/AMBER → VERDE, RED → ROJO.
    """
    regime = ms.get("regime", "RED")
    mt_ok = ms.get("mt_ok", regime == "GREEN")
    return {
        "status":            "VERDE" if mt_ok else "ROJO",
        "nh_nl_ok":          bool(ms.get("nh_nl_ok", False)),
        "qqq_above_wma_ok":  bool(ms.get("qqq_above_wma_ok", False)),
        "nh_nl_value":       ms.get("nh_nl_value"),
        "qqq_close":         ms.get("qqq_close"),
        "qqq_wma30":         ms.get("qqq_wma30"),
        "summary":           ms.get("summary", ""),
    }
