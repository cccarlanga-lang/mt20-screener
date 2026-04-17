"""
Enriquecimiento de portfolio_manual.json → posiciones completas
con stop, 2R, 3R, estado y nota.

Reglas de negocio (ver /docs/SCHEMA.md §2.3 y generate_dashboard_data.py):

  stop_inicial = WMA30(fecha_compra) * 0.97   si fecha_compra está
               = precio_compra * 0.97         fallback
  R_inicial    = precio_compra - stop_inicial  (FIJO)
  stop_actual  = max(stop_inicial, WMA30_actual * 0.97)
  level_2r     = precio_compra + 2 * R_inicial  (FIJO, anclado a entrada)
  level_3r     = precio_compra + 3 * R_inicial  (FIJO)
  pnl_pct      = (current_price / precio_compra - 1) * 100
  pnl_r        = (current_price - precio_compra) / R_inicial

Máquina de estados:

  STOP_ALERT   si current_price <= stop_actual
  SELL_3R      si pnl_r >= 3 y "3R" no está en partials_done
  SELL_2R      si pnl_r >= 2 y "2R" no está en partials_done
  RUNNER       si ambos parciales hechos
  HOLD         resto
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

from .config import MAX_POSITIONS, STOP_PCT
from .market import TickerSnapshot, build_snapshot

log = logging.getLogger(__name__)


@dataclass
class ManualPosition:
    """Lo que el usuario escribe en portfolio_manual.json."""
    ticker: str
    precio_compra: float
    fecha_compra: Optional[str] = None
    partials_done: Optional[List[str]] = None
    note: Optional[str] = None


def _compute_stop_inicial(precio_compra: float, snapshot: TickerSnapshot) -> float:
    """
    Stop inicial:
      - Si conocemos WMA30 en la fecha de compra → WMA30_entrada * 0.97
      - Si no → precio_compra * 0.97 (proxy razonable; Weinstein compra cerca de WMA30)
    """
    if snapshot.wma30_weekly_at is not None:
        return snapshot.wma30_weekly_at * STOP_PCT
    return precio_compra * STOP_PCT


def _compute_current_stop(stop_inicial: float, snapshot: TickerSnapshot) -> float:
    """Stop trailing al alza: nunca baja del stop inicial."""
    stop_trailing = snapshot.wma30_weekly * STOP_PCT
    return max(stop_inicial, stop_trailing)


def _classify_status(
    current_price: float,
    pnl_r: float,
    stop_actual: float,
    partials_done: List[str],
) -> str:
    if current_price <= stop_actual:
        return "STOP_ALERT"
    if pnl_r >= 3.0 and "3R" not in partials_done:
        return "SELL_3R"
    if pnl_r >= 2.0 and "2R" not in partials_done:
        return "SELL_2R"
    if "2R" in partials_done and "3R" in partials_done:
        return "RUNNER"
    return "HOLD"


def _auto_note(status: str, manual_note: Optional[str]) -> str:
    """Si el usuario no dio nota, autogenera una descriptiva según estado."""
    if manual_note:
        return manual_note
    return {
        "STOP_ALERT": "Cerró bajo stop. Salir 100% lunes apertura.",
        "SELL_2R":    "Cruzó 2R. Vender 25%, mantener stop intacto.",
        "SELL_3R":    "Cruzó 3R. Vender 25%, dejar el resto correr.",
        "RUNNER":     "Runner. Stop sigue a WMA30×0.97 cada viernes.",
        "HOLD":       "Posición en desarrollo.",
    }.get(status, "")


def _weeks_held(fecha_compra: Optional[str], data_date: str) -> Optional[int]:
    import pandas as pd
    if not fecha_compra:
        return None
    try:
        delta = pd.Timestamp(data_date) - pd.Timestamp(fecha_compra)
        return max(0, int(delta.days / 7))
    except Exception:
        return None


def enrich_position(manual: ManualPosition) -> dict:
    """
    Construye el dict listo para data.json → portfolio.positions[].
    Si falla la descarga, devuelve un dict degradado con status=STALE_DATA.
    """
    snap = build_snapshot(manual.ticker, manual.fecha_compra)
    partials = manual.partials_done or []

    if snap is None:
        log.error("%s: sin snapshot, degrado posición", manual.ticker)
        return {
            "symbol": manual.ticker,
            "name": manual.ticker,
            "entry_price": manual.precio_compra,
            "entry_date": manual.fecha_compra,
            "current_price": None,
            "pnl_pct": None,
            "pnl_r": None,
            "stop_price": round(manual.precio_compra * STOP_PCT, 2),
            "level_2r": None,
            "level_3r": None,
            "wma30": None,
            "mansfield": None,
            "subindustry": None,
            "status": "STALE_DATA",
            "sizing": "100%",
            "stale": True,
            "note": "Sin datos de mercado recientes. Revisa conexión / yfinance.",
        }

    stop_ini = _compute_stop_inicial(manual.precio_compra, snap)
    r_ini = manual.precio_compra - stop_ini
    if r_ini <= 0:
        # pathológico: el stop quedó por encima del precio de compra
        log.warning("%s: R_inicial <= 0 (stop %.2f >= entrada %.2f). Uso proxy 3%%.",
                    manual.ticker, stop_ini, manual.precio_compra)
        stop_ini = manual.precio_compra * STOP_PCT
        r_ini = manual.precio_compra - stop_ini

    # Breakeven tras 2R: si ya se vendió el parcial 2R, el suelo del stop
    # sube al precio de compra (breakeven). Antes de 2R, suelo = stop_inicial.
    stop_floor = max(stop_ini, manual.precio_compra) if "2R" in partials else stop_ini
    stop_now = _compute_current_stop(stop_floor, snap)
    level_2r = manual.precio_compra + 2 * r_ini
    level_3r = manual.precio_compra + 3 * r_ini
    pnl_pct = (snap.current_price / manual.precio_compra - 1.0) * 100.0
    pnl_r = (snap.current_price - manual.precio_compra) / r_ini

    status = _classify_status(snap.current_price, pnl_r, stop_now, partials)

    stop_dist_pct = (stop_now / snap.current_price - 1.0) * 100.0

    return {
        "symbol":            manual.ticker,
        "name":              manual.ticker,  # el engine puede enriquecer el nombre largo
        "entry_price":       round(manual.precio_compra, 2),
        "entry_date":        manual.fecha_compra,
        "weeks_held":        _weeks_held(manual.fecha_compra, snap.data_date),
        "current_price":     round(snap.current_price, 2),
        "pnl_pct":           round(pnl_pct, 2),
        "pnl_r":             round(pnl_r, 2),
        "stop_price":        round(stop_now, 2),
        "stop_initial":      round(stop_ini, 2),
        "stop_distance_pct": round(stop_dist_pct, 2),
        "level_2r":          round(level_2r, 2),
        "level_3r":          round(level_3r, 2),
        "wma30":             round(snap.wma30_weekly, 2),
        "mansfield":         round(snap.mansfield, 2) if snap.mansfield == snap.mansfield else None,
        "subindustry":       None,  # el motor semanal lo completa si lo tiene
        "status":            status,
        "sizing":            "100%",
        "partials_done":     partials,
        "stale":             snap.stale,
        "note":              _auto_note(status, manual.note),
    }


# =====================================================================
# Agregación y acciones derivadas
# =====================================================================
def enrich_portfolio(manual_positions: List[ManualPosition], capital_total: float) -> dict:
    """Devuelve el dict completo para data.json.portfolio."""
    enriched = [enrich_position(p) for p in manual_positions]

    # Exposición aproximada: asumimos 100%/n_pos (equal weight) por simplicidad.
    # El dashboard lo usa solo como métrica informativa.
    n_pos = len(enriched)
    exposure_pct = min(100.0, 100.0 * n_pos / MAX_POSITIONS)
    cash_pct = 100.0 - exposure_pct

    return {
        "capital_total":    capital_total,
        "n_positions":      n_pos,
        "n_max_positions":  MAX_POSITIONS,
        "cash_pct":         round(cash_pct, 2),
        "exposure_pct":     round(exposure_pct, 2),
        "positions":        enriched,
    }


def derive_actions(enriched_portfolio: dict, buys: List[dict], mt_status: str) -> dict:
    """
    A partir del portfolio enriquecido y de los signals de compra,
    construye actions_this_week = {buys, partials_2r, partials_3r, stops}.

    Respeta el bloqueo por MT rojo (no hay compras).
    """
    stops = []
    partials_2r = []
    partials_3r = []

    for pos in enriched_portfolio["positions"]:
        if pos["status"] == "STOP_ALERT":
            stops.append({
                "symbol":        pos["symbol"],
                "name":          pos.get("name", pos["symbol"]),
                "current_price": pos["current_price"],
                "stop_price":    pos["stop_price"],
                "rationale":     pos["note"],
            })
        elif pos["status"] == "SELL_2R":
            partials_2r.append({
                "symbol":             pos["symbol"],
                "name":               pos.get("name", pos["symbol"]),
                "current_price":      pos["current_price"],
                "level_2r":           pos["level_2r"],
                "shares_to_sell_pct": 25,
                "rationale":          "Vender 25% al cruzar 2R. Mantener stop intacto.",
            })
        elif pos["status"] == "SELL_3R":
            partials_3r.append({
                "symbol":             pos["symbol"],
                "name":               pos.get("name", pos["symbol"]),
                "current_price":      pos["current_price"],
                "level_3r":           pos["level_3r"],
                "shares_to_sell_pct": 25,
                "rationale":          "Vender 25% al cruzar 3R. Dejar el resto correr.",
            })

    # Compras: bloqueadas si MT rojo
    if mt_status == "ROJO":
        buys_out: List[dict] = []
    else:
        buys_out = buys

    return {
        "buys":         buys_out,
        "partials_2r":  partials_2r,
        "partials_3r":  partials_3r,
        "stops":        stops,
    }
