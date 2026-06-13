"""
Enriquecimiento de portfolio_manual.json → posiciones completas
con stop, 2R, 3R, estado, contabilidad realized+unrealized y nota.

Reglas de negocio (ver /docs/SCHEMA.md §2.3):

  stop_inicial = WMA30(fecha_compra) * 0.97   si fecha_compra está
               = precio_compra * 0.97         fallback
  R_inicial    = precio_compra - stop_inicial  (FIJO)
  stop_actual  = max(stop_inicial, WMA30_actual * 0.97)
  level_2r     = precio_compra + 2 * R_inicial  (FIJO, anclado a entrada)
  level_3r     = precio_compra + 3 * R_inicial  (FIJO)

Contabilidad (v1.1, ledger-based):
  Los exits (PARTIAL_2R, PARTIAL_3R, STOP_CLOSE, USER_CLOSE) están en
  trades_ledger.json. fold_trade_state() computa desde ahí:

    size_open_pct       — % notional aún abierto (100 si sin parciales)
    realized_pnl_pct    — P&L ya materializado por parciales
    realized_r          — R materializado por parciales
    unrealized_pnl_pct  — P&L flotante sobre el size_open a precio actual
    unrealized_r        — idem en R
    total_pnl_pct       — realized + unrealized
    total_r             — realized + unrealized

Máquina de estados (sin cambios):
  STOP_ALERT   si current_price <= stop_actual
  SELL_3R      si pnl_r (unrealized puro sobre entry) >= 3 y 3R no hecho
  SELL_2R      si pnl_r >= 2 y 2R no hecho
  RUNNER       si ambos parciales hechos
  HOLD         resto
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

from .config import MAX_POSITIONS, STOP_PCT
from .ledger import events_for_trade, fold_trade_state
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
    trade_id: Optional[str] = None
    entry_price_source: Optional[str] = None


def _compute_stop_inicial(precio_compra: float, snapshot: TickerSnapshot) -> float:
    if snapshot.wma30_weekly_at is not None:
        return snapshot.wma30_weekly_at * STOP_PCT
    return precio_compra * STOP_PCT


def _compute_current_stop(stop_inicial: float, snapshot: TickerSnapshot) -> float:
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


def _partials_from_events(events: list[dict]) -> list[str]:
    """Deriva ['2R','3R'] del ledger (source-of-truth) en vez de partials_done."""
    out: list[str] = []
    for ev in events:
        k = ev.get("kind")
        if k == "PARTIAL_2R" and "2R" not in out:
            out.append("2R")
        elif k == "PARTIAL_3R" and "3R" not in out:
            out.append("3R")
    return out


def enrich_position(manual: ManualPosition) -> dict:
    """
    Construye el dict listo para data.json → portfolio.positions[].
    Incluye breakdown realized/unrealized/total vía fold_trade_state.
    Si falla la descarga, devuelve un dict degradado con status=STALE_DATA.
    """
    snap = build_snapshot(manual.ticker, manual.fecha_compra)

    # Eventos del trade (si tiene trade_id)
    events: list[dict] = []
    if manual.trade_id:
        events = events_for_trade(manual.trade_id)

    # Deriva partials_done del ledger. Si no hay trade_id, cae al legacy.
    if events:
        partials = _partials_from_events(events)
    else:
        partials = manual.partials_done or []

    if snap is None:
        log.error("%s: sin snapshot, degrado posición", manual.ticker)
        # Degradación: con el entry fijo computamos al menos realized desde ledger
        stop_proxy = manual.precio_compra * STOP_PCT
        r_ini_proxy = manual.precio_compra - stop_proxy
        state = fold_trade_state(events, manual.precio_compra, r_ini_proxy,
                                 current_price=None) if events else None
        return {
            "symbol":            manual.ticker,
            "name":              manual.ticker,
            "entry_price":       manual.precio_compra,
            "entry_date":        manual.fecha_compra,
            "current_price":     None,
            "pnl_pct":           None,
            "pnl_r":             None,
            "stop_price":        round(stop_proxy, 2),
            "level_2r":          None,
            "level_3r":          None,
            "wma30":             None,
            "mansfield":         None,
            "subindustry":       None,
            "status":            "STALE_DATA",
            "sizing":            f"{int(state['size_open_pct'])}%" if state else "100%",
            "partials_done":     partials,
            "trade_id":          manual.trade_id,
            "size_open_pct":     state["size_open_pct"] if state else 100.0,
            "realized_pnl_pct":  state["realized_pnl_pct"] if state else 0.0,
            "realized_r":        state["realized_r"] if state else None,
            "unrealized_pnl_pct":None,
            "unrealized_r":      None,
            "total_pnl_pct":     state["realized_pnl_pct"] if state else None,
            "total_r":           state["realized_r"] if state else None,
            "partials_detail":   state["partials"] if state else [],
            "stale":             True,
            "note":              "Sin datos de mercado recientes. Revisa conexión / yfinance.",
        }

    # Stop & R_inicial
    stop_ini = _compute_stop_inicial(manual.precio_compra, snap)
    r_ini = manual.precio_compra - stop_ini
    if r_ini <= 0:
        log.warning("%s: R_inicial <= 0 (stop %.2f >= entrada %.2f). Uso proxy 3%%.",
                    manual.ticker, stop_ini, manual.precio_compra)
        stop_ini = manual.precio_compra * STOP_PCT
        r_ini = manual.precio_compra - stop_ini

    stop_floor = max(stop_ini, manual.precio_compra) if "2R" in partials else stop_ini
    stop_now = _compute_current_stop(stop_floor, snap)
    level_2r = manual.precio_compra + 2 * r_ini
    level_3r = manual.precio_compra + 3 * r_ini

    # Legacy: pnl crudo sobre precio entrada (para la máquina de estados)
    pnl_pct_raw = (snap.current_price / manual.precio_compra - 1.0) * 100.0
    pnl_r_raw = (snap.current_price - manual.precio_compra) / r_ini

    status = _classify_status(snap.current_price, pnl_r_raw, stop_now, partials)
    stop_dist_pct = (stop_now / snap.current_price - 1.0) * 100.0

    # Contabilidad desde ledger
    state = fold_trade_state(events, manual.precio_compra, r_ini, snap.current_price)

    size_open = state["size_open_pct"]
    sizing_label = f"{int(round(size_open))}%" if size_open < 100 else "100%"

    return {
        "symbol":            manual.ticker,
        "name":              manual.ticker,  # el engine puede enriquecer después
        "entry_price":       round(manual.precio_compra, 2),
        "entry_date":        manual.fecha_compra,
        "entry_price_source":manual.entry_price_source,
        "trade_id":          manual.trade_id,
        "weeks_held":        _weeks_held(manual.fecha_compra, snap.data_date),
        "current_price":     round(snap.current_price, 2),
        # Campos "legacy" (compat con dashboard/previous): pnl crudo sobre entrada
        "pnl_pct":           round(pnl_pct_raw, 2),
        "pnl_r":             round(pnl_r_raw, 2),
        "stop_price":        round(stop_now, 2),
        "stop_initial":      round(stop_ini, 2),
        "stop_distance_pct": round(stop_dist_pct, 2),
        "level_2r":          round(level_2r, 2),
        "level_3r":          round(level_3r, 2),
        "wma30":             round(snap.wma30_weekly, 2),
        "mansfield":         round(snap.mansfield, 2) if snap.mansfield == snap.mansfield else None,
        "subindustry":       None,
        "status":            status,
        "sizing":            sizing_label,
        "partials_done":     partials,
        # NUEVO: contabilidad desde ledger
        "size_open_pct":     size_open,
        "realized_pnl_pct":  state["realized_pnl_pct"],
        "realized_r":        state["realized_r"],
        "unrealized_pnl_pct":state["unrealized_pnl_pct"],
        "unrealized_r":      state["unrealized_r"],
        "total_pnl_pct":     state["total_pnl_pct"],
        "total_r":           state["total_r"],
        "partials_detail":   state["partials"],
        "stale":             snap.stale,
        "note":              _auto_note(status, manual.note),
    }


# =====================================================================
# Riesgo por posición (peso igual: capital/MAX_POSITIONS)
# =====================================================================
def _augment_position_risk(pos: dict, capital_total: float) -> None:
    """
    Añade in-place al dict de posición:
      - shares_implied:            capital_por_slot / entry_price
      - notional_usd:              shares_implied × current_price
      - risk_to_stop_usd:          max(0, current - stop) × shares  (airbag pendiente)
      - risk_to_stop_pct_capital:  risk_to_stop_usd / capital × 100
      - pending_loss_usd:          si current < stop: (current - stop) × shares (negativo)
    Todo referido al tamaño OPEN actual (size_open_pct).
    """
    entry = pos.get("entry_price") or 0
    cp    = pos.get("current_price")
    stop  = pos.get("stop_price")
    size_open = float(pos.get("size_open_pct") or 100.0) / 100.0
    capital_per_slot = capital_total / max(1, MAX_POSITIONS)

    if not entry or entry <= 0:
        pos["shares_implied"] = None
        pos["notional_usd"] = None
        pos["risk_to_stop_usd"] = None
        pos["risk_to_stop_pct_capital"] = None
        pos["pending_loss_usd"] = None
        return

    shares_full = capital_per_slot / entry
    shares = shares_full * size_open
    pos["shares_implied"] = round(shares, 4)

    notional = shares * cp if cp else None
    pos["notional_usd"] = round(notional, 2) if notional is not None else None

    if cp is not None and stop is not None:
        diff = cp - stop
        if diff >= 0:
            risk_usd = diff * shares
            pos["risk_to_stop_usd"] = round(risk_usd, 2)
            pos["risk_to_stop_pct_capital"] = round(100.0 * risk_usd / capital_total, 3)
            pos["pending_loss_usd"] = 0.0
        else:
            # precio ya por debajo del stop: no añade "riesgo futuro", sí pérdida ya acumulada
            pos["risk_to_stop_usd"] = 0.0
            pos["risk_to_stop_pct_capital"] = 0.0
            pos["pending_loss_usd"] = round(diff * shares, 2)
    else:
        pos["risk_to_stop_usd"] = None
        pos["risk_to_stop_pct_capital"] = None
        pos["pending_loss_usd"] = None


# =====================================================================
# Agregación y acciones derivadas
# =====================================================================
def enrich_portfolio(manual_positions: List[ManualPosition], capital_total: float) -> dict:
    enriched = [enrich_position(p) for p in manual_positions]
    for pos in enriched:
        _augment_position_risk(pos, capital_total)

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
    Construye actions_this_week = {buys, partials_2r, partials_3r, stops}.
    Las señales V19 SIEMPRE aparecen — el Market Timing es informativo,
    no un bloqueo automático. El usuario decide si entrar o no según el MT.
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

    # Siempre mostramos las señales. El MT es un filtro manual, no automático.
    # Se añade campo mt_warning para que el dashboard muestre un aviso si MT != VERDE.
    for b in buys:
        b["mt_status"] = mt_status

    return {
        "buys":         buys,
        "partials_2r":  partials_2r,
        "partials_3r":  partials_3r,
        "stops":        stops,
    }
