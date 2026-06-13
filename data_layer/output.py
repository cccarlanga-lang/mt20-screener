"""
Escritura atómica de docs/data.json y docs/update_status.json.

Escritura atómica = escribe a *.tmp y rename al final. Evita que el dashboard
lea un archivo a medio escribir si el proceso se interrumpe.

Staleness semáforo (meta.staleness):
  - level="fresh"  (green)   si days_since_data ≤ 5
  - level="aged"   (amber)   si 6 ≤ days_since_data ≤ 8
  - level="stale"  (red)     si days_since_data ≥ 9

(El pipeline corre semanalmente los viernes; 5 días cubre viernes→miércoles.)

Agregados Fase 2:
  - _compute_cockpit        → portfolio.cockpit (equity, DD, risk-at-stake, expectancy)
  - _build_action_queue     → actions_this_week.queue (lista única priorizada)
  - _persist_regime_snapshot/_load_regime_age → meta.regime (weeks_in_regime)
  - _persist_equity_snapshot                   → equity_history.json (serie para DD)
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .config import (
    DATA_OUT, DOCS_DIR, EQUITY_HIST, METHOD_LABEL,
    REGIME_HIST, SCHEMA_VERSION, STATUS_OUT,
)

log = logging.getLogger(__name__)


# =====================================================================
# IO atómico
# =====================================================================
def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".tmp",
        dir=path.parent, delete=False,
    ) as tmp:
        json.dump(payload, tmp, indent=2, ensure_ascii=False)
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)


def _load_json_or_default(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning("No pude leer %s (%s) — uso default", path, e)
        return default


# =====================================================================
# Staleness
# =====================================================================
def _compute_staleness(data_date: str) -> dict:
    try:
        d = datetime.strptime(data_date, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return {"level": "unknown", "days_since_data": None, "message": "data_date inválida"}
    today = date.today()
    days = (today - d).days
    if days <= 5:
        return {"level": "fresh", "days_since_data": days,
                "message": f"Datos frescos ({days}d desde último cierre)"}
    if days <= 8:
        return {"level": "aged", "days_since_data": days,
                "message": f"Datos de hace {days}d — debería refrescarse pronto"}
    return {"level": "stale", "days_since_data": days,
            "message": f"Datos obsoletos ({days}d) — ejecutar pipeline antes de operar"}


# =====================================================================
# Cockpit: equity, drawdown, risk-at-stake, expectancy
# =====================================================================
def _compute_cockpit(portfolio: dict, closed_trades: list[dict]) -> dict:
    """
    Calcula los 4 KPI grandes del cockpit superior.

    equity_total = capital_base + Σ realized(closed) + Σ unrealized(open)
    drawdown_pct = (equity_actual / max(equity_history) - 1) × 100   si history
                 = 0                                                  si primer snapshot
    risk_at_stake_usd = Σ max(0, current - stop) × shares_implied    (peso igual)
    expectancy_r      = avg(r_final) sobre trades cerrados           (None si vacío)
    """
    capital = float(portfolio.get("capital_total", 0) or 0)
    positions = portfolio.get("positions", []) or []

    realized_pnl_usd = 0.0
    unrealized_pnl_usd = 0.0
    risk_usd = 0.0
    pending_loss_usd = 0.0

    for pos in positions:
        # Unrealized en $ ≈ notional_usd - (shares × entry_price)
        shares = pos.get("shares_implied")
        entry = pos.get("entry_price")
        cp = pos.get("current_price")
        if shares and entry and cp is not None:
            unrealized_pnl_usd += shares * (cp - entry)

        # Realized del trade abierto en $ (parciales ya ejecutados)
        # Aproximación: realized_pnl_pct se refiere al notional original.
        # realized_usd ≈ realized_pnl_pct/100 × (capital/MAX) (peso igual del slot inicial)
        from .config import MAX_POSITIONS as _MX
        realized_pct = pos.get("realized_pnl_pct")
        if realized_pct is not None and entry:
            realized_pnl_usd += (realized_pct / 100.0) * (capital / _MX)

        # Riesgo pendiente hasta el stop
        r_usd = pos.get("risk_to_stop_usd")
        if r_usd is not None:
            risk_usd += float(r_usd)
        pl_usd = pos.get("pending_loss_usd")
        if pl_usd is not None:
            pending_loss_usd += float(pl_usd)

    # Realized de trades cerrados (del history)
    for t in closed_trades or []:
        pnl_pct = t.get("pnl_final_pct")
        if pnl_pct is None:
            continue
        # Aproximación: pnl_pct es sobre el notional original del slot.
        # Slot asumido = capital / MAX_POSITIONS en el momento (usamos capital actual).
        from .config import MAX_POSITIONS as _MX
        realized_pnl_usd += (float(pnl_pct) / 100.0) * (capital / _MX)

    equity_total = capital + realized_pnl_usd + unrealized_pnl_usd

    # Drawdown contra máximo histórico (equity_history.json)
    dd_pct, peak, peak_date = _drawdown_from_history(equity_total)

    # Expectancy: avg(r_final) de cerrados
    r_vals = [t.get("r_final") for t in (closed_trades or []) if t.get("r_final") is not None]
    if r_vals:
        expectancy_r = sum(r_vals) / len(r_vals)
        wins = [r for r in r_vals if r > 0]
        losses = [r for r in r_vals if r < 0]
        winrate = 100.0 * len(wins) / len(r_vals)
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
    else:
        expectancy_r = None
        winrate = None
        avg_win = None
        avg_loss = None

    return {
        "equity_total":         round(equity_total, 2),
        "capital_base":         round(capital, 2),
        "realized_pnl_usd":     round(realized_pnl_usd, 2),
        "unrealized_pnl_usd":   round(unrealized_pnl_usd, 2),
        "drawdown_pct":         round(dd_pct, 2) if dd_pct is not None else None,
        "peak_equity":          round(peak, 2) if peak is not None else None,
        "peak_date":            peak_date,
        "risk_at_stake_usd":    round(risk_usd, 2),
        "risk_at_stake_pct":    round(100.0 * risk_usd / capital, 2) if capital > 0 else None,
        "pending_loss_usd":     round(pending_loss_usd, 2),
        "expectancy_r":         round(expectancy_r, 3) if expectancy_r is not None else None,
        "winrate_pct":          round(winrate, 1) if winrate is not None else None,
        "avg_win_r":            round(avg_win, 3) if avg_win is not None else None,
        "avg_loss_r":           round(avg_loss, 3) if avg_loss is not None else None,
        "n_closed":             len(r_vals),
    }


def _drawdown_from_history(equity_now: float) -> tuple[float | None, float | None, str | None]:
    """
    Lee equity_history.json (lista de {date, equity}) y calcula DD desde peak.
    Si no hay history o es corto, devuelve (0, equity_now, today).
    """
    doc = _load_json_or_default(EQUITY_HIST, {"snapshots": []})
    snaps = doc.get("snapshots", []) or []
    if not snaps:
        return 0.0, equity_now, None
    # Considerar también el equity actual para no perder el máximo si sube HOY
    history_with_now = list(snaps) + [{"date": date.today().isoformat(), "equity": equity_now}]
    peak_entry = max(history_with_now, key=lambda s: s.get("equity", 0) or 0)
    peak = float(peak_entry.get("equity") or equity_now)
    peak_date = peak_entry.get("date")
    if peak <= 0:
        return 0.0, peak, peak_date
    dd = (equity_now / peak - 1.0) * 100.0
    return dd, peak, peak_date


# =====================================================================
# Unified Action Queue (Fase 2B)
# =====================================================================
_PRIORITY = {
    "STOP": 0,     # máxima urgencia
    "SELL_3R": 1,
    "SELL_2R": 2,
    "BUY": 3,      # menor prioridad (fill lunes)
}


def _build_action_queue(portfolio: dict, actions: dict) -> list[dict]:
    """
    Lista única ordenada por (severidad, −impacto$).
    Fusiona stops + partials_3r + partials_2r + buys con una estructura común.
    """
    queue: list[dict] = []

    # Index de posiciones por symbol para impacto $
    pos_by_sym = {p["symbol"]: p for p in portfolio.get("positions", [])}

    def _impact_usd_for_exit(pos: dict | None, sell_pct: float) -> float:
        """Impacto $ de un exit: notional afectado a precio actual."""
        if not pos:
            return 0.0
        shares = pos.get("shares_implied") or 0
        cp = pos.get("current_price") or 0
        return float(shares) * float(cp) * float(sell_pct) / 100.0

    # STOPS
    for s in actions.get("stops", []) or []:
        pos = pos_by_sym.get(s["symbol"])
        impact = _impact_usd_for_exit(pos, 100.0)
        queue.append({
            "priority":        _PRIORITY["STOP"],
            "kind":            "STOP",
            "symbol":          s["symbol"],
            "name":            s.get("name") or s["symbol"],
            "action_label":    "VENDER 100% al open del lunes",
            "fill_price_est":  s.get("current_price"),
            "reference_price": s.get("stop_price"),
            "size_pct":        100.0,
            "dollar_impact":   round(impact, 2),
            "r_impact":        pos.get("pnl_r") if pos else None,
            "rationale":       s.get("rationale") or "Cerró bajo stop",
            "severity":        "red",
        })

    # SELL_3R
    for p in actions.get("partials_3r", []) or []:
        pos = pos_by_sym.get(p["symbol"])
        impact = _impact_usd_for_exit(pos, float(p.get("shares_to_sell_pct", 25)))
        queue.append({
            "priority":        _PRIORITY["SELL_3R"],
            "kind":            "SELL_3R",
            "symbol":          p["symbol"],
            "name":            p.get("name") or p["symbol"],
            "action_label":    f"VENDER {p.get('shares_to_sell_pct', 25)}% al open del lunes",
            "fill_price_est":  p.get("current_price"),
            "reference_price": p.get("level_3r"),
            "size_pct":        float(p.get("shares_to_sell_pct", 25)),
            "dollar_impact":   round(impact, 2),
            "r_impact":        3.0,
            "rationale":       p.get("rationale") or "Cruzó 3R — asegura beneficio, deja correr",
            "severity":        "amber",
        })

    # SELL_2R
    for p in actions.get("partials_2r", []) or []:
        pos = pos_by_sym.get(p["symbol"])
        impact = _impact_usd_for_exit(pos, float(p.get("shares_to_sell_pct", 25)))
        queue.append({
            "priority":        _PRIORITY["SELL_2R"],
            "kind":            "SELL_2R",
            "symbol":          p["symbol"],
            "name":            p.get("name") or p["symbol"],
            "action_label":    f"VENDER {p.get('shares_to_sell_pct', 25)}% al open del lunes",
            "fill_price_est":  p.get("current_price"),
            "reference_price": p.get("level_2r"),
            "size_pct":        float(p.get("shares_to_sell_pct", 25)),
            "dollar_impact":   round(impact, 2),
            "r_impact":        2.0,
            "rationale":       p.get("rationale") or "Cruzó 2R — asegura parte, stop intacto",
            "severity":        "amber",
        })

    # BUYS
    capital = float(portfolio.get("capital_total") or 0)
    from .config import MAX_POSITIONS as _MX
    slot_usd = capital / max(1, _MX) if capital else 0
    for b in actions.get("buys", []) or []:
        queue.append({
            "priority":        _PRIORITY["BUY"],
            "kind":            "BUY",
            "subtype":         b.get("type"),  # FIRST_GEN / CONTINUATION
            "symbol":          b["symbol"],
            "name":            b.get("name") or b["symbol"],
            "country_flag":    b.get("country_flag"),
            "region":          b.get("region"),
            "action_label":    f"COMPRAR ~{slot_usd:,.0f}$ al open del lunes" if slot_usd else "COMPRAR al open del lunes",
            "fill_price_est":  b.get("entry_price"),
            "reference_price": b.get("stop_price"),
            "size_pct":        0.0,  # es entrada, no exit
            "dollar_impact":   round(slot_usd, 2),
            "r_impact":        None,
            "rationale":       b.get("rationale", ""),
            "severity":        "green",
        })

    # Orden: prioridad asc, desempate por dollar_impact desc
    queue.sort(key=lambda a: (a["priority"], -float(a.get("dollar_impact") or 0)))
    return queue


# =====================================================================
# Histórico de régimen (semanal append)
# =====================================================================
def _persist_regime_snapshot(mt_status: str, data_date: str) -> dict:
    """
    Append idempotente: si ya hay una entrada para data_date, no duplica.
    Devuelve {status, weeks_in_regime, since}.
    """
    doc = _load_json_or_default(REGIME_HIST, {"snapshots": []})
    snaps: list[dict] = doc.get("snapshots", []) or []

    # Evitar duplicar misma fecha
    if not any(s.get("date") == data_date for s in snaps):
        snaps.append({"date": data_date, "status": mt_status})
        snaps.sort(key=lambda s: s.get("date") or "")
        doc["snapshots"] = snaps
        _atomic_write_json(REGIME_HIST, doc)

    # Calcular desde cuándo estamos en el régimen actual (retrocediendo)
    weeks = 0
    since = data_date
    for s in reversed(snaps):
        if s.get("status") == mt_status:
            weeks += 1
            since = s.get("date") or since
        else:
            break
    return {
        "status":            mt_status,
        "weeks_in_regime":   weeks,
        "since":             since,
        "n_snapshots_total": len(snaps),
    }


# =====================================================================
# Histórico de equity (semanal append)
# =====================================================================
def _persist_equity_snapshot(equity: float, data_date: str) -> None:
    doc = _load_json_or_default(EQUITY_HIST, {"snapshots": []})
    snaps: list[dict] = doc.get("snapshots", []) or []
    if any(s.get("date") == data_date for s in snaps):
        return
    snaps.append({"date": data_date, "equity": round(float(equity), 2)})
    snaps.sort(key=lambda s: s.get("date") or "")
    doc["snapshots"] = snaps
    _atomic_write_json(EQUITY_HIST, doc)


# =====================================================================
# Public API
# =====================================================================
def write_data_json(
    market_timing: dict,
    portfolio: dict,
    actions: dict,
    signals: dict,
    monitor: dict,
    data_date: str,
) -> None:
    # Agregados Fase 2
    closed = portfolio.get("closed_trades", []) or []
    cockpit = _compute_cockpit(portfolio, closed)
    portfolio["cockpit"] = cockpit
    actions["queue"] = _build_action_queue(portfolio, actions)
    regime = _persist_regime_snapshot(market_timing.get("status") or "?", data_date)
    _persist_equity_snapshot(cockpit["equity_total"], data_date)

    staleness = _compute_staleness(data_date)
    payload = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "data_date":    data_date,
            "version":      SCHEMA_VERSION,
            "method":       METHOD_LABEL,
            "staleness":    staleness,
            "regime":       regime,
        },
        "market_timing":       market_timing,
        "portfolio":           portfolio,
        "actions_this_week":   actions,
        "signals":             signals,
        "live_signal_monitor": monitor,
    }
    _atomic_write_json(DATA_OUT, payload)
    log.info("[OK] %s actualizado (staleness=%s %dd, regime=%s %dw, equity=$%.0f, DD=%s%%)",
             DATA_OUT.name,
             staleness["level"], staleness.get("days_since_data") or -1,
             regime["status"], regime["weeks_in_regime"],
             cockpit["equity_total"],
             f"{cockpit['drawdown_pct']}" if cockpit['drawdown_pct'] is not None else "—")


def write_status(
    status: str,
    last_data_date: str | None = None,
    errors: list[str] | None = None,
    log_url: str | None = None,
    next_run_at: str | None = None,
) -> None:
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
