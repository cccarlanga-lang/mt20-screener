#!/usr/bin/env python3
"""
Wrapper ligero: ejecuta SOLO las fases de datos del weekly_engine
y emite signals_auto.json con el esquema v1.0.

NO instancia PortfolioEngine ni corre backtest.
Uso:
    python emit_signals_auto.py             # normal
    python emit_signals_auto.py --refresh   # forzar re-descarga de datos
"""
from __future__ import annotations

import logging
import sys

import numpy as np

from weekly_engine import (
    ATR_FULL,
    ATR_HALF,
    MOMENTUM_PERIOD,
    WMA_PERIOD,
    calc_wma,
    classify_entries,
    compute_market_timing,
    compute_stock_data,
    compute_subindustry_indices,
    get_sp500_tickers,
    load_data,
)
from data_layer.signals_emitter import write_signals_auto

log = logging.getLogger(__name__)


# =====================================================================
# Market Timing → regime
# =====================================================================
def _build_regime(nhnl_ok: bool, qqq_ok: bool) -> str:
    """GREEN si ambos OK, AMBER si uno, RED si ninguno."""
    if nhnl_ok and qqq_ok:
        return "GREEN"
    if nhnl_ok or qqq_ok:
        return "AMBER"
    return "RED"


# =====================================================================
# Entry point
# =====================================================================
def run() -> int:
    """Retorna 0 si OK, 1 si error."""
    force_refresh = "--refresh" in sys.argv

    print("=" * 60)
    print("EMIT SIGNALS AUTO — Weinstein Weekly Engine")
    print("=" * 60)

    # --- Pasos 1-6 del engine (sin backtest) -------------------------
    print("\n[1/6] Universo S&P 500...")
    universe, subind_map = get_sp500_tickers()

    print("\n[2/6] Datos semanales...")
    all_data = load_data(universe, force_refresh=force_refresh)
    if "SPY" not in all_data:
        print("[ERROR] Sin datos de SPY — abortando.")
        return 1
    spy_close = all_data["SPY"]["Close"]
    spy_dates = all_data["SPY"].index

    print("\n[3/6] Indices sub-industria GICS...")
    subind_indices = compute_subindustry_indices(all_data, subind_map, spy_dates)

    print("\n[4/6] Market Timing...")
    mt_verde, mt_details = compute_market_timing(all_data, universe)

    print("\n[5/6] Indicadores...")
    stock_ind = {}
    skipped = 0
    for sym in universe:
        if sym not in all_data:
            skipped += 1
            continue
        subind_name = subind_map.get(sym, "")
        subind_close = subind_indices.get(subind_name, spy_close)
        try:
            stock_ind[sym] = compute_stock_data(all_data[sym], spy_close, subind_close)
        except Exception:
            skipped += 1
    print(f"  {len(stock_ind)} acciones procesadas (skipped {skipped})")

    print("\n[6/6] Clasificando entradas...")
    signals_df = classify_entries(stock_ind)

    # --- Última fecha disponible (cierre de semana) ------------------
    last_date = spy_dates[-1]
    date_str = str(last_date)[:10]
    print(f"\n  Fecha de datos: {date_str}")

    # --- Market Status -----------------------------------------------
    nhnl_ok = False
    qqq_ok = False
    nh_nl_value = None

    if mt_details is not None and last_date in mt_details.index:
        row_mt = mt_details.loc[last_date]
        nhnl_ok = bool(row_mt.get("nhnl_ok", False))
        qqq_ok = bool(row_mt.get("qqq_ok", False))
        nh_nl_value = float(row_mt.get("nhnl", 0))

    qqq_close_val = None
    qqq_wma30_val = None
    qqq_data = all_data.get("QQQ")
    if qqq_data is not None:
        qqq_s = qqq_data["Close"].reindex(spy_dates, method="ffill")
        qqq_wma = calc_wma(qqq_s, WMA_PERIOD)
        if last_date in qqq_s.index:
            qqq_close_val = round(float(qqq_s.loc[last_date]), 2)
        if last_date in qqq_wma.index and not np.isnan(qqq_wma.loc[last_date]):
            qqq_wma30_val = round(float(qqq_wma.loc[last_date]), 2)

    regime = _build_regime(nhnl_ok, qqq_ok)
    summary = f"NH-NL {'OK' if nhnl_ok else 'FALLO'} | QQQ>WMA30 {'OK' if qqq_ok else 'FALLO'}"

    market_status = {
        "mt_ok": nhnl_ok and qqq_ok,
        "regime": regime,
        "summary": summary,
        "nh_nl_ok": nhnl_ok,
        "qqq_above_wma_ok": qqq_ok,
        "nh_nl_value": nh_nl_value,
        "qqq_close": qqq_close_val,
        "qqq_wma30": qqq_wma30_val,
    }

    # --- Extraer señales: semana actual + semana anterior -------------
    # TradingView muestra el estado actual de la señal, no solo la
    # semana exacta de transición. Para no perder entradas válidas de
    # la semana anterior que siguen activas, buscamos la señal en las
    # últimas 2 semanas siempre que el core_score siga siendo 5 hoy.
    signal_list: list[dict] = []
    prev_date = spy_dates[-2] if len(spy_dates) >= 2 else last_date

    for sym, df in signals_df.items():
        if last_date not in df.index:
            continue
        row = df.loc[last_date]

        # core_score debe ser 5 en la semana actual
        core = int(row.get("core_score", 0))
        if core != 5:
            continue

        # Buscar signal_type en semana actual; si no, en semana anterior
        sig_type = row.get("signal_type", "")
        if sig_type not in ("FIRST_GEN", "CONTINUATION"):
            if prev_date in df.index:
                prev_row = df.loc[prev_date]
                if int(prev_row.get("core_score", 0)) == 5:
                    sig_type = prev_row.get("signal_type", "")
        if sig_type not in ("FIRST_GEN", "CONTINUATION"):
            continue

        dist_atr = row["dist_atr"]
        # Solo entradas dentro de 2.0 ATR (tamaño 100%, alineado con TV)
        if np.isnan(dist_atr) or dist_atr < 0 or dist_atr > ATR_FULL:
            continue

        sizing = "100%"
        ms = row["mansfield"]
        if np.isnan(ms):
            ms = None

        close_px = float(row["close"])
        wma_val = float(row["wma"])
        stop_val = float(row["stop_wma"])

        subind_ms = row.get("subind_ms")
        if subind_ms is not None and np.isnan(subind_ms):
            subind_ms = None

        # Momentum: ROC sobre MOMENTUM_PERIOD semanas (%)
        mom = None
        idx_pos = df.index.get_loc(last_date)
        if idx_pos >= MOMENTUM_PERIOD:
            prev_close = float(df["close"].iloc[idx_pos - MOMENTUM_PERIOD])
            if prev_close > 0:
                mom = round(((close_px / prev_close) - 1) * 100, 2)

        # Distancia al WMA30 en %
        dist_wma_pct = round(((close_px - wma_val) / wma_val) * 100, 2) if wma_val > 0 else None

        setup = f"WEINSTEIN_{sig_type}"  # WEINSTEIN_FIRST_GEN / WEINSTEIN_CONTINUATION

        signal_list.append({
            "ticker": sym,
            "action": "BUY",
            "setup": setup,
            "timeframe": "W",
            "price": round(close_px, 2),
            "date": date_str,
            "score": core,
            "stop_reference": round(stop_val, 2),
            "distance_wma30_pct": dist_wma_pct,
            "mansfield": round(float(ms), 2) if ms is not None else None,
            "momentum": mom,
            "industry_mansfield": None,
            "subindustry_mansfield": round(float(subind_ms), 2) if subind_ms is not None else None,
            "notes": f"ATR dist {dist_atr:.1f}",
            # Extras consumidos por el pipeline
            "sizing": sizing,
            "atr_distance": round(float(dist_atr), 2),
            "name": sym,
            "subindustry": subind_map.get(sym),
        })

    # Ordenar por Mansfield RS descendente (misma lógica que el engine)
    signal_list.sort(key=lambda s: s.get("mansfield") or -999, reverse=True)

    # --- Escribir signals_auto.json ----------------------------------
    path = write_signals_auto(
        signals=signal_list,
        market_status=market_status,
        source="weekly_engine",
    )

    print(f"\n{'=' * 60}")
    print(f"[OK] {path.name}: {len(signal_list)} signals, regime={regime}")
    print(f"{'=' * 60}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    sys.exit(run())
