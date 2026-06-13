#!/usr/bin/env python3
"""
Wrapper ligero: ejecuta SOLO las fases de datos del weekly_engine
y emite signals_auto.json (USA) y signals_auto_eu.json (Europa)
con el esquema v1.0.

NO instancia PortfolioEngine ni corre backtest.
Uso:
    python emit_signals_auto.py             # normal (USA + EU)
    python emit_signals_auto.py --refresh   # forzar re-descarga de datos
    python emit_signals_auto.py --usa-only  # salta Europa
    python emit_signals_auto.py --eu-only   # salta USA
"""
from __future__ import annotations

import logging
import sys
import traceback

import numpy as np

from weekly_engine import (
    ATR_FULL,
    ATR_HALF,
    CACHE_FILE,
    CACHE_FILE_EU,
    MOMENTUM_PERIOD,
    WMA_PERIOD,
    calc_mansfield,
    calc_wma,
    classify_entries,
    compute_cfi,
    compute_market_timing,
    compute_stock_data,
    compute_subindustry_indices,
    get_sp500_tickers,
    get_stoxx600_tickers,
    load_data,
)
from data_layer.config import BASE_DIR, SIGNALS_EU_IN, SIGNALS_IN
from data_layer.signals_emitter import write_signals_auto

import json as _json

log = logging.getLogger(__name__)


# =====================================================================
# Top subindustrias por Mansfield (con delta 1w → aceleración)
# =====================================================================
def _dump_top_subindustries(
    *,
    subind_indices: dict,
    spy_close,
    subind_map: dict,
    region_label: str,
    top_n: int = 20,
) -> None:
    """
    Para cada subindustria con al menos 3 stocks, calcula Mansfield RS del índice
    sintético vs el benchmark regional (spy_close) y el delta 1w (aceleración).
    Dump a subindustries_auto.json (USA) / subindustries_auto_eu.json (EUROPE).
    """
    import numpy as np

    # count stocks per subindustry
    counts: dict[str, int] = {}
    for _sym, si in subind_map.items():
        if si:
            counts[si] = counts.get(si, 0) + 1

    rows: list[dict] = []
    for name, series in subind_indices.items():
        if series is None or len(series) < 60:
            continue
        n_stocks = counts.get(name, 0)
        if n_stocks < 3:
            continue
        try:
            ms_series = calc_mansfield(series, spy_close, ma_period=52)
        except Exception:
            continue
        ms_series = ms_series.dropna()
        if len(ms_series) < 2:
            continue
        ms_now = float(ms_series.iloc[-1])
        ms_prev = float(ms_series.iloc[-2])
        if np.isnan(ms_now):
            continue
        rows.append({
            "name":         name,
            "mansfield":    round(ms_now, 2),
            "mansfield_1w": round(ms_prev, 2) if not np.isnan(ms_prev) else None,
            "delta_1w":     round(ms_now - ms_prev, 2) if not np.isnan(ms_prev) else None,
            "accelerating": bool(ms_now > ms_prev) if not np.isnan(ms_prev) else None,
            "n_stocks":     n_stocks,
        })

    rows.sort(key=lambda r: r["mansfield"], reverse=True)
    top = rows[:top_n]

    fname = "subindustries_auto.json" if region_label == "USA" else "subindustries_auto_eu.json"
    out_path = BASE_DIR / fname
    doc = {
        "region":   region_label,
        "top":      top,
        "n_total":  len(rows),
    }
    with out_path.open("w", encoding="utf-8") as f:
        _json.dump(doc, f, indent=2, ensure_ascii=False)
    print(f"  top subindustrias ({region_label}): {len(top)} / {len(rows)} -> {out_path.name}")


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
# Región genérica
# =====================================================================
def _run_region(
    *,
    region_label: str,                  # "USA" / "EUROPE"
    get_tickers_fn,
    anchor_symbol: str,                 # "SPY" / "^STOXX"
    index_symbol: str,                  # "QQQ" / "^STOXX"
    extra_tickers: list[str],
    cache_file: str,
    out_path,
    source_label: str,
    force_refresh: bool,
) -> int:
    """
    Ejecuta pasos 1-6 del engine para una región y escribe su signals_auto*.json.
    Retorna el número de señales emitidas.
    """
    print(f"\n{'=' * 60}")
    print(f"  REGION: {region_label}")
    print(f"{'=' * 60}")

    # --- Pasos 1-6 del engine (sin backtest) -------------------------
    print(f"\n[1/6] Universo {region_label}...")
    universe, subind_map = get_tickers_fn()
    if not universe:
        print(f"[ERROR] Universo {region_label} vacío — salto.")
        return 0

    print(f"\n[2/6] Datos semanales {region_label}...")
    all_data = load_data(
        universe,
        force_refresh=force_refresh,
        cache_file=cache_file,
        extra_tickers=extra_tickers,
    )
    if anchor_symbol not in all_data:
        print(f"[WARN] Sin datos de anchor {anchor_symbol} — intentando continuar "
              f"con el mejor ticker disponible...")

    # Determinar anchor real (puede que Yahoo no tenga ^STOXX con histórico;
    # el engine ya hace fallback internamente en load_data)
    anchor_data = all_data.get(anchor_symbol)
    if anchor_data is None:
        if not all_data:
            print(f"[ERROR] Sin datos para {region_label} — abortando región.")
            return 0
        anchor_sym_used = max(all_data.keys(), key=lambda t: len(all_data[t].index))
        anchor_data = all_data[anchor_sym_used]
        print(f"  [WARN] usando {anchor_sym_used} como anchor de fechas")
    else:
        anchor_sym_used = anchor_symbol

    spy_close = anchor_data["Close"]
    spy_dates = anchor_data.index

    print(f"\n[3/6] Indices sub-industria {region_label}...")
    subind_indices = compute_subindustry_indices(all_data, subind_map, spy_dates)

    # --- Dump top subindustrias por Mansfield (con aceleración 1w) ---
    try:
        _dump_top_subindustries(
            subind_indices=subind_indices,
            spy_close=spy_close,
            subind_map=subind_map,
            region_label=region_label,
        )
    except Exception as _e:
        print(f"  [WARN] no pude dumpear top subindustrias: {_e}")

    print(f"\n[4/6] Market Timing {region_label}...")
    _mt_verde, mt_details = compute_market_timing(
        all_data, universe,
        anchor_symbol=anchor_sym_used,
        index_symbol=index_symbol,
    )

    print(f"\n[5/6] Indicadores {region_label}...")
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

    # Fecha de datos — necesaria antes del paso CFI
    last_date = spy_dates[-1]
    date_str  = str(last_date)[:10]
    print(f"\n  Fecha de datos: {date_str}")

    # --- [5.5] CFI cross-sectional (V19: 70% Mansfield + 30% CFI rank) ---
    print(f"\n[5.5/6] CFI 3.0 cross-sectional {region_label}...")
    _cfi_data: dict[str, dict] = {}
    _cfi_skip = 0
    for sym in stock_ind:
        if sym not in all_data:
            continue
        try:
            d = all_data[sym]
            if 'Volume' not in d.columns:
                _cfi_skip += 1
                continue
            _c = d['Close'].reindex(spy_dates, method='ffill')
            _v = d['Volume'].reindex(spy_dates, method='ffill')
            _cfi_s = compute_cfi(_c, _v, period=52)
            _cfi_s = _cfi_s.reindex(spy_dates, method='ffill')
            if last_date not in _cfi_s.index:
                _cfi_skip += 1
                continue
            _iloc = _cfi_s.index.get_loc(last_date)
            _now  = float(_cfi_s.iloc[_iloc])
            _4w   = float(_cfi_s.iloc[_iloc - 4]) if _iloc >= 4 else np.nan
            if np.isnan(_now):
                _cfi_skip += 1
                continue
            _cfi_data[sym] = {
                'cfi2':    _now,
                'cfi2_4w': _4w if not np.isnan(_4w) else None,
            }
        except Exception:
            _cfi_skip += 1
    print(f"  CFI calculado: {len(_cfi_data)} acciones (skip {_cfi_skip})")

    # Mansfield cross-sectional percentile at last_date
    _ms_pool: list[float] = []
    for sym, ind in stock_ind.items():
        if last_date in ind.index:
            _v = ind.loc[last_date, 'mansfield']
            if not np.isnan(_v):
                _ms_pool.append(float(_v))
    _ms_pool_sorted = sorted(_ms_pool)

    _cfi_pool: list[float] = [d['cfi2'] for d in _cfi_data.values()]
    _cfi_pool_sorted = sorted(_cfi_pool)

    def _pct_rank(val: float, pool_sorted: list) -> float:
        if not pool_sorted or np.isnan(val):
            return 50.0
        import bisect
        pos = bisect.bisect_left(pool_sorted, val)
        return round(pos / len(pool_sorted) * 100.0, 1)

    print(f"\n[6/6] Clasificando entradas {region_label}...")
    signals_df = classify_entries(stock_ind)

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
    qqq_data = all_data.get(index_symbol)
    if qqq_data is not None:
        qqq_s = qqq_data["Close"].reindex(spy_dates, method="ffill")
        qqq_wma = calc_wma(qqq_s, WMA_PERIOD)
        if last_date in qqq_s.index:
            qqq_close_val = round(float(qqq_s.loc[last_date]), 2)
        if last_date in qqq_wma.index and not np.isnan(qqq_wma.loc[last_date]):
            qqq_wma30_val = round(float(qqq_wma.loc[last_date]), 2)

    regime = _build_regime(nhnl_ok, qqq_ok)
    summary = f"NH-NL {'OK' if nhnl_ok else 'FALLO'} | {index_symbol}>SMA30 {'OK' if qqq_ok else 'FALLO'}"

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

    # --- Extraer señales de la vela semanal cerrada (V19) -------------
    # Transicion exacta a core=4 en last_date.
    # Filtros adicionales: vol_ok, dist_ATR <= 2.5, sizing siempre 100%.
    signal_list: list[dict] = []

    for sym, df in signals_df.items():
        if last_date not in df.index:
            continue
        row = df.loc[last_date]
        sig_type = row.get("signal_type", "")
        if sig_type not in ("FIRST_GEN", "CONTINUATION"):
            continue
        core = int(row.get("core_score", 0))
        if core != 4:
            continue

        # Volume filter (V19)
        vol_ok = bool(row.get("vol_ok", True))
        if not vol_ok:
            continue

        # ATR distance filter — hard cutoff 2.5 (V19)
        dist_atr = row["dist_atr"]
        if np.isnan(dist_atr) or dist_atr < 0 or dist_atr > ATR_HALF:
            continue

        # V19: always 100% sizing (no partial)
        sizing = "100%"

        ms = row["mansfield"]
        if np.isnan(ms):
            ms = None

        close_px = float(row["close"])
        wma_val  = float(row["wma"])
        stop_val = float(row["stop_wma"])

        subind_ms = row.get("subind_ms")
        if subind_ms is not None and np.isnan(subind_ms):
            subind_ms = None

        # Index position
        idx_pos = df.index.get_loc(last_date)

        # Mansfield slope 4w
        ms_slope_4w = None
        if idx_pos >= 4:
            ms_prev4 = df["mansfield"].iloc[idx_pos - 4]
            if ms is not None and not np.isnan(ms_prev4):
                ms_slope_4w = round(float(ms) - float(ms_prev4), 2)

        # Momentum ROC
        mom = None
        if idx_pos >= MOMENTUM_PERIOD:
            prev_close = float(df["close"].iloc[idx_pos - MOMENTUM_PERIOD])
            if prev_close > 0:
                mom = round(((close_px / prev_close) - 1) * 100, 2)

        # Distancia al SMA30 en %
        dist_wma_pct = round(((close_px - wma_val) / wma_val) * 100, 2) if wma_val > 0 else None

        # --- CFI fields (V19) ---
        _cfi_entry  = _cfi_data.get(sym)
        cfi2_val     = _cfi_entry['cfi2']         if _cfi_entry else None
        cfi2_4w_val  = _cfi_entry['cfi2_4w']      if _cfi_entry else None
        cfi_slope_4w = (round(cfi2_val - cfi2_4w_val, 2)
                        if cfi2_val is not None and cfi2_4w_val is not None else None)

        # Cross-sectional ranks
        ms_pct   = _pct_rank(float(ms), _ms_pool_sorted)   if ms  is not None else 50.0
        cfi_pct  = _pct_rank(cfi2_val,  _cfi_pool_sorted)  if cfi2_val is not None else None

        # Composite rank: 70% Mansfield percentile + 30% CFI rank (V19)
        composite_score = (round(0.7 * ms_pct + 0.3 * cfi_pct, 1)
                           if cfi_pct is not None
                           else round(ms_pct, 1))

        # --- WIR exit monitoring fields ---
        # dist_risk: ATR normalised 0-100 (ATR=2.5 -> 100)
        wir_dist_risk = round(dist_atr * 40.0, 1)

        # Sub-industry state (informativo para seguimiento WIR)
        _subind_ms_4w_val = None
        if subind_ms is not None and idx_pos >= 4:
            _sub_prev = df["subind_ms"].iloc[idx_pos - 4] if "subind_ms" in df.columns else np.nan
            if not np.isnan(_sub_prev):
                _subind_ms_4w_val = float(_sub_prev)

        if subind_ms is None:
            wir_subind_state = None
        elif subind_ms > 0:
            if _subind_ms_4w_val is None or subind_ms >= _subind_ms_4w_val:
                wir_subind_state = "ADVANCING"
            else:
                wir_subind_state = "WEAKENING"
        else:
            if _subind_ms_4w_val is not None and subind_ms > _subind_ms_4w_val:
                wir_subind_state = "BASING"
            else:
                wir_subind_state = "DECLINING"

        setup = f"WEINSTEIN_{sig_type}"

        signal_list.append({
            # Core fields
            "ticker":               sym,
            "action":               "BUY",
            "setup":                setup,
            "timeframe":            "W",
            "price":                round(close_px, 2),
            "date":                 date_str,
            "score":                core,
            "stop_reference":       round(stop_val, 2),
            "distance_wma30_pct":   dist_wma_pct,
            "mansfield":            round(float(ms), 2) if ms is not None else None,
            "ms_slope_4w":          ms_slope_4w,
            "momentum":             mom,
            "industry_mansfield":   None,
            "subindustry_mansfield": round(float(subind_ms), 2) if subind_ms is not None else None,
            "notes":                f"ATR dist {dist_atr:.2f}",
            # Sizing / distance
            "sizing":               sizing,
            "atr_distance":         round(float(dist_atr), 2),
            # Identifiers
            "name":                 sym,
            "subindustry":          subind_map.get(sym),
            "region":               region_label,
            # V19: CFI 3.0 fields
            "cfi2":                 round(cfi2_val, 2) if cfi2_val is not None else None,
            "cfi_rank":             round(cfi_pct, 1) if cfi_pct is not None else None,
            "cfi_slope_4w":         cfi_slope_4w,
            # V19: composite ranking
            "ms_rank":              round(ms_pct, 1),
            "composite_score":      composite_score,
            # WIR exit monitoring
            "wir_dist_risk":        wir_dist_risk,
            "wir_subind_state":     wir_subind_state,
        })

    # Ordenar por composite 70/30 descendente (V19)
    signal_list.sort(key=lambda s: s.get("composite_score") or -999, reverse=True)

    # --- Escribir JSON ------------------------------------------------
    path = write_signals_auto(
        signals=signal_list,
        market_status=market_status,
        source=source_label,
        path=out_path,
    )

    print(f"\n[OK] {path.name}: {len(signal_list)} signals, regime={regime}")
    return len(signal_list)


# =====================================================================
# Entry point
# =====================================================================
def run() -> int:
    """Retorna 0 si OK, 1 si error crítico."""
    force_refresh = "--refresh" in sys.argv
    usa_only = "--usa-only" in sys.argv
    eu_only  = "--eu-only"  in sys.argv

    print("=" * 60)
    print("EMIT SIGNALS AUTO — Weinstein Weekly Engine (USA + EUROPE)")
    print("=" * 60)

    usa_ok = False
    eu_ok  = False
    n_usa = 0
    n_eu  = 0

    # ── USA ──────────────────────────────────────────────────────────
    if not eu_only:
        try:
            n_usa = _run_region(
                region_label="USA",
                get_tickers_fn=get_sp500_tickers,
                anchor_symbol="SPY",
                index_symbol="QQQ",
                extra_tickers=["SPY", "QQQ"],
                cache_file=CACHE_FILE,
                out_path=SIGNALS_IN,
                source_label="weekly_engine",
                force_refresh=force_refresh,
            )
            usa_ok = True
        except Exception as e:
            print(f"\n[ERROR] USA falló: {e}")
            traceback.print_exc()
            usa_ok = False

    # ── EUROPE ───────────────────────────────────────────────────────
    if not usa_only:
        try:
            n_eu = _run_region(
                region_label="EUROPE",
                get_tickers_fn=get_stoxx600_tickers,
                anchor_symbol="^STOXX",
                index_symbol="^STOXX",
                # ^STOXX para timing; SPY como extra para compat de anchor si falla
                extra_tickers=["^STOXX", "^GDAXI", "SPY"],
                cache_file=CACHE_FILE_EU,
                out_path=SIGNALS_EU_IN,
                source_label="weekly_engine_eu",
                force_refresh=force_refresh,
            )
            eu_ok = True
        except Exception as e:
            # Europa NO debe romper USA: logueamos y seguimos
            print(f"\n[WARN] EUROPE falló (no crítico, USA sigue): {e}")
            traceback.print_exc()
            eu_ok = False

    print("\n" + "=" * 60)
    print(f"RESUMEN: USA {'OK' if usa_ok else 'FALLO'} ({n_usa} sig.), "
          f"EU {'OK' if eu_ok else 'FALLO'} ({n_eu} sig.)")
    print("=" * 60)

    # ── TPS Scanner (diario: triggers + watchlist) ────────────────────
    if not eu_only:   # solo cuando corremos USA (tiene weekly cache)
        try:
            from tps_scanner import run_scanner
            run_scanner()
        except Exception as _tps_e:
            print(f"\n[WARN] TPS Scanner fallo (no critico): {_tps_e}")

    # Éxito si al menos la región principal (USA) está OK cuando se ejecuta
    if not eu_only and not usa_ok:
        return 1
    if eu_only and not eu_ok:
        return 1
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    sys.exit(run())
