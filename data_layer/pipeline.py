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

from .config import MAX_POSITIONS, PORTFOLIO_IN, SIGNALS_EU_IN, SIGNALS_IN
from .ledger import append_event, ensure_migration, load_history_trades
from .output import write_data_json, write_status
from .portfolio import ManualPosition, derive_actions, enrich_portfolio

log = logging.getLogger(__name__)


# =====================================================================
# Readers de inputs
# =====================================================================
def _read_manual_portfolio() -> tuple[float, list[ManualPosition], dict]:
    """
    Lee portfolio_manual.json y devuelve (capital, positions, raw_doc).
    raw_doc se devuelve para que el pipeline pueda aplicar migración o
    Monday-open fills y escribirlo de vuelta antes de enriquecer.
    """
    if not PORTFOLIO_IN.exists():
        raise FileNotFoundError(f"Falta {PORTFOLIO_IN}. Crea portfolio_manual.json.")
    with PORTFOLIO_IN.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    # Compat: soporta lista plana o dict con "positions". Si es lista plana, la
    # normalizamos a dict {capital_total, positions} para poder mutarla.
    if isinstance(raw, list):
        raw = {"capital_total": 100_000.0, "positions": raw}

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
            trade_id=p.get("trade_id"),
            entry_price_source=p.get("entry_price_source"),
        ))
    return capital, positions, raw


def _write_manual_portfolio(doc: dict) -> None:
    """Reescribe portfolio_manual.json atómicamente (tmp + replace)."""
    import os as _os
    import tempfile as _tf
    parent = PORTFOLIO_IN.parent
    fd, tmp_path = _tf.mkstemp(prefix=".pm_", suffix=".tmp", dir=str(parent))
    try:
        with _os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2, ensure_ascii=False)
        _os.replace(tmp_path, PORTFOLIO_IN)
    except Exception:
        try:
            _os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _process_monday_open_fills(raw_doc: dict) -> bool:
    """
    Para cada posición con pending_monday_open=True:
      1. Intenta tomar el Open de yfinance del primer día hábil ≥ fecha_compra.
      2. Si lo consigue: actualiza precio_compra, elimina el flag, emite
         evento MONDAY_OPEN_FILL con el precio real.
      3. Si falla (sin datos aún, ticker sin cotización ese día, etc.): deja
         el flag para reintentar en la próxima ejecución.

    Devuelve True si mutó el doc (caller debe persistir).
    """
    try:
        import yfinance as yf
    except ImportError:
        log.info("yfinance no disponible: no proceso pending_monday_open_fills")
        return False

    import pandas as pd

    positions = raw_doc.get("positions", [])
    mutated = False

    for pos in positions:
        if not pos.get("pending_monday_open"):
            continue
        ticker = pos.get("ticker")
        fecha = pos.get("fecha_compra")
        trade_id = pos.get("trade_id")
        if not ticker or not fecha:
            continue

        try:
            start = pd.Timestamp(fecha)
        except Exception as e:
            log.warning("%s: fecha_compra inválida (%s): %s", ticker, fecha, e)
            continue
        end = start + pd.Timedelta(days=7)

        try:
            hist = yf.Ticker(ticker).history(
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                interval="1d",
                auto_adjust=False,
            )
        except Exception as e:
            log.warning("%s: fallo yfinance Monday-open: %s", ticker, e)
            continue

        if hist is None or hist.empty or "Open" not in hist.columns:
            log.info("%s: aún no hay apertura real para fecha_compra=%s — reintento luego",
                     ticker, fecha)
            continue

        first_row = hist.iloc[0]
        monday_open = first_row.get("Open")
        monday_date = first_row.name.strftime("%Y-%m-%d") if hasattr(first_row.name, "strftime") else fecha
        if monday_open is None or not (float(monday_open) > 0):
            log.info("%s: open inválido (%s) — reintento luego", ticker, monday_open)
            continue

        monday_open = round(float(monday_open), 4)
        prev_price = float(pos.get("precio_compra") or 0)
        pos["precio_compra"] = monday_open
        pos.pop("pending_monday_open", None)
        pos["entry_price_source"] = "monday_open"
        mutated = True

        # Emitir MONDAY_OPEN_FILL al ledger (size=0 → no afecta realized)
        if trade_id:
            append_event({
                "kind":             "MONDAY_OPEN_FILL",
                "trade_id":         trade_id,
                "ticker":           ticker,
                "date":             monday_date,
                "price":            monday_open,
                "size_pct":         0.0,
                "entry_price_ref":  monday_open,
                "r_realized":       None,
                "pnl_realized_pct": None,
                "price_source":     "monday_open",
                "reason":           f"Fill real del lunes (tentative={prev_price}, fill={monday_open})",
                "source":           "pipeline",
            })
        log.info("Monday-open fill: %s %s → %.4f (antes %.4f)",
                 ticker, monday_date, monday_open, prev_price)

    return mutated


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


def _read_signals_eu() -> dict | None:
    """
    Lee signals_auto_eu.json. Si no existe o falla, devuelve None.
    Europa es opcional: el dashboard debe funcionar aunque no haya datos EU.
    """
    if not SIGNALS_EU_IN.exists():
        log.info("signals_auto_eu.json no encontrado — dashboard solo mostrará USA")
        return None
    try:
        with SIGNALS_EU_IN.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning("signals_auto_eu.json inválido: %s — continúo solo con USA", e)
        return None


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


def _signal_to_buy_row(s: dict) -> dict:
    """Normaliza una señal BUY (USA o EU) a la estructura que consume el dashboard."""
    entry = s["price"]
    stop = s["stop_reference"]
    r_val = (entry - stop) if (entry and stop) else 0
    sig_type = _setup_to_type(s.get("setup", ""))
    ms_val = s.get("mansfield")
    sms_val = s.get("subindustry_mansfield")
    ms_str = f"{ms_val:+.2f}" if ms_val is not None else "N/A"
    sms_str = f"{sms_val:+.2f}" if sms_val is not None else "N/A"
    region = s.get("region") or ""

    # WMA30 reconstruida desde el stop (stop = WMA30 * 0.97)
    wma30 = round(stop / 0.97, 2) if stop else None
    dist_wma30 = s.get("distance_wma30_pct")
    if dist_wma30 is None and wma30 and entry:
        dist_wma30 = round((entry / wma30 - 1) * 100, 2)

    return {
        "symbol":              s["ticker"],
        "name":                s.get("name", s["ticker"]),
        "country":             s.get("country"),
        "country_flag":        s.get("country_flag"),
        "type":                sig_type,
        "entry_price":         entry,
        "wma30":               wma30,
        "distance_wma30_pct":  dist_wma30,
        "stop_price":          stop,
        "stop_distance_pct":   round((stop / entry - 1) * 100, 2) if entry else None,
        "atr_distance":        s.get("atr_distance"),
        "sizing":              s.get("sizing", "100%"),
        "level_2r":            round(entry + 2 * r_val, 2) if entry else None,
        "level_3r":            round(entry + 3 * r_val, 2) if entry else None,
        "mansfield":           ms_val,
        "subindustry_mansfield": sms_val,
        "subindustry":         s.get("subindustry"),
        "region":              region,
        "rationale":           f"{sig_type} · MS {ms_str} · Sub-MS {sms_str}" + (f" · {region}" if region else ""),
    }


def _build_buys_queue(
    signals_usa: dict,
    signals_eu: dict | None,
    portfolio_enriched: dict,
    max_positions: int = MAX_POSITIONS,
) -> tuple[list[dict], dict]:
    """
    Construye la cola de compras priorizada hasta rellenar los huecos libres.

    Criterios (sort key):
      1. Tipo: FIRST_GEN antes que CONTINUATION
      2. Mansfield RS DESC  (mayor fuerza relativa primero)
      3. Sub-industry Mansfield DESC (desempate)

    Huecos libres = max_positions − (n_actual − n_stops_pendientes)
    SELL_2R / SELL_3R NO liberan hueco (solo reducen tamaño).

    Se excluyen tickers ya en cartera que NO se estén vendiendo al 100%.

    Devuelve (lista_compras, meta) donde meta tiene info para el dashboard.
    """
    # 1) Combinar pool USA + EU
    pool_raw: list[dict] = []
    for doc in (signals_usa, signals_eu):
        if not doc:
            continue
        for s in doc.get("signals", []):
            if s.get("action") == "BUY":
                pool_raw.append(s)

    # 2) Excluir tickers ya en cartera (salvo los que se venden al 100%)
    in_portfolio_holding: set[str] = set()
    n_stops = 0
    for pos in portfolio_enriched.get("positions", []):
        if pos.get("status") == "STOP_ALERT":
            n_stops += 1
        else:
            in_portfolio_holding.add(pos["symbol"])

    pool_raw = [s for s in pool_raw if s["ticker"] not in in_portfolio_holding]

    # 3) Calcular huecos
    n_pos = portfolio_enriched.get("n_positions", 0)
    slots_libres = max(0, max_positions - (n_pos - n_stops))

    meta = {
        "n_positions":  n_pos,
        "n_stops":      n_stops,
        "max_positions": max_positions,
        "slots_libres": slots_libres,
        "n_candidates": len(pool_raw),
    }

    if slots_libres == 0 or not pool_raw:
        return [], meta

    # 4) Ordenar por (FG primero, Mansfield DESC, Sub-MS DESC)
    def _sort_key(s: dict):
        fg_rank = 0 if "FIRST_GEN" in s.get("setup", "") else 1
        ms = s.get("mansfield")
        sms = s.get("subindustry_mansfield")
        # None → al final
        ms_k  = -ms  if ms  is not None else 9999
        sms_k = -sms if sms is not None else 9999
        return (fg_rank, ms_k, sms_k)

    pool_raw.sort(key=_sort_key)

    # 5) Coger los primeros slots_libres y normalizar
    chosen = pool_raw[:slots_libres]
    return [_signal_to_buy_row(s) for s in chosen], meta


def _signals_region_block(signals_doc: dict) -> dict:
    """Convierte un signals_auto*.json (v1.0) → {first_gen, continuation}."""
    sigs = [s for s in signals_doc.get("signals", []) if s.get("action") == "BUY"]

    def _row(s):
        stop = s.get("stop_reference")
        price = s["price"]
        wma30 = round(stop / 0.97, 2) if stop else None
        dist_wma = s.get("distance_wma30_pct")
        if dist_wma is None and wma30 and price:
            dist_wma = round((price / wma30 - 1) * 100, 2)
        return {
            "symbol":        s["ticker"],
            "name":          s.get("name", s["ticker"]),
            "country":       s.get("country"),
            "country_flag":  s.get("country_flag"),
            "type":          _setup_to_type(s.get("setup", "")),
            "current_price": price,
            "wma30":         wma30,
            "distance_wma30_pct": dist_wma,
            "stop_price":    stop,
            "mansfield":     s.get("mansfield"),
            "ms_slope_4w":   s.get("ms_slope_4w"),
            "subind_ms":     s.get("subindustry_mansfield"),
            "atr_pct":       s.get("atr_distance"),
            "atr_distance":  s.get("atr_distance"),
            "sizing":        s.get("sizing", "100%"),
            "subindustry":   s.get("subindustry"),
            "core_score":    s.get("score", 4),
            "region":        s.get("region"),
            # V19: CFI + composite ranking
            "cfi2":            s.get("cfi2"),
            "cfi_rank":        s.get("cfi_rank"),
            "cfi_slope_4w":    s.get("cfi_slope_4w"),
            "ms_rank":         s.get("ms_rank"),
            "composite_score": s.get("composite_score"),
            # WIR exit monitoring
            "wir_dist_risk":    s.get("wir_dist_risk"),
            "wir_subind_state": s.get("wir_subind_state"),
        }

    return {
        "first_gen":    [_row(s) for s in sigs if "FIRST_GEN" in s.get("setup", "")],
        "continuation": [_row(s) for s in sigs if "CONTINUATION" in s.get("setup", "")],
    }


def _signals_to_panel(signals_doc_usa: dict, signals_doc_eu: dict | None) -> dict:
    """
    Construye el bloque signals de data.json con soporte bi-regional.

    Estructura:
        signals = {
          first_gen:    [...USA],           # compat con dashboard antiguo
          continuation: [...USA],
          usa:     { first_gen, continuation },
          europe:  { first_gen, continuation }   # vacío si no hay datos EU
        }
    """
    usa_block = _signals_region_block(signals_doc_usa)
    eu_block  = _signals_region_block(signals_doc_eu) if signals_doc_eu else {
        "first_gen": [], "continuation": [],
    }
    return {
        "first_gen":    usa_block["first_gen"],      # back-compat
        "continuation": usa_block["continuation"],   # back-compat
        "usa":          usa_block,
        "europe":       eu_block,
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
        capital, manual, raw_doc = _read_manual_portfolio()
        log.info("  cartera manual: %d posiciones, capital $%.0f", len(manual), capital)

        # 1) Migración: asigna trade_id retroactivo a posiciones pre-ledger
        mutated_mig = ensure_migration(raw_doc)
        # 2) Monday-open fills: si hay posiciones con pending_monday_open, intenta cubrir
        mutated_mon = _process_monday_open_fills(raw_doc)
        if mutated_mig or mutated_mon:
            _write_manual_portfolio(raw_doc)
            # Recargar ManualPosition para que recojan los nuevos trade_id / precio
            capital, manual, raw_doc = _read_manual_portfolio()
            log.info("  portfolio_manual.json actualizado (migration=%s, monday_fill=%s)",
                     mutated_mig, mutated_mon)

        signals_doc = _read_signals()
        signals_doc_eu = _read_signals_eu()
        ms = signals_doc.get("market_status", {})
        # Traducir regime (GREEN/AMBER/RED) → VERDE/NEUTRO/ROJO para dashboard
        mt_ok   = ms.get("mt_ok", False)
        _nhnl   = bool(ms.get("nh_nl_ok", False))
        _qqq    = bool(ms.get("qqq_above_wma_ok", False))
        if mt_ok or ms.get("regime") == "GREEN":
            mt_status = "VERDE"
        elif _nhnl or _qqq:
            mt_status = "NEUTRO"
        else:
            mt_status = "ROJO"
        log.info("  signals_auto: %d signals, regime=%s (MT=%s)",
                 len(signals_doc.get("signals", [])), ms.get("regime", "?"), mt_status)
        if signals_doc_eu:
            log.info("  signals_auto_eu: %d signals, regime=%s",
                     len(signals_doc_eu.get("signals", [])),
                     signals_doc_eu.get("market_status", {}).get("regime", "?"))

        # Enriquecer cartera
        portfolio = enrich_portfolio(manual, capital)

        # Completar subindustria de cada posición desde los universos
        # (sp500_tickers + stoxx600_tickers). Si no hay match, queda None.
        _enrich_positions_subindustry(portfolio.get("positions", []))

        # Cola de compras: USA + EU combinadas, priorizando FG > Mansfield > Sub-MS,
        # rellenando huecos libres de la cartera hasta MAX_POSITIONS.
        buys, buys_meta = _build_buys_queue(signals_doc, signals_doc_eu, portfolio)
        log.info(
            "  buys queue: %d candidatos USA+EU, %d posiciones, %d stops, %d huecos → %d compras",
            buys_meta["n_candidates"], buys_meta["n_positions"],
            buys_meta["n_stops"], buys_meta["slots_libres"], len(buys),
        )
        actions = derive_actions(portfolio, buys, mt_status)
        # Guardar meta para que el dashboard pueda explicar "X/10 huecos libres"
        actions["slots_meta"] = buys_meta

        signals_panel = _signals_to_panel(signals_doc, signals_doc_eu)
        monitor = _monitor_from_signals(signals_doc)

        # data_date: tomar del primer signal o de generated_at
        sigs_list = signals_doc.get("signals", [])
        data_date = sigs_list[0].get("date", "") if sigs_list else ""
        if not data_date:
            gen = signals_doc.get("generated_at", "")
            data_date = gen[:10] if gen else datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Trades cerrados (history) → últimos N para mostrar en dashboard
        closed_trades = load_history_trades()[:25]
        portfolio["closed_trades"] = closed_trades

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
    GREEN  → VERDE  (ambas condiciones OK — comprar con normalidad)
    AMBER  → NEUTRO (una condición OK — señales visibles, decisión manual)
    RED    → ROJO   (ninguna condición OK — máxima cautela)
    """
    regime = ms.get("regime", "RED")
    mt_ok = ms.get("mt_ok", regime == "GREEN")
    nhnl_ok = bool(ms.get("nh_nl_ok", False))
    qqq_ok  = bool(ms.get("qqq_above_wma_ok", False))

    if mt_ok or regime == "GREEN":
        status = "VERDE"
    elif nhnl_ok or qqq_ok:   # AMBER: al menos una condición pasa
        status = "NEUTRO"
    else:
        status = "ROJO"

    return {
        "status":            status,
        "nh_nl_ok":          nhnl_ok,
        "qqq_above_wma_ok":  qqq_ok,
        "nh_nl_value":       ms.get("nh_nl_value"),
        "qqq_close":         ms.get("qqq_close"),
        "qqq_wma30":         ms.get("qqq_wma30"),
        "summary":           ms.get("summary", ""),
        "indices":           _fetch_index_performance(),
        "top_subindustries": _load_top_subindustries(),
    }


def _load_top_subindustries(top_n: int = 10) -> dict:
    """
    Carga las top subindustrias por Mansfield RS desde los dumps que
    emit_signals_auto genera. Devuelve {usa: [...], eu: [...]} con top_n cada una.
    Si no existe algún fichero todavía, esa lista queda vacía.
    """
    from .config import BASE_DIR as _BASE
    out: dict[str, list[dict]] = {"usa": [], "eu": []}
    for fname, key, region in [("subindustries_auto.json",    "usa", "USA"),
                               ("subindustries_auto_eu.json", "eu",  "EU")]:
        p = _BASE / fname
        if not p.exists():
            continue
        try:
            with p.open("r", encoding="utf-8") as f:
                doc = json.load(f)
        except Exception as e:
            log.debug("no pude leer %s: %s", p.name, e)
            continue
        rows = list(doc.get("top", []))
        rows.sort(key=lambda r: (r.get("mansfield") or -999), reverse=True)
        for r in rows[:top_n]:
            item = dict(r)
            item["region"] = region
            out[key].append(item)
    return out


_SUBIND_MAP_CACHE: dict | None = None


def _load_subindustry_map() -> dict:
    """Carga y cachea el map ticker→subindustria de los 2 universos."""
    global _SUBIND_MAP_CACHE
    if _SUBIND_MAP_CACHE is not None:
        return _SUBIND_MAP_CACHE
    out: dict = {}
    try:
        from weekly_engine import get_sp500_tickers
        _, sub_map = get_sp500_tickers()
        out.update(sub_map)
    except Exception as e:
        log.debug("no cargo sp500 subind map: %s", e)
    try:
        from stoxx600_tickers import get_stoxx600_tickers
        _, sub_map = get_stoxx600_tickers()
        out.update(sub_map)
    except Exception as e:
        log.debug("no cargo stoxx600 subind map: %s", e)
    _SUBIND_MAP_CACHE = out
    return out


def _enrich_positions_subindustry(positions: list[dict]) -> None:
    """Muta cada posición añadiendo subindustry si la encuentra en los universos."""
    if not positions:
        return
    sub_map = _load_subindustry_map()
    for pos in positions:
        if pos.get("subindustry"):
            continue
        sym = pos.get("symbol")
        if sym and sym in sub_map:
            pos["subindustry"] = sub_map[sym]


def _fetch_index_performance() -> list[dict]:
    """
    Performance semanal de los 3 índices de referencia:
    SPY (S&P 500), QQQ (Nasdaq 100), ^STOXX (STOXX Europe 600).

    Devuelve lista de dicts con close actual y cambio % respecto al cierre
    semanal anterior. Si yfinance falla, devuelve lista vacía.
    """
    try:
        import yfinance as yf
    except ImportError:
        return []

    specs = [
        ("SPY",    "S&P 500",       "🇺🇸"),
        ("QQQ",    "Nasdaq 100",    "🇺🇸"),
        ("^STOXX", "STOXX 600",     "🇪🇺"),
    ]
    out: list[dict] = []
    for symbol, label, flag in specs:
        try:
            hist = yf.Ticker(symbol).history(period="3mo", interval="1wk")
            if hist is None or len(hist) < 2:
                continue
            # Última fila cerrada + la anterior
            closes = hist["Close"].dropna()
            if len(closes) < 2:
                continue
            last = float(closes.iloc[-1])
            prev = float(closes.iloc[-2])
            change_pct = (last / prev - 1) * 100 if prev > 0 else 0.0
            out.append({
                "symbol":     symbol,
                "label":      label,
                "flag":       flag,
                "close":      round(last, 2),
                "prev_close": round(prev, 2),
                "change_pct": round(change_pct, 2),
            })
        except Exception as e:
            log.warning("No pude fetchear performance de %s: %s", symbol, e)
    return out
