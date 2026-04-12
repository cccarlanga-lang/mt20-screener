#!/usr/bin/env python3
"""
METODO TENDENCIA 2.0 - MOTOR DE CARTERA SEMANAL
=================================================
Engine completo que:
  1. Descarga datos semanales de TODAS las acciones del S&P 500 (Yahoo Finance)
  2. Calcula indicadores: WMA30, Momentum, Mansfield RS (stock + sub-industria GICS), ATR
  3. Clasifica entradas: PRIMERA GENERACION vs CONTINUACION
  4. Semaforo Market Timing: NH-NL + QQQ>WMA30
  5. Gestiona cartera de 10 posiciones equal-weight
  6. Genera reporte semanal: que comprar, que vender, parciales, reemplazos

LOGICA FIRST-GEN vs CONTINUACION:
  - core_score = 5 condiciones (price>WMA, WMA rising, Mom>0, Mansfield>0, SubIndustryMS>0)
  - C5 usa Mansfield RS de GICS Sub-Industry (indice sintetico equal-weight) vs SPY
  - PRIMERA GENERACION: core estuvo en 0 (ninguna condicion) antes de llegar a 5
  - CONTINUACION: core nunca llego a 0, llega a 5 desde 1-4
  - Ambas generan entrada, ordenadas por Mansfield RS (first-gen y continuation)

USO:
  python weekly_engine.py              # Reporte semanal completo
  python weekly_engine.py --backtest   # Simulacion desde 2016 + reporte actual

Ejecutar cada fin de semana para obtener las acciones de la proxima semana.
"""

import yfinance as yf
import pandas as pd
import numpy as np
import os
import pickle
import json
import warnings
import time
import sys
from datetime import datetime, timedelta
from dataclasses import dataclass

warnings.filterwarnings('ignore')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(BASE_DIR, 'sp500_weekly_cache.pkl')
PORTFOLIO_FILE = os.path.join(BASE_DIR, 'portfolio_state.json')
OUTPUT_DIR = os.path.join(BASE_DIR, 'weekly_reports')

DATA_START = '2013-01-01'
BACKTEST_START = '2016-01-04'
INITIAL_CAPITAL = 100_000.0
COMMISSION_PCT = 0.10

# =====================================================================
# STRATEGY PARAMETERS
# =====================================================================
WMA_PERIOD = 30
MOMENTUM_PERIOD = 17
ATR_PERIOD = 14
MANSFIELD_MA = 52
ATR_FULL = 2.0       # 0-2.0 ATR = 100%
ATR_HALF = 4.0       # 2.0-4.0 = 50%, >4.0 = no entry
STOP_PCT = 0.97      # WMA30 * 0.97
PARTIAL_2R = 0.25
PARTIAL_3R = 0.25
MAX_POSITIONS = 10
NHNL_MA = 50

# =====================================================================
# S&P 500 UNIVERSE - SECTOR MAPPING
# =====================================================================
SECTOR_ETF_MAP = {
    'Information Technology': 'XLK',
    'Health Care': 'XLV',
    'Financials': 'XLF',
    'Consumer Discretionary': 'XLY',
    'Communication Services': 'XLC',
    'Industrials': 'XLI',
    'Consumer Staples': 'XLP',
    'Energy': 'XLE',
    'Utilities': 'XLU',
    'Real Estate': 'XLRE',
    'Materials': 'XLB',
}

def get_sp500_tickers():
    """Get S&P 500 tickers with sector ETF and GICS Sub-Industry mapping.
    Returns: (stock_universe {sym: etf}, subindustry_map {sym: subindustry_name})
    """
    try:
        import urllib.request
        from io import StringIO
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        html = urllib.request.urlopen(req, timeout=15).read().decode('utf-8')
        tables = pd.read_html(StringIO(html))
        df = tables[0]
        tickers = {}
        subind_map = {}
        for _, row in df.iterrows():
            sym = str(row['Symbol']).strip().replace('.', '-')
            sector = str(row['GICS Sector']).strip()
            subind = str(row['GICS Sub-Industry']).strip()
            etf = SECTOR_ETF_MAP.get(sector, 'XLK')
            tickers[sym] = etf
            subind_map[sym] = subind
        n_subind = len(set(subind_map.values()))
        print(f"  S&P 500: {len(tickers)} acciones, {n_subind} sub-industrias GICS")
        return tickers, subind_map
    except Exception as e:
        print(f"  Wikipedia fallo ({e}), usando universo reducido...")
        fallback = get_fallback_universe()
        # Fallback: use sector as sub-industry
        subind_fb = {sym: etf for sym, etf in fallback.items()}
        return fallback, subind_fb

def get_fallback_universe():
    """Fallback: large subset of S&P 500."""
    return {
        'AAPL': 'XLK', 'MSFT': 'XLK', 'NVDA': 'XLK', 'AVGO': 'XLK', 'ADBE': 'XLK',
        'CRM': 'XLK', 'AMD': 'XLK', 'INTC': 'XLK', 'TXN': 'XLK', 'QCOM': 'XLK',
        'AMAT': 'XLK', 'MU': 'XLK', 'LRCX': 'XLK', 'KLAC': 'XLK', 'SNPS': 'XLK',
        'CDNS': 'XLK', 'MCHP': 'XLK', 'ON': 'XLK', 'FTNT': 'XLK', 'PANW': 'XLK',
        'ORCL': 'XLK', 'ACN': 'XLK', 'CSCO': 'XLK', 'IBM': 'XLK', 'INTU': 'XLK',
        'NOW': 'XLK', 'PLTR': 'XLK', 'MSI': 'XLK', 'APH': 'XLK', 'ADI': 'XLK',
        'UNH': 'XLV', 'JNJ': 'XLV', 'LLY': 'XLV', 'PFE': 'XLV', 'ABBV': 'XLV',
        'MRK': 'XLV', 'TMO': 'XLV', 'ABT': 'XLV', 'AMGN': 'XLV', 'MDT': 'XLV',
        'ISRG': 'XLV', 'GILD': 'XLV', 'VRTX': 'XLV', 'BSX': 'XLV', 'SYK': 'XLV',
        'REGN': 'XLV', 'ELV': 'XLV', 'HCA': 'XLV', 'CI': 'XLV', 'DHR': 'XLV',
        'BRK-B': 'XLF', 'JPM': 'XLF', 'V': 'XLF', 'MA': 'XLF', 'BAC': 'XLF',
        'GS': 'XLF', 'MS': 'XLF', 'BLK': 'XLF', 'C': 'XLF', 'AXP': 'XLF',
        'SCHW': 'XLF', 'PGR': 'XLF', 'CB': 'XLF', 'ICE': 'XLF', 'CME': 'XLF',
        'AMZN': 'XLY', 'TSLA': 'XLY', 'HD': 'XLY', 'MCD': 'XLY', 'NKE': 'XLY',
        'LOW': 'XLY', 'SBUX': 'XLY', 'TJX': 'XLY', 'CMG': 'XLY', 'ORLY': 'XLY',
        'ROST': 'XLY', 'DHI': 'XLY', 'LEN': 'XLY', 'GM': 'XLY', 'F': 'XLY',
        'GOOGL': 'XLC', 'META': 'XLC', 'NFLX': 'XLC', 'DIS': 'XLC', 'CMCSA': 'XLC',
        'T': 'XLC', 'VZ': 'XLC', 'TMUS': 'XLC', 'CHTR': 'XLC', 'EA': 'XLC',
        'CAT': 'XLI', 'HON': 'XLI', 'UNP': 'XLI', 'BA': 'XLI', 'DE': 'XLI',
        'LMT': 'XLI', 'UPS': 'XLI', 'ADP': 'XLI', 'GD': 'XLI', 'ITW': 'XLI',
        'EMR': 'XLI', 'MMM': 'XLI', 'GE': 'XLI', 'RTX': 'XLI', 'WM': 'XLI',
        'PG': 'XLP', 'KO': 'XLP', 'PEP': 'XLP', 'COST': 'XLP', 'WMT': 'XLP',
        'PM': 'XLP', 'MO': 'XLP', 'CL': 'XLP', 'MDLZ': 'XLP', 'KHC': 'XLP',
        'XOM': 'XLE', 'CVX': 'XLE', 'COP': 'XLE', 'SLB': 'XLE', 'EOG': 'XLE',
        'MPC': 'XLE', 'PSX': 'XLE', 'VLO': 'XLE', 'OXY': 'XLE', 'HES': 'XLE',
        'NEE': 'XLU', 'DUK': 'XLU', 'SO': 'XLU', 'D': 'XLU', 'SRE': 'XLU',
        'AEP': 'XLU', 'EXC': 'XLU', 'XEL': 'XLU', 'ED': 'XLU', 'WEC': 'XLU',
        'PLD': 'XLRE', 'AMT': 'XLRE', 'EQIX': 'XLRE', 'SPG': 'XLRE', 'PSA': 'XLRE',
        'O': 'XLRE', 'WELL': 'XLRE', 'DLR': 'XLRE',
        'LIN': 'XLB', 'APD': 'XLB', 'SHW': 'XLB', 'FCX': 'XLB', 'NEM': 'XLB',
        'NUE': 'XLB', 'DOW': 'XLB', 'VMC': 'XLB',
    }

# =====================================================================
# DATA LOADING
# =====================================================================
def load_data(stock_universe, force_refresh=False):
    """Download weekly OHLCV for all S&P 500 + extras.
    CRITICAL: Aligns all data to SPY's weekly dates to prevent date mismatch bugs.
    """
    if not force_refresh and os.path.exists(CACHE_FILE):
        mod_time = os.path.getmtime(CACHE_FILE)
        age_hours = (time.time() - mod_time) / 3600
        if age_hours < 12:
            print("  Cargando cache (< 12h)...")
            with open(CACHE_FILE, 'rb') as f:
                cached = pickle.load(f)
            if len(cached) > len(stock_universe) * 0.7:
                return cached
            print("  Cache incompleto, re-descargando...")

    sector_etfs = sorted(set(stock_universe.values()))
    extra = ['SPY', 'QQQ']
    all_tickers = sorted(set(list(stock_universe.keys()) + sector_etfs + extra))

    print(f"  Descargando {len(all_tickers)} tickers semanales...")
    end_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

    batch_size = 100
    raw_data = {}

    for i in range(0, len(all_tickers), batch_size):
        batch = all_tickers[i:i+batch_size]
        print(f"    Batch {i//batch_size + 1}/{(len(all_tickers)-1)//batch_size + 1} "
              f"({len(batch)} tickers)...")
        try:
            raw = yf.download(batch, start=DATA_START, end=end_date,
                              interval='1wk', auto_adjust=True, threads=True,
                              progress=False)
            for ticker in batch:
                try:
                    if isinstance(raw.columns, pd.MultiIndex):
                        df = pd.DataFrame({
                            'Close': raw[('Close', ticker)],
                            'High': raw[('High', ticker)],
                            'Low': raw[('Low', ticker)],
                            'Open': raw[('Open', ticker)],
                            'Volume': raw[('Volume', ticker)],
                        })
                    else:
                        df = raw[['Close', 'High', 'Low', 'Open', 'Volume']].copy()
                    df = df.dropna(subset=['Close'])
                    if len(df) >= 80:
                        raw_data[ticker] = df
                except Exception:
                    pass
        except Exception as e:
            print(f"    Batch error: {e}")

    # CRITICAL: Align ALL stocks to SPY's weekly date grid
    # This prevents equity calculation bugs from date mismatches
    if 'SPY' not in raw_data:
        print("  ERROR: No SPY data!")
        return raw_data

    spy_dates = raw_data['SPY'].index
    print(f"  Alineando {len(raw_data)} tickers a {len(spy_dates)} fechas semanales de SPY...")

    stock_data = {}
    for ticker, df in raw_data.items():
        aligned = df.reindex(spy_dates, method='ffill')
        aligned = aligned.ffill().dropna(subset=['Close'])
        if len(aligned) >= 80:
            stock_data[ticker] = aligned

    print(f"  Total cargados y alineados: {len(stock_data)} tickers")

    with open(CACHE_FILE, 'wb') as f:
        pickle.dump(stock_data, f)

    return stock_data

# =====================================================================
# SUB-INDUSTRY SYNTHETIC INDICES
# =====================================================================
def compute_subindustry_indices(all_data, subind_map, spy_dates):
    """
    Compute equal-weighted GICS Sub-Industry price indices.
    For each sub-industry, normalize component stocks to 100 at start,
    then average. This creates a synthetic benchmark for that sub-industry.

    Returns: {subindustry_name: pd.Series of close prices aligned to spy_dates}
    """
    # Group stocks by sub-industry
    subind_stocks = {}
    for sym, si in subind_map.items():
        if si not in subind_stocks:
            subind_stocks[si] = []
        subind_stocks[si].append(sym)

    subind_indices = {}
    for si, syms in subind_stocks.items():
        closes = []
        for sym in syms:
            if sym in all_data:
                c = all_data[sym]['Close'].reindex(spy_dates, method='ffill')
                first_valid = c.first_valid_index()
                if first_valid is not None:
                    c_norm = c / c.loc[first_valid] * 100
                    closes.append(c_norm)
        if closes:
            subind_indices[si] = pd.concat(closes, axis=1).mean(axis=1)

    return subind_indices

# =====================================================================
# INDICATORS
# =====================================================================
def calc_wma(series, period):
    weights = np.arange(1, period + 1, dtype=float)
    return series.rolling(period).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)

def calc_atr(high, low, close, period):
    tr = pd.concat([high - low, (high - close.shift(1)).abs(),
                     (low - close.shift(1)).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def calc_mansfield(stock_close, bench_close, ma_period=52):
    rp = (stock_close / bench_close) * 100.0
    ma = rp.rolling(ma_period).mean()
    return ((rp / ma) - 1.0) * 100.0

def compute_stock_data(df, spy_close, subind_close):
    """Compute all indicators for one stock.
    C5 uses GICS Sub-Industry Mansfield RS (synthetic equal-weight index vs SPY).
    Returns DataFrame.
    """
    c, h, l, o = df['Close'], df['High'], df['Low'], df['Open']
    idx = df.index

    wma = calc_wma(c, WMA_PERIOD)
    atr = calc_atr(h, l, c, ATR_PERIOD)
    spy_al = spy_close.reindex(idx, method='ffill')
    sub_al = subind_close.reindex(idx, method='ffill')

    r = pd.DataFrame(index=idx)
    r['close'] = c
    r['open'] = o
    r['open_next'] = o.shift(-1)   # Open of NEXT week (for entry price)
    r['wma'] = wma
    r['atr'] = atr

    # 5 core conditions
    r['c1_above_wma'] = (c > wma).fillna(False)
    r['c2_wma_rising'] = (wma > wma.shift(1)).fillna(False)
    r['c3_mom_ok'] = ((c - c.shift(MOMENTUM_PERIOD)) > 0).fillna(False)

    ms = calc_mansfield(c, spy_al, MANSFIELD_MA)
    r['c4_ms_ok'] = (ms > 0).fillna(False)
    r['mansfield'] = ms

    # C5: Sub-Industry Mansfield RS (replaces broad sector ETF)
    sub_ms = calc_mansfield(sub_al, spy_al, MANSFIELD_MA)
    r['c5_subind_ok'] = (sub_ms > 0).fillna(False)
    r['subind_ms'] = sub_ms

    # Core score (0-5)
    r['core_score'] = (r['c1_above_wma'].astype(int) + r['c2_wma_rising'].astype(int) +
                       r['c3_mom_ok'].astype(int) + r['c4_ms_ok'].astype(int) +
                       r['c5_subind_ok'].astype(int))

    # ATR distance
    r['dist_atr'] = (c - wma) / atr

    # Stop
    r['stop_wma'] = wma * STOP_PCT

    # 52W high
    h52 = h.rolling(52, min_periods=20).max()
    r['dist_h52_pct'] = ((h52 - c) / h52) * 100.0

    return r

# =====================================================================
# FIRST-GEN vs CONTINUATION DETECTION
# =====================================================================
def classify_entries(stock_indicators):
    """
    For each stock, classify potential entries as FIRST-GEN or CONTINUATION.

    FIRST-GEN: core_score was 0 at some point before reaching 5
    CONTINUATION: core_score went from 1-4 to 5 without ever hitting 0

    Returns: dict {sym: DataFrame with 'signal_type' column}
    """
    result = {}
    for sym, ind in stock_indicators.items():
        core = ind['core_score']
        n = len(core)

        signal_type = pd.Series('', index=ind.index)
        was_zero = True  # Start fresh: assume reset at beginning
        was_five = False
        in_signal = False

        for i in range(n):
            cs = core.iloc[i]
            if pd.isna(cs):
                continue

            cs = int(cs)

            if cs == 0:
                was_zero = True
                was_five = False
                in_signal = False

            if cs == 5 and not in_signal:
                if was_zero:
                    signal_type.iloc[i] = 'FIRST_GEN'
                    in_signal = True
                    was_zero = False
                else:
                    signal_type.iloc[i] = 'CONTINUATION'
                    in_signal = True

            if cs < 5:
                in_signal = False

        ind_copy = ind.copy()
        ind_copy['signal_type'] = signal_type
        result[sym] = ind_copy

    return result

# =====================================================================
# MARKET TIMING (NH-NL + QQQ>WMA30)
# =====================================================================
def compute_market_timing(all_data, stock_universe):
    """
    Semaforo:
      VERDE: NH-NL rising + above MA50  AND  QQQ > WMA30
      ROJO: cualquiera falla
    """
    spy = all_data.get('SPY')
    qqq = all_data.get('QQQ')
    if spy is None:
        print("  ERROR: No SPY data")
        return None, None

    spy_close = spy['Close']
    idx = spy_close.index

    # NH-NL from universe
    nh_count = pd.Series(0, index=idx, dtype=float)
    nl_count = pd.Series(0, index=idx, dtype=float)
    for sym in stock_universe:
        if sym not in all_data:
            continue
        sc = all_data[sym]['Close'].reindex(idx, method='ffill')
        sh = all_data[sym]['High'].reindex(idx, method='ffill')
        sl = all_data[sym]['Low'].reindex(idx, method='ffill')
        h52 = sh.rolling(52, min_periods=52).max()
        l52 = sl.rolling(52, min_periods=52).min()
        nh_count += (sc >= h52 * 0.99).fillna(False).astype(float)
        nl_count += (sc <= l52 * 1.01).fillna(False).astype(float)

    nhnl = nh_count - nl_count
    nhnl_ma = nhnl.rolling(NHNL_MA, min_periods=10).mean()
    cond_nhnl = ((nhnl_ma > nhnl_ma.shift(1)) & (nhnl > nhnl_ma)).fillna(False)

    # QQQ > WMA30
    if qqq is not None:
        qqq_close = qqq['Close'].reindex(idx, method='ffill')
        qqq_wma = calc_wma(qqq_close, WMA_PERIOD)
        cond_qqq = (qqq_close > qqq_wma).fillna(False)
    else:
        cond_qqq = pd.Series(True, index=idx)

    # Semaforo
    verde = cond_nhnl & cond_qqq

    details = pd.DataFrame(index=idx)
    details['nhnl'] = nhnl
    details['nhnl_ma'] = nhnl_ma
    details['nhnl_ok'] = cond_nhnl
    details['qqq_ok'] = cond_qqq
    details['verde'] = verde

    green_pct = verde.sum() / len(verde) * 100
    print(f"  Semaforo VERDE: {verde.sum()}/{len(verde)} semanas ({green_pct:.0f}%)")
    print(f"    NH-NL OK: {cond_nhnl.sum()}, QQQ>WMA: {cond_qqq.sum()}")

    return verde, details

# =====================================================================
# PORTFOLIO ENGINE
# =====================================================================
class PortfolioEngine:
    """Manages the 10-position equal-weight portfolio."""

    def __init__(self):
        self.positions = {}  # sym -> position dict
        self.cash = INITIAL_CAPITAL
        self.trade_log = []
        self.weekly_log = []

    def get_equity(self, signals, date):
        eq = self.cash
        for sym, pos in self.positions.items():
            if sym in signals and date in signals[sym].index:
                eq += pos['shares_rem'] * signals[sym].loc[date]['close']
        return eq

    def process_week(self, date, signals, mt_verde, stock_universe, pending_entries=None):
        """
        Process one week. CRITICAL FLOW:
          - Signals are detected at Friday CLOSE
          - Entries execute at OPEN of the NEXT week (Monday)
          - So: this week we EXECUTE pending entries from last week's signal,
            and DETECT new signals for next week's execution.

        pending_entries: list of (sym, sizing, signal_row) detected LAST week
        Returns: (actions_dict, new_pending_entries for NEXT week)
        """
        actions = {
            'date': str(date)[:10],
            'exits': [],
            'partial_sells': [],
            'new_entries': [],     # actually executed this week (from last week's signals)
            'addons': [],
            'signals_detected': [], # signals found THIS week (to execute NEXT week)
            'mt_status': 'VERDE' if mt_verde else 'ROJO',
        }

        equity = self.get_equity(signals, date)
        pos_budget = equity / MAX_POSITIONS

        # ── 1. EXECUTE PENDING ENTRIES from last week's signals ──
        # These buy at THIS week's OPEN price
        if pending_entries and mt_verde:
            slots = MAX_POSITIONS - len(self.positions)
            for sym, sizing, sig_row_prev in pending_entries[:slots]:
                if sym in self.positions:
                    continue
                if sym not in signals or date not in signals[sym].index:
                    continue

                row_now = signals[sym].loc[date]
                entry_px = row_now['open']  # Buy at OPEN of this week
                if np.isnan(entry_px) or entry_px <= 0:
                    entry_px = row_now['close']  # fallback

                stop_px = sig_row_prev['stop_wma']  # Stop from signal week
                if np.isnan(stop_px):
                    stop_px = row_now['stop_wma']
                risk = entry_px - stop_px

                if risk <= 0 or np.isnan(entry_px) or np.isnan(stop_px):
                    continue

                alloc = min(pos_budget * (1.0 if sizing == '100%' else 0.5), self.cash)
                if alloc < pos_budget * 0.15:
                    break

                shares = int(alloc / entry_px)
                if shares <= 0:
                    continue
                cost = shares * entry_px * (1 + COMMISSION_PCT / 100)
                if cost > self.cash:
                    shares = int(self.cash / (entry_px * (1 + COMMISSION_PCT / 100)))
                    if shares <= 0:
                        continue
                    cost = shares * entry_px * (1 + COMMISSION_PCT / 100)

                self.cash -= cost
                self.positions[sym] = {
                    'entry_price': entry_px, 'stop_initial': stop_px,
                    'shares_init': shares, 'shares_rem': shares,
                    'total_cost': cost, 'total_proceeds': 0.0,
                    'level_2r': entry_px + 2.0 * risk,
                    'level_3r': entry_px + 3.0 * risk,
                    'entry_date': str(date)[:10], 'weeks': 0,
                    'exit_2r': False, 'exit_3r': False,
                    'sizing': sizing, 'addon_done': False,
                    'signal_type': 'FIRST_GEN',
                    'current_price': entry_px,
                    'current_stop': stop_px,
                    'current_pnl_pct': 0.0,
                }

                actions['new_entries'].append({
                    'symbol': sym, 'signal_type': 'FIRST_GEN',
                    'mansfield': sig_row_prev['mansfield'],
                    'dist_atr': sig_row_prev['dist_atr'],
                    'sizing': sizing,
                    'price': entry_px,  # OPEN price
                    'stop': stop_px,
                    'shares': shares,
                })

        # ── 2. UPDATE & CHECK EXITS at this week's CLOSE ──
        to_exit = []
        for sym, pos in list(self.positions.items()):
            if sym not in signals or date not in signals[sym].index:
                continue

            row = signals[sym].loc[date]
            pos['weeks'] += 1
            curr = row['close']
            if np.isnan(curr):
                continue

            stop = row['stop_wma']
            if np.isnan(stop):
                stop = pos['stop_initial']
            if pos.get('exit_2r') and not np.isnan(pos['entry_price']):
                stop = max(stop, pos['entry_price'])
            pos['current_stop'] = stop
            pos['current_price'] = curr
            pos['current_pnl_pct'] = (curr / pos['entry_price'] - 1) * 100

            # Partial 2R
            if (not pos.get('exit_2r') and pos['shares_rem'] == pos['shares_init']
                    and curr >= pos['level_2r']):
                n_sell = max(1, int(pos['shares_init'] * PARTIAL_2R))
                n_sell = min(n_sell, pos['shares_rem'])
                proceeds = n_sell * curr * (1 - COMMISSION_PCT / 100)
                self.cash += proceeds
                pos['total_proceeds'] += proceeds
                pos['shares_rem'] -= n_sell
                pos['exit_2r'] = True
                actions['partial_sells'].append({
                    'symbol': sym, 'type': '2R (25%)',
                    'shares_sold': n_sell, 'price': curr,
                    'remaining': pos['shares_rem'],
                })

            # Partial 3R
            if (pos.get('exit_2r') and not pos.get('exit_3r')
                    and curr >= pos['level_3r']):
                n_sell = max(1, int(pos['shares_init'] * PARTIAL_3R))
                n_sell = min(n_sell, pos['shares_rem'])
                proceeds = n_sell * curr * (1 - COMMISSION_PCT / 100)
                self.cash += proceeds
                pos['total_proceeds'] += proceeds
                pos['shares_rem'] -= n_sell
                pos['exit_3r'] = True
                actions['partial_sells'].append({
                    'symbol': sym, 'type': '3R (25%)',
                    'shares_sold': n_sell, 'price': curr,
                    'remaining': pos['shares_rem'],
                })

            # Full exit check at close
            if curr < stop or pos['shares_rem'] <= 0:
                to_exit.append(sym)

        for sym in to_exit:
            pos = self.positions[sym]
            if sym in signals and date in signals[sym].index:
                exit_px = signals[sym].loc[date]['close']
                proceeds = pos['shares_rem'] * exit_px * (1 - COMMISSION_PCT / 100)
                self.cash += proceeds
                pos['total_proceeds'] += proceeds
                pnl_pct = (pos['total_proceeds'] / pos['total_cost'] - 1) * 100

                actions['exits'].append({
                    'symbol': sym, 'entry_price': pos['entry_price'],
                    'exit_price': exit_px, 'pnl_pct': pnl_pct,
                    'weeks': pos['weeks'], 'signal_type': pos.get('signal_type', ''),
                })

                self.trade_log.append({
                    'symbol': sym, 'entry_date': pos['entry_date'],
                    'exit_date': str(date)[:10], 'entry_px': pos['entry_price'],
                    'exit_px': exit_px, 'pnl_pct': pnl_pct,
                    'weeks': pos['weeks'], 'hit_2r': pos.get('exit_2r', False),
                    'hit_3r': pos.get('exit_3r', False),
                    'signal_type': pos.get('signal_type', ''),
                })
            del self.positions[sym]

        # ── 3. ADD-ONS (50% -> 100%) at this week's open ──
        for sym, pos in list(self.positions.items()):
            if pos.get('sizing') != '50%' or pos.get('addon_done'):
                continue
            if sym not in signals or date not in signals[sym].index:
                continue
            row = signals[sym].loc[date]
            if (int(row.get('core_score', 0)) == 5 and
                    not np.isnan(row['dist_atr']) and
                    0 <= row['dist_atr'] <= ATR_FULL and mt_verde):
                addon_px = row['open'] if not np.isnan(row['open']) else row['close']
                target = pos['shares_init']
                cost_est = target * addon_px * (1 + COMMISSION_PCT / 100)
                if self.cash >= cost_est * 0.5:
                    actual = int(min(cost_est, self.cash) / addon_px)
                    if actual > 0:
                        cost = actual * addon_px * (1 + COMMISSION_PCT / 100)
                        self.cash -= cost
                        pos['shares_init'] += actual
                        pos['shares_rem'] += actual
                        pos['total_cost'] += cost
                        pos['entry_price'] = pos['total_cost'] / pos['shares_init']
                        risk = pos['entry_price'] - row['stop_wma']
                        if risk > 0:
                            pos['level_2r'] = pos['entry_price'] + 2.0 * risk
                            pos['level_3r'] = pos['entry_price'] + 3.0 * risk
                        pos['sizing'] = '100% (addon)'
                        pos['addon_done'] = True
                        actions['addons'].append({
                            'symbol': sym, 'shares_added': actual, 'price': addon_px,
                        })

        # ── 4. DETECT NEW SIGNALS at this week's CLOSE (for NEXT week execution) ──
        # BOTH entry types: FIRST_GEN priority, then CONTINUATION
        new_pending = []
        sector_etfs = set(SECTOR_ETF_MAP.values())
        skip = set(self.positions.keys()) | sector_etfs | {'SPY', 'QQQ'}

        for sym in stock_universe:
            if sym in skip or sym not in signals:
                continue
            if date not in signals[sym].index:
                continue

            row = signals[sym].loc[date]
            sig_type = row.get('signal_type', '')
            if sig_type not in ('FIRST_GEN', 'CONTINUATION'):
                continue

            core = int(row.get('core_score', 0))
            if core != 5:
                continue

            dist_atr = row['dist_atr']
            if np.isnan(dist_atr) or dist_atr < 0 or dist_atr > ATR_HALF:
                continue

            sizing = '100%' if dist_atr <= ATR_FULL else '50%'
            ms = row['mansfield']
            if np.isnan(ms):
                ms = -999

            new_pending.append((sym, sizing, row.to_dict(), ms, sig_type))

        # Sort by Mansfield RS descending (FG and CT ranked together)
        new_pending.sort(key=lambda x: x[3], reverse=True)
        # Keep only sym, sizing, row_dict
        new_pending_clean = [(sym, sz, rd) for sym, sz, rd, _, _ in new_pending]

        # Record in actions for the report
        for sym, sizing, row_dict, ms, sig_type in new_pending:
            actions['signals_detected'].append({
                'symbol': sym, 'mansfield': ms,
                'subind_ms': row_dict.get('subind_ms', 0),
                'dist_atr': row_dict.get('dist_atr', 0),
                'sizing': sizing,
                'close_price': row_dict.get('close', 0),
                'stop': row_dict.get('stop_wma', 0),
                'signal_type': sig_type,
            })

        # Record equity
        equity = self.get_equity(signals, date)
        actions['equity'] = equity
        actions['cash'] = self.cash
        actions['n_positions'] = len(self.positions)
        self.weekly_log.append(actions)

        return actions, new_pending_clean

# =====================================================================
# REPORTING
# =====================================================================
def print_weekly_report(actions, positions, mt_details_row=None):
    """Print a clear, actionable weekly report."""
    date = actions['date']
    mt = actions['mt_status']
    eq = actions['equity']
    n_pos = actions['n_positions']

    print(f"\n{'='*70}")
    print(f"  REPORTE SEMANAL - {date}")
    print(f"{'='*70}")

    # Market Timing
    print(f"\n  SEMAFORO MARKET TIMING: {mt}")
    if mt_details_row is not None:
        nhnl_ok = "OK" if mt_details_row.get('nhnl_ok', False) else "FALLO"
        qqq_ok = "OK" if mt_details_row.get('qqq_ok', False) else "FALLO"
        print(f"    NH-NL (rising + >MA50): {nhnl_ok}")
        print(f"    QQQ > WMA30:            {qqq_ok}")
    if mt == 'ROJO':
        print(f"    >> No se abren nuevas posiciones. Existentes siguen con stops.")

    # Portfolio status
    print(f"\n  CARTERA ({n_pos}/{MAX_POSITIONS} posiciones)")
    print(f"  Equity: ${eq:,.0f}   Cash: ${actions['cash']:,.0f}")

    if positions:
        print(f"\n  {'Sym':<7s} {'Entrada':>8s} {'Actual':>8s} {'P/L':>7s} {'Stop':>8s} "
              f"{'Sem':>4s} {'Sizing':<15s} {'Tipo':<13s} {'Status':<10s}")
        print(f"  {'-'*90}")

        sorted_pos = sorted(positions.items(),
                           key=lambda x: x[1].get('current_pnl_pct', 0), reverse=True)
        for sym, pos in sorted_pos:
            pnl = pos.get('current_pnl_pct', 0)
            status = "HOLD"
            if pos.get('exit_3r'):
                status = "RUNNER"
            elif pos.get('exit_2r'):
                status = "POST-2R"

            print(f"  {sym:<7s} ${pos['entry_price']:>7.2f} ${pos.get('current_price', 0):>7.2f} "
                  f"{pnl:+6.1f}% ${pos.get('current_stop', 0):>7.2f} "
                  f"{pos['weeks']:>3d}w {pos.get('sizing', '?'):<15s} "
                  f"{pos.get('signal_type', '?'):<13s} {status:<10s}")

    # EXITS
    if actions['exits']:
        print(f"\n  SALIDAS COMPLETAS:")
        for e in actions['exits']:
            print(f"    VENDER TODO {e['symbol']}: entrada ${e['entry_price']:.2f} -> "
                  f"salida ${e['exit_price']:.2f}  P/L: {e['pnl_pct']:+.1f}%  "
                  f"({e['weeks']}w)")

    # PARTIAL SELLS
    if actions['partial_sells']:
        print(f"\n  VENTAS PARCIALES:")
        for p in actions['partial_sells']:
            print(f"    VENDER {p['shares_sold']} acciones de {p['symbol']} "
                  f"({p['type']}) a ${p['price']:.2f}  "
                  f"(quedan {p['remaining']} acciones)")

    # ADD-ONS
    if actions['addons']:
        print(f"\n  ADD-ONS (50% -> 100%):")
        for a in actions['addons']:
            print(f"    COMPRAR {a['shares_added']} acciones mas de {a['symbol']} "
                  f"a ${a['price']:.2f}")

    # NEW ENTRIES (executed this week from last week's signals)
    if actions['new_entries']:
        print(f"\n  ENTRADAS EJECUTADAS ESTA SEMANA (al OPEN):")
        for e in actions['new_entries']:
            print(f"    COMPRADO {e['symbol']:<6s} {e['shares']} acciones a ${e['price']:.2f} (OPEN)  "
                  f"Stop: ${e['stop']:.2f}  "
                  f"ATR dist: {e['dist_atr']:.1f}  MS: {e['mansfield']:+.1f}  "
                  f"Sizing: {e['sizing']}")
    elif mt == 'ROJO':
        print(f"\n  Sin nuevas entradas (semaforo ROJO)")

    if not actions['exits'] and not actions['partial_sells'] and not actions['new_entries'] and not actions['addons']:
        print(f"\n  Sin acciones esta semana. Mantener posiciones actuales.")

def print_backtest_summary(engine, spy_close):
    """Print summary of full backtest."""
    if not engine.weekly_log:
        return

    equities = [(w['date'], w['equity']) for w in engine.weekly_log]
    eq_df = pd.DataFrame(equities, columns=['date', 'equity']).set_index('date')
    eq = eq_df['equity']

    years = len(eq) / 52.0
    if years <= 0 or eq.iloc[0] <= 0:
        return

    cagr = (eq.iloc[-1] / eq.iloc[0]) ** (1.0 / years) - 1
    rets = eq.pct_change().dropna()
    sharpe = rets.mean() / rets.std() * np.sqrt(52) if rets.std() > 0 else 0
    dd = (eq - eq.cummax()) / eq.cummax()
    maxdd = dd.min()
    calmar = cagr / abs(maxdd) if maxdd != 0 else 0

    trades = engine.trade_log
    n = len(trades)

    print(f"\n{'='*70}")
    print(f"  RESUMEN BACKTEST")
    print(f"{'='*70}")
    print(f"  Periodo:       {engine.weekly_log[0]['date']} -> {engine.weekly_log[-1]['date']}")
    print(f"  Capital final: ${eq.iloc[-1]:,.0f} (inicial: ${INITIAL_CAPITAL:,.0f})")
    print(f"  CAGR:          {cagr*100:+.1f}%")
    print(f"  Max Drawdown:  {maxdd*100:.1f}%")
    print(f"  Sharpe:        {sharpe:.2f}")
    print(f"  Calmar:        {calmar:.2f}")

    if n > 0:
        pnls = [t['pnl_pct'] for t in trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        fg_trades = [t for t in trades if t.get('signal_type') == 'FIRST_GEN']
        ct_trades = [t for t in trades if t.get('signal_type') == 'CONTINUATION']

        print(f"\n  Trades:        {n}")
        print(f"  Win Rate:      {len(wins)/n*100:.0f}%")
        print(f"  Avg Win:       {np.mean(wins):+.1f}%" if wins else "  Avg Win:       N/A")
        print(f"  Avg Loss:      {np.mean(losses):+.1f}%" if losses else "  Avg Loss:      N/A")
        print(f"  W/L Ratio:     {np.mean(wins)/abs(np.mean(losses)):.1f}" if wins and losses else "")
        pf = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else 999
        print(f"  Profit Factor: {pf:.1f}")
        print(f"  Avg Holding:   {np.mean([t['weeks'] for t in trades]):.0f} semanas")

        print(f"\n  Por tipo de entrada:")
        if fg_trades:
            fg_pnls = [t['pnl_pct'] for t in fg_trades]
            fg_wins = [p for p in fg_pnls if p > 0]
            print(f"    FIRST_GEN:    {len(fg_trades)} trades, "
                  f"WR={len(fg_wins)/len(fg_trades)*100:.0f}%, "
                  f"avg={np.mean(fg_pnls):+.1f}%")
        if ct_trades:
            ct_pnls = [t['pnl_pct'] for t in ct_trades]
            ct_wins = [p for p in ct_pnls if p > 0]
            print(f"    CONTINUATION: {len(ct_trades)} trades, "
                  f"WR={len(ct_wins)/len(ct_trades)*100:.0f}%, "
                  f"avg={np.mean(ct_pnls):+.1f}%")

        # Yearly
        print(f"\n  Desglose anual:")
        yearly = {}
        for t in trades:
            y = t['exit_date'][:4]
            if y not in yearly:
                yearly[y] = []
            yearly[y].append(t['pnl_pct'])
        for y in sorted(yearly):
            yp = yearly[y]
            yw = [p for p in yp if p > 0]
            print(f"    {y}: {len(yp):3d} trades, WR={len(yw)/len(yp)*100:3.0f}%, "
                  f"avg={np.mean(yp):+.1f}%")

    # SPY comparison
    spy_al = spy_close.reindex(eq.index, method='ffill').dropna()
    if len(spy_al) > 1:
        spy_cagr = (spy_al.iloc[-1] / spy_al.iloc[0]) ** (1.0 / years) - 1
        print(f"\n  SPY CAGR:      {spy_cagr*100:+.1f}%")
        print(f"  Alpha:         {(cagr-spy_cagr)*100:+.1f}%")

# =====================================================================
# SIGNALS DETECTED: FIRST-GEN entries to buy next Monday at OPEN
# =====================================================================
def print_signals_detected(actions, mt_status):
    """Show FIRST-GEN signals detected this week -> buy at OPEN next Monday."""
    sigs = actions.get('signals_detected', [])
    mt = mt_status or actions.get('mt_status', 'ROJO')
    n_pos = actions.get('n_positions', 0)
    slots = MAX_POSITIONS - n_pos

    print(f"\n  {'='*70}")
    print(f"  SENALES DETECTADAS ESTA SEMANA (FG + Continuation)")
    print(f"  >> Ejecutar al OPEN del LUNES de la proxima semana")
    print(f"  {'='*70}")

    def _print_sig_table(sigs_list, show_mark=False, slots_avail=0):
        print(f"\n  {'#':>3s} {'Simbolo':<7s} {'Tipo':<6s} {'MS stk':>7s} {'MS sub':>7s} "
              f"{'ATRd':>5s} {'Sz':<5s} {'Cierre':>9s} {'Stop':>9s} {'R%':>6s}")
        print(f"  {'-'*80}")
        for i, s in enumerate(sigs_list):
            risk_pct = 0
            if s['close_price'] > 0 and s['stop'] > 0:
                risk_pct = (1 - s['stop'] / s['close_price']) * 100
            t = 'FG' if s.get('signal_type') == 'FIRST_GEN' else 'CT'
            sub_ms = s.get('subind_ms', 0)
            if isinstance(sub_ms, float) and np.isnan(sub_ms):
                sub_ms = 0
            mark = ""
            if show_mark:
                mark = " <-- COMPRAR" if i < slots_avail else "     (espera)"
            print(f"  {i+1:>3d} {s['symbol']:<7s} {t:<6s} {s['mansfield']:>+6.1f} "
                  f"{sub_ms:>+6.1f} {s['dist_atr']:>5.1f} {s['sizing']:<5s} "
                  f"${s['close_price']:>8.2f} ${s['stop']:>8.2f} {risk_pct:>5.1f}%{mark}")

    if mt == 'ROJO':
        print(f"\n  SEMAFORO ROJO: no se abren nuevas posiciones.")
        if sigs:
            fg_c = sum(1 for s in sigs if s.get('signal_type') == 'FIRST_GEN')
            ct_c = sum(1 for s in sigs if s.get('signal_type') == 'CONTINUATION')
            print(f"  (Se detectaron {len(sigs)} senales [{fg_c} FG, {ct_c} CT] pero el semaforo las bloquea)")
            _print_sig_table(sigs)
        return

    if not sigs:
        print(f"\n  Sin senales esta semana. Nada que comprar.")
        return

    fg_c = sum(1 for s in sigs if s.get('signal_type') == 'FIRST_GEN')
    ct_c = sum(1 for s in sigs if s.get('signal_type') == 'CONTINUATION')
    print(f"\n  Total: {len(sigs)} senales (FG: {fg_c}, CT: {ct_c})")
    print(f"  Slots disponibles: {slots}/{MAX_POSITIONS}")
    if slots <= 0:
        print(f"  CARTERA LLENA - no se pueden abrir nuevas posiciones.")

    _print_sig_table(sigs, show_mark=True, slots_avail=slots)

    if slots > 0 and len(sigs) > 0:
        n_buy = min(slots, len(sigs))
        print(f"\n  ACCION: Poner ordenes de compra al OPEN del lunes para los {n_buy} primeros.")

# =====================================================================
# MAIN
# =====================================================================
def main():
    backtest_mode = '--backtest' in sys.argv
    force_refresh = '--refresh' in sys.argv

    print("=" * 70)
    print("METODO TENDENCIA 2.0 - MOTOR DE CARTERA SEMANAL")
    print("=" * 70)

    # 1. GET UNIVERSE
    print("\n[1/7] Obteniendo universo S&P 500 + GICS Sub-Industry...")
    stock_universe, subind_map = get_sp500_tickers()

    # 2. DOWNLOAD DATA
    print("\n[2/7] Descargando datos...")
    all_data = load_data(stock_universe, force_refresh=force_refresh)
    spy_close = all_data['SPY']['Close']
    spy_dates = all_data['SPY'].index

    # 3. SUB-INDUSTRY INDICES
    print("\n[3/7] Calculando indices de sub-industria GICS...")
    subind_indices = compute_subindustry_indices(all_data, subind_map, spy_dates)
    print(f"  {len(subind_indices)} sub-industrias calculadas")

    # 4. MARKET TIMING
    print("\n[4/7] Market Timing...")
    mt_verde, mt_details = compute_market_timing(all_data, stock_universe)

    # 5. COMPUTE INDICATORS
    print("\n[5/7] Indicadores (con sub-industria GICS)...")
    stock_ind = {}
    skipped = 0
    for sym in stock_universe:
        if sym not in all_data:
            skipped += 1
            continue
        subind_name = subind_map.get(sym, '')
        subind_close = subind_indices.get(subind_name, spy_close)
        try:
            stock_ind[sym] = compute_stock_data(
                all_data[sym], spy_close, subind_close)
        except Exception:
            skipped += 1
    print(f"  {len(stock_ind)} acciones procesadas (skipped {skipped})")

    # 6. CLASSIFY ENTRIES
    print("\n[6/7] Clasificando entradas (First-Gen vs Continuation)...")
    signals = classify_entries(stock_ind)

    # Count signals
    total_fg = sum(1 for sym in signals for i in range(len(signals[sym]))
                   if signals[sym]['signal_type'].iloc[i] == 'FIRST_GEN')
    total_ct = sum(1 for sym in signals for i in range(len(signals[sym]))
                   if signals[sym]['signal_type'].iloc[i] == 'CONTINUATION')
    print(f"  Total senales historicas: {total_fg} First-Gen, {total_ct} Continuation")

    # 6. RUN PORTFOLIO
    print("\n[7/7] Ejecutando motor de cartera...")

    # Get all dates
    all_dates = set()
    for sig in signals.values():
        all_dates.update(sig.index)
    dates = sorted([d for d in all_dates if d >= pd.Timestamp(BACKTEST_START)])

    engine = PortfolioEngine()
    pending_entries = []

    for date in dates:
        mt_ok = False
        if mt_verde is not None and date in mt_verde.index:
            mt_ok = bool(mt_verde.loc[date])

        actions, pending_entries = engine.process_week(date, signals, mt_ok, stock_universe, pending_entries)

    # PRINT RESULTS
    if backtest_mode:
        print_backtest_summary(engine, spy_close)

    # Last week report (always shown)
    if engine.weekly_log:
        last_actions = engine.weekly_log[-1]
        last_date = pd.Timestamp(last_actions['date'])

        # Get MT details for last date
        mt_row = None
        if mt_details is not None and last_date in mt_details.index:
            mt_row = mt_details.loc[last_date].to_dict()

        print_weekly_report(last_actions, engine.positions, mt_row)
        print_signals_detected(last_actions, last_actions['mt_status'])

    # Also show the 2nd-to-last and 3rd-to-last for context
    if len(engine.weekly_log) >= 3 and backtest_mode:
        print(f"\n{'='*70}")
        print(f"  ULTIMAS 4 SEMANAS (resumen)")
        print(f"{'='*70}")
        for w in engine.weekly_log[-4:]:
            exits_str = ', '.join([e['symbol'] for e in w['exits']]) or '-'
            entries_str = ', '.join([e['symbol'] for e in w['new_entries']]) or '-'
            partials_str = ', '.join([f"{p['symbol']}({p['type']})" for p in w['partial_sells']]) or '-'
            print(f"  {w['date']}  MT={w['mt_status']:<5s}  "
                  f"Equity=${w['equity']:>10,.0f}  Pos={w['n_positions']:>2d}  "
                  f"Salidas=[{exits_str}]  Entradas=[{entries_str}]  Parciales=[{partials_str}]")

    # Save portfolio state
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if engine.positions:
        portfolio_export = {}
        for sym, pos in engine.positions.items():
            portfolio_export[sym] = {
                'entry_price': pos['entry_price'],
                'entry_date': pos['entry_date'],
                'shares': pos['shares_rem'],
                'sizing': pos.get('sizing', ''),
                'signal_type': pos.get('signal_type', ''),
                'weeks': pos['weeks'],
                'stop': pos.get('current_stop', pos['stop_initial']),
                'exit_2r': pos.get('exit_2r', False),
                'exit_3r': pos.get('exit_3r', False),
                'level_2r': pos['level_2r'],
                'level_3r': pos['level_3r'],
            }

        with open(PORTFOLIO_FILE, 'w') as f:
            json.dump({
                'date': engine.weekly_log[-1]['date'] if engine.weekly_log else '',
                'equity': engine.weekly_log[-1]['equity'] if engine.weekly_log else 0,
                'cash': engine.cash,
                'positions': portfolio_export,
            }, f, indent=2, default=str)
        print(f"\n  Portfolio guardado en: {PORTFOLIO_FILE}")

    # Save last report to text file
    if engine.weekly_log:
        last_date_str = engine.weekly_log[-1]['date']
        report_file = os.path.join(OUTPUT_DIR, f'report_{last_date_str}.txt')

        import io
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        last_date_ts = pd.Timestamp(last_actions['date'])
        mt_row2 = None
        if mt_details is not None and last_date_ts in mt_details.index:
            mt_row2 = mt_details.loc[last_date_ts].to_dict()
        print_weekly_report(last_actions, engine.positions, mt_row2)
        print_signals_detected(last_actions, last_actions['mt_status'])
        sys.stdout = old_stdout

        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(buffer.getvalue())
        print(f"  Reporte guardado en: {report_file}")

    print(f"\n{'='*70}")
    print(f"  MOTOR COMPLETADO")
    print(f"{'='*70}")

if __name__ == '__main__':
    main()
