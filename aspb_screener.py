#!/usr/bin/env python3
"""
ASP-B WEEKLY SCREENER  — Dashboard Diario
==========================================
Filtra S&P 500 + NASDAQ buscando setups ASP-B activos.

Señales:
  BUY   – breakout confirmado en el último cierre semanal
  NEAR  – precio actual < 3% del nivel de entrada
  SETUP – estructura HL activa, aún lejos del breakout

Columnas extra:
  SMA30↑    – SMA30 semanal subiendo esta semana
  MS+       – Mansfield RS > 0 (outperformance relativa vs SPY)
  Stop      – 1st Pivot (Fib 0.618)
  TP1       – Fib 1.764
  R:R       – ratio riesgo / beneficio al TP1

Uso:
  python aspb_screener.py            # genera HTML y lo abre en el navegador
  python aspb_screener.py --no-open  # solo genera el archivo HTML
"""

import argparse
import json
import os
import pickle
import time
import datetime
import warnings
import webbrowser
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# RUTAS
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
CACHE_FILE = BASE_DIR / "aspb_screener_cache.pkl"
REPORT_DIR = BASE_DIR / "weekly_reports"
REPORT_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# PARÁMETROS ASP-B  (igual que el Pine Script)
# ─────────────────────────────────────────────────────────────────────────────
MIN_LEN   = 2
MAX_LEN   = 5
EMA_LEN   = 21
FIB_1ST   = 0.618
FIB_TP1   = 1.764
FIB_TP2   = 2.618
SMA_LEN   = 30    # Weinstein SMA30 semanal
MS_MA_LEN = 52    # Mansfield: media de 52 semanas del ratio RS

# ── Filtros de calidad adicionales ────────────────────────────────────────────
# Mínimo potencial al TP1 desde el entry (%). Setups con rango demasiado estrecho
# tienen poco potencial absoluto y el ruido semanal puede activar el stop.
MIN_TP1_PCT = 7.0

# Mínimo riesgo desde entry al stop (%). Rangos < 3% son demasiado estrechos;
# cualquier semana de volatilidad normal puede tocar el stop sin ser una señal real.
MIN_STOP_PCT = 3.0

CACHE_MAX_HOURS = 6   # Refresca datos si el cache tiene más de 6 h
DATA_YEARS      = 4   # Años de histórico semanal a descargar

BENCHMARK = "SPY"

# ─────────────────────────────────────────────────────────────────────────────
# UNIVERSO — S&P 500 completo + NASDAQ growth + 30 ADRs europeos
# Auto-descarga la lista del S&P 500 de Wikipedia (cache 30 días)
# ─────────────────────────────────────────────────────────────────────────────
_SP500_CACHE_JSON  = BASE_DIR / "_sp500_components.json"
_SP500_MAX_AGE_DAYS = 30

def _fetch_sp500_tickers():
    """Descarga lista S&P 500 de Wikipedia y la cachea 30 días.
    Devuelve lista de tickers o None si falla."""
    # 1. Cache JSON fresco
    if _SP500_CACHE_JSON.exists():
        age_d = (time.time() - _SP500_CACHE_JSON.stat().st_mtime) / 86400
        if age_d < _SP500_MAX_AGE_DAYS:
            try:
                tickers = json.loads(_SP500_CACHE_JSON.read_text())
                if len(tickers) > 400:
                    return tickers
            except Exception:
                pass
    # 2. Descargar de Wikipedia
    try:
        tables = pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            attrs={"id": "constituents"}, storage_options={"verify": False}
        )
        df = tables[0]
        tickers = [str(t).replace(".", "-") for t in df["Symbol"].tolist()]
        tickers = sorted(set(t for t in tickers if t and len(t) <= 6))
        if len(tickers) > 400:
            _SP500_CACHE_JSON.write_text(json.dumps(tickers))
            return tickers
    except Exception:
        pass
    return None

# Lista de respaldo hardcoded (~500 componentes clave)
_SP500_FALLBACK = [
    # Technology
    "AAPL","MSFT","NVDA","AVGO","ADBE","CRM","AMD","INTC","TXN","QCOM",
    "AMAT","MU","LRCX","KLAC","SNPS","CDNS","MCHP","FTNT","PANW","ORCL",
    "IBM","ACN","NOW","INTU","ON","PLTR","DDOG","SNOW","APP","ANSS","CDW",
    "CTSH","FSLR","GDDY","GRMN","HPE","HPQ","IT","JNPR","KEYS","LDOS",
    "MSI","NTAP","PTC","STX","TEL","TYL","VRSN","WDC","ZBRA","EPAM",
    "GLW","SWKS","QRVO","BR","GEN",
    # Health Care
    "UNH","JNJ","LLY","PFE","ABBV","MRK","TMO","ABT","AMGN","MDT",
    "ISRG","GILD","VRTX","BSX","SYK","REGN","MRNA","BIIB","IDXX","DXCM",
    "ALGN","ILMN","A","BAX","BDX","CI","CNC","DVA","ELV","EW","GEHC",
    "HCA","HOLX","IQV","LH","MCK","MOH","MTD","RMD","RVTY","TFX","UHS",
    "VTRS","WAT","ZBH","ZTS","CAH","CVS","DGX","COO","STE","PODD",
    # Financials
    "BRK-B","JPM","V","MA","BAC","GS","MS","BLK","C","AXP","SCHW",
    "MMC","WFC","CB","PGR","AON","ICE","CME","SPGI","MCO","AFL","AIG",
    "AIZ","AJG","ALL","AMP","BEN","BK","BRO","CBOE","CFG","CINF","COF",
    "DFS","EG","FITB","GL","GPN","HBAN","HIG","KEY","L","MET","MTB",
    "NDAQ","NTRS","PFG","RF","STT","SYF","TROW","TRV","UNM","USB","WTW",
    "ZION","FHN","SIVB","ALLY","RJF","SEIC","VOYA","FNF","LNC","RE",
    # Consumer Discretionary
    "AMZN","TSLA","HD","MCD","NKE","LOW","SBUX","TJX","CMG","ORLY",
    "ROST","DHI","BKNG","LEN","NVR","LULU","EBAY","MGM","AZO","BBY",
    "BWA","CCL","CZR","DECK","DPZ","ETSY","F","GM","GPC","HAS","HLT",
    "LKQ","LVS","MAR","NCLH","PHM","RCL","RL","TSCO","VFC","WHR","WYNN",
    "YUM","APTV","POOL","GPC","DKNG","RIVN","LCII","KMX","SIG","TPR",
    # Communication Services
    "GOOGL","META","NFLX","DIS","CMCSA","T","VZ","TMUS","EA","TTWO",
    "WBD","CHTR","FOX","FOXA","IPG","LYV","MTCH","NWSA","OMC","PARA","PINS",
    "SNAP","LUMN","WMG",
    # Industrials
    "CAT","HON","UNP","BA","DE","LMT","UPS","ADP","GD","ITW","EMR",
    "MMM","RTX","NOC","FDX","GE","CSX","NSC","ODFL","PCAR","FAST","VRSK",
    "PAYX","ALLE","AME","AXON","BALL","CARR","CBRE","CTAS","DAL","DOV",
    "ETN","EXPD","GNRC","GWW","HII","IEX","IR","J","JCI","JBHT","MAS",
    "NDSN","PH","PWR","RHI","ROK","RSG","SNA","SWK","TDG","TT","TXT",
    "UAL","URI","WM","WY","XYL","HUBB","FTV","LDOS","SAIC","LMT","OSK",
    # Consumer Staples
    "PG","KO","PEP","COST","WMT","PM","MO","CL","MDLZ","KDP","MNST",
    "ADM","BG","CAG","CHD","CLX","CPB","EL","GIS","HRL","HSY","K",
    "KMB","KR","MKC","SJM","STZ","SYY","TAP","TSN","WBA","KVUE",
    # Energy
    "XOM","CVX","COP","SLB","EOG","MPC","PSX","VLO","OXY","BKR","APA",
    "CTRA","DVN","FANG","HES","HAL","MRO","PXD","RRC","TPL","WMB","OKE",
    "KMI","LNG","TRGP",
    # Utilities
    "NEE","DUK","SO","D","SRE","AEP","EXC","XEL","AES","AWK","CMS",
    "CNP","DTE","ED","EIX","ES","ETR","EVRG","FE","LNT","NI","NRG",
    "PCG","PEG","PPL","WEC",
    # Real Estate
    "PLD","AMT","EQIX","SPG","PSA","O","AVB","CBRE","CPT","CCI","DLR",
    "EQR","ESS","EXR","FRT","HST","IRM","KIM","MAA","REG","SBAC","UDR",
    "WELL","ARE","BXP","NNN","VICI","MPW",
    # Materials
    "LIN","APD","SHW","FCX","NEM","NUE","ECL","ALB","AVY","CF","CTVA",
    "DD","DOW","EMN","FMC","IFF","IP","LYB","MOS","PKG","PPG","SEE",
    "VMC","WRK","X","CE","RPM","MLM",
]

# — Cargar S&P 500 (Wikipedia si disponible, fallback si no) —
_sp500_wiki = _fetch_sp500_tickers()
SP500 = sorted(set(_sp500_wiki if _sp500_wiki else _SP500_FALLBACK))

# NASDAQ growth names habitualmente fuera del S&P 500
NASDAQ_EXTRA = [
    "ASML","TSM","ARM","MRVL","MPWR","SMCI",
    "CRWD","ZS","NET","OKTA","HUBS","MDB","TEAM","GTLB",
    "COIN","PYPL","AFRM","UPST","NU",
    "UBER","ABNB","DASH","SHOP",
    "NIO","XPEV","LI",
    "MELI","PDD","BIDU","JD","SE",
    "CELH","DUOL","ZM","DOCU","ROKU",
    "NXPI",
]

# 30 blue chips europeos cotizando en USD en NYSE / NASDAQ
EUROPEAN_ADR = [
    # Energía
    "SHEL","BP","TTE","EQNR","E",
    # Pharma / Salud
    "NVO","AZN","NVS","GSK",
    # Tecnología / Semis
    "SAP","STM","ERIC","PHG","LOGI",
    # Consumo
    "UL","DEO","BTI",
    # Minería / Materiales
    "RIO","BHP",
    # Financiero / Seguros
    "ING","HSBC","AER","LYG",
    # Industrial / Autos
    "ABB","STLA","CNHI",
    # Varios
    "FLEX","ARGX","WPP","AMCR",
]

_sp500_set  = set(SP500)
_nasdaq_set = set(NASDAQ_EXTRA)
_eu_set     = set(EUROPEAN_ADR)

# Universo completo sin duplicados
ALL_TICKERS = sorted(set(SP500 + NASDAQ_EXTRA + EUROPEAN_ADR))
INDEX_MAP   = {
    t: ("NASDAQ" if t in _nasdaq_set and t not in _sp500_set else
        "EU-ADR" if t in _eu_set     and t not in _sp500_set else
        "SP500")
    for t in ALL_TICKERS
}

# ─────────────────────────────────────────────────────────────────────────────
# DESCARGA / CACHE DE DATOS
# ─────────────────────────────────────────────────────────────────────────────
def _load_from_yf():
    """Descarga datos semanales de todos los tickers + SPY. Puede tardar 1-2 min."""
    end   = datetime.date.today()
    start = end - datetime.timedelta(days=DATA_YEARS * 365 + 30)
    tickers_dl = sorted(set(ALL_TICKERS + [BENCHMARK]))

    print(f"  Descargando {len(tickers_dl)} tickers ({start} a {end})...")
    raw = yf.download(
        tickers_dl,
        start=str(start),
        end=str(end),
        interval="1wk",
        auto_adjust=True,
        progress=True,
    )
    # raw es MultiIndex: (OHLCV, ticker)
    opens   = raw["Open"]
    closes  = raw["Close"]
    highs   = raw["High"]
    lows    = raw["Low"]
    volumes = raw["Volume"] if "Volume" in raw.columns.get_level_values(0) else None

    data = {}
    for tkr in tickers_dl:
        if tkr not in closes.columns:
            continue
        cols = {
            "Open":   opens[tkr],
            "Close":  closes[tkr],
            "High":   highs[tkr],
            "Low":    lows[tkr],
        }
        if volumes is not None and tkr in volumes.columns:
            cols["Volume"] = volumes[tkr]
        df = pd.DataFrame(cols).dropna(subset=["Open","Close","High","Low"])
        if len(df) < 40:
            continue
        data[tkr] = df

    return data


def load_data(force_refresh=False):
    """Carga datos con cache pickle (se refresca cada CACHE_MAX_HOURS horas)."""
    if not force_refresh and CACHE_FILE.exists():
        age_h = (time.time() - CACHE_FILE.stat().st_mtime) / 3600
        if age_h < CACHE_MAX_HOURS:
            print(f"  Cache valido ({age_h:.1f}h). Cargando...")
            with open(CACHE_FILE, "rb") as f:
                cached = pickle.load(f)
            # Validar que el cache incluya columnas Open y Volume
            _sample = next((v for v in cached.values() if isinstance(v, pd.DataFrame)), None)
            if _sample is not None and "Open" in _sample.columns and "Volume" in _sample.columns:
                return cached
            print("  Cache incompleto (sin Open o Volume). Re-descargando...")

    data = _load_from_yf()
    with open(CACHE_FILE, "wb") as f:
        pickle.dump(data, f)
    return data


# ─────────────────────────────────────────────────────────────────────────────
# INDICADORES AUXILIARES
# ─────────────────────────────────────────────────────────────────────────────
def calc_ema(series, span):
    return series.ewm(span=span, adjust=False).mean()


def calc_sma(series, period):
    return series.rolling(period).mean()


def calc_mansfield(stock_close, spy_close, ma_len=MS_MA_LEN):
    """Mansfield RS = (ratio / media_N_ratio - 1) × 100."""
    spy_aligned = spy_close.reindex(stock_close.index, method="ffill")
    ratio = (stock_close / spy_aligned) * 100.0
    ma    = ratio.rolling(ma_len, min_periods=int(ma_len * 0.6)).mean()
    return ((ratio / ma) - 1.0) * 100.0


def calc_stage(closes_series, sma30_series):
    """
    Weinstein Stage analysis (1-4) basado en SMA30 semanal.

    Stage 1 – Basing:    precio ≈ SMA30, SMA30 plana
    Stage 2 – Advancing: precio > SMA30, SMA30 subiendo    ← objetivo
    Stage 3 – Topping:   precio ≈ SMA30, SMA30 aplanando tras subir
    Stage 4 – Declining: precio < SMA30, SMA30 bajando

    Devuelve 1, 2, 3 o 4. None si datos insuficientes.
    """
    sma = sma30_series.dropna()
    if len(sma) < 6:
        return None
    close   = float(closes_series.iloc[-1])
    sma_now = float(sma.iloc[-1])
    sma_ago = float(sma.iloc[-5])   # ~4 semanas atrás
    if sma_ago == 0:
        return None
    slope_pct = (sma_now - sma_ago) / sma_ago * 100   # % cambio en 4 semanas
    above     = close > sma_now
    if above and slope_pct > 0.5:
        return 2
    elif not above and slope_pct < -0.5:
        return 4
    elif above:
        return 3   # precio sobre SMA pero SMA plana → posible techo
    else:
        return 1   # precio bajo SMA y SMA plana → base


# ─────────────────────────────────────────────────────────────────────────────
# ALGORITMO DE PIVOTES  (equivalente exacto al Pine Script)
# ─────────────────────────────────────────────────────────────────────────────
def _find_pivot_lows(lows_arr, L):
    """
    Devuelve lista de (bar_index, value) con todos los pivot lows confirmados
    con L barras de confirmación a cada lado.
    """
    n = len(lows_arr)
    pivots = []
    for i in range(L, n - L):
        v = lows_arr[i]
        if all(v < lows_arr[i - j] for j in range(1, L + 1)) and \
           all(v < lows_arr[i + j] for j in range(1, L + 1)):
            pivots.append((i, v))
    return pivots


def find_aspb_setup(df, ema_len=EMA_LEN, min_len=MIN_LEN, max_len=MAX_LEN,
                    fib_1st=FIB_1ST, fib_tp1=FIB_TP1, fib_tp2=FIB_TP2):
    """
    Busca el mejor setup ASP-B activo en el último bar completo.

    Lógica idéntica al Pine Script:
    - Itera longitudes MIN_LEN … MAX_LEN
    - Para cada longitud busca el par (HL anterior, HL actual) más reciente
    - Verifica que ningún bar posterior haya violado el HL
    - Verifica que el último mínimo sea > EMA21(Low)
    - Ganador = el que tenga el 2nd Pivot (nivel de entrada) más bajo

    Devuelve None si no hay setup válido.
    """
    if len(df) < max_len * 4 + 20:
        return None

    closes = df["Close"].values
    highs  = df["High"].values
    lows   = df["Low"].values
    n      = len(lows)

    ema_l = calc_ema(df["Low"],   ema_len).values
    ema_c = calc_ema(df["Close"], ema_len).values

    # Filtro rápido: último mínimo sobre EMA21(Low)
    if lows[-1] < ema_l[-1]:
        return None

    best = None

    for L in range(min_len, max_len + 1):
        pivots = _find_pivot_lows(lows, L)
        if len(pivots) < 2:
            continue

        # Buscar el par HL más reciente que siga válido
        for k in range(len(pivots) - 1, 0, -1):
            curr_i, curr_p = pivots[k]
            prev_i, prev_p = pivots[k - 1]

            # Debe ser un Higher Low
            if curr_p <= prev_p:
                continue

            # Mínimo del segmento entre los dos pivots (necesitamos al menos 1 bar)
            if curr_i - prev_i < 2:
                continue

            # 2nd Pivot = máximo del High entre los dos lows (exclusivo)
            seg_highs = highs[prev_i + 1 : curr_i]
            if len(seg_highs) == 0:
                continue
            second_px = float(np.max(seg_highs))

            # Validar que ningún bar desde el HL hasta hoy haya cerrado bajo curr_p
            if np.any(lows[curr_i + 1 :] < curr_p):
                continue  # HL invalidado

            # Validar EMA en el momento del pivot
            if lows[curr_i] < ema_l[curr_i]:
                continue

            fib_rng = second_px - curr_p
            if fib_rng <= 0:
                continue

            # Ganador: el que tenga el HL más reciente (barra con índice más alto).
            # Criterio anterior era "entry más bajo" — incorrecto cuando existe un setup
            # antiguo con entry bajo y un setup nuevo con entry más alto: el nuevo
            # es el relevante para el trading actual y debe coincidir con TradingView.
            if best is None or curr_i > best["hl_bar"]:
                best = {
                    "len":       L,
                    "ll_px":     prev_p,   # Low anterior (el LL antes del HL)
                    "ll_bar":    prev_i,   # Índice de barra del LL
                    "hl_px":     curr_p,
                    "hl_bar":    curr_i,
                    "second_px": second_px,
                    "first_px":  curr_p + fib_rng * fib_1st,
                    "tp1_px":    curr_p + fib_rng * fib_tp1,
                    "tp2_px":    curr_p + fib_rng * fib_tp2,
                    "ema_close": ema_c[-1],
                }
            break  # Encontrado par válido para este L; pasar al siguiente L

    return best


# ─────────────────────────────────────────────────────────────────────────────
# SCREENING POR TICKER
# ─────────────────────────────────────────────────────────────────────────────
def _calc_vol_ratio(df, window=20):
    """
    Ratio de volumen de la última barra vs media de las `window` barras previas.
    Devuelve float o np.nan si no hay datos de volumen.
    """
    if "Volume" not in df.columns or len(df) < window + 2:
        return np.nan
    vols     = df["Volume"].dropna()
    if len(vols) < window + 2:
        return np.nan
    last_vol = float(vols.iloc[-1])
    ma_vol   = float(vols.iloc[-(window + 1):-1].mean())
    if ma_vol <= 0:
        return np.nan
    return round(last_vol / ma_vol, 2)


def _calc_rs_near_high(stock_close, spy_close, window=52, threshold_pct=5.0):
    """
    Devuelve True si la RS line está dentro del threshold_pct de su máximo
    de las últimas `window` semanas.
    """
    spy_aligned = spy_close.reindex(stock_close.index, method="ffill")
    rs_line  = stock_close / spy_aligned
    rs_valid = rs_line.dropna()
    if len(rs_valid) < window:
        return False
    rs_now = float(rs_valid.iloc[-1])
    rs_max = float(rs_valid.iloc[-window:].max())
    if rs_max <= 0:
        return False
    return (rs_max - rs_now) / rs_max * 100 <= threshold_pct


def _detect_failed_breakout(df, setup):
    """
    Detecta si el precio ya superó el entry DESPUÉS del HL y luego volvió a caer.

    Un 'failed breakout' ocurre cuando, tras formarse el HL:
      - El Close de alguna barra posterior supera el entry (la orden se habría activado)
      - Y el Close de una barra aún más posterior cae por debajo del entry (salió de la posición)

    Devuelve True si se detecta ese patrón; False en caso contrario.
    """
    hl_bar = setup["hl_bar"]
    entry  = setup["second_px"]
    closes = df["Close"].values

    post_hl = closes[hl_bar + 1:]
    for i, c in enumerate(post_hl):
        if c > entry:
            # ¿Hay algún cierre posterior por debajo del entry?
            if np.any(post_hl[i + 1:] < entry):
                return True
    return False


def screen_ticker(sym, df, spy_df):
    """
    Analiza un ticker. Devuelve dict con resultado o None si no hay setup.

    Filtros de calidad aplicados:
      1. Stage 2 obligatorio (Weinstein): precio > SMA30 y SMA30 subiendo.
      2. TP1 mínimo MIN_TP1_PCT %: el potencial al primer objetivo debe ser significativo.
      3. Stop mínimo MIN_STOP_PCT %: evitar rangos tan estrechos que el ruido active el stop.
      4. failed_breakout: columna informativa (no excluye, pero se marca en Telegram).
    """
    setup = find_aspb_setup(df)
    if setup is None:
        return None

    closes   = df["Close"].values
    last_c   = float(closes[-1])
    prev_c   = float(closes[-2]) if len(closes) >= 2 else last_c
    second   = setup["second_px"]
    first    = setup["first_px"]
    tp1      = setup["tp1_px"]
    tp2      = setup["tp2_px"]

    # ── Filtros de calidad de rango ────────────────────────────────────────────
    tp1_pct_val  = (tp1   - second) / second * 100.0
    stop_pct_val = (second - first)  / second * 100.0

    if tp1_pct_val  < MIN_TP1_PCT:
        return None   # Potencial demasiado pequeño (< 7 % al TP1)
    if stop_pct_val < MIN_STOP_PCT:
        return None   # Rango demasiado estrecho (< 3 % de stop)

    # ── Tipo de señal ─────────────────────────────────────────────────────────
    if last_c > second and prev_c <= second:
        signal = "BUY"          # Breakout confirmado este cierre semanal
    elif last_c > second:
        signal = "IN_TRADE"     # Ya en posición (precio sobre entry)
    elif last_c >= second * 0.97:
        signal = "NEAR"         # Dentro del 3%
    else:
        signal = "SETUP"        # Setup activo, aún lejos

    dist_pct  = (second - last_c) / second * 100.0   # >0 = below entry, <0 = above entry
    risk_pct  = stop_pct_val
    rr        = (tp1 - second) / (second - first) if (second - first) > 0 else np.nan
    pnl_pct   = (last_c / second - 1.0) * 100.0 if signal == "IN_TRADE" else np.nan

    # ── SMA30 semanal ─────────────────────────────────────────────────────────
    sma30     = calc_sma(df["Close"], SMA_LEN)
    sma30_val = float(sma30.iloc[-1]) if not np.isnan(sma30.iloc[-1]) else np.nan
    sma30_up  = bool(sma30.iloc[-1] > sma30.iloc[-2]) if (len(sma30) >= 2 and not np.isnan(sma30.iloc[-1]) and not np.isnan(sma30.iloc[-2])) else False

    # ── Mansfield RS ──────────────────────────────────────────────────────────
    mansfield   = calc_mansfield(df["Close"], spy_df["Close"])
    ms_val      = float(mansfield.iloc[-1]) if not np.isnan(mansfield.iloc[-1]) else np.nan
    ms_positive = bool(ms_val > 0) if not np.isnan(ms_val) else False

    # Filtro Mansfield ≥ 5 %  (backtest: elimina todos los stops, conserva 7/8 wins)
    ms_filter_ok = bool(ms_val >= 5.0) if not np.isnan(ms_val) else False

    # ── RS Line cerca de máximos 52 semanas ───────────────────────────────────
    rs_near_high = _calc_rs_near_high(df["Close"], spy_df["Close"])

    # ── Volumen del breakout vs media 20 semanas ──────────────────────────────
    vol_ratio = _calc_vol_ratio(df)

    # ── Weinstein Stage ───────────────────────────────────────────────────────
    stage = calc_stage(df["Close"], sma30)

    # Filtro Stage 2 obligatorio para BUY/NEAR/SETUP
    # IN_TRADE se mantiene siempre (ya estamos dentro)
    if signal != "IN_TRADE" and stage != 2:
        return None   # Solo operamos en Stage 2 (precio > SMA30 subiendo)

    # ── Failed breakout (informativo) ─────────────────────────────────────────
    # True si tras el HL el precio cruzó por encima del entry y luego volvió a caer.
    # No excluye la señal, pero se marca en Telegram con aviso.
    failed_breakout = _detect_failed_breakout(df, setup)

    return {
        "ticker":           sym,
        "index":            INDEX_MAP.get(sym, "SP500"),
        "signal":           signal,
        "close":            last_c,
        "entry":            second,
        "stop":             first,
        "tp1":              tp1,
        "tp2":              tp2,
        "dist_pct":         dist_pct,
        "risk_pct":         risk_pct,
        "rr":               rr,
        "pnl_pct":          pnl_pct,
        "sma30":            sma30_val,
        "sma30_up":         sma30_up,
        "mansfield":        ms_val,
        "ms_positive":      ms_positive,
        "ms_filter_ok":     ms_filter_ok,    # Mansfield >= 5 %
        "rs_near_high":     rs_near_high,    # RS line < 5% de máximos 52s
        "vol_ratio":        vol_ratio,       # Vol breakout / media 20s
        "fib_len":          setup["len"],
        "stage":            stage,
        "failed_breakout":  failed_breakout, # Hubo intento fallido post-HL
        # Coordenadas para gráficos
        "hl_bar":           setup["hl_bar"],
        "ll_bar":           setup.get("ll_bar"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# RUNNER PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────
def run_screener(force_refresh=False):
    print("\n===================================================")
    print("  ASP-B Weekly Screener  --  S&P 500 + NASDAQ")
    print("===================================================")

    data = load_data(force_refresh=force_refresh)
    spy_df = data.get(BENCHMARK)
    if spy_df is None:
        raise RuntimeError("No se pudo descargar SPY. Verifica tu conexión.")

    results = []
    errors  = []
    print(f"\n  Analizando {len(ALL_TICKERS)} tickers...")

    for sym in ALL_TICKERS:
        df = data.get(sym)
        if df is None:
            errors.append(sym)
            continue
        try:
            r = screen_ticker(sym, df, spy_df)
            if r:
                results.append(r)
        except Exception as e:
            errors.append(sym)

    df_res = pd.DataFrame(results)
    if df_res.empty:
        print("  Sin resultados.")
        return df_res

    # Ordenar: BUY → NEAR → IN_TRADE → SETUP, luego por dist_pct asc
    order = {"BUY": 0, "NEAR": 1, "IN_TRADE": 2, "SETUP": 3}
    df_res["_ord"] = df_res["signal"].map(order)
    df_res = df_res.sort_values(["_ord", "dist_pct"]).drop(columns="_ord")

    # Resumen terminal
    counts = df_res["signal"].value_counts()
    print(f"\n  +----------------------------------+")
    print(f"  |  BUY  (compras esta semana)  {counts.get('BUY', 0):>3} |")
    print(f"  |  NEAR (proximas entradas)    {counts.get('NEAR', 0):>3} |")
    print(f"  |  SETUP (estructuras)         {counts.get('SETUP', 0):>3} |")
    print(f"  |  IN_TRADE                    {counts.get('IN_TRADE', 0):>3} |")
    print(f"  +----------------------------------+")

    if errors:
        print(f"\n  [!] {len(errors)} tickers sin datos: {', '.join(errors[:10])}")

    return df_res


# ─────────────────────────────────────────────────────────────────────────────
# GENERADOR HTML — estructura idéntica al mensaje Telegram
# ─────────────────────────────────────────────────────────────────────────────

def _tier_val(r):
    """Tier: 2=Confluencia (MS≥5%+RS near high), 1=Filtro OK (MS≥5%), 0=sin filtro."""
    if not bool(r.get("ms_filter_ok", False)):
        return 0
    if bool(r.get("rs_near_high", False)):
        return 2
    return 1

def _fmt(v, decimals=2, prefix="", suffix=""):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    return f"{prefix}{v:,.{decimals}f}{suffix}"

def _row_html(r):
    sig   = r["signal"]
    tier  = _tier_val(r)

    # Color de la fila según tier
    if tier == 2:
        left_col = "#FFD700"
    elif tier == 1:
        left_col = "#10b981"
    elif sig == "IN_TRADE":
        left_col = "#8b5cf6"
    elif sig == "SETUP":
        left_col = "#3b82f6"
    else:
        left_col = "#334155"

    # Tier badge
    if tier == 2:
        tier_badge = '<span class="tier-badge tier-c">⭐ CONFLUENCIA</span>'
    elif tier == 1:
        tier_badge = '<span class="tier-badge tier-f">✅ FILTRO OK</span>'
    else:
        tier_badge = '<span class="tier-badge tier-n">—</span>'

    # Signal badge
    sig_map = {
        "BUY":      '<span class="badge buy">🔥 BUY</span>',
        "NEAR":     '<span class="badge near">👀 NEAR</span>',
        "IN_TRADE": '<span class="badge in">📈 IN</span>',
        "SETUP":    '<span class="badge setup">📊 SETUP</span>',
    }
    badge_html = sig_map.get(sig, "")

    # Stage badge
    stage = r.get("stage")
    if stage == 2:
        stage_badge = '<span class="mini-badge badge-ok">Stage 2</span>'
    elif stage == 4:
        stage_badge = '<span class="mini-badge badge-ko">Stage 4</span>'
    else:
        stage_badge = f'<span class="mini-badge" style="color:#94a3b8">Stage {stage or "?"}</span>'

    dist_v = r["dist_pct"]
    rr_v   = r["rr"]
    pnl_v  = r["pnl_pct"]
    ms_v   = r["mansfield"]

    dist_col = "#10b981" if abs(dist_v) < 1.5 else "#f59e0b" if abs(dist_v) < 3.5 else "#94a3b8"
    rr_col   = ("#10b981" if (not (isinstance(rr_v, float) and np.isnan(rr_v)) and rr_v >= 2.5)
                else "#f59e0b" if (not (isinstance(rr_v, float) and np.isnan(rr_v)) and rr_v >= 1.5)
                else "#ef4444")
    pnl_col  = "#10b981" if (not (isinstance(pnl_v, float) and np.isnan(pnl_v)) and pnl_v >= 0) else "#ef4444"
    ms_col   = "#10b981" if (not (isinstance(ms_v, float) and np.isnan(ms_v)) and ms_v >= 5) else \
               "#f59e0b" if (not (isinstance(ms_v, float) and np.isnan(ms_v)) and ms_v >= 0) else "#ef4444"

    fb = r.get("failed_breakout", False)
    fb_badge = ('<span class="mini-badge badge-warn" title="Breakout fallido previo">⚠️ FBrk</span>'
                if fb else "")

    idx = r.get("index", "SP500")
    idx_badge = ('<span class="idx-sp">SP500</span>' if idx == "SP500"
                 else '<span class="idx-nq">NASDAQ</span>' if idx == "NASDAQ"
                 else '<span class="idx-eu">EU-ADR</span>')

    pnl_txt = (f'<span style="color:{pnl_col}">{_fmt(pnl_v,1,suffix="%")}</span>'
               if sig == "IN_TRADE" else "—")

    return f"""
    <tr style="border-left:3px solid {left_col}">
      <td class="ticker-cell"><strong>{r['ticker']}</strong> {fb_badge}</td>
      <td>{tier_badge}</td>
      <td>{idx_badge}</td>
      <td>{badge_html}</td>
      <td>{stage_badge}</td>
      <td class="num" style="color:{ms_col};font-weight:600">{_fmt(ms_v,1,suffix="%")}</td>
      <td class="num">{_fmt(r['close'],2)}</td>
      <td class="num entry-col">{_fmt(r['entry'],2)}</td>
      <td class="num" style="color:{dist_col}">{_fmt(dist_v,1,suffix="%")}</td>
      <td class="num stop-col">{_fmt(r['stop'],2)}</td>
      <td class="num">{_fmt(r['risk_pct'],1,suffix="%")}</td>
      <td class="num tp1-col">{_fmt(r['tp1'],2)}</td>
      <td class="num tp2-col">{_fmt(r['tp2'],2)}</td>
      <td class="num" style="color:{rr_col}">{_fmt(rr_v,2)}</td>
      <td>{pnl_txt}</td>
    </tr>"""

_TABLE_HEADER = """
  <thead><tr>
    <th>Ticker</th><th>Tier</th><th>Idx</th><th>Señal</th><th>Stage</th>
    <th>Mansfield</th><th>Close</th><th>Entry</th><th>Dist%</th>
    <th>Stop</th><th>Riesgo%</th><th>TP1</th><th>TP2</th><th>R:R</th><th>P&amp;L</th>
  </tr></thead>"""

def _table_html(df_s):
    rows = "".join(_row_html(r) for _, r in df_s.iterrows())
    return f'<div class="table-wrap"><table>{_TABLE_HEADER}<tbody>{rows}</tbody></table></div>'

def _section_html(df_s, title, icon, color, subtitle=""):
    sub = f'<div class="sec-sub">{subtitle}</div>' if subtitle else ""
    if df_s.empty:
        return (f'<div class="section">'
                f'<div class="section-title" style="color:{color}">{icon} {title}'
                f'<span class="count">0</span></div>{sub}'
                f'<p class="empty-msg">Sin señales en este momento.</p></div>')
    return (f'<div class="section">'
            f'<div class="section-title" style="color:{color}">{icon} {title}'
            f'<span class="count">{len(df_s)}</span></div>{sub}'
            f'{_table_html(df_s)}</div>')


def generate_html(df, run_dt=None):
    if run_dt is None:
        run_dt = datetime.datetime.now()
    date_str = run_dt.strftime("%Y-%m-%d")
    time_str = run_dt.strftime("%H:%M")

    if df.empty:
        df2 = df.copy()
        df2["tier"] = 0
    else:
        df2 = df.copy()
        df2["tier"] = df2.apply(_tier_val, axis=1)

    # ── Clasificación igual que Telegram ─────────────────────────────────────
    # ÓRDENES: BUY + NEAR con tier≥1, dist dentro del 3%
    ordenes = df2[
        df2["signal"].isin(["BUY", "NEAR"]) &
        (df2["dist_pct"] >= -3.0) &
        (df2["tier"] >= 1)
    ].sort_values(["tier", "dist_pct"], ascending=[False, True])

    confluencia = ordenes[ordenes["tier"] == 2]
    filtro_ok   = ordenes[ordenes["tier"] == 1]

    # VIGILANCIA: SETUP cerca del entry (≤8%), tier≥1
    vigilancia = df2[
        (df2["signal"] == "SETUP") &
        (df2["dist_pct"] <= 8.0) &
        (df2["tier"] >= 1)
    ].sort_values("dist_pct")

    # EN POSICIÓN: IN_TRADE con tier≥1
    en_pos = df2[
        (df2["signal"] == "IN_TRADE") &
        (df2["tier"] >= 1)
    ].sort_values(["tier", "dist_pct"], ascending=[False, True])

    # Stats
    n_conf = len(confluencia)
    n_filt = len(filtro_ok)
    n_vig  = len(vigilancia)
    n_pos  = len(en_pos)
    n_ord  = n_conf + n_filt

    # ── Secciones HTML ────────────────────────────────────────────────────────
    def _ord_section():
        if n_ord == 0:
            return ('<div class="section"><div class="section-title" style="color:#10b981">'
                    '🎯 ÓRDENES A COLOCAR <span class="count">0</span></div>'
                    '<p class="empty-msg">Sin señales de calidad esta semana. Revisa el sábado que viene.</p></div>')
        parts = []
        if n_conf > 0:
            parts.append(f'<div class="subsec-title">⭐ CONFLUENCIA ({n_conf})'
                         '<span class="subsec-hint">MS ≥5% + RS en máximos 52s</span></div>'
                         + _table_html(confluencia))
        if n_filt > 0:
            parts.append(f'<div class="subsec-title filt">✅ FILTRO OK ({n_filt})'
                         '<span class="subsec-hint">MS ≥5%</span></div>'
                         + _table_html(filtro_ok))
        inner = "".join(parts)
        return (f'<div class="section">'
                f'<div class="section-title" style="color:#10b981">🎯 ÓRDENES A COLOCAR'
                f'<span class="count">{n_ord}</span></div>'
                f'<p class="sec-note">Coloca una orden <strong>BUY STOP</strong> al precio ENTRY antes del lunes.</p>'
                f'{inner}</div>')

    sec_ord = _ord_section()
    sec_vig = _section_html(vigilancia, "VIGILANCIA — próximas roturas", "👀", "#f59e0b",
                            '<p class="sec-note">Estructuras formadas. Pueden activarse la próxima semana.</p>')
    sec_pos = _section_html(en_pos, "YA EN POSICIÓN", "📈", "#8b5cf6",
                            '<p class="sec-note">Precio ya por encima del entry. Gestiona el stop activo.</p>')

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ASP-B Weekly — {date_str}</title>
<style>
  :root {{
    --bg:#0f172a; --card:#1e293b; --card2:#334155;
    --text:#e2e8f0; --text2:#94a3b8; --border:#334155;
    --gold:#FFD700; --green:#10b981; --red:#ef4444;
    --orange:#f59e0b; --blue:#3b82f6; --purple:#8b5cf6;
  }}
  *{{margin:0;padding:0;box-sizing:border-box}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
        background:var(--bg);color:var(--text);font-size:13px}}

  /* HEADER */
  .header{{background:linear-gradient(135deg,#1e293b 0%,#0f172a 100%);
           border-bottom:2px solid #FFD70040;padding:20px;text-align:center}}
  .header h1{{font-size:1.5rem;font-weight:800;letter-spacing:1px;
              background:linear-gradient(90deg,#FFD700,#10b981);
              -webkit-background-clip:text;-webkit-text-fill-color:transparent}}
  .header .sub{{color:var(--text2);font-size:0.8rem;margin-top:5px}}

  /* STATS */
  .stats-bar{{display:flex;gap:12px;flex-wrap:wrap;padding:16px 20px;
              background:var(--card);border-bottom:1px solid var(--border);
              justify-content:center}}
  .stat-card{{text-align:center;min-width:100px;padding:12px 18px;
              border-radius:12px;background:var(--bg);border:1px solid var(--border)}}
  .stat-card .v{{font-size:1.8rem;font-weight:800}}
  .stat-card .l{{font-size:0.65rem;color:var(--text2);text-transform:uppercase;margin-top:3px}}
  .v-conf{{color:var(--gold)}} .v-filt{{color:var(--green)}}
  .v-vig{{color:var(--orange)}} .v-pos{{color:var(--purple)}}

  /* CONTAINER */
  .container{{max-width:1500px;margin:0 auto;padding:20px 14px}}

  /* SECTION */
  .section{{margin-bottom:32px;border-radius:8px;
            border:1px solid var(--border);overflow:hidden}}
  .section-title{{font-size:1rem;font-weight:700;padding:10px 14px;
                  background:rgba(255,255,255,.04);border-bottom:1px solid var(--border);
                  display:flex;align-items:center;gap:10px}}
  .count{{background:rgba(255,255,255,.12);border-radius:12px;
          padding:2px 10px;font-size:0.75rem;font-weight:600}}
  .sec-note{{font-size:0.78rem;color:var(--text2);padding:6px 14px 4px;
             border-bottom:1px solid var(--border)}}
  .subsec-title{{font-size:0.85rem;font-weight:700;padding:8px 14px;
                 background:rgba(255,215,0,.06);color:var(--gold);
                 border-top:1px solid #FFD70020;border-bottom:1px solid #FFD70020;
                 display:flex;align-items:center;gap:8px}}
  .subsec-title.filt{{background:rgba(16,185,129,.06);color:var(--green);
                      border-top-color:#10b98120;border-bottom-color:#10b98120}}
  .subsec-hint{{font-size:0.68rem;font-weight:400;color:var(--text2)}}
  .empty-msg{{color:var(--text2);padding:18px 14px;font-size:0.85rem}}

  /* TABLE */
  .table-wrap{{overflow-x:auto}}
  table{{width:100%;border-collapse:collapse;min-width:1000px}}
  thead tr{{background:var(--card2)}}
  th{{padding:7px 10px;text-align:right;color:var(--text2);font-size:0.68rem;
      font-weight:600;text-transform:uppercase;letter-spacing:.4px;
      border-bottom:1px solid var(--border);white-space:nowrap}}
  th:first-child,th:nth-child(2),th:nth-child(3),th:nth-child(4),th:nth-child(5){{text-align:left}}
  td{{padding:8px 10px;border-bottom:1px solid rgba(51,65,85,.4);vertical-align:middle}}
  tr:hover{{background:rgba(255,255,255,.025)}}
  .num{{text-align:right;font-variant-numeric:tabular-nums}}
  .ticker-cell{{font-size:0.95rem;font-weight:700;white-space:nowrap}}
  .entry-col{{color:#60a5fa;font-weight:600}}
  .stop-col{{color:#fca5a5}}
  .tp1-col{{color:#6ee7b7}}
  .tp2-col{{color:#a7f3d0}}

  /* BADGES */
  .badge{{display:inline-block;padding:2px 7px;border-radius:4px;
          font-size:0.68rem;font-weight:700;white-space:nowrap}}
  .badge.buy{{background:rgba(16,185,129,.2);color:#10b981;border:1px solid #10b98130}}
  .badge.near{{background:rgba(245,158,11,.2);color:#f59e0b;border:1px solid #f59e0b30}}
  .badge.in{{background:rgba(139,92,246,.2);color:#a78bfa;border:1px solid #8b5cf630}}
  .badge.setup{{background:rgba(59,130,246,.2);color:#60a5fa;border:1px solid #3b82f630}}

  .tier-badge{{display:inline-block;padding:2px 8px;border-radius:4px;
               font-size:0.68rem;font-weight:700;white-space:nowrap}}
  .tier-c{{background:rgba(255,215,0,.15);color:#FFD700;border:1px solid #FFD70030}}
  .tier-f{{background:rgba(16,185,129,.15);color:#10b981;border:1px solid #10b98130}}
  .tier-n{{color:#475569;font-size:0.65rem}}

  .mini-badge{{display:inline-block;padding:2px 6px;border-radius:4px;font-size:0.65rem;font-weight:700}}
  .badge-ok{{background:rgba(16,185,129,.15);color:#10b981}}
  .badge-ko{{background:rgba(239,68,68,.15);color:#ef4444}}
  .badge-warn{{background:rgba(245,158,11,.15);color:#f59e0b}}
  .idx-sp{{color:#60a5fa;font-size:0.68rem;font-weight:600}}
  .idx-nq{{color:#a78bfa;font-size:0.68rem;font-weight:600}}
  .idx-eu{{color:#f59e0b;font-size:0.68rem;font-weight:600}}

  /* FOOTER */
  .footer{{text-align:center;color:var(--text2);font-size:0.7rem;
           padding:20px;border-top:1px solid var(--border);margin-top:24px}}
</style>
</head>
<body>

<div class="header">
  <h1>⚡ ASP-B Weekly Screener</h1>
  <div class="sub">Estrategia Oratnek · S&amp;P 500 · NASDAQ · EU-ADR — {date_str} · {time_str}</div>
</div>

<div class="stats-bar">
  <div class="stat-card"><div class="v v-conf">{n_conf}</div><div class="l">⭐ Confluencia</div></div>
  <div class="stat-card"><div class="v v-filt">{n_filt}</div><div class="l">✅ Filtro OK</div></div>
  <div class="stat-card"><div class="v v-vig">{n_vig}</div><div class="l">👀 Vigilancia</div></div>
  <div class="stat-card"><div class="v v-pos">{n_pos}</div><div class="l">📈 En posición</div></div>
</div>

<div class="container">
  {sec_ord}
  {sec_vig}
  {sec_pos}
</div>

<div class="footer">
  ASP-B Weekly Strategy v2 · Estrategia Oratnek · Pivot lengths {MIN_LEN}–{MAX_LEN} · EMA{EMA_LEN}<br>
  Fib Stop 0.618 · TP1 1.764 · TP2 2.618 · Mansfield RS vs SPY (MA{MS_MA_LEN}w) · MS ≥5% · Stage 2 · TP1 ≥7% · Stop ≥3%<br>
  <em>No constituye asesoramiento financiero. Verificar siempre en gráfico semanal.</em>
</div>
</body>
</html>"""

    return html


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="ASP-B Weekly Screener")
    parser.add_argument("--refresh", action="store_true",
                        help="Forzar re-descarga aunque el cache sea reciente")
    parser.add_argument("--no-open", action="store_true",
                        help="No abrir el HTML en el navegador automáticamente")
    args = parser.parse_args()

    run_dt  = datetime.datetime.now()
    df_res  = run_screener(force_refresh=args.refresh)

    # Guardar HTML
    date_str  = run_dt.strftime("%Y-%m-%d")
    html_path = REPORT_DIR / f"aspb_screener_{date_str}.html"
    html_content = generate_html(df_res, run_dt)
    html_path.write_text(html_content, encoding="utf-8")
    print(f"\n  Reporte: {html_path}")

    # Tambien guardar el ultimo en ruta fija para facil acceso
    latest_path = REPORT_DIR / "aspb_screener_LATEST.html"
    latest_path.write_text(html_content, encoding="utf-8")

    # Guardar CSV para revision
    csv_path = REPORT_DIR / f"aspb_screener_{date_str}.csv"
    if not df_res.empty:
        df_res.to_csv(csv_path, index=False)
        print(f"  CSV:     {csv_path}")

    # Abrir en navegador
    if not args.no_open:
        webbrowser.open(latest_path.as_uri())
        print(f"\n  Abriendo en el navegador...")

    print()
    return df_res


if __name__ == "__main__":
    main()
