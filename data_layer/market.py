"""
Descarga de precios + cálculo de indicadores (WMA30, ATR, Mansfield RS).

Filosofía:
  - Caché en disco para no depender de yfinance en caliente.
  - Reintentos con back-off en descargas.
  - NUNCA propaga la excepción: si un ticker falla, devuelve None
    y el llamante decide qué hacer (normalmente, marcarlo como stale).
"""
from __future__ import annotations

import logging
import pickle
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf

from .config import (
    ATR_PERIOD,
    CACHE_DAILY,
    CACHE_WEEKLY,
    MANSFIELD_MA,
    QQQ_TICKER,
    WMA_PERIOD,
)

log = logging.getLogger(__name__)

# =====================================================================
# Caché diario (precios de los últimos 2 años, resolución daily)
# =====================================================================
_CACHE_DAILY_TTL = timedelta(days=1)


def _load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open("rb") as f:
            return pickle.load(f)
    except Exception as e:
        log.warning("Caché %s corrupto (%s). Reinicializo.", path.name, e)
        return {}


def _save_cache(path: Path, obj: dict) -> None:
    try:
        with path.open("wb") as f:
            pickle.dump(obj, f)
    except Exception as e:
        log.warning("No pude guardar caché %s: %s", path.name, e)


def _is_fresh(saved_at: datetime, ttl: timedelta) -> bool:
    return datetime.utcnow() - saved_at < ttl


# =====================================================================
# Descarga robusta
# =====================================================================
def _download_with_retry(ticker: str, period: str, interval: str, tries: int = 3) -> Optional[pd.DataFrame]:
    """Descarga de yfinance con back-off exponencial. Devuelve None si falla todo."""
    for attempt in range(1, tries + 1):
        try:
            df = yf.download(
                ticker, period=period, interval=interval,
                progress=False, auto_adjust=True, threads=False,
            )
            if df is None or df.empty:
                raise ValueError("descarga vacía")
            # yfinance > 0.2 devuelve MultiIndex → aplanar
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
            return df
        except Exception as e:
            if attempt == tries:
                log.warning("yfinance falló para %s tras %d intentos: %s", ticker, tries, e)
                return None
            time.sleep(2 ** attempt)
    return None


def get_daily(ticker: str) -> Optional[pd.DataFrame]:
    """Precios diarios de los últimos 2 años. Caché con TTL 1 día."""
    cache = _load_cache(CACHE_DAILY)
    entry = cache.get(ticker)

    if entry and _is_fresh(entry["saved_at"], _CACHE_DAILY_TTL):
        return entry["df"]

    df = _download_with_retry(ticker, period="2y", interval="1d")
    if df is None:
        # fallback a caché aunque esté viejo
        return entry["df"] if entry else None

    cache[ticker] = {"df": df, "saved_at": datetime.utcnow()}
    _save_cache(CACHE_DAILY, cache)
    return df


def get_weekly(ticker: str) -> Optional[pd.DataFrame]:
    """
    Precios semanales. Usa el caché que ya comparte weekly_engine.py
    si el ticker está, o descarga bajo demanda.
    """
    cache = _load_cache(CACHE_WEEKLY)

    if ticker in cache:
        # caché de weekly_engine: {ticker: DataFrame} directamente
        df = cache[ticker]
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df

    df = _download_with_retry(ticker, period="5y", interval="1wk")
    if df is None:
        return None

    cache[ticker] = df
    _save_cache(CACHE_WEEKLY, cache)
    return df


# =====================================================================
# Indicadores
# =====================================================================
def wma(series: pd.Series, period: int) -> pd.Series:
    """Weighted moving average (Weinstein)."""
    weights = np.arange(1, period + 1)

    def _calc(window: np.ndarray) -> float:
        return float(np.dot(window, weights) / weights.sum())

    return series.rolling(period).apply(_calc, raw=True)


def atr(df: pd.DataFrame, period: int = ATR_PERIOD) -> pd.Series:
    """Average True Range clásico (Wilder)."""
    h, l, c = df["High"], df["Low"], df["Close"]
    prev = c.shift(1)
    tr = pd.concat([(h - l), (h - prev).abs(), (l - prev).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def mansfield_rs(stock_close: pd.Series, index_close: pd.Series, ma: int = MANSFIELD_MA) -> pd.Series:
    """
    Mansfield Relative Strength:
      MS_t = ((RS_t / MA(RS, ma)) - 1) * 100
    donde RS = close_stock / close_index.
    """
    aligned = pd.concat([stock_close, index_close], axis=1, join="inner").dropna()
    if aligned.empty or len(aligned) < ma:
        return pd.Series(dtype=float)
    rs = aligned.iloc[:, 0] / aligned.iloc[:, 1]
    rs_ma = rs.rolling(ma).mean()
    return ((rs / rs_ma) - 1.0) * 100.0


# =====================================================================
# Snapshot — lo que el enricher realmente consume
# =====================================================================
@dataclass
class TickerSnapshot:
    ticker: str
    current_price: float       # Último cierre disponible (daily)
    wma30_weekly: float        # WMA30 del semanal más reciente
    wma30_weekly_at: Optional[float]  # WMA30 en la fecha_compra (si aplica)
    atr_weekly: float          # ATR semanal actual
    mansfield: float           # Mansfield RS del último semanal
    data_date: str             # YYYY-MM-DD del último punto usado
    stale: bool                # True si usamos cache viejo / descarga falló


def build_snapshot(ticker: str, fecha_compra: Optional[str] = None) -> Optional[TickerSnapshot]:
    """
    Combina daily + weekly + SPY para un ticker.
    Devuelve None si no hay datos mínimos.
    """
    daily = get_daily(ticker)
    weekly = get_weekly(ticker)
    spy_weekly = get_weekly("SPY")

    if weekly is None or weekly.empty:
        log.warning("%s: sin datos semanales", ticker)
        return None

    stale = False
    if daily is None or daily.empty:
        log.warning("%s: sin daily, uso último cierre semanal", ticker)
        stale = True
        current_price = float(weekly["Close"].iloc[-1])
        data_date = weekly.index[-1].strftime("%Y-%m-%d")
    else:
        current_price = float(daily["Close"].iloc[-1])
        data_date = daily.index[-1].strftime("%Y-%m-%d")

    wma30 = wma(weekly["Close"], WMA_PERIOD)
    atr_w = atr(weekly, ATR_PERIOD)

    if wma30.dropna().empty:
        log.warning("%s: histórico insuficiente para WMA30", ticker)
        return None

    wma30_now = float(wma30.iloc[-1])
    atr_now = float(atr_w.iloc[-1]) if not atr_w.dropna().empty else float("nan")

    # WMA30 en la fecha de compra (si la conocemos)
    wma30_at = None
    if fecha_compra:
        try:
            target = pd.Timestamp(fecha_compra)
            # Buscar la última vela semanal <= fecha_compra
            idx = weekly.index[weekly.index <= target]
            if len(idx) > 0:
                wma30_at = float(wma30.loc[idx[-1]])
        except Exception as e:
            log.warning("%s: fecha_compra %s inválida: %s", ticker, fecha_compra, e)

    # Mansfield vs SPY
    ms_val = float("nan")
    if spy_weekly is not None and not spy_weekly.empty:
        ms_series = mansfield_rs(weekly["Close"], spy_weekly["Close"], MANSFIELD_MA)
        if not ms_series.dropna().empty:
            ms_val = float(ms_series.iloc[-1])

    return TickerSnapshot(
        ticker=ticker,
        current_price=current_price,
        wma30_weekly=wma30_now,
        wma30_weekly_at=wma30_at,
        atr_weekly=atr_now,
        mansfield=ms_val,
        data_date=data_date,
        stale=stale,
    )
