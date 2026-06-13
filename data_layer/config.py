"""Constantes del método Weinstein y rutas del proyecto."""
from pathlib import Path

# =====================================================================
# Rutas
# =====================================================================
BASE_DIR      = Path(__file__).resolve().parent.parent
PORTFOLIO_IN  = BASE_DIR / "portfolio_manual.json"
SIGNALS_IN    = BASE_DIR / "signals_auto.json"        # USA (S&P 500)
SIGNALS_EU_IN = BASE_DIR / "signals_auto_eu.json"     # Europa (STOXX 600)
DOCS_DIR      = BASE_DIR / "docs"
DATA_OUT      = DOCS_DIR / "data.json"
STATUS_OUT    = DOCS_DIR / "update_status.json"
REGIME_HIST   = BASE_DIR / "regime_history.json"        # snapshot semanal MT
EQUITY_HIST   = BASE_DIR / "equity_history.json"        # snapshot semanal equity
CACHE_DAILY   = BASE_DIR / "cache_daily.pkl"   # precios diarios (para "current_price" del viernes)
CACHE_WEEKLY  = BASE_DIR / "sp500_weekly_cache.pkl"  # compartido con weekly_engine.py

# =====================================================================
# Parámetros del método (alineados con weekly_engine.py)
# =====================================================================
WMA_PERIOD     = 30
MANSFIELD_MA   = 52
ATR_PERIOD     = 14
STOP_PCT       = 0.97    # stop = WMA30 * 0.97
PARTIAL_2R_PCT = 25
PARTIAL_3R_PCT = 25
MAX_POSITIONS  = 10

# Umbrales ATR para sizing (distancia entry - WMA30 en múltiplos de ATR semanal)
ATR_FULL = 2.0    # 0-2.0 ATR  → 100%
ATR_HALF = 4.0    # 2.0-4.0    → 50%    (>4.0 = no entry)

# Market Timing
QQQ_TICKER    = "QQQ"
NHNL_THRESHOLD = 0.0       # MA50 de NH-NL > 0 (puedes endurecer a 1.0)

# Métricas derivadas
STALE_DAYS_THRESHOLD = 8   # >8 días sin update → status PENDIENTE
MT_ROJO_BLOCKS_BUYS  = True

# Versión del contrato data.json (ver docs/SCHEMA.md)
SCHEMA_VERSION = "1.0"
METHOD_LABEL   = "V19 · Core 4/4 · SMA30 · Vol>MA10w · DistATR≤2.5 · Stop SMA30×0.97 · Composite 70%MS+30%CFI"
