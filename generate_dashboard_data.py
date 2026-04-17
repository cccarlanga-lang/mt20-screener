#!/usr/bin/env python3
"""
generate_dashboard_data.py
==========================

Entry point del pipeline semanal que produce:

  docs/data.json           ← lo que consume el dashboard (docs/index.html)
  docs/update_status.json  ← salud del pipeline (OK/EJECUTANDO/PENDIENTE/ERROR)

Flujo:

    portfolio_manual.json   signals_auto.json
           │                       │
           ▼                       ▼
    ┌─────────── data_layer.pipeline.build_dashboard_data ───────────┐
    │                                                                │
    │ 1. Parse portfolio_manual.json  (ticker + precio_compra)       │
    │ 2. Parse signals_auto.json      (motor semanal)                │
    │ 3. Enriquecer cada posición con yfinance + WMA30/Mansfield     │
    │ 4. Derivar actions_this_week (stops, parciales 2R/3R, compras) │
    │ 5. Bloquear compras si Market Timing == ROJO                   │
    │ 6. Escritura atómica de docs/data.json y docs/update_status    │
    └────────────────────────────────────────────────────────────────┘

Uso:

    python generate_dashboard_data.py                 # un run
    python generate_dashboard_data.py --verbose       # logs DEBUG
    python generate_dashboard_data.py --log-url URL   # marca URL del log de GH Actions

Codigo de salida:
    0 = OK
    1 = ERROR (el dashboard mostrará banner rojo)

Automatización: ver .github/workflows/update_dashboard.yml (viernes 22:00 GMT+1).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Genera docs/data.json para el dashboard Weinstein.")
    parser.add_argument("--verbose", action="store_true", help="logs DEBUG")
    parser.add_argument(
        "--log-url",
        default=os.environ.get("GITHUB_RUN_URL"),
        help="URL del run de GH Actions (para el banner de error en el dashboard)",
    )
    args = parser.parse_args()

    _configure_logging(args.verbose)

    # Import tardío para que el --help no requiera pandas/yfinance instalados
    from data_layer import build_dashboard_data

    ok = build_dashboard_data(log_url=args.log_url)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
