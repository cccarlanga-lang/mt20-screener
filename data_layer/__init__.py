"""
data_layer — Capa de datos del dashboard operativo Weinstein.

Arquitectura:
  portfolio_manual.json   (input, usuario edita)
  signals_auto.json       (input, motor semanal produce)
         │
         ▼
  [config · market · portfolio · output]
         │
         ▼
  docs/data.json          (output, dashboard consume)
  docs/update_status.json (output, dashboard consume)

Uso típico:
  from data_layer import build_dashboard_data
  build_dashboard_data()
"""
from .pipeline import build_dashboard_data

__all__ = ["build_dashboard_data"]
