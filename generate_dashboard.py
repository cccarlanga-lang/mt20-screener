#!/usr/bin/env python3
"""
METODO TENDENCIA 2.0 - GENERADOR DE DASHBOARD WEB
===================================================
Genera un archivo HTML estatico responsive (mobile-first) con:
  - Semaforo Market Timing
  - Senales de entrada esta semana (top ranked by Mansfield)
  - Cartera actual con seguimiento
  - Estadisticas de backtest
  - Info relevante del metodo

Se puede alojar en GitHub Pages, Netlify, o cualquier hosting estatico.
Ejecutar cada viernes despues del cierre para actualizar.
"""
import sys, os, time, json
import numpy as np
import pandas as pd
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from weekly_engine import (
    get_sp500_tickers, load_data, compute_stock_data,
    classify_entries, compute_market_timing, compute_subindustry_indices,
    calc_wma, SECTOR_ETF_MAP,
    WMA_PERIOD, ATR_FULL, ATR_HALF, STOP_PCT, PARTIAL_2R, PARTIAL_3R,
    MAX_POSITIONS, MANSFIELD_MA, MOMENTUM_PERIOD, NHNL_MA,
    BACKTEST_START, INITIAL_CAPITAL, COMMISSION_PCT,
    PortfolioEngine
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, 'docs')  # GitHub Pages uses /docs
PORTFOLIO_FILE = os.path.join(BASE_DIR, 'portfolio_state.json')


def run_engine():
    """Run the full engine and return all data needed for the dashboard."""
    print("[1/7] Universo S&P 500...")
    stock_universe, subind_map = get_sp500_tickers()

    print("[2/7] Descargando datos...")
    all_data = load_data(stock_universe)
    spy_close = all_data['SPY']['Close']
    spy_dates = all_data['SPY'].index

    print("[3/7] Sub-industrias GICS...")
    subind_indices = compute_subindustry_indices(all_data, subind_map, spy_dates)

    print("[4/7] Market Timing...")
    mt_verde, mt_details = compute_market_timing(all_data, stock_universe)

    print("[5/7] Indicadores...")
    stock_ind = {}
    for sym in stock_universe:
        if sym not in all_data:
            continue
        subind_name = subind_map.get(sym, '')
        subind_close = subind_indices.get(subind_name, spy_close)
        try:
            stock_ind[sym] = compute_stock_data(all_data[sym], spy_close, subind_close)
        except:
            pass

    print("[6/7] Clasificando entradas...")
    signals = classify_entries(stock_ind)

    all_dates_set = set()
    for sig in signals.values():
        all_dates_set.update(sig.index)
    dates = sorted([d for d in all_dates_set if d >= pd.Timestamp(BACKTEST_START)])

    print("[7/7] Simulando cartera...")
    engine = PortfolioEngine()
    pending_entries = []
    for date in dates:
        mt_ok = False
        if mt_verde is not None and date in mt_verde.index:
            mt_ok = bool(mt_verde.loc[date])
        actions, pending_entries = engine.process_week(date, signals, mt_ok, stock_universe, pending_entries)

    last_date = dates[-1]
    last_actions = engine.weekly_log[-1] if engine.weekly_log else {}

    # MT details for last date
    mt_row = {}
    if mt_details is not None and last_date in mt_details.index:
        mt_row = mt_details.loc[last_date].to_dict()

    # Backtest metrics
    equities = [(w['date'], w['equity']) for w in engine.weekly_log]
    eq_df = pd.DataFrame(equities, columns=['date', 'equity']).set_index('date')
    eq = eq_df['equity']
    years = len(eq) / 52.0
    cagr = (eq.iloc[-1] / eq.iloc[0]) ** (1.0 / years) - 1
    rets = eq.pct_change().dropna()
    sharpe = rets.mean() / rets.std() * np.sqrt(52) if rets.std() > 0 else 0
    dd = (eq - eq.cummax()) / eq.cummax()
    maxdd = dd.min()
    calmar = cagr / abs(maxdd) if maxdd != 0 else 0

    trades = engine.trade_log
    n_trades = len(trades)
    pnls = [t['pnl_pct'] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    wr = len(wins) / n_trades * 100 if n_trades else 0
    pf = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else 999
    avg_w = np.mean(wins) if wins else 0
    avg_l = np.mean(losses) if losses else 0

    spy_al = spy_close.reindex(eq.index, method='ffill').dropna()
    spy_cagr = (spy_al.iloc[-1] / spy_al.iloc[0]) ** (1.0 / years) - 1 if len(spy_al) > 1 else 0

    # Equity curve for chart (sampled monthly)
    eq_monthly = eq.iloc[::4]  # every 4 weeks ~ monthly
    eq_chart = [(str(d)[:10], float(v)) for d, v in eq_monthly.items()]

    # Weekly equity for last 52 weeks
    eq_recent = [(str(w['date']), w['equity']) for w in engine.weekly_log[-52:]]

    backtest = {
        'period_start': engine.weekly_log[0]['date'],
        'period_end': engine.weekly_log[-1]['date'],
        'final_equity': float(eq.iloc[-1]),
        'cagr': float(cagr * 100),
        'maxdd': float(maxdd * 100),
        'sharpe': float(sharpe),
        'calmar': float(calmar),
        'n_trades': n_trades,
        'win_rate': float(wr),
        'profit_factor': float(pf),
        'avg_win': float(avg_w),
        'avg_loss': float(avg_l),
        'spy_cagr': float(spy_cagr * 100),
        'alpha': float((cagr - spy_cagr) * 100),
        'eq_chart': eq_chart,
        'eq_recent': eq_recent,
    }

    # Positions
    positions = []
    for sym, pos in sorted(engine.positions.items(),
                           key=lambda x: x[1].get('current_pnl_pct', 0), reverse=True):
        status = "HOLD"
        if pos.get('exit_3r'): status = "RUNNER"
        elif pos.get('exit_2r'): status = "POST-2R"
        positions.append({
            'symbol': sym,
            'entry_price': round(pos['entry_price'], 2),
            'current_price': round(pos.get('current_price', 0), 2),
            'pnl_pct': round(pos.get('current_pnl_pct', 0), 1),
            'stop': round(pos.get('current_stop', pos['stop_initial']), 2),
            'weeks': pos['weeks'],
            'sizing': pos.get('sizing', ''),
            'status': status,
            'entry_date': pos['entry_date'],
            'level_2r': round(pos['level_2r'], 2),
            'level_3r': round(pos['level_3r'], 2),
            'subindustry': subind_map.get(sym, ''),
        })

    # Signals
    sigs = last_actions.get('signals_detected', [])
    signals_data = []
    for s in sigs:
        risk_pct = 0
        if s['close_price'] > 0 and s['stop'] > 0:
            risk_pct = (1 - s['stop'] / s['close_price']) * 100
        signals_data.append({
            'symbol': s['symbol'],
            'type': 'FG' if s.get('signal_type') == 'FIRST_GEN' else 'CT',
            'mansfield': round(s['mansfield'], 2),
            'subind_ms': round(s.get('subind_ms', 0), 2) if not np.isnan(s.get('subind_ms', 0)) else 0,
            'dist_atr': round(s['dist_atr'], 1),
            'sizing': s['sizing'],
            'close_price': round(s['close_price'], 2),
            'stop': round(s['stop'], 2),
            'risk_pct': round(risk_pct, 1),
            'subindustry': subind_map.get(s['symbol'], ''),
        })

    # Last 4 weeks summary
    recent_weeks = []
    for w in engine.weekly_log[-4:]:
        recent_weeks.append({
            'date': w['date'],
            'mt': w['mt_status'],
            'equity': w['equity'],
            'n_pos': w['n_positions'],
            'exits': [e['symbol'] for e in w['exits']],
            'entries': [e['symbol'] for e in w['new_entries']],
            'partials': [f"{p['symbol']}({p['type']})" for p in w['partial_sells']],
        })

    return {
        'date': str(last_date)[:10],
        'generated': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'mt_status': last_actions.get('mt_status', 'ROJO'),
        'mt_nhnl_ok': bool(mt_row.get('nhnl_ok', False)),
        'mt_qqq_ok': bool(mt_row.get('qqq_ok', False)),
        'equity': last_actions.get('equity', 0),
        'cash': last_actions.get('cash', 0),
        'n_positions': last_actions.get('n_positions', 0),
        'positions': positions,
        'signals': signals_data,
        'backtest': backtest,
        'recent_weeks': recent_weeks,
    }


def generate_html(data):
    """Generate a self-contained, mobile-responsive HTML dashboard."""

    mt_color = '#10b981' if data['mt_status'] == 'VERDE' else '#ef4444'
    mt_label = 'VERDE' if data['mt_status'] == 'VERDE' else 'ROJO'
    n_signals = len(data['signals'])
    slots_free = MAX_POSITIONS - data['n_positions']

    # Equity chart data for inline SVG
    eq_data = data['backtest']['eq_chart']

    # Build positions HTML
    pos_rows = ''
    for p in data['positions']:
        pnl_color = '#10b981' if p['pnl_pct'] >= 0 else '#ef4444'
        status_badge = {
            'RUNNER': '<span class="badge badge-green">RUNNER</span>',
            'POST-2R': '<span class="badge badge-blue">POST-2R</span>',
            'HOLD': '<span class="badge badge-gray">HOLD</span>',
        }.get(p['status'], '')

        pos_rows += f'''
        <div class="card position-card">
          <div class="pos-header">
            <span class="pos-symbol">{p['symbol']}</span>
            {status_badge}
            <span class="pos-pnl" style="color:{pnl_color}">{p['pnl_pct']:+.1f}%</span>
          </div>
          <div class="pos-details">
            <div class="pos-detail"><span class="label">Entrada</span><span class="value">${p['entry_price']:.2f}</span></div>
            <div class="pos-detail"><span class="label">Actual</span><span class="value">${p['current_price']:.2f}</span></div>
            <div class="pos-detail"><span class="label">Stop</span><span class="value stop-val">${p['stop']:.2f}</span></div>
            <div class="pos-detail"><span class="label">Semanas</span><span class="value">{p['weeks']}w</span></div>
            <div class="pos-detail"><span class="label">Sizing</span><span class="value">{p['sizing']}</span></div>
            <div class="pos-detail"><span class="label">Obj 2R</span><span class="value">${p['level_2r']:.2f}</span></div>
            <div class="pos-detail"><span class="label">Obj 3R</span><span class="value">${p['level_3r']:.2f}</span></div>
            <div class="pos-detail"><span class="label">Sub-Ind</span><span class="value small">{p['subindustry'][:30]}</span></div>
          </div>
        </div>'''

    # Build signals HTML
    sig_rows = ''
    for i, s in enumerate(data['signals']):
        is_buy = i < slots_free and data['mt_status'] == 'VERDE'
        type_badge = '<span class="badge badge-green">FG</span>' if s['type'] == 'FG' else '<span class="badge badge-orange">CT</span>'
        buy_mark = '<span class="badge badge-buy">COMPRAR</span>' if is_buy else ''
        blocked = ' blocked' if data['mt_status'] == 'ROJO' else ''

        sig_rows += f'''
        <div class="card signal-card{blocked}">
          <div class="sig-header">
            <span class="sig-rank">#{i+1}</span>
            <span class="sig-symbol">{s['symbol']}</span>
            {type_badge}
            {buy_mark}
          </div>
          <div class="sig-details">
            <div class="sig-detail"><span class="label">MS Stock</span><span class="value ms-val">{s['mansfield']:+.1f}</span></div>
            <div class="sig-detail"><span class="label">MS SubInd</span><span class="value">{s['subind_ms']:+.1f}</span></div>
            <div class="sig-detail"><span class="label">ATR Dist</span><span class="value">{s['dist_atr']:.1f}</span></div>
            <div class="sig-detail"><span class="label">Sizing</span><span class="value">{s['sizing']}</span></div>
            <div class="sig-detail"><span class="label">Cierre</span><span class="value">${s['close_price']:.2f}</span></div>
            <div class="sig-detail"><span class="label">Stop</span><span class="value stop-val">${s['stop']:.2f}</span></div>
            <div class="sig-detail"><span class="label">Riesgo</span><span class="value risk-val">{s['risk_pct']:.1f}%</span></div>
            <div class="sig-detail"><span class="label">Sub-Ind</span><span class="value small">{s['subindustry'][:28]}</span></div>
          </div>
        </div>'''

    # Recent weeks
    recent_html = ''
    for w in reversed(data['recent_weeks']):
        mt_dot = 'dot-green' if w['mt'] == 'VERDE' else 'dot-red'
        exits_str = ', '.join(w['exits']) if w['exits'] else '-'
        entries_str = ', '.join(w['entries']) if w['entries'] else '-'
        partials_str = ', '.join(w['partials']) if w['partials'] else '-'
        recent_html += f'''
        <tr>
          <td>{w['date']}</td>
          <td><span class="dot {mt_dot}"></span></td>
          <td>${w['equity']:,.0f}</td>
          <td>{w['n_pos']}</td>
          <td class="hide-mobile">{entries_str}</td>
          <td class="hide-mobile">{exits_str}</td>
          <td class="hide-mobile">{partials_str}</td>
        </tr>'''

    # Equity chart points for SVG
    if eq_data:
        vals = [v for _, v in eq_data]
        min_v = min(vals) * 0.95
        max_v = max(vals) * 1.05
        rng = max_v - min_v if max_v > min_v else 1
        n = len(eq_data)
        svg_w, svg_h = 800, 200
        points = []
        for i, (dt, v) in enumerate(eq_data):
            x = i / max(n - 1, 1) * svg_w
            y = svg_h - (v - min_v) / rng * svg_h
            points.append(f"{x:.1f},{y:.1f}")
        poly = ' '.join(points)
        # Fill area
        fill_points = f"0,{svg_h} " + poly + f" {svg_w},{svg_h}"
    else:
        poly = "0,100"
        fill_points = "0,200 0,100 800,200"
        min_v, max_v = 0, 100

    bt = data['backtest']

    html = f'''<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MT 2.0 Screener - {data['date']}</title>
<style>
  :root {{
    --bg: #0f172a; --card: #1e293b; --card2: #334155;
    --text: #e2e8f0; --text2: #94a3b8; --accent: #3b82f6;
    --green: #10b981; --red: #ef4444; --orange: #f59e0b;
    --blue: #3b82f6; --purple: #8b5cf6;
  }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg); color: var(--text);
    line-height: 1.5; padding: 0; max-width: 100vw; overflow-x: hidden;
  }}
  .container {{ max-width: 900px; margin: 0 auto; padding: 12px; }}

  /* HEADER */
  .header {{
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border-bottom: 2px solid var(--card2);
    padding: 16px 12px; text-align: center;
  }}
  .header h1 {{ font-size: 1.3rem; font-weight: 700; letter-spacing: 1px; }}
  .header .date {{ color: var(--text2); font-size: 0.85rem; margin-top: 4px; }}
  .header .updated {{ color: var(--text2); font-size: 0.7rem; }}

  /* SEMAFORO */
  .semaforo {{
    display: flex; align-items: center; justify-content: center; gap: 12px;
    padding: 16px; margin: 12px 0; border-radius: 12px;
    background: var(--card); border: 2px solid {mt_color};
  }}
  .semaforo-light {{
    width: 48px; height: 48px; border-radius: 50%;
    background: {mt_color}; box-shadow: 0 0 20px {mt_color}80;
  }}
  .semaforo-text {{ text-align: left; }}
  .semaforo-text h2 {{ font-size: 1.2rem; color: {mt_color}; }}
  .semaforo-text .conditions {{ font-size: 0.8rem; color: var(--text2); }}
  .cond-ok {{ color: var(--green); }}
  .cond-fail {{ color: var(--red); }}

  /* STATS BAR */
  .stats-bar {{
    display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin: 12px 0;
  }}
  .stat-card {{
    background: var(--card); border-radius: 10px; padding: 12px; text-align: center;
  }}
  .stat-card .stat-value {{ font-size: 1.3rem; font-weight: 700; color: var(--accent); }}
  .stat-card .stat-label {{ font-size: 0.7rem; color: var(--text2); text-transform: uppercase; }}

  /* SECTIONS */
  .section {{ margin: 20px 0; }}
  .section-title {{
    font-size: 1rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 1px; color: var(--text2); margin-bottom: 10px;
    padding-bottom: 6px; border-bottom: 1px solid var(--card2);
  }}

  /* CARDS */
  .card {{
    background: var(--card); border-radius: 10px; padding: 12px;
    margin-bottom: 8px; border: 1px solid var(--card2);
  }}
  .card.blocked {{ opacity: 0.6; }}

  /* POSITION CARD */
  .pos-header {{
    display: flex; align-items: center; gap: 8px; margin-bottom: 8px;
  }}
  .pos-symbol {{ font-size: 1.1rem; font-weight: 700; }}
  .pos-pnl {{ margin-left: auto; font-size: 1.1rem; font-weight: 700; }}
  .pos-details {{
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px;
  }}
  .pos-detail {{ display: flex; flex-direction: column; }}
  .pos-detail .label {{ font-size: 0.65rem; color: var(--text2); text-transform: uppercase; }}
  .pos-detail .value {{ font-size: 0.85rem; font-weight: 600; }}
  .pos-detail .value.small {{ font-size: 0.7rem; font-weight: 400; }}

  /* SIGNAL CARD */
  .sig-header {{
    display: flex; align-items: center; gap: 8px; margin-bottom: 8px;
  }}
  .sig-rank {{ font-size: 0.8rem; color: var(--text2); font-weight: 700; }}
  .sig-symbol {{ font-size: 1.1rem; font-weight: 700; }}
  .sig-details {{
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px;
  }}
  .sig-detail {{ display: flex; flex-direction: column; }}
  .sig-detail .label {{ font-size: 0.65rem; color: var(--text2); text-transform: uppercase; }}
  .sig-detail .value {{ font-size: 0.85rem; font-weight: 600; }}
  .sig-detail .value.small {{ font-size: 0.7rem; font-weight: 400; }}
  .stop-val {{ color: var(--red); }}
  .ms-val {{ color: var(--green); }}
  .risk-val {{ color: var(--orange); }}

  /* BADGES */
  .badge {{
    display: inline-block; padding: 2px 8px; border-radius: 6px;
    font-size: 0.7rem; font-weight: 700; text-transform: uppercase;
  }}
  .badge-green {{ background: #10b98120; color: var(--green); }}
  .badge-orange {{ background: #f59e0b20; color: var(--orange); }}
  .badge-blue {{ background: #3b82f620; color: var(--blue); }}
  .badge-gray {{ background: #64748b20; color: #64748b; }}
  .badge-buy {{
    background: var(--green); color: #000; font-weight: 800;
    animation: pulse 2s infinite;
  }}
  @keyframes pulse {{
    0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.7; }}
  }}

  /* BACKTEST */
  .bt-grid {{
    display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px;
  }}
  .bt-item {{
    display: flex; justify-content: space-between; padding: 6px 10px;
    background: var(--card2); border-radius: 6px;
  }}
  .bt-item .bt-label {{ color: var(--text2); font-size: 0.8rem; }}
  .bt-item .bt-value {{ font-weight: 700; font-size: 0.85rem; }}

  /* EQUITY CHART */
  .chart-container {{
    background: var(--card); border-radius: 10px; padding: 12px;
    margin: 12px 0; overflow: hidden;
  }}
  .chart-container svg {{ width: 100%; height: auto; }}

  /* TABLE */
  table {{ width: 100%; border-collapse: collapse; font-size: 0.8rem; }}
  th {{ color: var(--text2); font-weight: 600; text-align: left; padding: 6px 8px;
       border-bottom: 1px solid var(--card2); font-size: 0.7rem; text-transform: uppercase; }}
  td {{ padding: 6px 8px; border-bottom: 1px solid #1e293b40; }}
  .dot {{ display: inline-block; width: 10px; height: 10px; border-radius: 50%; }}
  .dot-green {{ background: var(--green); }}
  .dot-red {{ background: var(--red); }}

  /* METHOD INFO */
  .method-info {{ font-size: 0.75rem; color: var(--text2); line-height: 1.7; }}
  .method-info strong {{ color: var(--text); }}

  /* RESPONSIVE */
  @media (max-width: 600px) {{
    .pos-details, .sig-details {{ grid-template-columns: repeat(2, 1fr); }}
    .bt-grid {{ grid-template-columns: 1fr; }}
    .stats-bar {{ grid-template-columns: repeat(3, 1fr); gap: 4px; }}
    .stat-card {{ padding: 8px; }}
    .stat-card .stat-value {{ font-size: 1.1rem; }}
    .hide-mobile {{ display: none; }}
  }}

  /* FOOTER */
  .footer {{
    text-align: center; padding: 20px; color: var(--text2);
    font-size: 0.7rem; border-top: 1px solid var(--card2); margin-top: 30px;
  }}
</style>
</head>
<body>

<div class="header">
  <h1>METODO TENDENCIA 2.0</h1>
  <div class="date">Reporte semanal &mdash; {data['date']}</div>
  <div class="updated">Generado: {data['generated']} | S&P 500 | 10 posiciones</div>
</div>

<div class="container">

  <!-- SEMAFORO -->
  <div class="semaforo">
    <div class="semaforo-light"></div>
    <div class="semaforo-text">
      <h2>SEMAFORO {mt_label}</h2>
      <div class="conditions">
        NH-NL (rising + &gt;MA50): <span class="{'cond-ok' if data['mt_nhnl_ok'] else 'cond-fail'}">{'OK' if data['mt_nhnl_ok'] else 'FALLO'}</span>
        &nbsp;|&nbsp;
        QQQ &gt; WMA30: <span class="{'cond-ok' if data['mt_qqq_ok'] else 'cond-fail'}">{'OK' if data['mt_qqq_ok'] else 'FALLO'}</span>
      </div>
      <div class="conditions">
        {'Se pueden abrir nuevas posiciones' if data['mt_status'] == 'VERDE' else 'No se abren nuevas posiciones. Existentes siguen con stops.'}
      </div>
    </div>
  </div>

  <!-- STATS BAR -->
  <div class="stats-bar">
    <div class="stat-card">
      <div class="stat-value">${data['equity']:,.0f}</div>
      <div class="stat-label">Equity</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">{data['n_positions']}/{MAX_POSITIONS}</div>
      <div class="stat-label">Posiciones</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">{n_signals}</div>
      <div class="stat-label">Senales</div>
    </div>
  </div>

  <!-- CARTERA -->
  <div class="section">
    <div class="section-title">Cartera Actual</div>
    {pos_rows if pos_rows else '<div class="card"><p style="color:var(--text2);text-align:center;">Sin posiciones abiertas</p></div>'}
  </div>

  <!-- SENALES -->
  <div class="section">
    <div class="section-title">
      Senales de Entrada &mdash; Comprar al OPEN del Lunes
      {'<span style="color:var(--red);font-size:0.8rem;"> (BLOQUEADAS - Semaforo Rojo)</span>' if data['mt_status'] == 'ROJO' else ''}
    </div>
    {sig_rows if sig_rows else '<div class="card"><p style="color:var(--text2);text-align:center;">Sin senales esta semana</p></div>'}
  </div>

  <!-- EQUITY CHART -->
  <div class="section">
    <div class="section-title">Curva de Equity (Backtest {bt['period_start']} - {bt['period_end']})</div>
    <div class="chart-container">
      <svg viewBox="0 0 800 220" preserveAspectRatio="none">
        <defs>
          <linearGradient id="eqGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="#3b82f6" stop-opacity="0.3"/>
            <stop offset="100%" stop-color="#3b82f6" stop-opacity="0.02"/>
          </linearGradient>
        </defs>
        <polygon points="{fill_points}" fill="url(#eqGrad)"/>
        <polyline points="{poly}" fill="none" stroke="#3b82f6" stroke-width="2"/>
        <text x="5" y="15" fill="#94a3b8" font-size="11">${max_v:,.0f}</text>
        <text x="5" y="198" fill="#94a3b8" font-size="11">${min_v:,.0f}</text>
      </svg>
    </div>
  </div>

  <!-- BACKTEST STATS -->
  <div class="section">
    <div class="section-title">Estadisticas Backtest</div>
    <div class="bt-grid">
      <div class="bt-item"><span class="bt-label">CAGR</span><span class="bt-value" style="color:{'var(--green)' if bt['cagr']>0 else 'var(--red)'}">{bt['cagr']:+.1f}%</span></div>
      <div class="bt-item"><span class="bt-label">Max Drawdown</span><span class="bt-value" style="color:var(--red)">{bt['maxdd']:.1f}%</span></div>
      <div class="bt-item"><span class="bt-label">Sharpe</span><span class="bt-value">{bt['sharpe']:.2f}</span></div>
      <div class="bt-item"><span class="bt-label">Calmar</span><span class="bt-value">{bt['calmar']:.2f}</span></div>
      <div class="bt-item"><span class="bt-label">Profit Factor</span><span class="bt-value">{bt['profit_factor']:.1f}</span></div>
      <div class="bt-item"><span class="bt-label">Win Rate</span><span class="bt-value">{bt['win_rate']:.0f}%</span></div>
      <div class="bt-item"><span class="bt-label">Avg Win</span><span class="bt-value" style="color:var(--green)">{bt['avg_win']:+.1f}%</span></div>
      <div class="bt-item"><span class="bt-label">Avg Loss</span><span class="bt-value" style="color:var(--red)">{bt['avg_loss']:+.1f}%</span></div>
      <div class="bt-item"><span class="bt-label">Trades</span><span class="bt-value">{bt['n_trades']}</span></div>
      <div class="bt-item"><span class="bt-label">SPY CAGR</span><span class="bt-value">{bt['spy_cagr']:+.1f}%</span></div>
      <div class="bt-item"><span class="bt-label">Alpha</span><span class="bt-value" style="color:{'var(--green)' if bt['alpha']>0 else 'var(--red)'}">{bt['alpha']:+.1f}%</span></div>
      <div class="bt-item"><span class="bt-label">Capital Final</span><span class="bt-value">${bt['final_equity']:,.0f}</span></div>
    </div>
  </div>

  <!-- RECENT WEEKS -->
  <div class="section">
    <div class="section-title">Ultimas 4 Semanas</div>
    <div class="card" style="overflow-x:auto;">
      <table>
        <thead>
          <tr><th>Fecha</th><th>MT</th><th>Equity</th><th>Pos</th>
              <th class="hide-mobile">Entradas</th><th class="hide-mobile">Salidas</th>
              <th class="hide-mobile">Parciales</th></tr>
        </thead>
        <tbody>{recent_html}</tbody>
      </table>
    </div>
  </div>

  <!-- METHOD INFO -->
  <div class="section">
    <div class="section-title">Reglas del Metodo</div>
    <div class="card method-info">
      <p><strong>5 Condiciones Core:</strong> C1 Price&gt;WMA30 | C2 WMA30 rising | C3 Momentum 17w&gt;0 | C4 Mansfield RS&gt;0 | C5 Sub-Industry MS&gt;0</p>
      <p><strong>Sub-Industria:</strong> Mansfield RS del indice sintetico GICS Sub-Industry (127 grupos) vs SPY</p>
      <p><strong>Entrada:</strong> Core=5 al cierre del viernes &rarr; Comprar al OPEN del lunes | FG + CT ordenadas por Mansfield RS</p>
      <p><strong>ATR Sizing:</strong> 0-{ATR_FULL} ATR = 100% | {ATR_FULL}-{ATR_HALF} ATR = 50% | &gt;{ATR_HALF} = no entry</p>
      <p><strong>Stop:</strong> WMA30 &times; {STOP_PCT} | Breakeven tras 2R</p>
      <p><strong>Parciales:</strong> 2R = vender {int(PARTIAL_2R*100)}% | 3R = vender {int(PARTIAL_3R*100)}% | Runner = {int((1-PARTIAL_2R-PARTIAL_3R)*100)}% con trailing stop</p>
      <p><strong>Market Timing:</strong> NH-NL rising+&gt;MA{NHNL_MA} AND QQQ&gt;WMA30 = VERDE | Rojo = no nuevas entradas</p>
      <p><strong>Cartera:</strong> {MAX_POSITIONS} posiciones equiponderadas S&P 500</p>
    </div>
  </div>

</div>

<div class="footer">
  Metodo Tendencia 2.0 | Backtest {bt['period_start']}&ndash;{bt['period_end']} |
  CAGR {bt['cagr']:+.1f}% | Sharpe {bt['sharpe']:.2f} | Calmar {bt['calmar']:.2f}<br>
  Datos: Yahoo Finance | Actualizado: {data['generated']}
</div>

</body>
</html>'''

    return html


def main():
    t0 = time.time()
    print("=" * 60)
    print("GENERANDO DASHBOARD WEB - METODO TENDENCIA 2.0")
    print("=" * 60)

    data = run_engine()

    print("\nGenerando HTML...")
    html = generate_html(data)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'index.html')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    # Also save a copy with date
    dated_path = os.path.join(OUTPUT_DIR, f'report_{data["date"]}.html')
    with open(dated_path, 'w', encoding='utf-8') as f:
        f.write(html)

    # Save data as JSON for potential API use
    json_path = os.path.join(OUTPUT_DIR, 'data.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)

    elapsed = time.time() - t0
    print(f"\nDashboard generado en {elapsed:.0f}s:")
    print(f"  HTML:  {output_path}")
    print(f"  Dated: {dated_path}")
    print(f"  JSON:  {json_path}")
    print(f"\nPara ver: abrir {output_path} en el navegador")
    print(f"Para publicar: subir la carpeta 'docs/' a GitHub Pages")


if __name__ == '__main__':
    main()
