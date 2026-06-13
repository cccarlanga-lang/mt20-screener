#!/usr/bin/env python3
"""
ASP-B Telegram — Resumen semanal automático
============================================
Envía cada sábado (hora configurable) un mensaje con:
  * Estado del mercado (SPY vs SMA30)
  * ÓRDENES a colocar: cada señal con Entry, Stop-loss, TP1, TP2
  * VIGILANCIA: setups cerca del breakout
  * Estadísticas del backtest

Configuración: telegram_config.json en la misma carpeta.

Setup inicial (2 minutos):
  1. Telegram → busca @BotFather → /newbot → copia el token
  2. Escríbele cualquier mensaje a tu bot (ej: "hola")
  3. python aspb_telegram.py --setup   ← detecta chat_id automáticamente
  4. Pon "enabled": true en telegram_config.json
  5. python aspb_telegram.py           ← envía mensaje de prueba
"""

import os, json, datetime, time, threading, logging, sys
import urllib.request, urllib.error
import numpy as np, pandas as pd

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "telegram_config.json")
_LOG = logging.getLogger("aspb_telegram")

_DEFAULT_CONFIG = {
    "_instrucciones": (
        "1. Telegram → @BotFather → /newbot → copia el token  "
        "2. Escríbele un mensaje al bot  "
        "3. python aspb_telegram.py --setup  "
        "4. enabled: true  "
        "5. python aspb_telegram.py"
    ),
    "enabled":    False,
    "bot_token":  "PEGA_AQUI_TU_TOKEN",
    "chat_id":    "",
    "send_day":   "saturday",
    "send_hour":  8,
}

_last_sent_date = None


# ─── CONFIG ──────────────────────────────────────────────────────────────────

def load_config():
    # 1. Archivo local (uso normal en PC)
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, encoding="utf-8") as f:
                cfg = json.load(f)
            for k, v in _DEFAULT_CONFIG.items():
                cfg.setdefault(k, v)
            return cfg
        except Exception as e:
            _LOG.warning(f"Error leyendo {CONFIG_FILE}: {e}")

    # 2. Variables de entorno (GitHub Actions / servidor en la nube)
    env_token   = os.environ.get("TG_BOT_TOKEN", "").strip()
    env_chat_id = os.environ.get("TG_CHAT_ID", "").strip()
    if env_token and env_chat_id:
        return {
            **_DEFAULT_CONFIG,
            "enabled":   True,
            "bot_token": env_token,
            "chat_id":   env_chat_id,
        }

    # 3. Crear config por defecto
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(_DEFAULT_CONFIG, f, indent=2, ensure_ascii=False)
    print(f"  [Telegram] Config creada: {CONFIG_FILE}")
    return dict(_DEFAULT_CONFIG)


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def _usd(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    return f"${v:,.2f}"

def _pct(v, sign=True):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    return f"{'+' if sign and v >= 0 else ''}{v:.1f}%"

def _tier(r):
    if not bool(r.get("ms_filter_ok", False)):
        return 0
    if bool(r.get("rs_near_high", False)):
        return 2
    return 1


# ─── TELEGRAM API ────────────────────────────────────────────────────────────

def _tg_post(token, method, payload):
    """POST a la Telegram Bot API. Devuelve (ok, result_dict)."""
    url  = f"https://api.telegram.org/bot{token}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req  = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return True, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return False, {"error": str(e), "body": body}
    except Exception as e:
        return False, {"error": str(e)}


def get_chat_id(token):
    """
    Llama a getUpdates y devuelve el chat_id del último mensaje recibido.
    El usuario debe haberle escrito al bot antes de llamar esto.
    Devuelve (chat_id_str | None, error_str | None).
    """
    ok, result = _tg_post(token, "getUpdates", {"limit": 10, "offset": -10})
    if not ok:
        return None, result.get("error", "Error de red.")
    updates = result.get("result", [])
    if not updates:
        return None, (
            "Sin mensajes recientes. "
            "Escríbele cualquier mensaje a tu bot en Telegram y vuelve a ejecutar."
        )
    for upd in reversed(updates):
        msg = upd.get("message") or upd.get("channel_post")
        if msg and "chat" in msg:
            return str(msg["chat"]["id"]), None
    return None, "No se encontró chat_id. Escríbele al bot y repite."


def _send_raw(token, chat_id, text):
    """Envía un único chunk de texto (≤4096 chars). Devuelve (ok, err_str)."""
    ok, result = _tg_post(token, "sendMessage", {
        "chat_id":                  chat_id,
        "text":                     text,
        "parse_mode":               "HTML",
        "disable_web_page_preview": True,
    })
    if not ok:
        return False, result.get("body") or result.get("error", "Error desconocido")
    return True, ""


def send_message(token, chat_id, text):
    """
    Envía el texto completo dividiendo en chunks si supera 4096 caracteres.
    Devuelve (ok, err_str).
    """
    chunks = _split(text, 4096)
    for chunk in chunks:
        ok, err = _send_raw(token, chat_id, chunk)
        if not ok:
            return False, err
    return True, ""


def _split(text, max_len):
    """Divide por líneas respetando el límite de caracteres."""
    if len(text) <= max_len:
        return [text]
    chunks, cur = [], ""
    for line in text.split("\n"):
        candidate = cur + line + "\n"
        if len(candidate) > max_len:
            if cur:
                chunks.append(cur.rstrip("\n"))
            cur = line + "\n"
        else:
            cur = candidate
    if cur.strip():
        chunks.append(cur.rstrip("\n"))
    return chunks


# ─── CONSTRUCCIÓN DEL MENSAJE ─────────────────────────────────────────────────

_SEP  = "━━━━━━━━━━━━━━━━━━━━━"
_SEP2 = "─────────────────────────"


def build_message(df, last_update, stats=None, spy_ctx=None):
    """
    Construye el texto HTML de Telegram.
    Señales agrupadas por prioridad: ⭐ Confluencia primero, luego ✅ Filtro OK.
    Etiquetas válidas: <b>, <i>, <code>.
    """
    ts_str = last_update.strftime("%d/%m/%Y")
    lines  = []

    # ── Cabecera ──────────────────────────────────────────────────────────────
    lines += [f"⚡ <b>ASP-B Weekly — {ts_str}</b>", ""]

    # ── Estado del mercado ────────────────────────────────────────────────────
    if spy_ctx:
        if spy_ctx.get("above"):
            lines += [
                "✅ <b>MERCADO EN TENDENCIA — PUEDES OPERAR</b>",
                (f"SPY <code>{_usd(spy_ctx['close'])}</code>  ·  "
                 f"SMA30 <code>{_usd(spy_ctx['sma30'])}</code>  ·  "
                 f"<b>{_pct(spy_ctx['pct_sma'])}</b> sobre media  ·  "
                 f"Semana <b>{_pct(spy_ctx['wk_pct'])}</b>"),
                "",
            ]
        else:
            lines += [
                "🚫 <b>MERCADO BAJISTA — NO ABRIR POSICIONES</b>",
                (f"SPY <code>{_usd(spy_ctx['close'])}</code> bajo "
                 f"SMA30 <code>{_usd(spy_ctx['sma30'])}</code>  ·  "
                 f"<b>{_pct(spy_ctx['pct_sma'])}</b> bajo media  ·  "
                 f"Semana <b>{_pct(spy_ctx['wk_pct'])}</b>"),
                "<i>Mantén stops en posiciones abiertas. No abrir nuevas.</i>",
                "",
            ]

    # ── Clasificar señales ────────────────────────────────────────────────────
    # Usamos columna "tier_v" (sin guión bajo inicial) para que itertuples()
    # la devuelva correctamente como atributo nombrado.
    actua_t2, actua_t1, vig_list = [], [], []

    if df is not None and not df.empty:
        df2 = df.copy()
        df2["tier_v"] = df2.apply(_tier, axis=1)

        actua_pool = df2[
            df2["signal"].isin(["BUY", "NEAR"]) &
            (df2["dist_pct"] >= -3.0) &
            (df2["tier_v"] >= 1)
        ].sort_values(["tier_v", "dist_pct"], ascending=[False, True])

        vig_pool = df2[
            (df2["signal"] == "SETUP") &
            (df2["dist_pct"] <= 8.0) &
            (df2["tier_v"] >= 1)
        ].sort_values("dist_pct")

        for r in actua_pool.itertuples():
            (actua_t2 if r.tier_v == 2 else actua_t1).append(r)
        vig_list = list(vig_pool.itertuples())

    n_total = len(actua_t2) + len(actua_t1)

    # ── Helper: bloque de una señal ───────────────────────────────────────────
    def _signal_block(r):
        entry = float(r.entry)
        stop  = float(r.stop)
        tp1   = float(r.tp1)
        tp2   = float(r.tp2)
        dist  = float(r.dist_pct)
        sig   = r.signal
        idx_l = getattr(r, "index", "SP500")
        rr_v  = getattr(r, "rr", None)
        fb    = getattr(r, "failed_breakout", False)

        risk_pct  = (entry - stop)  / entry * 100
        gain1_pct = (tp1   - entry) / entry * 100
        gain2_pct = (tp2   - entry) / entry * 100

        sig_tag  = "🟢 BREAKOUT" if sig == "BUY" else "🔵 CERCA ENTRY"
        dist_txt = (f"ya por encima +{abs(dist):.1f}%" if dist < 0
                    else f"{dist:.1f}% hasta el entry")
        rr_part  = ""
        if rr_v is not None and not (isinstance(rr_v, float) and np.isnan(rr_v)):
            rr_part = f"  ·  R:R {rr_v:.1f}"

        lines_b = [
            f"📌 <b>{r.ticker}</b>  <code>{idx_l}</code>  {sig_tag}",
            f"<i>{dist_txt}{rr_part}</i>",
            f"   <b>ENTRY:  <code>{_usd(entry)}</code></b>",
            f"   STOP:   <code>{_usd(stop)}</code>  <i>−{risk_pct:.1f}%</i>",
            f"   TP1:    <code>{_usd(tp1)}</code>  <i>+{gain1_pct:.1f}%</i>",
            f"   TP2:    <code>{_usd(tp2)}</code>  <i>+{gain2_pct:.1f}%</i>",
        ]
        if fb:
            # El precio ya intentó romper y volvió — estructura menos limpia
            lines_b.append("   <i>⚠️ Hubo un intento fallido previo. Confirma en grafico.</i>")
        lines_b.append("")
        return lines_b

    # ── Sección órdenes ───────────────────────────────────────────────────────
    lines.append(_SEP)
    if n_total > 0:
        lines += [
            f"🎯 <b>ÓRDENES A COLOCAR — {n_total} señal{'es' if n_total != 1 else ''}</b>",
            "<i>Coloca una orden BUY STOP al precio ENTRY antes del lunes.</i>",
            "",
        ]
        if actua_t2:
            lines += [f"⭐ <b>CONFLUENCIA ({len(actua_t2)})</b>", ""]
            for r in actua_t2:
                lines += _signal_block(r)
        if actua_t1:
            lines += [f"✅ <b>FILTRO OK ({len(actua_t1)})</b>", ""]
            for r in actua_t1:
                lines += _signal_block(r)
    else:
        lines += [
            "🎯 <b>SIN ÓRDENES ESTA SEMANA</b>",
            "<i>Ninguna señal cumple los filtros. Revisa el sábado que viene.</i>",
            "",
        ]

    # ── Vigilancia ────────────────────────────────────────────────────────────
    n_vig = len(vig_list)
    if n_vig > 0:
        lines += [
            _SEP,
            f"👀 <b>VIGILANCIA — {n_vig} setup{'s' if n_vig != 1 else ''} cerca del breakout</b>",
            "<i>Estructuras formadas, aún por debajo del entry. Pueden activarse pronto.</i>",
            "",
        ]
        for r in vig_list:
            tier_s = "⭐" if r.tier_v == 2 else "✅"
            dist   = float(r.dist_pct)
            idx_l  = getattr(r, "index", "SP500")
            ms_v   = getattr(r, "mansfield", None)
            ms_s   = ""
            if ms_v is not None and not (isinstance(ms_v, float) and np.isnan(ms_v)):
                ms_s = f"  ·  MS {ms_v:+.1f}%"
            lines.append(
                f"{tier_s} <b>{r.ticker}</b>  <code>{idx_l}</code>  ·  "
                f"Entry <code>{_usd(r.entry)}</code>  ·  {dist:.1f}% al entry{ms_s}"
            )
        lines.append("")

    # ── Nota final ────────────────────────────────────────────────────────────
    lines += [
        _SEP2,
        "<i>Orden en el broker: <b>BUY STOP</b> al precio ENTRY.</i>",
        "<i>Si se activa → coloca <b>STOP-LOSS</b> de inmediato.</i>",
    ]

    return "\n".join(lines)


# ─── ENVÍO PÚBLICO ────────────────────────────────────────────────────────────

def send_weekly_summary(df, last_update, stats=None):
    """
    Construye y envía el resumen semanal por Telegram.
    Firma compatible con aspb_server.py.
    Devuelve (ok: bool, mensaje: str).
    """
    cfg = load_config()

    if not cfg.get("enabled"):
        return False, "Telegram desactivado (enabled=false en telegram_config.json)."

    token   = cfg.get("bot_token", "")
    chat_id = str(cfg.get("chat_id", "")).strip()

    if not token or token == _DEFAULT_CONFIG["bot_token"]:
        return False, "Falta bot_token en telegram_config.json."
    if not chat_id:
        return False, "Falta chat_id. Ejecuta:  python aspb_telegram.py --setup"

    # Contexto SPY desde el servidor
    spy_ctx = None
    try:
        from aspb_server import _state, _lock
        from aspb_screener import calc_sma, SMA_LEN
        with _lock:
            spy_df = _state.get("spy_df")
        if spy_df is not None and len(spy_df) >= 32:
            sma30   = calc_sma(spy_df["Close"], SMA_LEN)
            close   = float(spy_df["Close"].iloc[-1])
            sma_val = float(sma30.iloc[-1])
            prev_cl = float(spy_df["Close"].iloc[-2])
            spy_ctx = {
                "close":   close,
                "sma30":   sma_val,
                "above":   close > sma_val,
                "pct_sma": round((close / sma_val - 1) * 100, 1),
                "wk_pct":  round((close / prev_cl - 1) * 100, 1),
            }
    except Exception:
        pass

    text = build_message(df, last_update, stats=stats, spy_ctx=spy_ctx)
    ok, err = send_message(token, chat_id, text)

    if ok:
        print(f"  [Telegram] ✓ Enviado a chat_id {chat_id}")
        return True, f"Mensaje enviado a chat_id {chat_id}"
    else:
        print(f"  [Telegram] ✗ Error: {err}")
        return False, err


# ─── SCHEDULER ────────────────────────────────────────────────────────────────

_DAY_MAP = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


def start_telegram_scheduler(get_state_fn):
    """
    Arranca el hilo de envío automático.
    get_state_fn(): función que devuelve (df, last_update, stats).
    """
    global _last_sent_date
    cfg      = load_config()
    send_day = cfg.get("send_day", "saturday")
    send_h   = int(cfg.get("send_hour", 8))

    if cfg.get("enabled"):
        print(f"  [Telegram] Scheduler activo — envío los {send_day}s a las {send_h:02d}:00.")
    else:
        print("  [Telegram] Scheduler activo — envío desactivado (enabled=false).")

    def _loop():
        global _last_sent_date
        while True:
            try:
                _cfg = load_config()
                if _cfg.get("enabled"):
                    now        = datetime.datetime.now()
                    today      = now.date()
                    target_day = _DAY_MAP.get(_cfg.get("send_day", "saturday").lower(), 5)
                    target_h   = int(_cfg.get("send_hour", 8))

                    if (now.weekday() == target_day and
                            now.hour == target_h and
                            _last_sent_date != today):
                        print(f"  [Telegram] Es {_cfg['send_day']} a las {target_h:02d}h — enviando...")
                        try:
                            df, last_up, stats = get_state_fn()
                            ok, msg = send_weekly_summary(
                                df, last_up or datetime.datetime.now(), stats
                            )
                            if ok:
                                _last_sent_date = today
                            else:
                                print(f"  [Telegram] Error: {msg}")
                        except Exception as inner_e:
                            print(f"  [Telegram] Excepción al enviar: {inner_e}")
            except Exception as e:
                _LOG.warning(f"Scheduler tick error: {e}")
            time.sleep(900)   # comprobar cada 15 minutos

    t = threading.Thread(target=_loop, daemon=True, name="telegram-scheduler")
    t.start()
    return t


# ─── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n=== ASP-B Telegram ===\n")
    cfg = load_config()

    # ── Modo --setup: detectar chat_id automáticamente ────────────────────────
    if "--setup" in sys.argv:
        token = cfg.get("bot_token", "")
        if not token or token == _DEFAULT_CONFIG["bot_token"]:
            print(f"  Primero pon tu bot_token en:\n  {CONFIG_FILE}\n")
            sys.exit(1)
        print("  Buscando chat_id en los últimos mensajes recibidos por el bot...")
        chat_id, err = get_chat_id(token)
        if chat_id:
            cfg["chat_id"] = chat_id
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2, ensure_ascii=False)
            print(f"\n  ✅ chat_id detectado y guardado: {chat_id}")
            print(f"  Archivo actualizado: {CONFIG_FILE}")
            print("\n  Siguiente paso:")
            print("    1. Abre telegram_config.json")
            print("    2. Cambia  \"enabled\": false  →  \"enabled\": true")
            print("    3. Ejecuta:  python aspb_telegram.py\n")
        else:
            print(f"\n  ❌ {err}\n")
        sys.exit(0)

    # ── Modo normal: enviar mensaje de prueba ─────────────────────────────────
    if not cfg.get("enabled"):
        print(f"  Telegram desactivado. Edita:\n  {CONFIG_FILE}\n")
        print("  Pasos de configuración:")
        print("  1. Telegram → busca @BotFather → /newbot → copia el token")
        print("  2. Escríbele cualquier mensaje a tu bot (ej: 'hola')")
        print("  3. Pega el token en telegram_config.json → bot_token")
        print("  4. python aspb_telegram.py --setup   ← detecta chat_id")
        print("  5. Pon \"enabled\": true en telegram_config.json")
        print("  6. python aspb_telegram.py           ← prueba de envío\n")
        sys.exit(0)

    # ── Obtener datos REALES del screener ────────────────────────────────────
    print("  Ejecutando screener con datos reales (puede tardar 1-2 min)...")
    try:
        from aspb_screener import run_screener, calc_sma, SMA_LEN, load_data, BENCHMARK
        df_test = run_screener()

        # Contexto SPY real
        spy_test = None
        try:
            data   = load_data()
            spy_df = data.get(BENCHMARK)
            if spy_df is not None and len(spy_df) >= 32:
                sma30   = calc_sma(spy_df["Close"], SMA_LEN)
                close   = float(spy_df["Close"].iloc[-1])
                sma_val = float(sma30.iloc[-1])
                prev_cl = float(spy_df["Close"].iloc[-2])
                spy_test = {
                    "close":   close,
                    "sma30":   sma_val,
                    "above":   close > sma_val,
                    "pct_sma": round((close / sma_val - 1) * 100, 1),
                    "wk_pct":  round((close / prev_cl - 1) * 100, 1),
                }
        except Exception:
            pass

        if df_test.empty:
            print("  [!] Screener sin resultados - no hay senales activas.")
            sys.exit(0)

        print(f"  OK Screener listo - {len(df_test)} setups encontrados.")

    except Exception as exc:
        print(f"  [!] Error ejecutando screener: {exc}")
        print("  Revisa la conexion a Internet y que yfinance este instalado.")
        sys.exit(1)

    stats_test = {}

    token   = cfg["bot_token"]
    chat_id = str(cfg["chat_id"])
    print(f"  Enviando mensaje de prueba a chat_id {chat_id}...")

    text = build_message(df_test, datetime.datetime.now(), stats=stats_test, spy_ctx=spy_test or None)
    ok, err = send_message(token, chat_id, text)

    if ok:
        print("\n  [OK] Mensaje enviado. Revisa Telegram.")
    else:
        print(f"\n  [ERROR] {err}\n")
        if "401" in err or "Unauthorized" in err:
            print("  El token es incorrecto. Copialo de @BotFather con exactitud.")
        elif "400" in err or "chat not found" in err.lower():
            print("  chat_id incorrecto. Ejecuta:  python aspb_telegram.py --setup")
