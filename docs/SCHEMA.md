# Dashboard Data Contract — Weinstein Operativo

Este documento define el contrato JSON que consume el dashboard estático
(`docs/index.html`). El generador Python debe producir exactamente esta forma
para que la UI funcione sin cambios.

Hay **dos archivos** publicados en `docs/`:

| Archivo               | Propósito                                          | Frecuencia |
| --------------------- | -------------------------------------------------- | ---------- |
| `data.json`           | Estado operativo: cartera, acciones, señales       | Semanal    |
| `update_status.json`  | Salud del pipeline (cuándo corrió, errores)        | Cada job   |

Separamos los dos para que el dashboard pueda mostrar "PIPELINE EN ERROR"
incluso cuando `data.json` está obsoleto.

---

## 1. `update_status.json`

```jsonc
{
  "status": "OK",                          // "OK" | "EJECUTANDO" | "PENDIENTE" | "ERROR"
  "last_run_at": "2026-04-12T22:03:21Z",   // ISO-8601 UTC, último intento
  "next_run_at": "2026-04-19T22:00:00Z",   // ISO-8601 UTC, próximo cron
  "last_data_date": "2026-04-10",          // YYYY-MM-DD del último viernes con datos
  "errors": [],                            // array de strings, vacío si OK
  "log_url": null                          // string opcional con URL al log de GH Actions
}
```

### Estados y tratamiento visual

| `status`     | Cuándo                                    | Banner UI                       |
| ------------ | ----------------------------------------- | ------------------------------- |
| `OK`         | Último run terminó sin errores            | Verde, discreto                 |
| `EJECUTANDO` | Job en curso (cron lanzado)               | Azul pulsante                   |
| `PENDIENTE`  | Pasaron > 24 h del `next_run_at` sin OK   | Amber, "Actualización atrasada" |
| `ERROR`      | Último run falló                          | Rojo grueso, bloquea decisiones |

---

## 2. `data.json`

Estructura de alto nivel:

```jsonc
{
  "meta": { ... },
  "market_timing": { ... },
  "portfolio": { ... },
  "actions_this_week": { ... },
  "signals": { ... },
  "live_signal_monitor": { ... }
}
```

### 2.1 `meta`

```jsonc
{
  "generated_at": "2026-04-12T22:01:14Z",  // cuando se generó el JSON
  "data_date": "2026-04-10",                // viernes del cierre semanal
  "version": "1.0",
  "method": "Weinstein Stage 2 + Mansfield RS"
}
```

### 2.2 `market_timing`

```jsonc
{
  "status": "VERDE",                  // "VERDE" | "ROJO"
  "nh_nl_ok": true,                   // bool, regla NH-NL pasa
  "qqq_above_wma_ok": true,           // bool, QQQ > WMA30
  "nh_nl_value": 1.85,                // ratio observado
  "qqq_close": 481.20,
  "qqq_wma30": 462.10,
  "summary": "Mercado favorable para nuevas entradas"
}
```

**Regla de UI:** si `status == "ROJO"`, el panel de "Acciones a Tomar"
**no muestra COMPRAS** y aparece un banner rojo en el header.
Las VENTAS y STOPS sí se ejecutan en mercado rojo.

### 2.3 `portfolio`

```jsonc
{
  "capital_total": 100000,
  "n_positions": 4,
  "n_max_positions": 8,
  "cash_pct": 0,
  "exposure_pct": 100,
  "positions": [
    {
      "symbol": "BKR",
      "name": "Baker Hughes",
      "entry_price": 56.00,
      "entry_date": "2025-01-01",
      "weeks_held": 67,
      "current_price": 58.20,
      "pnl_pct": 3.93,                // % vs entry
      "pnl_r": 1.31,                  // R-multiple actual
      "stop_price": 54.32,
      "stop_distance_pct": -6.67,     // % desde precio actual al stop (negativo = stop abajo)
      "level_2r": 59.36,
      "level_3r": 61.04,
      "wma30": 56.00,
      "mansfield": 0.42,
      "subindustry": "Oil & Gas Equipment",
      "status": "HOLD",               // "HOLD" | "STOP_ALERT" | "SELL_2R" | "SELL_3R" | "RUNNER"
      "sizing": "100%",               // "100%" | "50%"
      "note": "Posición sólida, dejar correr"
    }
  ]
}
```

### 2.4 `actions_this_week`

Acciones recomendadas para ejecutar en la apertura del lunes.

```jsonc
{
  "buys": [
    {
      "symbol": "GOOGL",
      "name": "Alphabet",
      "type": "FIRST_GEN",            // "FIRST_GEN" | "CONTINUATION"
      "entry_price": 178.40,
      "stop_price": 173.05,
      "stop_distance_pct": -3.00,
      "atr_distance": 1.8,            // distancia entry-WMA30 en ATRs
      "sizing": "100%",               // "100%" | "50%"
      "level_2r": 189.10,
      "level_3r": 199.80,
      "mansfield": 0.85,
      "subindustry": "Internet Services",
      "rationale": "FIRST_GEN con MS alto y entrada cerca de WMA30"
    }
  ],
  "partials_2r": [
    {
      "symbol": "HSY",
      "name": "Hershey",
      "current_price": 228.40,
      "level_2r": 226.96,
      "shares_to_sell_pct": 25,
      "rationale": "Vender 25% al cruzar 2R"
    }
  ],
  "partials_3r": [],
  "stops": [
    {
      "symbol": "CNP",
      "name": "CenterPoint",
      "current_price": 40.10,
      "stop_price": 41.20,
      "rationale": "Cierre semanal bajo stop. Salir 100%."
    }
  ]
}
```

### 2.5 `signals`

Candidatos del semanal con `core_score == 5`. Solo informativos: el módulo
`actions_this_week.buys` ya selecciona los reales según presupuesto y MT.

```jsonc
{
  "first_gen": [
    {
      "symbol": "GOOGL",
      "name": "Alphabet",
      "type": "FIRST_GEN",
      "current_price": 178.40,
      "wma30": 175.20,
      "stop_price": 173.05,
      "mansfield": 0.85,
      "subind_ms": 0.72,
      "atr_pct": 2.1,
      "subindustry": "Internet Services",
      "core_score": 5
    }
  ],
  "continuation": [
    {
      "symbol": "AVGO",
      "name": "Broadcom",
      "type": "CONTINUATION",
      "current_price": 1820.50,
      "wma30": 1755.00,
      "stop_price": 1702.35,
      "mansfield": 0.78,
      "subind_ms": 0.65,
      "atr_pct": 2.8,
      "subindustry": "Semiconductors",
      "core_score": 5
    }
  ]
}
```

### 2.6 `live_signal_monitor`

Tickers que aún no han disparado pero están a < 1% de cruzar la WMA30
con MS positivo. Permite anticipar qué entrará la semana siguiente.

```jsonc
{
  "watchlist": [
    {
      "symbol": "NVDA",
      "name": "NVIDIA",
      "current_price": 875.20,
      "wma30": 872.10,
      "distance_to_wma_pct": 0.36,
      "mansfield": 0.92,
      "trigger_distance_pct": 0.50,
      "note": "A 0.5% del trigger semanal"
    }
  ]
}
```

---

## 3. Reglas de validación que el generador debe cumplir

1. **Todos los precios** son `float` con 2 decimales máximo cuando se renderizan,
   pero el JSON puede llevar más precisión.
2. **Todas las fechas** en formato `YYYY-MM-DD` o ISO-8601 con `Z` para UTC.
3. **`mansfield`** y **`subind_ms`** vienen como `float`, escala libre (la UI
   formatea con signo).
4. **Arrays vacíos son válidos** (`buys: []`, `stops: []`, etc.). La UI los trata
   como "nada que hacer en este bloque" y muestra el empty-state correspondiente.
5. **Si `market_timing.status == "ROJO"`**, el generador debe forzar
   `actions_this_week.buys = []`. La UI lo doble-checkea por defensa.
6. **`positions[].status`** debe ser computado por el generador, no por la UI:
   - `STOP_ALERT` si `current_price <= stop_price`
   - `SELL_2R` si `pnl_r >= 2.0` y todavía no se ha vendido el 25%
   - `SELL_3R` si `pnl_r >= 3.0` y todavía no se ha vendido el segundo 25%
   - `RUNNER` si `pnl_r >= 3.0` y los dos parciales ya están hechos
   - `HOLD` en cualquier otro caso

---

## 4. Versionado

El campo `meta.version` permite que la UI rechace o adapte JSONs antiguos.
Por ahora `1.0`. Cualquier cambio de breaking schema → bump a `2.0`.
