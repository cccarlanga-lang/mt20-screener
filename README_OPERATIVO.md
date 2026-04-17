# Sistema Weinstein - Guia Operativa

## Que hace el sistema

Cada viernes a las 22:00 (hora Madrid), el sistema:

1. Descarga datos semanales de todas las acciones del S&P 500
2. Calcula indicadores (WMA30, Mansfield RS, Momentum, ATR)
3. Detecta entradas Weinstein Stage 2 (First-Gen y Continuation)
4. Evalua el semaforo Market Timing (NH-NL + QQQ > WMA30)
5. Genera `signals_auto.json` con las senales de la semana
6. Lee tu cartera de `portfolio_manual.json`
7. Enriquece cada posicion con precios actuales, stops, niveles 2R/3R
8. Produce `docs/data.json` que alimenta el dashboard web
9. Hace commit y push automatico a GitHub Pages

El dashboard se actualiza solo. Tu solo editas UN archivo.

---

## Que archivo edito yo

**`portfolio_manual.json`** — es el UNICO archivo que tocas.

### Campos obligatorios
- `ticker`: simbolo de la accion (ej. "MSFT")
- `precio_compra`: precio al que compraste

### Campos opcionales (mejoran precision)
- `fecha_compra`: fecha YYYY-MM-DD (para calcular stop inicial real)
- `partials_done`: parciales ya ejecutados (ej. `["2R"]` o `["2R", "3R"]`)
- `note`: nombre o comentario libre

### Ejemplo real

```json
{
  "capital_total": 100000,
  "positions": [
    {
      "ticker": "MSFT",
      "precio_compra": 412.50,
      "fecha_compra": "2026-04-14",
      "note": "Microsoft"
    },
    {
      "ticker": "LMT",
      "precio_compra": 509.80,
      "fecha_compra": "2025-01-03",
      "partials_done": ["2R"],
      "note": "Lockheed Martin"
    }
  ]
}
```

### Cuando editar
- **Compras**: anade una linea con ticker + precio_compra
- **Vendes todo**: borra la linea entera
- **Parcial 2R ejecutado**: anade `"partials_done": ["2R"]`
- **Parcial 3R ejecutado**: cambia a `"partials_done": ["2R", "3R"]`

### Que NO tocar
- `signals_auto.json` — se genera solo
- `docs/data.json` — se genera solo
- `docs/update_status.json` — se genera solo
- `docs/index.html` — es el dashboard, no lo edites

---

## Como actualizar manualmente

### En local (tu ordenador)
```bash
cd weinstein_strategy

# Paso 1: Motor semanal (genera signals_auto.json)
python emit_signals_auto.py

# Paso 2: Pipeline (genera docs/data.json)
python generate_dashboard_data.py --verbose
```

### Desde GitHub (sin tocar tu ordenador)
1. Ve a tu repositorio en GitHub
2. Pestana **Actions**
3. Click en **Update Dashboard** (columna izquierda)
4. Boton **Run workflow** (esquina derecha)
5. Click **Run workflow**

---

## Como comprobar que todo va bien

### 1. Mira `docs/update_status.json`
- `"status": "OK"` = todo bien
- `"status": "ERROR"` = algo fallo (mira `"errors"`)
- `"last_data_date"` = fecha de los datos usados

### 2. Mira el dashboard
- Abre `docs/index.html` en el navegador (o la URL de GitHub Pages)
- El banner superior debe mostrar "OK" en verde
- Si muestra "PENDIENTE" o "ERROR", hay problema

### 3. Mira `signals_auto.json`
- `"regime": "GREEN"` = semaforo verde, se pueden abrir posiciones
- `"regime": "AMBER"` = solo una condicion OK, no se abren posiciones
- `"regime": "RED"` = semaforo rojo, no se abren posiciones
- Comprueba que `"signals"` tenga entradas (puede estar vacio si no hay senales)

---

## Errores mas comunes

| Problema | Causa | Solucion |
|----------|-------|----------|
| Dashboard muestra "PENDIENTE" | Hace mas de 8 dias sin actualizar | Lanza el workflow a mano desde GitHub Actions |
| Dashboard muestra "ERROR" | Fallo en el pipeline | Mira `docs/update_status.json` campo `"errors"` |
| `signals_auto.json` no se actualiza | yfinance no pudo descargar datos | Espera 1h y relanza; a veces Yahoo limita |
| Portfolio vacio en dashboard | `portfolio_manual.json` mal formateado | Comprueba las comas y comillas del JSON |
| Senales vacias | Normal si ninguna accion cumple las 5 condiciones | No es error, es que el mercado no da senales |
| GitHub Actions falla | Dependencia rota o timeout | Mira el log del run en la pestana Actions |

### Si falla yfinance
El motor (`emit_signals_auto.py`) abortara. Pero `signals_auto.json` anterior NO se borra.
El pipeline usara el signals_auto.json que ya existia. El dashboard no se rompe, solo muestra datos viejos.

### Si falla signals_auto.json
Si el archivo no existe o esta corrupto, el pipeline lo ignora y genera el dashboard
sin senales (panel de senales vacio, sin compras sugeridas). La cartera y los stops
siguen funcionando normal.

### Si falla GitHub Actions
El dashboard sigue mostrando los datos de la ultima ejecucion exitosa.
Mira el log del run fallido en la pestana Actions de tu repositorio en GitHub.
