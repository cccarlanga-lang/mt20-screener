# ============================================================================
#  setup_aspb_scheduler.ps1
#  Configura dos tareas automaticas en Windows Task Scheduler:
#
#  Tarea 1 — "ASP-B Telegram Semanal"
#    Cada sabado a las 08:00 (hora local) envia el screener por Telegram.
#    Es el backup local del GitHub Actions (que corre en la nube a las 08:00 CEST).
#
#  Tarea 2 — "ASP-B Bot Telegram"
#    Al iniciar sesion en Windows arranca el bot en background.
#    Mientras el PC esta encendido, cualquier mensaje al bot devuelve el screener.
#
#  Uso:
#    PowerShell -ExecutionPolicy Bypass -File setup_aspb_scheduler.ps1
# ============================================================================

$ErrorActionPreference = "Stop"

# ── Detectar Python ──────────────────────────────────────────────────────────
$PythonExe = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $PythonExe) {
    Write-Error "Python no encontrado en el PATH. Instala Python 3.x y vuelve a ejecutar."
    exit 1
}
Write-Host "Python: $PythonExe"

# ── Rutas ────────────────────────────────────────────────────────────────────
$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$TelegramPy = Join-Path $ScriptDir "aspb_telegram.py"

if (-not (Test-Path $TelegramPy)) {
    Write-Error "No se encuentra aspb_telegram.py en: $ScriptDir"
    exit 1
}

Write-Host "Script:  $TelegramPy"
Write-Host ""

# ── Tarea 1: Envio automatico cada sabado 08:00 ──────────────────────────────
$action1  = New-ScheduledTaskAction `
              -Execute       $PythonExe `
              -Argument      "`"$TelegramPy`"" `
              -WorkingDirectory $ScriptDir

$trigger1 = New-ScheduledTaskTrigger `
              -Weekly -WeeksInterval 1 -DaysOfWeek Saturday -At "08:00"

$settings1 = New-ScheduledTaskSettingsSet `
               -ExecutionTimeLimit (New-TimeSpan -Minutes 30) `
               -StartWhenAvailable `
               -RunOnlyIfNetworkAvailable

Register-ScheduledTask `
    -TaskName    "ASP-B Telegram Semanal" `
    -Description "Envia el screener semanal ASP-B por Telegram cada sabado a las 08:00" `
    -Action      $action1 `
    -Trigger     $trigger1 `
    -Settings    $settings1 `
    -Force | Out-Null

Write-Host "[OK] Tarea 1 creada: 'ASP-B Telegram Semanal' (sabados 08:00 hora local)"

# ── Tarea 2: Bot listener via carpeta Startup (no requiere admin) ─────────────
# Creamos un .bat en la carpeta Startup del usuario para que el bot arranque
# automaticamente al iniciar sesion, en segundo plano.
$StartupFolder = [Environment]::GetFolderPath("Startup")
$BatPath       = Join-Path $StartupFolder "aspb_bot_telegram.bat"

$BatContent = @"
@echo off
REM ASP-B Bot Telegram — arranca en background al iniciar sesion
start "" /B "$PythonExe" "$TelegramPy" --bot
"@

Set-Content -Path $BatPath -Value $BatContent -Encoding ASCII
Write-Host "[OK] Tarea 2 creada: bot en Startup → '$BatPath'"
Write-Host ""
Write-Host "======================================================="
Write-Host "  Configuracion completada."
Write-Host ""
Write-Host "  Opcion 1 — Automatica (sin hacer nada):"
Write-Host "    GitHub Actions: sabados 08:00 CEST (PC apagado: funciona igual)"
Write-Host "    Task Scheduler: sabados 08:00 hora local (backup si el PC esta encendido)"
Write-Host ""
Write-Host "  Opcion 2 — On-demand desde Telegram:"
Write-Host "    Cuando el PC este encendido, envia cualquier mensaje al bot"
Write-Host "    y recibiras el screener completo en 1-2 minutos."
Write-Host ""
Write-Host "  Para arrancar el bot ahora sin reiniciar:"
Write-Host "    Start-Process python -ArgumentList `"'$TelegramPy' --bot`" -WindowStyle Hidden"
Write-Host "======================================================="
