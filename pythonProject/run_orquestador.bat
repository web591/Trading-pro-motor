@echo off
title 🚀 TRADING ENGINE - SISTEMA ACTIVO

:: 1. Abrir Chrome con tus paneles de monitoreo (Unmineable y TradingView)
echo 🌐 Abriendo paneles visuales...
start chrome "https://unmineable.com/address/0x22c0f265c5ccc31e336793ee9420207a81ae2680?coin=USDT" "https://es.tradingview.com/chart/0MbLy4wx/"

:: 2. Entramos a la carpeta del proyecto
cd /d "C:\Github_Proyects\Trading-pro-motor"

echo ============================================
echo   INICIANDO ORQUESTADOR DESDE DISCO LOCAL C:
echo ============================================

:: 3. Ejecutamos el orquestador
".\.venv\Scripts\python.exe" "pythonProject\orquestador_pro_v2.3.py"

echo.
echo [!] El Orquestador se ha detenido.
pause
