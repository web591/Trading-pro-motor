@echo off
title 🚀 TRADING ENGINE - SISTEMA ACTIVO

:: 1. Abrir Chrome en el perfil de Trading (Default)
echo 🌐 Abriendo paneles visuales en tu perfil de Trading...
:: Usamos la ruta directa al ejecutable y forzamos el perfil Default
start "" "C:\Program Files\Google\Chrome\Application\chrome.exe" --profile-directory="Default" "https://unmineable.com" "https://es.tradingview.com"

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
