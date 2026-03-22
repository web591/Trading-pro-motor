@echo off
title 🚀 TRADING ENGINE - SISTEMA ACTIVO

:: 1. Despertar Chrome en el perfil de Trading (Default)
:: Al no poner URLs, Chrome abrirá exactamente como lo cerraste la última vez
echo 🌐 Restaurando sesión de Chrome (Perfil Trading)...
start "" "C:\Program Files\Google\Chrome\Application\chrome.exe" --profile-directory="Default"

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
