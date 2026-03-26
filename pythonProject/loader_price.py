import sys
import os
import runpy

# 1. PREPARAR LAS VARIABLES (LIMPIEZA DE ESPACIOS)
# Usamos .strip() para asegurar que no viajen espacios invisibles a config.py
db_host = (os.getenv('DB_HOST') or '').strip()
db_user = (os.getenv('DB_USER') or '').strip()
db_pass = (os.getenv('DB_PASS') or '').strip()
db_name = (os.getenv('DB_NAME') or '').strip()
proxy_url = (os.getenv('PROXY_URL') or '').strip()
finnhub = (os.getenv('FINNHUB_KEY') or '').strip()

# 2. GENERAR CONFIG.PY (El archivo que leerá el motor de precios)
config_content = f"""
import os
DB_CONFIG = {{
    'host': '{db_host}',
    'user': '{db_user}',
    'password': '{db_pass}',
    'database': '{db_name}',
    'port': int('{os.getenv('DB_PORT', 3306)}')
}}
FINNHUB_KEY = '{finnhub}'
ALPHA_VANTAGE_KEY = '{os.getenv('ALPHA_VANTAGE_KEY', '')}'
ENCRYPTION_KEY = '{os.getenv('ENCRYPTION_KEY', '')}'
PROXY_URL = '{proxy_url}'
"""

with open("config.py", "w") as f:
    f.write(config_content)

if __name__ == "__main__":
    print("✅ [CLOUD] Loader Price preparado (Variables limpias).")
    try:
        archivos = os.listdir('.')
        # Buscamos el archivo del motor (PRICE_SYNC_V1_03.py)
        motor_file = [f for f in archivos if f.startswith('PRICE_SYNC') and f.endswith('.py')]
        if motor_file:
            print(f"🚀 Iniciando motor: {motor_file[0]}")
            runpy.run_path(motor_file[0], run_name="__main__")
        else:
            print("❌ No se encontró ningún archivo PRICE_SYNC en la carpeta.")
    except Exception as e:
        print(f"❌ [ERROR CRÍTICO]: {e}")
        sys.exit(1)