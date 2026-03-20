import sys
import os

# 1. CREAR EL ARCHIVO CONFIG.PY FÍSICAMENTE PARA ENGAÑAR AL MOTOR
config_content = f"""
import os
DB_CONFIG = {{
    'host': '{os.getenv('DB_HOST')}',
    'user': '{os.getenv('DB_USER')}',
    'password': '{os.getenv('DB_PASS')}',
    'database': '{os.getenv('DB_NAME')}',
    'port': int('{os.getenv('DB_PORT', 3306)}')
}}
FINNHUB_KEY = '{os.getenv('FINNHUB_KEY')}'
ALPHA_VANTAGE_KEY = '{os.getenv('ALPHA_VANTAGE_KEY')}'
ENCRYPTION_KEY = '{os.getenv('ENCRYPTION_KEY')}'
"""

with open("config.py", "w") as f:
    f.write(config_content)

print("✅ [LOADER] Archivo config.py generado dinámicamente.")

# 2. EJECUCIÓN DEL MOTOR
try:
    import PRICE_SYNC_V1_03 as m
    print("🚀 [LOADER] Motor cargado. Iniciando actualización...")
    m.actualizar_precios()
    print("✅ [LOADER] Proceso finalizado exitosamente.")
except Exception as e:
    print(f"❌ [ERROR]: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)