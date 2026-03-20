import sys
import os
import runpy

# 1. GENERAR CONFIG.PY
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
PROXY_URL = '{os.getenv('PROXY_URL', '')}'
"""
with open("config.py", "w") as f:
    f.write(config_content)

if __name__ == "__main__":
    print("✅ [CLOUD] Loader Price preparado.")
    try:
        archivos = os.listdir('.')
        motor_file = [f for f in archivos if f.startswith('PRICE_SYNC') and f.endswith('.py')]
        if motor_file:
            runpy.run_path(motor_file[0], run_name="__main__")
    except Exception as e:
        print(f"❌ [ERROR]: {e}")
        sys.exit(1)