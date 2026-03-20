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
"""
with open("config.py", "w") as f:
    f.write(config_content)

if __name__ == "__main__":
    print("✅ [CLOUD] Loader Maestro preparado.")
    
    # El Maestro no suele necesitar proxy (Yahoo/Finnhub no bloquean GitHub),
    # pero si quieres usarlo, puedes añadir las líneas de proxy aquí.

    try:
        # 2. EJECUTAR EL MOTOR DIRECTAMENTE
        # Esto llamará al bloque 'if __name__ == "__main__"' del código maestro
        runpy.run_path("CÓDIGO_MAESTRO_V2_23.py", run_name="__main__")
        
        print("🏁 [CLOUD] Maestro finalizó correctamente.")
    except Exception as e:
        print(f"❌ [ERROR MAESTRO]: {e}")
        sys.exit(1)