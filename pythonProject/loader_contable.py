import sys
import os
import runpy
import time

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
ENCRYPTION_KEY = '{os.getenv('ENCRYPTION_KEY')}'
"""
with open("config.py", "w") as f:
    f.write(config_content)

if __name__ == "__main__":
    print("✅ [LOADER] Entorno preparado.")
    
    # 2. CONFIGURAR PROXY
    proxy_url = os.getenv('PROXY_URL')
    if proxy_url:
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
        print(f"🌐 [LOADER] Proxy configurado correctamente.")

    # 3. EJECUTAR MOTOR DIRECTO
    # Al no usar multiprocessing, el loader esperará a que el motor termine solo
    try:
        print("🚀 [LOADER] Lanzando motor...")
        runpy.run_path("motor_saldos_v6_6_6_24.py", run_name="__main__")
        print("🏁 [LOADER] El motor ha finalizado su ejecución.")
    except Exception as e:
        print(f"❌ [ERROR CRÍTICO]: {e}")
        sys.exit(1)