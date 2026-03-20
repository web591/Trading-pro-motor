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
ENCRYPTION_KEY = '{os.getenv('ENCRYPTION_KEY')}'
"""
with open("config.py", "w") as f:
    f.write(config_content)

if __name__ == "__main__":
    print("✅ [LOADER] Configuración preparada.")
    
    # 2. CONFIGURAR PROXY
    proxy_url = os.getenv('PROXY_URL')
    if proxy_url:
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
        print(f"🌐 [LOADER] Proxy de España activado.")

    # 3. EJECUTAR MOTOR
    try:
        print("🚀 [LOADER] Lanzando motor en modo sincrónico...")
        runpy.run_path("motor_saldos_v6_6_6_24.py", run_name="__main__")
        print("🏁 [LOADER] Motor finalizó correctamente su ciclo.")
    except Exception as e:
        print(f"❌ [ERROR]: {e}")
        sys.exit(1)