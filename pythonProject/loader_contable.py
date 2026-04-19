import sys
import os
import runpy

# 1. GENERAR CONFIG.PY
# Extraemos el host para poder usarlo en el NO_PROXY más abajo
db_host = os.getenv('DB_HOST')

config_content = f"""
import os
DB_CONFIG = {{
    'host': '{db_host}',
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
    
    # 2. CONFIGURAR PROXY CON BYPASS PARA BASE DE DATOS
    proxy_url = os.getenv('PROXY_URL')
    
    if proxy_url:
        # Configuramos proxy global
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
        
        # 💡 ESTA ES LA CLAVE: 
        # Le decimos a Python: "Usa proxy para todo, EXCEPTO para el host de la DB"
        # Esto evita el error 2003 de conexión a MySQL.
        os.environ['NO_PROXY'] = f"localhost,127.0.0.1,{db_host}"
        
        print(f"🌐 [LOADER] Proxy activo para APIs (Binance/BingX).")
        print(f"🛣️ [LOADER] Bypass activado para Base de Datos: {db_host}")

    # 3. EJECUTAR MOTOR
    try:
        print("🚀 [LOADER] Lanzando motor en modo sincrónico...")
        # Al correr runpy, el motor heredará estas variables de entorno
        runpy.run_path("motor_saldos_v6_6_6_34.py", run_name="__main__")
        print("🏁 [LOADER] Motor finalizó correctamente su ciclo.")
    except Exception as e:
        print(f"❌ [ERROR]: {e}")
        sys.exit(1)