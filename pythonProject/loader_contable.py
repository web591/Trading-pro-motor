import sys
import os
import runpy
import requests

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

# 2. CONFIGURAR PROXY PARA EVITAR BLOQUEO DE EE.UU. (Binance Error 451)
# Intentamos usar un proxy de una región permitida (Ej. Alemania o Francia)
print("🌐 [LOADER] Configurando entorno de red para Binance...")

# Usaremos una variable de entorno que las librerías de Python (requests/urllib) 
# detectan automáticamente para desviar el tráfico fuera de EE.UU.
# Nota: Si tienes un proxy propio, ponlo aquí. Si no, intentaremos este público:
os.environ['HTTPS_PROXY'] = "http://proxy.server:port" # Esto es un ejemplo

# 3. EJECUTAR EL MOTOR
try:
    print("🚀 [LOADER] Lanzando motor_saldos_v6_6_6_24.py...")
    # Engañamos al sistema para que crea que no estamos en un Datacenter de EE.UU.
    runpy.run_path("motor_saldos_v6_6_6_24.py", run_name="__main__")
    
except Exception as e:
    print(f"❌ [ERROR]: {e}")
    sys.exit(1)