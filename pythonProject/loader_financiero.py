import sys
import os
import mysql.connector

# 1. GENERAR CONFIG.PY AL VUELO
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

# 2. CONFIGURAR PROXY CON BYPASS PARA BASE DE DATOS
proxy_url = os.getenv('PROXY_URL')

if proxy_url:
    # Activamos el proxy para las librerías de Python (Requests, Binance, etc.)
    os.environ['HTTP_PROXY'] = proxy_url
    os.environ['HTTPS_PROXY'] = proxy_url
    
    # 💡 BYPASS CRÍTICO: Evita el error de conexión a MySQL
    # Excluimos localhost y el host de la base de datos del uso del proxy
    os.environ['NO_PROXY'] = f"localhost,127.0.0.1,{db_host}"
    
    print(f"🌐 [LOADER] Proxy configurado con Bypass para: {db_host}")

# 3. EJECUCIÓN DEL MOTOR FINANCIERO
try:
    import config  # Importamos el que acabamos de crear
    import motor_financiero_v1_3_4 as m
    
    print("🚀 [CLOUD] Iniciando Auditoría Financiera...")
    
    # Abrimos la conexión aquí para pasarla al motor
    db = mysql.connector.connect(**config.DB_CONFIG)
    
    # Ejecutamos el motor (que ahora ya tiene el sistema de LOCK que añadimos)
    m.ejecutar_motor_financiero(db)
    
    if db and db.is_connected():
        db.close()
    
    print("✅ [CLOUD] Auditoría completada y conexión cerrada.")
    
except Exception as e:
    print(f"❌ [ERROR]: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)