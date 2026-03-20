import sys
import os
import mysql.connector

# 1. GENERAR CONFIG.PY AL VUELO
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

# 2. EJECUCIÓN DEL MOTOR FINANCIERO
try:
    import config  # Importamos el que acabamos de crear
    import motor_financiero_v1_3_0 as m
    
    print("🚀 [CLOUD] Iniciando Auditoría Financiera...")
    
    # Abrimos la conexión aquí para pasarla al motor como requiere tu función
    db = mysql.connector.connect(**config.DB_CONFIG)
    m.ejecutar_motor_financiero(db)
    db.close()
    
    print("✅ [CLOUD] Auditoría completada y conexión cerrada.")
    
except Exception as e:
    print(f"❌ [ERROR]: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)