# Version 1.1 - loader_financiero.py

import sys
import mysql.connector
import config_cloud as config
sys.modules['config'] = config
m = __import__('motor_financiero_v1.3.0')

print("🚀 [CLOUD] Iniciando Auditoría Financiera...")
db = mysql.connector.connect(**config.DB_CONFIG)
m.ejecutar_motor_financiero(db)
db.close()
print("✅ Proceso terminado.")