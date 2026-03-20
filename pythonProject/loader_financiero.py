# Version 1.1 - loader_financiero.py

import sys
import mysql.connector
import config_cloud as config
sys.modules['config'] = config
import motor_financiero_v1_3_0 as m

print("🚀 [CLOUD] Iniciando Auditoría Financiera...")
db = mysql.connector.connect(**config.DB_CONFIG)
m.ejecutar_motor_financiero(db)
db.close()
print("✅ Proceso terminado.")