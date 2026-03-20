# Version 1.1 - loader_fundamentales.py

import sys
import config_cloud as config
sys.modules['config'] = config
m = __import__('fundamentales_engine_v1.5')

print("🚀 [CLOUD] Iniciando Fundamentales (Yahoo + 1 Alpha Vantage)...")
m.motor_actualizacion_activos()
m.motor_alpha_inteligente()
print("✅ Proceso terminado.")