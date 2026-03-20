# Version 1.1 - loader_fundamentales.py

import sys
import config_cloud as config
sys.modules['config'] = config
import fundamentales_engine_v1_5 as m

print("🚀 [CLOUD] Iniciando Fundamentales (Yahoo + 1 Alpha Vantage)...")
m.motor_actualizacion_activos() # Barrido Yahoo
m.motor_alpha_inteligente()      # 1 Crédito Alpha
print("✅ Proceso terminado.")