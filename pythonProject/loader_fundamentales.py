# Version 1.1 - loader_fundamentales.py

import sys
import config_cloud as config
sys.modules['config'] = config
import fundamentales_engine_v1_5 as m

print("🚀 [CLOUD] Iniciando Fundamentales...")
m.motor_actualizacion_activos()
m.motor_alpha_inteligente()
print("✅ Proceso terminado.")