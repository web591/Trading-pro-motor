# Version 1.0 - loader_maestro.py

import sys
import config_cloud as config
sys.modules['config'] = config
import MAESTRO_V2_23 as m

print("🚀 [CLOUD] Iniciando Maestro...")
m.bucle_operativo()
print("✅ Proceso terminado.")