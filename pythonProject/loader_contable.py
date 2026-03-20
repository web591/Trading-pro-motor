# Version 1.1 - loader_contable.py

import sys
import config_cloud as config
sys.modules['config'] = config
import motor_saldos_v6_6_6_24 as m

print("🚀 [CLOUD] Iniciando motor de saldos...")
m.main() if hasattr(m, 'main') else print("⚠️ No se halló función main")
print("✅ Proceso terminado.")