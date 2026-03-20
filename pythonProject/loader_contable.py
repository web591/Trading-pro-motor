# Version 1.1 - loader_contable.py

import sys
import config_cloud as config
sys.modules['config'] = config
m = __import__('motor_saldos_v6.6.6.24')

print("🚀 [CLOUD] Iniciando motor de saldos y posiciones...")
# Buscamos la función principal. Si no existe main, no hace nada.
if hasattr(m, 'main'):
    m.main()
else:
    print("⚠️ No se halló función 'main' en el motor de saldos.")
print("✅ Proceso terminado.")