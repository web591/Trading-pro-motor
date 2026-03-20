# Version 1.1 - loader_contable.py

import sys
import config_cloud as config
sys.modules['config'] = config
import motor_saldos_v6_6_6_24 as m

print("🚀 [CLOUD] Iniciando motor de saldos y posiciones...")
# Ejecuta la función principal de tu motor de saldos
m.ejecutar_control_saldos() if hasattr(m, 'ejecutar_control_saldos') else print("⚠️ No se halló función principal")
print("✅ Proceso terminado.")