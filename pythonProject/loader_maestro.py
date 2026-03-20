# Version 1.0 - loader_maestro.py

import sys
import config_cloud as config
sys.modules['config'] = config
import CÓDIGO_MAESTRO_V2_23 as m

print("🚀 [CLOUD] Iniciando búsqueda de nuevos activos...")
# Ejecuta un ciclo del maestro
m.procesar_tareas_pendientes() if hasattr(m, 'procesar_tareas_pendientes') else print("⚠️ No se halló función principal")
print("✅ Proceso terminado.")