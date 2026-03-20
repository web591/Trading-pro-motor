# Version 1.0 - loader_maestro.py

import sys
import config_cloud as config
sys.modules['config'] = config
m = __import__('CÓDIGO MAESTRO V2.23')

print("🚀 [CLOUD] Iniciando búsqueda de nuevos activos...")
# El maestro no tiene una función simple, ejecutamos su lógica de una vez
if hasattr(m, 'bucle_operativo'):
    m.bucle_operativo() 
print("✅ Proceso terminado.")