# Version 1.1 - loader_price.py

import sys
import config_cloud as config
sys.modules['config'] = config
# Importamos usando __import__ porque el nombre tiene puntos
m = __import__('PRICE_SYNC_V1.03') 

print("🚀 [CLOUD] Iniciando actualización de precios...")
m.actualizar_precios()
print("✅ Proceso terminado.")