import sys
import config_cloud as config
sys.modules['config'] = config
import PRICE_SYNC_V1_03 as m

print("🚀 [CLOUD] Iniciando actualización de precios...")
m.actualizar_precios()
print("✅ Proceso terminado.")