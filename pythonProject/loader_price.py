import sys
import os

# 1. PREPARACIÓN: Inyectamos config ANTES de importar el motor
try:
    import config_cloud
    sys.modules['config'] = config_cloud
    print("✅ [LOADER] Configuración inyectada correctamente.")
except ImportError:
    print("❌ [LOADER] No se encontró el archivo config_cloud.py")
    sys.exit(1)

# 2. EJECUCIÓN: Ahora sí traemos el motor
try:
    import PRICE_SYNC_V1_03 as m
    print("🚀 [LOADER] Motor cargado. Iniciando actualización...")
    m.actualizar_precios()
    print("✅ [LOADER] Proceso finalizado.")
except Exception as e:
    print(f"❌ [ERROR CRÍTICO]: {str(e)}")
    import traceback
    traceback.print_exc() # Esto nos dirá la línea exacta del error en GitHub
    sys.exit(1)