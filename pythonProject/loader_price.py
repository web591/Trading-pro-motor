import sys
import config_cloud

# 1. ESTO ES VITAL: Engañamos a los motores para que crean que config_cloud es 'config'
sys.modules['config'] = config_cloud

# 2. Ahora sí importamos el motor
try:
    import PRICE_SYNC_V1_03 as m
    print("🚀 [CLOUD] Motor de Precios cargado. Iniciando proceso...")
    m.actualizar_precios()
    print("✅ [CLOUD] Proceso completado exitosamente.")
except Exception as e:
    print(f"❌ [CLOUD] Error crítico: {e}")
    sys.exit(1)