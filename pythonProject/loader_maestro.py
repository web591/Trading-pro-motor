import sys
import config_cloud as config
sys.modules['config'] = config
import CÓDIGO_MAESTRO_V2_23 as m

print("🚀 [CLOUD] Iniciando Maestro...")
# Tu maestro no tiene una función 'main', el código corre al importar.
# Por seguridad, si definiste 'bucle_operativo', la llamamos:
if hasattr(m, 'bucle_operativo'):
    m.bucle_operativo()
print("✅ Proceso terminado.")