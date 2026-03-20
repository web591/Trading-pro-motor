import sys
import config_cloud as config
sys.modules['config'] = config
import motor_saldos_v6_6_6_24 as m

print("🚀 [CLOUD] Iniciando motor de saldos...")
# En tu archivo el proceso principal se llama actualizar_todo() o similar, 
# pero si no tiene una función main, lo ejecutamos directamente si existe.
if hasattr(m, 'procesar_binance'): # Solo para verificar que cargó
    print("📦 Motor cargado correctamente.")
# Nota: Si tu motor_saldos no tiene una función 'def main()', 
# asegúrate de añadir una al final del motor o llamarla aquí.