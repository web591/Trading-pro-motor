import sys
import os
import runpy
import multiprocessing
import time

# 1. GENERAR CONFIG.PY
config_content = f"""
import os
DB_CONFIG = {{
    'host': '{os.getenv('DB_HOST')}',
    'user': '{os.getenv('DB_USER')}',
    'password': '{os.getenv('DB_PASS')}',
    'database': '{os.getenv('DB_NAME')}',
    'port': int('{os.getenv('DB_PORT', 3306)}')
}}
ENCRYPTION_KEY = '{os.getenv('ENCRYPTION_KEY')}'
"""
with open("config.py", "w") as f:
    f.write(config_content)

def ejecutar_motor():
    # Esta función corre el motor original
    runpy.run_path("motor_saldos_v6_6_6_24.py", run_name="__main__")

if __name__ == "__main__":
    print("✅ [LOADER] config.py generado.")
    print("🚀 [LOADER] Iniciando Motor (Modo Ciclo Único para Nube)...")
    
    # Lanzamos el motor en un proceso separado
    p = multiprocessing.Process(target=ejecutar_motor)
    p.start()

    # ESPERAMOS 120 SEGUNDOS (2 minutos)
    # Tiempo suficiente para que procese Binance/BingX una vez
    time.sleep(120) 

    # Terminamos el proceso a la fuerza antes de que entre en el sleep de 5 min del motor
    if p.is_alive():
        print("⏱️ [LOADER] Tiempo límite alcanzado. Cerrando ciclo para ahorrar minutos.")
        p.terminate()
        p.join()

    print("✅ [LOADER] Proceso finalizado. GitHub volverá a arrancar en 5 min.")
    sys.exit(0)