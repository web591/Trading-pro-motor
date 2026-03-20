import sys
import os
import runpy
import multiprocessing
import time

# 1. GENERAR CONFIG.PY (Usando los Secrets de GitHub)
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
    # 2. CONFIGURACIÓN DE PROXY DESDE SEGUROS (SECRETS)
    # Ya no exponemos la contraseña aquí
    proxy_url = os.getenv('PROXY_URL')
    
    if proxy_url:
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
        print(f"🌐 [MOTOR] Saliendo por Proxy Seguro (Configurado en Secrets)...")
    else:
        print("⚠️ [MOTOR] No se detectó PROXY_URL. Binance podría fallar (Error 451).")
    
    try:
        # Ejecutamos el motor original
        runpy.run_path("motor_saldos_v6_6_6_24.py", run_name="__main__")
    except Exception as e:
        print(f"❌ [MOTOR ERROR] {e}")

if __name__ == "__main__":
    print("✅ [LOADER] Entorno preparado y config.py generado.")
    
    # Lanzamos el motor en un subproceso
    p = multiprocessing.Process(target=ejecutar_motor)
    p.start()

    # ESPERAMOS 210 SEGUNDOS (3.5 minutos)
    # Según tus logs, el Ciclo 1 con 2 usuarios termina en ~195 segundos.
    # 210s nos da el margen perfecto para cerrar antes de que empiece el Ciclo 2.
    time.sleep(210) 

    if p.is_alive():
        print("\n⏱️ [LOADER] Ciclo completado. Terminando proceso para evitar bucle infinito.")
        p.terminate()
        p.join()

    print("🏁 [LOADER] Proceso finalizado correctamente.")
    sys.exit(0)