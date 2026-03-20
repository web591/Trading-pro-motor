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
    # --- CONFIGURACIÓN DE WEBSHARE ---
    # Reemplaza con tus datos reales de Webshare
    # Formato: http://usuario:password@ip:puerto
    proxy_url = "http://khrsahil:wgm3gppg8ksg@64.137.96.74:6641"
    
    # Inyectamos el proxy en el proceso del motor
    os.environ['HTTP_PROXY'] = proxy_url
    os.environ['HTTPS_PROXY'] = proxy_url
    
    print(f"🌐 [MOTOR] Saliendo por Proxy de España...")
    
    # Ejecutamos tu motor original
    try:
        runpy.run_path("motor_saldos_v6_6_6_24.py", run_name="__main__")
    except Exception as e:
        print(f"❌ [MOTOR ERROR] {e}")

if __name__ == "__main__":
    print("✅ [LOADER] Entorno preparado.")
    
    # Lanzamos el motor en un subproceso
    p = multiprocessing.Process(target=ejecutar_motor)
    p.start()

    # Esperamos 3 minutos (180 seg). 
    # Tiempo de sobra para procesar los usuarios y trades una vez.
    time.sleep(300) 

    if p.is_alive():
        print("⏱️ [LOADER] Ciclo completado. Terminando proceso para evitar el sleep de 5min.")
        p.terminate()
        p.join()

    print("🏁 [LOADER] Proceso finalizado exitosamente.")
    sys.exit(0)