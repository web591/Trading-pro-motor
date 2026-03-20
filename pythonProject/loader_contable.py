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
    PROXY_URL: ${{ secrets.PROXY_URL }},
    'port': int('{os.getenv('DB_PORT', 3306)}')
}}
ENCRYPTION_KEY = '{os.getenv('ENCRYPTION_KEY')}'
"""
with open("config.py", "w") as f:
    f.write(config_content)

def ejecutar_motor():
    # En lugar de escribir la clave aquí, la leemos del Secreto de GitHub
    proxy_url = os.getenv('PROXY_URL')
    
    if proxy_url:
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
        print(f"🌐 [MOTOR] Saliendo por Proxy Seguro...")
    else:
        print("⚠️ [MOTOR] No se detectó PROXY_URL en los Secrets.")
    
    try:
        runpy.run_path("motor_saldos_v6_6_6_24.py", run_name="__main__")
    except Exception as e:
        print(f"❌ [MOTOR ERROR] {e}")

if __name__ == "__main__":
    print("✅ [LOADER] Entorno y Proxy preparados.")
    
    p = multiprocessing.Process(target=ejecutar_motor)
    p.start()

    # ESPERAMOS EXACTAMENTE 200 SEGUNDOS (3 min 20 seg)
    # Según tus logs, esto es lo que tarda un ciclo completo con 2 usuarios.
    # Al llegar aquí, cortamos el proceso para que no inicie el 2do ciclo.
    time.sleep(200) 

    if p.is_alive():
        print("\n⏱️ [LOADER] Ciclo 1 completado. Cerrando para evitar bucle infinito y ahorrar minutos.")
        p.terminate()
        p.join()

    print("🏁 [LOADER] Proceso finalizado. GitHub volverá a lanzarlo en 5 min.")
    sys.exit(0)