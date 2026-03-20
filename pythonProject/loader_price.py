import sys
import os
import runpy

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
FINNHUB_KEY = '{os.getenv('FINNHUB_KEY')}'
ALPHA_VANTAGE_KEY = '{os.getenv('ALPHA_VANTAGE_KEY')}'
ENCRYPTION_KEY = '{os.getenv('ENCRYPTION_KEY')}'
"""
with open("config.py", "w") as f:
    f.write(config_content)

if __name__ == "__main__":
    print("✅ [CLOUD] Loader Price preparado.")

    # 2. CONFIGURAR PROXY PARA BINANCE (IGUAL QUE EL CONTABLE)
    proxy_url = "http://brd-customer-hl_59960205-zone-espana:3r793x84f9m7@brd.superproxy.io:22225"
    os.environ['HTTP_PROXY'] = proxy_url
    os.environ['HTTPS_PROXY'] = proxy_url
    print("🌐 [CLOUD] Proxy de España activado para evitar bloqueos.")

    try:
        # 3. EJECUTAR EL MOTOR DE PRECIOS
        # Buscamos el archivo PRICE_SYNC dinámicamente
        archivos = os.listdir('.')
        motor_file = [f for f in archivos if f.startswith('PRICE_SYNC') and f.endswith('.py')]
        
        if motor_file:
            print(f"📦 Lanzando motor: {motor_file[0]}")
            runpy.run_path(motor_file[0], run_name="__main__")
        else:
            print("❌ No se encontró el archivo PRICE_SYNC_V1_03.py")
            
    except Exception as e:
        print(f"❌ [ERROR LOADER]: {e}")
        sys.exit(1)