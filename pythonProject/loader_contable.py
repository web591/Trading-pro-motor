import sys
import os
import runpy
import requests
import random

def obtener_proxy_valido():
    print("🔎 [LOADER] Buscando proxy fuera de USA...")
    # Usamos una API gratuita de proxies
    url = "https://proxylist.geonode.com/api/proxy-list?limit=10&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps&filter_lastChecked=150"
    try:
        r = requests.get(url, timeout=10)
        proxies = r.json().get('data', [])
        # Filtramos para NO usar USA
        valid_ones = [p for p in proxies if p['country'] != 'US']
        
        if valid_ones:
            selected = random.choice(valid_ones)
            px_str = f"http://{selected['ip']}:{selected['port']}"
            print(f"✅ [LOADER] Proxy seleccionado: {px_str} ({selected['country']})")
            return px_str
    except:
        print("⚠️ [LOADER] No se pudo obtener lista de proxies, intentando directo...")
    return None

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
with open("config.py", "w") as f: f.write(config_content)

# 2. CONFIGURAR ENTORNO
proxy = obtener_proxy_valido()
if proxy:
    os.environ['HTTP_PROXY'] = proxy
    os.environ['HTTPS_PROXY'] = proxy

# 3. LANZAR MOTOR
try:
    print("🚀 [LOADER] Lanzando motor_saldos_v6_6_6_24.py...")
    runpy.run_path("motor_saldos_v6_6_6_24.py", run_name="__main__")
except Exception as e:
    print(f"❌ [ERROR]: {e}")