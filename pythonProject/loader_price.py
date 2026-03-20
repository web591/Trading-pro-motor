import sys
import os

print("--- 🔍 REVISIÓN DE ARCHIVOS EN CLOUD ---")
archivos = os.listdir('.')
for a in archivos:
    print(f"📄 Encontrado: {a}")
print("---------------------------------------")

# 1. CREAR CONFIG.PY
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
print("✅ [LOADER] config.py generado.")

# 2. INTENTO DE IMPORTACIÓN CON NOMBRE DINÁMICO
try:
    # Buscamos el archivo que empiece por PRICE_SYNC
    motor_file = [f for f in archivos if f.startswith('PRICE_SYNC') and f.endswith('.py')]
    
    if not motor_file:
        print("❌ ERROR: No se encontró ningún archivo que empiece por PRICE_SYNC")
        sys.exit(1)
        
    nombre_modulo = motor_file[0].replace('.py', '')
    print(f"📦 Importando motor desde: {nombre_modulo}")
    
    m = __import__(nombre_modulo)
    
    print("🚀 [LOADER] Motor cargado. Iniciando...")
    m.actualizar_precios()
    print("✅ [LOADER] Proceso finalizado.")
    
except Exception as e:
    print(f"❌ [ERROR]: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)