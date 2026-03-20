import sys
import os

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
"""
with open("config.py", "w") as f:
    f.write(config_content)

# 2. EJECUTAR MOTOR
# ... (mantener generación de config.py) ...

try:
    import CÓDIGO_MAESTRO_V2_23 as m
    import mysql.connector
    import config
    
    print("🚀 [CLOUD] Iniciando ciclo único de Maestro...")
    conn = mysql.connector.connect(**config.DB_CONFIG)
    
    # Ejecutamos la lógica que busca tareas pendientes una sola vez
    # Nota: He adaptado esto para que no entre en el 'while True' del motor
    m.mapear_binance("BTC") # Prueba de carga
    # Aquí deberías llamar a la función principal de tu maestro si la encapsulaste
    # Si no, el motor se ejecutará al importar HASTA llegar al while True.
    
    conn.close()
    print("✅ [CLOUD] Ciclo Maestro finalizado.")
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)