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
ALPHA_VANTAGE_KEY = '{os.getenv('ALPHA_VANTAGE_KEY')}'
"""
with open("config.py", "w") as f:
    f.write(config_content)

# 2. EJECUTAR MOTOR
try:
    import fundamentales_engine_v1_5 as m
    print("🚀 [CLOUD] Ejecutando Fundamentales...")
    m.motor_actualizacion_activos()
    m.motor_alpha_inteligente()
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)