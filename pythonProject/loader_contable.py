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
ENCRYPTION_KEY = '{os.getenv('ENCRYPTION_KEY')}'
"""

with open("config.py", "w") as f:
    f.write(config_content)

print("✅ [LOADER] config.py generado exitosamente.")

# 2. EJECUTAR EL MOTOR COMO SI FUERA EL SCRIPT PRINCIPAL
try:
    print("🚀 [LOADER] Lanzando motor_saldos_v6_6_6_24.py en modo principal...")
    
    # runpy.run_path ejecuta el archivo completo, permitiendo que entre 
    # en el bloque 'if __name__ == "__main__":' automáticamente.
    runpy.run_path("motor_saldos_v6_6_6_24.py", run_name="__main__")
    
except Exception as e:
    print(f"❌ [ERROR] Fallo en ejecución: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)