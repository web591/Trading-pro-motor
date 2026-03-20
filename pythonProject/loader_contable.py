import sys
import os

# 1. GENERAR CONFIG.PY (Para que el motor lea las credenciales de la nube)
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

# 2. EJECUTAR EL MOTOR ORIGINAL
try:
    print("🚀 [LOADER] Iniciando motor_saldos_v6_6_6_24.py...")
    
    # Importar el motor ejecutará automáticamente su bloque 'if __name__ == "__main__":' 
    # o su lógica de inicio si está fuera de funciones.
    import motor_saldos_v6_6_6_24
    
except Exception as e:
    print(f"❌ [ERROR] Fallo al ejecutar el motor: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)