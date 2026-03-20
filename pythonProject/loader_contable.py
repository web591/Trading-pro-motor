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
ENCRYPTION_KEY = '{os.getenv('ENCRYPTION_KEY')}'
"""

with open("config.py", "w") as f:
    f.write(config_content)

print("✅ [LOADER] config.py generado exitosamente.")

# 2. EJECUTAR EL MOTOR
try:
    print("🚀 [LOADER] Importando motor...")
    import motor_saldos_v6_6_6_24 as motor
    
    print("⚙️ [LOADER] Forzando inicio de la función principal...")
    # Llamamos a la función que tiene tu bucle While True
    # Nota: Como GitHub tiene un tiempo límite, esto correrá hasta que 
    # se agoten los minutos de la instancia o procese el primer ciclo.
    motor.actualizar_saldos_continuo()
    
except Exception as e:
    print(f"❌ [ERROR] Fallo en ejecución: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)