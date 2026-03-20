import sys
import os
import mysql.connector

# 1. GENERAR CONFIG.PY
# Esto crea el archivo que tus motores necesitan para leer las claves de GitHub
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

# 2. EJECUTAR MOTOR
try:
    import config
    import motor_saldos_v6_6_6_24 as m
    
    print("🚀 [CLOUD] Intentando conectar a la DB de Hostinger...")
    
    # Añadimos un tiempo de espera (timeout) de 30 segundos
    db = mysql.connector.connect(
        **config.DB_CONFIG,
        connect_timeout=30
    )
    cursor = db.cursor(dictionary=True)
    
    # Obtenemos los usuarios activos
    cursor.execute("SELECT * FROM sys_usuarios_brokers WHERE status=1")
    usuarios = cursor.fetchall()
    
    if not usuarios:
        print("⚠️ No se encontraron usuarios activos para procesar.")
    
    for u in usuarios:
        print(f">> [CLOUD] Procesando User {u['user_id']} | {u['broker_name']}")
        
        # Usamos la MASTER_KEY del motor para descifrar las API Keys
        k = m.descifrar_dato(u['api_key'], m.MASTER_KEY)
        s = m.descifrar_dato(u['api_secret'], m.MASTER_KEY)
        
        if u['broker_name'].upper() == "BINANCE":
            print("   [+] Ejecutando Binance Spot y Futuros...")
            m.procesar_binance(db, u['user_id'], k, s)
            m.procesar_binance_um_futures(db, u['user_id'], k, s)
            
        elif u['broker_name'].upper() == "BINGX":
            print("   [+] Ejecutando BingX...")
            m.procesar_bingx(db, u['user_id'], k, s)
            
    db.close()
    print("✅ [CLOUD] Saldos actualizados correctamente.")

except Exception as e:
    print(f"❌ Error en el proceso: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)