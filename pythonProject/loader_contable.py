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

# 2. EJECUTAR MOTOR
# ... (mantener la parte de generar el config.py que ya tienes) ...

try:
    import config
    import mysql.connector
    import motor_saldos_v6_6_6_24 as m
    
    db = mysql.connector.connect(**config.DB_CONFIG)
    cursor = db.cursor(dictionary=True)
    
    # Obtenemos los usuarios activos para procesarlos uno a uno
    cursor.execute("SELECT * FROM sys_usuarios_brokers WHERE status=1")
    usuarios = cursor.fetchall()
    
    for u in usuarios:
        print(f">> [CLOUD] Procesando User {u['user_id']} | {u['broker_name']}")
        k = m.descifrar_dato(u['api_key'], m.MASTER_KEY)
        s = m.descifrar_dato(u['api_secret'], m.MASTER_KEY)
        
        if u['broker_name'].upper() == "BINANCE":
            m.procesar_binance(db, u['user_id'], k, s)
            m.procesar_binance_um_futures(db, u['user_id'], k, s)
        elif u['broker_name'].upper() == "BINGX":
            m.procesar_bingx(db, u['user_id'], k, s)
            
    db.close()
    print("✅ [CLOUD] Saldos actualizados una sola vez.")
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)