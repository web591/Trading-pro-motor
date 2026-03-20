import sys
import os
import mysql.connector

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

# 2. EJECUTAR MOTOR LLAMANDO A LAS FUNCIONES (PARA EVITAR WHILE TRUE)
try:
    import config
    import motor_saldos_v6_6_6_24 as m
    
    # MASTER_KEY para descifrar APIs
    MASTER_KEY = os.getenv('ENCRYPTION_KEY')
    
    db = mysql.connector.connect(**config.DB_CONFIG, connect_timeout=30)
    cursor = db.cursor(dictionary=True)
    
    cursor.execute("SELECT * FROM sys_usuarios_brokers WHERE status=1")
    usuarios = cursor.fetchall()
    
    print(f"🚀 [CLOUD] Procesando {len(usuarios)} usuarios...")
    for u in usuarios:
        k = m.descifrar_dato(u['api_key'], MASTER_KEY)
        s = m.descifrar_dato(u['api_secret'], MASTER_KEY)
        
        if u['broker_name'].upper() == "BINANCE":
            m.procesar_binance(db, u['user_id'], k, s)
            m.procesar_binance_um_futures(db, u['user_id'], k, s)
            m.procesar_binance_um_positions(db, u['user_id'], k, s)
            m.procesar_binance_cm_futures(db, u['user_id'], k, s)
            m.procesar_binance_cm_positions(db, u['user_id'], k, s)
        elif u['broker_name'].upper() == "BINGX":
            m.procesar_bingx(db, u['user_id'], k, s)
            m.procesar_bingx_positions(db, u['user_id'], k, s)
            
    db.close()
    print("✅ [CLOUD] Ciclo Contable finalizado exitosamente.")
except Exception as e:
    print(f"❌ Error en Cloud: {e}")
    sys.exit(1)