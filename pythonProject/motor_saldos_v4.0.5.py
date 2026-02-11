import mysql.connector
from binance.client import Client
import time, os, base64
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# --- CONFIGURACIÓN ---
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
ESPERA_CICLO = getattr(config, 'ESPERA_CICLO_RAPIDO', 120) 

def descifrar_dato(texto_cifrado, master_key):
    try:
        if not texto_cifrado or not master_key: return None
        raw = base64.b64decode(texto_cifrado.strip())
        if b":::" in raw: data, iv = raw.split(b":::")
        elif b"::" in raw: data, iv = raw.split(b"::")
        else: return None
        key_hash = sha256(master_key.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

def limpiar_ticker_pro(symbol):
    s = symbol.upper().strip()
    if s.startswith('LD'):
        if len(s) <= 2: return None
        s = s[2:]
    for suffix in ['USDT', 'USDC', 'BUSD', 'BTC', 'ETH', 'PERP', 'USD']:
        if s.endswith(suffix) and s != suffix:
            s = s[:-len(suffix)]
    s = s.replace('-', '').replace('_', '').strip()
    blacklist = ['USDT', 'USDC', 'BUSD', 'BNB', 'BTC', 'ETH', 'LD', '']
    if s in blacklist or len(s) < 2: return None
    return s

def ejecutar_radar_gema(conexion_db, user_id, lista_activos_raw):
    """
    Analiza los activos y los inserta en el buzón ASIGNADOS al usuario específico.
    """
    cursor = conexion_db.cursor(dictionary=True)
    nuevos_detectados = set()

    for raw in lista_activos_raw:
        ticker_limpio = limpiar_ticker_pro(raw)
        if not ticker_limpio: continue

        # 1. ¿Ya lo tiene este usuario activado en su cartera (sys_usuarios_activos)?
        # (Esto evita notificarle algo que ya está monitoreando)
        query_activo = """
            SELECT ua.id FROM sys_usuarios_activos ua
            JOIN sys_traductor_simbolos t ON ua.traductor_id = t.id
            WHERE ua.user_id = %s AND (t.ticker_motor = %s OR t.nombre_comun = %s)
            LIMIT 1
        """
        cursor.execute(query_activo, (user_id, raw, ticker_limpio))
        if cursor.fetchone(): continue 

        # 2. ¿Ya está pendiente o encontrado en el buzón PARA ESTE USUARIO?
        cursor.execute("SELECT id FROM sys_simbolos_buscados WHERE ticker = %s AND user_id = %s", (ticker_limpio, user_id))
        if cursor.fetchone(): continue

        nuevos_detectados.add(ticker_limpio)

    # Inserción masiva
    for ticker in nuevos_detectados:
        print(f" [RADAR] Usuario {user_id} -> Nuevo activo detectado: {ticker}")
        # Insertamos con user_id. Usamos IGNORE gracias al índice compuesto (user_id + ticker)
        sql = "INSERT IGNORE INTO sys_simbolos_buscados (user_id, ticker, status) VALUES (%s, %s, 'pendiente')"
        cursor.execute(sql, (user_id, ticker))
    
    conexion_db.commit()

def actualizar_saldos(conexion_db, user_id, api_key, api_secret):
    try:
        client = Client(api_key, api_secret)
        balances = client.get_account().get('balances', [])
        cursor = conexion_db.cursor(dictionary=True)
        activos_encontrados = []

        for b in balances:
            total = float(b['free']) + float(b['locked'])
            if total > 0.00001:
                asset = b['asset']
                activos_encontrados.append(asset)
                
                asset_limpio = limpiar_ticker_pro(asset) or asset
                # Buscar traductor globalmente (el traductor es común para todos)
                cursor.execute("SELECT id FROM sys_traductor_simbolos WHERE nombre_comun = %s OR ticker_motor = %s LIMIT 1", (asset_limpio, asset))
                res_t = cursor.fetchone()
                t_id = res_t['id'] if res_t else None

                # Guardar saldo (sys_saldos_usuarios sí tiene user_id y es correcto)
                sql = """
                    INSERT INTO sys_saldos_usuarios 
                    (user_id, broker_name, tipo_cuenta, asset, cantidad_total, traductor_id, last_update)
                    VALUES (%s, 'Binance', '', %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE 
                        cantidad_total = VALUES(cantidad_total),
                        traductor_id = IFNULL(VALUES(traductor_id), traductor_id),
                        last_update = NOW()
                """
                cursor.execute(sql, (user_id, asset, total, t_id))

        conexion_db.commit()
        
        # PASAMOS EL USER_ID AL RADAR
        ejecutar_radar_gema(conexion_db, user_id, activos_encontrados)

    except Exception as e:
        print(f" [!] Error en User {user_id}: {e}")

def iniciar_motor():
    print(f"\n{'='*40}\n GEMA v4.0.5 - MULTI-USER RADAR\n{'='*40}")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)

            cursor.execute("SELECT user_id, api_key, api_secret FROM api_keys WHERE status=1 AND broker_name='Binance'")
            usuarios = cursor.fetchall()

            for u in usuarios:
                print(f" -> Procesando Usuario {u['user_id']}...")
                k = descifrar_dato(u['api_key'], MASTER_KEY)
                s = descifrar_dato(u['api_secret'], MASTER_KEY)
                
                if k and s:
                    actualizar_saldos(db, u['user_id'], k, s)
                else:
                    print(f" [!] Error de cifrado para User {u['user_id']}")

            db.close()
        except Exception as e:
            print(f" [CRITICAL] {e}")
        
        print(f"\n Ciclo terminado. Esperando {ESPERA_CICLO}s...")
        time.sleep(ESPERA_CICLO)

if __name__ == "__main__":
    iniciar_motor()