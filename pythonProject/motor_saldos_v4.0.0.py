import mysql.connector
import time, os, base64, hmac, hashlib, requests, json
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# --- CONFIGURACIÓN DE SEGURIDAD ---
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

def descifrar_dato(t, m):
    try:
        raw = base64.b64decode(t.strip())
        if b":::" in raw: data, iv = raw.split(b":::")
        elif b"::" in raw: data, iv = raw.split(b"::")
        else: return None
        cipher = AES.new(sha256(m.encode()).digest(), AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

def binance_auth_req(api_key, api_secret, path, params=None):
    if params is None: params = {}
    params["timestamp"] = int(time.time() * 1000)
    query_string = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
    sig = hmac.new(api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
    url = f"https://api.binance.com{path}?{query_string}&signature={sig}"
    try:
        res = requests.get(url, headers={'X-MBX-APIKEY': api_key}, timeout=10)
        return res.json()
    except: return {}

def limpiar_ticker(symbol):
    for suffix in ['USDT', 'BTC', 'ETH', 'PERP', 'BUSD', '-']:
        symbol = symbol.replace(suffix, '')
    return symbol.strip()

def ejecutar_radar_y_saldos(db, user_id, api_key, api_secret):
    cursor = db.cursor(dictionary=True)
    
    # 1. Obtener saldos reales de Binance usando TU MÉTODO
    print(f" -> Consultando Binance para Usuario {user_id}...")
    res = binance_auth_req(api_key, api_secret, "/api/v3/account")
    
    if 'balances' not in res:
        print(f" [!] Error en respuesta de Binance: {res}")
        return

    for b in res['balances']:
        total = float(b['free']) + float(b['locked'])
        if total > 0.000001:  # Ignorar polvillo
            asset_raw = b['asset']
            asset_limpio = limpiar_ticker(asset_raw)
            
            # A. ¿Este activo ya lo conocemos en el sistema?
            # Buscamos en el traductor (aquí quité el user_id que daba error 1054)
            cursor.execute("SELECT id FROM sys_traductor_simbolos WHERE ticker_motor = %s LIMIT 1", (asset_raw,))
            trad = cursor.fetchone()
            t_id = trad['id'] if trad else None

            # B. Actualizamos o Insertamos el Saldo
            sql_saldo = """
                INSERT INTO sys_saldos_usuarios (user_id, broker_name, asset, cantidad_total, traductor_id, last_update)
                VALUES (%s, 'BINANCE', %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE 
                    cantidad_total = VALUES(cantidad_total),
                    traductor_id = IFNULL(VALUES(traductor_id), traductor_id),
                    last_update = NOW()
            """
            cursor.execute(sql_saldo, (user_id, asset_raw, total, t_id))

            # C. EL RADAR: Si no hay traductor, al buzón de búsqueda
            if not t_id:
                # ¿Ya está reportado como pendiente?
                cursor.execute("SELECT id FROM sys_simbolos_buscados WHERE ticker = %s AND user_id = %s AND status = 'pendiente'", (asset_limpio, user_id))
                if not cursor.fetchone():
                    print(f" [RADAR] ¡Activo nuevo detectado!: {asset_limpio}")
                    cursor.execute("INSERT INTO sys_simbolos_buscados (ticker, origen, status, user_id) VALUES (%s, 'MOTOR', 'pendiente', %s)", (asset_limpio, user_id))

    db.commit()
    print(f" [OK] Sincronización completada para Usuario {user_id}.")

def iniciar_motor():
    print(f"\n{'='*50}\n GEMA v4.0.0 - MODO RADAR ACTIVADO\n{'='*50}")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            
            # Sacamos las llaves de tu tabla api_keys
            cursor.execute("SELECT user_id, api_key, api_secret FROM api_keys WHERE status=1")
            for u in cursor.fetchall():
                k = descifrar_dato(u['api_key'], MASTER_KEY)
                s = descifrar_dato(u['api_secret'], MASTER_KEY)
                if k and s:
                    ejecutar_radar_y_saldos(db, u['user_id'], k, s)
            
            db.close()
        except Exception as e:
            print(f" [ERROR CRÍTICO] {e}")
        
        print(f"\n Esperando ciclo de {getattr(config, 'ESPERA_CICLO_RAPIDO', 60)}s...")
        time.sleep(getattr(config, 'ESPERA_CICLO_RAPIDO', 60))

if __name__ == "__main__":
    iniciar_motor()