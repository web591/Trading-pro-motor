import mysql.connector
from binance.client import Client
import time, os, base64, hmac, requests, hashlib, datetime, json
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# --- CONFIGURACIÓN ---
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
ESPERA_CICLO = 120 

def descifrar_dato(t, m):
    try:
        raw = base64.b64decode(t.strip())
        sep = b":::" if b":::" in raw else b"::"
        data, iv = raw.split(sep)
        cipher = AES.new(sha256(m.encode()).digest(), AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

def obtener_id_traductor(cur, symbol, motor_name):
    limpio = symbol.upper().replace('USDT', '').replace('USDC', '').replace('-', '')
    if limpio.startswith('LD'): limpio = limpio[2:]
    
    # Ajuste según PDF: nombre_comu_n
    cur.execute("SELECT id FROM sys_traductor_simbolos WHERE nombre_comu_n = %s OR nombre_comun = %s LIMIT 1", (limpio, limpio))
    res = cur.fetchone()
    if res: return res['id']
    
    try:
        cur.execute("INSERT IGNORE INTO sys_busqueda_resultados (ticker, motor) VALUES (%s, %s)", (limpio, motor_name))
    except: pass
    return None

# --- MOTOR BINANCE (CORREGIDO) ---
def motor_binance_v316(k, s, user_id, db):
    try:
        client = Client(k, s)
        cur = db.cursor(dictionary=True, buffered=True)
        
        # 1. SALDOS
        acc = client.get_account()
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name='Binance'", (user_id,))
        for b in acc['balances']:
            tot = float(b['free']) + float(b['locked'])
            if tot > 0.000001:
                tid = obtener_id_traductor(cur, b['asset'], 'binance_spot')
                cur.execute("""
                    INSERT INTO sys_saldos_usuarios (user_id, broker_name, asset, traductor_id, cantidad_total, last_update)
                    VALUES (%s, 'Binance', %s, %s, %s, NOW())
                """, (user_id, b['asset'], tid, tot))

        # 2. ÓRDENES (Corregido el WHERE que fallaba)
        # Eliminamos la referencia a broker_id que causó el error 1054
        cur.execute("DELETE FROM sys_open_orders WHERE user_id=%s", (user_id,))
        
        open_orders = client.get_open_orders()
        for o in open_orders:
            # Insertamos solo las columnas básicas que sabemos que existen
            cur.execute("""
                INSERT IGNORE INTO sys_open_orders (user_id, symbol, side, price, amount, status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (user_id, o['symbol'], o['side'], o['price'], o['origQty'], o['status']))
            
        print(f"   ✅ Binance OK (User {user_id})")
    except Exception as e:
        print(f"   ⚠️ Error en Binance (User {user_id}): {e}")

# --- MOTOR BINGX ---
def motor_bingx_v316(k, s, user_id, db, session):
    try:
        cur = db.cursor(dictionary=True, buffered=True)
        ts = int(time.time() * 1000)
        qs = f"timestamp={ts}"
        sig = hmac.new(s.encode(), qs.encode(), hashlib.sha256).hexdigest()
        res = session.get(f"https://open-api.bingx.com/openApi/spot/v1/account/balance?{qs}&signature={sig}", headers={'X-BX-APIKEY': k}).json()
        
        if res.get('code') == 0:
            cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name='BingX'", (user_id,))
            for b in res['data']['balances']:
                tot = float(b['free']) + float(b['locked'])
                if tot > 0.000001:
                    tid = obtener_id_traductor(cur, b['asset'], 'bingx_spot')
                    cur.execute("""
                        INSERT INTO sys_saldos_usuarios (user_id, broker_name, asset, traductor_id, cantidad_total, last_update)
                        VALUES (%s, 'BingX', %s, %s, %s, NOW())
                    """, (user_id, b['asset'], tid, tot))
            print(f"   ✅ BingX OK (User {user_id})")
    except Exception as e:
        print(f"   ⚠️ Error en BingX (User {user_id}): {e}")

# --- MAIN ---
if __name__ == "__main__":
    session = requests.Session()
    print("🚀 MOTOR v3.1.6 - LIMPIEZA DE COLUMNAS")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cur_main = db.cursor(dictionary=True, buffered=True)
            cur_main.execute("SELECT * FROM api_keys WHERE status=1")
            keys = cur_main.fetchall()

            for u in keys:
                k_d = descifrar_dato(u['api_key'], MASTER_KEY)
                s_d = descifrar_dato(u['api_secret'], MASTER_KEY)
                if not k_d or not s_d: continue
                
                if 'binance' in u['broker_name'].lower():
                    motor_binance_v316(k_d, s_d, u['user_id'], db)
                elif 'bingx' in u['broker_name'].lower():
                    motor_bingx_v316(k_d, s_d, u['user_id'], db, session)
            
            db.commit()
            db.close()
            print(f"✨ Ciclo completado. Esperando {ESPERA_CICLO}s...")
            time.sleep(ESPERA_CICLO)
        except Exception as e:
            print(f"⚠️ ERROR: {e}")
            time.sleep(30)