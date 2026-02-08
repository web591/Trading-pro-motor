import mysql.connector
from binance.client import Client
import time, os, base64, hmac, requests, hashlib, datetime, json
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# --- CONFIGURACIÓN DE LA v3.1.5 ---
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
ESPERA_CICLO = 120 

def descifrar_dato(t, m):
    try:
        raw = base64.b64decode(t.strip())
        # v3.1.5 soportaba ambos separadores por compatibilidad de migración
        sep = b":::" if b":::" in raw else b"::"
        data, iv = raw.split(sep)
        cipher = AES.new(sha256(m.encode()).digest(), AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

# --- EL TRADUCTOR RESILIENTE v3.1.5 ---
def obtener_id_traductor(cur, symbol, motor_name):
    # En esta versión el radar era simple y no bloqueante
    limpio = symbol.upper().replace('USDT', '').replace('USDC', '').replace('-', '')
    if limpio.startswith('LD'): limpio = limpio[2:] # Polvo de Binance
    
    cur.execute("SELECT id FROM sys_traductor_simbolos WHERE nombre_comun = %s LIMIT 1", (limpio,))
    res = cur.fetchone()
    if res: return res['id']
    
    # Radar ciego: Si falla por columnas, no importa
    try:
        cur.execute("INSERT IGNORE INTO sys_busqueda_resultados (ticker, motor, estado) VALUES (%s, %s, 'detectado')", (limpio, motor_name))
    except: pass
    return None

# --- MOTOR BINANCE v3.1.5 ---
def motor_binance_v315(k, s, user_id, db):
    try:
        client = Client(k, s)
        cur = db.cursor(dictionary=True, buffered=True)
        
        # 1. SALDOS SPOT
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

        # 2. ÓRDENES ABIERTAS (Sincronización Total)
        cur.execute("DELETE FROM sys_open_orders WHERE user_id=%s AND broker_id=1", (user_id,))
        open_orders = client.get_open_orders()
        for o in open_orders:
            cur.execute("""
                INSERT INTO sys_open_orders (user_id, broker_id, symbol, side, type, price, amount, status)
                VALUES (%s, 1, %s, %s, %s, %s, %s, %s)
            """, (user_id, o['symbol'], o['side'], o['type'], o['price'], o['origQty'], o['status']))
            
        print(f"   ✅ Binance OK (User {user_id}): {len(acc['balances'])} activos, {len(open_orders)} órdenes.")
    except Exception as e:
        print(f"   ⚠️ Error en Binance (User {user_id}): {e}")

# --- MOTOR BINGX v3.1.5 ---
def motor_bingx_v315(k, s, user_id, db, session):
    try:
        cur = db.cursor(dictionary=True, buffered=True)
        ts = int(time.time() * 1000)
        qs = f"timestamp={ts}"
        sig = hmac.new(s.encode(), qs.encode(), hashlib.sha256).hexdigest()
        
        # Solo Spot en la v3.1.5 estable
        url = f"https://open-api.bingx.com/openApi/spot/v1/account/balance?{qs}&signature={sig}"
        res = session.get(url, headers={'X-BX-APIKEY': k}).json()
        
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

# --- CICLO PRINCIPAL ---
if __name__ == "__main__":
    session = requests.Session()
    print("🚀 MOTOR v3.1.5 - RECUPERACIÓN DE ESTADO EXITOSO")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cur_main = db.cursor(dictionary=True, buffered=True)
            
            # Obtener llaves activas
            cur_main.execute("SELECT * FROM api_keys WHERE status=1")
            keys = cur_main.fetchall()
            print(f"\n--- Iniciando ciclo sobre {len(keys)} cuentas ---")

            for u in keys:
                k_d = descifrar_dato(u['api_key'], MASTER_KEY)
                s_d = descifrar_dato(u['api_secret'], MASTER_KEY)
                
                if not k_d or not s_d:
                    print(f"   ❌ Llaves inválidas para User {u['user_id']}")
                    continue
                
                if 'binance' in u['broker_name'].lower():
                    motor_binance_v315(k_d, s_d, u['user_id'], db)
                elif 'bingx' in u['broker_name'].lower():
                    motor_bingx_v315(k_d, s_d, u['user_id'], db, session)
            
            db.commit()
            db.close()
            print(f"✨ Ciclo completado con éxito. Esperando {ESPERA_CICLO}s...")
            time.sleep(ESPERA_CICLO)
        except Exception as e:
            print(f"⚠️ ERROR CRÍTICO DE CICLO: {e}")
            time.sleep(30)