import mysql.connector
from binance.client import Client
import time, sys, os, base64, hmac, requests, hashlib, random
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# --- SEGURIDAD ---
try:
    import config
    MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
except: sys.exit(1)

def descifrar_dato(t, m):
    try:
        r = base64.b64decode(t.strip())
        p = r.split(b"::")
        c = AES.new(sha256(m.encode()).digest(), AES.MODE_CBC, p[1])
        return unpad(c.decrypt(p[0]), AES.block_size).decode().strip()
    except: return None

def obtener_precio_db(cursor, asset):
    if asset in ['USDT', 'USDC', 'DAI', 'BUSD', 'FDUSD']: return 1.0
    # Limpieza para activos EARN (ej: LDADA -> ADA)
    a_clean = asset.replace('LD', '').replace('STAKE', '')
    cursor.execute("SELECT price FROM sys_precios_activos WHERE symbol = %s OR symbol = %s ORDER BY last_update DESC LIMIT 1", (a_clean, f"{a_clean}USDT"))
    res = cursor.fetchone()
    return float(res['price']) if res else 0.0

# --- LÓGICA DE BINANCE ---
def tarea_binance(key, sec, user_id, cur):
    try:
        client = Client(key, sec)
        # Spot & Earn
        acc = client.get_account()
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.00001:
                symbol = b['asset']
                # ETIQUETADO ORIGINAL
                if symbol in ['USDT', 'USDC', 'BUSD', 'DAI', 'FDUSD']: tipo = "CASH"
                elif symbol.startswith('LD') or symbol.startswith('STAKE'): tipo = "EARN"
                else: tipo = "SPOT"
                
                p = obtener_precio_db(cur, symbol)
                v_usd = total * p
                
                sql = "REPLACE INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, cantidad_bloqueada, equidad_neta, precio_referencia, valor_usd, last_update) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"
                cur.execute(sql, (user_id, 'Binance', tipo, symbol, total, float(b['free']), float(b['locked']), v_usd, p, v_usd))
        
        # Perpetual (Futuros)
        try:
            futs = client.futures_account_balance()
            for f in futs:
                if float(f['balance']) > 0.01:
                    eq = float(f['withdrawAvailable'])
                    sql_f = "REPLACE INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, pnl_no_realizado, equidad_neta, valor_usd, last_update) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"
                    cur.execute(sql_f, (user_id, 'Binance', 'PERPETUAL', f['asset'], float(f['balance']), float(f['balance']), 0.0, eq, eq))
        except: pass
    except Exception as e: print(f" Error Binance ID {user_id}: {e}")

# --- LÓGICA DE BINGX ---
def tarea_bingx(key, sec, user_id, cur, session):
    def sign(params):
        query = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        return hmac.new(sec.encode(), query.encode(), hashlib.sha256).hexdigest()

    url_base = "https://open-api.bingx.com"
    # Consultar Balance Spot
    ts = int(time.timestamp() * 1000)
    params = {"timestamp": ts, "apiKey": key}
    params["signature"] = sign(params)
    try:
        r = session.get(f"{url_base}/openApi/spot/v1/account/balance", params=params).json()
        for b in r['data']['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0:
                p = obtener_precio_db(cur, b['asset'])
                v_usd = total * p
                sql = "REPLACE INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, valor_usd, last_update) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())"
                cur.execute(sql, (user_id, 'BingX', 'SPOT', b['asset'], total, float(b['free']), v_usd))
    except: pass

# --- MOTOR PRINCIPAL ---
def motor():
    print("🚀 MOTOR v1.03 - MULTIUSUARIO ACTIVADO")
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})

    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cur = db.cursor(dictionary=True)
            
            # Obtener todos los usuarios con llaves activas
            cur.execute("SELECT user_id, broker_name, api_key, api_secret FROM api_keys WHERE status = 1")
            llaves = cur.fetchall()
            
            for r in llaves:
                uid, broker = r['user_id'], r['broker_name'].lower()
                print(f"👤 Sincronizando: ID {uid} | {broker.upper()}")
                k = descifrar_dato(r['api_key'], MASTER_KEY)
                s = descifrar_dato(r['api_secret'], MASTER_KEY)
                
                if k and s:
                    if broker == 'binance': tarea_binance(k, s, uid, cur)
                    elif broker == 'bingx': tarea_bingx(k, s, uid, cur, session)
            
            db.commit()
            cur.close(); db.close()
            espera = random.randint(150, 200)
            print(f"✅ Ciclo Terminado. Esperando {espera}s...")
            time.sleep(espera)
        except Exception as e:
            print(f"🔥 Error Motor: {e}")
            time.sleep(30)

if __name__ == "__main__": motor()