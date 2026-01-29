import mysql.connector
from binance.client import Client
import time, sys, os, base64, hmac, requests, hashlib, json
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
    stables = ['USDT', 'USDC', 'BUSD', 'DAI', 'FDUSD', 'LDUSDT', 'LDUSDC', 'LDBUSD']
    if asset.upper() in stables: return 1.0
    a_search = asset.upper().replace('LD', '').replace('LDB', '')
    try:
        cursor.execute("SELECT price FROM sys_precios_activos WHERE symbol = %s OR symbol = %s LIMIT 1", (a_search, f"{a_search}USDT"))
        res = cursor.fetchone()
        return float(res['price']) if res else 0.0
    except: return 0.0

# ... (Partes anteriores del código se mantienen igual: descifrado, obtener_precio_db, etc.)

def tarea_binance(key, sec, user_id, db):
    try:
        client = Client(key, sec)
        cur = db.cursor(dictionary=True)
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND broker_name = %s", (user_id, 'Binance'))
        
        # 1. SPOT
        acc = client.get_account()
        for b in acc['balances']:
            f, l = float(b['free']), float(b['locked'])
            total = f + l
            if total > 0:
                p = obtener_precio_db(cur, b['asset'])
                cur.execute("""INSERT INTO sys_saldos_usuarios 
                    (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, cantidad_bloqueada, precio_referencia, valor_usd, last_update) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
                    (user_id, 'Binance', 'SPOT', b['asset'], total, f, l, p, total * p))

        # 2. FUTUROS
        f_acc = client.futures_account()
        for f in f_acc['assets']:
            wb = float(f['walletBalance'])
            if wb > 0:
                upnl = float(f['unrealizedProfit'])
                m_maint = float(f.get('maintMargin', 0))
                m_init = float(f.get('initialMargin', 0))
                m_avail = float(f.get('maxWithdrawAmount', 0)) # Binance usa esto como disponible real
                eq = wb + upnl
                
                cur.execute("""INSERT INTO sys_saldos_usuarios 
                    (user_id, broker_name, tipo_cuenta, asset, cantidad_total, pnl_no_realizado, equidad_neta, 
                     margen_disponible, margen_usado, margen_mantenimiento, valor_usd, last_update) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
                    (user_id, 'Binance', 'PERPETUAL', f['asset'], wb, upnl, eq, m_avail, m_init, m_maint, eq))
        db.commit()
        print(f"   ✅ Binance ID {user_id}: OK")
    except Exception as e: print(f" ❌ Error Binance ID {user_id}: {e}")

def tarea_bingx(key, sec, user_id, db, session):
    try:
        cur = db.cursor(dictionary=True)
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND broker_name = %s", (user_id, 'BingX'))
        
        def bingx_req(path, params={}):
            params["timestamp"] = int(time.time() * 1000)
            qs = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])
            signature = hmac.new(sec.encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
            url = f"https://open-api.bingx.com{path}?{qs}&signature={signature}"
            return session.get(url, headers={'X-BX-APIKEY': key}).json()

        # 1. SPOT
        res_spot = bingx_req("/openApi/spot/v1/account/balance")
        if res_spot.get('code') == 0:
            for b in res_spot['data']['balances']:
                total = float(b['free']) + float(b['locked'])
                if total > 0:
                    p = obtener_precio_db(cur, b['asset'])
                    cur.execute("""INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, valor_usd, last_update) 
                                   VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())""",
                                (user_id, 'BingX', 'SPOT', b['asset'], total, float(b['free']), total * p))

        # 2. PERPETUOS - CORREGIDO nombres de columnas
        res_swap = bingx_req("/openApi/swap/v2/user/balance")
        if res_swap.get('code') == 0:
            d = res_swap['data']['balance']
            wb = float(d['balance'])
            if wb > 0:
                cur.execute("""INSERT INTO sys_saldos_usuarios 
                    (user_id, broker_name, tipo_cuenta, asset, cantidad_total, pnl_no_realizado, realised_profit, 
                     equidad_neta, margen_disponible, margen_usado, valor_usd, last_update) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
                    (user_id, 'BingX', 'PERPETUAL', d['asset'], wb, float(d['unrealizedProfit']), float(d['realisedProfit']),
                     float(d['equity']), float(d['availableMargin']), float(d['usedMargin']), float(d['equity'])))
        db.commit()
        print(f"   ✅ BingX ID {user_id}: OK")
    except Exception as e: print(f" ❌ Error BingX ID {user_id}: {e}")

# ... (El resto del motor se mantiene igual)

def motor():
    print("🚀 MOTOR v1.12 - GESTIÓN DE RIESGO ACTIVADA")
    session = requests.Session()
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, broker_name, api_key, api_secret FROM api_keys WHERE status = 1")
            for r in cursor.fetchall():
                k, s = descifrar_dato(r['api_key'], MASTER_KEY), descifrar_dato(r['api_secret'], MASTER_KEY)
                if 'binance' in r['broker_name'].lower(): tarea_binance(k, s, r['user_id'], db)
                elif 'bingx' in r['broker_name'].lower(): tarea_bingx(k, s, r['user_id'], db, session)
            db.close()
            print(f"--- Ciclo OK: {time.strftime('%H:%M:%S')} ---")
            time.sleep(120)
        except Exception as e: print(f"🔥 Error: {e}"); time.sleep(30)

if __name__ == "__main__": motor()