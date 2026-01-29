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

def tarea_binance(key, sec, user_id, db):
    try:
        client = Client(key, sec)
        cur = db.cursor(dictionary=True)
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND broker_name = %s", (user_id, 'Binance'))
        
        # 1. SPOT & EARN DATA CRUDA
        print(f"\n--- [DEBUG BINANCE ID {user_id}] ENDPOINT: /api/v3/account ---")
        acc = client.get_account()
        # Solo imprimimos balances con algo de dinero para no saturar la terminal
        balances_reales = [b for b in acc['balances'] if float(b['free']) + float(b['locked']) > 0]
        print(json.dumps(balances_reales, indent=2))

        for b in balances_reales:
            total = float(b['free']) + float(b['locked'])
            p_ref = obtener_precio_db(cur, b['asset'])
            v_usd = total * p_ref
            cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, precio_referencia, valor_usd, last_update) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())",
                       (user_id, 'Binance', 'SPOT', b['asset'], total, p_ref, v_usd))

        # 2. FUTUROS DATA CRUDA
        try:
            print(f"\n--- [DEBUG BINANCE ID {user_id}] ENDPOINT: /fapi/v2/account ---")
            f_acc = client.futures_account()
            # Filtramos solo activos con balance en futuros
            f_assets = [f for f in f_acc['assets'] if float(f['walletBalance']) > 0]
            print(json.dumps(f_assets, indent=2))

            for f in f_assets:
                wb = float(f['walletBalance'])
                upnl = float(f['unrealizedProfit'])
                equity = wb + upnl
                cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, pnl_no_realizado, equidad_neta, valor_usd, last_update) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())",
                           (user_id, 'Binance', 'PERPETUAL', f['asset'], wb, upnl, equity, equity))
        except Exception as e: print(f" Error Futuros Binance: {e}")
        
        db.commit()
    except Exception as e: print(f" ❌ Error Binance ID {user_id}: {e}")

def tarea_bingx(key, sec, user_id, db, session):
    try:
        cur = db.cursor(dictionary=True)
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND broker_name = %s", (user_id, 'BingX'))
        
        def bingx_req(path, params={}):
            params["timestamp"] = int(time.time() * 1000)
            qs = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])
            signature = hmac.new(sec.encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
            headers = {'X-BX-APIKEY': key}
            url = f"https://open-api.bingx.com{path}?{qs}&signature={signature}"
            return session.get(url, headers=headers).json()

        # 1. SPOT CRUDO
        print(f"\n--- [DEBUG BINGX ID {user_id}] ENDPOINT: /openApi/spot/v1/account/balance ---")
        res_spot = bingx_req("/openApi/spot/v1/account/balance")
        print(json.dumps(res_spot, indent=2))
        
        if res_spot.get('code') == 0:
            for b in res_spot['data']['balances']:
                total = float(b['free']) + float(b['locked'])
                if total > 0:
                    p = obtener_precio_db(cur, b['asset'])
                    cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, valor_usd, last_update) VALUES (%s, %s, %s, %s, %s, %s, NOW())",
                               (user_id, 'BingX', 'SPOT', b['asset'], total, total * p))

        # 2. CUENTA DE FONDOS (CAPITAL) CRUDO
        print(f"\n--- [DEBUG BINGX ID {user_id}] ENDPOINT: /openApi/wallets/v1/capital/getAsset ---")
        res_fund = bingx_req("/openApi/wallets/v1/capital/getAsset")
        print(json.dumps(res_fund, indent=2))

        # 3. FUTUROS PERPETUOS CRUDO
        print(f"\n--- [DEBUG BINGX ID {user_id}] ENDPOINT: /openApi/swap/v2/user/balance ---")
        res_swap = bingx_req("/openApi/swap/v2/user/balance")
        print(json.dumps(res_swap, indent=2))

        db.commit()
    except Exception as e: print(f" ❌ Error BingX ID {user_id}: {e}")

def motor():
    print("🚀 MOTOR v1.10 - MODO ESCÁNER FULL ENDPOINTS")
    session = requests.Session()
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, broker_name, api_key, api_secret FROM api_keys WHERE status = 1")
            llaves = cursor.fetchall()
            for r in llaves:
                k = descifrar_dato(r['api_key'], MASTER_KEY)
                s = descifrar_dato(r['api_secret'], MASTER_KEY)
                if 'binance' in r['broker_name'].lower(): tarea_binance(k, s, r['user_id'], db)
                elif 'bingx' in r['broker_name'].lower(): tarea_bingx(k, s, r['user_id'], db, session)
            db.close()
            time.sleep(120)
        except Exception as e:
            print(f"🔥 Error: {e}")
            time.sleep(30)

if __name__ == "__main__": motor()