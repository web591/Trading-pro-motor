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
except: 
    print("❌ Error: No se encontró la configuración o la llave maestra.")
    sys.exit(1)

def descifrar_dato(t, m):
    try:
        r = base64.b64decode(t.strip())
        p = r.split(b"::")
        c = AES.new(sha256(m.encode()).digest(), AES.MODE_CBC, p[1])
        return unpad(c.decrypt(p[0]), AES.block_size).decode().strip()
    except: return None

def obtener_precio_db(cursor, asset):
    if asset in ['USDT', 'USDC', 'DAI', 'BUSD', 'FDUSD']: return 1.0
    a_clean = asset.replace('LD', '').replace('STAKE', '').replace('SIMPLEEARN', '')
    try:
        # Buscamos en la tabla que PHP también consulta
        cursor.execute("SELECT price FROM sys_precios_activos WHERE symbol = %s OR symbol = %s LIMIT 1", (a_clean, f"{a_clean}USDT"))
        res = cursor.fetchone()
        return float(res['price']) if res else 0.0
    except: return 0.0

def tarea_binance(key, sec, user_id, db):
    try:
        client = Client(key, sec)
        cur = db.cursor(dictionary=True)
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND broker_name = %s", (user_id, 'Binance'))
        
        # 1. SPOT & EARN
        acc = client.get_account()
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.00001:
                symbol = b['asset']
                tipo = "SPOT"
                if symbol.startswith('LD') or symbol.startswith('STAKE'): tipo = "EARN"
                elif symbol in ['USDT', 'USDC', 'BUSD', 'DAI', 'FDUSD']: tipo = "CASH"
                
                p = obtener_precio_db(cur, symbol)
                v_usd = total * p
                
                sql = """INSERT INTO sys_saldos_usuarios 
                         (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, cantidad_bloqueada, precio_referencia, valor_usd, last_update) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"""
                cur.execute(sql, (user_id, 'Binance', tipo, symbol, total, float(b['free']), float(b['locked']), p, v_usd))
        
        # 2. FUTUROS (PERPETUAL) - Verificación de saldo y equidad
        try:
            futs = client.futures_account_balance()
            for f in futs:
                val_balance = float(f['balance'])
                if val_balance > 0.01:
                    eq_neta = float(f['withdrawAvailable']) # Equidad disponible
                    sql_f = """INSERT INTO sys_saldos_usuarios 
                               (user_id, broker_name, tipo_cuenta, asset, cantidad_total, equidad_neta, valor_usd, last_update) 
                               VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())"""
                    cur.execute(sql_f, (user_id, 'Binance', 'PERPETUAL', f['asset'], val_balance, eq_neta, eq_neta))
        except Exception as e_fut:
            print(f"  ⚠️ Binance Futures (ID {user_id}): Posible falta de permisos API o cuenta vacía. {e_fut}")

        db.commit()
    except Exception as e: 
        print(f" ❌ Error General Binance ID {user_id}: {e}")

def tarea_bingx(key, sec, user_id, db, session):
    try:
        cur = db.cursor(dictionary=True)
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND broker_name = %s", (user_id, 'BingX'))
        
        def bingx_request(path, params={}):
            params["timestamp"] = int(time.time() * 1000)
            params["apiKey"] = key
            # Firma: clave=valor unidos por & en orden alfabético
            query_string = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])
            signature = hmac.new(sec.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
            url = f"https://open-api.bingx.com{path}?{query_string}&signature={signature}"
            return session.get(url).json()

        # 1. BINGX SPOT
        res_spot = bingx_request("/openApi/spot/v1/account/balance")
        if res_spot.get('code') == 0:
            for b in res_spot['data']['balances']:
                total = float(b['free']) + float(b['locked'])
                if total > 0.0001:
                    p = obtener_precio_db(cur, b['asset'])
                    sql = "INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, valor_usd, last_update) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())"
                    cur.execute(sql, (user_id, 'BingX', 'SPOT', b['asset'], total, float(b['free']), total * p))
        
        # 2. BINGX FUND ACCOUNT (Donde suele estar el capital parado)
        # --- Cambios sugeridos para la Tarea Binance en v1.07 ---
        # Reemplaza la parte de futuros por esta que es más robusta:
        try:
            futs = client.futures_account() # Trae toda la cuenta, no solo balance
            assets_fut = futs.get('assets', [])
            for f in assets_fut:
                wallet_bal = float(f['walletBalance'])
                if wallet_bal > 0.01:
                    asset_name = f['asset']
                    # En futuros, el valor real es el balance de la billetera + PnL no realizado
                    # Pero para el balance estático usamos walletBalance
                    sql_f = """INSERT INTO sys_saldos_usuarios 
                               (user_id, broker_name, tipo_cuenta, asset, cantidad_total, equidad_neta, valor_usd, last_update) 
                               VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())"""
                    cur.execute(sql_f, (user_id, 'Binance', 'PERPETUAL', asset_name, wallet_bal, wallet_bal, wallet_bal))
        except Exception as e_fut:
            print(f"  ⚠️ Binance Futures (ID {user_id}): No se pudo obtener saldo. Error: {e_fut}")

        # --- Cambios para BingX en v1.07 ---
        # Añadir este endpoint para los futuros de BingX:
        # /openApi/swap/v2/user/balance  (Perpetual Futures)

def motor():
    print("🚀 MOTOR v1.06 - DEBUG MODE")
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, broker_name, api_key, api_secret FROM api_keys WHERE status = 1")
            llaves = cursor.fetchall()
            
            for r in llaves:
                uid = r['user_id']
                broker = r['broker_name'].lower()
                print(f"👤 Procesando: ID {uid} | {broker.upper()}")
                k = descifrar_dato(r['api_key'], MASTER_KEY)
                s = descifrar_dato(r['api_secret'], MASTER_KEY)
                
                if not k or not s:
                    print(f"  ⚠️ Error de descifrado para ID {uid}")
                    continue

                if 'binance' in broker: tarea_binance(k, s, uid, db)
                elif 'bingx' in broker: tarea_bingx(k, s, uid, db, session)
            
            db.close()
            espera = random.randint(120, 180)
            print(f"🏁 Ciclo completado. Durmiendo {espera}s...")
            time.sleep(espera)
        except Exception as e:
            print(f"🔥 Error Crítico Motor: {e}")
            time.sleep(30)

if __name__ == "__main__": motor()