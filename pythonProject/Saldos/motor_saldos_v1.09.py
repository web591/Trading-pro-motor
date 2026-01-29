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
    """Busca el precio sin alterar el asset original"""
    # Lista de Stables para forzar a 1.0
    stables = ['USDT', 'USDC', 'BUSD', 'DAI', 'FDUSD', 'LDUSDT', 'LDUSDC', 'LDBUSD']
    if asset.upper() in stables: return 1.0
    
    # Limpiamos solo para la búsqueda en la tabla de precios
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
        
        # 1. SPOT & EARN
        acc = client.get_account()
        print(f"   [Binance ID {user_id}] -> Procesando Spot/Earn")
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.00000001:
                p_ref = obtener_precio_db(cur, b['asset'])
                v_usd = total * p_ref # Forzamos el valor real basado en el precio que sí tenemos
                
                print(f"      - {b['asset']}: Cant {total} | Val Calc: ${v_usd:.2f}")
                
                cur.execute("""INSERT INTO sys_saldos_usuarios 
                    (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, cantidad_bloqueada, precio_referencia, valor_usd, last_update) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
                    (user_id, 'Binance', 'SPOT', b['asset'], total, float(b['free']), float(b['locked']), p_ref, v_usd))

        # 2. FUTUROS (Lógica de Equidad Neta)
        try:
            print(f"   [Binance ID {user_id}] -> Procesando Futuros")
            f_acc = client.futures_account()
            for f in f_acc['assets']:
                wb = float(f['walletBalance'])
                if wb > 0:
                    upnl = float(f['unrealizedProfit'])
                    equity = wb + upnl
                    # Aquí la equidad neta ES el valor real
                    print(f"      - Futuros {f['asset']}: Equity {equity:.2f}")
                    cur.execute("""INSERT INTO sys_saldos_usuarios 
                        (user_id, broker_name, tipo_cuenta, asset, cantidad_total, pnl_no_realizado, equidad_neta, valor_usd, last_update) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
                        (user_id, 'Binance', 'PERPETUAL', f['asset'], wb, upnl, equity, equity))
        except Exception as e: print(f"      ⚠️ Error Futuros: {e}")
        
        db.commit()
    except Exception as e: print(f" ❌ Error Binance ID {user_id}: {e}")

def tarea_bingx(key, sec, user_id, db, session):
    try:
        cur = db.cursor(dictionary=True)
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND broker_name = %s", (user_id, 'BingX'))
        
        def bingx_req(path, params={}):
            params["timestamp"] = int(time.time() * 1000)
            # Ordenar parámetros para la firma
            qs = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])
            signature = hmac.new(sec.encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
            
            # CORRECCIÓN DE HEADERS PARA BINGX
            headers = {
                'X-BX-APIKEY': key,
                'Content-Type': 'application/json'
            }
            url = f"https://open-api.bingx.com{path}?{qs}&signature={signature}"
            return session.get(url, headers=headers).json()

        print(f"   [BingX ID {user_id}] -> Procesando Spot")
        res = bingx_req("/openApi/spot/v1/account/balance")
        
        if res.get('code') == 0:
            for b in res['data']['balances']:
                total = float(b['free']) + float(b['locked'])
                if total > 0:
                    p = obtener_precio_db(cur, b['asset'])
                    v_usd = total * p
                    print(f"      - {b['asset']}: Cant {total} | Val Calc: ${v_usd:.2f}")
                    cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, valor_usd, last_update) VALUES (%s, %s, %s, %s, %s, %s, NOW())",
                               (user_id, 'BingX', 'SPOT', b['asset'], total, v_usd))
        else:
            print(f"      ⚠️ BingX Error: {res.get('msg')} (Code: {res.get('code')})")

        db.commit()
    except Exception as e: print(f" ❌ Error BingX ID {user_id}: {e}")

def motor():
    print("🚀 MOTOR v1.09 - DEBUG MODE (Valor Real & BingX Fix)")
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
                print(f"\n👤 Sincronizando: {r['broker_name']} (ID {r['user_id']})")
                
                if 'binance' in r['broker_name'].lower(): 
                    tarea_binance(k, s, r['user_id'], db)
                elif 'bingx' in r['broker_name'].lower(): 
                    tarea_bingx(k, s, r['user_id'], db, session)
            
            db.close()
            print(f"\n✅ Ciclo terminado: {time.strftime('%H:%M:%S')}")
            time.sleep(120)
        except Exception as e:
            print(f"🔥 Error Crítico: {e}")
            time.sleep(30)

if __name__ == "__main__": motor()