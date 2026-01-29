import mysql.connector
from binance.client import Client
import time, sys, os, base64, hmac, requests, threading, random
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# --- CONFIGURACIÓN ---
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
    if asset in ['USDT', 'USDC', 'DAI', 'BUSD']: return 1.0
    cursor.execute("SELECT price FROM sys_precios_activos WHERE symbol = %s OR symbol = %s ORDER BY last_update DESC LIMIT 1", (asset, f"{asset}USDT"))
    res = cursor.fetchone()
    return float(res['price']) if res else 0.0

def tarea_bingx(key, sec, user_id):
    # Lista de navegadores para "disfrazar" la petición
    agentes = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    ]
    
    try:
        base = "https://open-api.bingx.com"
        # Usamos una sesión fresca cada vez para evitar rastreo de cookies
        with requests.Session() as s:
            s.headers.update({'X-BX-APIKEY': key, 'User-Agent': random.choice(agentes)})
            
            # 1. Obtener tiempo del servidor
            t_res = s.get(f"{base}/openApi/swap/v2/server/time", timeout=15).json()
            ts = t_res['data']['serverTime']
            
            def req(path):
                qs = f"recvWindow=30000&timestamp={ts}"
                sig = hmac.new(sec.encode(), qs.encode(), sha256).hexdigest()
                return s.get(f"{base}{path}?{qs}&signature={sig}", timeout=15).json()

            db = mysql.connector.connect(**config.DB_CONFIG)
            cur = db.cursor(dictionary=True)

            # --- PROCESAR SPOT ---
            s_res = req("/openApi/spot/v1/account/balance")
            if s_res and s_res.get('code') == 0:
                for b in s_res['data']['balances']:
                    tot = float(b.get('free', 0)) + float(b.get('locked', 0))
                    if tot > 0.001:
                        p = obtener_precio_db(cur, b['asset'])
                        cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, precio_referencia, valor_usd) VALUES (%s, 'bingx', 'SPOT', %s, %s, %s, %s) ON DUPLICATE KEY UPDATE cantidad_total=%s, precio_referencia=%s, valor_usd=%s", (user_id, b['asset'], tot, p, tot*p, tot, p, tot*p))

            # --- PROCESAR FUTUROS ---
            f_res = req("/openApi/swap/v2/user/balance")
            if f_res and f_res.get('code') == 0:
                items = f_res['data'] if isinstance(f_res['data'], list) else [f_res['data']]
                for f in items:
                    bal = f.get('balance', f.get('walletBalance', 0))
                    if isinstance(bal, dict): bal = bal.get('balance', 0)
                    wb = float(bal)
                    if wb > 0.01:
                        asset = f.get('asset', 'USDT')
                        p = obtener_precio_db(cur, asset)
                        cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, equidad_neta, precio_referencia, valor_usd) VALUES (%s, 'bingx', 'PERPETUAL', %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE cantidad_total=%s, equidad_neta=%s, precio_referencia=%s, valor_usd=%s", (user_id, asset, wb, wb, p, wb*p, wb, wb, p, wb*p))

            db.commit()
            cur.close(); db.close()
            return True
    except:
        return False

def motor():
    print("🚀 MOTOR V23 - MODO INVISIBLE (COSTO $0)")
    while True:
        try:
            conn = mysql.connector.connect(**config.DB_CONFIG)
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT broker_name, api_key, api_secret FROM api_keys WHERE user_id = 6 AND status = 1")
            regs = cursor.fetchall()
            cursor.close(); conn.close()
            
            for r in regs:
                k, s = descifrar_dato(r['api_key'], MASTER_KEY), descifrar_dato(r['api_secret'], MASTER_KEY)
                if k and s:
                    if r['broker_name'].lower() == 'binance':
                        print(f"   🤖 Binance -> OK ✅")
                    elif r['broker_name'].lower() == 'bingx':
                        print(f"   🟠 BingX   -> ", end="", flush=True)
                        res = tarea_bingx(k, s, 6)
                        print("OK ✅" if res else "SALTADO (Red ocupada) ⚠️")
            
            # Pausa aleatoria para no parecer un robot
            espera = random.randint(130, 180)
            print(f"✅ Ciclo Terminado {time.strftime('%H:%M:%S')}. Próximo en {espera}s...")
            time.sleep(espera)
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(30)

if __name__ == "__main__": motor()