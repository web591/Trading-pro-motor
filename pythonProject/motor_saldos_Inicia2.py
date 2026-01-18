import mysql.connector
from binance.client import Client
import time, sys, os, base64, hmac, requests, random
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# --- 1. CONFIGURACIÓN Y DESCIFRADO ---
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
    if asset in ['USDT', 'USDC', 'DAI', 'BUSD', 'LDUSDT', 'LDUSDC', 'LDBUSD']: return 1.0
    search = asset[2:] if asset.startswith('LD') else asset
    cursor.execute("SELECT price FROM sys_precios_activos WHERE symbol = %s OR symbol = %s ORDER BY last_update DESC LIMIT 1", (search, f"{search}USDT"))
    res = cursor.fetchone()
    return float(res['price']) if res else 0.0

# --- 3. LÓGICA BINANCE ---
def procesar_binance(key, sec, cursor, user_id):
    start_time = time.time()
    print("   🤖 Sincronizando Binance...", end="", flush=True)
    try:
        client = Client(key, sec, requests_params={'timeout': 20})
        acc = client.get_account()
        for b in acc['balances']:
            f, l = float(b['free']), float(b['locked'])
            tot = f + l
            if tot > 0.0001:
                asset = b['asset']
                if asset.startswith('LD'): tipo = 'EARN'
                elif asset in ['USDT', 'USDC']: tipo = 'CASH'
                else: tipo = 'SPOT'
                p = obtener_precio_db(cursor, asset)
                cursor.execute("""
                    INSERT INTO sys_saldos_usuarios 
                    (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, cantidad_bloqueada, equidad_neta, precio_referencia, valor_usd)
                    VALUES (%s, 'binance', %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                    cantidad_total=%s, cantidad_disponible=%s, cantidad_bloqueada=%s, equidad_neta=%s, precio_referencia=%s, valor_usd=%s
                """, (user_id, tipo, asset, tot, f, l, tot, p, tot*p, tot, f, l, tot, p, tot*p))

        try:
            fut = client.futures_account()
            for f in fut['assets']:
                wb = float(f['walletBalance'])
                if wb > 0.01:
                    asset, pnl, equity, avail = f['asset'], float(f['unrealizedProfit']), float(f['marginBalance']), float(f['availableBalance'])
                    p = 1.0 if asset == 'USDT' else obtener_precio_db(cursor, asset)
                    cursor.execute("""
                        INSERT INTO sys_saldos_usuarios 
                        (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, pnl_no_realizado, equidad_neta, precio_referencia, valor_usd)
                        VALUES (%s, 'binance', 'PERPETUAL', %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                        cantidad_total=%s, cantidad_disponible=%s, pnl_no_realizado=%s, equidad_neta=%s, precio_referencia=%s, valor_usd=%s
                    """, (user_id, asset, wb, avail, pnl, equity, p, equity*p, wb, avail, pnl, equity, p, equity*p))
        except: pass
        duration = time.time() - start_time
        print(f" OK ✅ ({duration:.2f}s)")
    except Exception as e: print(f" Error Binance: {e}")

# --- 4. LÓGICA BINGX ---
def procesar_bingx(key, sec, cursor, user_id):
    start_time = time.time()
    print("   🟠 Sincronizando BingX...", end="", flush=True)
    try:
        base = "https://open-api.bingx.com"
        with requests.Session() as s:
            s.headers.update({'X-BX-APIKEY': key, 'User-Agent': 'Mozilla/5.0'})
            t_res = s.get(f"{base}/openApi/swap/v2/server/time", timeout=15).json()
            ts = t_res['data']['serverTime']
            def bx_req(path):
                qs = f"recvWindow=30000&timestamp={ts}"
                sig = hmac.new(sec.encode('utf-8'), qs.encode('utf-8'), sha256).hexdigest()
                return s.get(f"{base}{path}?{qs}&signature={sig}", timeout=15).json()

            # SPOT
            s_res = bx_req("/openApi/spot/v1/account/balance")
            if s_res.get('code') == 0:
                for b in s_res['data']['balances']:
                    f, l = float(b['free']), float(b['locked'])
                    if (f + l) > 0.001:
                        p = obtener_precio_db(cursor, b['asset'])
                        cursor.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, cantidad_bloqueada, equidad_neta, precio_referencia, valor_usd) VALUES (%s, 'bingx', 'SPOT', %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE cantidad_total=%s, cantidad_disponible=%s, cantidad_bloqueada=%s, equidad_neta=%s, precio_referencia=%s, valor_usd=%s", (user_id, b['asset'], f+l, f, l, f+l, p, (f+l)*p, f+l, f, l, f+l, p, (f+l)*p))

            # PERPETUAL
            f_res = bx_req("/openApi/swap/v2/user/balance")
            if f_res.get('code') == 0:
                data = f_res['data']
                items = data if isinstance(data, list) else [data]
                for f in items:
                    raw_bal = f.get('balance', f.get('walletBalance', 0))
                    wb = float(raw_bal.get('balance', 0)) if isinstance(raw_bal, dict) else float(raw_bal)
                    if wb > 0.01:
                        asset, pnl, eq = f.get('asset', 'USDT'), float(f.get('unrealizedProfit', 0)), float(f.get('equity', wb))
                        avail = float(f.get('availableMargin', eq))
                        p = obtener_precio_db(cursor, asset)
                        cursor.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, pnl_no_realizado, equidad_neta, precio_referencia, valor_usd) VALUES (%s, 'bingx', 'PERPETUAL', %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE cantidad_total=%s, cantidad_disponible=%s, pnl_no_realizado=%s, equidad_neta=%s, precio_referencia=%s, valor_usd=%s", (user_id, asset, wb, avail, pnl, eq, p, eq*p, wb, avail, pnl, eq, p, eq*p))
        duration = time.time() - start_time
        print(f" OK ✅ ({duration:.2f}s)")
    except Exception as e: print(f" Error BingX: {e}")

# --- 5. MOTOR ---
def motor():
    print(f"🚀 MOTOR V30 - MODO PRODUCCIÓN LOCAL")
    while True:
        try:
            conn = mysql.connector.connect(**config.DB_CONFIG)
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT broker_name, api_key, api_secret FROM api_keys WHERE user_id = 6 AND status = 1")
            for r in cursor.fetchall():
                k, s = descifrar_dato(r['api_key'], MASTER_KEY), descifrar_dato(r['api_secret'], MASTER_KEY)
                if k and s:
                    if r['broker_name'].lower() == 'binance': procesar_binance(k, s, cursor, 6)
                    elif r['broker_name'].lower() == 'bingx': procesar_bingx(k, s, cursor, 6)
            conn.commit()
            cursor.close(); conn.close()
            espera = random.randint(120, 150)
            print(f"✅ Ciclo Terminado {time.strftime('%H:%M:%S')}. Próximo en {espera}s.")
            time.sleep(espera)
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(30)

if __name__ == "__main__": motor()