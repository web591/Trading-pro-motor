import mysql.connector
from binance.client import Client
import time, sys, os, base64, hmac, requests
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# --- 1. CONFIGURACIÓN Y DESCIFRADO (TU LÓGICA) ---
try:
    import config
    MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
    if not MASTER_KEY:
        print("❌ ERROR: No se encontró ENCRYPTION_KEY")
        sys.exit(1)
except ImportError:
    print("❌ ERROR: No se encontró config.py")
    sys.exit(1)

def descifrar_dato(texto_base64, master_key):
    try:
        if not texto_base64: return None
        raw_combined = base64.b64decode(texto_base64.strip())
        if b"::" in raw_combined:
            partes = raw_combined.split(b"::")
            cifrado, iv = partes[0], partes[1]
            key_hash = sha256(master_key.encode('utf-8')).digest()
            cipher = AES.new(key_hash, AES.MODE_CBC, iv)
            return unpad(cipher.decrypt(cifrado), AES.block_size).decode('utf-8').strip()
    except: return None

# --- 2. UTILIDADES ---
def obtener_precio_db(cursor, asset):
    if asset in ['USDT', 'USDC', 'DAI', 'BUSD', 'LDUSDT', 'LDUSDC', 'LDBUSD']: return 1.0
    search = asset[2:] if asset.startswith('LD') else asset
    cursor.execute("SELECT price FROM sys_precios_activos WHERE symbol = %s OR symbol = %s ORDER BY last_update DESC LIMIT 1", (search, f"{search}USDT"))
    res = cursor.fetchone()
    return float(res['price']) if res else 0.0

# --- 3. LOGICA BINANCE (CON TU CLASIFICACIÓN EARN/SPOT/CASH) ---
def procesar_binance(key, sec, cursor, user_id):
    print(f"   🤖 Binance (ID:{user_id})...", end="", flush=True)
    start = time.time()
    try:
        # Añadimos un pequeño timeout al cliente para evitar que se cuelgue
        client = Client(key, sec, requests_params={'timeout': 20})
        acc = client.get_account()
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.00001:
                asset = b['asset']
                if asset.startswith('LD'): tipo = 'EARN'
                elif asset in ['USDT', 'USDC']: tipo = 'CASH'
                else: tipo = 'SPOT'
                
                p = obtener_precio_db(cursor, asset)
                cursor.execute("""
                    INSERT INTO sys_saldos_usuarios 
                    (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, cantidad_bloqueada, equidad_neta, precio_referencia, valor_usd)
                    VALUES (%s, 'binance', %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE cantidad_total=%s, cantidad_disponible=%s, cantidad_bloqueada=%s, equidad_neta=%s, precio_referencia=%s, valor_usd=%s
                """, (user_id, tipo, asset, total, float(b['free']), float(b['locked']), total, p, total*p, total, float(b['free']), float(b['locked']), total, p, total*p))
        
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
                        ON DUPLICATE KEY UPDATE cantidad_total=%s, cantidad_disponible=%s, pnl_no_realizado=%s, equidad_neta=%s, precio_referencia=%s, valor_usd=%s
                    """, (user_id, asset, wb, avail, pnl, equity, p, equity*p, wb, avail, pnl, equity, p, equity*p))
        except: pass
        print(f" OK ({time.time()-start:.2f}s)")
    except Exception as e: print(f" Error: {e}")

# --- 4. LOGICA BINGX (CON TU TRIPLE RAMA + BLINDAJE DE SESIÓN) ---
def procesar_bingx(key, sec, cursor, user_id, session):
    print(f"   🟠 BingX (ID:{user_id})...", end="", flush=True)
    start = time.time()
    try:
        def bx_req(path):
            ts = int(time.time() * 1000)
            qs = f"timestamp={ts}"
            sig = hmac.new(sec.encode('utf-8'), qs.encode('utf-8'), sha256).hexdigest()
            # Usamos la sesión blindada con headers de navegador
            return session.get(f"https://open-api.bingx.com{path}?{qs}&signature={sig}", 
                               headers={'X-BX-APIKEY': key}, timeout=15).json()

        # 1. SPOT
        s_res = bx_req("/openApi/spot/v1/account/balance")
        if s_res.get('code') == 0:
            for b in s_res['data']['balances']:
                total = float(b['free']) + float(b['locked'])
                if total > 0.01:
                    p = obtener_precio_db(cursor, b['asset'])
                    cursor.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, cantidad_bloqueada, equidad_neta, precio_referencia, valor_usd) VALUES (%s, 'bingx', 'SPOT', %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE cantidad_total=%s, cantidad_disponible=%s, cantidad_bloqueada=%s, equidad_neta=%s, precio_referencia=%s, valor_usd=%s", (user_id, b['asset'], total, float(b['free']), float(b['locked']), total, p, total*p, total, float(b['free']), float(b['locked']), total, p, total*p))

        # 2. PERPETUAL
        f_res = bx_req("/openApi/swap/v2/user/balance")
        if f_res.get('code') == 0:
            d = f_res['data']
            item = d['balance'] if 'balance' in d else d
            items = item if isinstance(item, list) else [item]
            for f in items:
                wb = float(f.get('balance', 0))
                if wb > 0.01:
                    asset, eq, pnl, avail = f.get('asset', 'USDT'), float(f.get('equity', wb)), float(f.get('unrealizedProfit', 0)), float(f.get('availableMargin', 0))
                    p = obtener_precio_db(cursor, asset)
                    cursor.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, pnl_no_realizado, equidad_neta, precio_referencia, valor_usd) VALUES (%s, 'bingx', 'PERPETUAL', %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE cantidad_total=%s, cantidad_disponible=%s, pnl_no_realizado=%s, equidad_neta=%s, precio_referencia=%s, valor_usd=%s", (user_id, asset, wb, avail, pnl, eq, p, eq*p, wb, avail, pnl, eq, p, eq*p))

        # 3. STANDARD FUTURES
        st_res = bx_req("/openApi/swap/v1/user/balance")
        if st_res.get('code') == 0:
            d_st = st_res['data']
            items_st = d_st if isinstance(d_st, list) else [d_st]
            for st in items_st:
                wb = float(st.get('balance', 0))
                if wb > 0.01:
                    asset = st.get('asset', 'USDT')
                    p = obtener_precio_db(cursor, asset)
                    cursor.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, equidad_neta, precio_referencia, valor_usd) VALUES (%s, 'bingx', 'STANDARD_FUT', %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE cantidad_total=%s, equidad_neta=%s, precio_referencia=%s, valor_usd=%s", (user_id, asset, wb, wb, p, wb*p, wb, wb, p, wb*p))
        print(f" OK ({time.time()-start:.2f}s)")
    except Exception as e: print(f" Error: {e}")

# --- 5. MOTOR PRINCIPAL ---
def motor():
    print("🚀 Motor Pro V31 (Conexión Blindada)")
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    while True:
        conn = None
        try:
            conn = mysql.connector.connect(**config.DB_CONFIG)
            cursor = conn.cursor(dictionary=True) # Este cursor se usará en todo el ciclo
            
            cursor.execute("SELECT broker_name, api_key, api_secret FROM api_keys WHERE user_id = 6 AND status = 1")
            registros = cursor.fetchall()

            for reg in registros:
                key = descifrar_dato(reg['api_key'], MASTER_KEY)
                sec = descifrar_dato(reg['api_secret'], MASTER_KEY)
                
                if reg['broker_name'].lower() == 'binance':
                    procesar_binance(key, sec, cursor, 6) # Pasamos el cursor
                elif reg['broker_name'].lower() == 'bingx':
                    procesar_bingx(key, sec, cursor, 6, session) # Pasamos el cursor
            
            conn.commit()
            cursor.close()
            conn.close() # Cerramos después de procesar todos los brokers
            print(f"✅ Saldos sincronizados: {time.strftime('%H:%M:%S')}")
            time.sleep(60)
        except Exception as e:
            print(f"❌ Error: {e}")
            if conn: conn.close()
            time.sleep(30)

if __name__ == "__main__":
    motor()