import mysql.connector
from binance.client import Client
import time, sys, os, base64, hmac, requests
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# --- 1. CONFIGURACIÓN Y DESCIFRADO ---
try:
    import config
    MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
    if not MASTER_KEY:
        print("❌ ERROR: No se encontró ENCRYPTION_KEY"); sys.exit(1)
except ImportError:
    print("❌ ERROR: No se encontró config.py"); sys.exit(1)

def descifrar_dato(texto_base64, master_key):
    try:
        if not texto_base64: return None
        raw_combined = base64.b64decode(texto_base64.strip())
        if b"::" in raw_combined:
            partes = raw_combined.split(b"::")
            key_hash = sha256(master_key.encode('utf-8')).digest()
            cipher = AES.new(key_hash, AES.MODE_CBC, partes[1])
            return unpad(cipher.decrypt(partes[0]), AES.block_size).decode('utf-8').strip()
    except Exception: return None

# --- 2. UTILIDADES ---
def obtener_precio_db(cursor, asset):
    """Busca el precio más reciente en la DB."""
    if asset in ['USDT', 'USDC', 'DAI', 'BUSD', 'LDUSDT', 'LDUSDC', 'LDBUSD']: return 1.0
    search = asset[2:] if asset.startswith('LD') else asset
    cursor.execute("SELECT price FROM sys_precios_activos WHERE symbol = %s OR symbol = %s ORDER BY last_update DESC LIMIT 1", (search, f"{search}USDT"))
    res = cursor.fetchone()
    return float(res['price']) if res else 0.0

# --- 3. LOGICA BINANCE ---
def procesar_binance(key, sec, user_id):
    print(f"🤖 Procesando Binance...")
    conn = None
    try:
        # Timeout de 10s para la conexión inicial
        client = Client(key, sec, requests_params={'timeout': 10})
        acc = client.get_account()
        
        # Conectamos a DB solo cuando ya tenemos los datos de la API
        conn = mysql.connector.connect(**config.DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # Spot y Earn
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.00001:
                asset = b['asset']
                tipo = 'EARN' if asset.startswith('LD') else ('CASH' if asset in ['USDT', 'USDC'] else 'SPOT')
                p = obtener_precio_db(cursor, asset)
                cursor.execute("""
                    INSERT INTO sys_saldos_usuarios 
                    (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, cantidad_bloqueada, equidad_neta, precio_referencia, valor_usd)
                    VALUES (%s, 'binance', %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE cantidad_total=%s, cantidad_disponible=%s, cantidad_bloqueada=%s, equidad_neta=%s, precio_referencia=%s, valor_usd=%s
                """, (user_id, tipo, asset, total, float(b['free']), float(b['locked']), total, p, total*p, 
                      total, float(b['free']), float(b['locked']), total, p, total*p))
        
        # Futuros
        try:
            fut = client.futures_account(timeout=10)
            for f in fut['assets']:
                wb = float(f['walletBalance'])
                if wb > 0.01:
                    asset, pnl, equity, avail = f['asset'], float(f['unrealizedProfit']), float(f['marginBalance']), float(f['availableBalance'])
                    p = 1.0 if asset == 'USDT' else obtener_precio_db(cursor, asset)
                    cursor.execute("""
                        INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, pnl_no_realizado, equidad_neta, precio_referencia, valor_usd)
                        VALUES (%s, 'binance', 'PERPETUAL', %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE cantidad_total=%s, cantidad_disponible=%s, pnl_no_realizado=%s, equidad_neta=%s, precio_referencia=%s, valor_usd=%s
                    """, (user_id, asset, wb, avail, pnl, equity, p, equity*p, wb, avail, pnl, equity, p, equity*p))
        except Exception as ef: print(f"   ⚠️ Sin datos de Futuros Binance: {ef}")
        
        conn.commit()
    except Exception as e: print(f"   ❌ Error Binance: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close(); conn.close()

# --- 4. LOGICA BINGX ---
def procesar_bingx(key, sec, user_id):
    print(f"🟠 Procesando BingX...")
    conn = None
    try:
        def bx_req(path):
            ts = int(time.time() * 1000)
            qs = f"timestamp={ts}"
            sig = hmac.new(sec.encode('utf-8'), qs.encode('utf-8'), sha256).hexdigest()
            # Timeout explícito de 10 segundos
            return requests.get(f"https://open-api.bingx.com{path}?{qs}&signature={sig}", 
                                headers={'X-BX-APIKEY': key}, timeout=10).json()

        # Obtenemos datos de la API primero
        s_res = bx_req("/openApi/spot/v1/account/balance")
        f_res = bx_req("/openApi/swap/v2/user/balance")

        # Conectamos a DB
        conn = mysql.connector.connect(**config.DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # Procesar Spot
        if s_res.get('code') == 0:
            for b in s_res['data']['balances']:
                total = float(b['free']) + float(b['locked'])
                if total > 0.01:
                    p = obtener_precio_db(cursor, b['asset'])
                    cursor.execute("""
                        INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, cantidad_bloqueada, equidad_neta, precio_referencia, valor_usd)
                        VALUES (%s, 'bingx', 'SPOT', %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE cantidad_total=%s, cantidad_disponible=%s, cantidad_bloqueada=%s, equidad_neta=%s, precio_referencia=%s, valor_usd=%s
                    """, (user_id, b['asset'], total, float(b['free']), float(b['locked']), total, p, total*p, total, float(b['free']), float(b['locked']), total, p, total*p))

        # Procesar Perpetual
        if f_res.get('code') == 0:
            d = f_res['data']
            items = d.get('balance', d) if isinstance(d, dict) else d
            if isinstance(items, dict): items = [items]
            for f in items:
                wb = float(f.get('balance', 0))
                if wb > 0.01:
                    asset, eq, pnl, av = f.get('asset', 'USDT'), float(f.get('equity', 0)), float(f.get('unrealizedProfit', 0)), float(f.get('availableMargin', 0))
                    p = obtener_precio_db(cursor, asset)
                    cursor.execute("""
                        INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, pnl_no_realizado, equidad_neta, precio_referencia, valor_usd)
                        VALUES (%s, 'bingx', 'PERPETUAL', %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE cantidad_total=%s, cantidad_disponible=%s, pnl_no_realizado=%s, equidad_neta=%s, precio_referencia=%s, valor_usd=%s
                    """, (user_id, asset, wb, av, pnl, eq, p, eq*p, wb, av, pnl, eq, p, eq*p))
        
        conn.commit()
    except Exception as e: print(f"   ❌ Error BingX: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close(); conn.close()

# --- 5. MOTOR PRINCIPAL ---
def motor():
    print("🚀 MOTOR PRO INICIADO (Modo: Estabilidad Hostinger)")
    while True:
        try:
            # 1. Obtener llaves (Conexión corta)
            conn_main = mysql.connector.connect(**config.DB_CONFIG)
            cursor_main = conn_main.cursor(dictionary=True)
            cursor_main.execute("SELECT broker_name, api_key, api_secret FROM api_keys WHERE user_id = 6 AND status = 1")
            regs = cursor_main.fetchall()
            cursor_main.close(); conn_main.close()

            # 2. Procesar cada exchange (Abre y cierra su propia conexión)
            for reg in regs:
                key = descifrar_dato(reg['api_key'], MASTER_KEY)
                sec = descifrar_dato(reg['api_secret'], MASTER_KEY)
                if not key or not sec: continue

                if reg['broker_name'].lower() == 'binance':
                    procesar_binance(key, sec, 6)
                elif reg['broker_name'].lower() == 'bingx':
                    procesar_bingx(key, sec, 6)
            
            print(f"✅ Ciclo OK: {time.strftime('%H:%M:%S')}. Durmiendo 120s...")
            
        except Exception as e:
            print(f"❌ Error Crítico en Ciclo: {e}")
        
        # 3. Pausa controlada
        time.sleep(120)

if __name__ == "__main__":
    motor()