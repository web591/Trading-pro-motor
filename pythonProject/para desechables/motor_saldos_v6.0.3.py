import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests, json
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

# ==========================================================
# 🔐 SEGURIDAD Y HELPERS
# ==========================================================
def descifrar_dato(t, m):
    try:
        if not t: return None
        raw = base64.b64decode(t.strip())
        partes = raw.rsplit(b":::", 1) if b":::" in raw else raw.rsplit(b"::", 1)
        if len(partes) != 2: return None
        data, iv = partes
        key_hash = sha256(m.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

# ==========================================================
# 🎯 VINCULACIÓN Y TRADUCTOR
# ==========================================================
def obtener_traductor_id(cursor, motor_fuente, ticker):
    ticker = ticker.upper().strip()
    sql = "SELECT id FROM sys_traductor_simbolos WHERE motor_fuente=%s AND ticker_motor=%s AND is_active=1 LIMIT 1"
    cursor.execute(sql, (motor_fuente, ticker))
    row = cursor.fetchone()
    if row: return row['id']

    ticker_limpio = ticker[2:] if ticker.startswith("LD") else (ticker[3:] if ticker.startswith("STK") else ticker)
    sql = "SELECT id FROM sys_traductor_simbolos WHERE underlying=%s AND motor_fuente=%s AND is_active=1 LIMIT 1"
    cursor.execute(sql, (ticker_limpio, motor_fuente))
    row = cursor.fetchone()
    if row: return row['id']

    sql = "SELECT id FROM sys_traductor_simbolos WHERE underlying=%s ORDER BY is_active DESC, fecha_creacion DESC LIMIT 1"
    cursor.execute(sql, (ticker_limpio,))
    row = cursor.fetchone()
    return row['id'] if row else None

def disparar_radar(cursor, uid, ticker, ctx):
    sql = "INSERT IGNORE INTO sys_simbolos_buscados (user_id, ticker, status, info) VALUES (%s,%s,'pendiente',%s)"
    cursor.execute(sql, (uid, ticker, f"Detectado en {ctx}"))

def obtener_precio_usd(cursor, tid, asset_name):
    asset_name = asset_name.upper()
    stables = ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']
    clean_ticker = asset_name.replace("LD", "").replace("STK", "")
    if clean_ticker in stables: return 1.0
    try:
        if tid:
            sql = "SELECT price FROM sys_precios_activos WHERE traductor_id = %s ORDER BY last_update DESC LIMIT 1"
            cursor.execute(sql, (tid,))
            row = cursor.fetchone()
            if row and row['price'] > 0: return float(row['price'])
        sql_fb = "SELECT p.price FROM sys_precios_activos p JOIN sys_traductor_simbolos t ON p.traductor_id = t.id WHERE t.underlying = %s AND t.is_active = 1 ORDER BY p.last_update DESC LIMIT 1"
        cursor.execute(sql_fb, (clean_ticker,))
        row_fb = cursor.fetchone()
        if row_fb: return float(row_fb['price'])
    except: pass
    return 0.0

# ==========================================================
# 🕒 GESTIÓN DE TIEMPO
# ==========================================================
def obtener_punto_inicio_sincro(cursor, llave_id, metodo):
    sql = "SELECT ultima_fecha_sincro FROM sys_sync_estado WHERE broker_id = %s AND metodo_api = %s LIMIT 1"
    cursor.execute(sql, (llave_id, metodo))
    row = cursor.fetchone()
    if row and row['ultima_fecha_sincro']: return int(row['ultima_fecha_sincro'])
    return int((time.time() - (30 * 24 * 60 * 60)) * 1000)

# ==========================================================
# 💰 REGISTRO DE SALDOS
# ==========================================================
def registrar_saldo(cursor, uid, tid, total, locked, asset_name, broker, tipo_cta="SPOT"):
    disponible = total - locked
    precio = obtener_precio_usd(cursor, tid, asset_name)
    valor_usd = total * precio
    margen_usado = locked if tipo_cta == "FUTURES" else 0.0
    sql = """
        INSERT INTO sys_saldos_usuarios 
        (user_id, broker_name, asset, traductor_id, cantidad_total, cantidad_disponible, 
         cantidad_bloqueada, margen_usado, valor_usd, precio_referencia, tipo_cuenta, tipo_lista, last_update)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'ACTIVO', NOW())
        ON DUPLICATE KEY UPDATE
        traductor_id = VALUES(traductor_id), cantidad_total = VALUES(cantidad_total),
        cantidad_disponible = VALUES(cantidad_disponible), cantidad_bloqueada = VALUES(cantidad_bloqueada),
        margen_usado = VALUES(margen_usado), valor_usd = VALUES(valor_usd),
        precio_referencia = VALUES(precio_referencia), last_update = NOW()
    """
    cursor.execute(sql, (uid, broker, asset_name, tid, total, disponible, locked, margen_usado, valor_usd, precio, tipo_cta))

# ==========================================================
# 🟨 PROCESADOR BINANCE
# ==========================================================
def procesar_binance(db, uid, k, s):
    try:
        client = Client(k, s)
        cursor = db.cursor(dictionary=True)
        acc = client.get_account()
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total <= 0.000001: continue
            tid = obtener_traductor_id(cursor, "binance_spot", b['asset'])
            registrar_saldo(cursor, uid, tid, total, float(b['locked']), b['asset'], "BINANCE", "SPOT")
        try:
            savings = client.get_simple_earn_account_realtime_data()
            for s_asset in savings.get('rows', []):
                total = float(s_asset['totalAmount'])
                if total <= 0: continue
                asset_ld = f"LD{s_asset['asset']}"
                tid = obtener_traductor_id(cursor, "binance_spot", asset_ld)
                registrar_saldo(cursor, uid, tid, total, 0, asset_ld, "BINANCE", "LENDING")
        except: pass 
        print(f"    [OK] Binance User {uid} procesado.")
    except Exception as e: print(f"    [!] Error Binance User {uid}: {e}")

# ==========================================================
# 🟦 PROCESADOR BINGX
# ==========================================================
def procesar_bingx(db, uid, ak, as_):
    try:
        cursor = db.cursor(dictionary=True)
        def bx_req(path, params=None):
            if params is None: params = {}
            params["timestamp"] = int(time.time()*1000)
            query = "&".join(f"{k}={params[k]}" for k in sorted(params))
            sig = hmac.new(as_.encode(), query.encode(), hashlib.sha256).hexdigest()
            url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
            return requests.get(url, headers={"X-BX-APIKEY": ak}, timeout=10).json()

        res_spot = bx_req("/openApi/spot/v1/account/balance")
        if res_spot.get("data"):
            for b in res_spot['data']['balances']:
                total = float(b['free']) + float(b['locked'])
                if total <= 0.000001: continue
                tid = obtener_traductor_id(cursor, "bingx_crypto", b['asset'])
                registrar_saldo(cursor, uid, tid, total, float(b['locked']), b['asset'], "BINGX", "SPOT")

        res_perp = bx_req("/openApi/swap/v2/user/balance")
        data_balance = res_perp.get("data", [])
        balances = data_balance if isinstance(data_balance, list) else [data_balance]
        for item in balances:
            ticker = item.get("asset")
            if not ticker or float(item.get("balance", 0)) <= 0: continue
            tid = obtener_traductor_id(cursor, "bingx_usdt_future", ticker)
            registrar_saldo(cursor, uid, tid, float(item.get("balance")), float(item.get("freezedMargin", 0)), ticker, "BINGX", "FUTURES")
        print(f"    [OK] BingX User {uid} procesado.")
    except Exception as e: print(f"    [!] Error BingX User {uid}: {e}")

# ==========================================================
# 🚀 EJECUCIÓN PRINCIPAL
# ==========================================================
def run():
    print("💎 MOTOR SALDOS v6.0.3 - MODO COMPATIBILIDAD")
    while True:
        db = None
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT id, user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1")
            users = cursor.fetchall()

            for u in users:
                k = descifrar_dato(u['api_key'], MASTER_KEY)
                s = descifrar_dato(u['api_secret'], MASTER_KEY)
                if not k or not s: continue
                
                if u['broker_name'].upper() == "BINANCE":
                    procesar_binance(db, u['user_id'], k, s)
                elif u['broker_name'].upper() == "BINGX":
                    procesar_bingx(db, u['user_id'], k, s)
                db.commit()
        except Exception as e: print(f"    [CRITICAL] Error: {e}")
        finally:
            if db and db.is_connected(): db.close()
        time.sleep(60)

if __name__ == "__main__":
    run()