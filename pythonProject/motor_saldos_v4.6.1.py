import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from datetime import datetime, timezone
import config

# ==========================================================
# 🛡️ CAPA 1: SEGURIDAD (AES-CBC)
# ==========================================================
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

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

def format_date_sql(ts_ms):
    return datetime.fromtimestamp(int(ts_ms)/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

# ==========================================================
# 🧠 CAPA 2: NORMALIZADOR ALINEADO A TUS CSV (Binance/BingX)
# ==========================================================
def normalizar_categoria_total(op_raw):
    op = str(op_raw).upper().strip()
    # Categorías extraídas de binance_carga_consolidada.py e importadorbingx_csv.py
    ingresos = ['STAKING REWARDS', 'SAVINGS INTEREST', 'BNB VAULT REWARDS', 'AIRDROP ASSETS', 
                'DISTRIBUTION', 'POOL DISTRIBUTION', 'LAUNCHPOOL', 'INTEREST', 'MINING_REWARD', 'REWARD']
    
    if any(x in op for x in ingresos): return "INGRESO_NO_OPERATIVO"
    if op in ["FEE", "TRANSACTION FEE", "COMMISSION"]: return "FEE"
    if op in ["FUNDING", "FUNDING FEE"]: return "FUNDING_FEE"
    if op in ["DEPOSIT", "INITIAL BALANCE"]: return "DEPOSIT"
    if op in ["WITHDRAW", "SEND"]: return "WITHDRAW"
    if op in ["TRADE", "PNL", "TRANSACTION BUY", "TRANSACTION SPEND"]: return "TRADE"
    return "OTRO"

# ==========================================================
# 📊 CAPA 3: INYECTORES DE BASE DE DATOS (TABLAS OFICIALES)
# ==========================================================
def registrar_en_ledger(db, uid, exch, cuenta, cat_raw, asset, monto, comision, pnl, fecha, id_ext):
    cursor = db.cursor()
    cat = normalizar_categoria_total(cat_raw)
    sql = """INSERT IGNORE INTO transacciones_globales 
             (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, comision, pnl_realizado, fecha_utc)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cursor.execute(sql, (id_ext, uid, exch, cuenta, cat, asset.upper(), float(monto), float(comision), float(pnl), fecha))
    db.commit()

def actualizar_orden_spot(db, uid, exch, symbol, side, price, qty, id_ext):
    cursor = db.cursor()
    sql = """REPLACE INTO sys_open_order_spot (user_id, exchange, symbol, side, price, qty, id_externo_ref, last_update)
             VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())"""
    cursor.execute(sql, (uid, exch, symbol, side, price, qty, id_ext))
    db.commit()

def actualizar_orden_swap(db, uid, exch, symbol, side, price, qty, id_ext):
    cursor = db.cursor()
    sql = """REPLACE INTO sys_open_orders (user_id, exchange, symbol, side, price, qty, id_externo_ref, last_update)
             VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())"""
    cursor.execute(sql, (uid, exch, symbol, side, price, qty, id_ext))
    db.commit()

# ==========================================================
# 🟨 CAPA 4: BINANCE - SINCRONIZACIÓN Y RADAR
# ==========================================================
def binance_full_sync(db, uid, k, s):
    client = Client(k, s)
    cursor = db.cursor(dictionary=True)
    
    # 1. Saldos, Radar Gema y Locked (v4.2.3 + Auditoría)
    acc = client.get_account()
    for b in acc['balances']:
        total = float(b['free']) + float(b['locked'])
        if total > 0.000001:
            asset = b['asset']
            # --- LÓGICA RADAR ---
            cursor.execute("SELECT id FROM sys_traductor_simbolos WHERE ticker_motor=%s LIMIT 1", (asset,))
            if not cursor.fetchone() and asset not in ['USDT', 'USDC']:
                cursor.execute("INSERT IGNORE INTO sys_simbolos_buscados (user_id, ticker, info) VALUES (%s, %s, 'Binance API Radar')", (uid, asset))
            
            # --- SALDOS ---
            cursor.execute("REPLACE INTO sys_saldos_usuarios (user_id, exchange, asset, cantidad_total, saldo_bloqueado, last_update) VALUES (%s, 'BINANCE', %s, %s, %s, NOW())", 
                           (uid, asset, total, b['locked']))

    # 2. Órdenes Abiertas (Spot)
    for oo in client.get_open_orders():
        actualizar_orden_spot(db, uid, "BINANCE", oo['symbol'], oo['side'], oo['price'], oo['origQty'], f"BN-SPOT-{oo['orderId']}")

    # 3. Flujo Contable (Depósitos)
    for d in client.get_deposit_history():
        registrar_en_ledger(db, uid, "BINANCE", "SPOT", "DEPOSIT", d['coin'], d['amount'], 0, 0, format_date_sql(d['insertTime']), f"BN-DEP-{d['id']}")

# ==========================================================
# 🟦 CAPA 5: BINGX - SINCRONIZACIÓN Y RADAR
# ==========================================================
def bingx_full_sync(db, uid, ak, as_):
    def bx_req(path, params=None):
        params = params or {}; params["timestamp"] = int(time.time() * 1000)
        qs = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), qs.encode(), hashlib.sha256).hexdigest()
        return requests.get(f"https://open-api.bingx.com{path}?{qs}&signature={sig}", headers={"X-BX-APIKEY": ak}).json()

    # 1. Órdenes Abiertas Spot BingX
    res_os = bx_req("/openApi/spot/v1/trade/openOrders")
    for o in res_os.get("data", {}).get("orders", []):
        actualizar_orden_spot(db, uid, "BINGX", o['symbol'], o['side'], o['price'], o['origQty'], f"BX-OS-{o['orderId']}")

    # 2. Órdenes Abiertas Swap (Perpetual/Standard)
    res_sw = bx_req("/openApi/swap/v2/trade/openOrders")
    for f in res_sw.get("data", []):
        actualizar_orden_swap(db, uid, "BINGX", f['symbol'], f['side'], f['price'], f['quantity'], f"BX-SW-{f['orderId']}")

    # 3. Income Swap (PNL y Funding)
    res_i = bx_req("/openApi/swap/v2/user/income")
    for i in res_i.get("data", []):
        registrar_en_ledger(db, uid, "BINGX", "FUTURES", i['incomeType'], i['asset'], i['income'], 0, 0, format_date_sql(i['time']), f"BX-INC-{i['tranId']}")

# ==========================================================
# 🚀 CAPA 6: MOTOR MAESTRO (EJECUCIÓN)
# ==========================================================
def iniciar_motor():
    print("💎 GEMA v4.6.5 - MOTOR TOTAL (RADAR + CONTABILIDAD + OPEN ORDERS)")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1")
            for u in cursor.fetchall():
                k, s = descifrar_dato(u['api_key'], MASTER_KEY), descifrar_dato(u['api_secret'], MASTER_KEY)
                if not k or not s: continue
                
                if u['broker_name'].upper() == 'BINANCE': binance_full_sync(db, u['user_id'], k, s)
                elif u['broker_name'].upper() == 'BINGX': bingx_full_sync(db, u['user_id'], k, s)
                
            db.close()
            print(f"[{datetime.now()}] Auditoría y Radar completados con éxito.")
        except Exception as e: print(f" [CRITICAL] {e}")
        time.sleep(180)

if __name__ == "__main__": iniciar_motor()