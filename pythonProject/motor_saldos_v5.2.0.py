import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests
from datetime import datetime, timezone
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from hashlib import sha256
import config

# ==========================================================
# 🛡️ SEGURIDAD Y FORMATO PHP
# ==========================================================
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

def descifrar_dato(t, m):
    try:
        if not t: return None
        raw = base64.b64decode(t.strip()); partes = raw.rsplit(b":::", 1) if b":::" in raw else raw.rsplit(b"::", 1)
        data, iv = partes; key_hash = sha256(m.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

def format_date_sql(ts_ms):
    return datetime.fromtimestamp(int(ts_ms)/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

# ==========================================================
# 🧠 CATEGORIZACIÓN (ESPEJO BINANCE_CONSO.CSV)
# ==========================================================
def normalizar_categoria_total(op_raw):
    op = str(op_raw).upper().strip()
    ingresos = ['STAKING', 'INTEREST', 'AIRDROP', 'REWARD', 'DISTRIBUTION', 'LAUNCHPOOL', 'DIVIDEND', 'SIMPLE_EARN', 'SAVINGS']
    if any(x in op for x in ingresos): return "INGRESO_NO_OPERATIVO"
    if any(x in op for x in ['COMMISSION', 'REBATE', 'CASH VOUCHER', 'CASHBACK']): return "COMMISSION"
    if op in ["FEE", "TRANSACTION FEE"]: return "FEE"
    if "FUNDING" in op: return "FUNDING_FEE"
    if any(x in op for x in ["DEPOSIT", "INITIAL BALANCE"]): return "DEPOSIT"
    if any(x in op for x in ["WITHDRAW", "SEND"]): return "WITHDRAW"
    if any(x in op for x in ["TRADE", "PNL", "BUY", "SELL", "TRANSACTION", "DRIBBLET"]): return "TRADE"
    return "OTRO"

# ==========================================================
# 📊 REGISTROS Y LIMPIEZA
# ==========================================================
def registrar_en_sistema(db, uid, exch, mercado, cat_raw, asset, monto, comi, pnl, fecha, id_ext, symbol=None):
    cursor = db.cursor()
    cat = normalizar_categoria_total(cat_raw)
    # 1. Tabla Global (Dashboard Principal)
    sql_global = """INSERT IGNORE INTO transacciones_globales 
                    (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, comision, pnl_realizado, fecha_utc)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cursor.execute(sql_global, (id_ext, uid, exch, mercado, cat, asset.upper(), monto, comi, pnl, fecha))
    # 2. Detalle de Mercado (Auditoría)
    if cat in ["TRADE", "FEE", "FUNDING_FEE"]:
        sql_detalle = """INSERT IGNORE INTO detalle_trades 
                        (id_externo_ref, user_id, exchange, tipo_mercado, symbol, fecha_utc, pnl_realizado, cantidad_ejecutada)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(sql_detalle, (id_ext, uid, exch, mercado, symbol or asset, fecha, pnl, monto))
    db.commit()

def limpiar_ordenes(db, uid, exch, tabla, ids_vivos):
    cursor = db.cursor()
    if not ids_vivos:
        cursor.execute(f"DELETE FROM {tabla} WHERE user_id = %s AND exchange = %s", (uid, exch))
    else:
        format_strings = ','.join(['%s'] * len(ids_vivos))
        cursor.execute(f"DELETE FROM {tabla} WHERE user_id = %s AND exchange = %s AND id_externo_ref NOT IN ({format_strings})", (uid, exch, *ids_vivos))
    db.commit()

# ==========================================================
# 🟨 BINANCE FULL ENDPOINTS
# ==========================================================
def binance_full_sync(db, uid, k, s):
    client = Client(k, s); cursor = db.cursor(dictionary=True)
    
    # 1. BALANCES & RADAR 4.2.3
    acc = client.get_account()
    for b in acc['balances']:
        total = float(b['free']) + float(b['locked'])
        if total > 0.000001:
            asset = b['asset'].upper()
            if asset not in ['USDT', 'USDC', 'FDUSD']: # Filtro Radar
                cursor.execute("SELECT id FROM sys_traductor_simbolos WHERE ticker_motor=%s LIMIT 1", (asset,))
                if not cursor.fetchone():
                    cursor.execute("INSERT IGNORE INTO sys_simbolos_buscados (user_id, ticker, info) VALUES (%s, %s, 'Binance API')", (uid, asset))
            cursor.execute("REPLACE INTO sys_saldos_usuarios (user_id, exchange, asset, cantidad_total, saldo_bloqueado, last_update) VALUES (%s, 'BINANCE', %s, %s, %s, NOW())", 
                           (uid, asset, total, b['locked']))

    # 2. OPEN ORDERS (SPOT & FUTURES)
    vivas = []
    # Spot
    for o in client.get_open_orders():
        id_ref = f"BN-SPOT-{o['orderId']}"; vivas.append(id_ref)
        cursor.execute("REPLACE INTO sys_open_order_spot (user_id, exchange, symbol, side, price, qty, id_externo_ref, last_update) VALUES (%s, 'BINANCE', %s, %s, %s, %s, %s, NOW())",
                       (uid, o['symbol'], o['side'], o['price'], o['origQty'], id_ref))
    # USDT-M Futures
    for f in client.futures_get_open_orders():
        id_ref = f"BN-FUT-{f['orderId']}"; vivas.append(id_ref)
        cursor.execute("REPLACE INTO sys_open_orders (user_id, exchange, symbol, side, price, qty, id_externo_ref, last_update) VALUES (%s, 'BINANCE', %s, %s, %s, %s, %s, NOW())",
                       (uid, f['symbol'], f['side'], f['price'], f['origQty'], id_ref))
    limpiar_ordenes(db, uid, "BINANCE", "sys_open_order_spot", [v for v in vivas if "SPOT" in v])
    limpiar_ordenes(db, uid, "BINANCE", "sys_open_orders", [v for v in vivas if "FUT" in v])

    # 3. HISTORIAL DE RECOMPENSAS (AIRDROPS, STAKING, DUST)
    # Airdrops/Dividends
    for div in client.get_asset_dividend_history()['rows']:
        registrar_en_sistema(db, uid, "BINANCE", "SPOT", "AIRDROP", div['asset'], div['amount'], 0, 0, format_date_sql(div['divTime']), f"BN-DIV-{div['id']}")
    # Dust (Conversiones BNB)
    dust = client.get_dust_log()
    for d_group in dust.get('userAssetDribblets', []):
        for d in d_group['userAssetDribbletDetails']:
            registrar_en_sistema(db, uid, "BINANCE", "SPOT", "DRIBBLET", "BNB", d['transferAmount'], d['serviceChargeAmount'], 0, format_date_sql(d_group['operateTime']), f"BN-DUST-{d['transId']}")
    # Depósitos/Retiros
    for dep in client.get_deposit_history():
        registrar_en_sistema(db, uid, "BINANCE", "SPOT", "DEPOSIT", dep['coin'], dep['amount'], 0, 0, format_date_sql(dep['insertTime']), f"BN-DEP-{dep['id']}")
    # Income Futures (Funding/PNL)
    try:
        for i in client.futures_income_history(limit=100):
            registrar_en_sistema(db, uid, "BINANCE", "USDT-M", i['incomeType'], i['asset'], i['income'], 0, 0, format_date_sql(i['time']), f"BN-INC-{i['tranId']}", i['symbol'])
    except: pass

# ==========================================================
# 🟦 BINGX FULL ENDPOINTS
# ==========================================================
def bingx_full_sync(db, uid, ak, as_):
    def bx_req(path, params=None):
        params = params or {}; params["timestamp"] = int(time.time() * 1000)
        qs = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), qs.encode(), hashlib.sha256).hexdigest()
        return requests.get(f"https://open-api.bingx.com{path}?{qs}&signature={sig}", headers={"X-BX-APIKEY": ak}).json()

    cursor = db.cursor(dictionary=True)
    # 1. BALANCES
    res_b = bx_req("/openApi/spot/v1/account/balance")
    if res_b.get("data"):
        for b in res_b['data']['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.000001:
                cursor.execute("REPLACE INTO sys_saldos_usuarios (user_id, exchange, asset, cantidad_total, saldo_bloqueado, last_update) VALUES (%s, 'BINGX', %s, %s, %s, NOW())", (uid, b['asset'], total, b['locked']))

    # 2. OPEN ORDERS BINGX
    vivas_bx = []
    res_o = bx_req("/openApi/swap/v2/trade/openOrders")
    if res_o.get("data"):
        for o in res_o['data']:
            id_ref = f"BX-ORD-{o['orderId']}"; vivas_bx.append(id_ref)
            cursor.execute("REPLACE INTO sys_open_orders (user_id, exchange, symbol, side, price, qty, id_externo_ref, last_update) VALUES (%s, 'BINGX', %s, %s, %s, %s, %s, NOW())", (uid, o['symbol'], o['side'], o['price'], o['quantity'], id_ref))
    limpiar_ordenes(db, uid, "BINGX", "sys_open_orders", vivas_bx)

# ==========================================================
# 🚀 MOTOR DE EJECUCIÓN
# ==========================================================
def iniciar_motor():
    print("💎 GEMA v5.2.0 - CIERRE CONTABLE TOTAL (API + CSV INTEGRADO)")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG); cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1")
            for u in cursor.fetchall():
                k, s = descifrar_dato(u['api_key'], MASTER_KEY), descifrar_dato(u['api_secret'], MASTER_KEY)
                if not k or not s: continue
                if u['broker_name'].upper() == 'BINANCE': binance_full_sync(db, u['user_id'], k, s)
                elif u['broker_name'].upper() == 'BINGX': bingx_full_sync(db, u['user_id'], k, s)
            db.close(); print(f"[{datetime.now()}] Auditoría completa.")
        except Exception as e: print(f" [CRITICAL] {e}")
        time.sleep(300)

if __name__ == "__main__": iniciar_motor()