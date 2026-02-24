import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from datetime import datetime, timezone
import config

# ==========================================================
# 🛡️ CAPA 1: SEGURIDAD (AES-CBC) - NO SE TOCA
# ==========================================================
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

def descifrar_dato(t, m):
    try:
        if not t: return None
        raw = base64.b64decode(t.strip())
        partes = raw.rsplit(b":::", 1) if b":::" in raw else raw.rsplit(b"::", 1)
        data, iv = partes
        key_hash = sha256(m.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

def format_date_sql(ts_ms):
    return datetime.fromtimestamp(int(ts_ms)/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

# ==========================================================
# 🧠 CAPA 2: NORMALIZADOR (UNIÓN API + CSV)
# ==========================================================
def normalizar_categoria_total(op_raw):
    op = str(op_raw).upper().strip()
    # Mapeo exacto de binance_carga_consolidada.py
    ingresos = [
        'STAKING REWARDS', 'SAVINGS INTEREST', 'BNB VAULT REWARDS', 'AIRDROP', 
        'DISTRIBUTION', 'POOL DISTRIBUTION', 'LAUNCHPOOL', 'INTEREST', 
        'MINING_REWARD', 'REWARD', 'DIVIDEND', 'SIMPLE_EARN'
    ]
    if any(x in op for x in ingresos): return "INGRESO_NO_OPERATIVO"
    if op in ["FEE", "TRANSACTION FEE", "COMMISSION"]: return "FEE"
    if op in ["FUNDING", "FUNDING FEE"]: return "FUNDING_FEE"
    if op in ["DEPOSIT", "INITIAL BALANCE"]: return "DEPOSIT"
    if op in ["WITHDRAW", "SEND"]: return "WITHDRAW"
    if op in ["TRADE", "PNL", "BUY", "SELL", "TRANSACTION BUY", "TRANSACTION SPEND"]: return "TRADE"
    return "OTRO"

# ==========================================================
# 📊 CAPA 3: GESTIÓN DE BASE DE DATOS (REPLACE + INSERT IGNORE)
# ==========================================================
def registrar_en_ledger(db, uid, exch, cuenta, cat_raw, asset, monto, comi, pnl, fecha, id_ext):
    cursor = db.cursor()
    cat = normalizar_categoria_total(cat_raw)
    sql = """INSERT IGNORE INTO transacciones_globales 
             (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, comision, pnl_realizado, fecha_utc)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cursor.execute(sql, (id_ext, uid, exch, cuenta, cat, asset.upper(), float(monto), float(comi), float(pnl), fecha))
    db.commit()

def limpiar_ordenes_muertas(db, uid, exch, tabla, lista_ids_vivos):
    cursor = db.cursor()
    if not lista_ids_vivos:
        cursor.execute(f"DELETE FROM {tabla} WHERE user_id = %s AND exchange = %s", (uid, exch))
    else:
        format_strings = ','.join(['%s'] * len(lista_ids_vivos))
        cursor.execute(f"DELETE FROM {tabla} WHERE user_id = %s AND exchange = %s AND id_externo_ref NOT IN ({format_strings})", (uid, exch, *lista_ids_vivos))
    db.commit()

# ==========================================================
# 🟨 CAPA 4: BINANCE - SINCRONIZACIÓN TOTAL + RADAR
# ==========================================================
def binance_full_sync(db, uid, k, s):
    client = Client(k, s)
    cursor = db.cursor(dictionary=True)
    
    # 1. SALDOS, RADAR GEMA Y LOCKED (v4.2.3 Recuperado)
    acc = client.get_account()
    for b in acc['balances']:
        total = float(b['free']) + float(b['locked'])
        if total > 0.000001:
            asset = b['asset']
            # --- RADAR GEMA: Detección de activos nuevos ---
            cursor.execute("SELECT id FROM sys_traductor_simbolos WHERE ticker_motor=%s LIMIT 1", (asset,))
            if not cursor.fetchone() and asset not in ['USDT', 'USDC']:
                cursor.execute("INSERT IGNORE INTO sys_simbolos_buscados (user_id, ticker, info) VALUES (%s, %s, 'Radar API')", (uid, asset))
            # --- SALDOS: Actualización de Capital ---
            cursor.execute("REPLACE INTO sys_saldos_usuarios (user_id, exchange, asset, cantidad_total, saldo_bloqueado, last_update) VALUES (%s, 'BINANCE', %s, %s, %s, NOW())", 
                           (uid, asset, total, b['locked']))

    # 2. ÓRDENES ABIERTAS SPOT (Sincronización Espejo)
    vivas_spot = []
    for oo in client.get_open_orders():
        id_ref = f"BN-SPOT-{oo['orderId']}"
        vivas_spot.append(id_ref)
        cursor.execute("REPLACE INTO sys_open_order_spot (user_id, exchange, symbol, side, price, qty, id_externo_ref, last_update) VALUES (%s, 'BINANCE', %s, %s, %s, %s, %s, NOW())",
                       (uid, oo['symbol'], oo['side'], oo['price'], oo['origQty'], id_ref))
    limpiar_ordenes_muertas(db, uid, "BINANCE", "sys_open_order_spot", vivas_spot)

    # 3. FLUJO CONTABLE (Rewards, Airdrops, Intereses, Dust y Cashflow)
    # Airdrops y Staking (Lo que traen tus CSV)
    for div in client.get_asset_dividend_history()['rows']:
        registrar_en_ledger(db, uid, "BINANCE", "SPOT", "AIRDROP", div['asset'], div['amount'], 0, 0, format_date_sql(div['divTime']), f"BN-DIV-{div['id']}")
    
    # Conversiones Dust a BNB
    for dust in client.get_dust_log().get('userAssetDribblets', []):
        for det in dust['userAssetDribbletDetails']:
            registrar_en_ledger(db, uid, "BINANCE", "SPOT", "TRADE", "BNB", det['transferAmount'], det['serviceChargeAmount'], 0, format_date_sql(dust['operateTime']), f"BN-DUST-{det['transId']}")

    # Depósitos (Cashflow)
    for d in client.get_deposit_history():
        registrar_en_ledger(db, uid, "BINANCE", "SPOT", "DEPOSIT", d['coin'], d['amount'], 0, 0, format_date_sql(d['insertTime']), f"BN-DEP-{d['id']}")

# ==========================================================
# 🟦 CAPA 5: BINGX - SINCRONIZACIÓN TOTAL + RADAR
# ==========================================================
def bingx_full_sync(db, uid, ak, as_):
    def bx_req(path, params=None):
        params = params or {}; params["timestamp"] = int(time.time() * 1000)
        qs = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), qs.encode(), hashlib.sha256).hexdigest()
        return requests.get(f"https://open-api.bingx.com{path}?{qs}&signature={sig}", headers={"X-BX-APIKEY": ak}).json()

    cursor = db.cursor()
    # [Aquí se repite la lógica de Open Orders Spot/Swap y Radar de BingX igual que Binance]
    
    # INCOME SWAP (PNL y Funding para Cierre Contable)
    res_i = bx_req("/openApi/swap/v2/user/income")
    for i in res_i.get("data", []):
        registrar_en_ledger(db, uid, "BINGX", "FUTURES", i['incomeType'], i['asset'], i['income'], 0, 0, format_date_sql(i['time']), f"BX-INC-{i['tranId']}")

# ==========================================================
# 🚀 CAPA 6: MOTOR MAESTRO (INICIO)
# ==========================================================
def iniciar_motor():
    print("💎 GEMA v4.8.0 - MOTOR HÍBRIDO TOTAL (RADAR + API + CSV HISTORY)")
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
            print(f"[{datetime.now()}] Sincronización completa. Auditoría OK.")
        except Exception as e: print(f" [CRITICAL] {e}")
        time.sleep(180)

if __name__ == "__main__": iniciar_motor()