# ==========================================================
# 📊 operativa.py - MOTOR CONTABLE AUDITABLE TOTAL
# Basado en v5.6.3 y v1.4.6 | Full Contabilidad + Raw Prints
# ==========================================================

import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# ==========================================================
# 🔐 SEGURIDAD Y HERRAMIENTAS
# ==========================================================
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
STABLES = ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']
BROKER_MAP = {"BINANCE": 1, "BINGX": 2, "BYBIT": 3}

def descifrar_dato(texto, master):
    try:
        raw = base64.b64decode(texto.strip())
        partes = raw.rsplit(b":::", 1) if b":::" in raw else raw.rsplit(b"::", 1)
        data, iv = partes
        key_hash = sha256(master.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

def limpiar_prefijos(ticker):
    ticker = ticker.upper().strip()
    for p in ["LD", "STK", "NCFX", "NCSK", "NCSI"]:
        if ticker.startswith(p): return ticker[len(p):]
    return ticker

# ==========================================================
# 🎯 RADAR Y TRADUCTOR (v5.6.3 + v1.4.6)
# ==========================================================
def obtener_traductor_id(cursor, motor_fuente, ticker, uid=None, contexto=""):
    ticker = ticker.upper().strip()
    m_fuente = motor_fuente.lower().strip()
    
    # 1. Match Exacto (Ticker Motor)
    cursor.execute("SELECT id FROM sys_traductor_simbolos WHERE motor_fuente=%s AND ticker_motor=%s AND is_active=1 LIMIT 1", (m_fuente, ticker))
    row = cursor.fetchone()
    if row: return row['id']

    # 2. Match por Underlying
    limpio = limpiar_prefijos(ticker)
    cursor.execute("SELECT id FROM sys_traductor_simbolos WHERE underlying=%s AND motor_fuente=%s ORDER BY is_active DESC LIMIT 1", (limpio, m_fuente))
    row = cursor.fetchone()
    if row: return row['id']
    
    # 3. RADAR (v1.4.6)
    if uid:
        cursor.execute("INSERT IGNORE INTO sys_simbolos_buscados (user_id, ticker, status, info) VALUES (%s, %s, 'pendiente', %s)", (uid, ticker, f"Contexto: {contexto}"))
    return None

# ==========================================================
# 💰 MOVIMIENTOS Y PRECIOS
# ==========================================================
def insertar_tx(cursor, uid, exchange_str, categoria, monto, asset, ts, endpoint, external_ref=None):
    if monto == 0: return
    tid = obtener_traductor_id(cursor, exchange_str, asset, uid, f"TX_{endpoint}")
    
    # PRINT ESTRATÉGICO (DATA PARA TABLAS)
    print(f"      [TX_LOG] {categoria} | Asset: {asset} | Monto: {monto:+.4f} | TID: {tid}")

    # Generación ID v1.4.6
    if external_ref:
        base_id = f"{exchange_str}-{endpoint}-{external_ref}"
    else:
        raw_id = f"{uid}-{exchange_str}-{categoria}-{asset}-{ts}-{monto}"
        base_id = f"{exchange_str}-{endpoint}-{hashlib.sha256(raw_id.encode()).hexdigest()[:15]}"

    cursor.execute("""
        INSERT IGNORE INTO transacciones_globales (user_id, exchange, id_externo, categoria, monto_neto, asset, traductor_id, fecha_utc)
        VALUES (%s, %s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s/1000))
    """, (uid, exchange_str.upper(), base_id, categoria, monto, asset, tid, ts))

def registrar_saldo(cursor, uid, exchange_str, ticker, total, free, locked):
    tid = obtener_traductor_id(cursor, exchange_str, ticker, uid, "SALDO")
    
    # Obtención Precio Referencia (v5.6.3)
    cursor.execute("SELECT price FROM sys_precios_activos WHERE traductor_id=%s ORDER BY last_update DESC LIMIT 1", (tid,))
    p_row = cursor.fetchone()
    precio = float(p_row['price']) if p_row else (1.0 if ticker in STABLES else 0.0)

    cursor.execute("""
        INSERT INTO sys_saldos_usuarios (user_id, broker_name, asset, traductor_id, cantidad_total, cantidad_disponible, cantidad_bloqueada, valor_usd, precio_referencia, last_update)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE traductor_id=VALUES(traductor_id), cantidad_total=VALUES(cantidad_total), valor_usd=VALUES(valor_usd), last_update=NOW()
    """, (uid, exchange_str.upper(), ticker, tid, total, free, locked, total * precio, precio))

# ==========================================================
# 🟨 BINANCE (FULL AUDIT v1.6.5)
# ==========================================================
def procesar_binance(db, uid, api_key, api_secret, b_id):
    cursor = db.cursor(dictionary=True)
    client = Client(api_key, api_secret)
    print(f"\n{'#'*60}\n# 🏦 AUDITORÍA BINANCE RAW DATA - UID: {uid}\n{'#'*60}")

    # 1. Balances Crudos
    acc = client.get_account()
    print(f"DEBUG_API_BALANCES: {acc['balances'][:2]}... (Muestra)")
    for b in acc['balances']:
        total = float(b['free']) + float(b['locked'])
        if total > 0.00000001:
            registrar_saldo(cursor, uid, "BINANCE", b['asset'], total, float(b['free']), float(b['locked']))

    # 2. Trades Spot (Segmentado con Try/Except v1.4.6)
    cursor.execute("SELECT ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente='binance' AND is_active=1")
    for row in cursor.fetchall():
        sym = row['ticker_motor']
        last_ts = obtener_last_sync(cursor, uid, b_id, f"SPOT_{sym}")
        try:
            trades = client.get_my_trades(symbol=sym, startTime=last_ts + 1)
            if trades:
                print(f"📈 [RAW_API_TRADE] {sym}: {trades[0]}") # DATA CRUDA PARA TABLAS
                for t in trades:
                    side = 1 if t['isBuyer'] else -1
                    insertar_tx(cursor, uid, "BINANCE", "TRADE", side * float(t['qty']), sym, t['time'], "SPOT", t['id'])
                    # FEE Contable
                    if float(t['commission']) > 0:
                        insertar_tx(cursor, uid, "BINANCE", "FEE", -float(t['commission']), t['commissionAsset'], t['time'], "FEE", t['id'])
                actualizar_sync(cursor, uid, b_id, f"SPOT_{sym}", trades[-1]['time'])
        except Exception as e:
            print(f"      ⚠️ Error en símbolo {sym}: {e}")
            continue

    # 3. Dust & Earn
    try:
        dust = client.get_dust_log()
        if dust.get("userAssetDribblets"):
            for log in dust["userAssetDribblets"]:
                for d in log["userAssetDribbletDetails"]:
                    insertar_tx(cursor, uid, "BINANCE", "DUST", -float(d['amount']), d['fromAsset'], log['operateTime'], "DUST", d['transId'])
                    insertar_tx(cursor, uid, "BINANCE", "DUST", float(d['transferedAmount']), "BNB", log['operateTime'], "DUST", d['transId'])
    except: pass

    db.commit()

# ==========================================================
# 🟦 BINGX (FULL AUDIT v1.6.5)
# ==========================================================
def procesar_bingx(db, uid, ak, sec, b_id):
    cursor = db.cursor(dictionary=True)
    print(f"\n{'#'*60}\n# 🏦 AUDITORÍA BINGX RAW DATA - UID: {uid}\n{'#'*60}")

    def bx_req(path, params=None):
        ts = int(time.time()*1000)
        params = params or {}
        params["timestamp"] = ts
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(sec.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        return requests.get(url, headers={"X-BX-APIKEY": ak}).json()

    # 1. Balances BingX
    res_bal = bx_req("/openApi/spot/v1/account/balance")
    print(f"DEBUG_BINGX_BAL: {str(res_bal)[:300]}...")
    if res_bal.get("data"):
        for b in res_bal['data']['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.000001:
                registrar_saldo(cursor, uid, "BINGX", b['asset'], total, float(b['free']), float(b['locked']))

    # 2. Trades BingX
    cursor.execute("SELECT ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente='bingx' AND is_active=1")
    for row in cursor.fetchall():
        sym = row['ticker_motor']
        res_tr = bx_req("/openApi/spot/v1/trade/myTrades", {"symbol": sym})
        if res_tr.get("data"):
            print(f"📈 [RAW_BINGX_TRADE] {sym}: {res_tr['data'][0]}")
            for t in res_tr["data"]:
                side = 1 if t['side'] == "BUY" else -1
                insertar_tx(cursor, uid, "BINGX", "TRADE", side * float(t['qty']), sym, int(t['time']), "SPOT", t.get('id'))
    
    db.commit()

# ==========================================================
# 🚀 ORQUESTADOR (Sync & Run)
# ==========================================================
def obtener_last_sync(cursor, user_id, broker_id, endpoint):
    cursor.execute("SELECT last_timestamp FROM sys_sync_estado WHERE user_id=%s AND broker_id=%s AND endpoint=%s", (user_id, broker_id, endpoint))
    row = cursor.fetchone()
    return row['last_timestamp'] if row else 0

def actualizar_sync(cursor, user_id, broker_id, endpoint, ts):
    cursor.execute("""
        INSERT INTO sys_sync_estado (user_id, broker_id, endpoint, last_timestamp, last_update)
        VALUES (%s, %s, %s, %s, NOW()) 
        ON DUPLICATE KEY UPDATE last_timestamp=VALUES(last_timestamp), last_update=NOW()
    """, (user_id, broker_id, endpoint, ts))

def run():
    print("🚀 MOTOR v1.6.5 INICIADO...")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1")
            for k in cursor.fetchall():
                ak = descifrar_dato(k['api_key'], MASTER_KEY)
                sec = descifrar_dato(k['api_secret'], MASTER_KEY)
                if not ak or not sec: continue
                
                b_name = k['broker_name'].upper()
                if b_name == "BINANCE": procesar_binance(db, k['user_id'], ak, sec, BROKER_MAP["BINANCE"])
                elif b_name == "BINGX": procesar_bingx(db, k['user_id'], ak, sec, BROKER_MAP["BINGX"])
            
            db.close()
            print("\n✅ Ciclo Completado. Esperando 60s...")
            time.sleep(60)
        except Exception as e:
            print(f"❌ ERROR CRÍTICO ORQUESTADOR: {e}")
            time.sleep(10)

if __name__ == "__main__":
    run()