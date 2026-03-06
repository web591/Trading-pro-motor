# ==========================================================
# 📊 operativa_TOTAL_V1.7.0.py
# UNIFICACIÓN: v5.6.3 + v1.4.6 + BINANCE + BINGX + RAW DEBUG
# ==========================================================

import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# --- CONFIGURACIÓN DE SEGURIDAD ---
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
STABLES = ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']

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
# 🎯 TRADUCTOR Y PRECIOS (Lógica v5.6.3)
# ==========================================================
def obtener_traductor_id(cursor, motor_fuente, ticker, uid=None):
    ticker = ticker.upper().strip()
    m_fuente = motor_fuente.lower().strip()
    
    cursor.execute("SELECT id FROM sys_traductor_simbolos WHERE motor_fuente=%s AND ticker_motor=%s AND is_active=1", (m_fuente, ticker))
    row = cursor.fetchone()
    if row: return row['id']
    
    limpio = limpiar_prefijos(ticker)
    cursor.execute("SELECT id FROM sys_traductor_simbolos WHERE underlying=%s AND motor_fuente=%s LIMIT 1", (limpio, m_fuente))
    row = cursor.fetchone()
    
    if not row and uid:
        cursor.execute("INSERT IGNORE INTO sys_simbolos_buscados (user_id, ticker, status, info) VALUES (%s, %s, 'pendiente', %s)", (uid, ticker, f"Fuente: {m_fuente}"))
    return row['id'] if row else None

def insertar_tx(cursor, uid, exchange, categoria, monto, asset, ts, endpoint, ref=None):
    if abs(monto) < 1e-8: return
    tid = obtener_traductor_id(cursor, exchange, asset, uid)
    
    print(f"      [INSERT] {categoria} | {asset} | {monto:+.6f} | TID: {tid if tid else 'MISSING'}")

    ref_id = f"{exchange}-{endpoint}-{ref}" if ref else f"{exchange}-{ts}-{hashlib.md5(str(monto).encode()).hexdigest()[:10]}"
    cursor.execute("""
        INSERT IGNORE INTO transacciones_globales (user_id, exchange, id_externo, categoria, monto_neto, asset, traductor_id, fecha_utc)
        VALUES (%s, %s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s/1000))
    """, (uid, exchange.upper(), ref_id, categoria, monto, asset, tid, ts))

# ==========================================================
# 🟨 BLOQUE BINANCE (Órdenes, Trades, Dust, Fees)
# ==========================================================
def procesar_binance(db, uid, ak, sec):
    cursor = db.cursor(dictionary=True)
    client = Client(ak, sec)
    print(f"\n--- 🔎 AUDITANDO BINANCE UID: {uid} ---")

    # 1. SALDOS
    acc = client.get_account()
    for b in acc['balances']:
        total = float(b['free']) + float(b['locked'])
        if total > 0.00000001:
            tid = obtener_traductor_id(cursor, "binance", b['asset'], uid)
            cursor.execute("""
                INSERT INTO sys_saldos_usuarios (user_id, broker_name, asset, traductor_id, cantidad_total) 
                VALUES (%s,'BINANCE',%s,%s,%s) ON DUPLICATE KEY UPDATE cantidad_total=VALUES(cantidad_total)
            """, (uid, b['asset'], tid, total))

    # 2. TRADES
    cursor.execute("SELECT ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente='binance' AND is_active=1")
    for row in cursor.fetchall():
        sym = row['ticker_motor']
        try:
            trades = client.get_my_trades(symbol=sym, limit=20) 
            if trades:
                print(f"   📊 DATA API BINANCE ({sym}): {trades[0]}")
                for t in trades:
                    side = 1 if t['isBuyer'] else -1
                    insertar_tx(cursor, uid, "binance", "TRADE", side * float(t['qty']), sym, t['time'], "SPOT", t['id'])
                    if float(t['commission']) > 0:
                        insertar_tx(cursor, uid, "binance", "FEE", -float(t['commission']), t['commissionAsset'], t['time'], "FEE", t['id'])
        except: continue

    # 3. DUST
    try:
        dust = client.get_dust_log()
        if dust.get("userAssetDribblets"):
            for log in dust["userAssetDribblets"]:
                for d in log["userAssetDribbletDetails"]:
                    insertar_tx(cursor, uid, "binance", "DUST", -float(d['amount']), d['fromAsset'], log['operateTime'], "DUST", d['transId'])
                    insertar_tx(cursor, uid, "binance", "DUST", float(d['transferedAmount']), "BNB", log['operateTime'], "DUST", d['transId'])
    except: pass
    db.commit()

# ==========================================================
# 🟦 BLOQUE BINGX (Saldos y Trades Spot)
# ==========================================================
def procesar_bingx(db, uid, ak, sec):
    cursor = db.cursor(dictionary=True)
    print(f"\n--- 🔎 AUDITANDO BINGX UID: {uid} ---")

    def bx_req(path, params=None):
        ts = int(time.time()*1000)
        params = params or {}
        params["timestamp"] = ts
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(sec.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        return requests.get(url, headers={"X-BX-APIKEY": ak}).json()

    # 1. SALDOS BINGX
    res_bal = bx_req("/openApi/spot/v1/account/balance")
    if res_bal.get("data"):
        for b in res_bal['data']['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.00000001:
                tid = obtener_traductor_id(cursor, "bingx", b['asset'], uid)
                cursor.execute("""
                    INSERT INTO sys_saldos_usuarios (user_id, broker_name, asset, traductor_id, cantidad_total) 
                    VALUES (%s,'BINGX',%s,%s,%s) ON DUPLICATE KEY UPDATE cantidad_total=VALUES(cantidad_total)
                """, (uid, b['asset'], tid, total))

    # 2. TRADES BINGX
    cursor.execute("SELECT ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente='bingx' AND is_active=1")
    for row in cursor.fetchall():
        sym = row['ticker_motor']
        res_tr = bx_req("/openApi/spot/v1/trade/myTrades", {"symbol": sym})
        if res_tr.get("data") and len(res_tr["data"]) > 0:
            print(f"   📊 DATA API BINGX ({sym}): {res_tr['data'][0]}")
            for t in res_tr["data"]:
                side = 1 if t['side'] == "BUY" else -1
                insertar_tx(cursor, uid, "bingx", "TRADE", side * float(t['qty']), sym, int(t['time']), "SPOT", t.get('id'))
    db.commit()

# ==========================================================
# 🚀 ORQUESTADOR PRINCIPAL
# ==========================================================
def run():
    print("🚀 MOTOR v1.7.0 INICIADO (BINANCE + BINGX)...")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1")
            for k in cursor.fetchall():
                ak = descifrar_dato(k['api_key'], MASTER_KEY)
                sec = descifrar_dato(k['api_secret'], MASTER_KEY)
                if not ak or not sec: continue
                
                broker = k['broker_name'].upper()
                if broker == "BINANCE":
                    procesar_binance(db, k['user_id'], ak, sec)
                elif broker == "BINGX":
                    procesar_bingx(db, k['user_id'], ak, sec)
            
            db.close()
            print("\n✅ Ciclo Completado. Durmiendo 60s...")
            time.sleep(60)
        except Exception as e:
            print(f"❌ ERROR CRÍTICO: {e}")
            time.sleep(10)

if __name__ == "__main__":
    run()