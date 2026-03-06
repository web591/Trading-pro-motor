# ==========================================================
# 📊 operativa_debug.py
# Motor Enterprise Unificado - Versión 1.4.5 DEBUG
# ==========================================================

import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests, json
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# ==========================================================
# 🔐 CONFIGURACIÓN Y CONSTANTES
# ==========================================================

MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
STABLES = ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']

BROKER_MAP = {
    "BINANCE": 1,
    "BINGX": 2,
    "BYBIT": 3
}

# ==========================================================
# 🔐 SEGURIDAD: DESCIFRADO DE LLAVES
# ==========================================================

def descifrar_dato(texto, master):
    try:
        if not texto: return None
        raw = base64.b64decode(texto.strip())
        partes = raw.rsplit(b":::", 1) if b":::" in raw else raw.rsplit(b"::", 1)
        data, iv = partes
        key_hash = sha256(master.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except Exception as e:
        print(f"❌ Error descifrando: {e}")
        return None

# ==========================================================
# 📊 GESTIÓN DE SINCRONIZACIÓN (sys_sync_estado)
# ==========================================================

def obtener_last_sync(cursor, user_id, broker_id, endpoint):
    try:
        cursor.execute("""
            SELECT last_timestamp FROM sys_sync_estado
            WHERE user_id=%s AND broker_id=%s AND endpoint=%s
            LIMIT 1
        """, (user_id, broker_id, endpoint))
    except mysql.connector.Error as err:
        if err.errno == 1054: 
            cursor.execute("""
                SELECT last_timestamp FROM sys_sync_estado
                WHERE user_id=%s AND endpoint=%s
                LIMIT 1
            """, (user_id, endpoint))
        else: raise err
        
    row = cursor.fetchone()
    ts = row['last_timestamp'] if row else 0
    print(f"   [SYNC] Endpoint: {endpoint} | Last TS: {ts}")
    return ts

def actualizar_sync(cursor, user_id, broker_id, endpoint, ts):
    print(f"   [SYNC UPDATE] Guardando TS {ts} para {endpoint}")
    cursor.execute("""
        INSERT INTO sys_sync_estado
        (user_id, broker_id, endpoint, last_timestamp, last_update)
        VALUES (%s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
        last_timestamp=VALUES(last_timestamp),
        last_update=NOW()
    """, (user_id, broker_id, endpoint, ts))

# ==========================================================
# 🎯 TRADUCTOR DE SÍMBOLOS Y RADAR
# ==========================================================

def limpiar_prefijos(ticker):
    ticker = ticker.upper().strip()
    for p in ["LD","STK","NCFX","NCSK","NCSI"]:
        if ticker.startswith(p):
            return ticker[len(p):]
    return ticker

def obtener_traductor_id(cursor, motor_fuente, ticker):
    ticker = ticker.upper().strip()
    cursor.execute("""
        SELECT id FROM sys_traductor_simbolos
        WHERE motor_fuente=%s AND ticker_motor=%s AND is_active=1
        LIMIT 1
    """, (motor_fuente, ticker))
    row = cursor.fetchone()
    if row: return row['id']

    limpio = limpiar_prefijos(ticker)
    cursor.execute("""
        SELECT id FROM sys_traductor_simbolos
        WHERE underlying=%s
        ORDER BY is_active DESC, fecha_creacion DESC
        LIMIT 1
    """, (limpio,))
    row = cursor.fetchone()
    return row['id'] if row else None

def disparar_radar(cursor, uid, ticker, contexto):
    print(f"   [RADAR] 📡 Ticker no encontrado: {ticker} en {contexto}")
    cursor.execute("""
        INSERT IGNORE INTO sys_simbolos_buscados
        (user_id, ticker, status, info)
        VALUES (%s,%s,'pendiente',%s)
    """, (uid, ticker, f"Detectado en {contexto}"))

# ==========================================================
# 💰 INSERCIÓN DE TRANSACCIONES Y DETALLES
# ==========================================================

def insertar_tx(cursor, uid, exchange_str, categoria, monto, asset, ts, endpoint, external_ref=None, precio=0, side=None):
    print(f"   [TX PENDING] {categoria} | {asset} | {monto} | {endpoint}")
    tid = obtener_traductor_id(cursor, exchange_str.lower(), asset)
    if not tid: disparar_radar(cursor, uid, asset, endpoint)

    if external_ref:
        base_id = f"{exchange_str}-{endpoint}-{external_ref}"
    else:
        raw = f"{uid}-{exchange_str}-{categoria}-{asset}-{ts}-{monto}-{endpoint}"
        base_id = f"{exchange_str}-{endpoint}-{hashlib.sha256(raw.encode()).hexdigest()[:20]}"

    cursor.execute("""
        INSERT IGNORE INTO transacciones_globales
        (user_id, exchange, id_externo, categoria, monto_neto, asset, traductor_id, fecha_utc)
        VALUES (%s,%s,%s,%s,%s,%s,%s,FROM_UNIXTIME(%s/1000))
    """, (uid, exchange_str, base_id, categoria, monto, asset, tid, ts))

    if categoria in ["TRADE", "FUTURES_TRADE", "MARGIN_TRADE"]:
        cursor.execute("""
            INSERT IGNORE INTO detalle_trades
            (id_externo_ref, side, precio, cantidad, asset_pair)
            VALUES (%s, %s, %s, %s, %s)
        """, (base_id, side, precio, abs(monto), asset))

# ==========================================================
# 🟨 BLOQUE BINANCE
# ==========================================================

def procesar_binance(db, uid, api_key, api_secret, b_id):
    cursor = db.cursor(dictionary=True)
    client = Client(api_key, api_secret)

    # 1. SALDOS
    try:
        acc = client.get_account()
        print(f"   [API] Respuesta balances recibida ({len(acc['balances'])} activos)")
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.00000001: 
                registrar_saldo(cursor, uid, "BINANCE", b['asset'], total, float(b['free']), float(b['locked']), 'SPOT')
    except Exception as e: print(f"❌ Error Binance Spot Balance: {e}")

    # 2. TRADES SPOT
    simbolos = obtener_simbolos_activos(cursor, "binance_spot")
    print(f"   [DEBUG] Buscando trades para {len(simbolos)} símbolos configurados...")
    for symbol in simbolos:
        last_ts = obtener_last_sync(cursor, uid, b_id, f"SPOT_TRADE_{symbol}")
        try:
            trades = client.get_my_trades(symbol=symbol, startTime=last_ts + 1, limit=1000)
            if trades:
                print(f"   [API] {len(trades)} TRADES nuevos encontrados para {symbol}")
                for t in trades:
                    print(f"      -> Trade ID {t['id']}: {t['qty']} {symbol}")
                    side = "BUY" if t['isBuyer'] else "SELL"
                    monto = float(t['qty']) if t['isBuyer'] else -float(t['qty'])
                    insertar_tx(cursor, uid, "BINANCE", "TRADE", monto, symbol, t['time'], "SPOT_TRADE", t['id'], float(t['price']), side)
                actualizar_sync(cursor, uid, b_id, f"SPOT_TRADE_{symbol}", max(tr['time'] for tr in trades))
        except Exception as e: continue

    # 3. MOVIMIENTOS CAJA
    try:
        deposits = client.get_deposit_history()
        if deposits: print(f"   [API] {len(deposits)} depósitos encontrados")
        for d in deposits:
            insertar_tx(cursor, uid, "BINANCE", "DEPOSIT", float(d['amount']), d['coin'], d['insertTime'], "DEPOSIT", d.get('txId'))
    except Exception as e: print(f"❌ Error Depósitos: {e}")

    db.commit()

# ==========================================================
# 🟦 BLOQUE BINGX
# ==========================================================

def procesar_bingx(db, uid, ak, sec, b_id):
    cursor = db.cursor(dictionary=True)
    print(f"   [BINGX] Iniciando peticiones...")
    def bx_req(path, params=None):
        params = params or {}
        params["timestamp"] = int(time.time()*1000)
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(sec.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        res = requests.get(url, headers={"X-BX-APIKEY": ak}, timeout=10).json()
        print(f"      [API BINGX] {path} -> {res.get('code', 'Error')} {res.get('msg', '')}")
        return res

    # 1. Saldos Spot
    try:
        res = bx_req("/openApi/spot/v1/account/balance")
        if res.get("data"):
            for b in res['data']['balances']:
                t = float(b['free']) + float(b['locked'])
                if t > 0: registrar_saldo(cursor, uid, "BINGX", b['asset'], t, float(b['free']), float(b['locked']))
    except: pass

    # 2. Trades Spot
    try:
        last_bx = obtener_last_sync(cursor, uid, b_id, "SPOT_ALL_BX")
        res_tr = bx_req("/openApi/spot/v1/trade/history", {"startTime": last_bx + 1})
        if res_tr.get("data") and res_tr["data"].get("trades"):
            trades = res_tr["data"]["trades"]
            print(f"   [API BINGX] {len(trades)} trades encontrados")
            for t in trades:
                side = t['side']
                monto = float(t['qty']) if side == "BUY" else -float(t['qty'])
                insertar_tx(cursor, uid, "BINGX", "TRADE", monto, t['symbol'], int(t['time']), "SPOT_TRADE", t.get('orderId'), float(t['price']), side)
            actualizar_sync(cursor, uid, b_id, "SPOT_ALL_BX", max(int(tr['time']) for tr in trades))
    except: pass

    db.commit()

# --- (Las funciones registrar_saldo, obtener_precio_usd, etc., se mantienen igual que las tuyas) ---

def registrar_saldo(cursor, uid, exchange_str, ticker, total, free, locked, subtipo='SPOT'):
    print(f"   [SALDO] {exchange_str} | {ticker} | Total: {total}")
    tid = obtener_traductor_id(cursor, exchange_str.lower(), ticker)
    if not tid: disparar_radar(cursor, uid, ticker, f"SALDO_{subtipo}")
    
    # Simulación de obtención de precio para el print
    cursor.execute("SELECT price FROM sys_precios_activos WHERE traductor_id=%s ORDER BY last_update DESC LIMIT 1", (tid,))
    p_row = cursor.fetchone()
    precio = float(p_row['price']) if p_row else 0.0

    cursor.execute("""
        INSERT INTO sys_saldos_usuarios
        (user_id, broker_name, asset, traductor_id, cantidad_total, cantidad_disponible,
         cantidad_bloqueada, valor_usd, precio_referencia, last_update)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON DUPLICATE KEY UPDATE
        traductor_id=VALUES(traductor_id), cantidad_total=VALUES(cantidad_total),
        valor_usd=VALUES(valor_usd), last_update=NOW()
    """, (uid, exchange_str, ticker, tid, total, free, locked, total*precio, precio))

def obtener_precio_usd(cursor, tid, asset):
    clean = limpiar_prefijos(asset)
    if clean in STABLES: return 1.0
    if tid:
        cursor.execute("SELECT price FROM sys_precios_activos WHERE traductor_id=%s ORDER BY last_update DESC LIMIT 1", (tid,))
        row = cursor.fetchone()
        if row and row['price'] > 0: return float(row['price'])
    return 0.0

def obtener_simbolos_activos(cursor, exchange):
    cursor.execute("SELECT ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente=%s AND is_active=1", (exchange.lower(),))
    return [r['ticker_motor'] for r in cursor.fetchall()]

def ejecutar_conciliacion(db):
    print("🔄 Iniciando Conciliación Ledger vs Snapshot...")
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT DISTINCT user_id, broker_name AS exchange, asset FROM sys_saldos_usuarios")
    registros = cursor.fetchall()
    for r in registros:
        cursor.execute("SELECT IFNULL(SUM(monto_neto),0) total FROM transacciones_globales WHERE user_id=%s AND exchange=%s AND asset=%s", (r['user_id'], r['exchange'], r['asset']))
        ledger = float(cursor.fetchone()['total'])
        cursor.execute("SELECT cantidad_total FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name=%s AND asset=%s", (r['user_id'], r['exchange'], r['asset']))
        snapshot = float(cursor.fetchone()['cantidad_total'])
        diff = snapshot - ledger
        print(f"      [CONCILIA] {r['exchange']} {r['asset']} -> Ledger: {ledger} | Snap: {snapshot} | Diff: {diff}")
        cursor.execute("INSERT INTO sys_conciliacion_saldos (user_id, exchange, asset, saldo_ledger, saldo_snapshot, diferencia, status, fecha) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())",
                       (r['user_id'], r['exchange'], r['asset'], ledger, snapshot, diff, "OK" if abs(diff) < 0.00001 else "DESCUADRE"))
    db.commit()

def run():
    print("🚀 MODO DEBUG ACTIVADO - MOTOR v1.4.5")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG, buffered=True)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1")
            for u in cursor.fetchall():
                key = descifrar_dato(u['api_key'], MASTER_KEY)
                sec = descifrar_dato(u['api_secret'], MASTER_KEY)
                if not key or not sec: 
                    print(f"⚠️ Salteando Usuario {u['user_id']} por error en llaves.")
                    continue
                
                b_name = u['broker_name'].upper()
                print(f"\n--- PROCESANDO {b_name} USUARIO {u['user_id']} ---")
                if b_name == "BINANCE":
                    procesar_binance(db, u['user_id'], key, sec, BROKER_MAP.get(b_name, 0))
                elif b_name == "BINGX":
                    procesar_bingx(db, u['user_id'], key, sec, BROKER_MAP.get(b_name, 0))

            ejecutar_conciliacion(db)
            db.close()
            print("\n✅ Ciclo finalizado. Esperando 60s...")
        except Exception as e:
            print(f"❌ [ERROR] {e}")
        time.sleep(60)

if __name__ == "__main__":
    run()