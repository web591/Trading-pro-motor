# ==========================================================
# 📊 operativa.py
# Motor Enterprise Unificado - Versión 1.4 INSTITUCIONAL
# ESTRUCTURA COMPLETA: SPOT, FUTURES, MARGIN, EARN & SYNC
# ==========================================================

import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# ==========================================================
# 🔐 CONFIGURACIÓN Y CONSTANTES
# ==========================================================

MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
STABLES = ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']

# Mapeo según maestros_brokers
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
        return None

# ==========================================================
# 📊 GESTIÓN DE SINCRONIZACIÓN (sys_sync_estado)
# ==========================================================

def obtener_last_sync(cursor, user_id, broker_id, endpoint):
    try:
        # Intento A: Usando broker_id (numérico)
        cursor.execute("""
            SELECT last_timestamp FROM sys_sync_estado
            WHERE user_id=%s AND broker_id=%s AND endpoint=%s
            LIMIT 1
        """, (user_id, broker_id, endpoint))
    except mysql.connector.Error as err:
        if err.errno == 1054: 
            # Intento B: Si falla, es que la tabla usa 'broker' (texto) o 'exchange'
            cursor.execute("""
                SELECT last_timestamp FROM sys_sync_estado
                WHERE user_id=%s AND endpoint=%s
                LIMIT 1
            """, (user_id, endpoint))
        else: raise err
        
    row = cursor.fetchone()
    return row['last_timestamp'] if row else 0

def actualizar_sync(cursor, user_id, broker_id, endpoint, ts):
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
    # Intento 1: Match Exacto
    cursor.execute("""
        SELECT id FROM sys_traductor_simbolos
        WHERE motor_fuente=%s AND ticker_motor=%s AND is_active=1
        LIMIT 1
    """, (motor_fuente, ticker))
    row = cursor.fetchone()
    if row: return row['id']

    # Intento 2: Match por Underlying
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
    cursor.execute("""
        INSERT IGNORE INTO sys_simbolos_buscados
        (user_id, ticker, status, info)
        VALUES (%s,%s,'pendiente',%s)
    """, (uid, ticker, f"Detectado en {contexto}"))

# ==========================================================
# 💰 VALORACIÓN Y PRECIOS
# ==========================================================

def obtener_precio_usd(cursor, tid, asset):
    clean = limpiar_prefijos(asset)
    if clean in STABLES: return 1.0
    if tid:
        cursor.execute("""
            SELECT price FROM sys_precios_activos
            WHERE traductor_id=%s
            ORDER BY last_update DESC LIMIT 1
        """, (tid,))
        row = cursor.fetchone()
        if row and row['price'] > 0: return float(row['price'])
    
    cursor.execute("""
        SELECT p.price FROM sys_precios_activos p
        JOIN sys_traductor_simbolos t ON p.traductor_id=t.id
        WHERE t.underlying=%s
        ORDER BY p.last_update DESC LIMIT 1
    """, (clean,))
    row = cursor.fetchone()
    return float(row['price']) if row else 0.0

def obtener_simbolos_activos(cursor, exchange):
    cursor.execute("""
        SELECT ticker_motor FROM sys_traductor_simbolos 
        WHERE motor_fuente=%s AND is_active=1
    """, (exchange.lower(),))
    return [r['ticker_motor'] for r in cursor.fetchall()]

# ==========================================================
# 💰 INSERCIÓN DE TRANSACCIONES Y DETALLES
# ==========================================================

def insertar_tx(cursor, uid, exchange_str, categoria, monto, asset, ts, endpoint, external_ref=None, precio=0, side=None):
    tid = obtener_traductor_id(cursor, exchange_str.lower(), asset)
    if not tid: disparar_radar(cursor, uid, asset, endpoint)

    # Generar ID Único
    if external_ref:
        base_id = f"{exchange_str}-{endpoint}-{external_ref}"
    else:
        raw = f"{uid}-{exchange_str}-{categoria}-{asset}-{ts}-{monto}-{endpoint}"
        base_id = f"{exchange_str}-{endpoint}-{hashlib.sha256(raw.encode()).hexdigest()[:20]}"

    # Transacciones Globales
    cursor.execute("""
        INSERT IGNORE INTO transacciones_globales
        (user_id, exchange, id_externo, categoria, monto_neto, asset, traductor_id, fecha_utc)
        VALUES (%s,%s,%s,%s,%s,%s,%s,FROM_UNIXTIME(%s/1000))
    """, (uid, exchange_str, base_id, categoria, monto, asset, tid, ts))

    # Detalle de Trades (para cálculo de PnL y Promedios)
    if categoria in ["TRADE", "FUTURES_TRADE", "MARGIN_TRADE"]:
        cursor.execute("""
            INSERT IGNORE INTO detalle_trades
            (id_externo_ref, side, precio, cantidad, asset_pair)
            VALUES (%s, %s, %s, %s, %s)
        """, (base_id, side, precio, abs(monto), asset))

# ==========================================================
# 🏦 SALDOS Y ÓRDENES (SEPARACIÓN SPOT/FUTUROS)
# ==========================================================

def registrar_saldo(cursor, uid, exchange_str, ticker, total, free, locked, subtipo='SPOT'):
    tid = obtener_traductor_id(cursor, exchange_str.lower(), ticker)
    if not tid: disparar_radar(cursor, uid, ticker, f"SALDO_{subtipo}")
    
    precio = obtener_precio_usd(cursor, tid, ticker)
    # Nota: Tu tabla sys_saldos_usuarios unifica pero diferencia por asset/broker
    cursor.execute("""
        INSERT INTO sys_saldos_usuarios
        (user_id, broker_name, asset, traductor_id, cantidad_total, cantidad_disponible,
         cantidad_bloqueada, valor_usd, precio_referencia, last_update)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON DUPLICATE KEY UPDATE
        traductor_id=VALUES(traductor_id), cantidad_total=VALUES(cantidad_total),
        cantidad_disponible=VALUES(cantidad_disponible), cantidad_bloqueada=VALUES(cantidad_bloqueada),
        valor_usd=VALUES(valor_usd), precio_referencia=VALUES(precio_referencia), last_update=NOW()
    """, (uid, exchange_str, ticker, tid, total, free, locked, total*precio, precio))

def registrar_orden_abierta(cursor, uid, exchange_str, symbol, order_id, side, price, qty, type_order, ts, mercado='SPOT'):
    tid = obtener_traductor_id(cursor, exchange_str.lower(), symbol)
    if not tid: tid = 0
    
    # Ajustado a tus columnas reales: id_order_ext, broker_name, etc.
    sql = """
        INSERT INTO sys_open_orders
        (id_order_ext, user_id, broker_name, traductor_id, symbol, side, type, price, qty, fecha_utc, estado, last_seen)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s/1000), 'ABIERTA', NOW())
        ON DUPLICATE KEY UPDATE
        price=VALUES(price), qty=VALUES(qty), last_seen=NOW()
    """
    try:
        cursor.execute(sql, (str(order_id), uid, exchange_str.upper(), tid, symbol, side, type_order, price, qty, ts))
    except Exception as e:
        print(f"⚠️ Error en sys_open_orders: {e}")

# ==========================================================
# 🟨 BLOQUE BINANCE: INTEGRACIÓN TOTAL
# ==========================================================

def procesar_binance(db, uid, api_key, api_secret, b_id):
    cursor = db.cursor(dictionary=True)
    client = Client(api_key, api_secret)

    # --- 1. LIMPIEZA DE ÓRDENES (REVISADO EXHAUSTIVAMENTE) ---
    try:
        # Intentamos borrar por user_id solamente (lo más seguro para no romper)
        # Esto limpia las órdenes abiertas para volver a bajarlas de la API
        cursor.execute("DELETE FROM sys_open_orders WHERE user_id=%s", (uid,))
    except mysql.connector.Error as err:
        print(f"⚠️ Nota: No se pudo limpiar sys_open_orders (esto no es crítico): {err}")

    # --- 2. SALDOS SPOT Y MARGIN ---
    try:
        acc = client.get_account()
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.00000001: 
                registrar_saldo(cursor, uid, "BINANCE", b['asset'], total, float(b['free']), float(b['locked']), 'SPOT')
        
        # Órdenes Spot Abiertas
        for o in client.get_open_orders():
            registrar_orden_abierta(cursor, uid, "BINANCE", o['symbol'], o['orderId'], o['side'], o['price'], o['origQty'], o['type'], o['time'], 'SPOT')
    except Exception as e: print(f"Error Binance Spot Balance: {e}")

    # --- 3. SALDOS FUTUROS (UM) ---
    try:
        fut_acc = client.futures_account()
        for p in fut_acc['assets']:
            total = float(p['walletBalance'])
            if total > 0:
                registrar_saldo(cursor, uid, "BINANCE", p['asset'], total, float(p['availableBalance']), float(p['maintMargin']), 'FUTURES')
    except: pass

    # --- 4. TRADES SPOT (INCREMENTAL) ---
    simbolos = obtener_simbolos_activos(cursor, "binance_spot")
    for symbol in simbolos:
        # Importante: Esto limpia cualquier residuo de consultas anteriores
        cursor.fetchall() 
        
        last_ts = obtener_last_sync(cursor, uid, b_id, f"SPOT_TRADE_{symbol}")
        try:
            trades = client.get_my_trades(symbol=symbol, startTime=last_ts + 1, limit=1000)
            if trades:
                for t in trades:
                    side = "BUY" if t['isBuyer'] else "SELL"
                    monto = float(t['qty']) if t['isBuyer'] else -float(t['qty'])
                    insertar_tx(cursor, uid, "BINANCE", "TRADE", monto, symbol, t['time'], "SPOT_TRADE", t['id'], float(t['price']), side)
                
                # Actualizar sync al final del bloque de trades
                actualizar_sync(cursor, uid, b_id, f"SPOT_TRADE_{symbol}", max(tr['time'] for tr in trades))
        except Exception as e:
            continue

    # --- 5. MOVIMIENTOS DE CAJA (DEPOSITS / WITHDRAWS) ---
    try:
        for d in client.get_deposit_history():
            insertar_tx(cursor, uid, "BINANCE", "DEPOSIT", float(d['amount']), d['coin'], d['insertTime'], "DEPOSIT", d.get('txId'))
        for w in client.get_withdraw_history():
            ts_w = int(time.mktime(time.strptime(w['applyTime'], "%Y-%m-%d %H:%M:%S"))) * 1000
            insertar_tx(cursor, uid, "BINANCE", "WITHDRAW", -float(w['amount']), w['coin'], ts_w, "WITHDRAW", w.get('id'))
    except: pass

    # --- 6. FUTUROS: TRADES E INGRESOS (FUNDING, PNL, ETC) ---
    try:
        last_f = obtener_last_sync(cursor, uid, b_id, "FUTURES_DATA")
        f_trades = client.futures_account_trades(startTime=last_f + 1)
        if f_trades:
            for ft in f_trades:
                side = ft['side']
                monto = float(ft['qty']) if side == "BUY" else -float(ft['qty'])
                insertar_tx(cursor, uid, "BINANCE", "FUTURES_TRADE", monto, ft['symbol'], ft['time'], "FUTURES_TRADE", ft['id'], float(ft['price']), side)
                # PnL Realizado
                if float(ft['realizedPnl']) != 0:
                    insertar_tx(cursor, uid, "BINANCE", "REALIZED_PNL", float(ft['realizedPnl']), ft['symbol'], ft['time'], "FUTURES_PNL", f"PNL_{ft['id']}")
            actualizar_sync(cursor, uid, b_id, "FUTURES_DATA", max(x['time'] for x in f_trades))
    except: pass

    db.commit()

# ==========================================================
# 🟦 BLOQUE BINGX: INTEGRACIÓN TOTAL
# ==========================================================

def procesar_bingx(db, uid, ak, sec, b_id):
    cursor = db.cursor(dictionary=True)
    def bx_req(path, params=None):
        params = params or {}
        params["timestamp"] = int(time.time()*1000)
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(sec.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        return requests.get(url, headers={"X-BX-APIKEY": ak}, timeout=10).json()

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
            for t in trades:
                side = t['side']
                monto = float(t['qty']) if side == "BUY" else -float(t['qty'])
                insertar_tx(cursor, uid, "BINGX", "TRADE", monto, t['symbol'], int(t['time']), "SPOT_TRADE", t.get('orderId'), float(t['price']), side)
            actualizar_sync(cursor, uid, b_id, "SPOT_ALL_BX", max(int(tr['time']) for tr in trades))
    except: pass

    db.commit()

# ==========================================================
# 🚀 ORQUESTADOR Y CONCILIACIÓN FINAL
# ==========================================================
def ejecutar_conciliacion(db):
    cursor = db.cursor(dictionary=True)
    # 1. Obtenemos los activos que tienen saldo actualmente
    cursor.execute("SELECT DISTINCT user_id, broker_name AS exchange, asset FROM sys_saldos_usuarios")
    registros = cursor.fetchall()
    
    for r in registros:
        # 2. Suma de transacciones (Ledger)
        cursor.execute("""
            SELECT IFNULL(SUM(monto_neto),0) total 
            FROM transacciones_globales 
            WHERE user_id=%s AND exchange=%s AND asset=%s
        """, (r['user_id'], r['exchange'], r['asset']))
        ledger = float(cursor.fetchone()['total'])

        # 3. Saldo Actual (Snapshot)
        cursor.execute("""
            SELECT cantidad_total FROM sys_saldos_usuarios 
            WHERE user_id=%s AND broker_name=%s AND asset=%s
        """, (r['user_id'], r['exchange'], r['asset']))
        row_snap = cursor.fetchone()
        snapshot = float(row_snap['cantidad_total']) if row_snap else 0.0

        diff = snapshot - ledger
        
        # 4. Inserción con tus columnas reales: saldo_ledger, saldo_snapshot, status, fecha
        sql_ins = """
            INSERT INTO sys_conciliacion_saldos 
            (user_id, exchange, asset, saldo_ledger, saldo_snapshot, diferencia, status, fecha)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        """
        estado_val = "OK" if abs(diff) < 0.00001 else "DESCUADRE"
        
        cursor.execute(sql_ins, (
            r['user_id'], r['exchange'], r['asset'], 
            ledger, snapshot, diff, estado_val
        ))
    
    db.commit()


def run():
    print("🚀 MOTOR INSTITUCIONAL v1.4.5 - FULL ECOSISTEMA")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG, buffered=True)
            cursor = db.cursor(dictionary=True)
            
            cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1")
            for u in cursor.fetchall():
                key = descifrar_dato(u['api_key'], MASTER_KEY)
                sec = descifrar_dato(u['api_secret'], MASTER_KEY)
                if not key or not sec: continue
                
                b_name = u['broker_name'].upper()
                b_id = BROKER_MAP.get(b_name, 0)

                print(f"🔄 Procesando {b_name} para Usuario {u['user_id']}...")
                if b_name == "BINANCE":
                    procesar_binance(db, u['user_id'], key, sec, b_id)
                elif b_name == "BINGX":
                    procesar_bingx(db, u['user_id'], key, sec, b_id)

            ejecutar_conciliacion(db)
            db.close()
            print("✅ Ciclo finalizado con éxito. Esperando próximo intervalo...")
        except Exception as e:
            print(f"[CRITICAL ERROR] {e}")
        
        time.sleep(60)

if __name__ == "__main__":
    run()