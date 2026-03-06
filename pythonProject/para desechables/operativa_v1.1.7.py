# ==========================================================
# operativa.py
# Motor Enterprise Unificado
# Versión 1.3 INSTITUCIONAL COMPLETA
# ==========================================================

import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# ==========================================================
# 🔐 CONFIGURACIÓN
# ==========================================================

MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
STABLES = ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']

# ==========================================================
# 🔐 SEGURIDAD
# ==========================================================

def descifrar_dato(texto, master):
    try:
        raw = base64.b64decode(texto.strip())
        partes = raw.rsplit(b":::", 1) if b":::" in raw else raw.rsplit(b"::", 1)
        data, iv = partes
        key_hash = sha256(master.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except:
        return None

# ==========================================================
# 📊 SYNC
# ==========================================================

def obtener_last_sync(cursor, user_id, broker_id, endpoint):
    # Ajustado a tu DB real: broker_id en lugar de exchange
    cursor.execute("""
        SELECT last_timestamp FROM sys_sync_estado
        WHERE user_id=%s AND broker_id=%s AND endpoint=%s
        LIMIT 1
    """, (user_id, broker_id, endpoint))
    row = cursor.fetchone()
    return row['last_timestamp'] if row else 0

def actualizar_sync(cursor, user_id, broker_id, endpoint, ts):
    # Ajustado a tu DB real: broker_id y last_update
    cursor.execute("""
        INSERT INTO sys_sync_estado
        (user_id, broker_id, endpoint, last_timestamp, last_update)
        VALUES (%s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
        last_timestamp=VALUES(last_timestamp),
        last_update=NOW()
    """, (user_id, broker_id, endpoint, ts))

# ==========================================================
# 🎯 TRADUCTOR + RADAR
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
    if row:
        return row['id']

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
# 💰 PRECIO USD
# ==========================================================

def obtener_precio_usd(cursor, tid, asset):

    clean = limpiar_prefijos(asset)

    if clean in STABLES:
        return 1.0

    if tid:
        cursor.execute("""
            SELECT price FROM sys_precios_activos
            WHERE traductor_id=%s
            ORDER BY last_update DESC
            LIMIT 1
        """, (tid,))
        row = cursor.fetchone()
        if row and row['price'] > 0:
            return float(row['price'])

    cursor.execute("""
        SELECT p.price
        FROM sys_precios_activos p
        JOIN sys_traductor_simbolos t ON p.traductor_id=t.id
        WHERE t.underlying=%s
        ORDER BY p.last_update DESC
        LIMIT 1
    """, (clean,))
    row = cursor.fetchone()

    return float(row['price']) if row else 0.0

# NUEVA LÓGICA DE BÚSQUEDA DIRIGIDA E INCREMENTAL
def obtener_simbolos_activos(cursor, exchange):
    cursor.execute("""
        SELECT ticker_motor FROM sys_traductor_simbolos 
        WHERE motor_fuente=%s AND is_active=1
    """, (exchange.lower(),))
    return [r['ticker_motor'] for r in cursor.fetchall()]

# ==========================================================
# 💰 INSERT TRANSACCIÓN INSTITUCIONAL v2 ID SEGURO
# ==========================================================

def insertar_tx(cursor, uid, exchange, categoria, monto, asset, ts, endpoint, external_ref=None):

    tid = obtener_traductor_id(cursor, exchange.lower(), asset)

    if not tid:
        disparar_radar(cursor, uid, asset, endpoint)

    # ==========================
    # 🔐 GENERADOR ID ÚNICO REAL
    # ==========================

    if external_ref:
        base_id = f"{exchange}-{endpoint}-{external_ref}"
    else:
        raw = f"{uid}-{exchange}-{categoria}-{asset}-{ts}-{monto}-{endpoint}"
        base_hash = hashlib.sha256(raw.encode()).hexdigest()[:20]
        base_id = f"{exchange}-{endpoint}-{base_hash}"

    cursor.execute("""
        INSERT IGNORE INTO transacciones_globales
        (user_id, exchange, id_externo,
         categoria, monto_neto,
         asset, traductor_id, fecha_utc)
        VALUES (%s,%s,%s,%s,%s,%s,%s,FROM_UNIXTIME(%s/1000))
    """, (
        uid, exchange, base_id,
        categoria, monto,
        asset, tid, ts
    ))

# ==========================================================
# 🏦 REGISTRO SALDOS
# ==========================================================

def registrar_saldo(cursor, uid, exchange, ticker, total, free, locked):

    tid = obtener_traductor_id(cursor, exchange.lower(), ticker)

    if not tid:
        disparar_radar(cursor, uid, ticker, "SALDO")

    precio = obtener_precio_usd(cursor, tid, ticker)
    valor_usd = total * precio

    cursor.execute("""
        INSERT INTO sys_saldos_usuarios
        (user_id, broker_name, asset, traductor_id,
         cantidad_total, cantidad_disponible,
         cantidad_bloqueada, valor_usd,
         precio_referencia, last_update)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON DUPLICATE KEY UPDATE
            traductor_id=VALUES(traductor_id),
            cantidad_total=VALUES(cantidad_total),
            cantidad_disponible=VALUES(cantidad_disponible),
            cantidad_bloqueada=VALUES(cantidad_bloqueada),
            valor_usd=VALUES(valor_usd),
            precio_referencia=VALUES(precio_referencia),
            last_update=NOW()
    """, (
        uid, exchange, ticker, tid,
        total, free, locked,
        valor_usd, precio
    ))

# ==========================================================
# 🟨 BINANCE COMPLETO INSTITUCIONAL
# ==========================================================

def procesar_binance(db, uid, api_key, api_secret):

    cursor = db.cursor(dictionary=True)
    client = Client(api_key, api_secret)

    # ========= SALDOS =========
    account = client.get_account()
    for b in account['balances']:
        total = float(b['free']) + float(b['locked'])
        if total > 0:
            registrar_saldo(cursor, uid, "BINANCE",
                            b['asset'],
                            total,
                            float(b['free']),
                            float(b['locked']))

    # ========= DEPOSITS =========
    for d in client.get_deposit_history():
        insertar_tx(cursor, uid, "BINANCE",
                    "DEPOSIT",
                    float(d['amount']),
                    d['coin'],
                    d['insertTime'],
                    "DEPOSIT",
                    external_ref=d.get('txId'))

    # ========= WITHDRAW =========
    for w in client.get_withdraw_history():
        ts = int(time.mktime(time.strptime(w['applyTime'], "%Y-%m-%d %H:%M:%S"))) * 1000
        insertar_tx(cursor, uid, "BINANCE",
                    "WITHDRAW",
                    -float(w['amount']),
                    w['coin'],
                    ts,
                    "WITHDRAW",
                    external_ref=w.get('id'))

    # ========= FUNDING =========
    for f in client.futures_income_history(incomeType="FUNDING_FEE"):
        insertar_tx(cursor, uid, "BINANCE",
                    "FUNDING",
                    float(f['income']),
                    f['asset'],
                    f['time'],
                    "FUNDING",
                    external_ref=f.get('tranId'))

    # ========= SPOT TRADES + FEE (BÚSQUEDA DIRIGIDA) =========
    # Usamos el traductor para saber qué activos buscar específicamente
    simbolos_a_buscar = obtener_simbolos_activos(cursor, "binance_spot")

    for symbol in simbolos_a_buscar:
        # Obtenemos desde dónde empezar a buscar para este usuario y símbolo
        last_ts = obtener_last_sync(cursor, uid, "BINANCE", f"SPOT_TRADE_{symbol}")

        try:
            # Pedimos solo trades nuevos (startTime = último guardado + 1 milisegundo)
            trades = client.get_my_trades(symbol=symbol, startTime=last_ts + 1)
        except:
            continue

        if trades:
            # Identificamos el tiempo de la operación más reciente para actualizar el sync
            max_ts = max(t['time'] for t in trades)
            
            for t in trades:
                ts = t['time']
                qty = float(t['qty'])
                side = 1 if t['isBuyer'] else -1
                commission = float(t['commission'])
                
                # Insertar el Trade
                insertar_tx(cursor, uid, "BINANCE", "TRADE", side * qty, 
                            symbol, ts, "SPOT_TRADE", external_ref=t['id'])

                # Insertar la Comisión si existe
                if commission > 0:
                    insertar_tx(cursor, uid, "BINANCE", "FEE", -commission, 
                                t['commissionAsset'], ts, "SPOT_FEE")
            
            # Guardamos el progreso para que en el siguiente loop no repita estos trades
            actualizar_sync(cursor, uid, "BINANCE", f"SPOT_TRADE_{symbol}", max_ts)

    # ========= FUTURES TRADES =========
    for t in client.futures_account_trades():

        ts = t['time']
        qty = float(t['qty'])
        side = 1 if t['side'] == "BUY" else -1

        insertar_tx(cursor, uid, "BINANCE",
                    "FUTURES_TRADE",
                    side * qty,
                    t['symbol'],
                    ts,
                    "FUTURES_TRADE",
                    external_ref=t['id'])

        realized = float(t['realizedPnl'])
        if realized != 0:
            insertar_tx(cursor, uid, "BINANCE",
                        "REALIZED_PNL",
                        realized,
                        t['symbol'],
                        ts,
                        "FUTURES_PNL",
                        external_ref=t.get('id'))

        fee = float(t['commission'])
        if fee > 0:
            insertar_tx(cursor, uid, "BINANCE",
                        "FEE",
                        -fee,
                        t['commissionAsset'],
                        ts,
                        "FUTURES_FEE",
                        external_ref=t.get('id'))

    # ========= INTERNAL TRANSFERS =========
    transfers = client.futures_account_transfer_history()

    for t in transfers.get("rows", []):
        ts = int(t['timestamp'])
        amount = float(t['amount'])
        direction = t['type']

        monto = amount if direction == "1" else -amount

        insertar_tx(cursor, uid, "BINANCE",
                    "TRANSFER_INTERNAL",
                    monto,
                    t['asset'],
                    ts,
                    "TRANSFER")

    # ========= AIRDROPS / INCOME =========
    try:
        incomes = client.futures_income_history(limit=200)
    except:
        incomes = []

    for inc in incomes:

        ts = inc['time']
        asset = inc['asset']
        amount = float(inc['income'])
        income_type = inc['incomeType']

        categoria = f"INCOME_{income_type}"

        if amount != 0:
            insertar_tx(cursor, uid, "BINANCE",
                        "INCOME",
                        amount,
                        asset,
                        ts,
                        categoria)

    # ========= SIMPLE EARN =========
    try:
        earn = client.get_simple_earn_flexible_position()
    except:
        earn = {}

    for pos in earn.get("rows", []):
        asset = pos['asset']
        total = float(pos['totalAmount'])

        if total > 0:
            registrar_saldo(cursor, uid,
                            "BINANCE",
                            asset,
                            total,
                            0,
                            total) 
                            
    try:
        rewards = client.get_simple_earn_flexible_rewards_history()
    except:
        rewards = {}

    for r in rewards.get("rows", []):
        insertar_tx(cursor, uid, "BINANCE",
                    "EARN_REWARD",
                    float(r['rewards']),
                    r['asset'],
                    int(r['time']),
                    "EARN_INTEREST")           

    # ========= DUST CONVERSION =========
    try:
        dust = client.get_dust_log()
    except:
        dust = {}

    for log in dust.get("userAssetDribblets", []):
        for d in log.get("userAssetDribbletDetails", []):

            asset = d['fromAsset']
            amount = float(d['amount'])
            transferred = float(d['transferedAmount'])
            ts = log['operateTime']

            insertar_tx(cursor, uid, "BINANCE",
                        "DUST_OUT",
                        -amount,
                        asset,
                        ts,
                        "DUST_CONVERSION")

            insertar_tx(cursor, uid, "BINANCE",
                        "DUST_IN",
                        transferred,
                        "BNB",
                        ts,
                        "DUST_CONVERSION")

    # ========= SPOT INCOME =========
    try:
        spot_income = client.get_asset_dividend_history()
    except:
        spot_income = {}

    for row in spot_income.get("rows", []):
        insertar_tx(cursor, uid, "BINANCE",
                    "AIR_DROP",
                    float(row['amount']),
                    row['asset'],
                    int(row['divTime']),
                    "SPOT_AIRDROP")

    # ========= OPEN ORDERS SPOT =========
    for order in client.get_open_orders():

        registrar_saldo(cursor, uid,
                        "BINANCE",
                        order['symbol'],
                        float(order['origQty']),
                        0,
                        float(order['origQty']))

    db.commit()
    print(f"[OK] Binance User {uid}")

# ==========================================================
# 🟦 BINGX COMPLETO INSTITUCIONAL
# ==========================================================

def procesar_bingx(db, uid, ak, sec):

    cursor = db.cursor(dictionary=True)

    def bx_req(path, params=None):
        if params is None:
            params = {}
        ts = int(time.time()*1000)
        params["timestamp"] = ts
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(sec.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        return requests.get(url, headers={"X-BX-APIKEY": ak}, timeout=10).json()

    try:
        res = bx_req("/openApi/spot/v1/account/balance")
        if res.get("data"):
            for b in res['data']['balances']:
                total = float(b['free']) + float(b['locked'])
                if total > 0:
                    registrar_saldo(cursor, uid, "BINGX",
                                    b['asset'],
                                    total,
                                    float(b['free']),
                                    float(b['locked']))


        res_dep = bx_req("/openApi/spot/v1/account/deposit/history", {"limit": 200})

        if res_dep.get("data"):
            for d in res_dep["data"].get("rows", []):
                insertar_tx(cursor, uid, "BINGX",
                            "DEPOSIT",
                            float(d['amount']),
                            d['asset'],
                            int(d['insertTime']),
                            "DEPOSIT",
                            external_ref=d.get('txId'))

        res_w = bx_req("/openApi/spot/v1/account/withdraw/history", {"limit": 200})

        if res_w.get("data"):
            for w in res_w["data"].get("rows", []):
                insertar_tx(cursor, uid, "BINGX",
                            "WITHDRAW",
                            -float(w['amount']),
                            w['asset'],
                            int(w['applyTime']),
                            "WITHDRAW",
                            external_ref=w.get('withdrawId'))                


        # BingX SPOT con Sync Incremental
        # Nota: Aquí puedes usar un símbolo genérico o repetir la lógica de símbolos
        last_ts_bx = obtener_last_sync(cursor, uid, "BINGX", "SPOT_TRADE_ALL")
        
        res_tr = bx_req("/openApi/spot/v1/trade/history", {
            "limit": 100, 
            "startTime": last_ts_bx + 1
        })

        if res_tr.get("data") and res_tr["data"].get("trades"):
            trades = res_tr["data"]["trades"]
            max_ts_bx = max(int(t['time']) for t in trades)
            
            for t in trades:
                ts = int(t['time'])
                side = 1 if t['side'] == "BUY" else -1
                insertar_tx(cursor, uid, "BINGX", "TRADE", side * float(t['qty']), 
                            t['symbol'], ts, "SPOT_TRADE", external_ref=t.get('orderId'))
            
            actualizar_sync(cursor, uid, "BINGX", "SPOT_TRADE_ALL", max_ts_bx)

        res_fut = bx_req("/openApi/swap/v2/trade/allOrders", {"limit": 200})

        if res_fut.get("data"):
            for t in res_fut["data"].get("orders", []):

                if t['status'] != "FILLED":
                    continue

                ts = int(t['updateTime'])
                qty = float(t['executedQty'])
                side = 1 if t['side'] == "BUY" else -1

                insertar_tx(cursor, uid, "BINGX",
                            "FUTURES_TRADE",
                            side * qty,
                            t['symbol'],
                            ts,
                            "FUTURES_TRADE",
                            external_ref=t.get('orderId'))

                realized = float(t.get("realizedPnl", 0))
                if realized != 0:
                    insertar_tx(cursor, uid, "BINGX",
                                "REALIZED_PNL",
                                realized,
                                t['symbol'],
                                ts,
                                "FUTURES_PNL")

        db.commit()
        print(f"[OK] BingX User {uid}")

    except Exception as e:
        print(f"[ERROR BINGX {uid}] {e}")

# ==========================================================
# 🏦 CONCILIACIÓN
# ==========================================================

def ejecutar_conciliacion(db):

    cursor = db.cursor(dictionary=True)

    cursor.execute("""
        SELECT DISTINCT user_id, broker_name AS exchange, asset
        FROM sys_saldos_usuarios
    """)

    for r in cursor.fetchall():

        cursor.execute("""
            SELECT IFNULL(SUM(monto_neto),0) total
            FROM transacciones_globales
            WHERE user_id=%s AND exchange=%s AND asset=%s
        """, (r['user_id'], r['exchange'], r['asset']))

        ledger = float(cursor.fetchone()['total'])

        cursor.execute("""
            SELECT cantidad_total FROM sys_saldos_usuarios
            WHERE user_id=%s AND broker_name=%s AND asset=%s
        """, (r['user_id'], r['exchange'], r['asset']))

        snapshot = float(cursor.fetchone()['cantidad_total'])

        diff = snapshot - ledger
        estado = "OK" if abs(diff) < 0.0001 else "DESCUADRE"

        cursor.execute("""
            INSERT INTO sys_conciliacion_saldos
            (user_id, exchange, asset,
             ledger_total, snapshot_total,
             diferencia, estado, fecha_revision)
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            r['user_id'], r['exchange'], r['asset'],
            ledger, snapshot, diff, estado
        ))

    db.commit()

# ==========================================================
# 🚀 ORQUESTADOR
# ==========================================================

def run():

    print("🚀 MOTOR ENTERPRISE v1.1.7 INICIADO")

    while True:

        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)

            cursor.execute("""
                SELECT user_id, api_key, api_secret, broker_name
                FROM api_keys
                WHERE status=1
            """)

            for u in cursor.fetchall():

                key = descifrar_dato(u['api_key'], MASTER_KEY)
                sec = descifrar_dato(u['api_secret'], MASTER_KEY)

                if not key or not sec:
                    continue

                broker = u['broker_name'].upper()

                if broker == "BINANCE":
                    procesar_binance(db, u['user_id'], key, sec)

                elif broker == "BINGX":
                    procesar_bingx(db, u['user_id'], key, sec)

            ejecutar_conciliacion(db)
            db.close()

        except Exception as e:
            print(f"[CRITICAL] {e}")

        time.sleep(60)

if __name__ == "__main__":
    run()