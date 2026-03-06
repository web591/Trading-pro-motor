# ==========================================================
# operativa.py
# Motor Enterprise Unificado
# Versión 1.0
# Basado en Motor Saldos v5.6.3
# ==========================================================

import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# ==========================================================
# 🔐 CONFIGURACIÓN GLOBAL
# ==========================================================

MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

STABLES = ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']

# ==========================================================
# 🔐 SEGURIDAD
# ==========================================================

def descifrar_dato(texto, master):
    try:
        if not texto:
            return None
        raw = base64.b64decode(texto.strip())
        partes = raw.rsplit(b":::", 1) if b":::" in raw else raw.rsplit(b"::", 1)
        if len(partes) != 2:
            return None
        data, iv = partes
        key_hash = sha256(master.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except:
        return None

# ==========================================================
# 🎯 TRADUCTOR
# ==========================================================

def limpiar_prefijos(ticker):
    ticker = ticker.upper().strip()
    if ticker.startswith("LD") and len(ticker) > 2:
        return ticker[2:]
    if ticker.startswith("STK") and len(ticker) > 3:
        return ticker[3:]
    return ticker

def obtener_traductor_id(cursor, motor_fuente, ticker):
    ticker = ticker.upper().strip()

    # 1. Exacto
    sql = """
        SELECT id FROM sys_traductor_simbolos 
        WHERE motor_fuente=%s AND ticker_motor=%s AND is_active=1 
        LIMIT 1
    """
    cursor.execute(sql, (motor_fuente, ticker))
    row = cursor.fetchone()
    if row:
        return row['id']

    # 2. Limpieza prefijos
    ticker_limpio = limpiar_prefijos(ticker)

    # 3. Underlying mismo motor
    sql = """
        SELECT id FROM sys_traductor_simbolos 
        WHERE underlying=%s AND motor_fuente=%s AND is_active=1 
        LIMIT 1
    """
    cursor.execute(sql, (ticker_limpio, motor_fuente))
    row = cursor.fetchone()
    if row:
        return row['id']

    # 4. Global activo
    sql = """
        SELECT id FROM sys_traductor_simbolos
        WHERE underlying=%s
        ORDER BY is_active DESC, fecha_creacion DESC
        LIMIT 1
    """
    cursor.execute(sql, (ticker_limpio,))
    row = cursor.fetchone()

    return row['id'] if row else None

def disparar_radar(cursor, uid, ticker, contexto):
    sql = """
        INSERT IGNORE INTO sys_simbolos_buscados
        (user_id, ticker, status, info)
        VALUES (%s,%s,'pendiente',%s)
    """
    cursor.execute(sql, (uid, ticker, f"Detectado en {contexto}"))

# ==========================================================
# 💰 PRECIO
# ==========================================================

def obtener_precio_usd(cursor, tid, asset_name):
    asset_name = asset_name.upper()
    clean = limpiar_prefijos(asset_name)

    if clean in STABLES:
        return 1.0

    try:
        if tid:
            sql = """
                SELECT price FROM sys_precios_activos
                WHERE traductor_id=%s
                ORDER BY last_update DESC
                LIMIT 1
            """
            cursor.execute(sql, (tid,))
            row = cursor.fetchone()
            if row and row['price'] > 0:
                return float(row['price'])

        sql_fb = """
            SELECT p.price
            FROM sys_precios_activos p
            JOIN sys_traductor_simbolos t ON p.traductor_id = t.id
            WHERE t.underlying=%s AND t.is_active=1
            ORDER BY p.last_update DESC
            LIMIT 1
        """
        cursor.execute(sql_fb, (clean,))
        row_fb = cursor.fetchone()
        if row_fb:
            return float(row_fb['price'])

    except:
        pass

    return 0.0

# ==========================================================
# 🏦 REGISTRO SALDOS
# ==========================================================

def registrar_saldo(cursor, uid, tid, total, locked, asset, broker, tipo_cta):

    disponible = total - locked
    precio = obtener_precio_usd(cursor, tid, asset)
    valor_usd = total * precio
    margen = locked if tipo_cta == "FUTURES" else 0.0

    sql = """
        INSERT INTO sys_saldos_usuarios
        (user_id, broker_name, asset, traductor_id,
         cantidad_total, cantidad_disponible,
         cantidad_bloqueada, margen_usado,
         valor_usd, precio_referencia,
         tipo_cuenta, tipo_lista, last_update)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'ACTIVO',NOW())
        ON DUPLICATE KEY UPDATE
            traductor_id=VALUES(traductor_id),
            cantidad_total=VALUES(cantidad_total),
            cantidad_disponible=VALUES(cantidad_disponible),
            cantidad_bloqueada=VALUES(cantidad_bloqueada),
            margen_usado=VALUES(margen_usado),
            valor_usd=VALUES(valor_usd),
            precio_referencia=VALUES(precio_referencia),
            tipo_cuenta=VALUES(tipo_cuenta),
            last_update=NOW()
    """

    cursor.execute(sql, (
        uid, broker, asset, tid,
        total, disponible,
        locked, margen,
        valor_usd, precio,
        tipo_cta
    ))

# ==========================================================
# 🟨 BINANCE
# ==========================================================

def procesar_binance(db, uid, api_key, api_secret):

    try:
        client = Client(api_key, api_secret)
        cursor = db.cursor(dictionary=True)

        account = client.get_account()

        for b in account['balances']:
            total = float(b['free']) + float(b['locked'])
            if total <= 0.000001:
                continue

            ticker = b['asset']
            tid = obtener_traductor_id(cursor, "binance_spot", ticker)

            registrar_saldo(cursor, uid, tid, total,
                            float(b['locked']),
                            ticker, "BINANCE", "SPOT")

            if not tid:
                disparar_radar(cursor, uid, ticker, "BINANCE SPOT")

        print(f"[OK] Binance User {uid}")

    except Exception as e:
        print(f"[ERROR BINANCE {uid}] {e}")


        # ==================================================
        # 🧾 SPOT TRADES (FASE CONTABLE)
        # ==================================================

        endpoint = "BINANCE_SPOT_TRADES"
        last_ts = obtener_last_sync(cursor, uid, "BINANCE", endpoint)

        trades = client.get_my_trades(
            symbol=None,
            limit=1000,
            recvWindow=60000
        )

        max_ts = last_ts

        for t in trades:

            if t['time'] <= last_ts:
                continue

            trade_id = f"BN-SPOT-{t['orderId']}-{t['id']}"
            symbol = t['symbol']
            side = t['isBuyer']
            qty = float(t['qty'])
            price = float(t['price'])
            commission = float(t['commission'])
            commission_asset = t['commissionAsset']
            ts_trade = t['time']

            lado = "BUY" if side else "SELL"

            # ===============================
            # INSERT detalle_trades
            # ===============================

            sql_det = """
                INSERT IGNORE INTO detalle_trades
                (user_id, exchange, tipo_producto, exchange_fuente,
                 tipo_mercado, id_externo_ref, fecha_utc,
                 symbol, lado, precio_ejecucion,
                 cantidad_ejecutada, pnl_realizado,
                 is_maker, broker, trade_id_externo)
                VALUES (%s,'BINANCE','SPOT','API',
                        'SPOT',%s,FROM_UNIXTIME(%s/1000),
                        %s,%s,%s,%s,0,
                        %s,'BINANCE',%s)
            """

            cursor.execute(sql_det, (
                uid, trade_id, ts_trade,
                symbol, lado, price, qty,
                int(t['isMaker']), t['id']
            ))

            # ===============================
            # INSERT TRADE (ledger)
            # ===============================

            monto = qty * price

            sql_tx_trade = """
                INSERT IGNORE INTO transacciones_globales
                (user_id, exchange, id_externo,
                 categoria, monto_neto,
                 asset, fecha_utc)
                VALUES (%s,'BINANCE',%s,
                        'TRADE',%s,
                        %s,FROM_UNIXTIME(%s/1000))
            """

            cursor.execute(sql_tx_trade, (
                uid, trade_id,
                monto,
                symbol,
                ts_trade
            ))

            # ===============================
            # INSERT FEE
            # ===============================

            fee_id = f"{trade_id}-FEE"

            sql_tx_fee = """
                INSERT IGNORE INTO transacciones_globales
                (user_id, exchange, id_externo,
                 categoria, monto_neto,
                 asset, fecha_utc)
                VALUES (%s,'BINANCE',%s,
                        'FEE',%s,
                        %s,FROM_UNIXTIME(%s/1000))
            """

            cursor.execute(sql_tx_fee, (
                uid, fee_id,
                -commission,
                commission_asset,
                ts_trade
            ))

            if ts_trade > max_ts:
                max_ts = ts_trade

        if max_ts > last_ts:
            actualizar_sync(cursor, uid, "BINANCE", endpoint, max_ts)


# ==================================================
# 💰 DEPOSITS
# ==================================================

endpoint = "BINANCE_DEPOSITS"
last_ts = obtener_last_sync(cursor, uid, "BINANCE", endpoint)

deposits = client.get_deposit_history()

max_ts = last_ts

for d in deposits:

    ts_dep = d['insertTime']
    if ts_dep <= last_ts:
        continue

    tx_id = f"BN-DEP-{d['txId']}"
    amount = float(d['amount'])
    asset = d['coin']

    sql = """
        INSERT IGNORE INTO transacciones_globales
        (user_id, exchange, id_externo,
         categoria, monto_neto,
         asset, fecha_utc)
        VALUES (%s,'BINANCE',%s,
                'DEPOSIT',%s,
                %s,FROM_UNIXTIME(%s/1000))
    """

    cursor.execute(sql, (
        uid, tx_id,
        amount,
        asset,
        ts_dep
    ))

    if ts_dep > max_ts:
        max_ts = ts_dep

if max_ts > last_ts:
    actualizar_sync(cursor, uid, "BINANCE", endpoint, max_ts)


# ==================================================
# 💸 WITHDRAWALS
# ==================================================

endpoint = "BINANCE_WITHDRAWALS"
last_ts = obtener_last_sync(cursor, uid, "BINANCE", endpoint)

withdraws = client.get_withdraw_history()

max_ts = last_ts

for w in withdraws:

    ts_w = w['applyTime']
    ts_w = int(time.mktime(time.strptime(ts_w, "%Y-%m-%d %H:%M:%S"))) * 1000

    if ts_w <= last_ts:
        continue

    tx_id = f"BN-WITH-{w['id']}"
    amount = float(w['amount'])
    asset = w['coin']

    sql = """
        INSERT IGNORE INTO transacciones_globales
        (user_id, exchange, id_externo,
         categoria, monto_neto,
         asset, fecha_utc)
        VALUES (%s,'BINANCE',%s,
                'WITHDRAW',%s,
                %s,FROM_UNIXTIME(%s/1000))
    """

    cursor.execute(sql, (
        uid, tx_id,
        -amount,
        asset,
        ts_w
    ))

    if ts_w > max_ts:
        max_ts = ts_w

if max_ts > last_ts:
    actualizar_sync(cursor, uid, "BINANCE", endpoint, max_ts)


# ==================================================
# 🟣 FUNDING (USDT-M)
# ==================================================

endpoint = "BINANCE_FUNDING"
last_ts = obtener_last_sync(cursor, uid, "BINANCE", endpoint)

fundings = client.futures_income_history(incomeType="FUNDING_FEE")

max_ts = last_ts

for f in fundings:

    ts_f = f['time']
    if ts_f <= last_ts:
        continue

    tx_id = f"BN-FUND-{f['tranId']}"
    amount = float(f['income'])
    asset = f['asset']

    sql = """
        INSERT IGNORE INTO transacciones_globales
        (user_id, exchange, id_externo,
         categoria, monto_neto,
         asset, fecha_utc)
        VALUES (%s,'BINANCE',%s,
                'FUNDING',%s,
                %s,FROM_UNIXTIME(%s/1000))
    """

    cursor.execute(sql, (
        uid, tx_id,
        amount,
        asset,
        ts_f
    ))

    if ts_f > max_ts:
        max_ts = ts_f

if max_ts > last_ts:
    actualizar_sync(cursor, uid, "BINANCE", endpoint, max_ts)


# ==================================================
# 📊 FUTURES TRADES
# ==================================================

endpoint = "BINANCE_FUTURES_TRADES"
last_ts = obtener_last_sync(cursor, uid, "BINANCE", endpoint)

f_trades = client.futures_account_trades()

max_ts = last_ts

for t in f_trades:

    ts_trade = t['time']
    if ts_trade <= last_ts:
        continue

    trade_id = f"BN-FUT-{t['orderId']}-{t['id']}"
    symbol = t['symbol']
    qty = float(t['qty'])
    price = float(t['price'])
    side = t['side']

    monto = qty * price

    sql = """
        INSERT IGNORE INTO transacciones_globales
        (user_id, exchange, id_externo,
         categoria, monto_neto,
         asset, fecha_utc)
        VALUES (%s,'BINANCE',%s,
                'TRADE',%s,
                %s,FROM_UNIXTIME(%s/1000))
    """

    cursor.execute(sql, (
        uid, trade_id,
        monto,
        symbol,
        ts_trade
    ))

    if ts_trade > max_ts:
        max_ts = ts_trade

if max_ts > last_ts:
    actualizar_sync(cursor, uid, "BINANCE", endpoint, max_ts)

# ==================================================
# 🔄 BINANCE INTERNAL TRANSFERS
# ==================================================

endpoint = "BINANCE_TRANSFERS"
last_ts = obtener_last_sync(cursor, uid, "BINANCE", endpoint)
max_ts = last_ts

try:
    transfers = client.futures_account_transfer_history()

    for t in transfers.get("rows", []):

        ts_tr = int(t["timestamp"])
        if ts_tr <= last_ts:
            continue

        tx_id = f"BN-TR-{t['tranId']}"
        amount = float(t["amount"])
        asset = t["asset"]

        # Si es transferencia hacia futures → negativo en spot
        direction = t.get("type", "")

        monto = amount if direction == "1" else -amount

        sql = """
            INSERT IGNORE INTO transacciones_globales
            (user_id, exchange, id_externo,
             categoria, monto_neto,
             asset, fecha_utc)
            VALUES (%s,'BINANCE',%s,
                    'TRANSFER_INTERNAL',%s,
                    %s,FROM_UNIXTIME(%s/1000))
        """

        cursor.execute(sql, (
            uid, tx_id,
            monto,
            asset,
            ts_tr
        ))

        if ts_tr > max_ts:
            max_ts = ts_tr

    if max_ts > last_ts:
        actualizar_sync(cursor, uid, "BINANCE", endpoint, max_ts)

    print(f"[OK] Binance Transfers User {uid}")

except Exception as e:
    print(f"[ERROR BINANCE TRANSFERS {uid}] {e}")

# ==========================================================
# 🟦 BINGX
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

        return requests.get(
            url,
            headers={"X-BX-APIKEY": ak},
            timeout=10
        ).json()

    try:
        res = bx_req("/openApi/spot/v1/account/balance")
        if res.get("data"):
            for b in res['data']['balances']:
                total = float(b['free']) + float(b['locked'])
                if total <= 0.000001:
                    continue

                ticker = b['asset']
                tid = obtener_traductor_id(cursor, "bingx_crypto", ticker)

                registrar_saldo(cursor, uid, tid, total,
                                float(b['locked']),
                                ticker, "BINGX", "SPOT")

        print(f"[OK] BingX User {uid}")

    except Exception as e:
        print(f"[ERROR BINGX {uid}] {e}")

# ==================================================
# 📊 BINGX SPOT TRADES
# ==================================================

endpoint = "BINGX_SPOT_TRADES"
last_ts = obtener_last_sync(cursor, uid, "BINGX", endpoint)

max_ts = last_ts

try:
    res_trades = bx_req("/openApi/spot/v1/trade/history", {"limit": 500})

    if res_trades.get("data"):
        trades = res_trades["data"].get("trades", [])

        for t in trades:

            ts_trade = int(t["time"])
            if ts_trade <= last_ts:
                continue

            trade_id = f"BX-SPOT-{t['orderId']}-{t['id']}"
            symbol = t["symbol"]
            qty = float(t["qty"])
            price = float(t["price"])
            side = t["side"]
            commission = float(t.get("commission", 0))
            commission_asset = t.get("commissionAsset", symbol)

            monto = qty * price

            # ===============================
            # INSERT detalle_trades
            # ===============================

            sql_det = """
                INSERT IGNORE INTO detalle_trades
                (user_id, exchange, tipo_producto, exchange_fuente,
                 tipo_mercado, id_externo_ref, fecha_utc,
                 symbol, lado, precio_ejecucion,
                 cantidad_ejecutada, pnl_realizado,
                 is_maker, broker, trade_id_externo)
                VALUES (%s,'BINGX','SPOT','API',
                        'SPOT',%s,FROM_UNIXTIME(%s/1000),
                        %s,%s,%s,%s,0,
                        %s,'BINGX',%s)
            """

            cursor.execute(sql_det, (
                uid, trade_id, ts_trade,
                symbol, side, price, qty,
                0, t["id"]
            ))

            # ===============================
            # INSERT TRADE
            # ===============================

            sql_tx_trade = """
                INSERT IGNORE INTO transacciones_globales
                (user_id, exchange, id_externo,
                 categoria, monto_neto,
                 asset, fecha_utc)
                VALUES (%s,'BINGX',%s,
                        'TRADE',%s,
                        %s,FROM_UNIXTIME(%s/1000))
            """

            cursor.execute(sql_tx_trade, (
                uid, trade_id,
                monto,
                symbol,
                ts_trade
            ))

            # ===============================
            # INSERT FEE
            # ===============================

            if commission != 0:
                fee_id = f"{trade_id}-FEE"

                sql_tx_fee = """
                    INSERT IGNORE INTO transacciones_globales
                    (user_id, exchange, id_externo,
                     categoria, monto_neto,
                     asset, fecha_utc)
                    VALUES (%s,'BINGX',%s,
                            'FEE',%s,
                            %s,FROM_UNIXTIME(%s/1000))
                """

                cursor.execute(sql_tx_fee, (
                    uid, fee_id,
                    -commission,
                    commission_asset,
                    ts_trade
                ))

            if ts_trade > max_ts:
                max_ts = ts_trade

    if max_ts > last_ts:
        actualizar_sync(cursor, uid, "BINGX", endpoint, max_ts)

    print(f"[OK] BingX Spot Trades User {uid}")

except Exception as e:
    print(f"[ERROR BINGX SPOT TRADES {uid}] {e}")

# ==================================================
# 📊 BINGX FUTURES TRADES (USDT-M)
# ==================================================

endpoint = "BINGX_FUTURES_TRADES"
last_ts = obtener_last_sync(cursor, uid, "BINGX", endpoint)

max_ts = last_ts

try:
    res_fut = bx_req("/openApi/swap/v2/trade/allOrders", {"limit": 500})

    if res_fut.get("data"):
        trades = res_fut["data"].get("orders", [])

        for t in trades:

            if t.get("status") != "FILLED":
                continue

            ts_trade = int(t["updateTime"])
            if ts_trade <= last_ts:
                continue

            trade_id = f"BX-FUT-{t['orderId']}"
            symbol = t["symbol"]
            qty = float(t["executedQty"])
            price = float(t["avgPrice"])
            side = t["side"]

            monto = qty * price

            # ===============================
            # INSERT detalle_trades
            # ===============================

            sql_det = """
                INSERT IGNORE INTO detalle_trades
                (user_id, exchange, tipo_producto, exchange_fuente,
                 tipo_mercado, id_externo_ref, fecha_utc,
                 symbol, lado, precio_ejecucion,
                 cantidad_ejecutada, pnl_realizado,
                 is_maker, broker, trade_id_externo)
                VALUES (%s,'BINGX','FUTURES','API',
                        'FUTURES',%s,FROM_UNIXTIME(%s/1000),
                        %s,%s,%s,%s,0,
                        0,'BINGX',%s)
            """

            cursor.execute(sql_det, (
                uid, trade_id, ts_trade,
                symbol, side, price, qty,
                t["orderId"]
            ))

            # ===============================
            # INSERT TRADE
            # ===============================

            sql_tx_trade = """
                INSERT IGNORE INTO transacciones_globales
                (user_id, exchange, id_externo,
                 categoria, monto_neto,
                 asset, fecha_utc)
                VALUES (%s,'BINGX',%s,
                        'TRADE',%s,
                        %s,FROM_UNIXTIME(%s/1000))
            """

            cursor.execute(sql_tx_trade, (
                uid, trade_id,
                monto,
                symbol,
                ts_trade
            ))

            if ts_trade > max_ts:
                max_ts = ts_trade

    if max_ts > last_ts:
        actualizar_sync(cursor, uid, "BINGX", endpoint, max_ts)

    print(f"[OK] BingX Futures Trades User {uid}")

except Exception as e:
    print(f"[ERROR BINGX FUTURES TRADES {uid}] {e}")

# ===============================
# INSERT REALIZED PNL (si existe)
# ===============================

realized = float(t.get("realizedPnl", 0))

if realized != 0:

    pnl_id = f"{trade_id}-PNL"

    sql_pnl = """
        INSERT IGNORE INTO transacciones_globales
        (user_id, exchange, id_externo,
         categoria, monto_neto,
         asset, fecha_utc)
        VALUES (%s,'BINGX',%s,
                'TRADE',%s,
                %s,FROM_UNIXTIME(%s/1000))
    """

    cursor.execute(sql_pnl, (
        uid, pnl_id,
        realized,
        symbol,
        ts_trade
    ))

# ==================================================
# 🔄 BINGX INTERNAL TRANSFERS
# ==================================================

endpoint = "BINGX_TRANSFERS"
last_ts = obtener_last_sync(cursor, uid, "BINGX", endpoint)
max_ts = last_ts

try:
    res_tr = bx_req("/openApi/spot/v1/account/transfer/history", {"limit": 500})

    if res_tr.get("data"):
        transfers = res_tr["data"].get("rows", [])

        for t in transfers:

            ts_tr = int(t["createTime"])
            if ts_tr <= last_ts:
                continue

            tx_id = f"BX-TR-{t['transferId']}"
            amount = float(t["amount"])
            asset = t["asset"]

            # Dirección
            from_acc = t.get("fromAccountType", "")
            to_acc = t.get("toAccountType", "")

            # Si sale de SPOT → negativo
            monto = -amount if from_acc == "SPOT" else amount

            sql = """
                INSERT IGNORE INTO transacciones_globales
                (user_id, exchange, id_externo,
                 categoria, monto_neto,
                 asset, fecha_utc)
                VALUES (%s,'BINGX',%s,
                        'TRANSFER_INTERNAL',%s,
                        %s,FROM_UNIXTIME(%s/1000))
            """

            cursor.execute(sql, (
                uid, tx_id,
                monto,
                asset,
                ts_tr
            ))

            if ts_tr > max_ts:
                max_ts = ts_tr

    if max_ts > last_ts:
        actualizar_sync(cursor, uid, "BINGX", endpoint, max_ts)

    print(f"[OK] BingX Transfers User {uid}")

except Exception as e:
    print(f"[ERROR BINGX TRANSFERS {uid}] {e}")

# ==========================================================
# 🏦 CONCILIACIÓN AUTOMÁTICA EMPRESARIAL
# Versión 1.0
# ==========================================================

def ejecutar_conciliacion(db):

    print("🔎 Iniciando Conciliación Empresarial...")

    cursor = db.cursor(dictionary=True)

    # ==========================
    # 1. Obtener combinaciones activas
    # ==========================

    sql_assets = """
        SELECT DISTINCT user_id, broker_name AS exchange, asset
        FROM sys_saldos_usuarios
        WHERE cantidad_total != 0
    """

    cursor.execute(sql_assets)
    registros = cursor.fetchall()

    for r in registros:

        user_id = r["user_id"]
        exchange = r["exchange"]
        asset = r["asset"]

        # ==========================
        # 2. Calcular ledger
        # ==========================

        sql_ledger = """
            SELECT IFNULL(SUM(monto_neto),0) AS total
            FROM transacciones_globales
            WHERE user_id=%s
              AND exchange=%s
              AND asset=%s
        """

        cursor.execute(sql_ledger, (user_id, exchange, asset))
        ledger_total = float(cursor.fetchone()["total"])

        # ==========================
        # 3. Obtener snapshot
        # ==========================

        sql_snapshot = """
            SELECT cantidad_total
            FROM sys_saldos_usuarios
            WHERE user_id=%s
              AND broker_name=%s
              AND asset=%s
            LIMIT 1
        """

        cursor.execute(sql_snapshot, (user_id, exchange, asset))
        snap_row = cursor.fetchone()

        if not snap_row:
            continue

        snapshot_total = float(snap_row["cantidad_total"])

        # ==========================
        # 4. Calcular diferencia
        # ==========================

        diferencia = snapshot_total - ledger_total

        estado = "OK"
        if abs(diferencia) >= 0.0001:
            estado = "DESCUADRE"

        # ==========================
        # 5. Registrar resultado
        # ==========================

        sql_insert = """
            INSERT INTO sys_conciliacion_saldos
            (user_id, exchange, asset,
             ledger_total, snapshot_total,
             diferencia, estado, fecha_revision)
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        """

        cursor.execute(sql_insert, (
            user_id,
            exchange,
            asset,
            ledger_total,
            snapshot_total,
            diferencia,
            estado
        ))

    db.commit()

    print("✅ Conciliación Finalizada.")

# ==========================================================
# 🚀 ORQUESTADOR
# ==========================================================

def run():

    print("🚀 MOTOR ENTERPRISE v1.0 INICIADO")

    while True:

        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)

            cursor.execute("""
                SELECT user_id, api_key, api_secret, broker_name
                FROM api_keys
                WHERE status=1
            """)

            users = cursor.fetchall()

            for u in users:

                key = descifrar_dato(u['api_key'], MASTER_KEY)
                sec = descifrar_dato(u['api_secret'], MASTER_KEY)

                if not key or not sec:
                    continue

                broker = u['broker_name'].upper()

                if broker == "BINANCE":
                    procesar_binance(db, u['user_id'], key, sec)

                elif broker == "BINGX":
                    procesar_bingx(db, u['user_id'], key, sec)


            # Ejecutar conciliación después de actualizar todo
            ejecutar_conciliacion(db)

            db.commit()
            db.close()

        except Exception as e:
            print(f"[CRITICAL] {e}")

        time.sleep(60)


if __name__ == "__main__":
# ==========================================================
# 📊 SYS SYNC ESTADO
# ==========================================================

def obtener_last_sync(cursor, user_id, exchange, endpoint):
    sql = """
        SELECT last_timestamp FROM sys_sync_estado
        WHERE user_id=%s AND exchange=%s AND endpoint=%s
        LIMIT 1
    """
    cursor.execute(sql, (user_id, exchange, endpoint))
    row = cursor.fetchone()
    return row['last_timestamp'] if row else 0


def actualizar_sync(cursor, user_id, exchange, endpoint, ts):
    sql = """
        INSERT INTO sys_sync_estado (user_id, exchange, endpoint, last_timestamp, last_update)
        VALUES (%s,%s,%s,%s,NOW())
        ON DUPLICATE KEY UPDATE
            last_timestamp=VALUES(last_timestamp),
            last_update=NOW()
    """
    cursor.execute(sql, (user_id, exchange, endpoint, ts))
    run()