# ==========================================================
# 🔷 MOTOR GLOBAL UNIFICADO (V7.1.0)
# Compatible con 6.6.6.05
# ==========================================================

import time, json, hmac, hashlib, requests
from datetime import datetime
from binance.client import Client
from binance.um_futures import UMFutures
from binance.cm_futures import CMFutures


# ==========================================================
# 🧠 REGISTRADOR UNIVERSAL
# ==========================================================
def registrar_trade(cursor, uid, trade, traductor, broker):

    sql = """
    INSERT IGNORE INTO detalle_trades (
        user_id, traductor_id, broker, categoria_producto,
        motor_fuente, tipo_investment,
        id_externo_ref, fecha_utc, symbol,
        lado, position_side,
        precio_ejecucion, cantidad_ejecutada,
        commission, commission_asset,
        quote_qty, pnl_realizado,
        is_maker, trade_id_externo,
        raw_json
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cursor.execute(sql, (
        uid,
        traductor['id'],
        broker,
        traductor['categoria_producto'],
        traductor['motor_fuente'],
        traductor['tipo_investment'],
        trade['orderId'],
        trade['fecha_sql'],
        trade['symbol'],
        trade['side'],
        trade.get('positionSide'),
        trade['price'],
        trade['qty'],
        trade.get('commission',0),
        trade.get('commissionAsset'),
        trade.get('quoteQty',0),
        trade.get('realizedPnl',0),
        trade.get('isMaker',0),
        trade['orderId'],
        json.dumps(trade)
    ))

    return cursor.rowcount > 0


# ==========================================================
# 🟡 BINANCE SPOT
# ==========================================================
def process_binance_spot(db, uid, k, s):

    cursor = db.cursor(dictionary=True)
    client = Client(k, s)
    ahora = int(time.time()*1000)

    cursor.execute("""
        SELECT * FROM sys_traductor_simbolos
        WHERE user_id=%s
        AND motor_fuente='binance_spot'
        AND is_active=1
    """, (uid,))
    simbolos = cursor.fetchall()

    for traductor in simbolos:
        symbol = traductor['ticker_motor']
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", f"spot_{symbol}")

        while start_ts < ahora:
            trades = client.get_my_trades(symbol=symbol, startTime=start_ts)
            if not trades: break

            for t in trades:
                trade = {
                    'orderId': str(t['id']),
                    'symbol': symbol,
                    'side': 'BUY' if t['isBuyer'] else 'SELL',
                    'price': float(t['price']),
                    'qty': float(t['qty']),
                    'quoteQty': float(t.get('quoteQty',0)),
                    'commission': float(t['commission']),
                    'commissionAsset': t['commissionAsset'],
                    'fecha_sql': datetime.fromtimestamp(
                        t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
                }

                registrar_trade(cursor, uid, trade, traductor, "BINANCE")

            start_ts = trades[-1]['time'] + 1

        actualizar_punto_sincro(cursor, uid, "BINANCE", f"spot_{symbol}", start_ts)

    print("[OK] BINANCE SPOT listo")


# ==========================================================
# 🟠 BINANCE FUTURES (UM + CM)
# ==========================================================
def process_binance_futures(db, uid, k, s):

    cursor = db.cursor(dictionary=True)
    client_um = UMFutures(key=k, secret=s)
    client_cm = CMFutures(key=k, secret=s)
    ahora = int(time.time()*1000)

    for motor, client, prefijo in [
        ("binance_usdt_future", client_um, "um"),
        ("binance_coin_future", client_cm, "cm")
    ]:

        cursor.execute("""
            SELECT * FROM sys_traductor_simbolos
            WHERE user_id=%s AND motor_fuente=%s AND is_active=1
        """, (uid, motor))
        simbolos = cursor.fetchall()

        for traductor in simbolos:
            symbol = traductor['ticker_motor']
            start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", f"{prefijo}_{symbol}")

            while start_ts < ahora:
                trades = client.get_account_trades(symbol=symbol, startTime=start_ts)
                if not trades: break

                for t in trades:
                    trade = {
                        'orderId': str(t['id']),
                        'symbol': symbol,
                        'side': 'BUY' if t['buyer'] else 'SELL',
                        'positionSide': t.get('positionSide'),
                        'price': float(t['price']),
                        'qty': float(t['qty']),
                        'quoteQty': float(t.get('quoteQty',0)),
                        'commission': float(t['commission']),
                        'commissionAsset': t['commissionAsset'],
                        'realizedPnl': float(t.get('realizedPnl',0)),
                        'isMaker': int(t.get('maker',False)),
                        'fecha_sql': datetime.fromtimestamp(
                            t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
                    }

                    registrar_trade(cursor, uid, trade, traductor, "BINANCE")

                start_ts = trades[-1]['time'] + 1

            actualizar_punto_sincro(cursor, uid, "BINANCE", f"{prefijo}_{symbol}", start_ts)

    print("[OK] BINANCE FUTURES listo")


# ==========================================================
# 🟢 BINGX (SPOT + FUTURES)
# ==========================================================
def process_bingx(db, uid, ak, as_):

    cursor = db.cursor(dictionary=True)

    def bx_req(path, params=None):
        if params is None: params = {}
        params["timestamp"] = int(time.time()*1000)
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        r = requests.get(url, headers={"X-BX-APIKEY": ak}, timeout=15)
        return r.json()

    ahora = int(time.time()*1000)
    siete_dias = 7 * 24 * 60 * 60 * 1000

    cursor.execute("""
        SELECT * FROM sys_traductor_simbolos
        WHERE user_id=%s AND is_active=1
        AND motor_fuente LIKE 'bingx%%'
    """, (uid,))
    simbolos = cursor.fetchall()

    for traductor in simbolos:
        symbol = traductor['ticker_motor']

        # SPOT
        if traductor['categoria_producto'] == 'SPOT':
            start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINGX", f"spot_{symbol}")
            res = bx_req("/openApi/spot/v1/trade/allOrders", {"symbol": symbol})

            if res.get("code") == 0 and res.get("data"):
                for t in res["data"]:
                    trade = {
                        'orderId': str(t['orderId']),
                        'symbol': symbol,
                        'side': t['side'],
                        'price': float(t.get('price',0)),
                        'qty': float(t.get('origQty',0)),
                        'quoteQty': float(t.get('cummulativeQuoteQty',0)),
                        'commission': 0,
                        'commissionAsset': None,
                        'fecha_sql': datetime.fromtimestamp(
                            t.get('updateTime')/1000).strftime('%Y-%m-%d %H:%M:%S')
                    }
                    registrar_trade(cursor, uid, trade, traductor, "BINGX")

            actualizar_punto_sincro(cursor, uid, "BINGX", f"spot_{symbol}", ahora)

        # FUTURES
        else:
            start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINGX", f"futures_{symbol}")

            while start_ts < ahora:
                end_ts = min(start_ts + siete_dias, ahora)

                res = bx_req("/openApi/swap/v2/trade/allOrders",
                             {"symbol": symbol, "startTime": start_ts, "endTime": end_ts})

                if res.get("code") == 0 and res.get("data"):
                    for t in res["data"]:
                        trade = {
                            'orderId': str(t['orderId']),
                            'symbol': symbol,
                            'side': t['side'],
                            'positionSide': t.get('positionSide'),
                            'price': float(t.get('avgPrice',0)),
                            'qty': float(t.get('executedQty',0)),
                            'quoteQty': float(t.get('cumQuote',0)),
                            'commission': abs(float(t.get('commission',0))),
                            'commissionAsset': 'USDT',
                            'realizedPnl': float(t.get('realizedProfit',0)),
                            'fecha_sql': datetime.fromtimestamp(
                                t.get('updateTime')/1000).strftime('%Y-%m-%d %H:%M:%S')
                        }
                        registrar_trade(cursor, uid, trade, traductor, "BINGX")

                start_ts = end_ts + 1

            actualizar_punto_sincro(cursor, uid, "BINGX", f"futures_{symbol}", start_ts)

    print("[OK] BINGX listo")