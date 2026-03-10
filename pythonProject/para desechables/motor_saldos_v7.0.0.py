# ==========================================================
# 🟦 FUTUROS ENTERPRISE EXTENSION (V7.0.0)
# Compatible con base 6.6.6.05
# No modifica funciones existentes
# ==========================================================

import time
import json
from datetime import datetime
from binance.um_futures import UMFutures
from binance.cm_futures import CMFutures

# ==========================================================
# 🧠 REGISTRADOR FUTUROS (USA TABLAS YA EXISTENTES)
# ==========================================================
def registrar_trade_futuro(cursor, uid, trade, traductor, broker):
    try:
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
            trade['commission'],
            trade['commissionAsset'],
            trade['quoteQty'],
            trade.get('realizedPnl', 0),
            trade.get('isMaker', 0),
            trade['orderId'],
            json.dumps(trade)
        ))

        return cursor.rowcount > 0

    except Exception as e:
        print("Error registrando futuro:", e)
        return False


# ==========================================================
# 🟡 BINANCE UM FUTURES
# ==========================================================
def process_binance_um_futures(db, uid, api_key, api_secret):
    cursor = db.cursor(dictionary=True)
    client = UMFutures(key=api_key, secret=api_secret)

    cursor.execute("""
        SELECT * FROM sys_traductor_simbolos
        WHERE user_id=%s
        AND motor_fuente='binance_usdt_future'
        AND is_active=1
    """, (uid,))
    simbolos = cursor.fetchall()

    ahora = int(time.time()*1000)

    for traductor in simbolos:
        symbol = traductor['ticker_motor'].upper()
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", f"um_{symbol}")
        nuevos = 0

        while start_ts < ahora:
            try:
                trades = client.get_account_trades(symbol=symbol, startTime=start_ts)
                if not trades:
                    break

                for t in trades:
                    trade = {
                        'orderId': str(t['id']),
                        'symbol': symbol,
                        'side': 'BUY' if t['buyer'] else 'SELL',
                        'positionSide': t.get('positionSide'),
                        'price': float(t['price']),
                        'qty': float(t['qty']),
                        'quoteQty': float(t.get('quoteQty', 0)),
                        'commission': float(t['commission']),
                        'commissionAsset': t['commissionAsset'],
                        'realizedPnl': float(t.get('realizedPnl', 0)),
                        'isMaker': int(t.get('maker', False)),
                        'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
                    }

                    if registrar_trade_futuro(cursor, uid, trade, traductor, "BINANCE"):
                        nuevos += 1

                start_ts = trades[-1]['time'] + 1

            except Exception as e:
                print("UM Error:", e)
                break

        actualizar_punto_sincro(cursor, uid, "BINANCE", f"um_{symbol}", start_ts)

        if nuevos > 0:
            print(f"[OK] BINANCE UM {symbol}: {nuevos} nuevos")


# ==========================================================
# 🟠 BINANCE COIN FUTURES
# ==========================================================
def process_binance_cm_futures(db, uid, api_key, api_secret):
    cursor = db.cursor(dictionary=True)
    client = CMFutures(key=api_key, secret=api_secret)

    cursor.execute("""
        SELECT * FROM sys_traductor_simbolos
        WHERE user_id=%s
        AND motor_fuente='binance_coin_future'
        AND is_active=1
    """, (uid,))
    simbolos = cursor.fetchall()

    ahora = int(time.time()*1000)

    for traductor in simbolos:
        symbol = traductor['ticker_motor'].upper()
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", f"cm_{symbol}")
        nuevos = 0

        while start_ts < ahora:
            try:
                trades = client.get_account_trades(symbol=symbol, startTime=start_ts)
                if not trades:
                    break

                for t in trades:
                    trade = {
                        'orderId': str(t['id']),
                        'symbol': symbol,
                        'side': 'BUY' if t['buyer'] else 'SELL',
                        'positionSide': t.get('positionSide'),
                        'price': float(t['price']),
                        'qty': float(t['qty']),
                        'quoteQty': float(t.get('quoteQty', 0)),
                        'commission': float(t['commission']),
                        'commissionAsset': t['commissionAsset'],
                        'realizedPnl': float(t.get('realizedPnl', 0)),
                        'isMaker': int(t.get('maker', False)),
                        'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
                    }

                    if registrar_trade_futuro(cursor, uid, trade, traductor, "BINANCE"):
                        nuevos += 1

                start_ts = trades[-1]['time'] + 1

            except Exception as e:
                print("CM Error:", e)
                break

        actualizar_punto_sincro(cursor, uid, "BINANCE", f"cm_{symbol}", start_ts)

        if nuevos > 0:
            print(f"[OK] BINANCE CM {symbol}: {nuevos} nuevos")


# ==========================================================
# 🟢 BINGX FUTURES
# ==========================================================
def process_bingx_futures(db, uid, api_key, api_secret):
    cursor = db.cursor(dictionary=True)

    def bx_req(path, params=None):
        import hmac, hashlib, requests
        if params is None: params = {}
        params["timestamp"] = int(time.time()*1000)
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        r = requests.get(url, headers={"X-BX-APIKEY": api_key}, timeout=15)
        return r.json()

    cursor.execute("""
        SELECT * FROM sys_traductor_simbolos
        WHERE user_id=%s
        AND motor_fuente LIKE 'bingx_%%'
        AND categoria_producto='FUTURES'
        AND is_active=1
    """, (uid,))
    simbolos = cursor.fetchall()

    siete_dias = 7 * 24 * 60 * 60 * 1000
    ahora = int(time.time()*1000)

    for traductor in simbolos:
        symbol = traductor['ticker_motor']
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
                        'price': float(t.get('avgPrice') or 0),
                        'qty': float(t.get('executedQty', 0)),
                        'quoteQty': float(t.get('cumQuote', 0)),
                        'commission': abs(float(t.get('commission', 0))),
                        'commissionAsset': 'USDT',
                        'realizedPnl': float(t.get('realizedProfit', 0)),
                        'fecha_sql': datetime.fromtimestamp(
                            t.get('updateTime')/1000
                        ).strftime('%Y-%m-%d %H:%M:%S')
                    }

                    registrar_trade_futuro(cursor, uid, trade, traductor, "BINGX")

                start_ts = end_ts + 1
            else:
                start_ts = end_ts + 1

        actualizar_punto_sincro(cursor, uid, "BINGX", f"futures_{symbol}", start_ts)

    print("[OK] BINGX FUTURES sincronizado")