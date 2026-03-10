# ==========================================================
# 💎 MOTOR v7.2.0 ENTERPRISE
# BASE: 6.6.6.05 + FUTURES COMPLETOS + PNL
# ==========================================================

import mysql.connector
from binance.client import Client
from binance.um_futures import UMFutures
from binance.cm_futures import CMFutures
import time, os, base64, hmac, hashlib, requests, json
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from datetime import datetime
import config


# ==========================================================
# 🔐 CONFIGURACIÓN
# ==========================================================

MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
bingx_session = requests.Session()


# ==========================================================
# 🔐 DESCIFRADO
# ==========================================================

def descifrar_dato(t, m):
    try:
        if not t: return None
        raw = base64.b64decode(t.strip())
        data, iv = raw.rsplit(b":::", 1)
        key_hash = sha256(m.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except:
        return None


# ==========================================================
# 🧠 SYNC TIME
# ==========================================================

def obtener_punto_inicio_sincro(cursor, uid, broker, endpoint):
    cursor.execute(
        "SELECT last_timestamp FROM sys_sync_estado WHERE user_id=%s AND broker=%s AND endpoint=%s",
        (uid, broker, endpoint)
    )
    row = cursor.fetchone()
    return int(row['last_timestamp']) if row and row['last_timestamp'] else 1633046400000


def actualizar_punto_sincro(cursor, uid, broker, endpoint, nuevo_ts):
    cursor.execute("""
        INSERT INTO sys_sync_estado (user_id, broker, endpoint, last_timestamp, last_update)
        VALUES (%s,%s,%s,%s,NOW())
        ON DUPLICATE KEY UPDATE last_timestamp=VALUES(last_timestamp), last_update=NOW()
    """, (uid, broker, endpoint, nuevo_ts))


# ==========================================================
# 🎯 REGISTRO COMPLETO (AHORA CON PNL)
# ==========================================================

def registrar_trade_completo(cursor, uid, t_data, info_traductor, broker_nombre):

    traductor_id = info_traductor['id'] if info_traductor else None
    categoria_producto = info_traductor['categoria_producto'] if info_traductor else "SPOT"
    tipo_investment = info_traductor['tipo_investment'] if info_traductor else "CRYPTO"
    motor_fuente = info_traductor['motor_fuente'] if info_traductor else broker_nombre.lower()

    id_vinculo = f"{uid}-{t_data['orderId']}"

    # GLOBAL
    cursor.execute("""
        INSERT IGNORE INTO transacciones_globales
        (id_externo,user_id,exchange,cuenta_tipo,categoria,asset,
         traductor_id,monto_neto,comision,fecha_utc,broker)
        VALUES (%s,%s,%s,%s,'TRADE',%s,%s,%s,%s,%s,%s)
    """, (
        id_vinculo,
        uid,
        broker_nombre,
        categoria_producto,
        t_data['symbol'],
        traductor_id,
        t_data.get('quoteQty',0),
        t_data.get('commission',0),
        t_data['fecha_sql'],
        broker_nombre
    ))

    # DETALLE
    cursor.execute("""
        INSERT IGNORE INTO detalle_trades
        (user_id,traductor_id,broker,motor_fuente,
         categoria_producto,tipo_investment,
         id_externo_ref,fecha_utc,symbol,lado,position_side,
         precio_ejecucion,cantidad_ejecutada,
         commission,commission_asset,quote_qty,
         pnl_realizado,is_maker,trade_id_externo,raw_json)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        uid,
        traductor_id,
        broker_nombre,
        motor_fuente,
        categoria_producto,
        tipo_investment,
        id_vinculo,
        t_data['fecha_sql'],
        t_data['symbol'],
        t_data['side'],
        t_data.get('positionSide'),
        t_data['price'],
        t_data['qty'],
        t_data.get('commission',0),
        t_data.get('commissionAsset'),
        t_data.get('quoteQty',0),
        t_data.get('realizedPnl',0),
        1 if t_data.get('isMaker') else 0,
        f"TRD-{t_data['orderId']}",
        json.dumps(t_data)
    ))

    return True


# ==========================================================
# 🟡 BINANCE COMPLETO (SPOT + UM + CM)
# ==========================================================

def procesar_binance(db, uid, k, s):

    cursor = db.cursor(dictionary=True)

    client_spot = Client(k, s)
    client_um = UMFutures(key=k, secret=s)
    client_cm = CMFutures(key=k, secret=s)

    ahora = int(time.time()*1000)
    siete_dias = 7*24*60*60*1000

    # ======================================================
    # 1️⃣ SPOT
    # ======================================================

    acc = client_spot.get_account()
    for b in acc['balances']:
        total = float(b['free']) + float(b['locked'])
        if total <= 0.000001: continue

        cursor.execute("""
            SELECT * FROM sys_traductor_simbolos
            WHERE motor_fuente='binance_spot'
            AND ticker_motor=%s
        """, (b['asset'],))
        info = cursor.fetchone()

        if info:
            registrar_saldo(cursor, uid, info, total, float(b['locked']), b['asset'], "BINANCE", "SPOT")

    # Trades Spot
    cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente='binance_spot'")
    simbolos = cursor.fetchall()

    for item in simbolos:

        symbol = item['ticker_motor']
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", f"spot_{symbol}")

        trades = client_spot.get_my_trades(symbol=symbol, startTime=start_ts)

        for t in trades:
            t_f = {
                'orderId': str(t['id']),
                'symbol': symbol,
                'side': 'BUY' if t['isBuyer'] else 'SELL',
                'price': float(t['price']),
                'qty': float(t['qty']),
                'quoteQty': float(t.get('quoteQty',0)),
                'commission': float(t['commission']),
                'commissionAsset': t['commissionAsset'],
                'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
            }

            registrar_trade_completo(cursor, uid, t_f, item, "BINANCE")

        actualizar_punto_sincro(cursor, uid, "BINANCE", f"spot_{symbol}", ahora)


    # ======================================================
    # 2️⃣ FUTURES UM + CM
    # ======================================================

    for motor, client, prefijo in [
        ('binance_usdt_future', client_um, 'um'),
        ('binance_coin_future', client_cm, 'cm')
    ]:

        cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente=%s", (motor,))
        simbolos = cursor.fetchall()

        for item in simbolos:

            symbol = item['ticker_motor']
            start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", f"{prefijo}_{symbol}")

            temp_start = start_ts

            while temp_start < ahora:

                temp_end = min(temp_start + siete_dias, ahora)

                trades = client.get_account_trades(
                    symbol=symbol,
                    startTime=temp_start,
                    endTime=temp_end
                )

                if not trades:
                    temp_start = temp_end
                    continue

                for t in trades:
                    t_f = {
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
                        'isMaker': t.get('maker',False),
                        'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
                    }

                    registrar_trade_completo(cursor, uid, t_f, item, "BINANCE")

                temp_start = temp_end

            actualizar_punto_sincro(cursor, uid, "BINANCE", f"{prefijo}_{symbol}", ahora)

    print("    [OK] Binance completo procesado.")


# ==========================================================
# 🟢 BINGX COMPLETO (SPOT + FUTURES)
# ==========================================================

def procesar_bingx(db, uid, ak, as_):

    cursor = db.cursor(dictionary=True)

    def bx_req(path, params=None):
        if params is None: params = {}
        params["timestamp"] = int(time.time()*1000)
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        r = bingx_session.get(url, headers={"X-BX-APIKEY": ak}, timeout=15)
        return r.json()

    ahora = int(time.time()*1000)
    siete_dias = 7*24*60*60*1000

    cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente LIKE 'bingx_%'")
    simbolos = cursor.fetchall()

    for item in simbolos:

        symbol = item['ticker_motor']

        # FUTURES
        if item['categoria_producto'] == "FUTURES":

            start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINGX", f"futures_{symbol}")
            temp_start = start_ts

            while temp_start < ahora:

                temp_end = min(temp_start + siete_dias, ahora)

                res = bx_req("/openApi/swap/v2/trade/allOrders",
                             {"symbol": symbol,
                              "startTime": temp_start,
                              "endTime": temp_end})

                if res.get("code") == 0 and res.get("data"):

                    for t in res["data"]:

                        if str(t.get("status")).upper() not in ["FILLED","CLOSED","COMPLETED"]:
                            continue

                        t_f = {
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

                        registrar_trade_completo(cursor, uid, t_f, item, "BINGX")

                temp_start = temp_end

            actualizar_punto_sincro(cursor, uid, "BINGX", f"futures_{symbol}", ahora)

    print("    [OK] BingX completo procesado.")


# ==========================================================
# 🚀 LOOP PRINCIPAL
# ==========================================================

def run():

    print("💎 MOTOR v7.2.0 ENTERPRISE")

    while True:

        print("\n=========================================================")
        print(f"🔄 INICIO CICLO: {datetime.now().strftime('%H:%M:%S')}")
        print("=========================================================")

        db = None

        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)

            cursor.execute("""
                SELECT user_id, api_key, api_secret, broker_name
                FROM api_keys
                WHERE status=1
            """)

            for u in cursor.fetchall():

                print(f">> TRABAJANDO: User {u['user_id']} | {u['broker_name']}")

                k = descifrar_dato(u['api_key'], MASTER_KEY)
                s = descifrar_dato(u['api_secret'], MASTER_KEY)

                if u['broker_name'].upper() == "BINANCE":
                    procesar_binance(db, u['user_id'], k, s)

                elif u['broker_name'].upper() == "BINGX":
                    procesar_bingx(db, u['user_id'], k, s)

                db.commit()

        except Exception as e:
            print(f"[CRITICAL ERROR] {e}")

        finally:
            if db and db.is_connected():
                db.close()

        print("=========================================================")
        print("✅ CICLO TERMINADO - ESPERANDO 5 MIN")
        print("=========================================================")

        time.sleep(300)


if __name__ == "__main__":
    run()