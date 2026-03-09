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

MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

# ==========================================================
# 🔐 SEGURIDAD Y HELPERS
# ==========================================================
def descifrar_dato(t, m):
    try:
        if not t: return None
        raw = base64.b64decode(t.strip())
        partes = raw.rsplit(b":::", 1) if b":::" in raw else raw.rsplit(b"::", 1)
        if len(partes) != 2: return None
        data, iv = partes
        key_hash = sha256(m.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

def obtener_traductor_id(cursor, motor_fuente, ticker):
    sql = "SELECT id, categoria_producto, tipo_investment FROM sys_traductor_simbolos WHERE motor_fuente=%s AND ticker_motor=%s LIMIT 1"
    cursor.execute(sql, (motor_fuente, ticker))
    return cursor.fetchone()

def registrar_saldo(cursor, uid, info_traductor, total, locked, asset, broker, tipo_cuenta):
    tid = info_traductor['id'] if info_traductor else None
    # Simplificado: el precio se actualiza por otro motor, aquí solo guardamos cantidades
    sql = """
        INSERT INTO sys_saldos_usuarios 
        (user_id, broker_name, asset, traductor_id, cantidad_total, cantidad_disponible, cantidad_bloqueada, tipo_cuenta, last_update) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW()) 
        ON DUPLICATE KEY UPDATE 
            cantidad_total=VALUES(cantidad_total), cantidad_disponible=VALUES(cantidad_disponible),
            cantidad_bloqueada=VALUES(cantidad_bloqueada), last_update=NOW()
    """
    cursor.execute(sql, (uid, broker, asset, tid, total, total-locked, locked, tipo_cuenta))

def obtener_punto_inicio_sincro(cursor, uid, broker, endpoint):
    sql = "SELECT last_timestamp FROM sys_sync_estado WHERE user_id = %s AND broker = %s AND endpoint = %s LIMIT 1"
    cursor.execute(sql, (uid, broker, endpoint))
    row = cursor.fetchone()
    return int(row['last_timestamp']) if row and row['last_timestamp'] else 1735689600000

def actualizar_punto_sincro(cursor, uid, broker, endpoint, nuevo_ts):
    sql = """
        INSERT INTO sys_sync_estado (user_id, broker, endpoint, last_timestamp, last_update)
        VALUES (%s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE last_timestamp = VALUES(last_timestamp), last_update = NOW()
    """
    cursor.execute(sql, (uid, broker, endpoint, nuevo_ts))

def registrar_trade_completo(cursor, uid, t_data, info_traductor, broker_nombre):
    try:
        tipo_prod = info_traductor['categoria_producto'] if info_traductor else 'SPOT'
        tipo_merc = info_traductor['tipo_investment'] if info_traductor else 'CRYPTO'
        id_vinculo = f"{uid}-{t_data['orderId']}"
        
        sql_global = "INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, comision, fecha_utc, broker) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql_global, (id_vinculo, uid, broker_nombre, tipo_prod, 'TRADE', t_data['symbol'], t_data['quoteQty'], t_data['commission'], t_data['fecha_sql'], broker_nombre))
        
        sql_detalle = """
            INSERT IGNORE INTO detalle_trades 
            (user_id, exchange, tipo_producto, exchange_fuente, tipo_mercado, id_externo_ref, fecha_utc, symbol, lado, precio_ejecucion, cantidad_ejecutada, commission, commission_asset, quote_qty, is_maker, broker, trade_id_externo, raw_json) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql_detalle, (uid, broker_nombre, tipo_prod, broker_nombre, tipo_merc, id_vinculo, t_data['fecha_sql'], t_data['symbol'], t_data['side'], t_data['price'], t_data['qty'], t_data['commission'], t_data['commissionAsset'], t_data['quoteQty'], 1 if t_data.get('isMaker') else 0, broker_nombre, f"TRD-{t_data['orderId']}", json.dumps(t_data)))
        return True
    except: return False

# ==========================================================
# 🟦 PROCESADOR BINGX (V6.8.8)
# ==========================================================
def procesar_bingx(db, uid, ak, as_):
    cursor = db.cursor(dictionary=True)
    def bx_req(path, params=None):
        if params is None: params = {}
        params["timestamp"] = int(time.time()*1000)
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        try:
            r = requests.get(url, headers={"X-BX-APIKEY": ak}, timeout=15)
            return r.json()
        except: return {}

    # 1. SALDOS SPOT
    try:
        res_bal = bx_req("/openApi/spot/v1/account/balance")
        if res_bal.get("data") and "balances" in res_bal['data']:
            for b in res_bal['data']['balances']:
                total = float(b.get('free', 0)) + float(b.get('locked', 0))
                if total > 0:
                    info = obtener_traductor_id(cursor, "bingx_crypto", b['asset'])
                    registrar_saldo(cursor, uid, info, total, float(b.get('locked', 0)), b['asset'], "BINGX", "SPOT")
    except: pass

    # 2. TRADES FUTUROS (SALTOS 7 DÍAS)
    cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente LIKE 'bingx_%%'")
    simbolos = cursor.fetchall()
    siete_dias_ms = 7 * 24 * 60 * 60 * 1000
    ahora_ts = int(time.time()*1000)

    for item in simbolos:
        symbol = item['ticker_motor']
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINGX", f"futures_{symbol}")
        temp_start = start_ts
        nuevos_bingx = 0
        
        while temp_start < ahora_ts:
            temp_end = min(temp_start + siete_dias_ms, ahora_ts)
            res = bx_req("/openApi/swap/v2/trade/allOrders", {"symbol": symbol, "startTime": temp_start, "endTime": temp_end})
            
            if res.get("code") == 0 and res.get("data"):
                for t in res["data"]:
                    if str(t.get('status')) in ['FILLED', 'CLOSED', 'COMPLETED']:
                        t_f = {
                            'orderId': str(t['orderId']), 'symbol': symbol, 'side': t['side'],
                            'price': float(t.get('avgPrice') or t.get('price') or 0),
                            'qty': float(t.get('executedQty', 0)), 'quoteQty': float(t.get('cumQuote', 0)),
                            'commission': abs(float(t.get('commission', 0))), 'commissionAsset': 'USDT',
                            'fecha_sql': datetime.fromtimestamp(t.get('updateTime', ahora_ts)/1000).strftime('%Y-%m-%d %H:%M:%S')
                        }
                        if registrar_trade_completo(cursor, uid, t_f, item, "BINGX"): nuevos_bingx += 1
                temp_start = temp_end
            else:
                temp_start = temp_end
        
        if nuevos_bingx > 0: print(f"    [OK] BingX {symbol}: {nuevos_bingx} importados.")
        actualizar_punto_sincro(cursor, uid, "BINGX", f"futures_{symbol}", ahora_ts)

# ==========================================================
# 🟨 PROCESADOR BINANCE (V6.8.8)
# ==========================================================
def procesar_binance(db, uid, k, s):
    try:
        cursor = db.cursor(dictionary=True)
        client_spot = Client(k, s); client_um = UMFutures(key=k, secret=s); client_cm = CMFutures(key=k, secret=s)
        
        # 1. SALDOS SPOT
        acc = client_spot.get_account()
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.000001:
                info = obtener_traductor_id(cursor, "binance_spot", b['asset'])
                registrar_saldo(cursor, uid, info, total, float(b['locked']), b['asset'], "BINANCE", "SPOT")

        # 2. TRADES UM & CM (SALTOS 7 DÍAS)
        ahora_ts = int(time.time() * 1000)
        siete_dias_ms = 7 * 24 * 60 * 60 * 1000

        for motor in ['binance_usdt_future', 'binance_coin_future']:
            cursor.execute(f"SELECT * FROM sys_traductor_simbolos WHERE motor_fuente = '{motor}'")
            simbolos = cursor.fetchall()
            client = client_um if motor == 'binance_usdt_future' else client_cm
            prefijo = "um" if motor == 'binance_usdt_future' else "cm"

            for item in simbolos:
                symbol = item['ticker_motor'].upper()
                temp_start = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", f"{prefijo}_{symbol}")
                nuevos = 0
                
                while temp_start < ahora_ts:
                    temp_end = min(temp_start + siete_dias_ms, ahora_ts)
                    try:
                        trades = client.get_account_trades(symbol=symbol, startTime=temp_start, endTime=temp_end)
                        if trades:
                            for t in trades:
                                t_f = {
                                    'orderId': str(t['id']), 'symbol': symbol,
                                    'side': 'BUY' if t.get('buyer') is True or t.get('isBuyer') is True else 'SELL',
                                    'price': float(t['price']), 'qty': float(t['qty']),
                                    'quoteQty': float(t.get('quoteQty') or (float(t['price']) * float(t['qty']))),
                                    'commission': float(t['commission']), 'commissionAsset': t['commissionAsset'],
                                    'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
                                }
                                if registrar_trade_completo(cursor, uid, t_f, item, "BINANCE"): nuevos += 1
                            temp_start = trades[-1]['time'] + 1
                        else: temp_start = temp_end
                    except: break
                
                if nuevos > 0: print(f"    [OK] Binance {prefijo.upper()} {symbol}: {nuevos} importados.")
                actualizar_punto_sincro(cursor, uid, "BINANCE", f"{prefijo}_{symbol}", ahora_ts)
                
    except Exception as e: print(f"    [ERROR BINANCE] {e}")

def run():
    print(f"💎 MOTOR v6.8.8 - FULL RECOVERY")
    while True:
        print(f"\n{'='*65}\n🔄 INICIO CICLO: {datetime.now().strftime('%H:%M:%S')}\n{'='*65}")
        db = None
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1")
            for u in cursor.fetchall():
                print(f">> TRABAJANDO: User {u['user_id']} | {u['broker_name']}")
                k, s = descifrar_dato(u['api_key'], MASTER_KEY), descifrar_dato(u['api_secret'], MASTER_KEY)
                if u['broker_name'].upper() == "BINANCE": procesar_binance(db, u['user_id'], k, s)
                elif u['broker_name'].upper() == "BINGX": procesar_bingx(db, u['user_id'], k, s)
                db.commit()
        except Exception as e: print(f"    [CRITICAL] {e}")
        finally:
            if db and db.is_connected(): db.close()
        print(f"\n{'='*65}\n✅ CICLO TERMINADO - ESPERANDO 5 MIN\n{'='*65}")
        time.sleep(300)

if __name__ == "__main__": run()