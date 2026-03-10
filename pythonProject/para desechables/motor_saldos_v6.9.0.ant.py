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
# 🔐 SEGURIDAD Y HELPERS (ESTRUCTURA 6.6.6)
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
    sql = "SELECT id, categoria_producto, tipo_investment FROM sys_traductor_simbolos WHERE motor_fuente=%s AND ticker_motor=%s AND is_active=1 LIMIT 1"
    cursor.execute(sql, (motor_fuente, ticker))
    return cursor.fetchone()

def obtener_precio_usd(cursor, tid, asset_name):
    asset_name = asset_name.upper()
    if asset_name.replace("LD", "").replace("STK", "") in ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']: return 1.0
    try:
        if tid:
            sql = "SELECT price FROM sys_precios_activos WHERE traductor_id = %s ORDER BY last_update DESC LIMIT 1"
            cursor.execute(sql, (tid,))
            row = cursor.fetchone()
            if row and row['price'] > 0: return float(row['price'])
    except: pass
    return 0.0

def registrar_saldo(cursor, uid, info_traductor, total, locked, asset, broker, tipo_cuenta):
    tid = info_traductor['id'] if info_traductor else None
    precio = obtener_precio_usd(cursor, tid, asset)
    valor_usd = total * precio
    sql = """
        INSERT INTO sys_saldos_usuarios 
        (user_id, broker_name, asset, traductor_id, cantidad_total, cantidad_disponible, cantidad_bloqueada, valor_usd, precio_referencia, tipo_cuenta, last_update) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()) 
        ON DUPLICATE KEY UPDATE 
            cantidad_total=VALUES(cantidad_total), cantidad_disponible=VALUES(cantidad_disponible),
            cantidad_bloqueada=VALUES(cantidad_bloqueada), valor_usd=VALUES(valor_usd), 
            precio_referencia=VALUES(precio_referencia), last_update=NOW()
    """
    cursor.execute(sql, (uid, broker, asset, tid, total, total-locked, locked, valor_usd, precio, tipo_cuenta))

# ==========================================================
# 🕒 GESTIÓN DE TIEMPO (AVANCE 6.8.8)
# ==========================================================
def obtener_punto_inicio_sincro(cursor, uid, broker, endpoint):
    sql = "SELECT last_timestamp FROM sys_sync_estado WHERE user_id = %s AND broker = %s AND endpoint = %s LIMIT 1"
    cursor.execute(sql, (uid, broker, endpoint))
    row = cursor.fetchone()
    # Por defecto: 1 de Enero 2025 si no hay registro
    return int(row['last_timestamp']) if row and row['last_timestamp'] else 1735689600000

def actualizar_punto_sincro(cursor, uid, broker, endpoint, nuevo_ts):
    sql = """
        INSERT INTO sys_sync_estado (user_id, broker, endpoint, last_timestamp, last_update)
        VALUES (%s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE last_timestamp = VALUES(last_timestamp), last_update = NOW()
    """
    cursor.execute(sql, (uid, broker, endpoint, nuevo_ts))

# ==========================================================
# 📉 REGISTRO DE TRADES (ESTRUCTURA 6.6.6)
# ==========================================================
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
    except Exception as e:
        print(f"        [!] ERROR DE INSERCIÓN DETALLE: {e}")
        return False

# ==========================================================
# 🟦 PROCESADOR BINGX (V6.9.0 - LEGACY + TIME BLOCK)
# ==========================================================
def procesar_bingx(db, uid, ak, as_):
    cursor = db.cursor(dictionary=True)
    print(f"    [DEBUG] Iniciando ciclo completo BingX para User {uid}...")
    
    def bx_req(path, params=None):
        if params is None: params = {}
        ts = int(time.time()*1000)
        params["timestamp"] = ts
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        try:
            r = requests.get(url, headers={"X-BX-APIKEY": ak}, timeout=10)
            return r.json()
        except: return {}

    def buscar_en_traductor_bingx(simbolo_api):
        if not simbolo_api: return None
        s_api = simbolo_api.upper().strip()
        s_limpio = s_api.replace("-", "").replace("/", "").replace("=X", "")
        und_api = s_limpio.replace("USDT", "").replace("USDC", "").replace("USD", "")

        sql = """SELECT id, ticker_motor, underlying, categoria_producto, tipo_investment 
                 FROM sys_traductor_simbolos 
                 WHERE is_active = 1 AND (
                    (motor_fuente LIKE 'bingx_%%' AND (ticker_motor = %s OR REPLACE(ticker_motor, '-', '') = %s OR underlying = %s))
                    OR (motor_fuente LIKE 'binance_%%' AND (ticker_motor = %s OR REPLACE(ticker_motor, '-', '') = %s))
                    OR (underlying = %s)
                 ) ORDER BY (motor_fuente LIKE 'bingx_%%') DESC LIMIT 1"""
        cursor.execute(sql, (s_api, s_limpio, und_api, s_api, s_limpio, und_api))
        res = cursor.fetchone()
        
        if not res: # Rescate para CFDs
            sql_cfd = """SELECT id, ticker_motor, underlying, categoria_producto, tipo_investment 
                         FROM sys_traductor_simbolos 
                         WHERE motor_fuente LIKE 'bingx_%%' 
                         AND %s LIKE CONCAT('%%', underlying, '%%')
                         AND is_active = 1 LIMIT 1"""
            cursor.execute(sql_cfd, (s_limpio,))
            res = cursor.fetchone()
        return res

    # 1. SALDOS SPOT Y PERPETUAL
    s_count_global = 0
    try:
        # Spot
        res_spot = bx_req("/openApi/spot/v1/account/balance")
        if res_spot.get("data") and "balances" in res_spot['data']:
            for b in res_spot['data']['balances']:
                total = float(b.get('free', 0)) + float(b.get('locked', 0))
                if total > 0.000001:
                    info = buscar_en_traductor_bingx(b['asset'])
                    registrar_saldo(cursor, uid, info, total, float(b.get('locked', 0)), b['asset'], "BINGX", "SPOT")
                    s_count_global += 1
        # Perp
        res_perp = bx_req("/openApi/swap/v2/user/balance")
        if res_perp.get("data") and res_perp['data'].get("balance"):
            bal = res_perp['data']['balance']
            total = float(bal.get("balance", 0))
            if total > 0.000001:
                info = buscar_en_traductor_bingx(bal.get("asset"))
                registrar_saldo(cursor, uid, info, total, float(bal.get("freezedMargin", 0)), bal.get("asset"), "BINGX", "FUTURES")
                s_count_global += 1
        print(f"    [OK] BingX Saldos actualizado: {s_count_global} activos totales.")
    except Exception as e: print(f"    [!] Error en Saldos BingX: {e}")

    # 2. OPEN ORDERS (SPOT + FUTURES)
    for path, tipo, tabla in [("/openApi/spot/v1/trade/openOrders", "SPOT", "sys_open_orders_spot"), 
                              ("/openApi/swap/v2/trade/openOrders", "FUTURES", "sys_open_orders")]:
        cursor.execute(f"DELETE FROM {tabla} WHERE user_id = %s AND broker_name = 'BINGX'", (uid,))
        res = bx_req(path)
        orders = res.get("data", {}).get("orders", []) if "spot" in path else res.get("data", {}).get("orders", [])
        if orders:
            for o in orders:
                sym = o.get('symbol')
                info = buscar_en_traductor_bingx(sym)
                sql = f"INSERT INTO {tabla} (id_order_ext, user_id, broker_name, traductor_id, symbol, side, type, price, qty, locked_amount, fecha_utc, estado, last_seen) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())"
                cursor.execute(sql, (str(o['orderId']), uid, "BINGX", info['id'] if info else None, sym, o['side'], o.get('type','LIMIT'), float(o['price']), float(o['origQty']), 0.0, datetime.fromtimestamp(o.get('time', time.time()*1000)/1000).strftime('%Y-%m-%d %H:%M:%S'), 'ABIERTA'))

    # 3. TRADES CON SALTOS DE 7 DÍAS (AVANCE 6.9.0)
    try:
        ahora_ts = int(time.time() * 1000)
        siete_dias_ms = 7 * 24 * 60 * 60 * 1000
        cursor.execute("SELECT DISTINCT ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente LIKE 'bingx_%%' AND is_active = 1")
        universo = cursor.fetchall()
        t_count = 0
        
        for item in universo:
            sym = item['ticker_motor']
            temp_start = obtener_punto_inicio_sincro(cursor, uid, "BINGX", f"trades_{sym}")
            
            while temp_start < ahora_ts:
                temp_end = min(temp_start + siete_dias_ms, ahora_ts)
                res_tr = bx_req("/openApi/swap/v2/trade/allOrders", {"symbol": sym, "startTime": temp_start, "endTime": temp_end})
                if res_tr.get("code") == 0 and res_tr.get("data"):
                    for t in res_tr["data"]:
                        if str(t.get('status')) in ['FILLED', 'PARTIALLY_FILLED', 'CLOSED', 'COMPLETED']:
                            info = buscar_en_traductor_bingx(sym)
                            t_f = {'orderId': str(t['orderId']), 'symbol': sym, 'side': t['side'], 'price': float(t.get('avgPrice') or t.get('price') or 0), 'qty': float(t.get('executedQty', 0)), 'quoteQty': float(t.get('cumQuote', 0)), 'commission': abs(float(t.get('commission', 0))), 'commissionAsset': 'USDT', 'fecha_sql': datetime.fromtimestamp(t.get('updateTime', ahora_ts)/1000).strftime('%Y-%m-%d %H:%M:%S')}
                            if registrar_trade_completo(cursor, uid, t_f, info, "BINGX"): t_count += 1
                temp_start = temp_end
            actualizar_punto_sincro(cursor, uid, "BINGX", f"trades_{sym}", ahora_ts)
        print(f"    [INFO] BingX Trades: {t_count} nuevos procesados.")
    except Exception as e: print(f"    [!] Error en Trades BingX: {e}")

# ==========================================================
# 🟨 PROCESADOR BINANCE (V6.9.0 - SPOT + UM + CM)
# ==========================================================
def procesar_binance(db, uid, k, s):
    try:
        cursor = db.cursor(dictionary=True)
        client_spot = Client(k, s); client_um = UMFutures(key=k, secret=s); client_cm = CMFutures(key=k, secret=s)
        ahora_ts = int(time.time() * 1000)
        siete_dias_ms = 7 * 24 * 60 * 60 * 1000

        # 1. SALDOS SPOT
        acc = client_spot.get_account()
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.000001:
                info = obtener_traductor_id(cursor, "binance_spot", b['asset'])
                registrar_saldo(cursor, uid, info, total, float(b['locked']), b['asset'], "BINANCE", "SPOT")

        # 2. TRADES (SPOT + UM + CM) CON SALTOS
        motores = [('binance_spot', client_spot, 'spot'), ('binance_usdt_future', client_um, 'um'), ('binance_coin_future', client_cm, 'cm')]
        
        for m_nombre, m_client, m_prefijo in motores:
            cursor.execute(f"SELECT * FROM sys_traductor_simbolos WHERE motor_fuente = '{m_nombre}' AND is_active = 1")
            simbolos = cursor.fetchall()
            t_total = 0
            
            for item in simbolos:
                sym = item['ticker_motor'].upper()
                temp_start = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", f"{m_prefijo}_{sym}")
                
                while temp_start < ahora_ts:
                    temp_end = min(temp_start + siete_dias_ms, ahora_ts)
                    try:
                        if m_prefijo == 'spot':
                            raw = m_client.get_my_trades(symbol=sym, startTime=temp_start, endTime=temp_end)
                        else:
                            raw = m_client.get_account_trades(symbol=sym, startTime=temp_start, endTime=temp_end)
                        
                        if raw:
                            for t in raw:
                                side = 'BUY' if t.get('isBuyer') is True or t.get('buyer') is True else 'SELL'
                                t_f = {'orderId': str(t.get('id') or t.get('orderId')), 'symbol': sym, 'side': side, 'price': float(t['price']), 'qty': float(t['qty']), 'quoteQty': float(t.get('quoteQty') or (float(t['price'])*float(t['qty']))), 'commission': float(t['commission']), 'commissionAsset': t['commissionAsset'], 'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')}
                                if registrar_trade_completo(cursor, uid, t_f, item, "BINANCE"): t_total += 1
                            temp_start = raw[-1]['time'] + 1
                        else: temp_start = temp_end
                    except: break
                actualizar_punto_sincro(cursor, uid, "BINANCE", f"{m_prefijo}_{sym}", ahora_ts)
            if t_total > 0: print(f"    [OK] Binance {m_prefijo.upper()}: {t_total} trades.")

        # 3. OPEN ORDERS SPOT
        cursor.execute("DELETE FROM sys_open_orders_spot WHERE user_id = %s AND broker_name = 'BINANCE'", (uid,))
        for oo in client_spot.get_open_orders():
            info = obtener_traductor_id(cursor, "binance_spot", oo['symbol'])
            sql = "INSERT INTO sys_open_orders_spot (id_order_ext, user_id, broker_name, traductor_id, symbol, side, type, price, qty, locked_amount, fecha_utc, estado, last_seen) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())"
            cursor.execute(sql, (str(oo['orderId']), uid, "BINANCE", info['id'] if info else None, oo['symbol'], oo['side'], oo['type'], float(oo['price']), float(oo['origQty']), 0.0, datetime.fromtimestamp(oo['time']/1000).strftime('%Y-%m-%d %H:%M:%S'), 'ABIERTA'))

    except Exception as e: print(f"    [!] Error Binance User {uid}: {e}")

# ==========================================================
# 🚀 EJECUCIÓN PRINCIPAL (6.9.0)
# ==========================================================
def run():
    print(f"💎 MOTOR v6.9.0 - LEGACY STABLE + HISTORY HUNTER")
    
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