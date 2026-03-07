import mysql.connector
from binance.client import Client
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
# 🕒 GESTIÓN DE TIEMPO
# ==========================================================
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

# ==========================================================
# 📉 REGISTRO DE TRADES
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
# 🟦 PROCESADOR BINGX (V6.6.8 - REPARACIÓN FINAL MAPPING)
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

        # 1. Búsqueda por Match Exacto o Underlying (BingX -> Binance -> Cualquier otro)
        sql = """SELECT id, ticker_motor, underlying, categoria_producto, tipo_investment 
                 FROM sys_traductor_simbolos 
                 WHERE is_active = 1 AND (
                    (motor_fuente LIKE 'bingx_%%' AND (ticker_motor = %s OR REPLACE(ticker_motor, '-', '') = %s OR underlying = %s))
                    OR (motor_fuente LIKE 'binance_%%' AND (ticker_motor = %s OR REPLACE(ticker_motor, '-', '') = %s))
                    OR (underlying = %s)
                 ) ORDER BY (motor_fuente LIKE 'bingx_%%') DESC LIMIT 1"""
        cursor.execute(sql, (s_api, s_limpio, und_api, s_api, s_limpio, und_api))
        res = cursor.fetchone()
        if res: return res

        # 2. CAPA DE RESCATE (Para AAPLX, TSLAX, etc.) - Si nada arriba funcionó
        sql_rescate = """SELECT id, ticker_motor, underlying, categoria_producto, tipo_investment 
                         FROM sys_traductor_simbolos 
                         WHERE is_active = 1 AND (motor_fuente LIKE 'bingx_%%' OR motor_fuente LIKE 'binance_%%')
                         AND %s LIKE CONCAT('%%', underlying, '%%')
                         LIMIT 1"""
        cursor.execute(sql_rescate, (s_limpio,))
        return cursor.fetchone()
        
        # BUSQUEDA NIVEL 2: Rescate para CFDs (AAPLX, TSLA, etc.)
        if not res:
            sql_cfd = """SELECT id, ticker_motor, underlying, categoria_producto, tipo_investment 
                         FROM sys_traductor_simbolos 
                         WHERE motor_fuente LIKE 'bingx_%%' 
                         AND %s LIKE CONCAT('%%', underlying, '%%')
                         AND is_active = 1 LIMIT 1"""
            cursor.execute(sql_cfd, (s_limpio,))
            res = cursor.fetchone()
        return res
    
    # --- 1. PROCESAR SALDOS (SPOT + FUTUROS) ---
    s_count_global = 0  # Esta será nuestra cuenta real
    
    # ==========================================================
    # --- 1. SPOT (Lógica v5.5.6) ---
    # ==========================================================
    try:
        res_spot = bx_req("/openApi/spot/v1/account/balance")
        if res_spot.get("data") and "balances" in res_spot['data']:
            for b in res_spot['data']['balances']:
                total = float(b.get('free', 0)) + float(b.get('locked', 0))
                if total <= 0.000001: continue
                ticker = b['asset']
                info = buscar_en_traductor_bingx(ticker)
                registrar_saldo(cursor, uid, info, total, float(b.get('locked', 0)), ticker, "BINGX", "SPOT")
                s_count_global += 1
            print(f"    [OK] BingX Spot procesado.")
    except Exception as e: 
        print(f"    [!] Error crítico en BingX Spot: {e}")

    # ==========================================================
    # --- 2. FUTURES PERPETUAL (Ajustado según Debug Master) ---
    # ==========================================================
    try:
        res_perp = bx_req("/openApi/swap/v2/user/balance")
        # El debug muestra que la estructura es: {"data": {"balance": {...}}}
        data_res = res_perp.get("data", {})
        balance_obj = data_res.get("balance")
        
        s_count_perp = 0
        
        if balance_obj and isinstance(balance_obj, dict):
            ticker = balance_obj.get("asset")
            if ticker:
                total = float(balance_obj.get("balance", 0))
                # En el debug vemos 'freezedMargin', lo usamos para 'locked'
                locked = float(balance_obj.get("freezedMargin", 0))
                
                if total > 0.000001:
                    info = buscar_en_traductor_bingx(ticker)
                    registrar_saldo(cursor, uid, info, total, locked, ticker, "BINGX", "FUTURES")
                    s_count_global += 1
                    s_count_perp += 1
        
        print(f"    [OK] BingX Perpetual procesado: {s_count_perp} activos.")
    except Exception as e: 
        print(f"    [!] Error crítico en BingX Perp: {e}")
                
    # AHORA SÍ: El print con la suma real de ambos
    print(f"    [OK] BingX Saldos actualizado: {s_count_global} activos totales.")

    # --- 2. OPEN ORDERS SPOT ---
    cursor.execute("DELETE FROM sys_open_orders_spot WHERE user_id = %s AND broker_name = 'BINGX'", (uid,))
    res_os = bx_req("/openApi/spot/v1/trade/openOrders")
    orders_s = res_os.get("data", {}).get("orders", [])
    for o in orders_s:
        sym = o.get('symbol')
        info = buscar_en_traductor_bingx(sym)
        if info:
            sql = "INSERT INTO sys_open_orders_spot (id_order_ext, user_id, broker_name, traductor_id, symbol, side, type, price, qty, locked_amount, fecha_utc, estado, last_seen) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())"
            cursor.execute(sql, (str(o['orderId']), uid, "BINGX", info['id'], sym, o['side'], o['type'], float(o['price']), float(o['origQty']), float(o.get('origQuoteOrderQty',0)), datetime.fromtimestamp(o['time']/1000).strftime('%Y-%m-%d %H:%M:%S'), 'ABIERTA'))

    # --- 3. OPEN ORDERS FUTURES (CON REGISTRO DE EMERGENCIA) ---
    cursor.execute("DELETE FROM sys_open_orders WHERE user_id = %s AND broker_name = 'BINGX'", (uid,))
    res_of = bx_req("/openApi/swap/v2/trade/openOrders")
    orders_f = res_of.get("data", {}).get("orders", [])
    
    for o in orders_f:
        sym = o.get('symbol')
        info = buscar_en_traductor_bingx(sym)
        
        # Si info es None, tid será None. 
        # IMPORTANTE: Si tu DB falla por NOT NULL, usaremos 0 o un ID genérico.
        tid = info['id'] if info else None
        
        sql = """INSERT INTO sys_open_orders 
                 (id_order_ext, user_id, broker_name, traductor_id, symbol, side, type, price, qty, locked_amount, fecha_utc, estado, last_seen) 
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())"""
        
        try:
            cursor.execute(sql, (
                str(o['orderId']), uid, "BINGX", tid, sym, o['side'], 'LIMIT', 
                float(o['price']), float(o['origQty']), 0.0, 
                datetime.fromtimestamp(o.get('updateTime', o.get('time', 0))/1000).strftime('%Y-%m-%d %H:%M:%S'), 
                'ABIERTA'
            ))
            if tid:
                print(f"    [OK] Orden Futuros: {sym} (ID:{tid})")
            else:
                print(f"    [AVISO] Orden {sym} registrada SIN traductor (ID=NULL).")
        except mysql.connector.Error as err:
            if err.errno == 1048: # Column cannot be null
                print(f"    [!] Error: La DB no permite ID nulo para {sym}. Saltando...")
            else:
                print(f"    [!] Error Inserción: {err}")

    # --- 4. SINCRONIZACIÓN DE TRADES BINGX (DEBUG MODE) ---
    try:
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINGX", "trades_futures")
        # Forzamos un rango amplio para la prueba
        res_tr = bx_req("/openApi/swap/v2/trade/allOrders", {"startTime": start_ts, "limit": 100})
        
        trades_raw = res_tr.get("data", [])
        
        # --- LÍNEA DE DIAGNÓSTICO ESTRATÉGICA ---
        if trades_raw:
            print(f"    [DEBUG] Muestra del primer trade recibido: {json.dumps(trades_raw[0])}")
        else:
            print(f"    [DEBUG] La API devolvió 'data' vacío. Status Code: {res_tr.get('code')}")
        # ----------------------------------------

        t_count = 0
        if isinstance(trades_raw, list):
            for t in trades_raw:
                # Ampliamos el filtro de status para no perder nada en la prueba
                status = str(t.get('status', '')).upper()
                if status in ['FILLED', 'PARTIALLY_FILLED', '2', 'CLOSED', 'COMPLETED']:
                    sym = t.get('symbol')
                    info = buscar_en_traductor_bingx(sym)
                    
                    t_f = {
                        'orderId': str(t['orderId']),
                        'symbol': sym,
                        'side': t['side'],
                        'price': float(t.get('avgPrice') or t.get('price') or 0),
                        'qty': float(t.get('executedQty', 0)),
                        'quoteQty': float(t.get('cumQuote', 0)),
                        'commission': abs(float(t.get('commission', 0))),
                        'commissionAsset': 'USDT',
                        'fecha_sql': datetime.fromtimestamp(t.get('updateTime', time.time()*1000)/1000).strftime('%Y-%m-%d %H:%M:%S'),
                        'isMaker': False 
                    }
                    
                    if registrar_trade_completo(cursor, uid, t_f, info, "BINGX"):
                        t_count += 1
            
            if res_tr.get("code") == 0:
                actualizar_punto_sincro(cursor, uid, "BINGX", "trades_futures", int(time.time()*1000))
                
        print(f"    [INFO] BingX Trades: {t_count} nuevos procesados.")
    except Exception as e:
        print(f"    [!] Error en Historial Trades BingX: {e}")

# ==========================================================
# 🟨 PROCESADOR BINANCE (CON LOGS ESTRATÉGICOS)
# ==========================================================
def procesar_binance(db, uid, k, s):
    try:
        client = Client(k, s)
        cursor = db.cursor(dictionary=True)
        
        # SALDOS
        acc = client.get_account()
        s_count = 0
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.000001:
                info = obtener_traductor_id(cursor, "binance_spot", b['asset'])
                registrar_saldo(cursor, uid, info, total, float(b['locked']), b['asset'], "BINANCE", "SPOT")
                s_count += 1
        print(f"    [OK] Binance Saldos actualizado: {s_count} activos.")

        # TRADES
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", "trades_spot")
        cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente = 'binance_spot' AND is_active = 1")
        diccionario = cursor.fetchall()
        t_count = 0
        for item in diccionario:
            try:
                raw_trades = client.get_my_trades(symbol=item['ticker_motor'], startTime=start_ts)
                for t in raw_trades:
                    t_f = {'orderId': str(t['orderId']), 'symbol': t['symbol'], 'side': 'BUY' if t['isBuyer'] else 'SELL', 'price': float(t['price']), 'qty': float(t['qty']), 'quoteQty': float(t['quoteQty']), 'commission': float(t['commission']), 'commissionAsset': t['commissionAsset'], 'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')}
                    if registrar_trade_completo(cursor, uid, t_f, item, "BINANCE"): t_count += 1
            except: continue
        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_spot", int(time.time()*1000))
        print(f"    [INFO] Binance Trades: {t_count} nuevos procesados.")

        # OPEN ORDERS
        cursor.execute("DELETE FROM sys_open_orders_spot WHERE user_id = %s AND broker_name = 'BINANCE'", (uid,))
        open_orders = client.get_open_orders()
        for oo in open_orders:
            info = obtener_traductor_id(cursor, "binance_spot", oo['symbol'])
            sql_oo = "INSERT INTO sys_open_orders_spot (id_order_ext, user_id, broker_name, traductor_id, symbol, side, type, price, qty, locked_amount, fecha_utc, estado, last_seen) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())"
            cursor.execute(sql_oo, (str(oo['orderId']), uid, "BINANCE", info['id'] if info else None, oo['symbol'], oo['side'], oo['type'], float(oo['price']), float(oo['origQty']), 0.0, datetime.fromtimestamp(oo['time']/1000).strftime('%Y-%m-%d %H:%M:%S'), 'ABIERTA'))
        print(f"    [OK] Binance Open Orders: {len(open_orders)} registradas.")

    except Exception as e: print(f"    [!] Error Binance User {uid}: {e}")

# ==========================================================
# 🚀 EJECUCIÓN PRINCIPAL
# ==========================================================
def run():
    print(f"💎 MOTOR v6.7.1 - debug bingxSALDOS + TRADES BINANCE-BINGX")
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