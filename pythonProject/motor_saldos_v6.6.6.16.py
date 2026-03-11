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
# 🚩 DISFRAZ BÁSICO BINGX v6.6.6.05
# ==========================================================

def get_headers_bingx(api_key):
    return {
        "X-BX-APIKEY": api_key,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://bingx.com/",
        "Connection": "keep-alive"
    }

# Session persistente (MUY IMPORTANTE)
bingx_session = requests.Session()

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

# ==========================================================
# 🎯 VINCULACIÓN MAESTRA v6.6.7 - ESCENARIO B (POR EXCHANGE)
# ==========================================================
def obtener_traductor_id(cursor, motor_fuente, ticker):
    ticker = ticker.upper().strip()

    # 1️⃣ Búsqueda exacta por motor + ticker
    sql = """
        SELECT id, categoria_producto, tipo_investment
        FROM sys_traductor_simbolos
        WHERE motor_fuente = %s
        AND ticker_motor = %s
        LIMIT 1
    """
    cursor.execute(sql, (motor_fuente, ticker))
    row = cursor.fetchone()
    if row:
        return row

    # 2️⃣ Limpieza de prefijos (LD, STK)
    ticker_limpio = ticker
    if ticker.startswith("LD") and len(ticker) > 2:
        ticker_limpio = ticker[2:]
    elif ticker.startswith("STK") and len(ticker) > 3:
        ticker_limpio = ticker[3:]

    # 3️⃣ Buscar por underlying pero SOLO en el mismo motor
    sql = """
        SELECT id, categoria_producto, tipo_investment
        FROM sys_traductor_simbolos
        WHERE motor_fuente = %s
        AND underlying = %s
        LIMIT 1
    """
    cursor.execute(sql, (motor_fuente, ticker_limpio))
    row = cursor.fetchone()

    return row if row else None

# --- LA FUNCIÓN QUE FALTABA ---
def disparar_radar(cursor, uid, ticker, ctx):
    sql = "INSERT IGNORE INTO sys_simbolos_buscados (user_id, ticker, status, info) VALUES (%s,%s,'pendiente',%s)"
    cursor.execute(sql, (uid, ticker, f"Detectado en {ctx}"))    

def obtener_precio_usd(cursor, tid, asset_name):
    asset_name = asset_name.upper()
    clean_ticker = asset_name.replace("LD", "").replace("STK", "")

    if clean_ticker in ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']:
        return 1.0

    try:
        if tid:
            sql = """
                SELECT price 
                FROM sys_precios_activos 
                WHERE traductor_id = %s 
                ORDER BY last_update DESC 
                LIMIT 1
            """
            cursor.execute(sql, (tid,))
            row = cursor.fetchone()
            if row and row['price'] > 0:
                return float(row['price'])

        # 🔵 Fallback por underlying
        sql_fb = """
            SELECT p.price
            FROM sys_precios_activos p
            JOIN sys_traductor_simbolos t ON p.traductor_id = t.id
            WHERE t.underlying = %s
            AND t.is_active = 1
            ORDER BY p.last_update DESC
            LIMIT 1
        """
        cursor.execute(sql_fb, (clean_ticker,))
        row_fb = cursor.fetchone()
        if row_fb and row_fb['price'] > 0:
            return float(row_fb['price'])

    except Exception as e:
        print(f"[Precio Error {asset_name}]: {e}")

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

def generar_tarea_incorporacion(cursor, uid, broker, symbol, contexto):
    sql = """
        INSERT IGNORE INTO sys_tareas_incorporacion
        (user_id, broker, symbol_detectado, contexto)
        VALUES (%s, %s, %s, %s)
    """
    cursor.execute(sql, (uid, broker, symbol, contexto))


# ==========================================================
# 🕒 GESTIÓN DE TIEMPO
# ==========================================================
def obtener_punto_inicio_sincro(cursor, uid, broker, endpoint):
    sql = "SELECT last_timestamp FROM sys_sync_estado WHERE user_id = %s AND broker = %s AND endpoint = %s LIMIT 1"
    cursor.execute(sql, (uid, broker, endpoint))
    row = cursor.fetchone()
    return int(row['last_timestamp']) if row and row['last_timestamp'] else 1633046400000

def actualizar_punto_sincro(cursor, uid, broker, endpoint, nuevo_ts):
    sql = """
        INSERT INTO sys_sync_estado (user_id, broker, endpoint, last_timestamp, last_update)
        VALUES (%s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE last_timestamp = VALUES(last_timestamp), last_update = NOW()
    """
    cursor.execute(sql, (uid, broker, endpoint, nuevo_ts))

# ==========================================================
# 📉 REGISTRO DE TRADES v6.7.1 (FIX BINGX FUTURES)
# ==========================================================
def registrar_trade (cursor, uid, t_data, info_traductor, broker_nombre):
    try:
        # 1. 🛡️ EXTRACCIÓN Y LÓGICA (Calculamos una sola vez)
        t_id = info_traductor['id'] if info_traductor else None
        cat_prod = info_traductor['categoria_producto'] if info_traductor else 'SPOT'
        tipo_inv = info_traductor['tipo_investment'] if info_traductor else 'CRYPTO'
        motor = info_traductor['motor_fuente'] if info_traductor else broker_nombre.lower()

        if broker_nombre.upper() == "BINGX" and t_data.get("es_futuro"):
            cat_prod = "FUTURES"

        # ID limpio (sin TRD- para mejor indexación)
        id_vinculo = f"{uid}-{t_data['orderId']}"

        # 2. 🌐 GLOBAL (Cambiamos 'exchange' por 'tipo_investment' para dar más info)
        sql_global = """
            INSERT IGNORE INTO transacciones_globales
            (id_externo, user_id, tipo_investment, cuenta_tipo, categoria, asset,
             traductor_id, monto_neto, comision, fecha_utc, broker)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cursor.execute(sql_global, (
            id_vinculo, uid, tipo_inv, cat_prod, 'TRADE', 
            t_data['symbol'], t_id, t_data.get('quoteQty', 0), 
            t_data.get('commission', 0), t_data['fecha_sql'], broker_nombre
        ))

        # 3. 🔍 DETALLE (Quitamos duplicidad de orderId y limpiamos isMaker)
        sql_detalle = """
            INSERT IGNORE INTO detalle_trades (
                user_id, traductor_id, broker, categoria_producto,
                motor_fuente, tipo_investment, id_externo_ref, fecha_utc, 
                symbol, lado, position_side, precio_ejecucion, 
                cantidad_ejecutada, commission, commission_asset,
                quote_qty, pnl_realizado, is_maker, trade_id_externo, raw_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cursor.execute(sql_detalle, (
            uid, t_id, broker_nombre, cat_prod, motor, tipo_inv,
            t_data['orderId'], t_data['fecha_sql'], t_data['symbol'],
            t_data.get('side', 'BUY'), t_data.get('positionSide'),
            t_data.get('price', 0), t_data.get('qty', 0),
            t_data.get('commission', 0), t_data.get('commissionAsset'),
            t_data.get('quoteQty', 0), t_data.get('realizedPnl', 0),
            1 if t_data.get('isMaker') else 0, f"TRD-{t_data['orderId']}",
            json.dumps(t_data)
        ))

        return True

    except Exception as e:
        print(f"❌ Error crítico: {e}")
        return False

# ==========================================================
# 🟦 PROCESADOR BINGX (V6.6.8 - REPARACIÓN FINAL MAPPING)
# ==========================================================
def procesar_bingx(db, uid, ak, as_):
    cursor = db.cursor(dictionary=True)
    print(f"    [DEBUG] Iniciando ciclo completo BingX para User {uid}...")
    
    def bx_req(path, params=None):
        if params is None:
            params = {}

        ts = int(time.time()*1000)
        params["timestamp"] = ts

        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), query.encode(), hashlib.sha256).hexdigest()

        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"

        try:
            r = bingx_session.get(
                url,
                headers=get_headers_bingx(ak),
                timeout=10
            )

            if r.status_code == 429:
                print("    [RATE LIMIT] BingX bloqueando. Esperando 3s...")
                time.sleep(3)

            return r.json()

        except Exception as e:
            print(f"    [BINGX ERROR]: {e}")
            return {}

    # ==========================================================
    # 🎯 BUSQUEDA TRADUCTOR BINGX - ESCENARIO B PURO
    # ==========================================================
    def buscar_en_traductor_bingx(simbolo_api):

        if not simbolo_api:
            return None

        ticker = simbolo_api.upper().strip()
        ticker_limpio = ticker.replace("-", "").replace("/", "").replace("=X", "")
        underlying = ticker_limpio.replace("USDT", "").replace("USDC", "").replace("USD", "")

        # 1️⃣ Exacto en cualquier categoría BingX
        sql = """
            SELECT id, categoria_producto, tipo_investment
            FROM sys_traductor_simbolos
            WHERE motor_fuente LIKE 'bingx_%'
            AND (
                ticker_motor = %s
                OR REPLACE(ticker_motor, '-', '') = %s
                OR underlying = %s
            )
            LIMIT 1
        """
        cursor.execute(sql, (ticker, ticker_limpio, underlying))
        row = cursor.fetchone()
        if row:
            return row

        # 2️⃣ Rescate por coincidencia parcial de underlying
        sql = """
            SELECT id, categoria_producto, tipo_investment
            FROM sys_traductor_simbolos
            WHERE motor_fuente LIKE 'bingx_%'
            AND %s LIKE CONCAT('%%', underlying, '%%')
            LIMIT 1
        """
        cursor.execute(sql, (ticker_limpio,))
        return cursor.fetchone()

        # 2. CAPA DE RESCATE (Para AAPLX, TSLAX, etc.) - Si nada arriba funcionó
        sql_rescate = """SELECT id, ticker_motor, underlying, categoria_producto, tipo_investment 
                         FROM sys_traductor_simbolos 
                         WHERE (motor_fuente LIKE 'bingx_%%' OR motor_fuente LIKE 'binance_%%')
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
                         LIMIT 1"""
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
                generar_tarea_incorporacion(cursor, uid, "BINGX", sym, "OPEN_ORDER_FUTURES")
        except mysql.connector.Error as err:
            if err.errno == 1048: # Column cannot be null
                print(f"    [!] Error: La DB no permite ID nulo para {sym}. Saltando...")
            else:
                print(f"    [!] Error Inserción: {err}")

    # --- 4. SINCRONIZACIÓN DE TRADES BINGX (Lógica Robusta) ---
    try:
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINGX", "trades_futures")
        res_tr = bx_req("/openApi/swap/v2/trade/allOrders", {"startTime": start_ts})
        
        t_count = 0
        # Aseguramos que data sea una lista antes de iterar
        trades_raw = res_tr.get("data", [])
        if isinstance(trades_raw, list):
            for t in trades_raw:
                # Procesamos órdenes que hayan tenido ejecución (FILLED / PARTIALLY_FILLED)
                status = str(t.get('status', '')).upper()
                if status in ['FILLED', 'PARTIALLY_FILLED']:
                    sym = t.get('symbol')
                    info = buscar_en_traductor_bingx(sym)

                    # 🔥 SI NO EXISTE EN TRADUCTOR → GENERAR TAREA
                    if not info:
                        print(f"    [AVISO] Trade {sym} SIN traductor → Generando tarea.")
                        generar_tarea_incorporacion(cursor, uid, "BINGX", sym, "TRADE_FUTURES")
                    # Mapeo de campos según la API de BingX Perpetual
                    t_f = {
                        'orderId': str(t['orderId']),
                        'symbol': sym,
                        'side': t['side'],
                        'price': float(t.get('avgPrice') or t.get('price') or 0),
                        'qty': float(t.get('executedQty', 0)),
                        'quoteQty': float(t.get('cumQuote', 0)),
                        'commission': abs(float(t.get('commission', 0))), # Usamos valor absoluto
                        'commissionAsset': 'USDT',
                        'fecha_sql': datetime.fromtimestamp(t.get('updateTime', time.time()*1000)/1000).strftime('%Y-%m-%d %H:%M:%S'),
                        'isMaker': False 
                    }
                    
                    t_f['es_futuro'] = True
                    
                    if registrar_trade(cursor, uid, t_f, info, "BINGX"):
                        t_count += 1
            
            # Solo actualizamos el timestamp si la respuesta fue exitosa
            if res_tr.get("code") == 0:
                actualizar_punto_sincro(cursor, uid, "BINGX", "trades_futures", int(time.time()*1000))
                
        print(f"    [INFO] BingX Trades: {t_count} nuevos procesados.")
    except Exception as e:
        print(f"    [!] Error en Historial Trades BingX: {e}")

# ==========================================================
# 🔎 BUSCADOR TRADUCTOR BINGX (GENÉRICO)
# ==========================================================
def buscar_en_traductor_bingx_position(cursor, symbol):

    symbol_clean = symbol.replace("-", "").upper()

    cursor.execute("""
        SELECT id, underlying
        FROM sys_traductor_simbolos
        WHERE motor_fuente LIKE 'bingx_%'
          AND REPLACE(ticker_motor, '-', '') = %s
        LIMIT 1
    """, (symbol_clean,))

    return cursor.fetchone()

# ==========================================================
# 🟣 BINGX FUTURES - POSITIONS
# Version 1.0
# ==========================================================
def procesar_bingx_positions(db, uid, ak, as_):

    cursor = db.cursor(dictionary=True)

    def bx_req(path, params=None):
        if params is None:
            params = {}

        ts = int(time.time() * 1000)
        params["timestamp"] = ts

        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), query.encode(), hashlib.sha256).hexdigest()

        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"

        r = bingx_session.get(
            url,
            headers=get_headers_bingx(ak),
            timeout=10
        )

        return r.json()

    try:

        res = bx_req("/openApi/swap/v2/user/positions")

        cursor.execute("""
            DELETE FROM sys_positions 
            WHERE user_id = %s 
            AND broker_name = 'BINGX'
        """, (uid,))

        posiciones = res.get("data", [])

        for p in posiciones:

            position_amt = float(p.get("positionAmt", 0))

            if abs(position_amt) > 0:

                symbol = p.get("symbol")

                info = buscar_en_traductor_bingx_position(cursor, symbol)

                cursor.execute("""
                    INSERT INTO sys_positions (
                        user_id, broker_name, traductor_id, symbol,
                        position_side, position_amt,
                        entry_price, mark_price,
                        unrealized_profit,
                        position_initial_margin,
                        maint_margin,
                        leverage, margin_type,
                        last_update
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                """, (
                    uid,
                    "BINGX",
                    info["id"] if info else None,
                    symbol,
                    "LONG" if position_amt > 0 else "SHORT",
                    position_amt,
                    float(p.get("avgPrice", 0)),
                    float(p.get("markPrice", 0)),
                    float(p.get("unrealizedProfit", 0)),
                    float(p.get("positionMargin", 0)),
                    float(p.get("maintMargin", 0)),
                    int(p.get("leverage", 0)),
                    p.get("marginType", "cross")
                ))

        print("    [BINGX] Positions actualizadas.")

    except Exception as e:
        print(f"    [BINGX POS ERROR] {e}")



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
        cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente = 'binance_spot'")
        diccionario = cursor.fetchall()
        t_count = 0
        for item in diccionario:
            try:
                raw_trades = client.get_my_trades(symbol=item['ticker_motor'], startTime=start_ts)
                for t in raw_trades:
                    t_f = {'orderId': str(t['orderId']), 'symbol': t['symbol'], 'side': 'BUY' if t['isBuyer'] else 'SELL', 'price': float(t['price']), 'qty': float(t['qty']), 'quoteQty': float(t['quoteQty']), 'commission': float(t['commission']), 'commissionAsset': t['commissionAsset'], 'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')}
                    if registrar_trade(cursor, uid, t_f, item, "BINANCE"): t_count += 1
            except: continue
        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_spot", int(time.time()*1000))
        print(f"    [INFO] Binance Trades: {t_count} nuevos procesados.")

        # OPEN ORDERS SPOT
        cursor.execute("DELETE FROM sys_open_orders_spot WHERE user_id = %s AND broker_name = 'BINANCE'", (uid,))
        open_orders = client.get_open_orders()
        for oo in open_orders:
            info = obtener_traductor_id(cursor, "binance_spot", oo['symbol'])
            sql_oo = "INSERT INTO sys_open_orders_spot (id_order_ext, user_id, broker_name, traductor_id, symbol, side, type, price, qty, locked_amount, fecha_utc, estado, last_seen) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())"
            cursor.execute(sql_oo, (str(oo['orderId']), uid, "BINANCE", info['id'] if info else None, oo['symbol'], oo['side'], oo['type'], float(oo['price']), float(oo['origQty']), 0.0, datetime.fromtimestamp(oo['time']/1000).strftime('%Y-%m-%d %H:%M:%S'), 'ABIERTA'))
        print(f"    [OK] Binance Open Orders: {len(open_orders)} registradas.")

    except Exception as e: print(f"    [!] Error Binance User {uid}: {e}")

# ==========================================================
# 🟦 PROCESADOR BINANCE UM FUTURES (USDT-M)
# v6.6.7.01 - SOLO FUTURES UM
# ==========================================================
def procesar_binance_um_futures(db, uid, k, s):

    try:
        client = UMFutures(key=k, secret=s)
        cursor = db.cursor(dictionary=True)

        print(f"    [UM] Iniciando Binance UM Futures...")

        # ======================================================
        # 1️⃣ SALDOS FUTURES
        # ======================================================
        acc = client.balance()
        s_count = 0

        for b in acc:
            total = float(b.get('balance', 0))
            locked = float(b.get('crossWalletBalance', 0)) - total

            if total > 0.000001:
                info = obtener_traductor_id(cursor, "binance_usdt_future", b['asset'])
                registrar_saldo(cursor, uid, info, total, abs(locked), b['asset'], "BINANCE", "FUTURES")
                s_count += 1

        print(f"    [UM] Saldos Futures: {s_count} activos.")

        # ======================================================
        # 2️⃣ TRADES HISTÓRICOS POR TICKER
        # ======================================================
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", "trades_um_futures")

        cursor.execute("""
            SELECT id, ticker_motor, categoria_producto, tipo_investment
            FROM sys_traductor_simbolos
            WHERE motor_fuente = 'binance_usdt_future'
        """)

        diccionario = cursor.fetchall()
        t_count = 0

        for item in diccionario:
            symbol = item['ticker_motor']

            try:
                trades = client.get_account_trades(symbol=symbol, startTime=start_ts)
                print(f"        [UM DEBUG] {symbol} -> {len(trades)} trades encontrados")
                for t in trades:
                    print(f"            Trade detectado: {t['orderId']} | {symbol}")    

                    t_f = {
                        'orderId': str(t['orderId']),
                        'symbol': t['symbol'],
                        'side': t['side'],
                        'positionSide': t.get('positionSide'),
                        'price': float(t.get('price', 0)),
                        'qty': float(t.get('qty', 0)),
                        'quoteQty': float(t.get('quoteQty', 0)),
                        'commission': float(t.get('commission', 0)),
                        'commissionAsset': t.get('commissionAsset'),
                        'realizedPnl': float(t.get('realizedPnl', 0)),
                        'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S'),
                        'isMaker': t.get('maker', False),
                        'es_futuro': True
                    }

                    if registrar_trade(cursor, uid, t_f, item, "BINANCE"):
                        t_count += 1

            except Exception as e:
                print(f"        [UM WARN] {symbol}: {e}")
                continue

        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_um_futures", int(time.time()*1000))

        print(f"    [UM] Trades Futures nuevos: {t_count}")


        # ======================================================
        # 3️⃣ OPEN ORDERS FUTURES (UM) - VERSION CORRECTA
        # ======================================================

        cursor.execute("""
            SELECT ticker_motor
            FROM sys_traductor_simbolos
            WHERE motor_fuente = 'binance_usdt_future'
        """)

        simbolos = cursor.fetchall()

        cursor.execute("""
            DELETE FROM sys_open_orders 
            WHERE user_id = %s 
            AND broker_name = 'BINANCE_UM'
        """, (uid,))

        oo_count = 0

        for row in simbolos:

            symbol = row['ticker_motor']

            try:
                orders = client.get_orders(symbol=symbol)

                abiertas = [
                    o for o in orders
                    if o["status"] in ["NEW", "PARTIALLY_FILLED"]
                ]

                for oo in abiertas:

                    info = obtener_traductor_id(cursor, "binance_usdt_future", symbol)

                    sql = """
                        INSERT INTO sys_open_orders
                        (id_order_ext, user_id, broker_name, traductor_id,
                         symbol, side, type, price, qty,
                         locked_amount, fecha_utc, estado, last_seen)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    """

                    cursor.execute(sql, (
                        str(oo['orderId']),
                        uid,
                        "BINANCE_UM",
                        info['id'] if info else None,
                        symbol,
                        oo['side'],
                        oo['type'],
                        float(oo.get('price', 0)),
                        float(oo.get('origQty', 0)),
                        0.0,
                        datetime.fromtimestamp(
                            oo['time']/1000
                        ).strftime('%Y-%m-%d %H:%M:%S'),
                        'ABIERTA'
                    ))

                    oo_count += 1

            except Exception as e:
                print(f"[UM OPEN ERROR {symbol}] {e}")

        print(f"    [UM] Open Orders: {oo_count}")        

    except Exception as e:
        print(f"    [UM CRITICAL ERROR] {e}")

# ==========================================================
# 🟧 PROCESADOR BINANCE CM FUTURES (COIN-M)
# v6.6.7.02
# ==========================================================
def procesar_binance_cm_futures(db, uid, k, s):

    try:
        client = CMFutures(key=k, secret=s)
        cursor = db.cursor(dictionary=True)

        print(f"    [CM] Iniciando Binance CM Futures...")

        # ======================================================
        # 1️⃣ SALDOS
        # ======================================================
        acc = client.balance()
        s_count = 0

        for b in acc:
            total = float(b.get('balance', 0))

            if total > 0.000001:
                info = obtener_traductor_id(cursor, "binance_coin_future", b['asset'])
                registrar_saldo(cursor, uid, info, total, 0.0, b['asset'], "BINANCE", "FUTURES")
                s_count += 1

        print(f"    [CM] Saldos: {s_count}")


        # ======================================================
        # 2️⃣ TRADES HISTÓRICOS CM
        # ======================================================
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", "trades_cm_futures")

        cursor.execute("""
            SELECT id, ticker_motor, categoria_producto, tipo_investment
            FROM sys_traductor_simbolos
            WHERE motor_fuente = 'binance_coin_future'
        """)

        diccionario = cursor.fetchall()
        t_count = 0

        for item in diccionario:

            symbol = item['ticker_motor']

            try:
                trades = client.get_account_trades(symbol=symbol, startTime=start_ts)
                print(f"        [CM DEBUG] {symbol} -> {len(trades)} trades encontrados")

                for t in trades:
                    print(f"            Trade detectado: {t['orderId']} | {symbol}")

                    t_f = {
                        'orderId': str(t['orderId']),
                        'symbol': t['symbol'],
                        'side': t['side'],
                        'positionSide': t.get('positionSide'),
                        'price': float(t.get('price', 0)),
                        'qty': float(t.get('qty', 0)),
                        'quoteQty': float(t.get('quoteQty', 0)),
                        'commission': float(t.get('commission', 0)),
                        'commissionAsset': t.get('commissionAsset'),
                        'realizedPnl': float(t.get('realizedPnl', 0)),
                        'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S'),
                        'isMaker': t.get('maker', False),
                        'es_futuro': True
                    }

                    if registrar_trade(cursor, uid, t_f, item, "BINANCE"):
                        t_count += 1

            except Exception as e:
                print(f"        [CM WARN] {symbol}: {e}")
                continue

        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_cm_futures", int(time.time()*1000))

        print(f"    [CM] Trades Futures nuevos: {t_count}")


        # ======================================================
        # 3️⃣ OPEN ORDERS CM - VERSION CORRECTA
        # ======================================================

        cursor.execute("""
            SELECT ticker_motor
            FROM sys_traductor_simbolos
            WHERE motor_fuente = 'binance_coin_future'
        """)

        simbolos = cursor.fetchall()

        cursor.execute("""
            DELETE FROM sys_open_orders 
            WHERE user_id = %s 
            AND broker_name = 'BINANCE_CM'
        """, (uid,))

        oo_count = 0

        for row in simbolos:

            symbol = row['ticker_motor']

            try:
                orders = client.get_orders(symbol=symbol)

                abiertas = [
                    o for o in orders
                    if o["status"] in ["NEW", "PARTIALLY_FILLED"]
                ]

                for oo in abiertas:

                    info = obtener_traductor_id(cursor, "binance_coin_future", symbol)

                    sql = """
                        INSERT INTO sys_open_orders
                        (id_order_ext, user_id, broker_name, traductor_id,
                         symbol, side, type, price, qty,
                         locked_amount, fecha_utc, estado, last_seen)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    """

                    cursor.execute(sql, (
                        str(oo['orderId']),
                        uid,
                        "BINANCE_CM",
                        info['id'] if info else None,
                        symbol,
                        oo['side'],
                        oo['type'],
                        float(oo.get('price', 0)),
                        float(oo.get('origQty', 0)),
                        0.0,
                        datetime.fromtimestamp(
                            oo['time']/1000
                        ).strftime('%Y-%m-%d %H:%M:%S'),
                        'ABIERTA'
                    ))

                    oo_count += 1

            except Exception as e:
                print(f"[CM OPEN ERROR {symbol}] {e}")

        print(f"    [CM] Open Orders: {oo_count}")

    except Exception as e:
        print(f"    [CM CRITICAL ERROR] {e}")

# ==========================================================
# 🟦 BINANCE UM FUTURES - POSITIONS
# Version 1.1 (FIX SDK)
# ==========================================================
def procesar_binance_um_positions(db, uid, k, s):

    from binance.um_futures import UMFutures
    client = UMFutures(key=k, secret=s)
    cursor = db.cursor(dictionary=True)

    try:
        posiciones = client.get_position_risk()

        cursor.execute("""
            DELETE FROM sys_positions 
            WHERE user_id = %s 
            AND broker_name = 'BINANCE_UM'
        """, (uid,))

        for p in posiciones:

            position_amt = float(p.get("positionAmt", 0))

            if abs(position_amt) > 0:

                info = obtener_traductor_id(
                    cursor, 
                    "binance_usdt_future", 
                    p["symbol"]
                )

                cursor.execute("""
                    INSERT INTO sys_positions (
                        user_id, broker_name, traductor_id, symbol,
                        position_side, position_amt,
                        entry_price, mark_price,
                        unrealized_profit,
                        position_initial_margin,
                        maint_margin,
                        leverage, margin_type,
                        last_update
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                """, (
                    uid,
                    "BINANCE_UM",
                    info["id"] if info else None,
                    p["symbol"],
                    "LONG" if position_amt > 0 else "SHORT",
                    position_amt,
                    float(p.get("entryPrice", 0)),
                    float(p.get("markPrice", 0)),
                    float(p.get("unRealizedProfit", 0)),
                    float(p.get("positionInitialMargin", 0)),
                    float(p.get("maintMargin", 0)),
                    int(p.get("leverage", 0)),
                    p.get("marginType", "cross")
                ))

        print("    [UM] Positions actualizadas.")

    except Exception as e:
        print(f"    [UM POS ERROR] {e}")

# ==========================================================
# 🟧 BINANCE CM FUTURES - POSITIONS
# Version 1.1 (FIX SDK)
# ==========================================================
def procesar_binance_cm_positions(db, uid, k, s):

    from binance.cm_futures import CMFutures
    client = CMFutures(key=k, secret=s)
    cursor = db.cursor(dictionary=True)

    try:
        posiciones = client.get_position_risk()

        cursor.execute("""
            DELETE FROM sys_positions 
            WHERE user_id = %s 
            AND broker_name = 'BINANCE_CM'
        """, (uid,))

        for p in posiciones:

            position_amt = float(p.get("positionAmt", 0))

            if abs(position_amt) > 0:

                info = obtener_traductor_id(
                    cursor, 
                    "binance_coin_future", 
                    p["symbol"]
                )

                cursor.execute("""
                    INSERT INTO sys_positions (
                        user_id, broker_name, traductor_id, symbol,
                        position_side, position_amt,
                        entry_price, mark_price,
                        unrealized_profit,
                        position_initial_margin,
                        maint_margin,
                        leverage, margin_type,
                        last_update
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                """, (
                    uid,
                    "BINANCE_CM",
                    info["id"] if info else None,
                    p["symbol"],
                    "LONG" if position_amt > 0 else "SHORT",
                    position_amt,
                    float(p.get("entryPrice", 0)),
                    float(p.get("markPrice", 0)),
                    float(p.get("unRealizedProfit", 0)),
                    float(p.get("positionInitialMargin", 0)),
                    float(p.get("maintMargin", 0)),
                    int(p.get("leverage", 0)),
                    p.get("marginType", "cross")
                ))

        print("    [CM] Positions actualizadas.")

    except Exception as e:
        print(f"    [CM POS ERROR] {e}")



# ==========================================================
# 🚀 EJECUCIÓN PRINCIPAL
# ==========================================================
def run():
    print(f"💎 MOTOR v6.6.6.16 - SALDOS + TRADES + OPEN ORDERS + POSITION BINANCE-BINGX")
    while True:
        print(f"\n{'='*65}\n🔄 INICIO CICLO: {datetime.now().strftime('%H:%M:%S')}\n{'='*65}")
        db = None
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1")


            for u in cursor.fetchall():
                print(f">> TRABAJANDO: User {u['user_id']} | {u['broker_name']}")

                k = descifrar_dato(u['api_key'], MASTER_KEY)
                s = descifrar_dato(u['api_secret'], MASTER_KEY)

                if u['broker_name'].upper() == "BINANCE":

                    print("        >>> SPOT BINANCE <<<")
                    procesar_binance(db, u['user_id'], k, s)

                    print("        >>> UM FUTURES BINANCE <<<")
                    procesar_binance_um_futures(db, u['user_id'], k, s)

                    print("        >>> CM FUTURES BINANCE <<<")
                    procesar_binance_cm_futures(db, u['user_id'], k, s)

                    print("        >>> UM FUTURES BINANCE POSITION <<<")
                    procesar_binance_um_positions(db, u['user_id'], k, s)

                    print("        >>> CM FUTURES BINANCE POSITION <<<")
                    procesar_binance_cm_positions(db, u['user_id'], k, s)



                elif u['broker_name'].upper() == "BINGX":

                    print("        >>> BINGX <<<")
                    procesar_bingx(db, u['user_id'], k, s)

                    print("        >>> BINGX  POSITION  <<<")
                    procesar_bingx_positions(db, u['user_id'], k, s)

                db.commit()

        except Exception as e: print(f"    [CRITICAL] {e}")
        finally:
            if db and db.is_connected(): db.close()
        print(f"\n{'='*65}\n✅ CICLO TERMINADO - ESPERANDO 5 MIN\n{'='*65}")
        time.sleep(300)

if __name__ == "__main__": run()