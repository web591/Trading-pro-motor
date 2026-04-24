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
import sys
import socket
import json

def verificar_y_obtener_traductor(cursor, uid, trade_id, symbol, motor_fuente):
    """
    Verifica si el trade ya existe. Si no, busca el traductor.
    Retorna el 'item' del traductor o None si debemos saltarlo.
    """
    # 1. Validación de Existencia (Regla de Oro 2)
    cursor.execute("SELECT id FROM sys_detalle_trades WHERE trade_id_externo=%s AND user_id=%s AND broker='BINANCE'", (str(trade_id), uid))
    if cursor.fetchone():
        print(f"        \033[93m[SKIP]\033[0m Trade {trade_id} ya existe en DB. Saltando...")
        return None

    # 2. Obtener el Traductor ID
    cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE ticker_motor=%s AND motor_fuente=%s", (symbol, motor_fuente))
    item = cursor.fetchone()
    if not item:
        print(f"        \033[91m[!] Símbolo {symbol} no encontrado en {motor_fuente}\033[0m")
    
    return item

def obtener_lock(cursor, lock_name, timeout=600):
    # Identificamos si es GitHub o Local para el log de la DB
    host = "GITHUB_ACTION" if os.getenv('GITHUB_ACTIONS') == 'true' else socket.gethostname()

    # Limpiamos locks viejos (10 min) para evitar bloqueos por cortes de luz o crashes
    cursor.execute(f"""
        DELETE FROM sys_locks 
        WHERE lock_name = %s 
        AND lock_time < NOW() - INTERVAL %s SECOND
    """, (lock_name, timeout))

    try:
        # Intentamos insertar el nuevo lock
        cursor.execute("""
            INSERT INTO sys_locks (lock_name, locked_by, lock_time) 
            VALUES (%s, %s, NOW())
        """, (lock_name, host))
        return True
    except:
        # Si falla el INSERT es porque el lock ya existe y está vigente
        return False

def liberar_lock(cursor, lock_name):
    try:
        cursor.execute("DELETE FROM sys_locks WHERE lock_name = %s", (lock_name,))
    except Exception as e:
        print(f"⚠️ Error al liberar lock: {e}")

# Forzar a que los prints salgan rápido en GitHub
sys.stdout.reconfigure(line_buffering=True)

def check_db_connection(db):
    """Revisa si la conexión sigue viva, si no, intenta reconectar."""
    try:
        db.ping(reconnect=True, attempts=3, delay=2)
    except:
        try:
            import config
            return mysql.connector.connect(**config.DB_CONFIG)
        except:
            return None
    return db
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
# 🎯 VINCULACIÓN MAESTRA v6.6.7 - UNIVERSAL (Binance & BingX)
# ==========================================================
def obtener_traductor_id_universal(cursor, motor_fuente, ticker_api):
    """
    v6.6.7 - Rescata IDs sin importar si el motor_fuente exacto cambió.
    Funciona para Cripto, Forex, Stocks, Indices.
    """
    if not ticker_api: return None
    
    # 1. Normalización inicial
    ticker = str(ticker_api).upper().strip()
    motor_fuente = motor_fuente.lower()

    # ---------------------------------------------------------
    # PASO 1: BÚSQUEDA EXACTA (Fidelidad 1:1)
    # ---------------------------------------------------------
    sql_exacto = """
        SELECT id, categoria_producto, tipo_investment, underlying, quote_asset 
        FROM sys_traductor_simbolos 
        WHERE motor_fuente = %s AND ticker_motor = %s
        LIMIT 1
    """
    cursor.execute(sql_exacto, (motor_fuente, ticker))
    res = cursor.fetchone()
    # Si cursor es dictionary=True devolverá dict, si no, tupla. 
    # Mantenemos la compatibilidad:
    if res: return res

    # ---------------------------------------------------------
    # PASO 2: LIMPIADOR UNIVERSAL (Rescate Elástico)
    # ---------------------------------------------------------
    ticker_limpio = ticker.replace("-", "").replace("/", "").replace("=X", "").replace("^", "")
    u_search = ticker_limpio

    if "bingx" in motor_fuente:
        for basura in ["NCSK", "2USD", "USDT", "USDC", "USD", "_PERP"]:
            u_search = u_search.replace(basura, "")
            
    elif "binance" in motor_fuente:
        for basura in ["LD", "STK", "USDT", "USDC"]:
            u_search = u_search.replace(basura, "")

    if not u_search: u_search = ticker_limpio

    # SQL de Rescate: Busca por ticker_motor sin guiones o por el underlying
    # El LIKE asegura que si buscas en 'bingx_perpetual' encuentre el ID en 'bingx_commodity'
    sql_rescate = """
        SELECT id, categoria_producto, tipo_investment, underlying, quote_asset 
        FROM sys_traductor_simbolos 
        WHERE motor_fuente LIKE %s 
        AND (REPLACE(ticker_motor, '-', '') = %s OR underlying = %s)
        LIMIT 1
    """
    
    # Extrae el nombre del broker (binance o bingx) y añade el comodín para el LIKE
    broker_base = motor_fuente.split('_')[0]
    motor_patron = f"{broker_base}%"
    
    cursor.execute(sql_rescate, (motor_patron, ticker_limpio, u_search))
    res_rescate = cursor.fetchone()
    
    return res_rescate # Retorna el resultado o None si no hubo match

    # Si llegamos aquí, el activo realmente no existe en el traductor
    return None

# --- LA FUNCIÓN QUE FALTABA ---
def disparar_radar(cursor, uid, ticker, ctx):
    sql = "INSERT IGNORE INTO sys_simbolos_buscados (user_id, ticker, status, info) VALUES (%s,%s,'pendiente',%s)"
    cursor.execute(sql, (uid, ticker, f"Detectado en {ctx}"))    

def obtener_precio_usd(cursor, tid, asset_name):
    """Retorna precio y asegura que las stables tengan valor 1.0"""
    try:
        asset_name = asset_name.upper()
        clean_ticker = asset_name.replace("LD", "").replace("STK", "").strip()

        # 1. Stables directas
        if clean_ticker in ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']:
            return 1.0

        # 2. Precio por Traductor ID
        if tid:
            sql = "SELECT price FROM sys_precios_activos WHERE traductor_id = %s ORDER BY last_update DESC LIMIT 1"
            cursor.execute(sql, (tid,))
            row = cursor.fetchone()
            if row and row['price'] > 0: 
                return float(row['price'])

        # 3. FALLBACK por Underlying (Importante para activos LD o ahorros)
        sql_fb = """
            SELECT p.price
            FROM sys_precios_activos p
            JOIN sys_traductor_simbolos t ON p.traductor_id = t.id
            WHERE t.underlying = %s AND t.is_active = 1
            ORDER BY p.last_update DESC LIMIT 1
        """
        cursor.execute(sql_fb, (clean_ticker,))
        row_fb = cursor.fetchone()
        if row_fb and row_fb['price'] > 0:
            return float(row_fb['price'])

    except Exception as e:
        print(f"[Precio Error {asset_name}]: {e}")
    return 0.0

# ==========================================================
# FINANCIAL TRADING MODULE - NORMALIZADOR DE COMISIONES
# Version: 1.0 (Opción A - Sweeper Continuo)
# ==========================================================
def normalizar_comisiones_pendientes(db, user_id):
    """
    Busca trades con comisión en 0 USD y los repara con un límite de seguridad.
    """
    db = check_db_connection(db)
    cursor = db.cursor(dictionary=True)
    try:
        sql_pendientes = """
            SELECT id_detalle, commission, commission_asset, quote_qty 
            FROM detalle_trades 
            WHERE user_id = %s 
              AND commission > 0 
              AND (commission_usd IS NULL OR commission_usd = 0)
        """
        cursor.execute(sql_pendientes, (user_id,))
        trades = cursor.fetchall()

        if not trades:
            return 

        stables = ['USDT', 'USDC', 'FDUSD', 'BUSD', 'DAI', 'PYUSD']

        for trade in trades:
            # 1. 🧹 LIMPIEZA: Quitamos comillas o espacios que puedan venir del exchange
            asset_raw = trade['commission_asset']
            if not asset_raw: continue
            asset = asset_raw.strip().replace('"', '').upper()

            precio_actual = 0.0

            # 2. LÓGICA DE PRECIO
            if asset in stables:
                precio_actual = 1.0
            else:
                sql_tid = """
                    SELECT id FROM sys_traductor_simbolos 
                    WHERE underlying = %s AND quote_asset = 'USDT' AND motor_fuente = 'binance_spot' 
                    LIMIT 1
                """
                cursor.execute(sql_tid, (asset,))
                res_tid = cursor.fetchone()

                if res_tid:
                    # Aquí es donde obtenemos el precio (BNB, BTC, etc)
                    precio_actual = obtener_precio_usd(cursor, res_tid['id'], asset)

            # 3. 🛡️ CÁLCULO CON ESCUDO DE SEGURIDAD
            if precio_actual and precio_actual > 0:
                com_nominal = float(trade['commission'])
                com_usd = com_nominal * float(precio_actual)
                
                # --- EL BLOQUE DE SEGURIDAD ---
                # Si la comisión calculada es mayor a $15 USD O es igual al monto del trade (error espejo), 
                # la bloqueamos para no ensuciar el dashboard.
                monto_trade = float(trade['quote_qty'])
                
                es_error_espejo = abs(com_nominal - monto_trade) < 0.00001
                es_valor_absurdo = com_usd > 15.0 # Ajusta este valor si haces trades institucionales muy grandes

                if es_error_espejo or es_valor_absurdo:
                    print(f"⚠️ BLOQUEO: ID {trade['id_detalle']} - Com: {com_nominal} {asset} calculaba ${com_usd:.2f}")
                    # Lo marcamos con un valor mínimo o 0 para que no vuelva a entrar al ciclo
                    cursor.execute("UPDATE detalle_trades SET commission_usd = 0.00000001 WHERE id_detalle = %s", (trade['id_detalle'],))
                else:
                    # 4. ACTUALIZACIÓN NORMAL
                    sql_update = "UPDATE detalle_trades SET commission_usd = %s, commission_asset = %s WHERE id_detalle = %s"
                    cursor.execute(sql_update, (com_usd, asset, trade['id_detalle']))
                    print(f"    [+] Comisión Normalizada: {asset} -> ${com_usd:.4f} USD")

        db.commit()
    except Exception as e:
        print(f"    [!] Error en Sweeper de Comisiones: {e}")
    finally:
        cursor.close()
# ==========================================================

def registrar_saldo(cursor, uid, info_traductor, total, locked, asset, broker, tipo_cuenta):
    tid = info_traductor['id'] if info_traductor else None
    precio = obtener_precio_usd(cursor, tid, asset)
    valor_usd = total * precio
    
    # Eliminada la columna 'status' que daba error 1054
    sql = """
        INSERT INTO sys_saldos_usuarios 
        (user_id, broker_name, asset, traductor_id, cantidad_total, cantidad_disponible, cantidad_bloqueada, valor_usd, precio_referencia, tipo_cuenta, last_update) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    """
    cursor.execute(sql, (uid, broker, asset, tid, total, total-locked, locked, valor_usd, precio, tipo_cuenta))

def generar_tarea_incorporacion(cursor, user_id, broker, ticker, contexto):
    try:
        # CORRECCIÓN: Usamos 'symbol_detectado' que es el nombre real de tu columna
        sql = """
            INSERT INTO sys_tareas_incorporacion 
            (user_id, broker, symbol_detectado, contexto, status, fecha_detectado)
            VALUES (%s, %s, %s, %s, 'PENDIENTE', NOW())
            ON DUPLICATE KEY UPDATE fecha_detectado = NOW()
        """
        cursor.execute(sql, (user_id, broker, ticker, contexto))
    except Exception as e:
        print(f"    [ERROR TAREAS] No se pudo crear tarea: {e}")

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
# 📉 REGISTRO DE TRADES v6.7.0 (AJUSTADO A TABLA REAL)
# ==========================================================
def registrar_trade(cursor, uid, t_data, info_traductor, broker_nombre):
    try:
        # --- BLOQUE 1: LIMPIEZA DE IDENTIFICADORES ---
        # Usamos id_externo_ref para la clave de integridad del sistema
        trade_id = str(t_data.get('tradeId', ''))
        order_id = str(t_data.get('orderId', '')) if t_data.get('orderId') else None
        id_externo_ref = f"{uid}-{trade_id}" 
        
        # --- BLOQUE 2: INGESTA PURA DE SÍMBOLOS ---
        symbol_puro = t_data.get('symbol', 'UNKNOWN')
        t_id = info_traductor['id'] if info_traductor else None
        cat_prod = info_traductor['categoria_producto'] if info_traductor else 'SPOT'
        tipo_inv = info_traductor['tipo_investment'] if info_traductor else 'CRYPTO'
        motor_fte = info_traductor['motor_fuente'] if info_traductor else f"{broker_nombre.lower()}_auto"

        # --- BLOQUE 3: COMISIONES INTELIGENTES ---
        com_asset = t_data.get('commissionAsset')
        com_nominal = float(t_data.get('commission', 0))
        com_usd = 0.0
        if com_nominal > 0 and com_asset:
            # Esta función debe existir en tu motor para el cálculo de USD
            precio_com = obtener_precio_usd(cursor, None, com_asset)
            com_usd = com_nominal * precio_com
            
        # --- BLOQUE 4: SEGURO DE VIDA (RAW JSON) ---
        # Guardamos el dump original por si hay que auditar
        raw_json = json.dumps(t_data)
        
        is_maker = 1 if t_data.get('isMaker', False) else 0
        reduce_only = 1 if t_data.get('reduceOnly', False) else 0
        pnl_realizado = float(t_data.get('realizedPnl', 0))
        position_side = t_data.get('positionSide') 
        
        # --- PREPARACIÓN SQL (ESTRICTA CON TU SCHEMA) ---
        # Eliminada columna 'es_futuro' que no existe en tu DB
        # Las columnas coinciden con el SHOW COLUMNS que enviaste
        sql = """
            INSERT IGNORE INTO detalle_trades (
                user_id, traductor_id, broker, categoria_producto, motor_fuente, 
                tipo_investment, id_externo_ref, fecha_utc, symbol, lado, 
                position_side, reduce_only, precio_ejecucion, cantidad_ejecutada, 
                commission, commission_asset, commission_usd, quote_qty, 
                pnl_realizado, is_maker, trade_id_externo, order_id_externo, 
                raw_json, revisado
            ) VALUES (
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, 
                %s, %s, %s, %s, 
                %s, %s, %s, %s, 
                %s, 0
            )
        """
        
        valores = (
            uid, t_id, broker_nombre, cat_prod, motor_fte, 
            tipo_inv, id_externo_ref, t_data.get('fecha_sql'), symbol_puro, t_data.get('side'), 
            position_side, reduce_only, float(t_data.get('price', 0)), float(t_data.get('qty', 0)), 
            com_nominal, com_asset, com_usd, float(t_data.get('quoteQty', 0)), 
            pnl_realizado, is_maker, trade_id, order_id, 
            raw_json
        )
        
        cursor.execute(sql, valores)
        return True
        
    except Exception as e:
        print(f"    [ERROR REGISTRO TRADE] {e} | Symbol: {t_data.get('symbol')}")
        return False

# ==========================================================
# 🟦 PROCESADOR BINGX (V6.8.6 - UNIFICADO CON BLOQUES)
# ==========================================================
def procesar_bingx(db, uid, ak, as_):
    db = check_db_connection(db)
    cursor = db.cursor(dictionary=True)
    # --- CONFIGURACIÓN DE SESIÓN PARA PROCESOS LARGOS ---
    cursor.execute("SET SESSION wait_timeout = 28800")
    cursor.execute("SET SESSION interactive_timeout = 28800")
    # 1. Si la tabla está bloqueada, espera 100 segundos antes de dar error
    cursor.execute("SET SESSION innodb_lock_wait_timeout = 100")
    # 2. Asegúrate de que cada cambio se grabe de inmediato en el disco
    cursor.execute("SET AUTOCOMMIT = 1")

    ahora_ms = int(time.time() * 1000)
    
    def bx_req(path, params=None):
        params = params or {}
        params["timestamp"] = int(time.time()*1000)
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        try:
            r = bingx_session.get(url, headers=get_headers_bingx(ak), timeout=10)
            if r.status_code == 429: time.sleep(2)
            return r.json()
        except Exception as e: 
            print(f"      [!] Error de conexión BingX: {e}")
            return {}

    # --- 1. SALDOS (Igual que antes) ---
    cursor.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND broker_name = 'BINGX'", (uid,))
    try:
        # Spot Balance
        res_spot = bx_req("/openApi/spot/v1/account/balance")
        for b in res_spot.get("data", {}).get("balances", []):
            total = float(b.get('free', 0)) + float(b.get('locked', 0))
            if total > 0.000001:
                info = obtener_traductor_id_universal(cursor, "bingx_spot", b['asset'])
                registrar_saldo(cursor, uid, info, total, float(b.get('locked', 0)), b['asset'], "BINGX", "SPOT")
        
        # Futures Balance
        res_perp = bx_req("/openApi/swap/v2/user/balance")
        bal = res_perp.get("data", {}).get("balance", {})
        if bal and float(bal.get("balance", 0)) > 0.000001:
            info = obtener_traductor_id_universal(cursor, "bingx_futures", bal.get("asset"))
            registrar_saldo(cursor, uid, info, float(bal["balance"]), float(bal.get("freezedMargin", 0)), bal["asset"], "BINGX", "FUTURES")
        db.commit()
    except Exception as e: print(f"    [!] Error en saldos BingX: {e}")

    # --- 2. OPEN ORDERS (Igual que antes) ---
    config_orders = [
        {"path": "/openApi/spot/v1/trade/openOrders", "tabla": "sys_open_orders_spot", "motor": "bingx_spot"},
        {"path": "/openApi/swap/v2/trade/openOrders", "tabla": "sys_open_orders", "motor": "bingx_futures"}
    ]
    for conf in config_orders:
        cursor.execute(f"DELETE FROM {conf['tabla']} WHERE user_id = %s AND broker_name = 'BINGX'", (uid,))
        res = bx_req(conf["path"])
        for o in res.get("data", {}).get("orders", []):
            sym = o.get('symbol')
            info = obtener_traductor_id_universal(cursor, conf["motor"], sym)
            tid = info['id'] if info else None
            ts = o.get('updateTime', o.get('time', 0))
            sql = f"INSERT INTO {conf['tabla']} (id_order_ext, user_id, broker_name, traductor_id, symbol, side, type, price, qty, locked_amount, fecha_utc, estado, last_seen) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'ABIERTA',NOW())"
            cursor.execute(sql, (str(o['orderId']), uid, "BINGX", tid, sym, o['side'], o.get('type', 'LIMIT'), float(o.get('price', 0)), float(o.get('origQty', 0)), 0.0, datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S')))

    # --- 3. TRADES SPOT (CON BLOQUES DE 24H) ---
    try:
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINGX", "trades_spot")
        current_start = start_ts - 60000 # 🛡️ Seguridad
        ventana_24h = 24 * 60 * 60 * 1000
        t_count_spot = 0

        while current_start < ahora_ms:
            current_end = min(current_start + ventana_24h, ahora_ms)
            res_tr = bx_req("/openApi/spot/v1/trade/myTrades", {"startTime": current_start, "endTime": current_end})
            trades = res_tr.get("data", [])
            
            for t in sorted(trades, key=lambda x: x.get('time', 0)):
                info = obtener_traductor_id_universal(cursor, "bingx_spot", t.get("symbol"))
                if not info: continue
                
                trade_data = {
                    "tradeId": str(t.get("id")), "orderId": str(t.get("orderId")), "symbol": t.get("symbol"),
                    "side": "BUY" if t.get("isBuyer") else "SELL", "price": float(t.get("price", 0)),
                    "qty": float(t.get("qty", 0)), "quoteQty": float(t.get("quoteQty", 0)),
                    "commission": abs(float(t.get("commission", 0))), "commissionAsset": t.get("commissionAsset", "USDT"),
                    "realizedPnl": 0, "fecha_sql": datetime.fromtimestamp(t.get("time", 0) / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                    "isMaker": 1 if t.get("isMaker") else 0, "categoria": info['categoria_producto'], 
                    "tipo_investment": info['tipo_investment'], "raw_json": json.dumps(t)
                }
                if registrar_trade(cursor, uid, trade_data, info, "BINGX"): t_count_spot += 1
            
            current_start = current_end
            if current_start < ahora_ms: time.sleep(0.2)
        
        actualizar_punto_sincro(cursor, uid, "BINGX", "trades_spot", ahora_ms)
        print(f"    [OK] BingX Spot Trades: {t_count_spot} procesados.")
    except Exception as e: print(f"    [!] Error Spot Trades BingX: {e}")

    # --- 4. TRADES FUTURES (CON BLOQUES DE 7 DÍAS) ---
    try:
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINGX", "trades_futures")
        current_start = start_ts - 60000 # 🛡️ Seguridad
        ventana_7d = 7 * 24 * 60 * 60 * 1000
        t_count_fut = 0

        while current_start < ahora_ms:
            current_end = min(current_start + ventana_7d, ahora_ms)
            # BingX usa 'allOrders' para historial, limitamos a 500 por bloque
            res_f = bx_req("/openApi/swap/v2/trade/allOrders", {"startTime": current_start, "endTime": current_end, "limit": 500})
            orders = res_f.get("data", {}).get("orders", [])
            
            for o in sorted(orders, key=lambda x: x['updateTime']):
                if o.get("status") != "FILLED": continue
                
                info = obtener_traductor_id_universal(cursor, "bingx_futures", o.get("symbol"))
                if not info: continue
                
                cat_final = 'FUTURES' if info['categoria_producto'] in ['SPOT', 'CRYPTO'] else info['categoria_producto']
                
                trade_data = {
                    "tradeId": f"BX-{o.get('orderId')}", "orderId": str(o["orderId"]), "symbol": o["symbol"],
                    "side": o["side"], "positionSide": o.get("positionSide", "BOTH"), "price": float(o.get("avgPrice", 0)),
                    "qty": float(o.get("executedQty", 0)), "quoteQty": float(o.get("cumQuote", 0)),
                    "commission": abs(float(o.get("commission", 0))), "commissionAsset": "USDT",
                    "realizedPnl": float(o.get("profit", 0)), "fecha_sql": datetime.fromtimestamp(int(o["updateTime"]) / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                    "isMaker": 1 if o.get("isMaker") else 0, "categoria": cat_final, 
                    "tipo_investment": info['tipo_investment'], "raw_json": json.dumps(o)
                }
                if registrar_trade(cursor, uid, trade_data, info, "BINGX"): t_count_fut += 1
                
            current_start = current_end
            if current_start < ahora_ms: time.sleep(0.2)

        actualizar_punto_sincro(cursor, uid, "BINGX", "trades_futures", ahora_ms)
        print(f"    [OK] BingX Futures Trades: {t_count_fut} procesados.")
    except Exception as e: print(f"    [!] Error Fut Trades BingX: {e}")
    
    finally:
        cursor.close()

# ==========================================================
# 🟣 BINGX FUTURES - POSITIONS (V1.3.1 - INTEGRADO V6.6.7)
# ==========================================================
def procesar_bingx_positions(db, uid, ak, as_):
    cursor = db.cursor(dictionary=True)

    def bx_req(path, params=None):
        if params is None: params = {}
        params["timestamp"] = int(time.time() * 1000)
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        r = bingx_session.get(url, headers=get_headers_bingx(ak), timeout=10)
        return r.json()

    try:
        res = bx_req("/openApi/swap/v2/user/positions")
        cursor.execute("DELETE FROM sys_positions WHERE user_id = %s AND broker_name = 'BINGX'", (uid,))

        posiciones = res.get("data", [])
        p_count = 0

        for p in posiciones:
            position_amt = float(p.get("positionAmt", 0))
            if position_amt != 0:
                symbol_puro = p.get("symbol")

                # v6.6.7 - No importa que no exista "bingx_perpetual" en la DB, 
                # el LIKE 'bingx%' encontrará el subyacente en el traductor.
                info = obtener_traductor_id_universal(cursor, "bingx_perpetual", symbol_puro)

                leverage = int(p.get("leverage", 0))
                m_type_raw = p.get("marginType", "CROSS").upper()
                margin_type = "ISOLATED" if "ISOLATED" in m_type_raw else "CROSS"
                lado_posicion = "LONG" if position_amt > 0 else "SHORT"

                cursor.execute("""
                    INSERT INTO sys_positions (
                        user_id, broker_name, traductor_id, symbol,
                        position_side, position_amt, entry_price, mark_price,
                        unrealized_profit, position_initial_margin, maint_margin,
                        leverage, margin_type, last_update
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                """, (
                    uid, "BINGX",
                    info["id"] if info else None,
                    symbol_puro, lado_posicion, position_amt,
                    float(p.get("avgPrice", 0)), float(p.get("markPrice", 0)),
                    float(p.get("unrealizedProfit", 0)), float(p.get("positionMargin", 0)),
                    float(p.get("maintMargin", 0)), leverage, margin_type
                ))
                p_count += 1
        
        db.commit()
        print(f"    [OK] Positions Bingx: {p_count} activas procesadas.")
    except Exception as e: print(f"    [BINGX POS ERROR] {e}")


# ==========================================================
# 🟨 PROCESADOR BINANCE SPOT - ARQUEOLOGÍA INTEGRADA
# ==========================================================
def procesar_binance(db, uid, k, s):
    db = check_db_connection(db) 
    cursor = db.cursor(dictionary=True)
    proxies = {'http': os.getenv('PROXY_URL'), 'https': os.getenv('PROXY_URL')}
    
    try:
        client = Client(k, s, requests_params={'proxies': proxies, 'timeout': 10})
        print(f"    >>> SPOT BINANCE USER {uid} <<<")
        
        # 1. Saldos (Se mantiene igual)
        cursor.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND broker_name = 'BINANCE' AND tipo_cuenta = 'SPOT'", (uid,))
        acc = client.get_account()
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.000001:
                info = obtener_traductor_id_universal(cursor, "binance_spot", b['asset'])
                registrar_saldo(cursor, uid, info, total, float(b['locked']), b['asset'], "BINANCE", "SPOT")
        db.commit()


        # --- TRADES SPOT CON ARQUEOLOGÍA (FORZANDO PASADO) ---
        # 1. Intentamos obtener dónde quedó la última vez
        cursor.execute("SELECT last_timestamp FROM sys_sync_estado WHERE user_id = %s AND broker = 'BINANCE' AND endpoint = 'trades_spot'", (uid,))
        row_sincro = cursor.fetchone()
        
        # 2. Si no hay registro, FORZAMOS el inicio en Enero de 2021 (1609459200000 ms)
        # Puedes poner 1577836800000 para 2020 si quieres ir más atrás.
        start_ts = row_sincro['last_timestamp'] if row_sincro else 1609459200000 
        
        ahora_ms = int(time.time() * 1000)
        ventana_24h = 24 * 60 * 60 * 1000 # Bloques de 24h para no saturar
        
        cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente = 'binance_spot'")
        diccionario = cursor.fetchall()

        for item in diccionario:
            ticker = item['ticker_motor']
            current_start = start_ts
            
            # Este bucle es el que hace la "magia" de viajar al pasado
            while current_start < ahora_ms:
                current_end = current_start + ventana_24h
                if current_end > ahora_ms: current_end = ahora_ms

                # Log de control para que veas qué fecha está revisando
                fecha_log = datetime.fromtimestamp(current_start/1000).strftime('%Y-%m-%d')
                print(f"      [*] Escaneando {ticker} | Fecha: {fecha_log}...")

                try:
                    raw_trades = client.get_my_trades(
                        symbol=ticker, 
                        startTime=current_start, 
                        endTime=current_end,
                        limit=1000
                    )
                    
                    if raw_trades:
                        print(f"        [!] ¡ENCONTRADOS! {len(raw_trades)} trades en {ticker} el {fecha_log}")
                        for t in raw_trades:
                            t_f = {
                                'tradeId': str(t['id']), 'orderId': str(t['orderId']), 
                                'symbol': t['symbol'], 'side': 'BUY' if t['isBuyer'] else 'SELL', 
                                'price': float(t['price']), 'qty': float(t['qty']), 
                                'quoteQty': float(t['quoteQty']), 'commission': float(t['commission']), 
                                'commissionAsset': t['commissionAsset'], 
                                'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
                            }
                            # El registro se hace bloque por bloque
                            registrar_trade(cursor, uid, t_f, item, "BINANCE")
                        
                        db.commit() # GRABA DE INMEDIATO EN LA DB
                
                except Exception as e:
                    print(f"      [!] Error en bloque {ticker}: {e}")
                    time.sleep(1) # Pausa si hay error de red
                    break 

                # Avanzamos al siguiente bloque de 24 horas
                current_start = current_end + 1
                
                # Respeto al Rate Limit de Binance para no ser baneados
                time.sleep(0.05)

        # Al terminar todo el historial de todas las monedas, actualizamos el punto de sincronización
        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_spot", ahora_ms)
    except Exception as e:
        print(f"    [!] Error Spot: {e}")
    finally:
        cursor.close()

# ==========================================================
# 🟦 BINANCE UM FUTURES - FIX ERROR COLUMNA Y BACKFILL
# ==========================================================
def procesar_binance_um_futures(db, uid, k, s):
    db = check_db_connection(db)
    cursor = db.cursor(dictionary=True)
    try:
        client = UMFutures(key=k, secret=s)
        print(f"    >>> UM FUTURES USER {uid} <<<")

        # 1. SALDOS
        cursor.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND broker_name = 'BINANCE' AND tipo_cuenta = 'FUTURES'", (uid,))
        acc = client.balance()
        for b in acc:
            if float(b.get('balance', 0)) > 0:
                info = obtener_traductor_id_universal(cursor, "binance_usdt_future", b['asset'])
                registrar_saldo(cursor, uid, info, float(b['balance']), 0.0, b['asset'], "BINANCE", "FUTURES")
        db.commit()

        # 2. TRADES HISTÓRICOS (Ventana 7 días)
        cursor.execute("SELECT last_timestamp FROM sys_sync_estado WHERE user_id=%s AND broker='BINANCE' AND endpoint='trades_um_futures'", (uid,))
        row = cursor.fetchone()
        start_ts = row['last_timestamp'] if row else 1577836800000 
        
        ahora_ms = int(time.time() * 1000)
        ventana_7d = 7 * 24 * 60 * 60 * 1000 

        cursor.execute("SELECT id, ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente = 'binance_usdt_future'")
        for item in cursor.fetchall():
            current_start = start_ts
            while current_start < ahora_ms:
                current_end = current_start + ventana_7d
                try:
                    trades = client.get_account_trades(symbol=item['ticker_motor'], startTime=current_start, endTime=current_end)
                    if trades:
                        for t in trades:
                            t_f = {'tradeId': str(t['id']), 'orderId': str(t['orderId']), 'symbol': t['symbol'], 'side': t['side'],
                                   'price': float(t['price']), 'qty': float(t['qty']), 'quoteQty': float(t['quoteQty']),
                                   'commission': float(t['commission']), 'commissionAsset': t['commissionAsset'],
                                   'realizedPnl': float(t.get('realizedPnl', 0)), 'es_futuro': True,
                                   'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')}
                            registrar_trade(cursor, uid, t_f, item, "BINANCE")
                        db.commit()
                except: break
                current_start = current_end + 1
        
        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_um_futures", ahora_ms)
    except Exception as e:
        print(f"    [UM ERROR] {e}")
    finally:
        cursor.close()

# ==========================================================
# 🟦 POSITIONS - FIX ERROR DE ARGUMENTOS (Se agregaron uid, k, s)
# ==========================================================
def procesar_binance_um_positions(db, uid, k, s):
    db = check_db_connection(db)
    cursor = db.cursor(dictionary=True)
    try:
        client = UMFutures(key=k, secret=s)
        posiciones = client.get_position_risk()
        cursor.execute("DELETE FROM sys_positions WHERE user_id = %s AND broker_name = 'BINANCE_UM'", (uid,))
        for p in posiciones:
            if float(p.get("positionAmt", 0)) != 0:
                # ... (resto de tu lógica de insert sys_positions igual)
                pass
        db.commit()
    except Exception as e:
        print(f"    [UM POS ERROR] {e}")
    finally:
        cursor.close()

# ==========================================================
# 🟧 PROCESADOR BINANCE CM FUTURES - ARQUEOLOGÍA COMPLETA
# ==========================================================
def procesar_binance_cm_futures(db, uid, k, s):
    db = check_db_connection(db)
    cursor = db.cursor(dictionary=True)
    asset_actual = "INICIO"
    
    try:
        # Usamos la librería CMFutures de binance-futures-connector
        client = CMFutures(key=k, secret=s)
        print(f"    >>> CM FUTURES BINANCE USER {uid} <<<")

        # 1. SALDOS CM (COIN-MARGIN)
        cursor.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND broker_name = 'BINANCE' AND tipo_cuenta = 'CM_FUTURES'", (uid,))
        acc = client.balance()
        s_count = 0
        for b in acc:
            asset_actual = b['asset']
            total = float(b.get('balance', 0))
            if total > 0.000001:
                info = obtener_traductor_id_universal(cursor, "binance_coin_future", asset_actual)
                registrar_saldo(cursor, uid, info, total, 0.0, asset_actual, "BINANCE", "CM_FUTURES")
                s_count += 1
        print(f"    [CM] Saldos: {s_count} activos registrados.")
        db.commit()

        # 2. TRADES HISTÓRICOS CM CON ARQUEOLOGÍA (Bloques de 7 días)
        # Buscamos el último punto de sincronización específico para CM
        cursor.execute("""
            SELECT last_timestamp FROM sys_sync_estado 
            WHERE user_id=%s AND broker='BINANCE' AND endpoint='trades_cm_futures'
        """, (uid,))
        row = cursor.fetchone()
        
        # Si es nuevo, empezamos desde 2020 (1577836800000 ms)
        start_ts = row['last_timestamp'] if row else 1577836800000 
        punto_rastreo = start_ts - 60000 # 1 min de solapamiento por seguridad
        ahora_ms = int(time.time() * 1000)
        ventana_7d = 7 * 24 * 60 * 60 * 1000 # Límite de la API de Binance para CM
        
        # Obtenemos los símbolos que debemos rastrear para CM
        cursor.execute("SELECT id, ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente = 'binance_coin_future'")
        diccionario = cursor.fetchall()
        t_count = 0

        for item in diccionario:
            symbol = item['ticker_motor']
            current_start = punto_rastreo
            print(f"      [*] Arqueología CM: {symbol}...")
            
            while current_start < ahora_ms:
                current_end = current_start + ventana_7d
                if current_end > ahora_ms: current_end = ahora_ms

                try:
                    # Llamada a la API para obtener trades en el bloque de tiempo
                    trades = client.get_account_trades(symbol=symbol, startTime=current_start, endTime=current_end)
                    
                    if trades:
                        print(f"        [OK] Bloque CM {symbol}: {len(trades)} trades encontrados.")
                        for t in sorted(trades, key=lambda x: x['time']):
                            t_f = {
                                'tradeId': str(t['id']), 
                                'orderId': str(t['orderId']),
                                'symbol': t['symbol'], 
                                'side': t['side'],
                                'positionSide': t.get('positionSide'), 
                                'price': float(t.get('price', 0)),
                                'qty': float(t.get('qty', 0)), 
                                'quoteQty': float(t.get('baseQty', 0)), # En CM se usa baseQty para el valor nominal
                                'commission': float(t.get('commission', 0)), 
                                'commissionAsset': t.get('commissionAsset'),
                                'realizedPnl': float(t.get('realizedPnl', 0)),
                                'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S'),
                                'isMaker': t.get('maker', False), 
                                'es_futuro': True
                            }
                            if registrar_trade(cursor, uid, t_f, item, "BINANCE"):
                                t_count += 1
                        db.commit() # Guardamos por cada bloque exitoso
                except Exception as e:
                    print(f"      [!] Error en bloque CM {symbol}: {e}")
                    break # Si falla la API para este símbolo, pasamos al siguiente
                
                # Avanzamos al siguiente bloque
                current_start = current_end + 1
                if current_start < ahora_ms: time.sleep(0.1) # Rate Limit

        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_cm_futures", ahora_ms)
        print(f"    [CM] Total Trades procesados: {t_count}")

        # 3. OPEN ORDERS CM
        cursor.execute("DELETE FROM sys_open_orders WHERE user_id = %s AND broker_name = 'BINANCE_CM'", (uid,))
        oo_count = 0
        for row in diccionario:
            symbol = row['ticker_motor']
            try:
                orders = client.get_orders(symbol=symbol)
                abiertas = [o for o in orders if o["status"] in ["NEW", "PARTIALLY_FILLED"]]
                for oo in abiertas:
                    tid = row['id']
                    sql = "INSERT INTO sys_open_orders (id_order_ext, user_id, broker_name, traductor_id, symbol, side, type, price, qty, locked_amount, fecha_utc, estado, last_seen) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())"
                    cursor.execute(sql, (str(oo['orderId']), uid, "BINANCE_CM", tid, symbol, oo['side'], oo['type'], float(oo.get('price', 0)), float(oo.get('origQty', 0)), 0.0, datetime.fromtimestamp(oo['time']/1000).strftime('%Y-%m-%d %H:%M:%S'), 'ABIERTA'))
                    oo_count += 1
            except: continue
        print(f"    [CM] Open Orders: {oo_count}")

    except Exception as e:
        print(f"    [CM ERROR CRÍTICO] Asset: {asset_actual} | Error: {e}")
    finally:
        cursor.close()

# ==========================================================
# 🟧 BINANCE CM FUTURES - POSICIONES (v1.3)
# ==========================================================
def procesar_binance_cm_positions(db, uid, k, s):
    db = check_db_connection(db)
    cursor = db.cursor(dictionary=True)
    try:
        client = CMFutures(key=k, secret=s)
        posiciones = client.get_position_risk()
        
        # Limpiamos posiciones viejas del broker específico
        cursor.execute("DELETE FROM sys_positions WHERE user_id = %s AND broker_name = 'BINANCE_CM'", (uid,))
        p_count = 0
        
        for p in posiciones:
            position_amt = float(p.get("positionAmt", 0))
            if position_amt != 0:
                symbol_puro = p.get("symbol")
                info = obtener_traductor_id_universal(cursor, "binance_coin_future", symbol_puro)
                
                leverage = int(p.get("leverage", 0))
                margin_type = p.get("marginType", "CROSS").upper()
                lado_posicion = "LONG" if position_amt > 0 else "SHORT"

                cursor.execute("""
                    INSERT INTO sys_positions (
                        user_id, broker_name, traductor_id, symbol,
                        position_side, position_amt, entry_price, mark_price,
                        unrealized_profit, leverage, margin_type, last_update
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                """, (
                    uid, "BINANCE_CM", info["id"] if info else None, symbol_puro,
                    lado_posicion, position_amt, float(p.get("entryPrice", 0)),
                    float(p.get("markPrice", 0)), float(p.get("unRealizedProfit", 0)),
                    leverage, margin_type
                ))
                p_count += 1
        db.commit()
        print(f"    [CM] Posiciones activas: {p_count}")
    except Exception as e:
        print(f"    [CM POS ERROR] {e}")
    finally:
        cursor.close()

def procesar_csv_spot(cursor, uid, fila):
    # Cabeceras SPOT: symbol,id,orderId,orderListId,price,qty,quoteQty,commission,commissionAsset,time,isBuyer...
    trade_id = fila['id']
    symbol = fila['symbol']
    
    item = verificar_y_obtener_traductor(cursor, uid, trade_id, symbol, 'binance_spot')
    if not item: return False

    # Convertir 18-10-2021 21:04:19 a YYYY-MM-DD HH:MM:SS
    fecha_dt = datetime.strptime(fila['time'], '%d-%m-%Y %H:%M:%S')
    
    t_f = {
        'tradeId': str(trade_id),
        'orderId': str(fila['orderId']),
        'symbol': symbol,
        'side': 'BUY' if fila['isBuyer'] == 'True' else 'SELL',
        'price': float(fila['price']),
        'qty': float(fila['qty']),
        'quoteQty': float(fila['quoteQty']),
        'commission': float(fila['commission']),
        'commissionAsset': fila['commissionAsset'],
        'fecha_sql': fecha_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'isMaker': fila['isMaker'] == 'True',
        'es_futuro': False,
        'json_raw': json.dumps(fila) # JSON SINTÉTICO PARA AUDITORÍA
    }
    
    print(f"        \033[92m[NEW]\033[0m Insertando trade SPOT {trade_id} ({fecha_dt.strftime('%Y-%m-%d')})...")
    return registrar_trade(cursor, uid, t_f, item, "BINANCE")

def procesar_csv_um(cursor, uid, fila):
    # Cabeceras UM: symbol,id,orderId,side,price,qty,realizedPnl,quoteQty,commission,commissionAsset,time,positionSide...
    trade_id = fila['id']
    symbol = fila['symbol']
    
    item = verificar_y_obtener_traductor(cursor, uid, trade_id, symbol, 'binance_usd_future')
    if not item: return False

    fecha_dt = datetime.strptime(fila['time'], '%d-%m-%Y %H:%M:%S')
    
    t_f = {
        'tradeId': str(trade_id),
        'orderId': str(fila['orderId']),
        'symbol': symbol,
        'side': fila['side'], # Aquí ya viene BUY o SELL directo
        'positionSide': fila.get('positionSide', 'BOTH'),
        'price': float(fila['price']),
        'qty': float(fila['qty']),
        'quoteQty': float(fila['quoteQty']),
        'commission': float(fila['commission']),
        'commissionAsset': fila['commissionAsset'],
        'realizedPnl': float(fila['realizedPnl']),
        'fecha_sql': fecha_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'isMaker': fila['maker'] == 'True',
        'es_futuro': True,
        'json_raw': json.dumps(fila)
    }
    
    print(f"        \033[92m[NEW]\033[0m Insertando trade UM {trade_id} ({fecha_dt.strftime('%Y-%m-%d')})...")
    return registrar_trade(cursor, uid, t_f, item, "BINANCE")

def procesar_csv_cm(cursor, uid, fila):
    # Cabeceras CM: symbol,id,orderId,pair,side,price,qty,realizedPnl,marginAsset,baseQty,commission,commissionAsset,time...
    trade_id = fila['id']
    # OJO: En Coin-M el symbol real a veces trae _PERP, usamos el campo pair si symbol falla, pero según tu CSV el symbol es ADAUSD_PERP
    symbol = fila['symbol'] 
    
    item = verificar_y_obtener_traductor(cursor, uid, trade_id, symbol, 'binance_coin_future')
    if not item: return False

    fecha_dt = datetime.strptime(fila['time'], '%d-%m-%Y %H:%M:%S')
    
    t_f = {
        'tradeId': str(trade_id),
        'orderId': str(fila['orderId']),
        'symbol': symbol,
        'side': fila['side'],
        'positionSide': fila.get('positionSide', 'BOTH'),
        'price': float(fila['price']),
        'qty': float(fila['qty']), # En CM son contratos
        'quoteQty': float(fila['baseQty']), # El notional real está en baseQty
        'commission': float(fila['commission']),
        'commissionAsset': fila['commissionAsset'],
        'realizedPnl': float(fila['realizedPnl']),
        'fecha_sql': fecha_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'isMaker': fila['maker'] == 'True',
        'es_futuro': True,
        'json_raw': json.dumps(fila)
    }
    
    print(f"        \033[92m[NEW]\033[0m Insertando trade CM {trade_id} ({fecha_dt.strftime('%Y-%m-%d')})...")
    return registrar_trade(cursor, uid, t_f, item, "BINANCE")

def procesar_csv_spot(cursor, uid, fila):
    # Cabeceras SPOT: symbol,id,orderId,orderListId,price,qty,quoteQty,commission,commissionAsset,time,isBuyer...
    trade_id = fila['id']
    symbol = fila['symbol']
    
    item = verificar_y_obtener_traductor(cursor, uid, trade_id, symbol, 'binance_spot')
    if not item: return False

    # Convertir 18-10-2021 21:04:19 a YYYY-MM-DD HH:MM:SS
    fecha_dt = datetime.strptime(fila['time'], '%d-%m-%Y %H:%M:%S')
    
    t_f = {
        'tradeId': str(trade_id),
        'orderId': str(fila['orderId']),
        'symbol': symbol,
        'side': 'BUY' if fila['isBuyer'] == 'True' else 'SELL',
        'price': float(fila['price']),
        'qty': float(fila['qty']),
        'quoteQty': float(fila['quoteQty']),
        'commission': float(fila['commission']),
        'commissionAsset': fila['commissionAsset'],
        'fecha_sql': fecha_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'isMaker': fila['isMaker'] == 'True',
        'es_futuro': False,
        'json_raw': json.dumps(fila) # JSON SINTÉTICO PARA AUDITORÍA
    }
    
    print(f"        \033[92m[NEW]\033[0m Insertando trade SPOT {trade_id} ({fecha_dt.strftime('%Y-%m-%d')})...")
    return registrar_trade(cursor, uid, t_f, item, "BINANCE")

def procesar_csv_um(cursor, uid, fila):
    # Cabeceras UM: symbol,id,orderId,side,price,qty,realizedPnl,quoteQty,commission,commissionAsset,time,positionSide...
    trade_id = fila['id']
    symbol = fila['symbol']
    
    item = verificar_y_obtener_traductor(cursor, uid, trade_id, symbol, 'binance_usd_future')
    if not item: return False

    fecha_dt = datetime.strptime(fila['time'], '%d-%m-%Y %H:%M:%S')
    
    t_f = {
        'tradeId': str(trade_id),
        'orderId': str(fila['orderId']),
        'symbol': symbol,
        'side': fila['side'], # Aquí ya viene BUY o SELL directo
        'positionSide': fila.get('positionSide', 'BOTH'),
        'price': float(fila['price']),
        'qty': float(fila['qty']),
        'quoteQty': float(fila['quoteQty']),
        'commission': float(fila['commission']),
        'commissionAsset': fila['commissionAsset'],
        'realizedPnl': float(fila['realizedPnl']),
        'fecha_sql': fecha_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'isMaker': fila['maker'] == 'True',
        'es_futuro': True,
        'json_raw': json.dumps(fila)
    }
    
    print(f"        \033[92m[NEW]\033[0m Insertando trade UM {trade_id} ({fecha_dt.strftime('%Y-%m-%d')})...")
    return registrar_trade(cursor, uid, t_f, item, "BINANCE")

def procesar_csv_cm(cursor, uid, fila):
    # Cabeceras CM: symbol,id,orderId,pair,side,price,qty,realizedPnl,marginAsset,baseQty,commission,commissionAsset,time...
    trade_id = fila['id']
    # OJO: En Coin-M el symbol real a veces trae _PERP, usamos el campo pair si symbol falla, pero según tu CSV el symbol es ADAUSD_PERP
    symbol = fila['symbol'] 
    
    item = verificar_y_obtener_traductor(cursor, uid, trade_id, symbol, 'binance_coin_future')
    if not item: return False

    fecha_dt = datetime.strptime(fila['time'], '%d-%m-%Y %H:%M:%S')
    
    t_f = {
        'tradeId': str(trade_id),
        'orderId': str(fila['orderId']),
        'symbol': symbol,
        'side': fila['side'],
        'positionSide': fila.get('positionSide', 'BOTH'),
        'price': float(fila['price']),
        'qty': float(fila['qty']), # En CM son contratos
        'quoteQty': float(fila['baseQty']), # El notional real está en baseQty
        'commission': float(fila['commission']),
        'commissionAsset': fila['commissionAsset'],
        'realizedPnl': float(fila['realizedPnl']),
        'fecha_sql': fecha_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'isMaker': fila['maker'] == 'True',
        'es_futuro': True,
        'json_raw': json.dumps(fila)
    }
    
    print(f"        \033[92m[NEW]\033[0m Insertando trade CM {trade_id} ({fecha_dt.strftime('%Y-%m-%d')})...")
    return registrar_trade(cursor, uid, t_f, item, "BINANCE")

def ingestor_hibrido_csv(db, uid):
    # Solo procesar para el usuario 6 como solicitaste
    if uid != 6: return

    print(f"\n    {'='*50}")
    print(f"    📂 INICIANDO INGESTA HISTÓRICA CSV (USER {uid})")
    print(f"    {'='*50}")
    
    db = check_db_connection(db)
    cursor = db.cursor(dictionary=True)
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    
    # Buscar todos los CSVs en la carpeta
    archivos_csv = [f for f in os.listdir(directorio_actual) if f.endswith('.csv')]
    
    for archivo in archivos_csv:
        ruta_completa = os.path.join(directorio_actual, archivo)
        print(f"    [*] Analizando archivo: {archivo}")
        
        try:
            with open(ruta_completa, mode='r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                cabeceras = reader.fieldnames
                
                # DETECCIÓN AUTOMÁTICA DEL TIPO DE CSV
                if 'isBuyer' in cabeceras:
                    tipo_csv = "SPOT"
                    procesador = procesar_csv_spot
                elif 'marginAsset' in cabeceras:
                    tipo_csv = "COIN-M (CM)"
                    procesador = procesar_csv_cm
                elif 'realizedPnl' in cabeceras:
                    tipo_csv = "USD-M (UM)"
                    procesador = procesar_csv_um
                else:
                    print(f"      [!] Formato desconocido. Saltando {archivo}")
                    continue
                
                print(f"      -> Detectado formato: {tipo_csv}")
                insertados = 0
                
                for fila in reader:
                    if not fila.get('id'): continue # Evitar líneas en blanco
                    
                    if procesador(cursor, uid, fila):
                        insertados += 1
                
                db.commit()
                print(f"      ✅ Archivo procesado. {insertados} nuevos trades guardados.")
                
        except Exception as e:
            print(f"      ❌ Error leyendo {archivo}: {e}")

    # ==========================================================
    # RUTINA DE LIMPIEZA POST-CARGA (Detección de Duplicados)
    # ==========================================================
    print(f"\n    🧹 Ejecutando limpieza de duplicados post-carga...")
    sql_clean = """
        DELETE t1 FROM sys_detalle_trades t1
        INNER JOIN sys_detalle_trades t2 
        WHERE 
            t1.id > t2.id AND 
            t1.trade_id_externo = t2.trade_id_externo AND 
            t1.user_id = t2.user_id AND 
            t1.user_id = %s AND
            t1.broker = 'BINANCE';
    """
    try:
        cursor.execute(sql_clean, (uid,))
        db.commit()
        if cursor.rowcount > 0:
            print(f"    ✨ Limpieza exitosa: Se eliminaron {cursor.rowcount} duplicados redundantes.")
        else:
            print(f"    ✨ Base de datos impecable. Cero duplicados encontrados.")
    except Exception as e:
        print(f"    ⚠️ Error en limpieza: {e}")
        
    cursor.close()        
# ==========================================================
# 🚀 LÓGICA DE UN SOLO CICLO (CON LOCK )
# Version 6.6.6.26
# ==========================================================
def ejecutar_ciclo_completo():
    print(f"💎 MOTOR v6.6.6.38 - SALDOS + TRADES + OPEN ORDERS + POSITION BINANCE-BINGX INSERT HOMOGENEOS")
    print(f"\n{'='*65}\n🔄 INICIO CICLO: {datetime.now().strftime('%H:%M:%S')}\n{'='*65}")

    db = None
    try:
        db = mysql.connector.connect(**config.DB_CONFIG)
        cursor = db.cursor(dictionary=True)

        # 🔐 INTENTO DE LOCK GLOBAL CONTABLE
        if not obtener_lock(cursor, "LOCK_CONTABLE"):
            print("⛔ LOCK_CONTABLE activo en otro entorno → se cancela este ciclo")
            return

        cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1")

        if u['binance_api_key'] and u['binance_api_secret']:
                print(f">> TRABAJANDO: User {uid} | Binance")
                
                # 1. CARGA HISTÓRICA POR CSV (Solo insertará lo que falte)
                ingestor_hibrido_csv(db, uid)
                
                # 2. CONTINÚA LA API NORMAL (Hibridación)
                procesar_binance(db, uid, u['binance_api_key'], u['binance_api_secret'])
                # procesar_binance_um...
                # procesar_binance_cm...

        for u in cursor.fetchall():
            db = check_db_connection(db)
            print(f">> TRABAJANDO: User {u['user_id']} | {u['broker_name']}")
            
            k = descifrar_dato(u['api_key'], MASTER_KEY)
            s = descifrar_dato(u['api_secret'], MASTER_KEY)

            if u['broker_name'].upper() == "BINANCE":
                db = check_db_connection(db)
                print("        >>> SPOT BINANCE <<<")
                procesar_binance(db, u['user_id'], k, s)

                db = check_db_connection(db)
                print("        >>> UM FUTURES BINANCE <<<")
                procesar_binance_um_futures(db, u['user_id'], k, s)
                db = check_db_connection(db)
                procesar_binance_um_positions(db, u['user_id'], k, s)

                db = check_db_connection(db)
                print("        >>> CM FUTURES BINANCE <<<")
                procesar_binance_cm_futures(db, u['user_id'], k, s)
                db = check_db_connection(db)
                procesar_binance_cm_positions(db, u['user_id'], k, s)

            elif u['broker_name'].upper() == "BINGX":
                db = check_db_connection(db)
                print("        >>> BINGX <<<")
                procesar_bingx(db, u['user_id'], k, s)

                db = check_db_connection(db)
                print("        >>> BINGX  POSITION  <<<")
                procesar_bingx_positions(db, u['user_id'], k, s)

            # 🚀 NORMALIZADOR GLOBAL (OPCIÓN A)
            # Se ejecuta aquí para capturar trades de SPOT, COIN-M y FUTUROS por igual
            db = check_db_connection(db)
            print(f"        >>> 🧹 SWEEPER DE COMISIONES (USER: {u['user_id']}) <<<")
            normalizar_comisiones_pendientes(db, u['user_id'])

            db.commit()

    except Exception as e: 
        print(f"    [CRITICAL] {e}")

    finally:
        # 🔓 LIBERAR LOCK SIEMPRE CON SEGURIDAD
        try:
            if db and db.is_connected():
                cursor_lock = db.cursor()
                liberar_lock(cursor_lock, "LOCK_CONTABLE")
                db.commit()
                cursor_lock.close()
                print("🔓 [LOCK] Liberado correctamente.")
        except Exception as ex:
            print(f"⚠️ No se pudo liberar lock: {ex}")

        if db and db.is_connected(): 
            db.close()

    print(f"\n{'='*65}\n✅ CICLO TERMINADO\n{'='*65}")

# ==========================================================
# 🚀 EJECUCIÓN PRINCIPAL DUAL
# ==========================================================
def run():
    # Detectamos si estamos en GitHub Actions
    is_github = os.getenv('GITHUB_ACTIONS') == 'true'

    if is_github:
        # MODO GITHUB: Solo una vez y termina
        ejecutar_ciclo_completo()
    else:
        # MODO LOCAL (TU PC): Bucle infinito
        while True:
            ejecutar_ciclo_completo()
            print(" esperando 2 min para el siguiente ciclo...")
            time.sleep(120)

if __name__ == "__main__":
    run()