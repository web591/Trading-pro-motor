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
def obtener_traductor_id_universal(cursor, motor_fuente, ticker_api):
    """
    Sigue la lógica para todos: 
    1. Busca exacto (Fidelidad 1:1).
    2. Si es BingX y falla, aplica el rescate (REPLACE/LIKE) que tenías antes.
    """
    if not ticker_api: return None
    ticker = str(ticker_api).upper().strip()
    
    # PASO 1: Búsqueda Exacta (Binance y BingX Fiel)
    sql_exacto = "SELECT id, categoria_producto, tipo_investment FROM sys_traductor_simbolos WHERE motor_fuente = %s AND ticker_motor = %s LIMIT 1"
    cursor.execute(sql_exacto, (motor_fuente, ticker))
    res = cursor.fetchone()
    if res: return res

    # PASO 2: Rescate Elástico (Solo BingX - Escenario B que ya tenías)
    if "bingx" in motor_fuente:
        ticker_limpio = ticker.replace("-", "").replace("/", "").replace("=X", "")
        underlying = ticker_limpio.replace("USDT", "").replace("USDC", "").replace("USD", "")
        
        sql_rescate = """
            SELECT id, categoria_producto, tipo_investment FROM sys_traductor_simbolos 
            WHERE motor_fuente LIKE 'bingx_%%' 
            AND (ticker_motor = %s OR REPLACE(ticker_motor, '-', '') = %s OR underlying = %s OR %s LIKE CONCAT('%%', underlying, '%%')) 
            LIMIT 1
        """
        cursor.execute(sql_rescate, (ticker, ticker_limpio, underlying, ticker_limpio))
        return cursor.fetchone()
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
# 📉 REGISTRO DE TRADES v6.7.0 (PURA FIDELIDAD API)
# ==========================================================
def registrar_trade(cursor, uid, t_data, info_traductor, broker_nombre):
    try:
        # --- BLOQUE 1: LIMPIEZA DE IDENTIFICADORES ---
        # Extraemos el ID crudo del trade y la orden (sin inventar nada)
        trade_id = str(t_data.get('tradeId', ''))
        order_id = str(t_data.get('orderId', '')) if t_data.get('orderId') else None
        
        # ANCLA DE IDEMPOTENCIA: Únicamente {user_id}-{trade_id}
        # Eliminamos cualquier lógica de prefijos "BX-" o "BN-"
        id_externo = f"{uid}-{trade_id}"
        
        # --- BLOQUE 2: INGESTA PURA DE SÍMBOLOS ---
        # Guardamos el símbolo EXACTO como lo entrega la API (ej: BTC-USDT o BTCUSDT)
        symbol_puro = t_data.get('symbol', 'UNKNOWN')
        
        # El traductor_id ahora es opcional. Si es un símbolo nuevo, insertará NULL
        t_id = info_traductor['id'] if info_traductor else None
        cat_prod = info_traductor['categoria_producto'] if info_traductor else 'SPOT'
        tipo_inv = info_traductor['tipo_investment'] if info_traductor else 'CRYPTO'
        motor_fte = info_traductor['motor_fuente'] if info_traductor else f"{broker_nombre.lower()}_auto"

        # --- BLOQUE 3: COMISIONES INTELIGENTES (Se mantiene intacto) ---
        com_asset = t_data.get('commissionAsset')
        com_nominal = float(t_data.get('commission', 0))
        com_usd = 0.0
        if com_nominal > 0 and com_asset:
            precio_com = obtener_precio_usd(cursor, None, com_asset)
            com_usd = com_nominal * precio_com
            
        # --- BLOQUE 4: SEGURO DE VIDA (RAW JSON) ---
        raw_json = json.dumps(t_data)
        
        # --- EXTRACCIÓN DE RIESGO FUTUROS ---
        is_maker = 1 if t_data.get('isMaker', False) else 0
        reduce_only = 1 if t_data.get('reduceOnly', False) else 0
        pnl_realizado = float(t_data.get('realizedPnl', 0))
        position_side = t_data.get('positionSide') # Puede ser LONG, SHORT o None (en Spot)
        
        # --- PREPARACIÓN SQL ---
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
            tipo_inv, id_externo, t_data.get('fecha_sql'), symbol_puro, t_data.get('side'), 
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
# 🟦 PROCESADOR BINGX (V6.8.5 - OPTIMIZADO)
# ==========================================================
def procesar_bingx(db, uid, ak, as_):
    cursor = db.cursor(dictionary=True)
    
    def bx_req(path, params=None):
        params = params or {}
        params["timestamp"] = int(time.time()*1000)
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        try:
            r = bingx_session.get(url, headers=get_headers_bingx(ak), timeout=10)
            if r.status_code == 429: time.sleep(3)
            return r.json()
        except Exception as e: return {}

    # --- 1. SALDOS (SPOT + FUTUROS) ---
    cursor.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND broker_name = 'BINGX'", (uid,))
    s_count_global = 0

    # Procesar SPOT
    try:
        res_spot = bx_req("/openApi/spot/v1/account/balance")
        for b in res_spot.get("data", {}).get("balances", []):
            total = float(b.get('free', 0)) + float(b.get('locked', 0))
            if total > 0.000001:
                info = obtener_traductor_id_universal(cursor, "bingx_spot", b['asset'])
                registrar_saldo(cursor, uid, info, total, float(b.get('locked', 0)), b['asset'], "BINGX", "SPOT")
                s_count_global += 1
    except Exception as e: print(f" [!] Error Spot Balance: {e}")

    # Procesar FUTUROS (Estructura específica de BingX)
    try:
        res_perp = bx_req("/openApi/swap/v2/user/balance")
        bal = res_perp.get("data", {}).get("balance", {})
        if bal and float(bal.get("balance", 0)) > 0.000001:
            info = obtener_traductor_id_universal(cursor, "bingx_futures", bal.get("asset"))
            registrar_saldo(cursor, uid, info, float(bal["balance"]), float(bal.get("freezedMargin", 0)), bal["asset"], "BINGX", "FUTURES")
            s_count_global += 1
    except Exception as e: print(f" [!] Error Perp Balance: {e}")
    
    db.commit()
    print(f"    [OK] BingX Saldos: {s_count_global} activos.")

    # --- 2. OPEN ORDERS (SPOT & FUTURES) ---
    config_orders = [
        {"path": "/openApi/spot/v1/trade/openOrders", "tabla": "sys_open_orders_spot", "motor": "bingx_spot"},
        {"path": "/openApi/swap/v2/trade/openOrders", "tabla": "sys_open_orders", "motor": "bingx_futures"}
    ]

    for conf in config_orders:
        cursor.execute(f"DELETE FROM {conf['tabla']} WHERE user_id = %s AND broker_name = 'BINGX'", (uid,))
        res = bx_req(conf["path"])
        orders = res.get("data", {}).get("orders", [])
        for o in orders:
            sym = o.get('symbol')
            info = obtener_traductor_id_universal(cursor, conf["motor"], sym)
            tid = info['id'] if info else None
            
            # Alerta si no hay traductor en Futuros (tu requerimiento pendiente)
            if not tid and conf["motor"] == "bingx_futures":
                generar_tarea_incorporacion(cursor, uid, "BINGX", sym, "OPEN_ORDER_FUTURES")

            sql = f"""INSERT INTO {conf['tabla']} (id_order_ext, user_id, broker_name, traductor_id, symbol, side, type, price, qty, locked_amount, fecha_utc, estado, last_seen) 
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'ABIERTA',NOW())"""
            try:
                p, q = float(o.get('price', 0)), float(o.get('origQty', 0))
                l_amt = float(o.get('origQuoteOrderQty', 0)) if "spot" in conf["tabla"] else 0.0
                ts = o.get('updateTime', o.get('time', 0))
                cursor.execute(sql, (str(o['orderId']), uid, "BINGX", tid, sym, o['side'], o.get('type', 'LIMIT'), p, q, l_amt, datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S')))
            except: continue

    # --- 3. TRADES (SPOT) ---
    try:
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINGX", "trades_spot")
        res_tr = bx_req("/openApi/spot/v1/trade/myTrades", {"startTime": start_ts})
        trades = res_tr.get("data", []) if isinstance(res_tr.get("data"), list) else res_tr.get("data", {}).get("trades", [])
        for t in sorted(trades, key=lambda x: x.get('time', 0)):
            info = obtener_traductor_id_universal(cursor, "bingx_spot", t.get("symbol"))
            if not info:
                generar_tarea_incorporacion(cursor, uid, "BINGX", t.get("symbol"), "TRADE_SPOT")
                continue
            
            trade_data = {
                "tradeId": str(t.get("id")), "orderId": str(t.get("orderId")), "symbol": t.get("symbol"),
                "side": "BUY" if t.get("isBuyer") else "SELL", "price": float(t.get("price", 0)),
                "qty": float(t.get("qty", 0)), "quoteQty": float(t.get("quoteQty", 0)),
                "commission": abs(float(t.get("commission", 0))), "commissionAsset": t.get("commissionAsset", "USDT"),
                "realizedPnl": 0, "fecha_sql": datetime.utcfromtimestamp(t.get("time", 0) / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                "isMaker": 1 if t.get("isMaker") else 0, "categoria": info['categoria_producto'], "tipo_investment": info['tipo_investment'], "raw_json": json.dumps(t)
            }
            registrar_trade(cursor, uid, trade_data, info, "BINGX")
        actualizar_punto_sincro(cursor, uid, "BINGX", "trades_spot", int(time.time() * 1000))
    except Exception as e: print(f" [!] Error Spot Trades: {e}")

    # --- 4. TRADES (FUTURES) ---
    try:
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINGX", "trades_futures")
        res_f = bx_req("/openApi/swap/v2/trade/allOrders", {"limit": 100})
        max_ts = start_ts
        for o in sorted(res_f.get("data", {}).get("orders", []), key=lambda x: x['updateTime']):
            if o.get("status") != "FILLED" or int(o["updateTime"]) <= start_ts: continue
            max_ts = max(max_ts, int(o["updateTime"]))
            info = obtener_traductor_id_universal(cursor, "bingx_futures", o.get("symbol"))
            if not info:
                generar_tarea_incorporacion(cursor, uid, "BINGX", o.get("symbol"), "TRADE_FUTURES")
                continue
            
            cat_final = 'FUTURES' if info['categoria_producto'] in ['SPOT', 'CRYPTO'] else info['categoria_producto']
            trade_data = {
                "tradeId": f"{uid}-{o.get('orderId')}", "orderId": str(o["orderId"]), "symbol": o["symbol"],
                "side": o["side"], "positionSide": o.get("positionSide", "BOTH"), "price": float(o.get("avgPrice", 0)),
                "qty": float(o.get("executedQty", 0)), "quoteQty": float(o.get("cumQuote", 0)),
                "commission": abs(float(o.get("commission", 0))), "commissionAsset": "USDT",
                "realizedPnl": float(o.get("profit", 0)), "fecha_sql": datetime.utcfromtimestamp(int(o["updateTime"]) / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                "isMaker": 1 if o.get("isMaker") else 0, "categoria": cat_final, "tipo_investment": info['tipo_investment'], "raw_json": json.dumps(o)
            }
            registrar_trade(cursor, uid, trade_data, info, "BINGX")
        actualizar_punto_sincro(cursor, uid, "BINGX", "trades_futures", max_ts)
    except Exception as e: print(f" [!] Error Fut Trades: {e}")


# ==========================================================
# 🟣 BINGX FUTURES - POSITIONS
# Version 1.3 (Uso de Traductor Genérico & Riesgo)
# ==========================================================
def procesar_bingx_positions(db, uid, ak, as_):
    cursor = db.cursor(dictionary=True)

    def bx_req(path, params=None):
        if params is None: params = {}
        ts = int(time.time() * 1000)
        params["timestamp"] = ts
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        r = bingx_session.get(url, headers=get_headers_bingx(ak), timeout=10)
        return r.json()

    try:
        res = bx_req("/openApi/swap/v2/user/positions")
        
        cursor.execute("""
            DELETE FROM sys_positions 
            WHERE user_id = %s 
            AND broker_name = 'BINGX'
        """, (uid,))

        posiciones = res.get("data", [])
        p_count = 0

        for p in posiciones:
            position_amt = float(p.get("positionAmt", 0))

            if position_amt != 0:
                symbol_puro = p.get("symbol") # Ejemplo: "BTC-USDT"

                # 🔄 USAMOS TU FUNCIÓN GENÉRICA DE ORIGEN
                # Nota: En BingX solemos usar 'bingx_perpetual' como fuente principal para swap v2
                info = obtener_traductor_id_universal(
                    cursor, 
                    "bingx_perpetual", 
                    symbol_puro
                )

                # Extracción de Riesgo
                leverage = int(p.get("leverage", 0))
                m_type_raw = p.get("marginType", "CROSS").upper()
                margin_type = "ISOLATED" if "ISOLATED" in m_type_raw else "CROSS"
                lado_posicion = "LONG" if position_amt > 0 else "SHORT"

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
                    uid, "BINGX",
                    info["id"] if info else None,
                    symbol_puro,
                    lado_posicion,
                    position_amt,
                    float(p.get("avgPrice", 0)),
                    float(p.get("markPrice", 0)),
                    float(p.get("unrealizedProfit", 0)),
                    float(p.get("positionMargin", 0)),
                    float(p.get("maintMargin", 0)),
                    leverage,
                    margin_type
                ))

                p_count += 1
        
        db.commit()
        print(f"    [OK] Positions Bingx: {p_count} activas procesadas.")

    except Exception as e:
        print(f"    [BINGX POS ERROR] {e}")

# ==========================================================
# 🟨 PROCESADOR BINANCE (CORREGIDO V6.6.6.26)
# ==========================================================
def procesar_binance(db, uid, k, s):
    # El cursor se define al principio para que esté disponible en todo el proceso
    cursor = db.cursor(dictionary=True)
    
    # 1. Configurar Cliente con Proxy (Clave para GitHub)
    proxies = {
        'http': os.getenv('PROXY_URL'),
        'https': os.getenv('PROXY_URL')
    }
    
    try:
        # Intentamos conectar a Binance
        client = Client(k, s, requests_params={'proxies': proxies, 'timeout': 10})
        
        # --- SALDOS SPOT ---
        print(f"    >>> SPOT BINANCE USER {uid} <<<")
        cursor.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND broker_name = 'BINANCE' AND tipo_cuenta = 'SPOT'", (uid,))
        
        acc = client.get_account()
        s_count = 0
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.000001:
                info = obtener_traductor_id_universal(cursor, "binance_spot", b['asset'])
                registrar_saldo(cursor, uid, info, total, float(b['locked']), b['asset'], "BINANCE", "SPOT")
                s_count += 1
        print(f"    [OK] Binance Saldos actualizado: {s_count} activos.")
        db.commit()

        # --- TRADES SPOT ---
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", "trades_spot")
        cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente = 'binance_spot'")
        diccionario = cursor.fetchall()
        t_count = 0
        for item in diccionario:
            try:
                raw_trades = client.get_my_trades(symbol=item['ticker_motor'], startTime=start_ts)
                for t in sorted(raw_trades, key=lambda x: x['time']):
                    t_f = {
                        'tradeId': str(t['id']),
                        'orderId': str(t['orderId']), 
                        'symbol': t['symbol'], 
                        'side': 'BUY' if t['isBuyer'] else 'SELL', 
                        'price': float(t['price']), 
                        'qty': float(t['qty']), 
                        'quoteQty': float(t['quoteQty']), 
                        'commission': float(t['commission']), 
                        'commissionAsset': t['commissionAsset'], 
                        'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
                    }
                    if registrar_trade(cursor, uid, t_f, item, "BINANCE"): t_count += 1
            except: 
                continue
        
        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_spot", int(time.time()*1000))
        print(f"    [SPOT] Binance Trades: {t_count} nuevos procesados.")

        # --- OPEN ORDERS ---
        cursor.execute("DELETE FROM sys_open_orders_spot WHERE user_id = %s AND broker_name = 'BINANCE'", (uid,))
        open_orders = client.get_open_orders()
        for oo in open_orders:
            info = obtener_traductor_id_universal(cursor, "binance_spot", oo['symbol'])
            sql_oo = "INSERT INTO sys_open_orders_spot (id_order_ext, user_id, broker_name, traductor_id, symbol, side, type, price, qty, locked_amount, fecha_utc, estado, last_seen) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())"
            cursor.execute(sql_oo, (str(oo['orderId']), uid, "BINANCE", info['id'] if info else None, oo['symbol'], oo['side'], oo['type'], float(oo['price']), float(oo['origQty']), 0.0, datetime.fromtimestamp(oo['time']/1000).strftime('%Y-%m-%d %H:%M:%S'), 'ABIERTA'))
        print(f"    [OK] Binance Spot Open Orders: {len(open_orders)} registradas.")

    except Exception as e:
        print(f"    [!] Error Crítico en Binance User {uid}: {e}")
    finally:
        cursor.close() # Siempre cerramos el cursor de este usuario
# ==========================================================
# 🟦 PROCESADOR BINANCE UM FUTURES (USDT-M) - v6.6.7.04
# ==========================================================
def procesar_binance_um_futures(db, uid, k, s):

    try:
        client = UMFutures(key=k, secret=s)
        cursor = db.cursor(dictionary=True)

        print(f"    [UM] Iniciando Binance UM Futures...")
    # Dentro de procesar_binance_um_futures
        cursor.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND broker_name = 'BINANCE' AND tipo_cuenta = 'FUTURES'", (uid,))

        # 1. SALDOS (Igual que CM)
        acc = client.balance()
        s_count = 0
        for b in acc:
            total = float(b.get('balance', 0))
            if total > 0.000001:
                info = obtener_traductor_id_universal(cursor, "binance_usdt_future", b['asset'])
                registrar_saldo(cursor, uid, info, total, 0.0, b['asset'], "BINANCE", "FUTURES")
                s_count += 1
        print(f"    [UM] Saldos: {s_count}")

        # 2. TRADES HISTÓRICOS UM (Misma lógica de sincronización inteligente)
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", "trades_um_futures")
        
        cursor.execute("""
            SELECT id, ticker_motor, categoria_producto, tipo_investment, motor_fuente
            FROM sys_traductor_simbolos
            WHERE motor_fuente = 'binance_usdt_future' 
        """)

        diccionario = cursor.fetchall()
        t_count = 0
        max_ts_procesado = start_ts

        for item in diccionario:
            symbol = item['ticker_motor']
            try:
                # Traemos trades desde el último punto conocido (start_ts)
                trades = client.get_account_trades(symbol=symbol, startTime=start_ts)
                
                if not trades: continue

                for t in sorted(trades, key=lambda x: x['time']):
                    # En UM (USDT-M) sí usamos quoteQty porque es el valor en USDT
                    val_qty = float(t.get('qty', 0))
                    val_quote_qty = float(t.get('quoteQty', 0))
                    
                    t_f = {
                        'tradeId': str(t['id']),'orderId': str(t['orderId']),
                        'symbol': t['symbol'],'side': t['side'],
                        'positionSide': t.get('positionSide'),'price': float(t.get('price', 0)),
                        'qty': val_qty,'quoteQty': val_quote_qty, 
                        'commission': float(t.get('commission', 0)),'commissionAsset': t.get('commissionAsset'),
                        'realizedPnl': float(t.get('realizedPnl', 0)),
                        'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S'),
                        'isMaker': t.get('maker', False),'es_futuro': True
                    }

                    if registrar_trade(cursor, uid, t_f, item, "BINANCE"):
                        t_count += 1
                        # Guardamos el tiempo del trade más reciente
                        if t['time'] > max_ts_procesado:
                            max_ts_procesado = t['time']

            except Exception as e:
                # print(f"        [UM WARN] {symbol}: {e}")
                continue

        # Sincronización idéntica a CM:
        # Si hubo trades, actualizamos al último trade + 1ms para no repetir.
        # Si no hubo, actualizamos al tiempo actual para que el puntero no se quede pegado en el pasado.
        if t_count > 0:
            actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_um_futures", max_ts_procesado + 1)
        else:
            actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_um_futures", int(time.time()*1000))

        print(f"    [UM] Trades Futures nuevos: {t_count}")

    except Exception as e:
        print(f"    [UM ERROR] {e}")

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

                    info = obtener_traductor_id_universal(cursor, "binance_usdt_future", symbol)

                    sql = """
                        INSERT INTO sys_open_orders
                        (id_order_ext, user_id, broker_name, traductor_id,
                         symbol, side, type, price, qty,
                         locked_amount, fecha_utc, estado, last_seen)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    """

                    cursor.execute(sql, (
                        str(oo['orderId']),uid,"BINANCE_UM",info['id'] if info else None,symbol,oo['side'],oo['type'],
                        float(oo.get('price', 0)),float(oo.get('origQty', 0)),0.0,
                        datetime.fromtimestamp(
                            oo['time']/1000
                        ).strftime('%Y-%m-%d %H:%M:%S'),
                        'ABIERTA'
                    ))

                    oo_count += 1

            except Exception as e:
                print(f"[UM OPEN ERROR {symbol}] {e}")
        print(f"    [UM] Open Futuros Orders: {oo_count}")        
    except Exception as e:
        print(f"    [UM CRITICAL ERROR] {e}")

# ==========================================================
# 🟦 BINANCE UM FUTURES - POSITIONS
# Version 1.2 (Fidelidad API & Riesgo)
# ==========================================================
def procesar_binance_um_positions(db, uid, k, s):
    from binance.um_futures import UMFutures
    client = UMFutures(key=k, secret=s)
    cursor = db.cursor(dictionary=True)

    try:
        posiciones = client.get_position_risk()

        # Limpiamos las posiciones anteriores para refrescar con la foto actual
        cursor.execute("""
            DELETE FROM sys_positions 
            WHERE user_id = %s 
            AND broker_name = 'BINANCE_UM'
        """, (uid,))
        
        p_count = 0
        for p in posiciones:
            # 1. Cantidad de la posición (mantenemos el signo: negativo es Short)
            position_amt = float(p.get("positionAmt", 0))

            # Solo procesamos si hay una posición activa
            if position_amt != 0:
                # 2. Símbolo Crudo (Fidelidad 1:1)
                symbol_puro = p.get("symbol")

                # 3. Traductor Flexible (si no existe, info es None)
                info = obtener_traductor_id_universal(
                    cursor, 
                    "binance_usdt_future", 
                    symbol_puro
                )
                
                # 4. Extracción de Riesgo
                # Determinamos el lado basándonos en el signo numérico
                lado_posicion = "LONG" if position_amt > 0 else "SHORT"
                
                # Apalancamiento y Tipo de Margen
                leverage = int(p.get("leverage", 0))
                margin_type = p.get("marginType", "CROSS").upper()

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
                    info["id"] if info else None, # Si no hay traductor, entra NULL
                    symbol_puro,
                    lado_posicion,
                    position_amt,
                    float(p.get("entryPrice", 0)),
                    float(p.get("markPrice", 0)),
                    float(p.get("unRealizedProfit", 0)),
                    float(p.get("positionInitialMargin", 0)),
                    float(p.get("maintMargin", 0)),
                    leverage, 
                    margin_type
                ))

                p_count += 1
        
        db.commit() # Aseguramos que se guarden los cambios
        print(f"    [UM] Binance Position Risk: {p_count} activas procesadas.")

    except Exception as e:
        print(f"    [UM POS ERROR] {e}")


# ==========================================================
# 🟧 PROCESADOR BINANCE CM FUTURES (COIN-M) - v6.6.7.03 (FIXED)
# ==========================================================
def procesar_binance_cm_futures(db, uid, k, s):
    try:
        client = CMFutures(key=k, secret=s)
        cursor = db.cursor(dictionary=True)

        print(f"    [CM] Iniciando Binance CM Futures...")

        # 1. SALDOS (Se mantiene igual)
        acc = client.balance()
        s_count = 0
        for b in acc:
            total = float(b.get('balance', 0))
            if total > 0.000001:
                info = obtener_traductor_id_universal(cursor, "binance_coin_future", b['asset'])
                registrar_saldo(cursor, uid, info, total, 0.0, b['asset'], "BINANCE", "FUTURES")
                s_count += 1
        print(f"    [CM] Saldos: {s_count}")

        # 2. TRADES HISTÓRICOS CM
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", "trades_cm_futures")
        
        # IMPORTANTE: Si el timestamp es muy viejo (ej. año 2025), Binance puede dar problemas.
        # Si start_ts es 0 o muy viejo, podrías limitarlo a los últimos 7 días.

        cursor.execute("""
            SELECT id, ticker_motor, categoria_producto, tipo_investment, motor_fuente
            FROM sys_traductor_simbolos
            WHERE motor_fuente = 'binance_coin_future'
        """)

        diccionario = cursor.fetchall()
        t_count = 0
        max_ts_procesado = start_ts

        for item in diccionario:
            symbol = item['ticker_motor']
            try:
                # Traemos trades desde el último punto conocido
                trades = client.get_account_trades(symbol=symbol, startTime=start_ts)
                
                if not trades: continue

                for t in sorted(trades, key=lambda x: x['time']):
                    # COIN-M NO TIENE quoteQty, usamos baseQty (cantidad en crypto)
                    # o qty (cantidad de contratos)
                    val_qty = float(t.get('qty', 0))
                    val_base_qty = float(t.get('baseQty', 0))
                    
                    t_f = {
                        'tradeId': str(t['id']),'orderId': str(t['orderId']),
                        'symbol': t['symbol'],'side': t['side'],
                        'positionSide': t.get('positionSide'),'price': float(t.get('price', 0)),
                        'qty': val_qty,'quoteQty': val_base_qty, # En CM usamos baseQty como referencia de volumen
                        'commission': float(t.get('commission', 0)),'commissionAsset': t.get('commissionAsset'),
                        'realizedPnl': float(t.get('realizedPnl', 0)),
                        'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S'),
                        'isMaker': t.get('maker', False),'es_futuro': True
                    }

                    if registrar_trade(cursor, uid, t_f, item, "BINANCE"):
                        t_count += 1
                        # Guardamos el tiempo del trade más reciente
                        if t['time'] > max_ts_procesado:
                            max_ts_procesado = t['time']

            except Exception as e:
                # print(f"        [CM WARN] {symbol}: {e}")
                continue

        # SOLO ACTUALIZAMOS SI ENCONTRAMOS ALGO O SI QUEREMOS AVANZAR EL PUNTERO
        # Para evitar el salto al futuro, si t_count > 0, actualizamos al max_ts + 1ms
        if t_count > 0:
            actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_cm_futures", max_ts_procesado + 1)
        else:
            # Si no hubo trades, podemos avanzar el puntero pero con cautela.
            # Aquí lo ideal es dejarlo como estaba o avanzar solo si estamos seguros
            # de que no hay desfase. Por ahora, lo actualizamos al tiempo de la ejecución
            # solo si el t_count fue 0 para no quedar atrapados en un bucle.
            actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_cm_futures", int(time.time()*1000))

        print(f"    [CM] Trades Futures nuevos: {t_count}")

        # ... (Resto de la función Open Orders y Positions se mantiene igual)


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

                    info = obtener_traductor_id_universal(cursor, "binance_coin_future", symbol)

                    sql = """
                        INSERT INTO sys_open_orders
                        (id_order_ext, user_id, broker_name, traductor_id,
                         symbol, side, type, price, qty,
                         locked_amount, fecha_utc, estado, last_seen)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    """

                    cursor.execute(sql, (
                        str(oo['orderId']),uid,
                        "BINANCE_CM",info['id'] if info else None,
                        symbol,oo['side'],oo['type'],
                        float(oo.get('price', 0)),float(oo.get('origQty', 0)),0.0,
                        datetime.fromtimestamp(
                            oo['time']/1000
                        ).strftime('%Y-%m-%d %H:%M:%S'),
                        'ABIERTA'
                    ))

                    oo_count += 1

            except Exception as e:
                print(f"[CM OPEN ERROR {symbol}] {e}")

        print(f"    [CM] Binance  Open Orders: {oo_count}")

    except Exception as e:
        print(f"    [CM CRITICAL ERROR] {e}")


# ==========================================================
# 🟧 BINANCE CM FUTURES - POSITIONS
# Version 1.3 (Fidelidad API & Riesgo)
# ==========================================================
def procesar_binance_cm_positions(db, uid, k, s):
    from binance.cm_futures import CMFutures
    client = CMFutures(key=k, secret=s)
    cursor = db.cursor(dictionary=True)

    try:
        # La API de Coin-M también usa get_position_risk
        posiciones = client.get_position_risk()

        # Limpiamos solo lo de este broker y usuario
        cursor.execute("""
            DELETE FROM sys_positions 
            WHERE user_id = %s 
            AND broker_name = 'BINANCE_CM'
        """, (uid,))

        p_count = 0

        for p in posiciones:
            # 1. Cantidad de la posición (mantenemos signo original)
            position_amt = float(p.get("positionAmt", 0))

            # Solo procesamos si hay una posición abierta
            if position_amt != 0:
                
                # 2. Símbolo Crudo (Fidelidad API)
                # En CM los símbolos suelen ser como 'BTCUSD_PERP'
                symbol_puro = p.get("symbol")

                # 3. Traductor Flexible
                info = obtener_traductor_id_universal(
                    cursor, 
                    "binance_coin_future", 
                    symbol_puro
                )

                # 4. Datos de Riesgo
                leverage = int(p.get("leverage", 0))
                margin_type = p.get("marginType", "CROSS").upper()
                lado_posicion = "LONG" if position_amt > 0 else "SHORT"

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
                    symbol_puro,
                    lado_posicion,
                    position_amt,
                    float(p.get("entryPrice", 0)),
                    float(p.get("markPrice", 0)),
                    float(p.get("unRealizedProfit", 0)),
                    float(p.get("positionInitialMargin", 0)),
                    float(p.get("maintMargin", 0)),
                    leverage,
                    margin_type
                ))

                p_count += 1

        db.commit()
        print(f"    [CM] Binance Positions reales: {p_count}")

    except Exception as e:
        print(f"    [CM POS ERROR] {e}")

# ==========================================================
# 🚀 LÓGICA DE UN SOLO CICLO (CON LOCK )
# Version 6.6.6.26
# ==========================================================
def ejecutar_ciclo_completo():
    print(f"💎 MOTOR v6.6.6.28 - SALDOS + TRADES + OPEN ORDERS + POSITION BINANCE-BINGX")
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

        for u in cursor.fetchall():
            print(f">> TRABAJANDO: User {u['user_id']} | {u['broker_name']}")
            
            k = descifrar_dato(u['api_key'], MASTER_KEY)
            s = descifrar_dato(u['api_secret'], MASTER_KEY)

            if u['broker_name'].upper() == "BINANCE":
                print("        >>> SPOT BINANCE <<<")
                procesar_binance(db, u['user_id'], k, s)

                print("        >>> UM FUTURES BINANCE <<<")
                procesar_binance_um_futures(db, u['user_id'], k, s)
                procesar_binance_um_positions(db, u['user_id'], k, s)

                print("        >>> CM FUTURES BINANCE <<<")
                procesar_binance_cm_futures(db, u['user_id'], k, s)
                procesar_binance_cm_positions(db, u['user_id'], k, s)

            elif u['broker_name'].upper() == "BINGX":
                print("        >>> BINGX <<<")
                procesar_bingx(db, u['user_id'], k, s)

                print("        >>> BINGX  POSITION  <<<")
                procesar_bingx_positions(db, u['user_id'], k, s)

                # 🚀 NORMALIZADOR GLOBAL (OPCIÓN A)
                # Se ejecuta aquí para capturar trades de SPOT, COIN-M y FUTUROS por igual
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