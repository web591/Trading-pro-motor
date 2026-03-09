import mysql.connector
from binance.client import Client
from binance.um_futures import UMFutures
from binance.cm_futures import CMFutures
import time, os, base64, hmac, hashlib, requests, json
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from datetime import datetime, timedelta
import config

MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

# ==========================================================
# 🔐 SEGURIDAD Y HELPERS (Sin cambios)
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
# 🟦 PROCESADOR BINGX (V6.8.2 - AUTOPSIA DE RESPUESTA)
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
            # --- PRINT ESTRATÉGICO BINGX ---
            if r.status_code != 200:
                print(f"    [AVISO] BingX HTTP {r.status_code} en {path}")
            return r.json()
        except Exception as e: 
            print(f"    [!] Error en petición BingX: {e}")
            return {"code": -1, "msg": str(e)}

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
        return cursor.fetchone()
    
    # 1. SALDOS BINGX
    try:
        res_spot = bx_req("/openApi/spot/v1/account/balance")
        if res_spot.get("data") and "balances" in res_spot['data']:
            for b in res_spot['data']['balances']:
                total = float(b.get('free', 0)) + float(b.get('locked', 0))
                if total > 0.000001:
                    info = buscar_en_traductor_bingx(b['asset'])
                    registrar_saldo(cursor, uid, info, total, float(b.get('locked', 0)), b['asset'], "BINGX", "SPOT")
        
        res_perp = bx_req("/openApi/swap/v2/user/balance")
        balance_obj = res_perp.get("data", {}).get("balance")
        if balance_obj and isinstance(balance_obj, dict):
            ticker = balance_obj.get("asset")
            total = float(balance_obj.get("balance", 0))
            if total > 0.000001:
                info = buscar_en_traductor_bingx(ticker)
                registrar_saldo(cursor, uid, info, total, float(balance_obj.get("freezedMargin", 0)), ticker, "BINGX", "FUTURES")
        print(f"    [OK] BingX Saldos sincronizados.")
    except: pass

    # 2. TRADES BINGX (CON FIX DE TIPO DE DATO)
    try:
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINGX", "trades_futures")
        limit_7d = int((datetime.now() - timedelta(days=6)).timestamp() * 1000)
        if start_ts < limit_7d: start_ts = limit_7d

        cursor.execute("SELECT DISTINCT ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente LIKE 'bingx_%%' AND is_active = 1")
        universo = cursor.fetchall()
        t_count = 0
        
        for item in universo:
            sym = item['ticker_motor']
            res_tr = bx_req("/openApi/swap/v2/trade/allOrders", {"symbol": sym, "startTime": start_ts})
            
            # --- CAPTURA DEL ERROR 'STR' ---
            if not isinstance(res_tr, dict):
                print(f"    [ERROR CRÍTICO] La API para {sym} devolvió un String y no un Objeto.")
                continue

            if res_tr.get("code") == 0:
                for t in res_tr.get("data", []):
                    if str(t.get('status')).upper() in ['FILLED', 'CLOSED', 'COMPLETED']:
                        info = buscar_en_traductor_bingx(sym)
                        t_f = {
                            'orderId': str(t['orderId']), 'symbol': sym, 'side': t['side'],
                            'price': float(t.get('avgPrice') or t.get('price') or 0),
                            'qty': float(t.get('executedQty', 0)), 'quoteQty': float(t.get('cumQuote', 0)),
                            'commission': abs(float(t.get('commission', 0))), 'commissionAsset': 'USDT',
                            'fecha_sql': datetime.fromtimestamp(t.get('updateTime', time.time()*1000)/1000).strftime('%Y-%m-%d %H:%M:%S')
                        }
                        if registrar_trade_completo(cursor, uid, t_f, info, "BINGX"): t_count += 1
            elif res_tr.get("code") != 0:
                # Esto nos dirá si es error de firma, tiempo, etc.
                print(f"    [AVISO API] BingX {sym} respondió: {res_tr.get('msg')} (Code: {res_tr.get('code')})")

        actualizar_punto_sincro(cursor, uid, "BINGX", "trades_futures", int(time.time()*1000))
        print(f"    [INFO] BingX Trades: {t_count} nuevos procesados.")
    except Exception as e:
        print(f"    [!] Error fatal en ciclo trades BingX: {e}")

# ==========================================================
# 🟨 PROCESADOR BINANCE (V6.8.2 - LOCALIZADOR DE MAPPING)
# ==========================================================
def procesar_binance(db, uid, k, s):
    try:
        cursor = db.cursor(dictionary=True)
        client_spot = Client(k, s)
        client_um = UMFutures(key=k, secret=s)
        client_cm = CMFutures(key=k, secret=s)
        
        # 1. SALDOS SPOT (Sin cambios, ya funciona)
        acc = client_spot.get_account()
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.000001:
                info = obtener_traductor_id(cursor, "binance_spot", b['asset'])
                registrar_saldo(cursor, uid, info, total, float(b['locked']), b['asset'], "BINANCE", "SPOT")
        print(f"    [OK] Binance Saldos sincronizados.")

        # 2. TRADES FUTUROS UM (DEBUG CHISMOSO)
        um_count = 0
        start_um = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", "trades_futures_um")
        cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente = 'binance_usdt_future' AND is_active = 1")
        simbolos_um = cursor.fetchall()
        
        # --- PRINT DE DIAGNÓSTICO ---
        if not simbolos_um:
            print("    [ALERTA] No hay símbolos 'binance_usdt_future' activos en la DB.")

        for item in simbolos_um:
            try:
                trades = client_um.get_account_trades(symbol=item['ticker_motor'], startTime=start_um)
                for t in trades:
                    t_f = {'orderId': str(t['id']), 'symbol': item['ticker_motor'], 'side': 'BUY' if t['isBuyer'] else 'SELL', 'price': float(t['price']), 'qty': float(t['qty']), 'quoteQty': float(t['quoteQty']), 'commission': float(t['commission']), 'commissionAsset': t['commissionAsset'], 'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')}
                    if registrar_trade_completo(cursor, uid, t_f, item, "BINANCE"): um_count += 1
            except Exception as e: continue
        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_futures_um", int(time.time()*1000))

        # 3. TRADES FUTUROS CM (AUTOPSIA)
        cm_count = 0
        start_cm = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", "trades_futures_cm")
        cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente = 'binance_coin_future' AND is_active = 1")
        simbolos_cm = cursor.fetchall()
        
        print(f"    [DEBUG] Analizando {len(simbolos_cm)} símbolos para Binance CM...")

        for item in simbolos_cm:
            sym = item['ticker_motor']
            try:
                # El truco en CM es que a veces el símbolo debe ser exacto (BTCUSD_PERP)
                trades = client_cm.get_account_trades(symbol=sym, startTime=start_cm)
                if trades:
                    print(f"    [!] ¡ÉXITO! Encontrados {len(trades)} trades en CM para {sym}")
                    for t in trades:
                        t_f = {'orderId': str(t['id']), 'symbol': sym, 'side': 'BUY' if t['isBuyer'] else 'SELL', 'price': float(t['price']), 'qty': float(t['qty']), 'quoteQty': float(t['quoteQty']), 'commission': float(t['commission']), 'commissionAsset': t['commissionAsset'], 'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')}
                        if registrar_trade_completo(cursor, uid, t_f, item, "BINANCE"): cm_count += 1
            except Exception as e:
                # Aquí capturamos si Binance dice "Invalid Symbol"
                if "Invalid symbol" in str(e):
                    print(f"    [ERROR MAPPING] Binance CM no reconoce el símbolo: {sym}")
                continue
        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_futures_cm", int(time.time()*1000))

        print(f"    [FIN BINANCE] UM: {um_count} | CM: {cm_count}")

    except Exception as e: print(f"    [!] Error Crítico Binance User {uid}: {e}")

# ==========================================================
# 🚀 EJECUCIÓN PRINCIPAL
# ==========================================================
def run():
    print(f"💎 MOTOR v6.8.2 - THE DETECTIVE")
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