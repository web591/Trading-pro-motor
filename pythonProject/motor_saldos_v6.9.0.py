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

# ==========================================================
# 🎯 VINCULACIÓN MAESTRA (AMARRE FINAL)
# ==========================================================
def obtener_traductor_id(cursor, uid, motor_fuente, ticker, context_data=""):
    ticker = ticker.upper().strip()
    
    # 1. Búsqueda Directa: QUITAMOS 'is_active=1' para que encuentre historia de 2021
    sql = "SELECT id, categoria_producto, tipo_investment FROM sys_traductor_simbolos WHERE motor_fuente=%s AND ticker_motor=%s LIMIT 1"
    cursor.execute(sql, (motor_fuente, ticker))
    row = cursor.fetchone()
    if row: return row

    # 2. Limpieza de Prefijos (Lógica v5.6.3)
    ticker_limpio = ticker
    if ticker.startswith("LD") and len(ticker) > 2: ticker_limpio = ticker[2:]
    elif ticker.startswith("STK") and len(ticker) > 3: ticker_limpio = ticker[3:]

    # 3. Búsqueda por Underlying
    sql_und = "SELECT id, categoria_producto, tipo_investment FROM sys_traductor_simbolos WHERE underlying=%s AND motor_fuente=%s LIMIT 1"
    cursor.execute(sql_und, (ticker_limpio, motor_fuente))
    row_und = cursor.fetchone()
    if row_und: return row_und

    # 4. RADAR: Si no existe, lo anotamos para revisión
    disparar_radar(cursor, uid, ticker, f"Motor: {motor_fuente} | {context_data}")

    # 5. EL PUENTE: Retornamos ID 0 para evitar el error 'NOT NULL' en la DB
    return {'id': 0, 'categoria_producto': 'SPOT', 'tipo_investment': 'CRYPTO'}

def disparar_radar(cursor, uid, ticker, info):
    sql = "INSERT IGNORE INTO sys_simbolos_buscados (user_id, ticker, status, info) VALUES (%s, %s, 'pendiente', %s)"
    cursor.execute(sql, (uid, ticker, info))
    print(f"    [RADAR] !!! Símbolo desconocido {ticker} registrado para revisión.")

def obtener_precio_usd(cursor, tid, asset_name):
    asset_name = asset_name.upper()
    if asset_name.replace("LD", "").replace("STK", "") in ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']: return 1.0
    try:
        if tid and tid > 0: # Solo buscamos precio si el ID es real (mayor a 0)
            sql = "SELECT price FROM sys_precios_activos WHERE traductor_id = %s ORDER BY last_update DESC LIMIT 1"
            cursor.execute(sql, (tid,))
            row = cursor.fetchone()
            if row and row['price'] > 0: return float(row['price'])
    except: pass
    return 0.0

def registrar_saldo(cursor, uid, info_traductor, total, locked, asset, broker, tipo_cuenta):
    tid = info_traductor['id'] # Aquí ya nunca será NULL gracias al ID 0
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
    # Si no hay registro, arranca en 01-Oct-2021
    return int(row['last_timestamp']) if row and row['last_timestamp'] else 1633046400000

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
        tipo_prod = info_traductor['categoria_producto']
        tipo_merc = info_traductor['tipo_investment']
        id_vinculo = f"{uid}-{t_data['orderId']}"
        
        sql_global = "INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, comision, fecha_utc, broker) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql_global, (id_vinculo, uid, broker_nombre, tipo_prod, 'TRADE', t_data['symbol'], t_data['quoteQty'], t_data['commission'], t_data['fecha_sql'], broker_nombre))

        sql_detalle = """
            INSERT IGNORE INTO detalle_trades 
            (user_id, exchange, tipo_producto, exchange_fuente, tipo_mercado, id_externo_ref, fecha_utc, symbol, lado, precio_ejecucion, cantidad_ejecutada, commission, commission_asset, quote_qty, is_maker, broker, trade_id_externo, traductor_id, raw_json) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql_detalle, (uid, broker_nombre, tipo_prod, broker_nombre, tipo_merc, id_vinculo, t_data['fecha_sql'], t_data['symbol'], t_data['side'], t_data['price'], t_data['qty'], t_data['commission'], t_data['commissionAsset'], t_data['quoteQty'], 1 if t_data.get('isMaker') else 0, broker_nombre, f"TRD-{t_data['orderId']}", info_traductor['id'], json.dumps(t_data)))
        return True
    except Exception as e:
        print(f"        [!] ERROR DE INSERCIÓN DETALLE: {e}")
        return False

# ==========================================================
# 🟦 PROCESADOR BINGX (ACTUALIZADO)
# ==========================================================
def procesar_bingx(db, uid, ak, as_):
    cursor = db.cursor(dictionary=True)
    
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

    # --- 1. SALDOS ---
    res_spot = bx_req("/openApi/spot/v1/account/balance")
    if res_spot.get("data") and "balances" in res_spot['data']:
        for b in res_spot['data']['balances']:
            total = float(b.get('free', 0)) + float(b.get('locked', 0))
            if total <= 0.000001: continue
            info = obtener_traductor_id(cursor, uid, "bingx_spot", b['asset'], "Saldo Spot")
            registrar_saldo(cursor, uid, info, total, float(b.get('locked', 0)), b['asset'], "BINGX", "SPOT")

    # --- 2. TRADES FUTUROS ---
    start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINGX", "trades_futures")
    res_tr = bx_req("/openApi/swap/v2/trade/allOrders", {"startTime": start_ts})
    trades_raw = res_tr.get("data", [])
    if isinstance(trades_raw, list):
        for t in trades_raw:
            if str(t.get('status')).upper() in ['FILLED', 'PARTIALLY_FILLED']:
                info = obtener_traductor_id(cursor, uid, "bingx_futures", t['symbol'], "Historial Trades")
                t_f = {
                    'orderId': str(t['orderId']), 'symbol': t['symbol'], 'side': t['side'],
                    'price': float(t.get('avgPrice') or t.get('price') or 0),
                    'qty': float(t.get('executedQty', 0)), 'quoteQty': float(t.get('cumQuote', 0)),
                    'commission': abs(float(t.get('commission', 0))), 'commissionAsset': 'USDT',
                    'fecha_sql': datetime.fromtimestamp(t.get('updateTime', time.time()*1000)/1000).strftime('%Y-%m-%d %H:%M:%S')
                }
                registrar_trade_completo(cursor, uid, t_f, info, "BINGX")
    actualizar_punto_sincro(cursor, uid, "BINGX", "trades_futures", int(time.time()*1000))

# ==========================================================
# 🟨 PROCESADOR BINANCE (ACTUALIZADO)
# ==========================================================
def procesar_binance(db, uid, k, s):
    try:
        client = Client(k, s)
        cursor = db.cursor(dictionary=True)
        
        # SALDOS
        acc = client.get_account()
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.000001:
                info = obtener_traductor_id(cursor, uid, "binance_spot", b['asset'], "Saldo Spot")
                registrar_saldo(cursor, uid, info, total, float(b['locked']), b['asset'], "BINANCE", "SPOT")

        # TRADES (QUITAMOS EL FILTRO is_active=1 para buscar historia)
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", "trades_spot")
        cursor.execute("SELECT ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente = 'binance_spot'")
        for item in cursor.fetchall():
            try:
                raw_trades = client.get_my_trades(symbol=item['ticker_motor'], startTime=start_ts)
                for t in raw_trades:
                    info = obtener_traductor_id(cursor, uid, "binance_spot", t['symbol'], "Historial Trades")
                    t_f = {'orderId': str(t['orderId']), 'symbol': t['symbol'], 'side': 'BUY' if t['isBuyer'] else 'SELL', 'price': float(t['price']), 'qty': float(t['qty']), 'quoteQty': float(t['quoteQty']), 'commission': float(t['commission']), 'commissionAsset': t['commissionAsset'], 'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')}
                    registrar_trade_completo(cursor, uid, t_f, info, "BINANCE")
            except: continue
        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_spot", int(time.time()*1000))

    except Exception as e: print(f"    [!] Error Binance User {uid}: {e}")

# ==========================================================
# 🚀 EJECUCIÓN PRINCIPAL
# ==========================================================
def run():
    print(f"💎 MOTOR v6.9.0 - SALDOS + TRADES BINANCE-BINGX")
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