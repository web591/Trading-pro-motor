import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests, json
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from datetime import datetime, timedelta
import config

MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

# ==========================================================
# 🔐 SEGURIDAD Y HELPERS (SIN CAMBIOS)
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
    except Exception as e:
        print(f"        [!] ERROR DE INSERCIÓN: {e}")
        return False

# ==========================================================
# 🟦 PROCESADOR BINGX (V6.7.5 - FIX 7 DAYS)
# ==========================================================
def procesar_bingx(db, uid, ak, as_):
    cursor = db.cursor(dictionary=True)
    start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINGX", "trades_futures")
    
    # FIX BINGX: Si la fecha es de hace más de 7 días, ajustamos para evitar error
    limit_7d = int((datetime.now() - timedelta(days=6)).timestamp() * 1000)
    if start_ts < limit_7d:
        print(f"    [AVISO] Ajustando inicio BingX a los últimos 7 días para evitar error de rango.")
        start_ts = limit_7d

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
        except: return {"code": -1, "msg": "Error de conexión"}

    cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente LIKE 'bingx_%%' AND is_active = 1")
    universo = cursor.fetchall()
    
    t_count = 0
    for item in universo:
        sym = item['ticker_motor']
        res_tr = bx_req("/openApi/swap/v2/trade/allOrders", {"symbol": sym, "startTime": start_ts})
        
        trades_raw = res_tr.get("data", [])
        if isinstance(trades_raw, list) and len(trades_raw) > 0:
            print(f"    [OK] BingX detectó {len(trades_raw)} trades en {sym}")
            for t in trades_raw:
                if str(t.get('status', '')).upper() in ['FILLED', 'PARTIALLY_FILLED', 'CLOSED', 'COMPLETED']:
                    t_f = {
                        'orderId': str(t['orderId']), 'symbol': sym, 'side': t['side'],
                        'price': float(t.get('avgPrice') or t.get('price') or 0),
                        'qty': float(t.get('executedQty', 0)), 'quoteQty': float(t.get('cumQuote', 0)),
                        'commission': abs(float(t.get('commission', 0))), 'commissionAsset': 'USDT',
                        'fecha_sql': datetime.fromtimestamp(t.get('updateTime', time.time()*1000)/1000).strftime('%Y-%m-%d %H:%M:%S')
                    }
                    if registrar_trade_completo(cursor, uid, t_f, item, "BINGX"): t_count += 1
    
    actualizar_punto_sincro(cursor, uid, "BINGX", "trades_futures", int(time.time()*1000))
    print(f"    [INFO] BingX: {t_count} nuevos procesados.")

# ==========================================================
# 🟨 PROCESADOR BINANCE (V6.7.5 - RE-CHECK)
# ==========================================================
def procesar_binance(db, uid, k, s):
    try:
        client = Client(k, s)
        cursor = db.cursor(dictionary=True)
        
        mapping_futuros = [
            {'motor': 'binance_usdt_future', 'endpoint': 'trades_futures_um'},
            {'motor': 'binance_coin_future', 'endpoint': 'trades_futures_cm'}
        ]

        for config_f in mapping_futuros:
            start_ts_f = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", config_f['endpoint'])
            cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente = %s AND is_active = 1", (config_f['motor'],))
            dict_fut = cursor.fetchall()
            
            tf_count = 0
            for item in dict_fut:
                sym = item['ticker_motor']
                try:
                    # Intento 1: Historial de Trades (User Trades)
                    if config_f['motor'] == 'binance_usdt_future':
                        f_trades = client.futures_account_trades(symbol=sym, startTime=start_ts_f)
                    else:
                        f_trades = client.futures_coin_account_trades(symbol=sym, startTime=start_ts_f)
                    
                    if not f_trades: # Intento 2: Buscar en todas las órdenes finalizadas por si acaso
                         if config_f['motor'] == 'binance_usdt_future':
                            f_trades = client.futures_get_all_orders(symbol=sym, startTime=start_ts_f)
                    
                    if f_trades:
                        for t in f_trades:
                            # Filtramos solo lo ejecutado
                            status = str(t.get('status', 'FILLED')).upper()
                            if status in ['FILLED', 'PARTIALLY_FILLED']:
                                t_f = {
                                    'orderId': str(t['orderId']), 'symbol': t['symbol'], 
                                    'side': t.get('side', 'BUY'),
                                    'price': float(t.get('avgPrice', t.get('price', 0))), 
                                    'qty': abs(float(t.get('executedQty', t.get('qty', 0)))), 
                                    'quoteQty': abs(float(t.get('cumQuote', 0))), 
                                    'commission': float(t.get('commission', 0)), 
                                    'commissionAsset': t.get('commissionAsset', 'USDT'), 
                                    'fecha_sql': datetime.fromtimestamp(t['updateTime']/1000).strftime('%Y-%m-%d %H:%M:%S')
                                }
                                if registrar_trade_completo(cursor, uid, t_f, item, "BINANCE"): tf_count += 1
                except: continue
            
            actualizar_punto_sincro(cursor, uid, "BINANCE", config_f['endpoint'], int(time.time()*1000))
            print(f"    [INFO] Binance {config_f['motor']}: {tf_count} nuevos.")

    except Exception as e: print(f"    [!] Error Binance User {uid}: {e}")

# ==========================================================
# 🚀 EJECUCIÓN PRINCIPAL
# ==========================================================
def run():
    print(f"💎 MOTOR v6.7.5 - FIX BINGX 7 DAYS")
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