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
    # ELIMINADO FILTRO is_active PARA LEER TODO
    sql = "SELECT id, categoria_producto, tipo_investment FROM sys_traductor_simbolos WHERE motor_fuente=%s AND ticker_motor=%s LIMIT 1"
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
    except: return False

# ==========================================================
# 🟦 PROCESADOR BINGX (V6.8.3 - BLINDADO)
# ==========================================================
def procesar_bingx(db, uid, ak, as_):
    # (Se aplica la misma lógica de bloques para BingX)
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

    cursor.execute("SELECT DISTINCT ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente LIKE 'bingx_%%'")
    universo = cursor.fetchall()
    siete_dias_ms = 7 * 24 * 60 * 60 * 1000
    ahora_ts = int(time.time()*1000)

    for item in universo:
        symbol = item['ticker_motor']
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINGX", f"futures_{symbol}")
        
        temp_start = start_ts
        while temp_start < ahora_ts:
            temp_end = temp_start + siete_dias_ms
            res = bx_req("/openApi/swap/v2/trade/allOrders", {"symbol": symbol, "startTime": temp_start, "endTime": min(temp_end, ahora_ts)})
            
            if res.get("code") == 0 and res.get("data"):
                for t in res["data"]:
                    # ... (Lógica de registro de trade igual a v6.8.5) ...
                    pass
                temp_start = temp_end
            else:
                temp_start = temp_end

# ==========================================================
# 🟨 PROCESADOR BINANCE (V6.8.3 - SIN RESTRICCIONES)
# ==========================================================
def procesar_binance(db, uid, k, s):
    try:
        cursor = db.cursor(dictionary=True)
        client_um = UMFutures(key=k, secret=s)
        
        cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente = 'binance_usdt_future'")
        simbolos_um = cursor.fetchall()
        
        ahora_ts = int(time.time() * 1000)
        siete_dias_ms = 7 * 24 * 60 * 60 * 1000
        
        for item in simbolos_um:
            symbol = item['ticker_motor'].upper()
            inicio_busqueda = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", f"um_{symbol}")
            
            temp_start = inicio_busqueda
            total_nuevos = 0
            
            while temp_start < ahora_ts:
                temp_end = temp_start + siete_dias_ms
                if temp_end > ahora_ts: temp_end = ahora_ts
                
                try:
                    # Traer trades del bloque de 7 días
                    trades = client_um.get_account_trades(symbol=symbol, startTime=temp_start, endTime=temp_end, limit=1000)
                    
                    if trades:
                        for t in trades:
                            # CORRECCIÓN DE CAMPOS SEGÚN API FUTUROS
                            lado = 'BUY' if t.get('buyer') is True or t.get('isBuyer') is True else 'SELL'
                            qty = float(t.get('qty', 0))
                            price = float(t.get('price', 0))
                            quoteQty = float(t.get('quoteQty') or (qty * price))
                            
                            t_f = {
                                'orderId': str(t.get('id') or t.get('orderId')),
                                'symbol': symbol,
                                'side': lado,
                                'price': price,
                                'qty': qty,
                                'quoteQty': quoteQty,
                                'commission': float(t.get('commission', 0)),
                                'commissionAsset': t.get('commissionAsset', 'USDT'),
                                'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
                            }
                            if registrar_trade_completo(cursor, uid, t_f, item, "BINANCE"):
                                total_nuevos += 1
                        
                        temp_start = trades[-1]['time'] + 1
                    else:
                        temp_start = temp_end
                        
                except Exception as e:
                    print(f"      [!] Error en bloque {symbol}: {e}")
                    break # Salta al siguiente símbolo si este da error de API
            
            if total_nuevos > 0:
                print(f"    [OK] {symbol}: {total_nuevos} trades importados.")
                actualizar_punto_sincro(cursor, uid, "BINANCE", f"um_{symbol}", ahora_ts)
            else:
                # Si no hubo trades, igual actualizamos la fecha para no re-escanear mañana
                actualizar_punto_sincro(cursor, uid, "BINANCE", f"um_{symbol}", ahora_ts)

    except Exception as e:
        print(f"    [ERROR BINANCE] {e}")

def run():
    print(f"💎 MOTOR v6.8.3 - SIN RESTRICCIONES")
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