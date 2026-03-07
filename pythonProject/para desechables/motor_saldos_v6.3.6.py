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
    # Usamos tus columnas reales: categoria_producto y tipo_investment
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
# 🕒 GESTIÓN DE TIEMPO (RESTAURADA)
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
# 🟦 PROCESADOR BINGX (LÓGICA v5.5.6 COMPLETA)
# ==========================================================
def procesar_bingx(db, uid, ak, as_):
    cursor = db.cursor(dictionary=True)
    print(f"    [DEBUG] Iniciando BingX para User {uid}...")
    
    def bx_req(path, params=None):
        if params is None: params = {}
        ts = int(time.time()*1000)
        params["timestamp"] = ts
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        r = requests.get(url, headers={"X-BX-APIKEY": ak}, timeout=10).json()
        if r.get("code") != 0:
            print(f"    [!] Error API BingX: {r.get('msg')} (Code: {r.get('code')})")
        return r

    try:
        res_spot = bx_req("/openApi/spot/v1/account/balance")
        if res_spot.get("data"):
            for b in res_spot['data']['balances']:
                total = float(b['free']) + float(b['locked'])
                if total <= 0.000001: continue
                info = obtener_traductor_id(cursor, "bingx_crypto", b['asset'])
                registrar_saldo(cursor, uid, info, total, float(b['locked']), b['asset'], "BINGX", "SPOT")
            print(f"    [OK] BingX Saldo Spot procesado.")
    except Exception as e: print(f"    [!] Error BingX Spot: {e}")

    try:
        res_perp = bx_req("/openApi/swap/v2/user/balance")
        data_balance = res_perp.get("data", {})
        balances = data_balance if isinstance(data_balance, list) else ([data_balance.get("balance", {})] if "balance" in data_balance else [data_balance])
        for item in balances:
            ticker = item.get("asset")
            if not ticker or float(item.get("balance", 0)) <= 0: continue
            info = obtener_traductor_id(cursor, "bingx_usdt_future", ticker)
            registrar_saldo(cursor, uid, info, float(item['balance']), float(item.get('freezedMargin', 0)), ticker, "BINGX", "FUTURES")
        print(f"    [OK] BingX Saldos Perpetual procesado.")
    except Exception as e: print(f"    [!] Error BingX Perp: {e}")

# ==========================================================
# 🟨 PROCESADOR BINANCE (CON GESTIÓN DE TIEMPO)
# ==========================================================
def procesar_binance(db, uid, k, s):
    try:
        client = Client(k, s)
        cursor = db.cursor(dictionary=True)
        acc = client.get_account()
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.000001:
                info = obtener_traductor_id(cursor, "binance_spot", b['asset'])
                registrar_saldo(cursor, uid, info, total, float(b['locked']), b['asset'], "BINANCE", "SPOT")
        print(f"    [OK] Binance Saldos actualizado.")

        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", "trades_spot")
        cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente = 'binance_spot' AND is_active = 1")
        diccionario = cursor.fetchall()
        
        total_ingresados = 0
        for item in diccionario:
            pair = item['ticker_motor']
            if not any(pair.endswith(x) for x in ['USDT', 'BTC', 'ETH', 'BNB']): continue
            try:
                raw_trades = client.get_my_trades(symbol=pair, startTime=start_ts)
                if raw_trades:
                    print(f"    [DEBUG] {pair}: {len(raw_trades)} trades encontrados.")
                    for t in raw_trades:
                        t_f = {
                            'orderId': str(t['orderId']), 'symbol': t['symbol'], 
                            'side': 'BUY' if t['isBuyer'] else 'SELL', 'price': float(t['price']), 
                            'qty': float(t['qty']), 'quoteQty': float(t['quoteQty']), 
                            'commission': float(t['commission']), 'commissionAsset': t['commissionAsset'], 
                            'isMaker': t.get('isMaker'), 'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
                        }
                        if registrar_trade_completo(cursor, uid, t_f, item, "BINANCE"):
                            total_ingresados += 1
            except: continue
        
        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_spot", int(time.time()*1000))
        print(f"    [INFO] Binance Trades: {total_ingresados} registros procesados.")
    except Exception as e: print(f"    [!] Error Binance User {uid}: {e}")

# ==========================================================
# 🚀 EJECUCIÓN PRINCIPAL
# ==========================================================
def run():
    print(f"💎 MOTOR v6.3.6 - RESTAURACIÓN COMPLETA")
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