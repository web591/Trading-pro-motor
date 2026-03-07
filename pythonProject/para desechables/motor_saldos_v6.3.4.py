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

def obtener_datos_traductor(cursor, motor_fuente, ticker):
    # Ajustado a tus columnas reales: categoria_producto, tipo_investment
    sql = "SELECT id, categoria_producto, tipo_investment, motor_fuente FROM sys_traductor_simbolos WHERE motor_fuente=%s AND ticker_motor=%s AND is_active=1 LIMIT 1"
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

def registrar_saldo(cursor, uid, tid, total, locked, asset, broker, tipo_cuenta):
    precio = obtener_precio_usd(cursor, tid, asset)
    valor_usd = total * precio
    sql = """
        INSERT INTO sys_saldos_usuarios 
        (user_id, broker_name, asset, traductor_id, cantidad_total, cantidad_disponible, cantidad_bloqueada, valor_usd, precio_referencia, tipo_cuenta, last_update) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()) 
        ON DUPLICATE KEY UPDATE 
            cantidad_total=VALUES(cantidad_total), 
            cantidad_disponible=VALUES(cantidad_disponible),
            cantidad_bloqueada=VALUES(cantidad_bloqueada),
            valor_usd=VALUES(valor_usd), 
            precio_referencia=VALUES(precio_referencia),
            last_update=NOW()
    """
    cursor.execute(sql, (uid, broker, asset, tid, total, total-locked, locked, valor_usd, precio, tipo_cuenta))

# ==========================================================
# 🕒 GESTIÓN DE TIEMPO Y TRADES
# ==========================================================
def obtener_punto_inicio_sincro(cursor, uid, broker, endpoint):
    sql = "SELECT last_timestamp FROM sys_sync_estado WHERE user_id = %s AND broker = %s AND endpoint = %s LIMIT 1"
    cursor.execute(sql, (uid, broker, endpoint))
    row = cursor.fetchone()
    # Si no hay registro, por defecto 01 Ene 2026
    return int(row['last_timestamp']) if row and row['last_timestamp'] else 1735689600000

def actualizar_punto_sincro(cursor, uid, broker, endpoint, nuevo_ts):
    sql = """
        INSERT INTO sys_sync_estado (user_id, broker, endpoint, last_timestamp, last_update)
        VALUES (%s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE last_timestamp = VALUES(last_timestamp), last_update = NOW()
    """
    cursor.execute(sql, (uid, broker, endpoint, nuevo_ts))

def registrar_trade_completo(cursor, uid, t_data, info_traductor):
    try:
        # MAPEO CORRECTO SEGÚN TU TABLA sys_traductor_simbolos
        # categoria_producto -> Ej: 'SPOT'
        # tipo_investment -> Ej: 'CRYPTO'
        tipo_prod = info_traductor['categoria_producto'] if info_traductor else 'SPOT'
        tipo_merc = info_traductor['tipo_investment'] if info_traductor else 'SPOT'
        broker_f  = 'BINANCE' # Por ahora fijo para Binance, se puede dinamizar

        id_vinculo = f"{uid}-{t_data['orderId']}"
        
        # 1. INSERT EN TRANSACCIONES_GLOBALES
        sql_global = """
            INSERT IGNORE INTO transacciones_globales 
            (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, comision, fecha_utc, broker) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql_global, (
            id_vinculo, uid, broker_f, tipo_prod, 'TRADE', 
            t_data['symbol'], t_data['quoteQty'], t_data['commission'], 
            t_data['fecha_sql'], broker_f
        ))

        # 2. INSERT EN DETALLE_TRADES (ADN Completo)
        sql_detalle = """
            INSERT IGNORE INTO detalle_trades 
            (user_id, exchange, tipo_producto, exchange_fuente, tipo_mercado, 
             id_externo_ref, fecha_utc, symbol, lado, precio_ejecucion, 
             cantidad_ejecutada, commission, commission_asset, quote_qty, 
             is_maker, broker, trade_id_externo, raw_json) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        raw_json_str = json.dumps(t_data)
        valores_detalle = (
            uid, broker_f, tipo_prod, broker_f, tipo_merc,
            id_vinculo, t_data['fecha_sql'], t_data['symbol'], t_data['side'], 
            t_data['price'], t_data['qty'], t_data['commission'], 
            t_data['commissionAsset'], t_data['quoteQty'],
            1 if t_data.get('isMaker') else 0, broker_f, 
            f"TRD-{t_data['orderId']}", raw_json_str
        )
        cursor.execute(sql_detalle, valores_detalle)
        return True
    except Exception as e:
        print(f"        [!] ERROR DE INSERCIÓN DETALLE: {e}")
        return False

# ==========================================================
# 🟨 PROCESADOR BINANCE
# ==========================================================
def procesar_binance(db, uid, k, s):
    try:
        client = Client(k, s)
        cursor = db.cursor(dictionary=True)
        
        # --- 1. PROCESAR SALDOS ---
        acc = client.get_account()
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.000001:
                info = obtener_datos_traductor(cursor, "binance_spot", b['asset'])
                tid = info['id'] if info else None
                registrar_saldo(cursor, uid, tid, total, float(b['locked']), b['asset'], "BINANCE", "SPOT")
        print(f"    [OK] Binance Saldos actualizado.")

        # --- 2. PROCESAR TRADES (HISTORIAL) ---
        cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE motor_fuente = 'binance_spot' AND is_active = 1")
        diccionario = cursor.fetchall()
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", "trades_spot")
        
        total_ingresados = 0
        for item in diccionario:
            pair = item['ticker_motor']
            # Filtro básico de seguridad para pares comunes
            if not any(pair.endswith(x) for x in ['USDT', 'BTC', 'ETH', 'BNB', 'FDUSD']): continue
            
            try:
                raw_trades = client.get_my_trades(symbol=pair, startTime=start_ts)
                if raw_trades:
                    print(f"    [DEBUG] {pair}: {len(raw_trades)} trades encontrados.")
                    for t in raw_trades:
                        t_f = {
                            'orderId': str(t['orderId']), 
                            'symbol': t['symbol'], 
                            'side': 'BUY' if t['isBuyer'] else 'SELL', 
                            'price': float(t['price']), 
                            'qty': float(t['qty']), 
                            'quoteQty': float(t['quoteQty']), 
                            'commission': float(t['commission']), 
                            'commissionAsset': t['commissionAsset'],
                            'isMaker': t.get('isMaker'),
                            'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
                        }
                        if registrar_trade_completo(cursor, uid, t_f, item):
                            total_ingresados += 1
            except Exception as e:
                continue
                
        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_spot", int(time.time()*1000))
        print(f"    [INFO] Binance Trades: {total_ingresados} registros procesados.")
        
    except Exception as e:
        print(f"    [!] Error crítico Binance User {uid}: {e}")

# ==========================================================
# 🚀 EJECUCIÓN
# ==========================================================
def run():
    print(f"💎 MOTOR v6.3.3 - MULTI-BROKER (PRUEBA 2026)")
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
                    procesar_binance(db, u['user_id'], k, s)
                # (Aquí iría BingX siguiendo la misma lógica)
                
                db.commit()
        except Exception as e:
            print(f"    [CRITICAL] Error en bucle principal: {e}")
        finally:
            if db and db.is_connected():
                db.close()
        
        print(f"\n{'='*65}\n✅ CICLO TERMINADO - ESPERANDO 5 MIN\n{'='*65}")
        time.sleep(300)

if __name__ == "__main__":
    run()