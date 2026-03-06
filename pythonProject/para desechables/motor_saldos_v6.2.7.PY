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
    sql = "SELECT id FROM sys_traductor_simbolos WHERE motor_fuente=%s AND ticker_motor=%s AND is_active=1 LIMIT 1"
    cursor.execute(sql, (motor_fuente, ticker))
    row = cursor.fetchone()
    return row['id'] if row else None

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

# ==========================================================
# 🕒 GESTIÓN DE TIEMPO
# ==========================================================
def obtener_punto_inicio_sincro(cursor, uid, broker, endpoint):
    sql = "SELECT last_timestamp FROM sys_sync_estado WHERE user_id = %s AND broker = %s AND endpoint = %s LIMIT 1"
    cursor.execute(sql, (uid, broker, endpoint))
    row = cursor.fetchone()
    return int(row['last_timestamp']) if row and row['last_timestamp'] else 1738281600000

def actualizar_punto_sincro(cursor, uid, broker, endpoint, nuevo_ts):
    sql = """
        INSERT INTO sys_sync_estado (user_id, broker, endpoint, last_timestamp, last_update)
        VALUES (%s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE last_timestamp = VALUES(last_timestamp), last_update = NOW()
    """
    cursor.execute(sql, (uid, broker, endpoint, nuevo_ts))

# ==========================================================
# 📥 REGISTRO DE TRADES (Sincronizado con transacciones_globales)
# ==========================================================
def registrar_trade_completo(cursor, uid, t_data):
    try:
        # 1. INSERT EN TRANSACCIONES_GLOBALES
        sql_global = """
            INSERT IGNORE INTO transacciones_globales 
            (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, comision, fecha_utc, broker) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        valores_global = (
            t_data['orderId'],      # id_externo
            uid,                    # user_id
            'BINANCE',              # exchange
            'SPOT',                 # cuenta_tipo
            'TRADE',                # categoria
            t_data['symbol'],       # asset
            t_data['quoteQty'],     # monto_neto (valor en USDT)
            t_data['commission'],   # comision
            t_data['fecha_sql'],    # fecha_utc
            'BINANCE'               # broker
        )
        cursor.execute(sql_global, valores_global)

        # 2. INSERT EN DETALLE_TRADES (Aquí estaba el fallo)
        # Agregamos user_id, broker y trade_id_externo que son OBLIGATORIOS en tu tabla
        sql_detalle = """
            INSERT IGNORE INTO detalle_trades 
            (user_id, exchange, tipo_producto, exchange_fuente, tipo_mercado, 
             id_externo_ref, fecha_utc, symbol, lado, precio_ejecucion, 
             cantidad_ejecutada, broker, trade_id_externo) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Generamos un trade_id_externo único para que la DB no lo rechace
        trade_id_unico = f"TRD-{t_data['orderId']}"
        
        valores_detalle = (
            uid,                    # user_id (OBLIGATORIO)
            'BINANCE',              # exchange
            'SPOT',                 # tipo_producto
            'BINANCE',              # exchange_fuente
            'SPOT',                 # tipo_mercado
            t_data['orderId'],      # id_externo_ref (Vínculo con la global)
            t_data['fecha_sql'],    # fecha_utc
            t_data['symbol'],       # symbol
            t_data['side'],         # lado
            t_data['price'],        # precio_ejecucion
            t_data['qty'],          # cantidad_ejecutada
            'BINANCE',              # broker (OBLIGATORIO)
            trade_id_unico          # trade_id_externo (OBLIGATORIO)
        )
        
        cursor.execute(sql_detalle, valores_detalle)
        
        # Si llegamos aquí, se guardó en AMBAS
        print(f"        [✓] REGISTRO DOBLE OK: {t_data['symbol']} | ID: {t_data['orderId']}")
        return True

    except Exception as e:
        # Esto te dirá exactamente qué columna falta si vuelve a fallar
        print(f"        [!] ERROR CRÍTICO DB: {e}")
        return False

# ==========================================================
# 🟨 PROCESADOR BINANCE
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
                tid = obtener_traductor_id(cursor, "binance_spot", b['asset'])
                precio = obtener_precio_usd(cursor, tid, b['asset'])
                sql_s = "INSERT INTO sys_saldos_usuarios (user_id, broker_name, asset, traductor_id, cantidad_total, cantidad_disponible, cantidad_bloqueada, valor_usd, precio_referencia, tipo_cuenta, last_update) VALUES (%s, 'BINANCE', %s, %s, %s, %s, %s, %s, %s, 'SPOT', NOW()) ON DUPLICATE KEY UPDATE cantidad_total=VALUES(cantidad_total), valor_usd=VALUES(valor_usd), last_update=NOW()"
                cursor.execute(sql_s, (uid, b['asset'], tid, total, float(b['free']), float(b['locked']), total*precio, precio))
        print(f"    [OK] Radar Saldos actualizado.")

        # HISTORIAL
        cursor.execute("SELECT ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente = 'binance_spot' AND is_active = 1")
        diccionario = cursor.fetchall()
        
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", "trades_spot")
        print(f"    [Radar] Analizando {len(diccionario)} pares desde {datetime.fromtimestamp(start_ts/1000)}...")

        total_ingresados = 0
        for item in diccionario:
            pair = item['ticker_motor']
            if not any(pair.endswith(x) for x in ['USDT', 'BTC', 'ETH', 'BNB']): continue
            
            try:
                raw_trades = client.get_my_trades(symbol=pair, startTime=start_ts)
                if raw_trades:
                    print(f"    [API] {len(raw_trades)} trades en {pair}")
                    for t in raw_trades:
                        t_f = {
                            'orderId': str(t['orderId']), 'symbol': t['symbol'], 
                            'side': 'BUY' if t['isBuyer'] else 'SELL',
                            'price': float(t['price']), 'qty': float(t['qty']), 
                            'quoteQty': float(t['quoteQty']),
                            'commission': float(t['commission']), 'commissionAsset': t['commissionAsset'],
                            'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
                        }
                        if registrar_trade_completo(cursor, uid, t_f):
                            total_ingresados += 1
            except: continue 
        
        print(f"    [INFO] Finalizado: {total_ingresados} registros guardados.")
        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_spot", int(time.time()*1000))

    except Exception as e: print(f"    [!] Error User {uid}: {e}")

# ==========================================================
# 🚀 MOTOR
# ==========================================================
def run():
    print(f"💎 MOTOR v6.2.4 - PRODUCCIÓN VALIDADA")
    while True:
        print(f"\n{'='*65}\n🔄 INICIO CICLO: {datetime.now().strftime('%H:%M:%S')}\n{'='*65}")
        db = None
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1")
            users = cursor.fetchall()
            for u in users:
                print(f">> TRABAJANDO: User {u['user_id']} | {u['broker_name']}")
                k, s = descifrar_dato(u['api_key'], MASTER_KEY), descifrar_dato(u['api_secret'], MASTER_KEY)
                if u['broker_name'].upper() == "BINANCE":
                    procesar_binance(db, u['user_id'], k, s)
                db.commit()
        except Exception as e: print(f"    [CRITICAL] {e}")
        finally:
            if db and db.is_connected(): db.close()
        
        print(f"\n{'='*65}\n✅ CICLO TERMINADO\n{'='*65}")
        time.sleep(300)

if __name__ == "__main__": run()