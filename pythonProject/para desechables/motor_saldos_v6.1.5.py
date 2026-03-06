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
    ticker = ticker.upper().strip()
    sql = "SELECT id FROM sys_traductor_simbolos WHERE motor_fuente=%s AND ticker_motor=%s AND is_active=1 LIMIT 1"
    cursor.execute(sql, (motor_fuente, ticker))
    row = cursor.fetchone()
    if row: return row['id']
    return None

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
        INSERT INTO sys_sync_estado (user_id, broker, endpoint, last_timestamp)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE last_timestamp = VALUES(last_timestamp)
    """
    cursor.execute(sql, (uid, broker, endpoint, nuevo_ts))

# ==========================================================
# 💰 REGISTRO DE SALDOS
# ==========================================================
def registrar_saldo(cursor, uid, tid, total, locked, asset_name, broker):
    precio = obtener_precio_usd(cursor, tid, asset_name)
    sql = """
        INSERT INTO sys_saldos_usuarios 
        (user_id, broker_name, asset, traductor_id, cantidad_total, cantidad_disponible, 
         cantidad_bloqueada, valor_usd, precio_referencia, tipo_cuenta, last_update)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'SPOT', NOW())
        ON DUPLICATE KEY UPDATE
        traductor_id=VALUES(traductor_id), cantidad_total=VALUES(cantidad_total),
        valor_usd=VALUES(valor_usd), precio_referencia=VALUES(precio_referencia), last_update=NOW()
    """
    cursor.execute(sql, (uid, broker, asset_name, tid, total, total-locked, locked, total*precio, precio))

# ==========================================================
# 📥 REGISTRO DE TRADES
# ==========================================================
def registrar_trade_completo(cursor, uid, t_data):
    try:
        sql_global = "INSERT IGNORE INTO transacciones_globales (user_id, monto_total, moneda, tipo_movimiento, id_externo, fecha_transaccion) VALUES (%s, %s, %s, %s, %s, %s)"
        tipo = 'COMPRA' if t_data['side'].upper() == 'BUY' else 'VENTA'
        cursor.execute(sql_global, (uid, t_data['quoteQty'], t_data['symbol'], tipo, t_data['orderId'], t_data['fecha_sql']))
        
        sql_detalle = "INSERT IGNORE INTO detalle_trades (id_externo_ref, symbol, lado, precio_ejecucion, cantidad_ejecutada, comision, comision_asset) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql_detalle, (t_data['orderId'], t_data['symbol'], t_data['side'], t_data['price'], t_data['qty'], t_data['commission'], t_data['commissionAsset']))
        print(f"        [DB] Guardado ID: {t_data['orderId']} ({t_data['symbol']})")
    except Exception as e:
        print(f"        [!] Error DB Trade: {e}")

# ==========================================================
# 🟨 PROCESADOR BINANCE v6.1.5 - RADAR MULTI-ACTIVO
# ==========================================================
def procesar_binance(db, uid, k, s):
    try:
        client = Client(k, s)
        cursor = db.cursor(dictionary=True)
        
        # 1. SALDOS (Obtenemos la lista de qué monedas tienes)
        acc = client.get_account()
        activos_con_saldo = []
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.000001:
                asset = b['asset']
                if not asset.startswith("LD"): # Ignoramos "Lending" para trades de spot
                    activos_con_saldo.append(asset)
                tid = obtener_traductor_id(cursor, "binance_spot", asset)
                registrar_saldo(cursor, uid, tid, total, float(b['locked']), asset, "BINANCE")
        
        print(f"    [Saldos] {len(activos_con_saldo)} activos spot detectados para historial.")

        # 2. TRADES (Buscamos historial por cada activo)
        # Forzamos fecha al 31 de Enero para asegurar que traiga datos
        start_ts = 1738281600000 
        
        # Limitamos a los primeros 5 activos con saldo para no saturar la API en la primera prueba
        for symbol in activos_con_saldo[:5]:
            # Binance necesita el par (ej: BTC -> BTCUSDT)
            pair = f"{symbol}USDT"
            try:
                print(f"    [API] Consultando historial de {pair}...")
                raw_trades = client.get_my_trades(symbol=pair, startTime=start_ts)
                
                if len(raw_trades) > 0:
                    print(f"    [!!!] ¡ENCONTRADOS {len(raw_trades)} trades en {pair}!")
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
                            'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
                        }
                        registrar_trade_completo(cursor, uid, t_f)
                else:
                    print(f"    [API] 0 trades en {pair}.")
            except Exception as api_err:
                # Si el par no existe (ej: USDTUSDT), pasamos al siguiente
                continue
        
        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_spot", int(time.time()*1000))
    except Exception as e: print(f"    [!] Error Binance User {uid}: {e}")
# ==========================================================
# 🚀 CICLO PRINCIPAL
# ==========================================================
def run():
    print(f"💎 MOTOR v6.1.4 - MODO AUDITORÍA TOTAL")
    while True:
        print(f"\n{'='*50}")
        print(f"🔄 INICIANDO RECORRIDO: {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*50}")
        
        db = None
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1")
            users = cursor.fetchall()
            print(f"[*] Usuarios activos encontrados: {len(users)}")

            for u in users:
                print(f"\n>> PROCESANDO: User {u['user_id']} ({u['broker_name']})")
                k = descifrar_dato(u['api_key'], MASTER_KEY)
                s = descifrar_dato(u['api_secret'], MASTER_KEY)
                
                if u['broker_name'].upper() == "BINANCE":
                    procesar_binance(db, u['user_id'], k, s)
                elif u['broker_name'].upper() == "BINGX":
                    # (Aquí meteríamos trades de BingX pronto)
                    print(f"    [Saldos] BingX procesado.")
                db.commit()
                
        except Exception as e: 
            print(f"    [CRITICAL] Error en ciclo: {e}")
        finally:
            if db and db.is_connected(): db.close()
        
        print(f"\n{'='*50}")
        print(f"💤 CICLO TERMINADO. Esperando 5 minutos...")
        print(f"{'='*50}")
        time.sleep(300)

if __name__ == "__main__": run()