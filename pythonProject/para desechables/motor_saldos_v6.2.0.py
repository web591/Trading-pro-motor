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
# 🕒 GESTIÓN DE TIEMPO (Sincronización)
# ==========================================================
def obtener_punto_inicio_sincro(cursor, uid, broker, endpoint):
    sql = "SELECT last_timestamp FROM sys_sync_estado WHERE user_id = %s AND broker = %s AND endpoint = %s LIMIT 1"
    cursor.execute(sql, (uid, broker, endpoint))
    row = cursor.fetchone()
    # Si no hay fecha, iniciamos en 31 de Enero 2026
    return int(row['last_timestamp']) if row and row['last_timestamp'] else 1738281600000

def actualizar_punto_sincro(cursor, uid, broker, endpoint, nuevo_ts):
    sql = """
        INSERT INTO sys_sync_estado (user_id, broker, endpoint, last_timestamp, last_update)
        VALUES (%s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE last_timestamp = VALUES(last_timestamp), last_update = NOW()
    """
    cursor.execute(sql, (uid, broker, endpoint, nuevo_ts))

# ==========================================================
# 📥 REGISTRO DE TRADES
# ==========================================================
def registrar_trade_completo(cursor, uid, t_data):
    try:
        # transacciones_globales
        sql_global = """
            INSERT IGNORE INTO transacciones_globales 
            (user_id, monto_fiat, moneda, tipo_movimiento, id_externo, fecha_creacion) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        tipo = 'COMPRA' if t_data['side'].upper() == 'BUY' else 'VENTA'
        cursor.execute(sql_global, (uid, t_data['quoteQty'], t_data['symbol'], tipo, t_data['orderId'], t_data['fecha_sql']))
        
        # detalle_trades
        sql_detalle = """
            INSERT IGNORE INTO detalle_trades 
            (id_externo_ref, symbol, lado, precio_ejecucion, cantidad_ejecutada, comision, comision_asset) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql_detalle, (t_data['orderId'], t_data['symbol'], t_data['side'], t_data['price'], t_data['qty'], t_data['commission'], t_data['commissionAsset']))
        return True
    except Exception as e:
        print(f"        [!] Error DB: {e}")
        return False

# ==========================================================
# 🟨 PROCESADOR BINANCE (Dual)
# ==========================================================
def procesar_binance(db, uid, k, s):
    try:
        client = Client(k, s)
        cursor = db.cursor(dictionary=True)
        
        # 1. SALDOS
        acc = client.get_account()
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0.000001:
                tid = obtener_traductor_id(cursor, "binance_spot", b['asset'])
                precio = obtener_precio_usd(cursor, tid, b['asset'])
                sql_s = "INSERT INTO sys_saldos_usuarios (user_id, broker_name, asset, traductor_id, cantidad_total, cantidad_disponible, cantidad_bloqueada, valor_usd, precio_referencia, tipo_cuenta, last_update) VALUES (%s, 'BINANCE', %s, %s, %s, %s, %s, %s, %s, 'SPOT', NOW()) ON DUPLICATE KEY UPDATE cantidad_total=VALUES(cantidad_total), valor_usd=VALUES(valor_usd), last_update=NOW()"
                cursor.execute(sql_s, (uid, b['asset'], tid, total, float(b['free']), float(b['locked']), total*precio, precio))
        print(f"    [OK] Saldos Actualizados.")

        # 2. HISTORIAL (Radar por Diccionario)
        cursor.execute("SELECT ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente = 'binance_spot' AND is_active = 1")
        diccionario = cursor.fetchall()
        
        start_ts = obtener_punto_inicio_sincro(cursor, uid, "BINANCE", "trades_spot")
        print(f"    [Radar] Escaneando desde: {datetime.fromtimestamp(start_ts/1000)}")
        
        trades_nuevos_totales = 0
        for item in diccionario:
            pair = item['ticker_motor']
            if not any(pair.endswith(x) for x in ['USDT', 'BTC', 'ETH']): continue
            
            try:
                raw = client.get_my_trades(symbol=pair, startTime=start_ts)
                if raw:
                    for t in raw:
                        t_f = {
                            'orderId': str(t['orderId']), 'symbol': t['symbol'], 'side': 'BUY' if t['isBuyer'] else 'SELL',
                            'price': float(t['price']), 'qty': float(t['qty']), 'quoteQty': float(t['quoteQty']),
                            'commission': float(t['commission']), 'commissionAsset': t['commissionAsset'],
                            'fecha_sql': datetime.fromtimestamp(t['time']/1000).strftime('%Y-%m-%d %H:%M:%S')
                        }
                        if registrar_trade_completo(cursor, uid, t_f):
                            trades_nuevos_totales += 1
            except: continue
        
        print(f"    [Historial] Se detectaron e insertaron {trades_nuevos_totales} movimientos nuevos.")
        actualizar_punto_sincro(cursor, uid, "BINANCE", "trades_spot", int(time.time()*1000))
        
    except Exception as e: print(f"    [!] Error User {uid}: {e}")

# ==========================================================
# 🚀 MOTOR PRINCIPAL
# ==========================================================
def run():
    print(f"💎 MOTOR v6.2.0 - SINCRONIZADOR MAESTRO")
    while True:
        print(f"\n{'='*60}\n🔄 INICIO RECORRIDO: {datetime.now().strftime('%H:%M:%S')}\n{'='*60}")
        db = mysql.connector.connect(**config.DB_CONFIG)
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1")
        users = cursor.fetchall()
        
        for u in users:
            print(f">> User {u['user_id']} ({u['broker_name']})")
            k, s = descifrar_dato(u['api_key'], MASTER_KEY), descifrar_dato(u['api_secret'], MASTER_KEY)
            if u['broker_name'].upper() == "BINANCE":
                procesar_binance(db, u['user_id'], k, s)
            db.commit()
        
        db.close()
        print(f"\n{'='*60}\n✅ RECORRIDO FINALIZADO\n{'='*60}")
        time.sleep(300)

if __name__ == "__main__": run()