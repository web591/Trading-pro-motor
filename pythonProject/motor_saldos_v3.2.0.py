import mysql.connector
from binance.client import Client
import time, os, base64, hmac, requests, hashlib
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# --- CONFIGURACIÓN MAESTRA ---
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
ESPERA_CICLO = 120 

def descifrar_dato(t, m):
    try:
        raw = base64.b64decode(t.strip())
        sep = b":::" if b":::" in raw else b"::"
        data, iv = raw.split(sep)
        cipher = AES.new(sha256(m.encode()).digest(), AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

# --- NÚCLEO DE INTELIGENCIA (RADAR & TRADUCTOR) ---
def obtener_id_traductor(cur, ticker_bruto, user_id, broker_id, motor_fuente):
    """
    Busca el ID en el traductor. Si no existe, dispara el Radar.
    """
    # Limpieza estándar
    s = ticker_bruto.upper().replace('-', '').replace('_', '').replace('PERP', '')
    if s.startswith('LD'): s = s[2:]
    
    # 1. Intento de búsqueda directa
    cur.execute("""
        SELECT id FROM sys_traductor_simbolos 
        WHERE ticker_broker = %s AND motor_fuente = %s
    """, (ticker_bruto, motor_fuente))
    res = cur.fetchone()
    
    if res: return res['id']
    
    # 2. Si falla, buscar moneda base para ver si es 'mapeo_pendiente'
    ticker_base = s.replace('USDT', '').replace('USDC', '').replace('BTC', '').replace('ETH', '')
    cur.execute("SELECT id FROM sys_traductor_simbolos WHERE nombre_comun = %s LIMIT 1", (ticker_base,))
    conocida = cur.fetchone()
    
    estado = 'mapeo_pendiente' if conocida else 'detectado'
    
    # 3. Registrar en Radar
    cur.execute("""
        INSERT IGNORE INTO sys_busqueda_resultados 
        (ticker, estado, origen, motor, user_id, fecha_deteccion) 
        VALUES (%s, %s, 'sistema', %s, %s, NOW())
    """, (ticker_base, estado, motor_fuente, user_id))
    
    return None

# --- REGISTRO CONTABLE ÚNICO ---
def registrar_movimiento_contable(cur, user_id, broker_id, d):
    """
    Escribe en detalle_trades. id_externo_ref previene duplicados.
    """
    sql = """
        INSERT IGNORE INTO detalle_trades 
        (id_externo_ref, user_id, broker_id, symbol, cantidad_original, 
         precio_original, fee_trading, fee_funding, moneda_fee, pnl_bruto, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s/1000))
    """
    cur.execute(sql, (
        d['id_ref'], user_id, broker_id, d['symbol'], d['qty'], 
        d['price'], d['fee'], d.get('funding', 0), d['fee_asset'], d['pnl'], d['time']
    ))

# --- MÓDULO BINANCE ---
def motor_binance(key, sec, user_id, db):
    client = Client(key, sec)
    cur = db.cursor(dictionary=True)
    
    # Saldos Spot
    for b in client.get_account()['balances']:
        tot = float(b['free']) + float(b['locked'])
        if tot > 0.000001:
            tid = obtener_id_traductor(cur, b['asset'], user_id, 1, 'binance_spot')
            cur.execute("""
                INSERT INTO sys_saldos_usuarios (user_id, broker_name, asset, traductor_id, cantidad_total, last_update)
                VALUES (%s, 'Binance', %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE cantidad_total=%s, traductor_id=%s, last_update=NOW()
            """, (user_id, b['asset'], tid, tot, tot, tid))
    
    # Trades Recientes (Ejemplo BTCUSDT)
    # En producción, esto iteraría sobre los símbolos activos del usuario
    try:
        for t in client.get_my_trades(symbol='BTCUSDT', limit=20):
            registrar_movimiento_contable(cur, user_id, 1, {
                'id_ref': f"BN-T-{t['id']}", 'symbol': 'BTCUSDT', 'qty': t['qty'],
                'price': t['price'], 'fee': t['commission'], 'fee_asset': t['commissionAsset'],
                'pnl': 0, 'time': t['time']
            })
    except: pass

# --- MÓDULO BINGX ---
def motor_bingx(key, sec, user_id, db):
    cur = db.cursor(dictionary=True)
    host = "https://open-api.bingx.com"
    
    def req(path, params=None):
        p = params or {}
        p["timestamp"] = int(time.time() * 1000)
        qs = "&".join([f"{k}={p[k]}" for k in sorted(p.keys())])
        sig = hmac.new(sec.encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
        return requests.get(f"{host}{path}?{qs}&signature={sig}", headers={'X-BX-APIKEY': key}).json()

    # Saldos Spot BingX
    res = req("/openApi/spot/v1/account/balance")
    if res.get('code') == 0:
        for b in res['data']['balances']:
            tot = float(b['free']) + float(b['locked'])
            if tot > 0.000001:
                tid = obtener_id_traductor(cur, b['asset'], user_id, 2, 'bingx_spot')
                cur.execute("""
                    INSERT INTO sys_saldos_usuarios (user_id, broker_name, asset, traductor_id, cantidad_total, last_update)
                    VALUES (%s, 'BingX', %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE cantidad_total=%s, traductor_id=%s, last_update=NOW()
                """, (user_id, b['asset'], tid, tot, tot, tid))

    # Trades BingX (Perpetuos - Muy importante para PNL y Funding)
    res_t = req("/openApi/swap/v2/trade/allOrders", {"symbol": "BTC-USDT", "limit": 20})
    if res_t.get('code') == 0:
        for o in res_t['data']:
            if o['status'] == 'FILLED':
                registrar_movimiento_contable(cur, user_id, 2, {
                    'id_ref': f"BX-T-{o['orderId']}", 'symbol': o['symbol'], 'qty': o['executedQty'],
                    'price': o['avgPrice'], 'fee': o.get('commission', 0), 'fee_asset': 'USDT',
                    'pnl': o.get('realizedProfit', 0), 'time': o['updateTime']
                })

# --- EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cur = db.cursor(dictionary=True)
            cur.execute("SELECT * FROM api_keys WHERE status=1")
            
            for u in cur.fetchall():
                k = descifrar_dato(u['api_key'], MASTER_KEY)
                s = descifrar_dato(u['api_secret'], MASTER_KEY)
                if not k or not s: continue
                
                print(f"🔄 Procesando {u['broker_name']} - Usuario: {u['user_id']}")
                
                if 'binance' in u['broker_name'].lower():
                    motor_binance(k, s, u['user_id'], db)
                elif 'bingx' in u['broker_name'].lower():
                    motor_bingx(k, s, u['user_id'], db)
            
            db.commit()
            db.close()
            print(f"✅ Ciclo finalizado. Esperando {ESPERA_CICLO}s...")
            time.sleep(ESPERA_CICLO)
        except Exception as e:
            print(f"⚠️ Error en motor: {e}")
            time.sleep(30)