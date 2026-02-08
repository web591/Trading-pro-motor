import mysql.connector
from binance.client import Client
import time, os, base64, hmac, requests, hashlib
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# --- CONFIGURACIÓN DE SEGURIDAD ---
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

# --- EL RADAR (SISTEMA DE IDENTIFICACIÓN) ---
def motor_radar_v3(cur, symbol_bruto, user_id, motor_name):
    """
    Busca el ID en el traductor. Si no existe, lo manda a búsqueda.
    """
    # 1. Buscar por ticker_broker (según estructura PDF)
    cur.execute("SELECT id FROM sys_traductor_simbolos WHERE ticker_broker = %s LIMIT 1", (symbol_bruto,))
    res = cur.fetchone()
    if res: return res['id']
    
    # 2. Si no existe, limpiar para el Radar
    limpio = symbol_bruto.upper().replace('USDT', '').replace('USDC', '').replace('-', '').replace('PERP', '')
    if limpio.startswith('LD'): limpio = limpio[2:]
    
    # 3. Insertar en Radar (sys_busqueda_resultados)
    cur.execute("""
        INSERT IGNORE INTO sys_busqueda_resultados (ticker, estado, origen, motor, user_id, fecha_deteccion) 
        VALUES (%s, 'pendiente', 'sistema', %s, %s, NOW())
    """, (limpio, motor_name, user_id))
    return None

# --- REGISTRO CONTABLE (TRADES & FUTUROS) ---
def registrar_trade_v3(cur, user_id, broker_id, d):
    """
    Inserta en detalle_trades. 
    Nota: total_fees y pnl_neto son STORED GENERATED en el PDF, no se envían.
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

# --- MOTOR BINANCE (SPOT + FUTURES + CAPITAL) ---
def procesar_binance(k, s, user_id, db):
    client = Client(k, s)
    cur = db.cursor(dictionary=True)
    
    # 1. SALDOS SPOT Y MARGIN
    acc = client.get_account()
    for b in acc['balances']:
        tot = float(b['free']) + float(b['locked'])
        if tot > 0.000001:
            tid = motor_radar_v3(cur, b['asset'], user_id, 'binance_spot')
            cur.execute("""
                INSERT INTO sys_saldos_usuarios (user_id, broker_name, asset, traductor_id, cantidad_total, last_update)
                VALUES (%s, 'Binance', %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE cantidad_total=%s, traductor_id=%s, last_update=NOW()
            """, (user_id, b['asset'], tid, tot, tot, tid))

    # 2. TRADES DE FUTUROS (CON PNL BRUTO Y FUNDING)
    try:
        futures_trades = client.futures_account_trades(limit=50)
        for ft in futures_trades:
            registrar_trade_v3(cur, user_id, 1, {
                'id_ref': f"BN-FUT-{ft['id']}", 'symbol': ft['symbol'], 'qty': ft['qty'],
                'price': ft['price'], 'fee': ft['commission'], 'fee_asset': ft['commissionAsset'],
                'pnl': ft['realizedPnl'], 'time': ft['time'], 'funding': 0 # El funding en Binance va por Income History
            })
    except: pass

# --- MOTOR BINGX (SPOT + SWAP V2) ---
def procesar_bingx(k, s, user_id, db, session):
    cur = db.cursor(dictionary=True)
    def bx_req(path, params=None):
        p = params or {}; p["timestamp"] = int(time.time() * 1000)
        qs = "&".join([f"{k}={p[k]}" for k in sorted(p.keys())])
        sig = hmac.new(s.encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
        return session.get(f"https://open-api.bingx.com{path}?{qs}&signature={sig}", headers={'X-BX-APIKEY': k}).json()

    # 1. SALDOS BINGX
    res_s = bx_req("/openApi/spot/v1/account/balance")
    if res_s.get('code') == 0:
        for b in res_s['data']['balances']:
            tot = float(b['free']) + float(b['locked'])
            if tot > 0.000001:
                tid = motor_radar_v3(cur, b['asset'], user_id, 'bingx_spot')
                cur.execute("""
                    INSERT INTO sys_saldos_usuarios (user_id, broker_name, asset, traductor_id, cantidad_total, last_update)
                    VALUES (%s, 'BingX', %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE cantidad_total=%s, traductor_id=%s, last_update=NOW()
                """, (user_id, b['asset'], tid, tot, tot, tid))

    # 2. TRADES DE FUTUROS BINGX (SWAP V2)
    # Aquí capturamos el PNL realizado de BingX
    res_f = bx_req("/openApi/swap/v2/trade/allOrders", {"limit": 50})
    if res_f.get('code') == 0:
        for o in res_f['data']:
            if o['status'] == 'FILLED':
                registrar_trade_v3(cur, user_id, 2, {
                    'id_ref': f"BX-FUT-{o['orderId']}", 'symbol': o['symbol'], 'qty': o['executedQty'],
                    'price': o['avgPrice'], 'fee': o.get('commission', 0), 'fee_asset': 'USDT',
                    'pnl': o.get('realizedProfit', 0), 'time': o['updateTime']
                })

# --- EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    session = requests.Session()
    print("🚀 MOTOR v3.3.0 - BIBLIA CONTABLE INTEGRAL")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cur = db.cursor(dictionary=True)
            cur.execute("SELECT * FROM api_keys WHERE status=1")
            
            for u in cur.fetchall():
                key_d = descifrar_dato(u['api_key'], MASTER_KEY)
                sec_d = descifrar_dato(u['api_secret'], MASTER_KEY)
                if not key_d or not sec_d: continue
                
                print(f"📊 Procesando {u['broker_name']} | User: {u['user_id']}")
                if 'binance' in u['broker_name'].lower():
                    procesar_binance(key_d, sec_d, u['user_id'], db)
                elif 'bingx' in u['broker_name'].lower():
                    procesar_bingx(key_d, sec_d, u['user_id'], db, session)
            
            db.commit()
            db.close()
            print(f"✅ Ciclo finalizado. Espera: {ESPERA_CICLO}s")
            time.sleep(ESPERA_CICLO)
        except Exception as e:
            print(f"⚠️ Error Crítico: {e}")
            time.sleep(30)