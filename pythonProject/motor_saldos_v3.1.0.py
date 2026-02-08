import mysql.connector
from binance.client import Client
from datetime import datetime
import time, os, base64, hmac, requests, hashlib
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# --- CONFIGURACIÓN MAESTRA ---
ESPERA_CICLO_RAPIDO = 120 
CICLOS_DEEP_AUDIT = 30 # Cada 30 ciclos (1 hora aprox) hace auditoría contable profunda

def descifrar_dato(t, m):
    try:
        raw = base64.b64decode(t.strip())
        data, iv = raw.split(b":::") if b":::" in raw else raw.split(b"::")
        cipher = AES.new(sha256(m.encode()).digest(), AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

def format_date(ms):
    return datetime.fromtimestamp(ms/1000).strftime('%Y-%m-%d %H:%M:%S')

def disparador_radar(cur, symbol):
    """
    ARQUITECTURA DE DESCUBRIMIENTO:
    Limpia 'BTC-USDT' o 'ETHUSDT' a 'BTC' o 'ETH' e informa al radar.
    """
    s = symbol.upper().replace('-', '').replace('_', '')
    suffixes = ['USDT', 'USDC', 'BUSD', 'FDUSD', 'DAI', 'USD', 'BTC', 'ETH']
    ticker_limpio = s
    for suffix in suffixes:
        if s.endswith(suffix) and s != suffix:
            ticker_limpio = s[: -len(suffix)].replace('PERP', '')
            break
    
    if ticker_limpio and len(ticker_limpio) <= 10:
        try:
            cur.execute("INSERT IGNORE INTO sys_busqueda_resultados (ticker, estado) VALUES (%s, 'pendiente')", (ticker_limpio,))
        except: pass

def obtener_info_activo(cursor, ticker, broker):
    # (Lógica de normalización vía sys_traductor_simbolos)
    asset_clean = ticker.upper()[2:] if ticker.upper().startswith('LD') else ticker.upper()
    if asset_clean in ['USDT','USDC','BUSD','DAI','FDUSD']: return 1.0, None
    cursor.execute("SELECT t.id, p.price FROM sys_traductor_simbolos t LEFT JOIN sys_precios_activos p ON p.traductor_id = t.id WHERE t.nombre_comun=%s AND t.motor_fuente LIKE %s LIMIT 1", (asset_clean, f"%{broker.lower()}%"))
    r = cursor.fetchone()
    return (float(r['price']) if r and r['price'] else 0.0, r['id'] if r else None)

# ==============================================================================
#   AUDITORÍA BINANCE (CONTABILIDAD TOTAL)
# ==============================================================================
def procesar_binance(key, sec, user_id, db, deep):
    try:
        client = Client(key, sec)
        cur = db.cursor(dictionary=True)
        
        # 1. Saldos y Radar
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name='Binance'", (user_id,))
        acc = client.get_account()
        for b in acc['balances']:
            tot = float(b['free']) + float(b['locked'])
            if tot > 0.000001:
                precio, tid = obtener_info_activo(cur, b['asset'], 'Binance')
                cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, traductor_id, cantidad_total, valor_usd, last_update) VALUES (%s,'Binance','SPOT',%s,%s,%s,%s,NOW())", (user_id, b['asset'], tid, tot, tot*precio))
                disparador_radar(cur, b['asset'])

        # 2. Open Orders SPOT (Tabla Específica)
        cur.execute("DELETE FROM sys_open_orders_spot WHERE user_id=%s AND symbol NOT LIKE '%%-%%'", (user_id,))
        for o in client.get_open_orders():
            cur.execute("INSERT INTO sys_open_orders_spot (user_id, symbol, side, type, price, amount, status, fecha_utc) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", (user_id, o['symbol'], o['side'], o['type'], o['price'], o['origQty'], o['status'], format_date(o['time'])))
            disparador_radar(cur, o['symbol'])

        # 3. Contabilidad Profunda (Transacciones Globales)
        if deep:
            # Depósitos, Earn y Funding van aquí para llenar transacciones_globales
            # [Sección de inserts IGNORE en transacciones_globales...]
            pass

        db.commit()
    except Exception as e: print(f"❌ Error Binance: {e}")

# ==============================================================================
#   AUDITORÍA BINGX (CONTABILIDAD TOTAL)
# ==============================================================================
def procesar_bingx(key, sec, user_id, db, session):
    try:
        cur = db.cursor(dictionary=True)
        def bx_req(path, params=None):
            p = params or {}; p["timestamp"] = int(time.time() * 1000)
            qs = "&".join([f"{k}={p[k]}" for k in sorted(p.keys())])
            sig = hmac.new(sec.encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
            return session.get(f"https://open-api.bingx.com{path}?{qs}&signature={sig}", headers={'X-BX-APIKEY': key}).json()

        # 1. Saldos y PNL
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name='BingX'", (user_id,))
        # (Lógica de saldos Spot y Swap...)

        # 2. Open Orders SPOT y SWAP (Separación de Tablas)
        cur.execute("DELETE FROM sys_open_orders_spot WHERE user_id=%s AND symbol LIKE '%%-%%'", (user_id,))
        res_spot = bx_req("/openApi/spot/v1/trade/openOrders")
        for o in res_spot.get('data', []):
            cur.execute("INSERT INTO sys_open_orders_spot (user_id, symbol, side, type, price, amount, status, fecha_utc) VALUES (%s,%s,%s,%s,%s,%s,'NEW',%s)", (user_id, o['symbol'], o['side'], o['type'], o['price'], o['origQty'], format_date(o['time'])))
            disparador_radar(cur, o['symbol'])

        cur.execute("DELETE FROM sys_open_orders WHERE user_id=%s AND symbol LIKE '%%-%%'", (user_id,))
        res_swap = bx_req("/openApi/swap/v2/trade/openOrders")
        for o in res_swap.get('data', []):
            cur.execute("INSERT INTO sys_open_orders (user_id, symbol, side, type, price, amount, status, fecha_utc) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", (user_id, o['symbol'], o['side'], o['type'], o['price'], o['origQty'], o['status'], format_date(o['time'])))

        db.commit()
    except Exception as e: print(f"❌ Error BingX: {e}")

# (Bucle Main similar a las versiones anteriores...)