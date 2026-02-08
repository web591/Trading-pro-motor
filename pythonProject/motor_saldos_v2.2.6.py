import mysql.connector
from binance.client import Client
import time, os, base64, hmac, requests, hashlib
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# --- CONFIGURACIÓN MAESTRA ---
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
ESPERA_CICLO_RAPIDO = 120 
CICLOS_DEEP_AUDIT = 30 # Cada 30 ciclos (~1 hora) audita depósitos/retiros

def descifrar_dato(t, m):
    try:
        raw = base64.b64decode(t.strip())
        if b":::" in raw: data, iv = raw.split(b":::")
        elif b"::" in raw: data, iv = raw.split(b"::")
        else: return None
        cipher = AES.new(sha256(m.encode()).digest(), AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

def limpiar_ticker_profesional(symbol):
    """Mantiene TUSD y filtra tokens apalancados (UP/DOWN)"""
    s = symbol.upper().replace('-', '').replace('_', '')
    blacklist = ['UP', 'DOWN', 'BULL', 'BEAR']
    if any(word in s for word in blacklist): return None
    suffixes = ['USDT', 'USDC', 'BUSD', 'FDUSD', 'DAI', 'USD', 'BTC', 'ETH']
    for suffix in suffixes:
        if s.endswith(suffix) and s != suffix:
            res = s[: -len(suffix)].replace('PERP', '')
            return res if len(res) <= 10 else None
    return s if len(s) <= 10 else None

def disparador_radar(cur, symbol):
    """Duda 1: ÚNICO. Inserta en sys_busqueda_resultados sin duplicar."""
    ticker_limpio = limpiar_ticker_profesional(symbol)
    if not ticker_limpio: return
    try:
        cur.execute("INSERT IGNORE INTO sys_busqueda_resultados (ticker, estado) VALUES (%s, 'pendiente')", (ticker_limpio,))
    except: pass

def obtener_info_activo(cursor, ticker, broker):
    asset_upper = ticker.upper()
    asset_clean = asset_upper[2:] if asset_upper.startswith('LD') else asset_upper
    stables = ['USDT','USDC','BUSD','DAI','FDUSD']
    if asset_clean in stables: return 1.0, None
    cursor.execute("""
        SELECT t.id, p.price FROM sys_traductor_simbolos t
        LEFT JOIN sys_precios_activos p ON p.traductor_id = t.id
        WHERE t.nombre_comun=%s AND t.motor_fuente LIKE %s LIMIT 1
    """, (asset_clean, f"%{broker.lower()}%"))
    r = cursor.fetchone()
    if r: return float(r['price']) if r['price'] else 0.0, r['id']
    return 0.0, None

# --- AUDITORÍA BINANCE (Saldos + Trades + Órdenes + Historial) ---
def procesar_binance(key, sec, user_id, db, deep):
    try:
        client = Client(key, sec)
        cur = db.cursor(dictionary=True)
        
        # 1. Saldos
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name='Binance'", (user_id,))
        acc = client.get_account()
        for b in acc['balances']:
            tot = float(b['free']) + float(b['locked'])
            if tot > 0.000001:
                precio, tid = obtener_info_activo(cur, b['asset'], 'Binance')
                cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, traductor_id, cantidad_total, valor_usd, last_update) VALUES (%s,'Binance','SPOT',%s,%s,%s,%s,NOW())", (user_id, b['asset'], tid, tot, tot*precio))
                disparador_radar(cur, b['asset'])

        # 2. Trades y Órdenes
        cur.execute("SELECT DISTINCT ticker_motor AS sym FROM sys_traductor_simbolos WHERE is_active=1 AND motor_fuente LIKE '%binance%'")
        for row in cur.fetchall():
            sym = row['sym']
            try:
                # Trades (Ráfagas)
                for t in client.get_my_trades(symbol=sym, limit=1000):
                    id_ext = f"BN-T-{t['id']}"
                    cur.execute("INSERT IGNORE INTO detalle_trades (user_id, id_externo_ref, symbol, lado, precio_ejecucion, cantidad_ejecutada, fecha_utc) VALUES (%s,%s,%s,%s,%s,%s,FROM_UNIXTIME(%s/1000))", (user_id, id_ext, sym, 'BUY' if t['isBuyer'] else 'SELL', t['price'], t['qty'], t['time']))
                    m_n = float(t['qty']) if t['isBuyer'] else -float(t['qty'])
                    cur.execute("INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, categoria, asset, monto_neto, fecha_utc) VALUES (%s,%s,'Binance','TRADE',%s,%s,FROM_UNIXTIME(%s/1000))", (id_ext, user_id, sym, m_n, t['time']))
                
                # Órdenes Abiertas Spot
                cur.execute("DELETE FROM Sys_open_order_spot WHERE user_id=%s AND symbol=%s", (user_id, sym))
                for o in client.get_open_orders(symbol=sym):
                    cur.execute("INSERT INTO Sys_open_order_spot (user_id, symbol, side, type, price, amount, status, fecha_utc) VALUES (%s,%s,%s,%s,%s,%s,%s,FROM_UNIXTIME(%s/1000))", (user_id, sym, o['side'], o['type'], o['price'], o['origQty'], o['status'], o['time']))
            except: continue

        # 3. DUDA 2: DEPOSITS, WITHDRAWALS & DUST (Solo en ciclo Deep)
        if deep:
            # Depósitos
            for dep in client.get_deposit_history():
                cur.execute("INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, categoria, asset, monto_neto, fecha_utc) VALUES (%s,%s,'Binance','DEPOSIT',%s,%s,FROM_UNIXTIME(%s/1000))", (f"BN-DEP-{dep['txId']}", user_id, dep['coin'], float(dep['amount']), dep['insertTime']))
            # Retiros
            for w in client.get_withdraw_history():
                cur.execute("INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, categoria, asset, monto_neto, fecha_utc) VALUES (%s,%s,'Binance','WITHDRAW',%s,%s,%s)", (f"BN-WTH-{w['id']}", user_id, w['coin'], -float(w['amount']), w['applyTime']))
            # Dust (Polvo de activos a BNB)
            dust = client.get_dust_log()
            if dust and 'userAssetDribblets' in dust:
                for d in dust['userAssetDribblets']:
                    for dtl in d['userAssetDribbletDetails']:
                        cur.execute("INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, categoria, asset, monto_neto, fecha_utc) VALUES (%s,%s,'Binance','DUST',%s,%s,FROM_UNIXTIME(%s/1000))", (f"BN-DUST-{dtl['transId']}", user_id, dtl['fromAsset'], -float(dtl['amount']), d['operateTime']))

        db.commit()
    except Exception as e: print(f"❌ Error Binance: {e}")

# --- AUDITORÍA BINGX (Saldos + Órdenes Spot/Perp) ---
def procesar_bingx(key, sec, user_id, db, session):
    try:
        cur = db.cursor(dictionary=True)
        def bx_req(path, params=None, method="GET"):
            p = params or {}; p["timestamp"] = int(time.time() * 1000)
            qs = "&".join([f"{k}={p[k]}" for k in sorted(p.keys())])
            sig = hmac.new(sec.encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
            url = f"https://open-api.bingx.com{path}?{qs}&signature={sig}"
            return session.request(method, url, headers={'X-BX-APIKEY': key}).json()

        # 1. BingX Spot
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name='BingX' AND tipo_cuenta='SPOT'", (user_id,))
        cur.execute("DELETE FROM Sys_open_order_spot WHERE user_id=%s AND symbol LIKE '%%-USDT'", (user_id,))
        res_s = bx_req("/openApi/spot/v1/account/balance")
        if res_s.get('code') == 0:
            for b in res_s['data']['balances']:
                tot = float(b['free']) + float(b['locked'])
                if tot > 0.000001:
                    precio, tid = obtener_info_activo(cur, b['asset'], 'BingX')
                    cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, traductor_id, cantidad_total, valor_usd, last_update) VALUES (%s,'BingX','SPOT',%s,%s,%s,%s,NOW())", (user_id, b['asset'], tid, tot, tot*precio))
                    disparador_radar(cur, b['asset'])

        # 2. BingX Perpetual
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name='BingX' AND tipo_cuenta='PERPETUAL'", (user_id,))
        cur.execute("DELETE FROM sys_open_order WHERE user_id=%s", (user_id,))
        res_p = bx_req("/openApi/swap/v2/user/positions")
        if res_p.get('code') == 0:
            for pos in res_p.get('data', []):
                amt = abs(float(pos['positionAmt']))
                if amt > 0.000001:
                    precio, tid = obtener_info_activo(cur, pos['symbol'], 'BingX')
                    cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, traductor_id, cantidad_total, valor_usd, last_update) VALUES (%s,'BingX','PERPETUAL',%s,%s,%s,%s,NOW())", (user_id, pos['symbol'], tid, amt, amt*precio))
                    disparador_radar(cur, pos['symbol'])
        
        # Órdenes Perp
        res_oo_p = bx_req("/openApi/swap/v2/trade/openOrders")
        if res_oo_p.get('code') == 0:
            for o in res_oo_p.get('data', []):
                cur.execute("INSERT INTO sys_open_order (user_id, symbol, side, type, price, amount, status, fecha_utc) VALUES (%s,%s,%s,%s,%s,%s,%s,FROM_UNIXTIME(%s/1000))", (user_id, o['symbol'], o['side'], o['type'], o['price'], o['origQty'], o['status'], o['updateTime']))

        db.commit()
    except Exception as e: print(f"❌ Error BingX: {e}")

if __name__ == "__main__":
    session = requests.Session(); ciclo = 0
    print("🚀 MOTOR v2.2.6 - INTEGRACIÓN TOTAL (Saldos+Trades+Orders+DeepAudit)")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cur = db.cursor(dictionary=True)
            cur.execute("SELECT user_id, broker_name, api_key, api_secret FROM api_keys WHERE status=1")
            users = cur.fetchall()
            deep = (ciclo % CICLOS_DEEP_AUDIT == 0) # Gatillo para depósitos/retiros
            for u in users:
                k, s = descifrar_dato(u['api_key'], MASTER_KEY), descifrar_dato(u['api_secret'], MASTER_KEY)
                if not (k and s): continue
                if 'binance' in u['broker_name'].lower(): procesar_binance(k, s, u['user_id'], db, deep)
                elif 'bingx' in u['broker_name'].lower(): procesar_bingx(k, s, u['user_id'], db, session)
            db.close(); ciclo += 1
            print(f"✅ Ciclo {ciclo} finalizado. Deep Audit: {'SÍ' if deep else 'NO'}"); time.sleep(ESPERA_CICLO_RAPIDO)
        except Exception as e: print(f"⚠️ Error Principal: {e}"); time.sleep(30)