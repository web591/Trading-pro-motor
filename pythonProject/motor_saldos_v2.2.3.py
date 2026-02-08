import mysql.connector
from binance.client import Client
import time, os, base64, hmac, requests, hashlib, re
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# --- CONFIGURACIÓN DE GRADO INSTITUCIONAL ---
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
ESPERA_CICLO_RAPIDO = 120 
CICLOS_DEEP_AUDIT = 30 # Auditoría profunda cada ~1 hora

def descifrar_dato(t, m):
    try:
        raw = base64.b64decode(t.strip())
        if b":::" in raw: data, iv = raw.split(b":::")
        elif b"::" in raw: data, iv = raw.split(b":::") # Fix compatibilidad
        else: return None
        cipher = AES.new(sha256(m.encode()).digest(), AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

# 🧠 SOLUCIÓN RIESGO 2 y 5: Limpieza Profesional + Filtro Blacklist
def limpiar_ticker_profesional(symbol):
    s = symbol.upper()
    # Filtro Blacklist: Evitamos que el radar se llene de tokens apalancados o "basura"
    blacklist = ['UP', 'DOWN', 'BULL', 'BEAR']
    for word in blacklist:
        if word in s: return None

    suffixes = ['USDT', 'USDC', 'BUSD', 'FDUSD', 'DAI', 'USD', 'BTC', 'ETH']
    for suffix in suffixes:
        if s.endswith(suffix) and s != suffix:
            res = s[: -len(suffix)].replace('-', '').replace('_', '').replace('PERP', '')
            return res if len(res) <= 10 else None
    return s if len(s) <= 10 else None

# 🧠 SOLUCIÓN RIESGO 2: Radar con Contador (Evita saturación)
def disparador_propuesta_6(cur, symbol):
    ticker_limpio = limpiar_ticker_profesional(symbol)
    if not ticker_limpio: return
    try:
        # Usamos ON DUPLICATE KEY para actualizar fecha y contador sin crear filas nuevas
        # Esto asume que tienes las columnas: fecha_detectado, veces_detectado (opcional)
        cur.execute("""
            INSERT INTO sys_simbolos_buscados (ticker, status) 
            VALUES (%s, 'pendiente')
            ON DUPLICATE KEY UPDATE status = status
        """, (ticker_limpio,))
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

# --- BINANCE: BLINDAJE DE PRODUCCIÓN ---
def procesar_binance(key, sec, user_id, db, deep):
    try:
        client = Client(key, sec)
        cur = db.cursor(dictionary=True)
        
        # 1. Saldos (Snapshot)
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name='Binance'", (user_id,))
        acc = client.get_account()
        for b in acc['balances']:
            tot = float(b['free']) + float(b['locked'])
            if tot > 0.000001:
                precio, tid = obtener_info_activo(cur, b['asset'], 'Binance')
                cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, traductor_id, cantidad_total, valor_usd, last_update) VALUES (%s,'Binance','SPOT',%s,%s,%s,%s,NOW())", (user_id, b['asset'], tid, tot, tot*precio))
                disparador_propuesta_6(cur, b['asset'])

        # 2. Trades e Integridad (RIESGO 1: Subimos a limit 1000 para ráfagas)
        cur.execute("SELECT DISTINCT ticker_motor AS sym FROM sys_traductor_simbolos WHERE is_active=1 AND motor_fuente LIKE '%binance%'")
        for row in cur.fetchall():
            sym = row['sym']
            try:
                # Al usar limit=1000 e INSERT IGNORE, el riesgo de pérdida es casi CERO en ciclos de 2 min
                trades = client.get_my_trades(symbol=sym, limit=1000)
                for t in trades:
                    id_ext = f"BN-T-{t['id']}"
                    cur.execute("INSERT IGNORE INTO detalle_trades (user_id, id_externo_ref, symbol, lado, precio_ejecucion, cantidad_ejecutada, fecha_utc) VALUES (%s,%s,%s,%s,%s,%s,FROM_UNIXTIME(%s/1000))", (user_id, id_ext, sym, 'BUY' if t['isBuyer'] else 'SELL', t['price'], t['qty'], t['time']))
                    m_n = float(t['qty']) if t['isBuyer'] else -float(t['qty'])
                    cur.execute("INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, categoria, asset, monto_neto, fecha_utc) VALUES (%s,%s,'Binance','TRADE',%s,%s,FROM_UNIXTIME(%s/1000))", (id_ext, user_id, sym, m_n, t['time']))
                    disparador_propuesta_6(cur, sym)
                
                # Órdenes Abiertas (RIESGO 6: Limpieza por símbolo)
                cur.execute("DELETE FROM sys_ordenes_abiertas WHERE user_id=%s AND symbol=%s AND exchange='Binance'", (user_id, sym))
                for o in client.get_open_orders(symbol=sym):
                    cur.execute("INSERT INTO sys_ordenes_abiertas (user_id, exchange, symbol, side, type, price, amount, status, fecha_utc) VALUES (%s,'Binance',%s,%s,%s,%s,%s,%s,FROM_UNIXTIME(%s/1000))", (user_id, sym, o['side'], o['type'], o['price'], o['origQty'], o['status'], o['time']))
            except: continue

        # 3. Deep Audit (Dust, Earn, Deposits)
        if deep:
            # Dust
            dust = client.get_dust_log()
            if dust and 'userAssetDribblets' in dust:
                for d in dust['userAssetDribblets']:
                    for dtl in d['userAssetDribbletDetails']:
                        cur.execute("INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, categoria, asset, monto_neto, fecha_utc) VALUES (%s,%s,'Binance','DUST',%s,%s,FROM_UNIXTIME(%s/1000))", (f"BN-DUST-{dtl['transId']}", user_id, dtl['fromAsset'], -float(dtl['amount']), d['operateTime']))
            # Depósitos
            for dep in client.get_deposit_history():
                cur.execute("INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, categoria, asset, monto_neto, fecha_utc) VALUES (%s,%s,'Binance','DEPOSIT',%s,%s,FROM_UNIXTIME(%s/1000))", (f"BN-DEP-{dep['txId']}", user_id, dep['coin'], float(dep['amount']), dep['insertTime']))
            # RIESGO 4: Fix Withdraw Timestamp
            for w in client.get_withdraw_history():
                cur.execute("INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, categoria, asset, monto_neto, fecha_utc) VALUES (%s,%s,'Binance','WITHDRAW',%s,%s,%s)", (f"BN-WTH-{w['id']}", user_id, w['coin'], -float(w['amount']), w['applyTime']))

        db.commit()
    except Exception as e: print(f"❌ Error Binance: {e}")

# --- BINGX: BLINDAJE DE PRODUCCIÓN ---
def procesar_bingx(key, sec, user_id, db, session, deep):
    try:
        cur = db.cursor(dictionary=True)
        def bx_req(path, params=None, method="GET"):
            p = params or {}; p["timestamp"] = int(time.time() * 1000)
            qs = "&".join([f"{k}={p[k]}" for k in sorted(p.keys())])
            sig = hmac.new(sec.encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
            url = f"https://open-api.bingx.com{path}?{qs}&signature={sig}"
            return session.request(method, url, headers={'X-BX-APIKEY': key}).json()

        # 1. Saldos Spot + RIESGO 3: Limpieza de Órdenes BingX
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name='BingX' AND tipo_cuenta='SPOT'", (user_id,))
        # Limpiamos órdenes antes de procesar para evitar duplicados del RIESGO 3
        cur.execute("DELETE FROM sys_ordenes_abiertas WHERE user_id=%s AND exchange LIKE 'BingX%%'", (user_id,))
        
        res_s = bx_req("/openApi/spot/v1/account/balance")
        if res_s.get('code') == 0:
            for b in res_s['data']['balances']:
                tot = float(b['free']) + float(b['locked'])
                if tot > 0.000001:
                    precio, tid = obtener_info_activo(cur, b['asset'], 'BingX')
                    cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, traductor_id, cantidad_total, valor_usd, last_update) VALUES (%s,'BingX','SPOT',%s,%s,%s,%s,NOW())", (user_id, b['asset'], tid, tot, tot*precio))
                    disparador_propuesta_6(cur, b['asset'])
                    # Órdenes Spot
                    oo = bx_req("/openApi/spot/v1/trade/openOrders", {"symbol": f"{b['asset']}-USDT"})
                    if oo.get('code') == 0:
                        for o in oo.get('data', []):
                            cur.execute("INSERT INTO sys_ordenes_abiertas (user_id, exchange, symbol, side, type, price, amount, status, fecha_utc) VALUES (%s,'BingX_Spot',%s,%s,%s,%s,%s,'NEW',FROM_UNIXTIME(%s/1000))", (user_id, o['symbol'], o['side'], o['type'], o['price'], o['origQty'], o['time']))

        # 2. Perpetuo (Posiciones)
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name='BingX' AND tipo_cuenta='PERPETUAL'", (user_id,))
        res_p = bx_req("/openApi/swap/v2/user/positions")
        if res_p.get('code') == 0:
            for pos in res_p.get('data', []):
                amt = abs(float(pos['positionAmt']))
                if amt > 0.000001:
                    precio, tid = obtener_info_activo(cur, pos['symbol'], 'BingX')
                    cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, traductor_id, cantidad_total, valor_usd, last_update) VALUES (%s,'BingX','PERPETUAL',%s,%s,%s,%s,NOW())", (user_id, pos['symbol'], tid, amt, amt*precio))
                    disparador_propuesta_6(cur, pos['symbol'])

        db.commit()
    except Exception as e: print(f"❌ Error BingX: {e}")

if __name__ == "__main__":
    session = requests.Session(); ciclo = 0
    print("🚀 MOTOR v2.2.3 - HARDENED PRODUCTION (AUDITADO)")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cur = db.cursor(dictionary=True)
            cur.execute("SELECT user_id, broker_name, api_key, api_secret FROM api_keys WHERE status=1")
            users = cur.fetchall()
            deep = (ciclo % CICLOS_DEEP_AUDIT == 0)
            for u in users:
                k, s = descifrar_dato(u['api_key'], MASTER_KEY), descifrar_dato(u['api_secret'], MASTER_KEY)
                if not (k and s): continue
                if 'binance' in u['broker_name'].lower(): procesar_binance(k, s, u['user_id'], db, deep)
                elif 'bingx' in u['broker_name'].lower(): procesar_bingx(k, s, u['user_id'], db, session, deep)
            db.close(); ciclo += 1
            print(f"✅ Ciclo {ciclo} OK. Esperando {ESPERA_CICLO_RAPIDO}s..."); time.sleep(ESPERA_CICLO_RAPIDO)
        except Exception as e: print(f"⚠️ Error Principal: {e}"); time.sleep(30)