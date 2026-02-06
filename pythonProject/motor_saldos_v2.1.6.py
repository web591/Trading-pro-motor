import mysql.connector
from binance.client import Client
import time, os, base64, hmac, requests, hashlib
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# --- CONFIGURACIÓN ---
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
ESPERA_CICLO_RAPIDO = 120
CICLOS_PARA_STATEMENTS = 240 

def descifrar_dato(t, m):
    try:
        raw = base64.b64decode(t.strip())
        if b":::" in raw: data, iv = raw.split(b":::")
        elif b"::" in raw: data, iv = raw.split(b"::")
        else: return None
        cipher = AES.new(sha256(m.encode()).digest(), AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

def disparador_busqueda_automatica(cur, symbol):
    """Limpia el ticker para que el buscador encuentre la moneda base"""
    try:
        # Eliminamos sufijos comunes de pares para dejar solo la moneda
        c = symbol.upper().replace('USDT','').replace('USDC','').replace('USD','').replace('_PERP','').replace('BUSD','')
        if c:
            cur.execute("INSERT IGNORE INTO sys_simbolos_buscados (ticker, status) VALUES (%s, 'pendiente')", (c,))
    except: pass

def obtener_info_activo(cursor, ticker, broker):
    """Maneja activos normales y activos 'LD' de Binance Earn"""
    asset_upper = ticker.upper()
    # Si empieza con LD (Earn), buscamos el precio de la moneda base
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

def procesar_binance(key, sec, user_id, db, sync_statements):
    try:
        client = Client(key, sec)
        cur = db.cursor(dictionary=True)
        
        # 1. SALDOS (Auditado para manejar LDAssets)
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name='Binance'", (user_id,))
        acc = client.get_account()
        for b in acc['balances']:
            tot = float(b['free']) + float(b['locked'])
            if tot > 0.000001:
                precio, tid = obtener_info_activo(cur, b['asset'], 'Binance')
                cur.execute("""
                    INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, traductor_id, cantidad_total, valor_usd, last_update) 
                    VALUES (%s,'Binance','SPOT',%s,%s,%s,%s,NOW())
                """, (user_id, b['asset'], tid, tot, tot*precio))

        # 2. RADAR DE TRADES Y ÓRDENES
        cur.execute("""
            SELECT DISTINCT ticker_motor AS sym FROM sys_traductor_simbolos WHERE is_active=1 AND motor_fuente LIKE '%binance%'
            UNION SELECT DISTINCT symbol AS sym FROM detalle_trades WHERE user_id=%s
        """, (user_id,))
        universo = [row['sym'] for row in cur.fetchall() if row['sym']]
        
        for sym in universo:
            disparador_busqueda_automatica(cur, sym)
            try:
                if "_PERP" not in sym:
                    cur.execute("SELECT ultimo_timestamp FROM sys_sync_puntos WHERE user_id=%s AND symbol=%s AND tipo_sincro='SPOT_TRADE'", (user_id, sym))
                    row = cur.fetchone()
                    desde = row['ultimo_timestamp'] if row else 1735689600000
                    trades = client.get_my_trades(symbol=sym, startTime=desde + 1)
                    for t in trades:
                        id_ext = f"BIN-T-{t['id']}"
                        cur.execute("INSERT IGNORE INTO detalle_trades (user_id, id_externo_ref, symbol, lado, precio_ejecucion, cantidad_ejecutada, fecha_utc) VALUES (%s,%s,%s,%s,%s,%s,FROM_UNIXTIME(%s/1000))", (user_id, id_ext, sym, 'BUY' if t['isBuyer'] else 'SELL', t['price'], t['qty'], t['time']))
                        m_n = float(t['qty']) if t['isBuyer'] else -float(t['qty'])
                        cur.execute("INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, fecha_utc) VALUES (%s,%s,'Binance','SPOT','TRADE',%s,%s,FROM_UNIXTIME(%s/1000))", (id_ext, user_id, sym, m_n, t['time']))
                    if trades:
                        cur.execute("REPLACE INTO sys_sync_puntos (user_id, broker, symbol, tipo_sincro, ultimo_timestamp) VALUES (%s,'Binance',%s,'SPOT_TRADE',%s)", (user_id, sym, max(x['time'] for x in trades)))
                
                # Órdenes Abiertas
                cur.execute("DELETE FROM sys_ordenes_abiertas WHERE user_id=%s AND symbol=%s", (user_id, sym))
                es_fut = "_PERP" in sym
                oo = client.futures_get_open_orders(symbol=sym) if es_fut else client.get_open_orders(symbol=sym)
                for o in oo:
                    cur.execute("INSERT INTO sys_ordenes_abiertas (user_id, exchange, symbol, side, type, price, amount, status, fecha_utc) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,FROM_UNIXTIME(%s/1000))", (user_id, 'Binance_Futures' if es_fut else 'Binance', sym, o['side'], o['type'], o['price'], o['origQty'], o['status'], o.get('time', o.get('updateTime'))))
            except: continue

        # 3. CASH FLOW (AUDITORÍA CRUZADA: Depósitos, Earn, Dust)
        if sync_statements:
            # Depósitos y Retiros
            try:
                for d in client.get_deposit_history():
                    cur.execute("INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, categoria, asset, monto_neto, fecha_utc) VALUES (%s,%s,'Binance','DEPOSIT',%s,%s,FROM_UNIXTIME(%s/1000))", (f"DEP-{d.get('id', d.get('txId'))}", user_id, d['coin'], float(d['amount']), d['insertTime']))
                for w in client.get_withdraw_history():
                    cur.execute("INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, categoria, asset, monto_neto, fecha_utc) VALUES (%s,%s,'Binance','WITHDRAW',%s,%s,FROM_UNIXTIME(%s/1000))", (f"WIT-{w.get('id', w.get('txId'))}", user_id, w['coin'], -float(w['amount']), w['applyTime']))
            except: pass

            # Intereses (Intentamos nombres de funciones según versión de librería)
            try:
                divs = None
                try: divs = client.get_asset_dividend()
                except: divs = client.get_asset_distribution()
                if divs and 'rows' in divs:
                    for d in divs['rows']:
                        cur.execute("INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, categoria, asset, monto_neto, fecha_utc) VALUES (%s,%s,'Binance','EARN_INTEREST',%s,%s,FROM_UNIXTIME(%s/1000))", (f"DIV-{d['id']}", user_id, d['asset'], float(d['amount']), d['divTime']))
            except: pass

            # Polvo (Dust)
            try:
                dust = client.get_dust_log()
                if dust and 'userAssetDribblets' in dust:
                    for dr in dust['userAssetDribblets']:
                        for dtl in dr['userAssetDribbletDetails']:
                            cur.execute("INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, categoria, asset, monto_neto, fecha_utc) VALUES (%s,%s,'Binance','DUST',%s,%s,FROM_UNIXTIME(%s/1000))", (f"DST-{dtl['transId']}", user_id, dtl['fromAsset'], -float(dtl['amount']), dr['operateTime']))
            except: pass

        db.commit()
    except Exception as e: print(f"❌ Error Binance ID {user_id}: {e}")

def procesar_bingx(key, sec, user_id, db, session):
    try:
        cur = db.cursor(dictionary=True)
        def bx_req(path, params=None):
            p = params or {}; p["timestamp"] = int(time.time() * 1000)
            qs = "&".join([f"{k}={p[k]}" for k in sorted(p.keys())])
            sig = hmac.new(sec.encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
            return session.get(f"https://open-api.bingx.com{path}?{qs}&signature={sig}", headers={'X-BX-APIKEY': key}).json()

        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name='BingX'", (user_id,))
        res_s = bx_req("/openApi/spot/v1/account/balance")
        if res_s.get('code') == 0:
            for b in res_s['data']['balances']:
                tot = float(b['free']) + float(b['locked'])
                if tot > 0.000001:
                    precio, tid = obtener_info_activo(cur, b['asset'], 'BingX')
                    cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, traductor_id, cantidad_total, valor_usd, last_update) VALUES (%s,'BingX','SPOT',%s,%s,%s,%s,NOW())", (user_id, b['asset'], tid, tot, tot*precio))
        
        res_std = bx_req("/openApi/agent/v1/asset/getPrincipal")
        if res_std.get('code') == 0:
            for a in res_std.get('data', []):
                amt = float(a.get('amount', 0))
                if amt > 0.01:
                    precio, tid = obtener_info_activo(cur, a['asset'], 'BingX')
                    cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, traductor_id, cantidad_total, valor_usd, last_update) VALUES (%s,'BingX','STANDARD',%s,%s,%s,%s,NOW())", (user_id, a['asset'], tid, amt, amt*precio))
        db.commit()
    except Exception as e: print(f"❌ Error BingX ID {user_id}: {e}")

if __name__ == "__main__":
    session = requests.Session(); ciclo = 0
    print("🚀 MOTOR v2.1.6 - AUDITADO Y REFORZADO")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cur = db.cursor(dictionary=True)
            cur.execute("SELECT user_id, broker_name, api_key, api_secret FROM api_keys WHERE status=1")
            users = cur.fetchall()
            sync_st = (ciclo % CICLOS_PARA_STATEMENTS == 0)
            for u in users:
                k, s = descifrar_dato(u['api_key'], MASTER_KEY), descifrar_dato(u['api_secret'], MASTER_KEY)
                if not k or not s: continue
                if 'binance' in u['broker_name'].lower(): 
                    procesar_binance(k, s, u['user_id'], db, sync_st)
                elif 'bingx' in u['broker_name'].lower(): 
                    procesar_bingx(k, s, u['user_id'], db, session)
            db.close(); ciclo += 1
            print(f"✅ Ciclo {ciclo} completado."); time.sleep(ESPERA_CICLO_RAPIDO)
        except Exception as e: 
            print(f"⚠️ Error Principal: {e}"); time.sleep(30)