import mysql.connector
from binance.client import Client
import time, os, base64, hmac, requests, hashlib, datetime, json
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# --- CONFIGURACIÓN ---
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
ESPERA_CICLO_RAPIDO = 120 
CICLOS_DEEP_AUDIT = 15

def descifrar_dato(t, m):
    try:
        raw = base64.b64decode(t.strip())
        sep = b":::" if b":::" in raw else b"::"
        data, iv = raw.split(sep)
        cipher = AES.new(sha256(m.encode()).digest(), AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except Exception as e:
        print(f"DEBUG [Cifrado]: Error descifrando llave: {e}")
        return None

def format_date(ms):
    return datetime.datetime.fromtimestamp(ms / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

# --- 1. IDENTIDAD ---
def obtener_id_traductor(cur, symbol, user_id, motor_name):
    limpio = symbol.upper().replace('USDT', '').replace('USDC', '').replace('-', '').replace('PERP', '')
    if limpio.startswith('LD'): limpio = limpio[2:]
    
    # Usamos buffered=True en el cursor para evitar "Unread result"
    query = "SELECT id FROM sys_traductor_simbolos WHERE nombre_comun = %s AND (motor_fuente = %s OR motor_fuente = 'global') LIMIT 1"
    cur.execute(query, (limpio, motor_name))
    res = cur.fetchone()
    
    if res:
        return res['id']
    
    print(f"DEBUG [Radar]: Símbolo {limpio} no encontrado. Enviando a sys_busqueda_resultados...")
    cur.execute("INSERT IGNORE INTO sys_busqueda_resultados (ticker, estado, origen, motor, user_id, fecha_deteccion) VALUES (%s, 'detectado', 'sistema', %s, %s, NOW())", (limpio, motor_name, user_id))
    return None

# --- 2. REGISTRO CONTABLE ---
def registrar_trade_db(cur, user_id, broker_id, d):
    print(f"DEBUG [Trade]: Registrando trade {d['id_ref']} para {d['symbol']}")
    sql = """INSERT IGNORE INTO detalle_trades (id_externo_ref, user_id, broker_id, symbol, cantidad_original, precio_original, fee_trading, fee_funding, moneda_fee, pnl_bruto, created_at)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cur.execute(sql, (d['id_ref'], user_id, broker_id, d['symbol'], d['qty'], d['price'], d['fee'], d.get('funding', 0), d['fee_asset'], d['pnl'], d['date']))

def registrar_transaccion_global(cur, d):
    print(f"DEBUG [Global]: Registrando {d['cat']} de {d['asset']} ({d['monto']})")
    sql = """INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, categoria, asset, monto_neto, fecha_utc) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    cur.execute(sql, (d['id'], d['user_id'], d['exchange'], d['cat'], d['asset'], d['monto'], d['date']))

# --- 3. MOTORES ---

def motor_binance(k, s, user_id, db, deep):
    print(f"--- INICIO BINANCE (User {user_id}) ---")
    client = Client(k, s)
    cur = db.cursor(dictionary=True, buffered=True)
    
    # Saldos
    acc = client.get_account()
    print(f"DEBUG [Binance]: Procesando {len(acc['balances'])} balances.")
    cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name='Binance'", (user_id,))
    for b in acc['balances']:
        tot = float(b['free']) + float(b['locked'])
        if tot > 0.000001:
            tid = obtener_id_traductor(cur, b['asset'], user_id, 'binance_spot')
            cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, asset, traductor_id, cantidad_total, last_update) VALUES (%s, 'Binance', %s, %s, %s, NOW())", (user_id, b['asset'], tid, tot))

    if deep:
        print(f"DEBUG [Binance Deep]: Iniciando auditoría profunda...")
        # Depósitos
        deps = client.get_deposit_history()
        print(f"API DATA [Binance Deps]: {len(deps)} registros encontrados.")
        for dep in deps:
            registrar_transaccion_global(cur, {'id': f"BN-DEP-{dep['txId']}", 'user_id': user_id, 'exchange': 'Binance', 'cat': 'DEPOSIT', 'asset': dep['coin'], 'monto': dep['amount'], 'date': format_date(dep['insertTime'])})
        
        # Dust
        dust = client.get_dust_log()
        if dust and 'userAssetDribblets' in dust:
            print(f"API DATA [Binance Dust]: {len(dust['userAssetDribblets'])} logs de dust.")
            for d in dust['userAssetDribblets']:
                for dtl in d['userAssetDribbletDetails']:
                    registrar_transaccion_global(cur, {'id': f"BN-DUST-{dtl['transId']}", 'user_id': user_id, 'exchange': 'Binance', 'cat': 'DUST', 'asset': dtl['fromAsset'], 'monto': -float(dtl['amount']), 'date': format_date(d['operateTime'])})

        # Futures Trades
        try:
            f_trades = client.futures_account_trades(limit=50)
            print(f"API DATA [Binance Fut]: {len(f_trades)} trades encontrados.")
            for ft in f_trades:
                registrar_trade_db(cur, user_id, 1, {'id_ref': f"BN-F-{ft['id']}", 'symbol': ft['symbol'], 'qty': ft['qty'], 'price': ft['price'], 'fee': ft['commission'], 'fee_asset': ft['commissionAsset'], 'pnl': ft['realizedPnl'], 'date': format_date(ft['time'])})
        except Exception as e:
            print(f"DEBUG [Binance Fut]: Error o sin acceso a futuros: {e}")

def motor_bingx(k, s, user_id, db, session, deep):
    print(f"--- INICIO BINGX (User {user_id}) ---")
    cur = db.cursor(dictionary=True, buffered=True)
    def req(path, params=None):
        p = params or {}; p["timestamp"] = int(time.time() * 1000)
        qs = "&".join([f"{k}={p[k]}" for k in sorted(p.keys())])
        sig = hmac.new(s.encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{qs}&signature={sig}"
        r = session.get(url, headers={'X-BX-APIKEY': k}).json()
        # Log del JSON crudo (primeros 200 caracteres para no saturar)
        print(f"API RAW [BingX {path}]: {json.dumps(r)[:200]}...")
        return r

    # Saldos Spot
    res = req("/openApi/spot/v1/account/balance")
    if res.get('code') == 0:
        cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id=%s AND broker_name='BingX'", (user_id,))
        for b in res['data']['balances']:
            tot = float(b['free']) + float(b['locked'])
            if tot > 0.000001:
                tid = obtener_id_traductor(cur, b['asset'], user_id, 'bingx_spot')
                cur.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, asset, traductor_id, cantidad_total, last_update) VALUES (%s,'BingX',%s,%s,%s,NOW())", (user_id, b['asset'], tid, tot))

    if deep:
        print(f"DEBUG [BingX Deep]: Iniciando perpetuos y standard...")
        # Perpetual (Swap V2)
        res_f = req("/openApi/swap/v2/trade/allOrders", {"limit": 50})
        if res_f.get('code') == 0:
            for o in res_f['data']:
                if o['status'] == 'FILLED':
                    registrar_trade_db(cur, user_id, 2, {'id_ref': f"BX-F-{o['orderId']}", 'symbol': o['symbol'], 'qty': o['executedQty'], 'price': o['avgPrice'], 'fee': o.get('commission', 0), 'fee_asset': 'USDT', 'pnl': o.get('realizedProfit', 0), 'date': format_date(o['updateTime'])})

        # Standard Futures
        res_std = req("/openApi/contract/v1/allOrders", {"limit": 20})
        if res_std.get('code') == 0 and res_std.get('data'):
            for o in res_std['data']:
                registrar_trade_db(cur, user_id, 2, {'id_ref': f"BX-STD-{o['orderId']}", 'symbol': o['symbol'], 'qty': o['volume'], 'price': o['price'], 'fee': 0, 'fee_asset': 'USDT', 'pnl': o.get('profit', 0), 'date': format_date(o['createTime'])})

# --- MAIN ---
if __name__ == "__main__":
    session = requests.Session(); ciclo = 0
    print("🚀 MOTOR v3.5.1 [FORENSE] - INICIANDO...")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cur_main = db.cursor(dictionary=True, buffered=True)
            
            # Intentar Bloqueo
            print(f"DEBUG [DB]: Intentando bloqueo GET_LOCK...")
            cur_main.execute("SELECT GET_LOCK('motor_contable', 0)")
            
            deep = (ciclo % CICLOS_DEEP_AUDIT == 0)
            print(f"\n--- INICIO CICLO {ciclo} ({'DEEP' if deep else 'FAST'}) ---")

            cur_main.execute("SELECT * FROM api_keys WHERE status=1")
            users = cur_main.fetchall()
            print(f"DEBUG [DB]: {len(users)} API Keys activas encontradas.")

            for u in users:
                k_d = descifrar_dato(u['api_key'], MASTER_KEY)
                s_d = descifrar_dato(u['api_secret'], MASTER_KEY)
                
                if not k_d or not s_d:
                    print(f"⚠️ ERROR: No se pudo descifrar las llaves para User {u['user_id']}")
                    continue
                
                if 'binance' in u['broker_name'].lower():
                    motor_binance(k_d, s_d, u['user_id'], db, deep)
                elif 'bingx' in u['broker_name'].lower():
                    motor_bingx(k_d, s_d, u['user_id'], db, session, deep)
            
            print(f"DEBUG [DB]: Haciendo commit de los datos...")
            db.commit()
            cur_main.execute("SELECT RELEASE_LOCK('motor_contable')")
            db.close(); ciclo += 1
            print(f"--- FIN CICLO {ciclo-1}. ESPERA {ESPERA_CICLO_RAPIDO}s ---")
            time.sleep(ESPERA_CICLO_RAPIDO)
            
        except mysql.connector.Error as err:
            print(f"❌ ERROR SQL: {err}")
            time.sleep(30)
        except Exception as e:
            print(f"⚠️ ERROR CRÍTICO: {e}")
            import traceback
            traceback.print_exc() # Esto te dirá la línea exacta del error
            time.sleep(30)