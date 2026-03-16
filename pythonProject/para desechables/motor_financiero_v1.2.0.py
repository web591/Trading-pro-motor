import requests
import time
import hmac
import hashlib
import mysql.connector
import base64
import os
import json
from urllib.parse import urlencode
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# Intento de cargar config si existe
try:
    import config
except ImportError:
    config = None

# ==========================================================
# 🚩 DISFRAZ Y SEGURIDAD
# ==========================================================
bingx_session = requests.Session()
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

def get_headers_bingx(api_key):
    return {
        "X-BX-APIKEY": api_key,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://bingx.com/",
        "Connection": "keep-alive"
    }

def descifrar_dato(t, m):
    try:
        if not t or not m: return None
        raw = base64.b64decode(t.strip())
        partes = raw.rsplit(b":::", 1) if b":::" in raw else raw.rsplit(b"::", 1)
        if len(partes) != 2: return None
        data, iv = partes
        key_hash = sha256(m.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except Exception as e:
        print(f"        [!] Error descifrando: {e}")
        return None

# ==========================================================
# 🎯 VINCULACIÓN Y PRECIOS (ACTUALIZADO CON TU LÓGICA DE UNDERLYING)
# ==========================================================
def obtener_traductor_id(cursor, motor_fuente, ticker):
    ticker = ticker.upper().strip()
    
    # 1. Búsqueda Exacta
    sql = "SELECT id FROM sys_traductor_simbolos WHERE motor_fuente = %s AND ticker_motor = %s LIMIT 1"
    cursor.execute(sql, (motor_fuente, ticker))
    row = cursor.fetchone()
    if row: return row
    
    # 2. Búsqueda por Underlying + SPOT (Tu idea para LD y Tickers sin par)
    ticker_limpio = ticker[2:] if ticker.startswith("LD") else (ticker[3:] if ticker.startswith("STK") else ticker)
    
    sql = """
        SELECT id FROM sys_traductor_simbolos 
        WHERE underlying = %s AND categoria_producto = 'SPOT' 
        LIMIT 1
    """
    cursor.execute(sql, (ticker_limpio,))
    res = cursor.fetchone()
    
    if not res:
        # No imprimimos error aquí para permitir que monedas como MITO fluyan con ID NULL
        pass
    return res

def obtener_precio_usd(cursor, tid, asset_name):
    # Bypass para Stablecoins (Como pediste, no necesitan traductor si son estas)
    if asset_name.upper() in ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']: return 1.0
    
    try:
        if tid:
            tid_val = tid['id'] if isinstance(tid, dict) else tid[0]
            sql = "SELECT price FROM sys_precios_activos WHERE traductor_id = %s ORDER BY last_update DESC LIMIT 1"
            cursor.execute(sql, (tid_val,))
            row = cursor.fetchone()
            if row: return float(row['price'] if isinstance(row, dict) else row[0])
    except: pass
    return 0.0

# ==========================================================
# SYNC ESTADO
# ==========================================================
def obtener_sync(cursor,user_id,broker,endpoint):
    sql="SELECT last_timestamp FROM sys_sync_estado WHERE user_id=%s AND broker=%s AND endpoint=%s"
    cursor.execute(sql,(user_id,broker,endpoint))
    r=cursor.fetchone()
    ts = r['last_timestamp'] if r and isinstance(r, dict) else (r[0] if r else 0)
    return ts

def guardar_sync(cursor,user_id,broker,endpoint,timestamp):
    sql="""
    INSERT INTO sys_sync_estado (user_id,broker,endpoint,last_timestamp)
    VALUES(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE last_timestamp=%s
    """
    cursor.execute(sql,(user_id,broker,endpoint,timestamp,timestamp))

# ==========================================================
# REGISTRO CONTABLE GLOBAL
# ==========================================================
def registrar_transaccion_global(cursor, data):
    res_traductor = obtener_traductor_id(cursor, data["broker"], data["asset"])
    cuenta_tipo = "SPOT"
    tipo_inv = "CRYPTO"
    traductor_id = None

    if res_traductor:
        traductor_id = res_traductor['id'] if isinstance(res_traductor, dict) else res_traductor[0]
        sql_info = "SELECT categoria_producto, tipo_investment FROM sys_traductor_simbolos WHERE id = %s"
        cursor.execute(sql_info, (traductor_id,))
        info_extra = cursor.fetchone()
        if info_extra:
            cuenta_tipo = info_extra['categoria_producto'] if isinstance(info_extra, dict) else info_extra[0]
            tipo_inv = info_extra['tipo_investment'] if isinstance(info_extra, dict) else info_extra[1]

    id_final = f"{data['user_id']}-CASH-{data['id_externo']}"
    sql = """
    INSERT INTO transacciones_globales 
    (id_externo, user_id, tipo_investment, cuenta_tipo, categoria, asset, 
     traductor_id, monto_neto, fecha_utc, broker, raw_json_backup)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE monto_neto = VALUES(monto_neto), raw_json_backup = VALUES(raw_json_backup)
    """
    cursor.execute(sql, (
        id_final, data["user_id"], tipo_inv, cuenta_tipo, data["tipo_evento"], 
        data["asset"], traductor_id, data["cantidad"], data["fecha"], 
        data["broker"], data.get("raw", "{}")
    ))

# ==========================================================
# 📝 REGISTRO MAESTRO (ACTUALIZADO PARA PERMITIR MITO/AIRDROPS)
# ==========================================================
def registrar_cashflow(cursor, data):
    tid = obtener_traductor_id(cursor, data["broker"], data["asset"])
    precio = obtener_precio_usd(cursor, tid, data["asset"])
    valor_usd = float(data["cantidad"]) * precio

    traductor_id_final = None
    if tid:
        traductor_id_final = tid['id'] if isinstance(tid, dict) else tid[0]

    sql = """
    INSERT INTO sys_cashflows (user_id, broker, tipo_evento, asset, cantidad, ticker_motor, valor_usd, fecha_utc, id_externo, raw_json, traductor_id)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE raw_json=VALUES(raw_json), valor_usd=VALUES(valor_usd), traductor_id=VALUES(traductor_id)
    """
    cursor.execute(sql, (data["user_id"], data["broker"], data["tipo_evento"], data["asset"], 
                         data["cantidad"], data["ticker_motor"], valor_usd, data["fecha"], 
                         data["id_externo"], data.get("raw", "{}"), traductor_id_final))
    
    registrar_transaccion_global(cursor, data)

def rate_limit():
    time.sleep(0.15)

# ==========================================================
# 🔌 BINANCE FUNCTIONS
# ==========================================================
def binance_sign(secret, query):
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()

def binance_dividends(db, user_id, key, secret):
    cursor = db.cursor(dictionary=True)
    endpoint = "BINANCE_DIVIDEND"
    last_sync = obtener_sync(cursor, user_id, "BINANCE", endpoint)
    print(f"    [+] {endpoint}: Buscando beneficios de Earn/Staking...")
    
    params = {"startTime": last_sync, "limit": 500, "timestamp": int(time.time()*1000)}
    query = urlencode(params)
    url = f"https://api.binance.com/sapi/v1/asset/assetDividend?{query}&signature={binance_sign(secret, query)}"
    r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()

    count = 0
    if "rows" in r:
        max_time = last_sync
        for d in r["rows"]:
            ts = int(d["divTime"])
            if ts <= last_sync: continue
            registrar_cashflow(cursor, {
                "user_id": user_id, "broker": "BINANCE", "tipo_evento": "DIVIDEND",
                "asset": d["asset"], "cantidad": float(d["amount"]), "ticker_motor": d["enInfo"],
                "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts/1000)),
                "id_externo": f"BN-DIV-{d['id']}", "raw": json.dumps(d)
            })
            if ts > max_time: max_time = ts
            count += 1
        guardar_sync(cursor, user_id, "BINANCE", endpoint, max_time)
    print(f"    [OK] {endpoint}: {count} procesados.")

def binance_income(db, user_id, key, secret):
    cursor = db.cursor(dictionary=True)
    endpoint = "BINANCE_INCOME"
    last_sync = obtener_sync(cursor, user_id, "BINANCE", endpoint)
    print(f"    [+] {endpoint}: Sincronizando desde TS {last_sync}...")
    
    base = "https://fapi.binance.com/fapi/v1/income"
    count = 0
    while True:
        params = {"startTime": last_sync, "limit": 1000, "timestamp": int(time.time()*1000)}
        query = urlencode(params)
        url = f"{base}?{query}&signature={binance_sign(secret, query)}"
        r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()

        if not r or not isinstance(r, list): break 

        max_time = last_sync
        for i in r:
            ts = int(i["time"])
            registrar_cashflow(cursor, {
                "user_id": user_id, "broker": "BINANCE", "tipo_evento": i["incomeType"],
                "asset": i["asset"], "cantidad": float(i["income"]), "ticker_motor": i.get("symbol"),
                "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts/1000)),
                "id_externo": f"BN-INC-{i['tranId']}", "raw": json.dumps(i)
            })
            if ts > max_time: max_time = ts
            count += 1

        guardar_sync(cursor, user_id, "BINANCE", endpoint, max_time)
        last_sync = max_time + 1
        if len(r) < 1000: break
        rate_limit()
    print(f"    [OK] {endpoint}: {count} procesados.")

def binance_deposits(db, user_id, key, secret):
    cursor = db.cursor(dictionary=True)
    endpoint = "BINANCE_DEPOSIT"
    last_sync = obtener_sync(cursor, user_id, "BINANCE", endpoint)
    print(f"    [+] {endpoint}: Buscando nuevos depósitos...")
    
    params = {"timestamp": int(time.time()*1000)}
    query = urlencode(params)
    url = f"https://api.binance.com/sapi/v1/capital/deposit/hisrec?{query}&signature={binance_sign(secret, query)}"
    r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
    
    count = 0
    max_time = last_sync
    if isinstance(r, list):
        for d in r:
            ts = int(d["insertTime"])
            if ts <= last_sync: continue
            registrar_cashflow(cursor, {
                "user_id": user_id, "broker": "BINANCE", "tipo_evento": "DEPOSIT",
                "asset": d["coin"], "cantidad": float(d["amount"]), "ticker_motor": None,
                "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts/1000)),
                "id_externo": f"BN-DEP-{d['txId']}", "raw": json.dumps(d)
            })
            if ts > max_time: max_time = ts
            count += 1
    
    guardar_sync(cursor, user_id, "BINANCE", endpoint, max_time)
    print(f"    [OK] {endpoint}: {count} nuevos.")

def binance_withdraw(db, user_id, key, secret):
    cursor = db.cursor(dictionary=True)
    print(f"    [+] BINANCE_WITHDRAW: Verificando historial...")
    params = {"timestamp": int(time.time()*1000)}
    query = urlencode(params)
    url = f"https://api.binance.com/sapi/v1/capital/withdraw/history?{query}&signature={binance_sign(secret, query)}"
    r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
    count = 0
    if isinstance(r, list):
        for w in r:
            registrar_cashflow(cursor, {
                "user_id": user_id, "broker": "BINANCE", "tipo_evento": "WITHDRAW",
                "asset": w["coin"], "cantidad": -float(w["amount"]), "ticker_motor": None,
                "fecha": w["applyTime"], "id_externo": f"BN-WITH-{w['id']}", "raw": json.dumps(w)
            })
            count += 1
    print(f"    [OK] BINANCE_WITHDRAW: {count} registros.")

def binance_dust_log(db, uid, key, secret):
    cursor = db.cursor(dictionary=True)
    print(f"    [+] BINANCE_DUST: Procesando conversiones a BNB...")
    params = urlencode({"timestamp": int(time.time()*1000)})
    url = f"https://api.binance.com/sapi/v1/asset/dribblet?{params}&signature={binance_sign(secret, params)}"
    r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
    if "userAssetDribblets" not in r: return

    for entry in r["userAssetDribblets"]:
        fecha = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(entry["operateTime"]/1000))
        for detail in entry["userAssetDribbletDetails"]:
            registrar_cashflow(cursor, {
                "user_id": uid, "broker": "BINANCE", "tipo_evento": "DUST_OUT",
                "asset": detail["fromAsset"], "cantidad": -float(detail["amount"]), "ticker_motor": None,
                "fecha": fecha, "id_externo": f"BN-DUST-{detail['transId']}-OUT", "raw": json.dumps(detail)
            })
        registrar_cashflow(cursor, {
            "user_id": uid, "broker": "BINANCE", "tipo_evento": "DUST_IN",
            "asset": "BNB", "cantidad": float(entry["totalTransferedAmount"]), "ticker_motor": None,
            "fecha": fecha, "id_externo": f"BN-DUST-{entry['operateTime']}-IN", "raw": json.dumps(entry)
        })
    print(f"    [OK] BINANCE_DUST: Finalizado.")

def binance_convert_history(db, uid, key, secret):
    cursor = db.cursor(dictionary=True)
    print(f"    [+] BINANCE_CONVERT: Sincronizando (30 días)...")
    ts = int(time.time()*1000)
    params = urlencode({"timestamp": ts, "startTime": ts - (30*24*60*60*1000)})
    url = f"https://api.binance.com/sapi/v1/convert/tradeFlow?{params}&signature={binance_sign(secret, params)}"
    r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
    if "list" not in r: return
    for c in r["list"]:
        fecha = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(c["createTime"]/1000))
        registrar_cashflow(cursor, {
            "user_id": uid, "broker": "BINANCE", "tipo_evento": "CONVERT_OUT",
            "asset": c["fromAsset"], "cantidad": -float(c["fromAmount"]), "ticker_motor": None,
            "fecha": fecha, "id_externo": f"BN-CONV-{c['orderId']}-OUT", "raw": json.dumps(c)
        })
        registrar_cashflow(cursor, {
            "user_id": uid, "broker": "BINANCE", "tipo_evento": "CONVERT_IN",
            "asset": c["toAsset"], "cantidad": float(c["toAmount"]), "ticker_motor": None,
            "fecha": fecha, "id_externo": f"BN-CONV-{c['orderId']}-IN", "raw": json.dumps(c)
        })
    print(f"    [OK] BINANCE_CONVERT: Finalizado.")

def binance_transfers(db, user_id, key, secret):
    cursor = db.cursor(dictionary=True)
    endpoint = "BINANCE_TRANSFER"
    print(f"    [+] {endpoint}: Revisando transferencias internas...")
    params = {"timestamp": int(time.time()*1000)}
    query = urlencode(params)
    url = f"https://api.binance.com/sapi/v1/asset/transfer?{query}&signature={binance_sign(secret, query)}"
    r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
    if "rows" not in r: return
    for t in r["rows"]:
        registrar_cashflow(cursor, {
            "user_id": user_id, "broker": "BINANCE", "tipo_evento": "TRANSFER",
            "asset": t["asset"], "cantidad": float(t["amount"]), "ticker_motor": None,
            "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(t["timestamp"]/1000)),
            "id_externo": f"BN-TR-{t['tranId']}", "raw": json.dumps(t)
        })
    print(f"    [OK] {endpoint}: Finalizado.")

# ==========================================================
# 🔌 BINGX FUNCTIONS
# ==========================================================
def bingx_income(db, user_id, key, secret):
    cursor = db.cursor(dictionary=True)
    endpoint = "BINGX_INCOME"
    last_sync = obtener_sync(cursor, user_id, "BINGX", endpoint)
    print(f"    [+] {endpoint}: Sincronizando desde TS {last_sync}...")
    
    params = {"limit": 100, "timestamp": int(time.time()*1000)}
    query = urlencode(params)
    sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    url = f"https://open-api.bingx.com/openApi/swap/v2/user/income?{query}&signature={sig}"
    r = requests.get(url, headers=get_headers_bingx(key)).json()

    count = 0
    if r.get("code") == 0 and "data" in r:
        max_ts = last_sync
        for i in r["data"]:
            ts = int(i["time"])
            if ts <= last_sync: continue
            registrar_cashflow(cursor, {
                "user_id": user_id, "broker": "BINGX", "tipo_evento": i["incomeType"],
                "asset": i["asset"], "cantidad": float(i["income"]), "ticker_motor": i.get("symbol"),
                "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts/1000)),
                "id_externo": f"BX-INC-{ts}-{i['asset']}", "raw": json.dumps(i)
            })
            if ts > max_ts: max_ts = ts
            count += 1
        guardar_sync(cursor, user_id, "BINGX", endpoint, max_ts)
    print(f"    [OK] {endpoint}: {count} procesados.")

def bingx_deposits(db, user_id, key, secret):
    cursor = db.cursor(dictionary=True)
    print(f"    [+] BINGX_DEPOSIT: Consultando...")
    params = {"timestamp": int(time.time()*1000)}
    query = urlencode(params)
    sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    url = f"https://open-api.bingx.com/openApi/api/v3/capital/deposit/hisrec?{query}&signature={sig}"
    r = requests.get(url, headers=get_headers_bingx(key)).json()
    if "data" not in r: return
    for d in r["data"]:
        registrar_cashflow(cursor, {
            "user_id": user_id, "broker": "BINGX", "tipo_evento": "DEPOSIT",
            "asset": d["coin"], "cantidad": float(d["amount"]), "ticker_motor": None,
            "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(d["insertTime"]/1000)),
            "id_externo": f"BX-DEP-{d['txId']}", "raw": json.dumps(d)
        })
    print(f"    [OK] BINGX_DEPOSIT: Finalizado.")

def bingx_withdraw(db, user_id, key, secret):
    cursor = db.cursor(dictionary=True)
    print(f"    [+] BINGX_WITHDRAW: Consultando...")
    params = {"timestamp": int(time.time()*1000)}
    query = urlencode(params)
    sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    url = f"https://open-api.bingx.com/openApi/api/v3/capital/withdraw/history?{query}&signature={sig}"
    r = requests.get(url, headers=get_headers_bingx(key)).json()
    if "data" not in r: return
    for w in r["data"]:
        registrar_cashflow(cursor, {
            "user_id": user_id, "broker": "BINGX", "tipo_evento": "WITHDRAW",
            "asset": w["coin"], "cantidad": -float(w["amount"]), "ticker_motor": None,
            "fecha": w["applyTime"], "id_externo": f"BX-WITH-{w['id']}", "raw": json.dumps(w)
        })
    print(f"    [OK] BINGX_WITHDRAW: Finalizado.")

# ==========================================================
# 🚀 EJECUCIÓN PRINCIPAL
# ==========================================================
def ejecutar_motor_financiero(db):
    print(f"\n{'='*60}")
    print(f"💎 MOTOR FINANCIERO v1.2.0 - AUDITORÍA Y DIVIDENDOS")
    print(f"{'='*60}")
    
    cursor = db.cursor(dictionary=True)
    sql = "SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1"
    
    try:
        cursor.execute(sql)
        usuarios = cursor.fetchall()
        print(f"[*] Usuarios activos encontrados: {len(usuarios)}")
        
        for u in usuarios:
            print(f"\n>> TRABAJANDO: User {u['user_id']} | {u['broker_name']}")
            
            k = descifrar_dato(u['api_key'], MASTER_KEY)
            s = descifrar_dato(u['api_secret'], MASTER_KEY)

            if not k or not s:
                continue

            broker = u['broker_name'].upper()

            if broker == "BINANCE":
                binance_income(db, u['user_id'], k, s)
                binance_dividends(db, u['user_id'], k, s) # NUEVO: Dividendos de Earn
                binance_deposits(db, u['user_id'], k, s)
                binance_withdraw(db, u['user_id'], k, s)
                binance_convert_history(db, u['user_id'], k, s)
                binance_dust_log(db, u['user_id'], k, s)
                binance_transfers(db, u['user_id'], k, s)

            elif broker == "BINGX":
                bingx_income(db, u['user_id'], k, s)
                bingx_deposits(db, u['user_id'], k, s)
                bingx_withdraw(db, u['user_id'], k, s)

            db.commit()
            print(f"    [v] Cambios guardados para User {u['user_id']}.")

    except Exception as e:
        print(f"\n[CRITICAL] Error en ejecución: {e}")
    
    print(f"\n{'='*60}")
    print(f"🏁 CICLO FINALIZADO")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    db = None
    try:
        db = mysql.connector.connect(**config.DB_CONFIG)
        ejecutar_motor_financiero(db)
    except Exception as e:
        print(f"\n[ERROR DE CONEXIÓN] No se pudo conectar a la DB: {e}")
    finally:
        if db and db.is_connected():
            db.close()
            print("[*] Conexión a DB cerrada.")