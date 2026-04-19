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
import socket
from datetime import datetime, timezone

# ==========================================================
# 🔧 UTILIDADES DE CONTROL (Añadidas para evitar errores de ejecución)
# ==========================================================
def rate_limit(segundos=0.5):
    """Evita el error de definición y bloqueos de API"""
    import time
    time.sleep(segundos)

# Intento de cargar config si existe
try:
    import config
except ImportError:
    config = None

def obtener_lock(cursor, lock_name, timeout=900): # 15 minutos para el financiero
    host = "GITHUB_ACTION" if os.getenv('GITHUB_ACTIONS') == 'true' else socket.gethostname()
    cursor.execute("DELETE FROM sys_locks WHERE lock_name = %s AND lock_time < NOW() - INTERVAL %s SECOND", (lock_name, timeout))
    try:
        cursor.execute("INSERT INTO sys_locks (lock_name, locked_by, lock_time) VALUES (%s, %s, NOW())", (lock_name, host))
        return True
    except:
        return False

def liberar_lock(cursor, lock_name):
    cursor.execute("DELETE FROM sys_locks WHERE lock_name = %s", (lock_name,))

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
# 🎯 VINCULACIÓN MAESTRA v6.6.7 - UNIVERSAL (Binance & BingX)
# ==========================================================
def obtener_traductor_id_universal(cursor, motor_fuente, ticker_api):
    """
    v6.6.7 - Rescata IDs sin importar si el motor_fuente exacto cambió.
    Especialmente útil para Cashflows donde el ticker puede venir sucio.
    """
    if not ticker_api: return None
    
    ticker = str(ticker_api).upper().strip()
    motor_fuente = motor_fuente.lower()

    # 1. BÚSQUEDA EXACTA
    sql_exacto = """
        SELECT id, categoria_producto, tipo_investment, underlying, quote_asset 
        FROM sys_traductor_simbolos 
        WHERE motor_fuente = %s AND ticker_motor = %s
        LIMIT 1
    """
    cursor.execute(sql_exacto, (motor_fuente, ticker))
    res = cursor.fetchone()
    if res: return res

    # 2. RESCATE ELÁSTICO (Si falla la exacta)
    ticker_limpio = ticker.replace("-", "").replace("/", "").replace("=X", "").replace("^", "")
    u_search = ticker_limpio

    if "bingx" in motor_fuente:
        for basura in ["NCSK", "2USD", "USDT", "USDC", "USD", "_PERP"]:
            u_search = u_search.replace(basura, "")
    elif "binance" in motor_fuente:
        for basura in ["LD", "STK", "USDT", "USDC"]:
            u_search = u_search.replace(basura, "")

    if not u_search: u_search = ticker_limpio

    sql_rescate = """
        SELECT id, categoria_producto, tipo_investment, underlying, quote_asset 
        FROM sys_traductor_simbolos 
        WHERE motor_fuente LIKE %s 
        AND (REPLACE(ticker_motor, '-', '') = %s OR underlying = %s)
        LIMIT 1
    """
    motor_patron = f"{motor_fuente.split('_')[0]}%"
    cursor.execute(sql_rescate, (motor_patron, ticker_limpio, u_search))
    return cursor.fetchone()

def obtener_precio_usd(cursor, tid, asset_name):
    """
    Busca el precio más reciente usando el ID Maestro de Yahoo como ancla principal.
    """
    asset_upper = asset_name.upper().strip().replace('"', '')
    
    # 1. 🛡️ BYPASS PARA STABLECOINS
    if asset_upper in ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD', 'FDUSD']: 
        return 1.0
    
    try:
        # Extraer el ID si viene como diccionario o tupla
        tid_val = tid['id'] if isinstance(tid, dict) else tid
        if isinstance(tid_val, (list, tuple)): tid_val = tid_val[0]

        # 2. 🔍 INTENTO A: Por el ID proporcionado (Si el traductor ya nos dio el bueno)
        if tid_val:
            sql = "SELECT price FROM sys_precios_activos WHERE traductor_id = %s ORDER BY last_update DESC LIMIT 1"
            cursor.execute(sql, (tid_val,))
            row = cursor.fetchone()
            if row:
                price = row['price'] if isinstance(row, dict) else row[0]
                if price and float(price) > 0:
                    return float(price)

        # 3. 🔍 INTENTO B: BLINDAJE MAESTRO (Cruzado por nombre + Motor Yahoo)
        # Si el ID de arriba falló o era NULL, buscamos el precio del registro de Yahoo para ese asset
        sql_yahoo = """
            SELECT p.price 
            FROM sys_precios_activos p
            JOIN sys_traductor_simbolos t ON p.traductor_id = t.id
            WHERE t.underlying = %s AND t.motor_fuente = 'yahoo_sym'
            ORDER BY p.last_update DESC LIMIT 1
        """
        cursor.execute(sql_yahoo, (asset_upper,))
        row_y = cursor.fetchone()
        if row_y:
            price = row_y['price'] if isinstance(row_y, dict) else row_y[0]
            if price and float(price) > 0:
                return float(price)

        # 4. 🔍 INTENTO C: Fallback General (Cualquier motor que tenga precio para ese underlying)
        sql_any = """
            SELECT p.price 
            FROM sys_precios_activos p
            JOIN sys_traductor_simbolos t ON p.traductor_id = t.id
            WHERE t.underlying = %s 
            ORDER BY p.last_update DESC LIMIT 1
        """
        cursor.execute(sql_any, (asset_upper,))
        row_any = cursor.fetchone()
        if row_any:
            price = row_any['price'] if isinstance(row_any, dict) else row_any[0]
            if price and float(price) > 0:
                return float(price)

    except Exception as e:
        print(f"      [!] Error sutil en obtener_precio_usd ({asset_upper}): {e}")
    
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

def registrar_cashflow(cursor, d):
    """
    v1.3.4 - Registrador Unificado con Vinculación Maestra.
    Busca el traductor_id automáticamente antes de insertar.
    """
    # 1. Intentamos obtener el traductor_id usando nuestra función maestra
    # Usamos el ticker_motor o el asset como base para la búsqueda
    ticker_busqueda = d.get('ticker_motor') if d.get('ticker_motor') else d.get('asset')
    
    info_traductor = obtener_traductor_id_universal(cursor, d['broker'], ticker_busqueda)
    
    traductor_id = None
    if info_traductor:
        # Si es un diccionario (cursor dictionary=True)
        if isinstance(info_traductor, dict):
            traductor_id = info_traductor['id']
        else: # Si es una tupla
            traductor_id = info_traductor[0]

    # 2. Insertamos en sys_cashflows
    sql = """
        INSERT INTO sys_cashflows (
            user_id, broker, tipo_evento, asset, cantidad, 
            ticker_motor, fecha_utc, id_externo, raw_json, 
            traductor_id, revisado
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        ON DUPLICATE KEY UPDATE 
            traductor_id = VALUES(traductor_id),
            raw_json = VALUES(raw_json)
    """
    
    # Aseguramos que el JSON sea válido
    raw_json = d.get('raw') if isinstance(d.get('raw'), str) else json.dumps(d.get('raw'))

    valores = (
        d['user_id'], d['broker'].upper(), d['tipo_evento'].upper(),
        d['asset'].upper(), d['cantidad'], d.get('ticker_motor'),
        d['fecha'], d['id_externo'], raw_json, traductor_id
    )
    
    cursor.execute(sql, valores)

# ==========================================================
# 🔌 BINANCE FUNCTIONS
# ==========================================================
def binance_sign(secret, query):
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()

def binance_dividends(db, user_id, key, secret):
    cursor = db.cursor(dictionary=True)
    endpoint = "BINANCE_DIVIDEND"
    last_sync = obtener_sync(cursor, user_id, "BINANCE", endpoint)
    start_ts = last_sync + 1 if last_sync > 0 else int((time.time() - 90*24*3600)*1000)
    end_now = int(time.time()*1000)
    print(f"    [+] {endpoint}: Escaneo por bloques desde {time.strftime('%Y-%m-%d', time.gmtime(start_ts/1000))}...")

    count_total = 0
    max_ts_global = last_sync

    while start_ts < end_now:
        chunk_end = start_ts + (7 * 24 * 60 * 60 * 1000)  # bloques de 7 días
        if chunk_end > end_now:
            chunk_end = end_now

        params = {
            "startTime": start_ts,
            "endTime": chunk_end,
            "limit": 500,
            "timestamp": int(time.time()*1000)
        }

        query = urlencode(params)
        signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://api.binance.com/sapi/v1/asset/assetDividend?{query}&signature={signature}"

        try:
            r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
            if "rows" in r and r["rows"]:
                for d in r["rows"]:
                    ts = int(d["divTime"])
                    if ts <= last_sync:
                        continue
                    tran_id = d.get("tranId", d.get("id"))
                    registrar_cashflow(cursor, {
                        "user_id": user_id,
                        "broker": "BINANCE",
                        "tipo_evento": "DIVIDEND",
                        "asset": d["asset"],
                        "cantidad": float(d["amount"]),
                        "ticker_motor": d.get("enInfo", "Flexible"),
                        "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts/1000)),
                        "id_externo": f"BN-DIV-{tran_id}",
                        "raw": json.dumps(d)
                    })
                    if ts > max_ts_global:
                        max_ts_global = ts
                    count_total += 1
            start_ts = chunk_end + 1
            rate_limit()
        except Exception as e:
            print(f"    [!] Error en bloque dividendos: {e}")
            break

    if max_ts_global > last_sync:
        guardar_sync(cursor, user_id, "BINANCE", endpoint, max_ts_global)
    print(f"    [OK] {endpoint}: {count_total} procesados.")

# ==========================================================
# BINANCE INCOME NORMALIZADO
# Version 1.1 (ALINEADO A MODELO CONTABLE)
# ==========================================================
def binance_income(db, user_id, key, secret):

    cursor = db.cursor(dictionary=True)
    endpoint = "BINANCE_INCOME"
    last_sync = obtener_sync(cursor, user_id, "BINANCE", endpoint)
    
    if last_sync == 0:
        actual_start = int((time.time() - 90*24*3600)*1000)
    else:
        actual_start = last_sync + 1

    print(f"    [+] {endpoint}: Sincronizando desde TS {actual_start}...")
    
    base = "https://fapi.binance.com/fapi/v1/income"
    count = 0

    while True:
        params = {
            "startTime": actual_start,
            "limit": 1000,
            "timestamp": int(time.time()*1000)
        }

        query = urlencode(params)
        url = f"{base}?{query}&signature={binance_sign(secret, query)}"

        r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()

        if not r or not isinstance(r, list):
            break 

        max_time = actual_start

        for i in r:
            ts = int(i["time"])
            tipo_api = i["incomeType"]

            # 🔥 FILTRO INTELIGENTE
            if tipo_api not in ["FUNDING_FEE", "REALIZED_PNL"]:
                continue

            # 🔥 NORMALIZACIÓN
            if tipo_api == "FUNDING_FEE":
                tipo_evento = "FUNDING"
            elif tipo_api == "REALIZED_PNL":
                tipo_evento = "PNL_VALIDACION"

            asset = i["asset"]
            monto = float(i["income"])
            symbol = i.get("symbol") or asset

            fecha_sql = time.strftime(
                '%Y-%m-%d %H:%M:%S',
                time.gmtime(ts/1000)
            )

            id_ext = f"BN-INC-{user_id}-{i['tranId']}"

            registrar_cashflow(cursor, {
                "user_id": user_id,
                "broker": "BINANCE",
                "tipo_evento": tipo_evento,
                "asset": asset,
                "cantidad": monto,
                "ticker_motor": symbol,
                "fecha": fecha_sql,
                "id_externo": id_ext,
                "raw": json.dumps(i)
            })

            if ts > max_time:
                max_time = ts

            count += 1

        guardar_sync(cursor, user_id, "BINANCE", endpoint, max_time)
        actual_start = max_time + 1

        if len(r) < 1000:
            break

        rate_limit()

    print(f"    [OK] {endpoint}: {count} procesados.")
    
# ==========================================================
# SECCIÓN MODIFICADA: DEPÓSITOS Y RETIROS (FILTRO BINANCE PAY)
# ==========================================================
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
            
            raw_str = json.dumps(d)
            
            # NUEVO FILTRO INTELIGENTE: Si es Binance Pay, lo marcamos como TRANSFER
            if "Received from" in raw_str or "Paid to" in raw_str:
                evento_asignado = "TRANSFER_IN"
            else:
                evento_asignado = "DEPOSIT"
            
            registrar_cashflow(cursor, {
                "user_id": user_id, "broker": "BINANCE", "tipo_evento": evento_asignado,
                "asset": d["coin"], "cantidad": float(d["amount"]), "ticker_motor": None,
                "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts/1000)),
                "id_externo": f"BN-DEP-{d['txId']}", "raw": raw_str
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
            raw_str = json.dumps(w)
            
            # NUEVO FILTRO INTELIGENTE: Si es Binance Pay, lo marcamos como TRANSFER
            if "Received from" in raw_str or "Paid to" in raw_str:
                evento_asignado = "TRANSFER_OUT"
            else:
                evento_asignado = "WITHDRAW"

            registrar_cashflow(cursor, {
                "user_id": user_id, "broker": "BINANCE", "tipo_evento": evento_asignado,
                "asset": w["coin"], "cantidad": -float(w["amount"]), "ticker_motor": None,
                "fecha": w["applyTime"], "id_externo": f"BN-WITH-{w['id']}", "raw": raw_str
            })
            count += 1
    print(f"    [OK] BINANCE_WITHDRAW: {count} registros.")
# ==========================================================

def binance_dust_log(db, uid, key, secret):
    cursor = db.cursor(dictionary=True)
    endpoint = "BINANCE_DUST"
    last_sync = obtener_sync(cursor, uid, "BINANCE", endpoint)
    
    # Si no hay sync, empezamos en Octubre 2021
    start_ts = int(last_sync + 1) if last_sync > 0 else 1633046400000 
    end_now = int(time.time() * 1000)
    
    print(f"    [+] {endpoint}: Iniciando escaneo histórico desde 2021...")

    while start_ts < end_now:
        # Usamos bloques de 90 días para Dust (es más estable que 30)
        chunk_end = start_ts + (90 * 24 * 60 * 60 * 1000)
        if chunk_end > end_now: chunk_end = end_now
        
        fecha_h = time.strftime('%Y-%m-%d', time.gmtime(start_ts/1000))
        print(f"        [..] Dust: Escaneando desde {fecha_h}...", end="\r")
        
        params = {
            "startTime": start_ts,
            "endTime": chunk_end,
            "timestamp": int(time.time()*1000)
        }
        
        query = urlencode(params)
        signature = binance_sign(secret, query)
        url = f"https://api.binance.com/sapi/v1/asset/dribblet?{query}&signature={signature}"
        
        try:
            r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
            
            if "userAssetDribblets" in r and r["userAssetDribblets"]:
                count_bloque = 0
                for entry in r["userAssetDribblets"]:
                    ts = int(entry["operateTime"])
                    fecha = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts/1000))
                    
                    if "userAssetDribbletDetails" in entry:
                        for detail in entry["userAssetDribbletDetails"]:
                            asset_s = detail["fromAsset"]
                            registrar_cashflow(cursor, {
                                "user_id": uid, "broker": "BINANCE", "tipo_evento": "DUST_OUT",
                                "asset": asset_s, "cantidad": -float(detail["amount"]), 
                                "ticker_motor": None, "fecha": fecha, 
                                "id_externo": f"BN-DUST-{detail['transId']}-{asset_s}", 
                                "raw": json.dumps(detail)
                            })
                    
                    asset_e = entry.get("operatingAsset", "BNB")
                    registrar_cashflow(cursor, {
                        "user_id": uid, "broker": "BINANCE", "tipo_evento": "DUST_IN",
                        "asset": asset_e, "cantidad": float(entry["totalTransferedAmount"]), 
                        "ticker_motor": None, "fecha": fecha, 
                        "id_externo": f"BN-DUST-{ts}-IN", 
                        "raw": json.dumps(entry)
                    })
                    count_bloque += 1
                
                if count_bloque > 0:
                    print(f"\n        [OK] Bloque {fecha_h}: {count_bloque} conversiones encontradas.")

            # Guardamos progreso del bloque
            guardar_sync(cursor, uid, "BINANCE", endpoint, chunk_end)
            db.commit()
            
            start_ts = chunk_end + 1
            rate_limit()

        except Exception as e:
            print(f"\n    [!] Error en bloque Dust: {e}")
            break
            
    print(f"\n    [OK] {endpoint}: Historial completo sincronizado.")

def binance_convert_history(db, uid, key, secret):
    cursor = db.cursor(dictionary=True)
    endpoint = "BINANCE_CONVERT"
    last_sync = obtener_sync(cursor, uid, "BINANCE", endpoint)
    
    start_ts = last_sync + 1 if last_sync > 0 else 1633046400000
    end_now = int(time.time() * 1000)
    
    print(f"    [+] {endpoint}: Iniciando escaneo histórico por bloques...")

    while start_ts < end_now:
        chunk_end = start_ts + (30 * 24 * 60 * 60 * 1000)
        if chunk_end > end_now: chunk_end = end_now
        
        # MOSTRAR PROGRESO EN CONSOLA
        fecha_h = time.strftime('%Y-%m-%d', time.gmtime(start_ts/1000))
        print(f"        [..] Convert: Escaneando desde {fecha_h}...", end="\r")
        
        params = {
            "timestamp": int(time.time()*1000),
            "startTime": start_ts,
            "endTime": chunk_end,
            "limit": 1000
        }
        
        query = urlencode(params)
        url = f"https://api.binance.com/sapi/v1/convert/tradeFlow?{query}&signature={binance_sign(secret, query)}"
        
        try:
            r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
            if "list" in r and r["list"]:
                for c in r["list"]:
                    ts = int(c["createTime"])
                    fecha = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts/1000))
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
            
            guardar_sync(cursor, uid, "BINANCE", endpoint, chunk_end)
            db.commit()
            start_ts = chunk_end + 1
            rate_limit()
            
        except Exception as e:
            print(f"\n    [!] Error en bloque Convert: {e}")
            break

    print(f"\n    [OK] {endpoint}: Historial actualizado.")

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

def binance_mining(db, user_id, key, secret):
    cursor = db.cursor(dictionary=True)
    endpoint = "BINANCE_MINING"
    
    mining_accounts = ["EppETHJafa", "EthJafa01"]
    algoritmos = ["etchash", "ethash"] 
    
    last_sync = obtener_sync(cursor, user_id, "BINANCE", endpoint)
    
    if last_sync == 0:
        actual_start = int((time.time() - 90*24*3600)*1000)
        print(f"    [+] {endpoint}: Escaneo histórico (Límite API 90 días)...")
    else:
        actual_start = last_sync + 1
        print(f"    [+] {endpoint}: Verificando nuevos desde {time.strftime('%Y-%m-%d', time.gmtime(actual_start/1000))}...")
    
    max_ts_global = last_sync
    count = 0

    for account in mining_accounts:
        for algo in algoritmos:
            params = {
                "algo": algo, 
                "userName": account,
                "timestamp": int(time.time()*1000)
            }
            
            query = urlencode(params)
            signature = binance_sign(secret, query)
            url = f"https://api.binance.com/sapi/v1/mining/payment/list?{query}&signature={signature}"
            
            try:
                r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
                
                if r.get("code") == 0 and "data" in r and "accountProfits" in r["data"]:
                    for p in r["data"]["accountProfits"]:
                        ts = int(p["time"])
                        
                        if ts <= last_sync: continue
                        
                        cantidad = p.get("profitAmount", p.get("dayProfit", p.get("amount", 0.0)))
                        
                        registrar_cashflow(cursor, {
                            "user_id": user_id,
                            "broker": "BINANCE",
                            "tipo_evento": "MINING_PAYMENT",
                            "asset": p["coinName"],
                            "cantidad": float(cantidad),
                            "ticker_motor": f"POOL-{account}-{algo}",
                            "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts/1000)),
                            "id_externo": f"BN-MINE-{account}-{ts}-{p['coinName']}",
                            "raw": json.dumps(p)
                        })
                        
                        if ts > max_ts_global: max_ts_global = ts
                        count += 1
                time.sleep(0.2)
            except Exception as e:
                print(f"    [!] Error minando {account} ({algo}): {e}")

    if max_ts_global > last_sync:
        guardar_sync(cursor, user_id, "BINANCE", endpoint, max_ts_global)
        db.commit()
    
    print(f"    [OK] {endpoint}: {count} nuevos pagos registrados correctamente.")

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

    try:
        r = requests.get(url, headers=get_headers_bingx(key)).json()
    except Exception as e:
        print(f"    [!] Error conexión BingX Income: {e}")
        return

    count = 0
    max_ts = last_sync   

    if r.get("code") == 0 and r.get("data"):
        for i in r["data"]:
            ts = int(i["time"])
            if ts <= last_sync:
                continue

            asset = i["asset"]
            symbol = i.get("symbol", asset)
            monto = float(i["income"])
            tipo_api = i["incomeType"]

            fecha_sql = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts/1000))
            id_ext = f"BX-INC-{ts}-{asset}-{tipo_api}"

            if tipo_api == "REALIZED_PNL":
                categoria = "PNL VALIDACION"
            elif tipo_api in ["TRADING_FEE", "COMMISSION"]:
                categoria = "FEE"
            elif tipo_api == "FUNDING_FEE":
                categoria = "FUNDING"
            else:
                categoria = "OTHER"

            registrar_cashflow(cursor, {
                "user_id": user_id,
                "broker": "BINGX",
                "tipo_evento": categoria,
                "asset": asset,
                "cantidad": monto,
                "ticker_motor": symbol,
                "fecha": fecha_sql,
                "id_externo": id_ext,
                "raw": json.dumps(i)
            })

            if ts > max_ts:
                max_ts = ts

            count += 1

        guardar_sync(cursor, user_id, "BINGX", endpoint, max_ts)
    
    print(f"    [OK] {endpoint}: {count} procesados. Nuevo TS: {max_ts}")

def bingx_deposits(db, user_id, key, secret):
    cursor = db.cursor(dictionary=True)
    print(f"    [+] BINGX_DEPOSIT: Consultando...")
    params = {"timestamp": int(time.time()*1000)}
    query = urlencode(params)
    sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    url = f"https://open-api.bingx.com/openApi/api/v3/capital/deposit/hisrec?{query}&signature={sig}"
    
    r = requests.get(url, headers=get_headers_bingx(key)).json()
        
    if "data" not in r or not r["data"]:
        print(f"    [OK] BINGX_DEPOSIT: 0 procesados.")
        return

    count = 0
    for d in r["data"]:
        ts = d["insertTime"]
        fecha_sql = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts/1000))
        
        registrar_cashflow(cursor, {
            "user_id": user_id, 
            "broker": "BINGX",
            "tipo_evento": "DEPOSIT",
            "asset": d["coin"], 
            "cantidad": float(d["amount"]),
            "ticker_motor": d["coin"], # En depósitos el ticker es la moneda
            "fecha": fecha_sql,
            "id_externo": f"BX-DEP-{d['txId']}",
            "raw": d
        })
        count += 1
    print(f"    [OK] BINGX_DEPOSIT: {count} procesados.")

def bingx_withdraw(db, user_id, key, secret):
    cursor = db.cursor(dictionary=True)
    print(f"    [+] BINGX_WITHDRAW: Consultando...")
    params = {"timestamp": int(time.time()*1000)}
    query = urlencode(params)
    sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    url = f"https://open-api.bingx.com/openApi/api/v3/capital/withdraw/history?{query}&signature={sig}"
    
    r = requests.get(url, headers=get_headers_bingx(key)).json()
    if "data" not in r or not r["data"]:
        print(f"    [OK] BINGX_WITHDRAW: 0 procesados.")
        return
    
    count = 0
    for w in r["data"]:
        fecha_sql = w["applyTime"]
        
        registrar_cashflow(cursor, {
            "user_id": user_id, 
            "broker": "BINGX",
            "tipo_evento": "WITHDRAW",
            "asset": w["coin"], 
            "cantidad": -float(w["amount"]), 
            "ticker_motor": w["coin"],
            "fecha": fecha_sql,
            "id_externo": f"BX-WITH-{w['id']}",
            "raw": w
        })
        count += 1
    print(f"    [OK] BINGX_WITHDRAW: {count} procesados.")

def normalizar_cashflows_pendientes(db, user_id):
    cursor = db.cursor(dictionary=True)
    try:
        # Buscamos cashflows sin valor USD
        sql = """
            SELECT id_cashflow, asset, cantidad 
            FROM sys_cashflows 
            WHERE user_id = %s AND (valor_usd IS NULL OR valor_usd = 0)
        """
        cursor.execute(sql, (user_id,))
        pendientes = cursor.fetchall()

        stables = ['USDT', 'USDC', 'FDUSD', 'BUSD', 'DAI']

        for item in pendientes:
            asset = item['asset'].upper()
            precio = 0.0

            if asset in stables:
                precio = 1.0
            else:
                # Intentamos buscar precio solo para activos conocidos (BNB, BTC, ETH, etc.)
                # Si obtener_precio_usd falla, devolverá 0 y lo ignoramos (tus activos raros)
                sql_tid = "SELECT id FROM sys_traductor_simbolos WHERE underlying = %s LIMIT 1"
                cursor.execute(sql_tid, (asset,))
                res = cursor.fetchone()
                if res:
                    precio = obtener_precio_usd(cursor, res['id'], asset)

            if precio > 0:
                v_usd = float(item['cantidad']) * float(precio)
                # Escudo de seguridad (opcional para cashflows, suelen ser montos pequeños)
                if v_usd < 500: 
                    cursor.execute(
                        "UPDATE sys_cashflows SET valor_usd = %s WHERE id_cashflow = %s",
                        (v_usd, item['id_cashflow'])
                    )
        db.commit()
    except Exception as e:
        print(f"Error normalizando cashflows: {e}")
    finally:
        cursor.close()

# ==========================================================
# 🚀 EJECUCIÓN PRINCIPAL CON LOCK
# ==========================================================
def ejecutar_motor_financiero(db):
    cursor = db.cursor(dictionary=True)

    if not obtener_lock(cursor, "LOCK_FINANCIERO"):
        print(f"\n⛔ [SKIP] LOCK_FINANCIERO activo en otro entorno. Abortando ciclo.")
        return

    db.commit()

    print(f"\n{'='*60}")
    print(f"💎 MOTOR FINANCIERO v1.3.5 - AUDITORÍA Y DIVIDENDOS")
    print(f"{'='*60}")
    
    try:
        sql = "SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1"
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
                binance_dividends(db, u['user_id'], k, s)
                binance_mining(db, u['user_id'], k, s)
                binance_deposits(db, u['user_id'], k, s)
                binance_withdraw(db, u['user_id'], k, s)
                binance_convert_history(db, u['user_id'], k, s)
                binance_dust_log(db, u['user_id'], k, s)

            elif broker == "BINGX":
                bingx_income(db, u['user_id'], k, s)
                bingx_deposits(db, u['user_id'], k, s)
                bingx_withdraw(db, u['user_id'], k, s)

            # 2. 🧹 NORMALIZACIÓN (Aquí es el lugar correcto)
            # Una vez que terminó de bajar todo de los brokers, limpiamos lo pendiente en USD
            print(f"    >>> 🧹 SWEEPER DE CASHFLOWS (USER: {u['user_id']}) <<<")
            normalizar_cashflows_pendientes(db, u['user_id'])   

            db.commit()
            print(f"    [v] Cambios guardados para User {u['user_id']}.")

    except Exception as e:
        print(f"\n[CRITICAL] Error en ejecución: {e}")
    
    finally:
        print("\n🔓 [SISTEMA] Liberando LOCK_FINANCIERO...")
        liberar_lock(cursor, "LOCK_FINANCIERO")
        db.commit()
        cursor.close()

    print(f"\n{'='*60}")
    print(f"🏁 CICLO FINALIZADO")
    print(f"{'='*60}\n")

# ==========================================================
# 🧠 CONTROL DE HORARIOS (PC vs GITHUB)
# ==========================================================
def motor_debe_ejecutar():
    is_github = os.getenv('GITHUB_ACTIONS') == 'true'
    # Forma moderna: obtiene hora actual con zona horaria UTC explícita
    hora_utc = datetime.now(timezone.utc).hour

    if not is_github:
        if hora_utc in [3, 4, 5]:
            print("[SKIP] Mi PC cede el turno a GitHub en este horario.")
            return False
        return True

    return True

# ==========================================================
# 🚀 EJECUCIÓN PRINCIPAL DUAL
# ==========================================================
if __name__ == "__main__":
    is_github = os.getenv('GITHUB_ACTIONS') == 'true'

    if is_github:
        db = None
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            ejecutar_motor_financiero(db)
        finally:
            if db and db.is_connected():
                db.close()
    else:
        while True:
            if not motor_debe_ejecutar():
                print("[SKIP] GitHub fuera de horario o modo local desactivado. Reintentando en 10 min...")
                time.sleep(600)
                continue

            db = None
            try:
                db = mysql.connector.connect(**config.DB_CONFIG)
                ejecutar_motor_financiero(db)
            except Exception as e:
                print(f"\n[ERROR EN EL CICLO] {e}")
            finally:
                if db and db.is_connected():
                    db.close()
            
            print("⏳ Esperando 1 hora...")
            time.sleep(3600)