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
    """
    Busca el precio más reciente. 
    Añade un fallback para buscar por nombre si el TID falla.
    """
    asset_upper = asset_name.upper().strip().replace('"', '')
    
    # 1. 🛡️ BYPASS PARA STABLECOINS (Seguridad absoluta)
    if asset_upper in ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD', 'FDUSD']: 
        return 1.0
    
    try:
        # Extraer el ID limpio del traductor
        tid_val = tid['id'] if isinstance(tid, dict) else tid
        if isinstance(tid_val, (list, tuple)): tid_val = tid_val[0]

        # 2. 🔍 INTENTO A: Por Traductor ID (Lo más preciso)
        if tid_val:
            sql = "SELECT price FROM sys_precios_activos WHERE traductor_id = %s ORDER BY last_update DESC LIMIT 1"
            cursor.execute(sql, (tid_val,))
            row = cursor.fetchone()
            if row:
                price = row['price'] if isinstance(row, dict) else row[0]
                if price and float(price) > 0:
                    return float(price)

        # 3. 🔍 INTENTO B: Por Nombre de Activo (Fallback para Cashflows)
        # Útil si el cashflow es de un activo que no tradeas pero del que tenemos precio 
        # guardado de otro usuario o de una actualización general.
        sql_name = """
            SELECT p.price 
            FROM sys_precios_activos p
            JOIN sys_traductor_simbolos t ON p.traductor_id = t.id
            WHERE t.underlying = %s 
            ORDER BY p.last_update DESC LIMIT 1
        """
        cursor.execute(sql_name, (asset_upper,))
        row_n = cursor.fetchone()
        if row_n:
            price = row_n['price'] if isinstance(row_n, dict) else row_n[0]
            if price and float(price) > 0:
                return float(price)

    except Exception as e:
        print(f"      [!] Error sutil en obtener_precio_usd ({asset_upper}): {e}")
    
    # 4. 🏳️ RENDICIÓN: Si no hay precio, devolvemos 0. 
    # El Sweeper lo ignorará o lo dejará en 0 según lo que hablamos.
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

def registrar_cashflow(cursor, data):
    ticker_ref = data.get("ticker_motor") or data["asset"]
    tid = obtener_traductor_id(cursor, data["broker"], ticker_ref)  
    
    # 1. Obtener el precio de mercado por defecto
    precio = obtener_precio_usd(cursor, tid, data["asset"])
    
    # --- LÓGICA INTELIGENTE PARA DUST (USDC / USDT / BNB) ---
    raw_str = str(data.get("raw", "{}"))
    es_dust = "DUST" in str(data.get("tipo_evento", "")) or "BN-DUST" in str(data.get("id_externo", ""))

    
    if es_dust:

        # SOLO para Dust IN hacia stablecoin
        if data["tipo_evento"] == "DUST_IN":

            if (
                '"targetAsset": "USDC"' in raw_str or
                '"targetAsset": "USDT"' in raw_str or
                '"targetAsset": "FDUSD"' in raw_str or
                '"targetAsset": "BUSD"' in raw_str
            ):
                precio = 1.0            
    # -------------------------------------------------------

    valor_usd = float(data["cantidad"]) * precio

    traductor_id_final = None
    if tid:
        traductor_id_final = tid['id'] if isinstance(tid, dict) else tid

    sql = """
    INSERT INTO sys_cashflows (user_id, broker, tipo_evento, asset, cantidad, ticker_motor, valor_usd, fecha_utc, id_externo, raw_json, traductor_id)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE 
        asset=VALUES(asset),
        valor_usd=VALUES(valor_usd),
        raw_json=VALUES(raw_json)
    """
    cursor.execute(sql, (data["user_id"], data["broker"], data["tipo_evento"], data["asset"], 
                         data["cantidad"], data["ticker_motor"], valor_usd, data["fecha"], 
                         data["id_externo"], data.get("raw", "{}"), traductor_id_final))
    
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
                        "id_externo": f"{user_id}-BN-DIV-{tran_id}",
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

    nuevo_sync = max(max_ts_global, chunk_end)

    guardar_sync(
        cursor,
        user_id,
        "BINANCE",
        endpoint,
        nuevo_sync
    )

    print(f"    [OK] {endpoint}: {count_total} procesados.")


# ==========================================================
# 🔌 BINANCE INCOME (FUTURES CASHFLOW)
# ==========================================================
def binance_income(db, user_id, key, secret):

    cursor = db.cursor(dictionary=True)

    endpoint = "BINANCE_INCOME"

    last_sync = obtener_sync(
        cursor,
        user_id,
        "BINANCE",
        endpoint
    )

    # OCTUBRE 2022
    start_ts = (
        last_sync + 1
        if last_sync > 0
        else 1664582400000
    )

    end_now = int(time.time() * 1000)

    print(f"    [+] {endpoint}: Reconstruyendo histórico Futures...")

    count = 0
    max_ts_global = last_sync

    while start_ts < end_now:

        # bloques 30 días
        chunk_end = start_ts + (30 * 24 * 60 * 60 * 1000)

        if chunk_end > end_now:
            chunk_end = end_now

        fecha_h = time.strftime(
            '%Y-%m-%d',
            time.gmtime(start_ts / 1000)
        )

        print(
            f"        [..] Income: Escaneando desde {fecha_h}...",
            end="\r"
        )

        params = {
            "startTime": start_ts,
            "endTime": chunk_end,
            "limit": 1000,
            "timestamp": int(time.time() * 1000)
        }

        query = urlencode(params)

        url = (
            f"https://fapi.binance.com/fapi/v1/income?"
            f"{query}&signature={binance_sign(secret, query)}"
        )

        try:

            r = requests.get(
                url,
                headers={"X-MBX-APIKEY": key},
                timeout=90
            ).json()

            if isinstance(r, list):

                for i in r:

                    ts = int(i["time"])


                    income_type = i["incomeType"]

                    # ==========================================================
                    # NORMALIZACIÓN FINANCIERA (MISMA LÓGICA BINGX)
                    # ==========================================================

                    if income_type == "REALIZED_PNL":
                        categoria = "TRADE"

                    elif income_type in ["COMMISSION", "TRADING_FEE"]:
                        categoria = "FEE"

                    elif income_type == "FUNDING_FEE":
                        categoria = "FUNDING"

                    elif income_type in [
                        "TRANSFER",
                        "INTERNAL_TRANSFER"
                    ]:
                        categoria = "TRANSFER"

                    else:
                        categoria = "OTHER"

                    registrar_cashflow(cursor, {
                        "user_id": user_id,
                        "broker": "BINANCE",
                        "tipo_evento": categoria,
                        "asset": i["asset"],
                        "cantidad": float(i["income"]),
                        "ticker_motor": i.get("symbol"),
                        "fecha": time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.gmtime(ts / 1000)
                        ),
                        "id_externo": (
                            f"{user_id}-BN-INC-"
                            f"{i['tranId']}"
                        ),
                        "raw": json.dumps(i)
                    })

                    count += 1

                    if ts > max_ts_global:
                        max_ts_global = ts

            start_ts = chunk_end + 1

            nuevo_sync = max(max_ts_global, chunk_end)

            guardar_sync(
                cursor,
                user_id,
                "BINANCE",
                endpoint,
                nuevo_sync
            )

            db.commit()

            rate_limit()

        except Exception as e:

            print(f"\n    [!] Error en bloque Binance Income: {e}")

            break

    print(
        f"\n    [OK] {endpoint}: "
        f"{count} registros históricos procesados."
    )

# ==========================================================
# SECCIÓN MODIFICADA: DEPÓSITOS Y RETIROS (FILTRO BINANCE PAY)
# ==========================================================
def binance_deposits(db, user_id, key, secret):
    cursor = db.cursor(dictionary=True)

    endpoint = "BINANCE_DEPOSIT"
    last_sync = obtener_sync(cursor, user_id, "BINANCE", endpoint)

    start_ts = last_sync + 1 if last_sync > 0 else 1601510400000
    end_now = int(time.time() * 1000)

    print(f"    [+] {endpoint}: Reconstruyendo histórico...")

    count = 0
    max_ts_global = last_sync

    while start_ts < end_now:

        chunk_end = start_ts + (90 * 24 * 60 * 60 * 1000)

        if chunk_end > end_now:
            chunk_end = end_now

        fecha_h = time.strftime('%Y-%m-%d', time.gmtime(start_ts/1000))
        print(f"        [..] Deposit: Escaneando desde {fecha_h}...", end="\r")

        params = {
            "startTime": start_ts,
            "endTime": chunk_end,
            "timestamp": int(time.time()*1000)
        }

        query = urlencode(params)

        url = f"https://api.binance.com/sapi/v1/capital/deposit/hisrec?{query}&signature={binance_sign(secret, query)}"

        r = requests.get(
            url,
            headers={"X-MBX-APIKEY": key}
        ).json()

        if isinstance(r, list):

            for d in r:

                ts = int(d["insertTime"])

                raw_str = json.dumps(d)

                if "Received from" in raw_str or "Paid to" in raw_str:
                    evento_asignado = "TRANSFER_IN"
                else:
                    evento_asignado = "DEPOSIT"

                registrar_cashflow(cursor, {
                    "user_id": user_id,
                    "broker": "BINANCE",
                    "tipo_evento": evento_asignado,
                    "asset": d["coin"],
                    "cantidad": float(d["amount"]),
                    "ticker_motor": None,
                    "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts/1000)),
                    "id_externo": f"{user_id}-BN-DEP-{d['txId']}",
                    "raw": raw_str
                })

                count += 1

                if ts > max_ts_global:
                    max_ts_global = ts

        start_ts = chunk_end + 1

        nuevo_sync = max(max_ts_global, chunk_end)

        guardar_sync(
            cursor,
            user_id,
            "BINANCE",
            endpoint,
            nuevo_sync
        )


        db.commit()

        rate_limit()

    print(f"\n    [OK] {endpoint}: {count} registros históricos procesados.")

def binance_withdraw(db, user_id, key, secret):

    cursor = db.cursor(dictionary=True)

    endpoint = "BINANCE_WITHDRAW"

    last_sync = obtener_sync(cursor, user_id, "BINANCE", endpoint)

    start_ts = last_sync + 1 if last_sync > 0 else 1601510400000
    end_now = int(time.time() * 1000)

    print(f"    [+] {endpoint}: Reconstruyendo histórico...")

    count = 0
    max_ts_global = last_sync

    while start_ts < end_now:

        chunk_end = start_ts + (90 * 24 * 60 * 60 * 1000)

        if chunk_end > end_now:
            chunk_end = end_now

        fecha_h = time.strftime('%Y-%m-%d', time.gmtime(start_ts/1000))

        print(f"        [..] Withdraw: Escaneando desde {fecha_h}...", end="\r")

        params = {
            "startTime": start_ts,
            "endTime": chunk_end,
            "timestamp": int(time.time()*1000)
        }

        query = urlencode(params)

        url = f"https://api.binance.com/sapi/v1/capital/withdraw/history?{query}&signature={binance_sign(secret, query)}"

        r = requests.get(
            url,
            headers={"X-MBX-APIKEY": key}
        ).json()

        if isinstance(r, list):

            for w in r:
                ts = int(datetime.strptime(
                    w["applyTime"],
                    "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=timezone.utc).timestamp() * 1000)

                raw_str = json.dumps(w)

                if "Received from" in raw_str or "Paid to" in raw_str:
                    evento_asignado = "TRANSFER_OUT"
                else:
                    evento_asignado = "WITHDRAW"

                registrar_cashflow(cursor, {
                    "user_id": user_id,
                    "broker": "BINANCE",
                    "tipo_evento": evento_asignado,
                    "asset": w["coin"],
                    "cantidad": -float(w["amount"]),
                    "ticker_motor": None,
                    "fecha": w["applyTime"],
                    "id_externo": f"{user_id}-BN-WITH-{w['id']}",
                    "raw": raw_str
                })

                count += 1
                if ts > max_ts_global:
                    max_ts_global = ts

        start_ts = chunk_end + 1

        nuevo_sync = max(max_ts_global, chunk_end)

        guardar_sync(
            cursor,
            user_id,
            "BINANCE",
            endpoint,
            nuevo_sync
        )

        db.commit()

        rate_limit()

    print(f"\n    [OK] {endpoint}: {count} registros históricos procesados.")

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
                                "id_externo": f"{uid}-BN-DUST-{detail['transId']}-{asset_s}", 
                                "raw": json.dumps(detail)
                            })

                    # ==========================================================
                    # DUST_IN (Asset recibido REAL)
                    # ==========================================================

                    target_asset = None

                    if (
                        "userAssetDribbletDetails" in entry
                        and len(entry["userAssetDribbletDetails"]) > 0
                    ):
                        target_asset = entry["userAssetDribbletDetails"][0].get("targetAsset")

                    # Fallback defensivo
                    if not target_asset:
                        target_asset = entry.get("operatingAsset", "BNB")

                    registrar_cashflow(cursor, {
                        "user_id": uid,
                        "broker": "BINANCE",
                        "tipo_evento": "DUST_IN",
                        "asset": target_asset,
                        "cantidad": float(entry["totalTransferedAmount"]),
                        "ticker_motor": None,
                        "fecha": fecha,
                        "id_externo": f"{uid}-BN-DUST-{ts}-{target_asset}-IN",
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
    
    start_ts = last_sync + 1 if last_sync > 0 else 1601510400000
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
                        "fecha": fecha, "id_externo": f"{uid}-BN-CONV-{c['orderId']}-OUT", "raw": json.dumps(c)
                    })
                    registrar_cashflow(cursor, {
                        "user_id": uid, "broker": "BINANCE", "tipo_evento": "CONVERT_IN",
                        "asset": c["toAsset"], "cantidad": float(c["toAmount"]), "ticker_motor": None,
                        "fecha": fecha, "id_externo": f"{uid}-BN-CONV-{c['orderId']}-IN", "raw": json.dumps(c)
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

    params = {
        "timestamp": int(time.time()*1000)
    }

    query = urlencode(params)

    url = f"https://api.binance.com/sapi/v1/asset/transfer?{query}&signature={binance_sign(secret, query)}"

    r = requests.get(
        url,
        headers={"X-MBX-APIKEY": key}
    ).json()

    if "rows" not in r:
        print(f"    [OK] {endpoint}: 0 registros.")
        return

    for t in r["rows"]:

        registrar_cashflow(cursor, {
            "user_id": user_id,
            "broker": "BINANCE",
            "tipo_evento": "TRANSFER",
            "asset": t["asset"],
            "cantidad": float(t["amount"]),
            "ticker_motor": None,
            "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(t["timestamp"]/1000)),
            "id_externo": f"{user_id}-BN-TR-{t['tranId']}",
            "raw": json.dumps(t)
        })

    db.commit()

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
                            "id_externo": f"{user_id}-BN-MINE-{account}-{ts}-{p['coinName']}",
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
# 🛡️ SAFE TIMESTAMP PARSER
# ==========================================================
def safe_timestamp(value):

    if value is None:
        return 0

    try:

        # timestamp numérico
        if isinstance(value, (int, float)):
            return int(value)

        value = str(value).strip()

        if value == "":
            return 0

        # timestamp string numérico
        if value.isdigit():
            return int(value)

        # formato fecha SQL
        dt = datetime.strptime(
            value,
            "%Y-%m-%d %H:%M:%S"
        )

        return int(
            dt.replace(
                tzinfo=timezone.utc
            ).timestamp() * 1000
        )

    except:
        return 0


# ==========================================================
# 🔌 BINGX FUNCTIONS
# ==========================================================

# ==========================================================
# 🔌 BINGX INCOME (VERSIÓN FINAL ESTABLE)
# ==========================================================
def bingx_income(db, user_id, key, secret):

    cursor = db.cursor(dictionary=True)

    endpoint = "BINGX_INCOME"

    last_sync = obtener_sync(
        cursor,
        user_id,
        "BINGX",
        endpoint
    )

    # OCTUBRE 2022
    start_ts = (
        last_sync + 1
        if last_sync > 0
        else 1664582400000
    )

    end_now = int(time.time() * 1000)

    print(f"    [+] {endpoint}: Reconstruyendo histórico Futures...")

    count = 0
    max_ts_global = last_sync

    while start_ts < end_now:

        # bloques de 30 días
        chunk_end = start_ts + (30 * 24 * 60 * 60 * 1000)

        if chunk_end > end_now:
            chunk_end = end_now

        fecha_h = time.strftime(
            '%Y-%m-%d',
            time.gmtime(start_ts / 1000)
        )

        print(
            f"        [..] Income: Escaneando desde {fecha_h}...",
            end="\r"
        )

        params = {
            "startTime": start_ts,
            "endTime": chunk_end,
            "limit": 1000,
            "timestamp": int(time.time() * 1000)
        }

        query = urlencode(params)

        sig = hmac.new(
            secret.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()

        url = (
            "https://open-api.bingx.com"
            "/openApi/swap/v2/user/income"
            f"?{query}&signature={sig}"
        )

        try:

            r = requests.get(
                url,
                headers=get_headers_bingx(key),
                timeout=90
            ).json()

            income_list = []

            # ==========================================================
            # NORMALIZACIÓN RESPUESTA API
            # ==========================================================

            if isinstance(r, dict):

                if r.get("code") == 0:

                    if isinstance(r.get("data"), dict):
                        income_list = r["data"].get("list", [])

                    elif isinstance(r.get("data"), list):
                        income_list = r["data"]

            elif isinstance(r, list):
                income_list = r

            # ==========================================================
            # PROCESAMIENTO
            # ==========================================================

            for i in income_list:

                ts = safe_timestamp(
                    i.get("time")
                )

                if ts <= 0:
                    continue

                if ts <= last_sync:
                    continue

                asset = (
                    i.get("asset")
                    or i.get("currency")
                    or "USDT"
                )

                symbol = (
                    i.get("symbol")
                    or asset
                )

                monto = float(
                    i.get("income", 0)
                )

                tipo_api = (
                    i.get("incomeType")
                    or i.get("type")
                    or "OTHER"
                )

                # ==========================================================
                # NORMALIZACIÓN FINANCIERA
                # ==========================================================

                if tipo_api == "REALIZED_PNL":
                    categoria = "TRADE"

                elif tipo_api in [
                    "TRADING_FEE",
                    "COMMISSION"
                ]:
                    categoria = "FEE"

                elif tipo_api == "FUNDING_FEE":
                    categoria = "FUNDING"

                elif tipo_api in [
                    "TRANSFER",
                    "INTERNAL_TRANSFER"
                ]:
                    categoria = "TRANSFER"

                else:
                    categoria = "OTHER"

                fecha_sql = time.strftime(
                    '%Y-%m-%d %H:%M:%S',
                    time.gmtime(ts / 1000)
                )

                tran_id = (
                    i.get("tranId")
                    or i.get("id")
                    or ts
                )

                id_ext = (
                    f"{user_id}-BX-INC-"
                    f"{tran_id}-{asset}-{tipo_api}"
                )

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

                count += 1

                if ts > max_ts_global:
                    max_ts_global = ts

            # avanzar bloque
            start_ts = chunk_end + 1

            # guardar progreso real
            guardar_sync(
                cursor,
                user_id,
                "BINGX",
                endpoint,
                max_ts_global
            )

            db.commit()

            rate_limit()

        except Exception as e:

            print(
                f"\n    [!] Error en bloque BingX Income: {e}"
            )

            break

    print(
        f"\n    [OK] {endpoint}: "
        f"{count} registros históricos procesados."
    )


# ==========================================================
# 🔌 BINGX DEPOSITS
# ==========================================================
def bingx_deposits(db, user_id, key, secret):

    cursor = db.cursor(dictionary=True)

    endpoint = "BINGX_DEPOSIT"

    last_sync = obtener_sync(
        cursor,
        user_id,
        "BINGX",
        endpoint
    )

    start_ts = (
        last_sync + 1
        if last_sync > 0
        else 1601510400000
    )

    end_now = int(time.time() * 1000)

    print(f"    [+] {endpoint}: Reconstruyendo histórico...")

    count = 0
    max_ts_global = last_sync

    while start_ts < end_now:

        chunk_end = start_ts + (30 * 24 * 60 * 60 * 1000)

        if chunk_end > end_now:
            chunk_end = end_now

        fecha_h = time.strftime(
            '%Y-%m-%d',
            time.gmtime(start_ts / 1000)
        )

        print(
            f"        [..] Deposit: Escaneando desde {fecha_h}...",
            end="\r"
        )

        params = {
            "startTime": start_ts,
            "endTime": chunk_end,
            "timestamp": int(time.time() * 1000)
        }

        query = urlencode(params)

        sig = hmac.new(
            secret.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()

        url = (
            "https://open-api.bingx.com"
            "/openApi/api/v3/capital/deposit/hisrec"
            f"?{query}&signature={sig}"
        )

        try:

            r = requests.get(
                url,
                headers=get_headers_bingx(key),
                timeout=90
            ).json()

            deposit_list = []

            if isinstance(r, dict):

                if "data" in r:

                    if isinstance(r["data"], dict):
                        deposit_list = r["data"].get("list", [])

                    elif isinstance(r["data"], list):
                        deposit_list = r["data"]

            elif isinstance(r, list):
                deposit_list = r

            for d in deposit_list:

                ts = safe_timestamp(
                    d.get("insertTime")
                )

                if ts <= 0:
                    continue

                fecha_sql = time.strftime(
                    '%Y-%m-%d %H:%M:%S',
                    time.gmtime(ts / 1000)
                )

                txid = d.get(
                    "txId",
                    f"NO-TX-{ts}"
                )

                registrar_cashflow(cursor, {
                    "user_id": user_id,
                    "broker": "BINGX",
                    "tipo_evento": "DEPOSIT",
                    "asset": d.get("coin"),
                    "cantidad": float(d.get("amount", 0)),
                    "ticker_motor": None,
                    "fecha": fecha_sql,
                    "id_externo": f"{user_id}-BX-DEP-{txid}",
                    "raw": json.dumps(d)
                })

                count += 1

                if ts > max_ts_global:
                    max_ts_global = ts

            start_ts = chunk_end + 1

            # ✅ SOLO avanzamos realmente procesado
            guardar_sync(
                cursor,
                user_id,
                "BINGX",
                endpoint,
                max_ts_global
            )

            db.commit()

            rate_limit()

        except Exception as e:

            print(
                f"\n    [!] Error en bloque BingX Deposit: {e}"
            )

            break

    print(
        f"\n    [OK] {endpoint}: "
        f"{count} registros históricos procesados."
    )
# ==========================================================
# 🔌 BINGX WITHDRAW
# ==========================================================
def bingx_withdraw(db, user_id, key, secret):

    cursor = db.cursor(dictionary=True)

    endpoint = "BINGX_WITHDRAW"

    last_sync = obtener_sync(
        cursor,
        user_id,
        "BINGX",
        endpoint
    )

    start_ts = (
        last_sync + 1
        if last_sync > 0
        else 1601510400000
    )

    end_now = int(time.time() * 1000)

    print(f"    [+] {endpoint}: Reconstruyendo histórico...")

    count = 0
    max_ts_global = last_sync

    while start_ts < end_now:

        chunk_end = start_ts + (30 * 24 * 60 * 60 * 1000)

        if chunk_end > end_now:
            chunk_end = end_now

        fecha_h = time.strftime(
            '%Y-%m-%d',
            time.gmtime(start_ts / 1000)
        )

        print(
            f"        [..] Withdraw: Escaneando desde {fecha_h}...",
            end="\r"
        )

        params = {
            "startTime": start_ts,
            "endTime": chunk_end,
            "timestamp": int(time.time() * 1000)
        }

        query = urlencode(params)

        sig = hmac.new(
            secret.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()

        url = (
            "https://open-api.bingx.com"
            "/openApi/api/v3/capital/withdraw/history"
            f"?{query}&signature={sig}"
        )

        try:

            r = requests.get(
                url,
                headers=get_headers_bingx(key),
                timeout=90
            ).json()

            withdraw_list = []

            if isinstance(r, dict):

                if "data" in r:

                    if isinstance(r["data"], dict):
                        withdraw_list = r["data"].get("list", [])

                    elif isinstance(r["data"], list):
                        withdraw_list = r["data"]

            elif isinstance(r, list):
                withdraw_list = r

            for w in withdraw_list:

                ts = safe_timestamp(
                    w.get("applyTime")
                )

                if ts <= 0:
                    continue

                fecha_sql = time.strftime(
                    '%Y-%m-%d %H:%M:%S',
                    time.gmtime(ts / 1000)
                )

                wid = w.get(
                    "id",
                    f"NOID-{ts}"
                )

                registrar_cashflow(cursor, {
                    "user_id": user_id,
                    "broker": "BINGX",
                    "tipo_evento": "WITHDRAW",
                    "asset": w.get("coin"),
                    "cantidad": -float(w.get("amount", 0)),
                    "ticker_motor": None,
                    "fecha": fecha_sql,
                    "id_externo": f"{user_id}-BX-WITH-{wid}",
                    "raw": json.dumps(w)
                })

                count += 1

                if ts > max_ts_global:
                    max_ts_global = ts

            start_ts = chunk_end + 1

            guardar_sync(
                cursor,
                user_id,
                "BINGX",
                endpoint,
                max_ts_global
            )

            db.commit()

            rate_limit()

        except Exception as e:

            print(
                f"\n    [!] Error en bloque BingX Withdraw: {e}"
            )

            break

    print(
        f"\n    [OK] {endpoint}: "
        f"{count} registros históricos procesados."
    )


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
                if abs(v_usd) < 1000000:
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
    print(f"💎 MOTOR FINANCIERO v1.3.8 - AUDITORÍA Y DIVIDENDOS")
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
                # binance_transfers(db, u['user_id'], k, s)

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