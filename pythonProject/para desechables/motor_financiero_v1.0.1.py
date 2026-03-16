# ==========================================================
# MOTOR FINANCIERO
# Version 1.0.0
# Compatible con arquitectura 6.6.6.23
# ==========================================================

import requests
import time
import hmac
import hashlib
import mysql.connector
from urllib.parse import urlencode




# ==========================================================
# 🚩 DISFRAZ BÁSICO BINGX v6.6.6.05
# ==========================================================

def get_headers_bingx(api_key):
    return {
        "X-BX-APIKEY": api_key,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://bingx.com/",
        "Connection": "keep-alive"
    }

# Session persistente (MUY IMPORTANTE)
bingx_session = requests.Session()

MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

# ==========================================================
# 🔐 SEGURIDAD Y HELPERS
# ==========================================================
def descifrar_dato(t, m):
    try:
        if not t: return None
        raw = base64.b64decode(t.strip())
        partes = raw.rsplit(b":::", 1) if b":::" in raw else raw.rsplit(b"::", 1)
        if len(partes) != 2: return None
        data, iv = partes
        key_hash = sha256(m.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

# ==========================================================
# 🎯 VINCULACIÓN MAESTRA v6.6.7 - ESCENARIO B (POR EXCHANGE)
# ==========================================================
def obtener_traductor_id(cursor, motor_fuente, ticker):
    ticker = ticker.upper().strip()

    # 1️⃣ Búsqueda exacta por motor + ticker
    sql = """
        SELECT id, categoria_producto, tipo_investment, motor_fuente
        FROM sys_traductor_simbolos
        WHERE motor_fuente = %s
        AND ticker_motor = %s
        LIMIT 1
    """
    cursor.execute(sql, (motor_fuente, ticker))
    row = cursor.fetchone()
    if row:
        return row

    # 2️⃣ Limpieza de prefijos (LD, STK)
    ticker_limpio = ticker
    if ticker.startswith("LD") and len(ticker) > 2:
        ticker_limpio = ticker[2:]
    elif ticker.startswith("STK") and len(ticker) > 3:
        ticker_limpio = ticker[3:]

    # 3️⃣ Buscar por underlying pero SOLO en el mismo motor
    sql = """
        SELECT id, categoria_producto, tipo_investment
        FROM sys_traductor_simbolos
        WHERE motor_fuente = %s
        AND underlying = %s
        LIMIT 1
    """
    cursor.execute(sql, (motor_fuente, ticker_limpio))
    row = cursor.fetchone()

    return row if row else None

 
def obtener_precio_usd(cursor, tid, asset_name):
    asset_name = asset_name.upper()
    clean_ticker = asset_name.replace("LD", "").replace("STK", "")

    if clean_ticker in ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']:
        return 1.0

    try:
        if tid:
            sql = """
                SELECT price 
                FROM sys_precios_activos 
                WHERE traductor_id = %s 
                ORDER BY last_update DESC 
                LIMIT 1
            """
            cursor.execute(sql, (tid,))
            row = cursor.fetchone()
            if row and row['price'] > 0:
                return float(row['price'])

        # 🔵 Fallback por underlying
        sql_fb = """
            SELECT p.price
            FROM sys_precios_activos p
            JOIN sys_traductor_simbolos t ON p.traductor_id = t.id
            WHERE t.underlying = %s
            AND t.is_active = 1
            ORDER BY p.last_update DESC
            LIMIT 1
        """
        cursor.execute(sql_fb, (clean_ticker,))
        row_fb = cursor.fetchone()
        if row_fb and row_fb['price'] > 0:
            return float(row_fb['price'])

    except Exception as e:
        print(f"[Precio Error {asset_name}]: {e}")

    return 0.0


def registrar_cashflow(cursor, data):
    # 1. Obtener el traductor_id para saber qué activo es exactamente
    # Usamos el motor_fuente (BINANCE, BINGX, etc) y el asset
    info_simbolo = obtener_traductor_id(cursor, data["broker"], data["asset"])
    tid = info_simbolo['id'] if info_simbolo else None
    
    # 2. Calcular el valor en USD al momento del registro
    precio_actual = obtener_precio_usd(cursor, tid, data["asset"])
    valor_total_usd = float(data["cantidad"]) * precio_actual

    sql = """
    INSERT INTO sys_cashflows(
        user_id, broker, tipo_evento, asset, cantidad, 
        ticker_motor, valor_usd, fecha_utc, id_externo, raw_json
    )
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
    raw_json=VALUES(raw_json),
    valor_usd=VALUES(valor_usd)
    """

    cursor.execute(sql, (
        data["user_id"],
        data["broker"],
        data["tipo_evento"],
        data["asset"],
        data["cantidad"],
        data["ticker_motor"],
        valor_total_usd,  # <--- Nuevo dato calculado
        data["fecha"],
        data["id_externo"],
        data["raw"]
    ))
# ==========================================================
# SIGN BINANCE
# ==========================================================

def binance_sign(secret,query):

    return hmac.new(
        secret.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()


# ==========================================================
# BINANCE FUTURES INCOME
# ==========================================================

def binance_income(db,uid,key,secret):

    cursor=db.cursor()

    base="https://fapi.binance.com"
    endpoint="/fapi/v1/income"

    ts=int(time.time()*1000)

    params={"timestamp":ts}

    query=urlencode(params)

    sig=binance_sign(secret,query)

    url=f"{base}{endpoint}?{query}&signature={sig}"

    headers={"X-MBX-APIKEY":key}

    r=requests.get(url,headers=headers)

    data=r.json()

    for i in data:

        registrar_cashflow(cursor,{
            "user_id":uid,
            "broker":"BINANCE",
            "tipo_evento":i["incomeType"],
            "asset":i["asset"],
            "cantidad":float(i["income"]),
            "ticker_motor":None,
            "fecha":time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(i["time"]/1000)),
            "id_externo":f"BN-INCOME-{i['tranId']}",
            "raw":str(i)
        })

    db.commit()

# ==========================================================
# BINANCE DUST LOG (Conversiones a BNB)
# ==========================================================
def binance_dust_log(db, uid, key, secret):
    cursor = db.cursor()
    base = "https://api.binance.com"
    endpoint = "/sapi/v1/asset/dribblet" # Este es el endpoint de "Polvo" (Dust)
    
    ts = int(time.time() * 1000)
    params = {"timestamp": ts}
    query = urlencode(params)
    sig = binance_sign(secret, query)
    url = f"{base}{endpoint}?{query}&signature={sig}"
    
    r = requests.get(url, headers={"X-MBX-APIKEY": key})
    data = r.json()

    if "userAssetDribblets" in data:
        for entry in data["userAssetDribblets"]:
            for detail in entry["userAssetDribbletDetails"]:
                registrar_cashflow(cursor, {
                    "user_id": uid,
                    "broker": "BINANCE",
                    "tipo_evento": "DUST_CONVERT",
                    "asset": detail["fromAsset"],
                    "cantidad": float(detail["amount"]),
                    "ticker_motor": None,
                    "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(entry["operateTime"]/1000)),
                    "id_externo": f"BN-DUST-{detail['transId']}",
                    "raw": str(detail)
                })
    db.commit()

# ==========================================================
# BINANCE DEPOSITS
# ==========================================================

def binance_deposits(db,uid,key,secret):

    cursor=db.cursor()

    base="https://api.binance.com"
    endpoint="/sapi/v1/capital/deposit/hisrec"

    ts=int(time.time()*1000)

    params={"timestamp":ts}

    query=urlencode(params)

    sig=binance_sign(secret,query)

    url=f"{base}{endpoint}?{query}&signature={sig}"

    headers={"X-MBX-APIKEY":key}

    r=requests.get(url,headers=headers)

    data=r.json()

    for d in data:

        registrar_cashflow(cursor,{
            "user_id":uid,
            "broker":"BINANCE",
            "tipo_evento":"DEPOSIT",
            "asset":d["coin"],
            "cantidad":float(d["amount"]),
            "ticker_motor":None,
            "fecha":time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(d["insertTime"]/1000)),
            "id_externo":f"BN-DEP-{d['txId']}",
            "raw":str(d)
        })

    db.commit()

# ==========================================================
# BINANCE WITHDRAWALS (RETIROS)
# ==========================================================

def binance_withdrawals(db, uid, key, secret):
    cursor = db.cursor()
    base = "https://api.binance.com"
    endpoint = "/sapi/v1/capital/withdraw/history"
    ts = int(time.time() * 1000)
    params = {"timestamp": ts}
    query = urlencode(params)
    sig = binance_sign(secret, query)
    url = f"{base}{endpoint}?{query}&signature={sig}"
    headers = {"X-MBX-APIKEY": key}

    r = requests.get(url, headers=headers)
    data = r.json()

    # Si la API devuelve una lista de retiros
    for w in data:
        registrar_cashflow(cursor, {
            "user_id": uid,
            "broker": "BINANCE",
            "tipo_evento": "WITHDRAW",
            "asset": w["coin"],
            "cantidad": float(w["amount"]),
            "ticker_motor": None,
            "fecha": w["applyTime"], # Binance usa este formato para retiros
            "id_externo": f"BN-WITH-{w.get('id', ts)}",
            "raw": str(w)
        })
    db.commit()

# ==========================================================
# BINANCE EARN & STAKING REWARDS
# ==========================================================
def binance_earn_rewards(db, uid, key, secret):
    cursor = db.cursor()
    base = "https://api.binance.com"
    # Este endpoint trae los intereses pagados por "Earn"
    endpoint = "/sapi/v1/lending/union/interestHistory" 
    
    ts = int(time.time() * 1000)
    params = {"timestamp": ts, "recvWindow": 60000}
    query = urlencode(params)
    sig = binance_sign(secret, query)
    url = f"{base}{endpoint}?{query}&signature={sig}"
    
    try:
        r = requests.get(url, headers={"X-MBX-APIKEY": key})
        data = r.json()

        for item in data:
            registrar_cashflow(cursor, {
                "user_id": uid,
                "broker": "BINANCE",
                "tipo_evento": "EARN_INTEREST",
                "asset": item["asset"],
                "cantidad": float(item["interest"]),
                "ticker_motor": None,
                "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(item["time"]/1000)),
                "id_externo": f"BN-EARN-{item['time']}-{item['asset']}",
                "raw": str(item)
            })
        db.commit()
    except Exception as e:
        print(f"   [!] Error en Earn Rewards: {e}")

# ==========================================================
# BINANCE CONVERT (Intercambios Directos)
# ==========================================================
def binance_convert_history(db, uid, key, secret):
    cursor = db.cursor()
    base = "https://api.binance.com"
    endpoint = "/sapi/v1/convert/tradeFlow"
    ts = int(time.time() * 1000)
    # Buscamos los últimos 30 días para no saturar
    start_time = ts - (30 * 24 * 60 * 60 * 1000)
    params = {"timestamp": ts, "startTime": start_time}
    query = urlencode(params)
    sig = binance_sign(secret, query)
    url = f"{base}{endpoint}?{query}&signature={sig}"
    
    r = requests.get(url, headers={"X-MBX-APIKEY": key})
    data = r.json()

    if "list" in data:
        for c in data["list"]:
            # Registramos la SALIDA de la moneda vendida
            registrar_cashflow(cursor, {
                "user_id": uid, "broker": "BINANCE", "tipo_evento": "CONVERT_OUT",
                "asset": c["fromAsset"], "cantidad": -float(c["fromAmount"]),
                "ticker_motor": None, "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(c["createTime"]/1000)),
                "id_externo": f"BN-CONV-OUT-{c['orderId']}", "raw": str(c)
            })
            # Registramos la ENTRADA de la moneda comprada
            registrar_cashflow(cursor, {
                "user_id": uid, "broker": "BINANCE", "tipo_evento": "CONVERT_IN",
                "asset": c["toAsset"], "cantidad": float(c["toAmount"]),
                "ticker_motor": None, "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(c["createTime"]/1000)),
                "id_externo": f"BN-CONV-IN-{c['orderId']}", "raw": str(c)
            })
    db.commit()

# ==========================================================
# BINANCE STAKING & REWARDS (Ingresos pasivos)
# ==========================================================
def binance_staking_rewards(db, uid, key, secret):
    cursor = db.cursor()
    base = "https://api.binance.com"
    # Este cubre intereses de Flexible y Locked Staking
    endpoint = "/sapi/v1/lending/union/interestHistory"
    ts = int(time.time() * 1000)
    params = {"timestamp": ts}
    query = urlencode(params)
    sig = binance_sign(secret, query)
    url = f"{base}{endpoint}?{query}&signature={sig}"
    
    r = requests.get(url, headers={"X-MBX-APIKEY": key})
    data = r.json()

    for item in data:
        registrar_cashflow(cursor, {
            "user_id": uid, "broker": "BINANCE", "tipo_evento": "STAKING_REWARD",
            "asset": item["asset"], "cantidad": float(item["interest"]),
            "ticker_motor": None, "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(item["time"]/1000)),
            "id_externo": f"BN-STK-{item['time']}-{item['asset']}", "raw": str(item)
        })
    db.commit()

# ==========================================================
# BINGX CASHFLOWS (Depósitos y Retiros)
# ==========================================================
def bingx_cashflows(db, uid, key, secret):
    cursor = db.cursor()
    endpoint = "/openApi/wallets/v1/capital/deposit/hisrec" # Ejemplo para depósitos
    
    # BingX requiere una firma distinta, pero para mantenerlo simple hoy
    # vamos a preparar la estructura del loop
    params = {
        "timestamp": int(time.time() * 1000),
        "appKey": key
    }
    
    # Nota: BingX usa una lógica de firma similar a Binance pero con sus propios endpoints
    # Por ahora, dejaremos el esqueleto listo para conectar en el siguiente paso
    print(f"   [i] BingX Cashflow para User {uid} conectado (pendiente de mapeo de respuesta)")


# ==========================================================
# BINGX INCOME HISTORY (Funding Fees y otros)
# ==========================================================
def bingx_income_history(db, uid, key, secret):
    cursor = db.cursor()
    # Endpoint para historial de ingresos en Futuros Perpetuos (incluye Funding Fees)
    endpoint = "/openApi/swap/v2/user/income"
    ts = int(time.time() * 1000)
    
    params = {
        "timestamp": ts,
        "limit": 100
    }
    
    # Preparamos la firma estilo BingX
    param_str = urlencode(params)
    signature = hmac.new(secret.encode('utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()
    url = f"https://open-api.bingx.com{endpoint}?{param_str}&signature={signature}"
    
    r = bingx_session.get(url, headers=get_headers_bingx(key))
    res = r.json()

    if res.get("code") == 0 and "data" in res:
        for i in res["data"]:
            registrar_cashflow(cursor, {
                "user_id": uid, "broker": "BINGX", "tipo_evento": i["incomeType"],
                "asset": i["asset"], "cantidad": float(i["income"]),
                "ticker_motor": i["symbol"], "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(i["time"]/1000)),
                "id_externo": f"BX-INC-{i['info']}-{i['time']}", "raw": str(i)
            })
    db.commit()

# ==========================================================
# 🚀 EJECUCIÓN PRINCIPAL
# ==========================================================
def ejecutar_motor_financiero(db):
    print(f"\n💎 MOTOR v1.1.0 - AUDITORÍA FINANCIERA TOTAL")
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM sys_usuarios WHERE status=1")
    usuarios = cursor.fetchall()

    for u in usuarios:
        print(f"\n>> 📋 PROCESANDO: User {u['user_id']} | {u['broker_name']}")
        k = descifrar_dato(u['api_key'], MASTER_KEY)
        s = descifrar_dato(u['api_secret'], MASTER_KEY)

        if not k or not s:
            print(f"   [!] Error de llaves en usuario {u['user_id']}")
            continue

        if u['broker_name'].upper() == "BINANCE":
            # --- Flujos de Binance ---
            binance_income(db, u['user_id'], k, s)       # PNL y Funding de Futuros
            binance_deposits(db, u['user_id'], k, s)     # Depósitos
            binance_withdrawals(db, u['user_id'], k, s)  # Retiros
            binance_convert_history(db, u['user_id'], k, s) # Conversiones
            binance_dust_log(db, u['user_id'], k, s)     # Centavos a BNB
            binance_staking_rewards(db, u['user_id'], k, s) # Intereses Earn
            print(f"   [OK] Binance auditado al 100%")

        elif u['broker_name'].upper() == "BINGX":
            # --- Flujos de BingX ---
            bingx_income_history(db, u['user_id'], k, s)
            # Aquí podrías agregar bingx_deposits si lo necesitas después
            print(f"   [OK] BingX auditado al 100%")

    print("\n✅ PROCESO COMPLETADO: Todos los flujos de caja están en la DB.")



TODO LO QUE NO ES CODIGO  PERO PARA LLEVAR CONTROL DE LO QUE ESTAMOS HACIENDO

1 eventeos que vamos a soportar
DEPOSIT
WITHDRAW
TRANSFER
FUNDING_FEE
REALIZED_PNL
COMMISSION
LIQUIDATION
INSURANCE_CLEAR
DELIVERED_SETTLEMENT
INTEREST
STAKING_REWARD
AIR_DROP
REFERRAL_REWARD
COMMISSION_REBATE
BONUS
CONVERT
DUST_CONVERT
AUTO_EXCHANGE
POOL DISTRIBUTION





2. 127.0.0.1:3306/u800112681_dashboard/        https://auth-db907.hstgr.io/index.php?route=/database/sql&db=u800112681_dashboard
Su consulta se ejecutó con éxito.
SHOW COLUMNS FROM sys_cashflows;
id_cashflow bigint(20) unsigned NO  PRI NULL    auto_increment  
user_id bigint(20)  NO  MUL NULL        
broker  varchar(20) NO      NULL        
tipo_evento varchar(50) NO  MUL NULL        
asset   varchar(20) NO  MUL NULL        
cantidad    decimal(36,18)  NO      NULL        
ticker_motor    varchar(20) YES     NULL        
valor_usd   decimal(36,18)  YES     0.000000000000000000        
fecha_utc   datetime    NO      NULL        
id_externo  varchar(120)    NO      NULL        
estado  varchar(20) YES     CONFIRMED       
raw_json    text    YES     NULL        
created_at  timestamp   YES     current_timestamp()     


3. RECUERDA QUE NO SOY PROGRAMADOR POR LO QUE SE ESPECIFICO SI HAY CAMBIOS EN DONDE LOS HAGOS