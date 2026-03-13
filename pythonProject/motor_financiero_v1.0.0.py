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

# --- LA FUNCIÓN QUE FALTABA ---
def disparar_radar(cursor, uid, ticker, ctx):
    sql = "INSERT IGNORE INTO sys_simbolos_buscados (user_id, ticker, status, info) VALUES (%s,%s,'pendiente',%s)"
    cursor.execute(sql, (uid, ticker, f"Detectado en {ctx}"))    

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


# ==========================================================
# REGISTRAR EVENTO
# ==========================================================

def registrar_cashflow(cursor,data):

    sql="""
    INSERT INTO sys_cashflows(
        user_id,
        broker,
        tipo_evento,
        asset,
        cantidad,
        ticker_motor,
        fecha_utc,
        id_externo,
        raw_json
    )
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
    raw_json=VALUES(raw_json)
    """

    cursor.execute(sql,(
        data["user_id"],
        data["broker"],
        data["tipo_evento"],
        data["asset"],
        data["cantidad"],
        data["ticker_motor"],
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

3.  NO CONSIDERASTE LAS LLAVES POR ESTO LAS ESTAMOS PONIENDO
4. RECUERDA QUE NO SOY PROGRAMADOR POR LO QUE SE ESPECIFICO SI HAY CAMBIOS EN DONDE LOS HAGOS