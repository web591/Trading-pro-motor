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
    except: return None

# ==========================================================
# 🎯 VINCULACIÓN Y PRECIOS
# ==========================================================
def obtener_traductor_id(cursor, motor_fuente, ticker):
    ticker = ticker.upper().strip()
    sql = "SELECT id FROM sys_traductor_simbolos WHERE motor_fuente = %s AND ticker_motor = %s LIMIT 1"
    cursor.execute(sql, (motor_fuente, ticker))
    row = cursor.fetchone()
    if row: return row
    
    ticker_limpio = ticker[2:] if ticker.startswith("LD") else (ticker[3:] if ticker.startswith("STK") else ticker)
    sql = "SELECT id FROM sys_traductor_simbolos WHERE motor_fuente = %s AND underlying = %s LIMIT 1"
    cursor.execute(sql, (motor_fuente, ticker_limpio))
    return cursor.fetchone()

def obtener_precio_usd(cursor, tid, asset_name):
    if asset_name.upper() in ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']: return 1.0
    try:
        if tid:
            sql = "SELECT price FROM sys_precios_activos WHERE traductor_id = %s ORDER BY last_update DESC LIMIT 1"
            cursor.execute(sql, (tid['id'] if isinstance(tid, dict) else tid,))
            row = cursor.fetchone()
            if row: return float(row['price'] if isinstance(row, dict) else row[0])
    except: pass
    return 0.0

# ==========================================================
# 📝 REGISTRO MAESTRO
# ==========================================================
def registrar_cashflow(cursor, data):
    tid = obtener_traductor_id(cursor, data["broker"], data["asset"])
    precio = obtener_precio_usd(cursor, tid, data["asset"])
    valor_usd = float(data["cantidad"]) * precio

    sql = """
    INSERT INTO sys_cashflows (user_id, broker, tipo_evento, asset, cantidad, ticker_motor, valor_usd, fecha_utc, id_externo, raw_json)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE raw_json=VALUES(raw_json), valor_usd=VALUES(valor_usd)
    """
    cursor.execute(sql, (data["user_id"], data["broker"], data["tipo_evento"], data["asset"], 
                         data["cantidad"], data["ticker_motor"], valor_usd, data["fecha"], 
                         data["id_externo"], data["raw"]))

# ==========================================================
# 🔌 BINANCE FUNCTIONS
# ==========================================================
def binance_sign(secret, query):
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()

def binance_income(db, uid, key, secret):
    cursor = db.cursor()
    url = f"https://fapi.binance.com/fapi/v1/income?timestamp={int(time.time()*1000)}"
    url += f"&signature={binance_sign(secret, url.split('?')[1])}"
    r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
    for i in r:
        registrar_cashflow(cursor, {
            "user_id": uid, "broker": "BINANCE", "tipo_evento": i["incomeType"], "asset": i["asset"],
            "cantidad": float(i["income"]), "ticker_motor": i.get("symbol"),
            "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(i["time"]/1000)),
            "id_externo": f"BN-INC-{i['tranId']}", "raw": json.dumps(i)
        })
    db.commit()

def binance_deposits(db, uid, key, secret):
    cursor = db.cursor()
    params = urlencode({"timestamp": int(time.time()*1000)})
    url = f"https://api.binance.com/sapi/v1/capital/deposit/hisrec?{params}&signature={binance_sign(secret, params)}"
    r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
    for d in r:
        registrar_cashflow(cursor, {
            "user_id": uid, "broker": "BINANCE", "tipo_evento": "DEPOSIT", "asset": d["coin"],
            "cantidad": float(d["amount"]), "ticker_motor": None,
            "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(d["insertTime"]/1000)),
            "id_externo": f"BN-DEP-{d['txId']}", "raw": json.dumps(d)
        })
    db.commit()

def binance_withdrawals(db, uid, key, secret):
    cursor = db.cursor()
    params = urlencode({"timestamp": int(time.time()*1000)})
    url = f"https://api.binance.com/sapi/v1/capital/withdraw/history?{params}&signature={binance_sign(secret, params)}"
    r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
    for w in r:
        registrar_cashflow(cursor, {
            "user_id": uid, "broker": "BINANCE", "tipo_evento": "WITHDRAW", "asset": w["coin"],
            "cantidad": float(w["amount"]), "ticker_motor": None, "fecha": w["applyTime"],
            "id_externo": f"BN-WITH-{w.get('id', int(time.time()))}", "raw": json.dumps(w)
        })
    db.commit()

def binance_dust_log(db, uid, key, secret):
    cursor = db.cursor()
    params = urlencode({"timestamp": int(time.time()*1000)})
    url = f"https://api.binance.com/sapi/v1/asset/dribblet?{params}&signature={binance_sign(secret, params)}"
    r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
    if "userAssetDribblets" in r:
        for entry in r["userAssetDribblets"]:
            for detail in entry["userAssetDribbletDetails"]:
                registrar_cashflow(cursor, {
                    "user_id": uid, "broker": "BINANCE", "tipo_evento": "DUST_CONVERT", "asset": detail["fromAsset"],
                    "cantidad": float(detail["amount"]), "ticker_motor": None,
                    "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(entry["operateTime"]/1000)),
                    "id_externo": f"BN-DUST-{detail['transId']}", "raw": json.dumps(detail)
                })
    db.commit()

def binance_staking_rewards(db, uid, key, secret):
    cursor = db.cursor()
    params = urlencode({"timestamp": int(time.time()*1000)})
    url = f"https://api.binance.com/sapi/v1/lending/union/interestHistory?{params}&signature={binance_sign(secret, params)}"
    r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
    for item in r:
        registrar_cashflow(cursor, {
            "user_id": uid, "broker": "BINANCE", "tipo_evento": "STAKING_REWARD", "asset": item["asset"],
            "cantidad": float(item["interest"]), "ticker_motor": None,
            "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(item["time"]/1000)),
            "id_externo": f"BN-STK-{item['time']}-{item['asset']}", "raw": json.dumps(item)
        })
    db.commit()

def binance_convert_history(db, uid, key, secret):
    cursor = db.cursor()
    ts = int(time.time()*1000)
    params = urlencode({"timestamp": ts, "startTime": ts - (30*24*60*60*1000)})
    url = f"https://api.binance.com/sapi/v1/convert/tradeFlow?{params}&signature={binance_sign(secret, params)}"
    r = requests.get(url, headers={"X-MBX-APIKEY": key}).json()
    if "list" in r:
        for c in r["list"]:
            registrar_cashflow(cursor, {
                "user_id": uid, "broker": "BINANCE", "tipo_evento": "CONVERT_OUT", "asset": c["fromAsset"],
                "cantidad": -float(c["fromAmount"]), "ticker_motor": None, "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(c["createTime"]/1000)),
                "id_externo": f"BN-CONV-OUT-{c['orderId']}", "raw": json.dumps(c)
            })
            registrar_cashflow(cursor, {
                "user_id": uid, "broker": "BINANCE", "tipo_evento": "CONVERT_IN", "asset": c["toAsset"],
                "cantidad": float(c["toAmount"]), "ticker_motor": None, "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(c["createTime"]/1000)),
                "id_externo": f"BN-CONV-IN-{c['orderId']}", "raw": json.dumps(c)
            })
    db.commit()

# ==========================================================
# 🔌 BINGX FUNCTIONS
# ==========================================================
def bingx_income_history(db, uid, key, secret):
    cursor = db.cursor()
    params = urlencode({"timestamp": int(time.time()*1000), "limit": 100})
    sig = hmac.new(secret.encode(), params.encode(), hashlib.sha256).hexdigest()
    url = f"https://open-api.bingx.com/openApi/swap/v2/user/income?{params}&signature={sig}"
    r = bingx_session.get(url, headers=get_headers_bingx(key)).json()
    if r.get("code") == 0 and "data" in r:
        for i in r["data"]:
            registrar_cashflow(cursor, {
                "user_id": uid, "broker": "BINGX", "tipo_evento": i["incomeType"], "asset": i["asset"],
                "cantidad": float(i["income"]), "ticker_motor": i.get("symbol"),
                "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(i["time"]/1000)),
                "id_externo": f"BX-INC-{i['asset']}-{i['income']}-{i['time']}", "raw": json.dumps(i)
            })
    db.commit()

# ==========================================================
# 🚀 EJECUCIÓN
# ==========================================================
def ejecutar_motor_financiero(db):
    print("\n💎 MOTOR v1.1.1 - AUDITORÍA FINANCIERA TOTAL")
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM sys_usuarios WHERE status=1")
    usuarios = cursor.fetchall()

    for u in usuarios:
        print(f"\n>> Auditando: {u['broker_name']} (User {u['user_id']})")
        k = descifrar_dato(u['api_key'], MASTER_KEY)
        s = descifrar_dato(u['api_secret'], MASTER_KEY)
        if not k or not s: continue

        if u['broker_name'].upper() == "BINANCE":
            binance_income(db, u['user_id'], k, s)
            binance_deposits(db, u['user_id'], k, s)
            binance_withdrawals(db, u['user_id'], k, s)
            binance_convert_history(db, u['user_id'], k, s)
            binance_dust_log(db, u['user_id'], k, s)
            binance_staking_rewards(db, u['user_id'], k, s)
        elif u['broker_name'].upper() == "BINGX":
            bingx_income_history(db, u['user_id'], k, s)

    print("\n✅ Auditoría Finalizada.")