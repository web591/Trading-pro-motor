# ==========================================================
# 🏛 OPERATIVA ENTERPRISE FULL
# Versión 1.3
# ==========================================================

import mysql.connector
from binance.client import Client
import requests
import os
import base64
import hmac
import hashlib
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from datetime import datetime
import time
import config

# ==========================================================
# 🔐 SEGURIDAD AES-CBC
# ==========================================================

def decrypt_api_key(encrypted_value):
    key = sha256(config.MASTER_KEY.encode()).digest()
    raw = base64.b64decode(encrypted_value)
    iv = raw[:16]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted = unpad(cipher.decrypt(raw[16:]), AES.block_size)
    return decrypted.decode()

# ==========================================================
# 🔌 DB FRESCA
# ==========================================================

def conectar_db():
    return mysql.connector.connect(
        host=config.DB_HOST,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        database=config.DB_NAME
    )

# ==========================================================
# 🔄 SYNC ESTADO
# ==========================================================

def obtener_sync(db, broker, endpoint):
    c = db.cursor()
    c.execute("""
        SELECT ultimo_timestamp
        FROM sys_sync_estado
        WHERE broker=%s AND endpoint=%s
    """, (broker, endpoint))
    r = c.fetchone()
    return r[0] if r else 0

def actualizar_sync(db, broker, endpoint, ts):
    c = db.cursor()
    c.execute("""
        INSERT INTO sys_sync_estado (broker, endpoint, ultimo_timestamp)
        VALUES (%s,%s,%s)
        ON DUPLICATE KEY UPDATE ultimo_timestamp=%s
    """, (broker, endpoint, ts, ts))
    db.commit()

# ==========================================================
# 📦 INSERT CONTABLE
# ==========================================================

def insertar_tx(db, data):
    c = db.cursor()
    c.execute("""
        INSERT IGNORE INTO transacciones_globales
        (user_id, exchange, asset, tipo, monto_neto, fecha_utc, id_externo)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (
        data["user_id"],
        data["exchange"],
        data["asset"],
        data["tipo"],
        data["monto"],
        data["fecha"],
        data["id"]
    ))
    db.commit()

# ==========================================================
# 🟡 BINANCE FULL
# ==========================================================

def procesar_binance(db, user_id, key_enc, secret_enc):

    print("🔶 BINANCE FULL")

    key = decrypt_api_key(key_enc)
    secret = decrypt_api_key(secret_enc)

    client = Client(key, secret)

    # ================= SPOT TRADES =================

    endpoint = "BN_SPOT_TRADES"
    since = obtener_sync(db, "BINANCE", endpoint)

    account = client.get_account()
    balances = account["balances"]

    for b in balances:
        asset = b["asset"]

        try:
            trades = client.get_my_trades(symbol=asset+"USDT")
        except:
            continue

        for t in trades:
            if t["time"] <= since:
                continue

            insertar_tx(db, {
                "user_id": user_id,
                "exchange": "BINANCE",
                "asset": asset,
                "tipo": "TRADE",
                "monto": float(t["qty"]),
                "fecha": datetime.utcfromtimestamp(t["time"]/1000),
                "id": "BN-SPOT-"+str(t["id"])
            })

            actualizar_sync(db, "BINANCE", endpoint, t["time"])

    # ================= FUTURES USDT-M =================

    endpoint = "BN_FUTURES_TRADES"
    since = obtener_sync(db, "BINANCE", endpoint)

    futures = client.futures_account_trades()

    for t in futures:
        if t["time"] <= since:
            continue

        insertar_tx(db, {
            "user_id": user_id,
            "exchange": "BINANCE",
            "asset": t["symbol"],
            "tipo": "TRADE",
            "monto": float(t["qty"]),
            "fecha": datetime.utcfromtimestamp(t["time"]/1000),
            "id": "BN-FUT-"+str(t["id"])
        })

        actualizar_sync(db, "BINANCE", endpoint, t["time"])

    # ================= DEPOSITS =================

    endpoint = "BN_DEPOSITS"
    since = obtener_sync(db, "BINANCE", endpoint)

    deposits = client.get_deposit_history()

    for d in deposits:
        if d["insertTime"] <= since:
            continue

        insertar_tx(db, {
            "user_id": user_id,
            "exchange": "BINANCE",
            "asset": d["coin"],
            "tipo": "DEPOSIT",
            "monto": float(d["amount"]),
            "fecha": datetime.utcfromtimestamp(d["insertTime"]/1000),
            "id": "BN-DEP-"+d["txId"]
        })

        actualizar_sync(db, "BINANCE", endpoint, d["insertTime"])

    # ================= WITHDRAWALS =================

    endpoint = "BN_WITHDRAW"
    since = obtener_sync(db, "BINANCE", endpoint)

    withdrawals = client.get_withdraw_history()

    for w in withdrawals:
        if w["applyTime"]:
            ts = int(datetime.strptime(w["applyTime"], "%Y-%m-%d %H:%M:%S").timestamp()*1000)
        else:
            continue

        if ts <= since:
            continue

        insertar_tx(db, {
            "user_id": user_id,
            "exchange": "BINANCE",
            "asset": w["coin"],
            "tipo": "WITHDRAW",
            "monto": -float(w["amount"]),
            "fecha": datetime.utcfromtimestamp(ts/1000),
            "id": "BN-WITH-"+w["id"]
        })

        actualizar_sync(db, "BINANCE", endpoint, ts)

# ==========================================================
# 🔵 BINGX FULL
# ==========================================================

def procesar_bingx(db, user_id, key_enc, secret_enc):

    print("🔷 BINGX FULL")

    key = decrypt_api_key(key_enc)
    secret = decrypt_api_key(secret_enc)

    timestamp = str(int(time.time()*1000))
    params = f"timestamp={timestamp}"
    signature = hmac.new(secret.encode(), params.encode(), hashlib.sha256).hexdigest()

    headers = {
        "X-BX-APIKEY": key,
        "User-Agent": "Mozilla/5.0"
    }

    url = f"https://open-api.bingx.com/openApi/swap/v2/trade/allOrders?{params}&signature={signature}"

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print("❌ Error BingX")
        return

    data = r.json().get("data", [])

    endpoint = "BX_FUTURES"
    since = obtener_sync(db, "BINGX", endpoint)

    for t in data:
        ts = int(t["updateTime"])
        if ts <= since:
            continue

        insertar_tx(db, {
            "user_id": user_id,
            "exchange": "BINGX",
            "asset": t["symbol"],
            "tipo": "TRADE",
            "monto": float(t["executedQty"]),
            "fecha": datetime.utcfromtimestamp(ts/1000),
            "id": "BX-FUT-"+str(t["orderId"])
        })

        actualizar_sync(db, "BINGX", endpoint, ts)

# ==========================================================
# 🏦 CONCILIACIÓN ENTERPRISE
# ==========================================================

def ejecutar_conciliacion(db):

    print("🔎 CONCILIACIÓN ENTERPRISE")

    c = db.cursor(dictionary=True)

    c.execute("""
        SELECT user_id, broker_name, asset, cantidad_total, cantidad_bloqueada
        FROM sys_saldos_usuarios
    """)

    rows = c.fetchall()

    for r in rows:

        c.execute("""
            SELECT IFNULL(SUM(monto_neto),0) total
            FROM transacciones_globales
            WHERE user_id=%s AND exchange=%s AND asset=%s
        """, (r["user_id"], r["broker_name"], r["asset"]))

        ledger = float(c.fetchone()["total"])
        snapshot = float(r["cantidad_total"])
        locked = float(r.get("cantidad_bloqueada", 0))

        diferencia = snapshot - (ledger + locked)

        estado = "OK" if abs(diferencia) < 0.0001 else "DESCUADRE"

        c.execute("""
            INSERT INTO sys_conciliacion_saldos
            (user_id, exchange, asset,
             ledger_total, snapshot_total,
             diferencia, estado, fecha_revision)
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            r["user_id"],
            r["broker_name"],
            r["asset"],
            ledger,
            snapshot,
            diferencia,
            estado
        ))

    db.commit()
    print("✅ CONCILIACIÓN FINALIZADA")

# ==========================================================
# 🚀 ORQUESTADOR
# ==========================================================

def run():

    db = conectar_db()
    c = db.cursor(dictionary=True)

    c.execute("SELECT * FROM api_keys WHERE activa=1")
    cuentas = c.fetchall()

    for cuenta in cuentas:

        if cuenta["broker"] == "BINANCE":
            procesar_binance(db, cuenta["user_id"], cuenta["api_key"], cuenta["api_secret"])

        elif cuenta["broker"] == "BINGX":
            procesar_bingx(db, cuenta["user_id"], cuenta["api_key"], cuenta["api_secret"])

    ejecutar_conciliacion(db)

    db.close()

# ==========================================================
# 🔥 MAIN
# ==========================================================

if __name__ == "__main__":
    run()