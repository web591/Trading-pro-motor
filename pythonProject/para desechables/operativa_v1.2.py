# ==========================================================
# 🏛 OPERATIVA ENTERPRISE
# Versión 1.2
# ==========================================================

import mysql.connector
from binance.client import Client
import requests
import os
import base64
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from datetime import datetime
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
# 🔌 CONEXIÓN FRESCA
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

def obtener_ultimo_sync(db, broker, endpoint):
    cursor = db.cursor()
    cursor.execute("""
        SELECT ultimo_timestamp
        FROM sys_sync_estado
        WHERE broker=%s AND endpoint=%s
    """, (broker, endpoint))
    row = cursor.fetchone()
    return row[0] if row else 0

def actualizar_sync(db, broker, endpoint, timestamp):
    cursor = db.cursor()
    cursor.execute("""
        INSERT INTO sys_sync_estado (broker, endpoint, ultimo_timestamp)
        VALUES (%s,%s,%s)
        ON DUPLICATE KEY UPDATE ultimo_timestamp=%s
    """, (broker, endpoint, timestamp, timestamp))
    db.commit()

# ==========================================================
# 📦 INSERT CONTABLE
# ==========================================================

def insertar_transaccion(db, data):
    cursor = db.cursor()
    sql = """
        INSERT IGNORE INTO transacciones_globales
        (user_id, exchange, asset, tipo, monto_neto, fecha_utc, id_externo)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """
    cursor.execute(sql, (
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
# 🟡 BINANCE
# ==========================================================

def procesar_binance(db, user_id, api_key_enc, api_secret_enc):

    print("🔶 Binance Iniciando...")

    api_key = decrypt_api_key(api_key_enc)
    api_secret = decrypt_api_key(api_secret_enc)

    client = Client(api_key, api_secret)

    # === SPOT TRADES ===
    endpoint = "SPOT_TRADES"
    since = obtener_ultimo_sync(db, "BINANCE", endpoint)

    symbols = client.get_account()["balances"]

    for s in symbols:
        asset = s["asset"]
        try:
            trades = client.get_my_trades(symbol=asset+"USDT")
        except:
            continue

        for t in trades:
            if t["time"] <= since:
                continue

            insertar_transaccion(db, {
                "user_id": user_id,
                "exchange": "BINANCE",
                "asset": asset,
                "tipo": "TRADE",
                "monto": float(t["qty"]),
                "fecha": datetime.utcfromtimestamp(t["time"]/1000),
                "id": "BN-SPOT-"+str(t["id"])
            })

            actualizar_sync(db, "BINANCE", endpoint, t["time"])

    print("✅ Binance OK")

# ==========================================================
# 🔵 BINGX
# ==========================================================

def procesar_bingx(db, user_id, api_key_enc, api_secret_enc):

    print("🔷 BingX Iniciando...")

    api_key = decrypt_api_key(api_key_enc)
    api_secret = decrypt_api_key(api_secret_enc)

    headers = {
        "X-BX-APIKEY": api_key,
        "User-Agent": "Mozilla/5.0"
    }

    endpoint = "BINGX_TRADES"
    since = obtener_ultimo_sync(db, "BINGX", endpoint)

    url = "https://open-api.bingx.com/openApi/swap/v2/trade/allOrders"
    r = requests.get(url, headers=headers)

    if r.status_code != 200:
        print("❌ Error BingX")
        return

    data = r.json()

    for t in data.get("data", []):
        ts = int(t.get("updateTime", 0))

        if ts <= since:
            continue

        insertar_transaccion(db, {
            "user_id": user_id,
            "exchange": "BINGX",
            "asset": t.get("symbol", "UNKNOWN"),
            "tipo": "TRADE",
            "monto": float(t.get("executedQty", 0)),
            "fecha": datetime.utcfromtimestamp(ts/1000),
            "id": "BX-TRD-"+str(t.get("orderId"))
        })

        actualizar_sync(db, "BINGX", endpoint, ts)

    print("✅ BingX OK")

# ==========================================================
# 🏦 CONCILIACIÓN EMPRESARIAL
# ==========================================================

def ejecutar_conciliacion(db):

    print("🔎 Conciliación...")

    cursor = db.cursor(dictionary=True)

    cursor.execute("""
        SELECT DISTINCT user_id, broker_name AS exchange, asset, cantidad_total
        FROM sys_saldos_usuarios
    """)

    rows = cursor.fetchall()

    for r in rows:

        cursor.execute("""
            SELECT IFNULL(SUM(monto_neto),0) total
            FROM transacciones_globales
            WHERE user_id=%s AND exchange=%s AND asset=%s
        """, (r["user_id"], r["exchange"], r["asset"]))

        ledger = float(cursor.fetchone()["total"])
        snapshot = float(r["cantidad_total"])

        diferencia = snapshot - ledger
        estado = "OK" if abs(diferencia) < 0.0001 else "DESCUADRE"

        cursor.execute("""
            INSERT INTO sys_conciliacion_saldos
            (user_id, exchange, asset,
             ledger_total, snapshot_total,
             diferencia, estado, fecha_revision)
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            r["user_id"],
            r["exchange"],
            r["asset"],
            ledger,
            snapshot,
            diferencia,
            estado
        ))

    db.commit()

    print("✅ Conciliación Finalizada")

# ==========================================================
# 🚀 ORQUESTADOR PRINCIPAL
# ==========================================================

def run():

    db = conectar_db()
    cursor = db.cursor(dictionary=True)

    cursor.execute("SELECT * FROM api_keys WHERE activa=1")
    cuentas = cursor.fetchall()

    for c in cuentas:

        if c["broker"] == "BINANCE":
            procesar_binance(db, c["user_id"], c["api_key"], c["api_secret"])

        elif c["broker"] == "BINGX":
            procesar_bingx(db, c["user_id"], c["api_key"], c["api_secret"])

    ejecutar_conciliacion(db)

    db.close()

# ==========================================================
# 🔥 MAIN
# ==========================================================

if __name__ == "__main__":
    run()