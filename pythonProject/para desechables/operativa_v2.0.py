# ==========================================================
# 🏛 OPERATIVA V2 - SALDOS + CONTABILIDAD INTEGRADA
# Versión 2.1 ENTERPRISE
# Integrado con Traductor + Radar + Triple Amarre
# ==========================================================

import mysql.connector
from binance.client import Client
import requests
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
# 🔐 SEGURIDAD
# ==========================================================

def decrypt_api_key(encrypted_value):
    key = sha256(config.MASTER_KEY.encode()).digest()
    raw = base64.b64decode(encrypted_value)
    iv = raw[:16]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted = unpad(cipher.decrypt(raw[16:]), AES.block_size)
    return decrypted.decode()

# ==========================================================
# 🔌 DB
# ==========================================================

def conectar_db():
    return mysql.connector.connect(
        host=config.DB_HOST,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        database=config.DB_NAME
    )

# ==========================================================
# 🧠 LIMPIEZA PREFIJOS
# ==========================================================

def limpiar_prefijos(symbol):
    prefijos = ["LD", "STK", "NCFX", "NCSK", "NCSI"]
    for p in prefijos:
        if symbol.startswith(p):
            return symbol[len(p):]
    return symbol

# ==========================================================
# 🔗 TRIPLE AMARRE
# ==========================================================

def resolver_underlying(db, ticker_motor):

    ticker_limpio = limpiar_prefijos(ticker_motor)

    c = db.cursor(dictionary=True)
    c.execute("""
        SELECT id, underlying
        FROM sys_traductor_simbolos
        WHERE ticker_motor=%s
        LIMIT 1
    """, (ticker_motor,))

    row = c.fetchone()

    if row:
        return row["id"], row["underlying"]

    # Si no existe, enviar a radar
    c.execute("""
        INSERT IGNORE INTO sys_simbolos_buscados
        (symbol, fecha_solicitud, estado)
        VALUES (%s, NOW(), 'pendiente')
    """, (ticker_limpio,))
    db.commit()

    return None, ticker_limpio

# ==========================================================
# 📦 INSERT CONTABLE INTEGRADO
# ==========================================================

def insertar_tx(db, user_id, exchange, ticker_motor, tipo, monto, fecha, id_ext):

    traductor_id, underlying = resolver_underlying(db, ticker_motor)

    c = db.cursor()

    c.execute("""
        INSERT IGNORE INTO transacciones_globales
        (user_id, exchange, asset, tipo, monto_neto,
         fecha_utc, id_externo, traductor_id, underlying)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        user_id,
        exchange,
        ticker_motor,
        tipo,
        monto,
        fecha,
        id_ext,
        traductor_id,
        underlying
    ))

    db.commit()

# ==========================================================
# 🔄 SYNC
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
# 🟡 BINANCE OPERATIVA
# ==========================================================

def procesar_binance(db, user_id, key_enc, secret_enc):

    print("🔶 BINANCE INTEGRADO")

    key = decrypt_api_key(key_enc)
    secret = decrypt_api_key(secret_enc)

    client = Client(key, secret)

    # ================= SALDOS =================
    account = client.get_account()
    balances = account["balances"]

    for b in balances:

        free = float(b["free"])
        locked = float(b["locked"])
        asset = b["asset"]

        if free == 0 and locked == 0:
            continue

        c = db.cursor()
        c.execute("""
            INSERT INTO sys_saldos_usuarios
            (user_id, broker_name, asset,
             cantidad_total, cantidad_bloqueada, fecha_actualizacion)
            VALUES (%s,'BINANCE',%s,%s,%s,NOW())
            ON DUPLICATE KEY UPDATE
            cantidad_total=%s,
            cantidad_bloqueada=%s,
            fecha_actualizacion=NOW()
        """, (user_id, asset, free, locked, free, locked))
        db.commit()

    # ================= SPOT TRADES =================
    endpoint = "BN_SPOT"
    since = obtener_sync(db, "BINANCE", endpoint)

    for b in balances:

        asset = b["asset"]

        try:
            trades = client.get_my_trades(symbol=asset+"USDT")
        except:
            continue

        for t in trades:

            if t["time"] <= since:
                continue

            qty = float(t["qty"])
            side = t["isBuyer"]
            monto = qty if side else -qty

            insertar_tx(
                db, user_id, "BINANCE",
                asset,
                "TRADE",
                monto,
                datetime.utcfromtimestamp(t["time"]/1000),
                "BN-SPOT-"+str(t["id"])
            )

            actualizar_sync(db, "BINANCE", endpoint, t["time"])

    print("✅ Binance OK")

# ==========================================================
# 🔵 BINGX OPERATIVA
# ==========================================================

def procesar_bingx(db, user_id, key_enc, secret_enc):

    print("🔷 BINGX INTEGRADO")

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

        qty = float(t["executedQty"])
        side = t["side"]
        monto = qty if side == "BUY" else -qty

        insertar_tx(
            db, user_id, "BINGX",
            t["symbol"],
            "TRADE",
            monto,
            datetime.utcfromtimestamp(ts/1000),
            "BX-FUT-"+str(t["orderId"])
        )

        actualizar_sync(db, "BINGX", endpoint, ts)

    print("✅ BingX OK")

# ==========================================================
# 🏦 CONCILIACIÓN
# ==========================================================

def ejecutar_conciliacion(db):

    print("🔎 CONCILIACIÓN ENTERPRISE")

    c = db.cursor(dictionary=True)
    c.execute("""
        SELECT user_id, broker_name, asset,
               cantidad_total, cantidad_bloqueada
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
        locked = float(r["cantidad_bloqueada"])

        diferencia = snapshot - (ledger + locked)
        estado = "OK" if abs(diferencia) < 0.0001 else "DESCUADRE"

        c.execute("""
            INSERT INTO sys_conciliacion_saldos
            (user_id, exchange, asset,
             ledger_total, snapshot_total,
             diferencia, estado, fecha_revision)
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            r["user_id"], r["broker_name"], r["asset"],
            ledger, snapshot, diferencia, estado
        ))

    db.commit()
    print("✅ Conciliación Finalizada")

# ==========================================================
# 🚀 MAIN
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

if __name__ == "__main__":
    run()