# ==========================================================
# operativa.py
# Motor Enterprise Unificado
# Versión 1.1 (Estable y Ordenado)
# Basado en Motor Saldos v5.6.3
# ==========================================================

import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# ==========================================================
# 🔐 CONFIGURACIÓN GLOBAL
# ==========================================================

MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
STABLES = ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']

# ==========================================================
# 🔐 SEGURIDAD
# ==========================================================

def descifrar_dato(texto, master):
    try:
        raw = base64.b64decode(texto.strip())
        partes = raw.rsplit(b":::", 1) if b":::" in raw else raw.rsplit(b"::", 1)
        data, iv = partes
        key_hash = sha256(master.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except:
        return None

# ==========================================================
# 📊 SYS SYNC ESTADO
# ==========================================================

def obtener_last_sync(cursor, user_id, exchange, endpoint):
    cursor.execute("""
        SELECT last_timestamp FROM sys_sync_estado
        WHERE user_id=%s AND exchange=%s AND endpoint=%s
        LIMIT 1
    """, (user_id, exchange, endpoint))
    row = cursor.fetchone()
    return row['last_timestamp'] if row else 0

def actualizar_sync(cursor, user_id, exchange, endpoint, ts):
    cursor.execute("""
        INSERT INTO sys_sync_estado
        (user_id, exchange, endpoint, last_timestamp, last_update)
        VALUES (%s,%s,%s,%s,NOW())
        ON DUPLICATE KEY UPDATE
        last_timestamp=VALUES(last_timestamp),
        last_update=NOW()
    """, (user_id, exchange, endpoint, ts))

# ==========================================================
# 🎯 TRADUCTOR + RADAR
# ==========================================================

def limpiar_prefijos(ticker):
    ticker = ticker.upper().strip()
    for p in ["LD","STK","NCFX","NCSK","NCSI"]:
        if ticker.startswith(p):
            return ticker[len(p):]
    return ticker

def obtener_traductor_id(cursor, motor_fuente, ticker):
    ticker = ticker.upper().strip()

    cursor.execute("""
        SELECT id FROM sys_traductor_simbolos
        WHERE motor_fuente=%s AND ticker_motor=%s AND is_active=1
        LIMIT 1
    """, (motor_fuente, ticker))
    row = cursor.fetchone()
    if row:
        return row['id']

    limpio = limpiar_prefijos(ticker)

    cursor.execute("""
        SELECT id FROM sys_traductor_simbolos
        WHERE underlying=%s
        ORDER BY is_active DESC, fecha_creacion DESC
        LIMIT 1
    """, (limpio,))
    row = cursor.fetchone()
    return row['id'] if row else None

def disparar_radar(cursor, uid, ticker, contexto):
    cursor.execute("""
        INSERT IGNORE INTO sys_simbolos_buscados
        (user_id, ticker, status, info)
        VALUES (%s,%s,'pendiente',%s)
    """, (uid, ticker, f"Detectado en {contexto}"))

# ==========================================================
# 💰 INSERT TRANSACCIÓN CON TRADUCTOR
# ==========================================================

def insertar_tx(cursor, uid, exchange, categoria, monto, asset, ts, endpoint):

    tid = obtener_traductor_id(cursor, exchange.lower(), asset)

    if not tid:
        disparar_radar(cursor, uid, asset, endpoint)

    tx_id = f"{exchange}-{endpoint}-{ts}-{asset}"

    cursor.execute("""
        INSERT IGNORE INTO transacciones_globales
        (user_id, exchange, id_externo,
         categoria, monto_neto,
         asset, traductor_id, fecha_utc)
        VALUES (%s,%s,%s,%s,%s,%s,%s,FROM_UNIXTIME(%s/1000))
    """, (
        uid, exchange, tx_id,
        categoria, monto,
        asset, tid, ts
    ))

# ==========================================================
# 🟨 BINANCE (CORREGIDO)
# ==========================================================

def procesar_binance(db, uid, api_key, api_secret):

    cursor = db.cursor(dictionary=True)
    client = Client(api_key, api_secret)

    # ===== SALDOS =====
    account = client.get_account()

    for b in account['balances']:
        total = float(b['free']) + float(b['locked'])
        if total <= 0.000001:
            continue

        ticker = b['asset']
        tid = obtener_traductor_id(cursor, "binance_spot", ticker)

        if not tid:
            disparar_radar(cursor, uid, ticker, "BINANCE_SALDO")

        cursor.execute("""
            INSERT INTO sys_saldos_usuarios
            (user_id, broker_name, asset, traductor_id,
             cantidad_total, cantidad_disponible,
             cantidad_bloqueada, last_update)
            VALUES (%s,'BINANCE',%s,%s,%s,%s,%s,NOW())
            ON DUPLICATE KEY UPDATE
            traductor_id=VALUES(traductor_id),
            cantidad_total=VALUES(cantidad_total),
            cantidad_disponible=VALUES(cantidad_disponible),
            cantidad_bloqueada=VALUES(cantidad_bloqueada),
            last_update=NOW()
        """, (
            uid, ticker, tid,
            total, float(b['free']),
            float(b['locked'])
        ))

    # ===== DEPOSITS =====
    deposits = client.get_deposit_history()

    for d in deposits:
        insertar_tx(cursor, uid, "BINANCE",
                    "DEPOSIT",
                    float(d['amount']),
                    d['coin'],
                    d['insertTime'],
                    "DEPOSIT")

    db.commit()
    print(f"[OK] Binance User {uid}")

# ==========================================================
# 🟦 BINGX (CORREGIDO)
# ==========================================================

def procesar_bingx(db, uid, ak, sec):

    cursor = db.cursor(dictionary=True)

    def bx_req(path, params=None):
        if params is None:
            params = {}
        ts = int(time.time()*1000)
        params["timestamp"] = ts
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(sec.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        return requests.get(url, headers={"X-BX-APIKEY": ak}, timeout=10).json()

    try:
        res = bx_req("/openApi/spot/v1/account/balance")

        if res.get("data"):
            for b in res['data']['balances']:
                total = float(b['free']) + float(b['locked'])
                if total <= 0.000001:
                    continue

                ticker = b['asset']
                tid = obtener_traductor_id(cursor, "bingx_crypto", ticker)

                if not tid:
                    disparar_radar(cursor, uid, ticker, "BINGX_SALDO")

                cursor.execute("""
                    INSERT INTO sys_saldos_usuarios
                    (user_id, broker_name, asset, traductor_id,
                     cantidad_total, cantidad_disponible,
                     cantidad_bloqueada, last_update)
                    VALUES (%s,'BINGX',%s,%s,%s,%s,%s,NOW())
                    ON DUPLICATE KEY UPDATE
                    traductor_id=VALUES(traductor_id),
                    cantidad_total=VALUES(cantidad_total),
                    cantidad_disponible=VALUES(cantidad_disponible),
                    cantidad_bloqueada=VALUES(cantidad_bloqueada),
                    last_update=NOW()
                """, (
                    uid, ticker, tid,
                    total, float(b['free']),
                    float(b['locked'])
                ))

        db.commit()
        print(f"[OK] BingX User {uid}")

    except Exception as e:
        print(f"[ERROR BINGX {uid}] {e}")

# ==========================================================
# 🏦 CONCILIACIÓN
# ==========================================================

def ejecutar_conciliacion(db):

    cursor = db.cursor(dictionary=True)

    cursor.execute("""
        SELECT DISTINCT user_id, broker_name AS exchange, asset
        FROM sys_saldos_usuarios
    """)

    registros = cursor.fetchall()

    for r in registros:

        cursor.execute("""
            SELECT IFNULL(SUM(monto_neto),0) total
            FROM transacciones_globales
            WHERE user_id=%s AND exchange=%s AND asset=%s
        """, (r['user_id'], r['exchange'], r['asset']))

        ledger = float(cursor.fetchone()['total'])

        cursor.execute("""
            SELECT cantidad_total FROM sys_saldos_usuarios
            WHERE user_id=%s AND broker_name=%s AND asset=%s
            LIMIT 1
        """, (r['user_id'], r['exchange'], r['asset']))

        snapshot = float(cursor.fetchone()['cantidad_total'])

        diff = snapshot - ledger
        estado = "OK" if abs(diff) < 0.0001 else "DESCUADRE"

        cursor.execute("""
            INSERT INTO sys_conciliacion_saldos
            (user_id, exchange, asset,
             ledger_total, snapshot_total,
             diferencia, estado, fecha_revision)
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            r['user_id'], r['exchange'], r['asset'],
            ledger, snapshot, diff, estado
        ))

    db.commit()

# ==========================================================
# 🚀 ORQUESTADOR
# ==========================================================

def run():

    print("🚀 MOTOR ENTERPRISE v1.1 INICIADO")

    while True:

        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)

            cursor.execute("""
                SELECT user_id, api_key, api_secret, broker_name
                FROM api_keys
                WHERE status=1
            """)

            users = cursor.fetchall()

            for u in users:

                key = descifrar_dato(u['api_key'], MASTER_KEY)
                sec = descifrar_dato(u['api_secret'], MASTER_KEY)

                if not key or not sec:
                    continue

                broker = u['broker_name'].upper()

                if broker == "BINANCE":
                    procesar_binance(db, u['user_id'], key, sec)

                elif broker == "BINGX":
                    procesar_bingx(db, u['user_id'], key, sec)

            ejecutar_conciliacion(db)

            db.close()

        except Exception as e:
            print(f"[CRITICAL] {e}")

        time.sleep(60)

if __name__ == "__main__":
    run()