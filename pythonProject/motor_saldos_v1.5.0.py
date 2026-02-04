# ==========================================================
# MOTOR MAESTRO v1.5.0 — Binance Earn + Airdrops (REST) Bingx sincronizado
# ==========================================================

import mysql.connector
from binance.client import Client
import time, os, base64, hmac, requests, hashlib
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# ---------------- TIEMPOS (NO TOCADOS) ----------------
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
ESPERA_CICLO_RAPIDO = 120
CICLOS_PARA_STATEMENTS = 240

# ======================================================
# SEGURIDAD
# ======================================================
def descifrar_dato(t, m):
    try:
        raw = base64.b64decode(t.strip())
        data, iv = raw.split(b"::")
        cipher = AES.new(sha256(m.encode()).digest(), AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except:
        return None

# ======================================================
# PRECIO DESDE DB (VÍA TRADUCTOR)
# ======================================================
def obtener_precio_db(cursor, ticker):
    stables = ['USDT','USDC','BUSD','DAI','FDUSD']
    if ticker.upper() in stables:
        return 1.0

    cursor.execute("""
        SELECT p.price
        FROM sys_traductor_simbolos t
        JOIN sys_precios_activos p ON p.traductor_id = t.id
        WHERE t.nombre_comun=%s OR t.ticker_motor=%s
        LIMIT 1
    """, (ticker, f"{ticker}USDT"))

    r = cursor.fetchone()
    return float(r['price']) if r else 0.0

# ======================================================
# DISPARADOR INTELIGENTE (100% REAL BD)
# ======================================================
def disparador_inteligente_pro(cursor, user_id, ticker, cantidad):
    stables = ['USDT','USDC','BUSD','DAI','FDUSD']
    ticker = normalizar_simbolo(ticker)

    if ticker.upper() in stables or cantidad == 0:

        return

    cursor.execute("""
        SELECT id FROM sys_traductor_simbolos
        WHERE nombre_comun=%s OR ticker_motor=%s
        LIMIT 1
    """, (ticker, f"{ticker}USDT"))

    r = cursor.fetchone()
    if not r:
        cursor.execute("""
            INSERT IGNORE INTO sys_simbolos_buscados (ticker, status)
            VALUES (%s,'pendiente')
        """, (ticker,))
        return

    traductor_id = r['id']

    cursor.execute("""
        INSERT INTO sys_usuarios_activos
        (user_id, traductor_id, ticker_especifico, tipo_lista)
        VALUES (%s,%s,%s,'ACTIVO')
        ON DUPLICATE KEY UPDATE
            tipo_lista='ACTIVO',
            ticker_especifico=VALUES(ticker_especifico)
    """, (user_id, traductor_id, ticker))

# ======================================================
# NORMALIZADOR DE SÍMBOLOS (v1.4.3)
# ======================================================
def normalizar_simbolo(symbol):
    if symbol.startswith("LD"):
        return symbol[2:]
    return symbol


# ======================================================
# BINANCE
# ======================================================
def procesar_binance(key, sec, user_id, db, descargar_statements):
    try:
        client = Client(key, sec)
        cur = db.cursor(dictionary=True)        

        # ==========================================================
        # v1.3.0 — CACHE REAL DE POSICIONES (UNA VEZ POR CICLO)
        # ==========================================================
        positions_raw = client.futures_position_information()

        positions_map = {
            p['symbol']: abs(float(p['positionAmt']))
            for p in positions_raw
        }

        # Para evitar disparos repetidos por símbolo
        symbols_cerrados = set()

        # Para consolidar PNL por ciclo
        pnl_por_simbolo = {}

        # ================= SPOT SALDOS =================
        for b in client.get_account()['balances']:
            total = float(b['free']) + float(b['locked'])
            if total > 0:
                p = obtener_precio_db(cur, b['asset'])
                cur.execute("""
                    INSERT INTO sys_saldos_usuarios
                    (user_id, broker_name, tipo_cuenta, asset,
                     cantidad_total, cantidad_disponible,
                     cantidad_bloqueada, precio_referencia,
                     valor_usd, last_update, is_active)
                    VALUES (%s,'Binance','SPOT',%s,%s,%s,%s,%s,%s,NOW(),1)
                    ON DUPLICATE KEY UPDATE
                        cantidad_total=VALUES(cantidad_total),
                        valor_usd=VALUES(valor_usd),
                        last_update=NOW(),
                        is_active=1
                """, (user_id, b['asset'], total,
                      float(b['free']), float(b['locked']),
                      p, total*p))
                disparador_inteligente_pro(cur, user_id, b['asset'], total)

        # ================= FUTURES SALDOS =================
        for f in client.futures_account()['assets']:
            wb = float(f['walletBalance'])
            if wb != 0:
                eq = wb + float(f['unrealizedProfit'])
                cur.execute("""
                    INSERT INTO sys_saldos_usuarios
                    (user_id, broker_name, tipo_cuenta, asset,
                     cantidad_total, equidad_neta,
                     valor_usd, last_update, is_active)
                    VALUES (%s,'Binance','PERPETUAL',%s,%s,%s,%s,NOW(),1)
                    ON DUPLICATE KEY UPDATE
                        cantidad_total=VALUES(cantidad_total),
                        equidad_neta=VALUES(equidad_neta),
                        last_update=NOW(),
                        is_active=1
                """, (user_id, f['asset'], wb, eq, eq))
                disparador_inteligente_pro(cur, user_id, f['asset'], wb)

        # ================= OPEN ORDERS FUTURES =================
        cur.execute("DELETE FROM sys_open_orders WHERE user_id=%s AND broker_name='Binance'", (user_id,))
        for o in client.futures_get_open_orders():
            cur.execute("SELECT id FROM sys_traductor_simbolos WHERE ticker_motor=%s LIMIT 1", (o['symbol'],))
            t = cur.fetchone()
            if not t:
                continue
            cur.execute("""
                INSERT INTO sys_open_orders
                (id_order_ext,user_id,broker_name,traductor_id,
                 symbol,side,type,price,qty,locked_amount,fecha_utc)
                VALUES (%s,%s,'Binance',%s,%s,%s,%s,%s,%s,%s,
                        FROM_UNIXTIME(%s/1000))
            """, (o['orderId'], user_id, t['id'], o['symbol'],
                  o['side'], o['type'],
                  float(o['price']), float(o['origQty']),
                  float(o['price'])*float(o['origQty']),
                  o['time']))

        # ================= OPEN ORDERS SPOT (NUEVO) =================
        cur.execute("DELETE FROM sys_open_orders_spot WHERE user_id=%s AND broker_name='Binance'", (user_id,))
        for o in client.get_open_orders():
            cur.execute("SELECT id FROM sys_traductor_simbolos WHERE ticker_motor=%s LIMIT 1", (o['symbol'],))
            t = cur.fetchone()
            if not t:
                continue
            qty = float(o['origQty'])
            price = float(o['price'])
            cur.execute("""
                INSERT INTO sys_open_orders_spot
                (id_order_ext,user_id,broker_name,traductor_id,
                 symbol,side,type,price,qty,locked_amount,fecha_utc)
                VALUES (%s,%s,'Binance',%s,%s,%s,%s,%s,%s,%s,
                        FROM_UNIXTIME(%s/1000))
            """, (o['orderId'], user_id, t['id'], o['symbol'],
                  o['side'], o['type'],
                  price, qty, price*qty, o['time']))

        # ==========================================================
        # 4. TRADES — HISTÓRICO + CONSOLIDACIÓN
        # ==========================================================
        trades = client.futures_account_trades(limit=20)

        for t in trades:
            id_ext = f"BIN-T-{t['id']}"
            symbol = t['symbol']
            asset_base = symbol.replace('USDT', '')
            pnl_real = float(t['realizedPnl']) - float(t['commission'])

            pnl_por_simbolo[symbol] = pnl_por_simbolo.get(symbol, 0) + pnl_real

            cur.execute(
                """INSERT IGNORE INTO transacciones_globales
                (id_externo, user_id, exchange, cuenta_tipo,
                 categoria, asset, monto_neto,
                 timestamp_ms, descripcion)
                VALUES (%s,%s,'Binance','FUTURES',
                        'TRADE',%s,%s,%s,%s)""",
                (
                    id_ext, user_id, asset_base,
                    pnl_real, t['time'],
                    f"{t['side']} {symbol}"
                )
            )

            cur.execute(
                """INSERT IGNORE INTO detalle_trades
                (user_id, id_externo_ref, fecha_utc, symbol,
                 lado, precio_ejecucion,
                 cantidad_ejecutada, pnl_realizado, is_maker)
                VALUES (%s,%s,FROM_UNIXTIME(%s/1000),%s,%s,%s,%s,%s,%s)""",
                (
                    user_id,
                    id_ext,
                    t['time'],
                    symbol,
                    t['side'],
                    float(t['price']),
                    float(t['qty']),
                    float(t.get('realizedPnl', 0)),
                    1 if t['maker'] else 0
                )
            )


        # ==========================================================
        # 5. CIERRE REAL DE POSICIONES (NO POR TRADE)
        # ==========================================================
        for symbol, qty in positions_map.items():
            if qty == 0 and symbol not in symbols_cerrados:
                symbols_cerrados.add(symbol)
                asset_base = symbol.replace('USDT', '')

                # Marca cierre real
                cur.execute(
                    """UPDATE sys_usuarios_activos
                    SET tipo_lista='HISTORICOS',
                        is_active=0,
                        fecha_cierre_real=NOW()
                    WHERE user_id=%s AND asset=%s""",
                    (user_id, asset_base)
                )

        # ==========================================================
        # 6. CONSOLIDADO PNL POR CICLO
        # ==========================================================
        for symbol, pnl in pnl_por_simbolo.items():
            cur.execute(
                """INSERT INTO pnl_consolidado_ciclos
                (user_id, exchange, symbol, pnl_total, fecha_ciclo)
                VALUES (%s,'Binance',%s,%s,NOW())""",
                (user_id, symbol, pnl)
            )


        # ================= BINANCE AIRDROPS / REWARDS =================
        if descargar_statements:
            try:
                ts = int(time.time() * 1000)
                query = f"timestamp={ts}"
                sig = hmac.new(sec.encode(), query.encode(), hashlib.sha256).hexdigest()

                url = f"https://api.binance.com/sapi/v1/asset/assetDividend?{query}&signature={sig}"
                headers = {"X-MBX-APIKEY": key}

                res = requests.get(url, headers=headers, timeout=10).json()

                for d in res.get("rows", []):
                    id_ext = f"BN-AIRDROP-{d['asset']}-{d['divTime']}"

                    cur.execute("""
                        INSERT IGNORE INTO transacciones_globales
                        (id_externo, user_id, exchange, cuenta_tipo, categoria,
                         asset, monto_neto, fecha_utc, descripcion)
                        VALUES
                        (%s,%s,'Binance','SPOT','AIRDROP',
                         %s,%s,FROM_UNIXTIME(%s/1000),'Binance Asset Dividend')
                    """, (
                        id_ext,
                        user_id,
                        d["asset"],
                        float(d["amount"]),
                        d["divTime"]
                    ))

            except Exception as e:
                print("⚠️ Airdrop API no disponible:", e)

        # ================= BINANCE EARN — REALTIME (CONTABLE) =================
        if descargar_statements:
            try:
                end_ts = int(time.time() * 1000)
                start_ts = 0  # histórico completo (Binance limita internamente)

                current = 1
                size = 100

                while True:
                    query = (
                        f"startTime={start_ts}"
                        f"&endTime={end_ts}"
                        f"&timestamp={end_ts}"
                        f"&size={size}"
                        f"&current={current}"
                    )
                    sig = hmac.new(sec.encode(), query.encode(), hashlib.sha256).hexdigest()

                    url = (
                        "https://api.binance.com/sapi/v1/"
                        "simple-earn/flexible/history/rewardsRecord"
                        f"?{query}&signature={sig}"
                    )

                    headers = {"X-MBX-APIKEY": key}
                    res = requests.get(url, headers=headers, timeout=10)

                    if not res.headers.get("Content-Type", "").startswith("application/json"):
                        break

                    data = res.json()
                    rows = data.get("rows", [])
                    if not rows:
                        break

                    for r in rows:
                        if r.get("rewardType") != "REALTIME":
                            continue

                        id_ext = f"BN-EARN-{r['asset']}-{r['time']}"

                        cur.execute("""
                            INSERT IGNORE INTO transacciones_globales
                            (id_externo, user_id, exchange, cuenta_tipo, categoria,
                             asset, monto_neto, fecha_utc, descripcion)
                            VALUES
                            (%s,%s,'Binance','EARN','INTEREST',
                             %s,%s,FROM_UNIXTIME(%s/1000),'Binance Earn REALTIME')
                        """, (
                            id_ext,
                            user_id,
                            r["asset"],
                            float(r["rewards"]),
                            r["time"]
                        ))

                    current += 1

            except Exception as e:
                print("⚠️ Binance Earn REALTIME error:", e)


        # ================= BINANCE DEPOSITS — PAGINACIÓN COMPLETA =================
        if descargar_statements:
            try:
                ts = int(time.time() * 1000)
                current = 1
                size = 100
                while True:
                    query = f"timestamp={ts}&limit={size}&current={current}"
                    sig = hmac.new(sec.encode(), query.encode(), hashlib.sha256).hexdigest()
                    url = f"https://api.binance.com/sapi/v1/capital/deposit/hisrec?{query}&signature={sig}"
                    headers = {"X-MBX-APIKEY": key}

                    res = requests.get(url, headers=headers, timeout=10).json()
                    if not res or len(res) == 0:
                        break

                    for d in res:
                        id_ext = f"BN-DEPOSIT-{d.get('txId','NA')}-{d.get('insertTime')}"
                        
                        cur.execute("""
                            INSERT IGNORE INTO transacciones_globales
                            (id_externo, user_id, exchange, cuenta_tipo, categoria,
                             asset, monto_neto, fecha_utc, descripcion)
                            VALUES
                            (%s,%s,'Binance','SPOT','DEPOSIT',
                             %s,%s,FROM_UNIXTIME(%s/1000),'Binance Deposit')
                        """, (
                            id_ext,
                            user_id,
                            d.get('coin'),
                            float(d.get('amount',0)),
                            d.get('insertTime')
                        ))

                    if len(res) < size:
                        break  # última página
                    current += 1

            except Exception as e:
                print("⚠️ Deposits API error:", e)


        # ================= BINANCE WITHDRAWALS — PAGINACIÓN COMPLETA =================
        if descargar_statements:
            try:
                ts = int(time.time() * 1000)
                current = 1
                size = 100
                while True:
                    query = f"timestamp={ts}&limit={size}&current={current}"
                    sig = hmac.new(sec.encode(), query.encode(), hashlib.sha256).hexdigest()
                    url = f"https://api.binance.com/sapi/v1/capital/withdraw/history?{query}&signature={sig}"
                    headers = {"X-MBX-APIKEY": key}

                    data = requests.get(url, headers=headers, timeout=10).json()
                    withdraws = data.get('withdrawList') if isinstance(data, dict) else []
                    if not withdraws:
                        break

                    for w in withdraws:
                        id_ext = f"BN-WITHDRAW-{w.get('id','NA')}-{w.get('applyTime')}"

                        cur.execute("""
                            INSERT IGNORE INTO transacciones_globales
                            (id_externo, user_id, exchange, cuenta_tipo, categoria,
                             asset, monto_neto, fecha_utc, descripcion)
                            VALUES
                            (%s,%s,'Binance','SPOT','WITHDRAW',
                             %s,%s,FROM_UNIXTIME(%s/1000),'Binance Withdrawal')
                        """, (
                            id_ext,
                            user_id,
                            w.get('coin'),
                            -abs(float(w.get('amount',0))),
                            w.get('applyTime')
                        ))

                    if len(withdraws) < size:
                        break  # última página
                    current += 1

            except Exception as e:
                print("⚠️ Withdraw API error:", e)

        db.commit()

    except Exception as e:
        print(f"❌ Error Binance ID {user_id}: {e}")


# ==============================================================================
#   LÓGICA BINGX — NORMALIZADA Y CONTABLE
# ==============================================================================
def procesar_bingx(key, sec, user_id, db, session, descargar_statements):
    try:
        cur = db.cursor(dictionary=True)

        # ---------------- REQUEST BINGX ----------------
        def bingx_req(path, params=None):
            if params is None:
                params = {}
            params["timestamp"] = int(time.time() * 1000)
            qs = "&".join([f"{k}={params[k]}" for k in sorted(params)])
            sig = hmac.new(sec.encode(), qs.encode(), hashlib.sha256).hexdigest()
            url = f"https://open-api.bingx.com{path}?{qs}&signature={sig}"
            return session.get(url, headers={"X-BX-APIKEY": key}, timeout=10).json()

        # ================= SPOT SALDOS =================
        res_spot = bingx_req("/openApi/spot/v1/account/balance")
        if res_spot.get("code") == 0:
            for b in res_spot["data"]["balances"]:
                total = float(b["free"]) + float(b["locked"])
                if total > 0:
                    precio = obtener_precio_db(cur, b["asset"])
                    cur.execute("""
                        INSERT INTO sys_saldos_usuarios
                        (user_id, broker_name, tipo_cuenta, asset,
                         cantidad_total, valor_usd, last_update, is_active)
                        VALUES (%s,'BingX','SPOT',%s,%s,%s,NOW(),1)
                        ON DUPLICATE KEY UPDATE
                            cantidad_total=VALUES(cantidad_total),
                            valor_usd=VALUES(valor_usd),
                            last_update=NOW(),
                            is_active=1
                    """, (user_id, b["asset"], total, total * precio))

                    disparador_inteligente_pro(cur, user_id, b["asset"], total)

        # ================= PERPETUAL SALDOS =================
        res_swap = bingx_req("/openApi/swap/v2/user/balance")
        if res_swap.get("code") == 0:
            d = res_swap["data"]["balance"]
            wb = float(d["balance"])
            if wb != 0:
                cur.execute("""
                    INSERT INTO sys_saldos_usuarios
                    (user_id, broker_name, tipo_cuenta, asset,
                     cantidad_total, pnl_no_realizado, realised_profit,
                     equidad_neta, margen_disponible, margen_usado,
                     valor_usd, last_update, is_active)
                    VALUES (%s,'BingX','PERPETUAL',%s,%s,%s,%s,%s,%s,%s,%s,NOW(),1)
                """, (
                    user_id,
                    d["asset"],
                    wb,
                    float(d["unrealizedProfit"]),
                    float(d["realisedProfit"]),
                    float(d["equity"]),
                    float(d["availableMargin"]),
                    float(d["usedMargin"]),
                    float(d["equity"])
                ))

                disparador_inteligente_pro(cur, user_id, d["asset"], wb)

        # ================= TRADES (SOLO TRAZA) =================
        res_trades = bingx_req("/openApi/swap/v2/trade/order/history", {"limit": 5})
        if res_trades.get("code") == 0:
            for o in res_trades["data"]["orders"]:
                id_ext = f"BX-T-{o['orderId']}"

                cur.execute("""
                    INSERT IGNORE INTO transacciones_globales
                    (id_externo, user_id, exchange, cuenta_tipo,
                     categoria, asset, monto_neto, fecha_utc)
                    VALUES (%s,%s,'BingX','PERPETUAL',
                            'TRADE','USDT',0,
                            FROM_UNIXTIME(%s/1000))
                """, (id_ext, user_id, o["time"]))

                cur.execute("""
                    INSERT IGNORE INTO detalle_trades
                    (user_id, id_externo_ref, fecha_utc, symbol,
                     lado, precio_ejecucion, cantidad_ejecutada,
                     pnl_realizado, is_maker)
                    VALUES (%s,%s,FROM_UNIXTIME(%s/1000),
                            %s,%s,%s,%s,0,0)
                """, (
                    user_id,
                    id_ext,
                    o["time"],
                    o["symbol"],
                    o["side"],
                    float(o["price"]),
                    float(o["executedQty"])
                ))


        # ================= STATEMENTS / INCOME =================
        if descargar_statements:
            res_inc = bingx_req("/openApi/swap/v2/user/income", {"limit": 20})
            if res_inc.get("code") == 0:
                for i in res_inc["data"]:

                    tipo = i["incomeType"]

                    if tipo == "FUNDING":
                        categoria = "FUNDING_FEE"
                    elif tipo == "REALIZED_PNL":
                        categoria = "PNL_REALIZED"
                    elif tipo == "COMMISSION":
                        categoria = "COMMISSION"
                    elif tipo == "BONUS":
                        categoria = "BONUS"
                    elif tipo == "TRANSFER":
                        categoria = "TRANSFER_INTERNAL"
                    else:
                        categoria = "TRADE"

                    id_ext = f"BX-INC-{i['id']}"

                    cur.execute("""
                        INSERT IGNORE INTO transacciones_globales
                        (id_externo, user_id, exchange, cuenta_tipo,
                         categoria, asset, monto_neto,
                         fecha_utc, descripcion)
                        VALUES (%s,%s,'BingX','PERPETUAL',
                                %s,%s,%s,
                                FROM_UNIXTIME(%s/1000),%s)
                    """, (
                        id_ext,
                        user_id,
                        categoria,
                        i["asset"],
                        float(i["income"]),
                        i["time"],
                        categoria
                    ))

        db.commit()

    except Exception as e:
        print(f"❌ Error BingX ID {user_id}: {e}")



# ======================================================
# MOTOR
# ======================================================
def motor_maestro():
    print("🚀 MOTOR v1.5.0 — Binance Earn + Airdrops (REST)")
    session = requests.Session()
    ciclo = 0

    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cur = db.cursor(dictionary=True)
            cur.execute("SELECT user_id, broker_name, api_key, api_secret FROM api_keys WHERE status=1")
            usuarios = cur.fetchall()

            statements = (ciclo % CICLOS_PARA_STATEMENTS == 0)

            for u in usuarios:
                k = descifrar_dato(u['api_key'], MASTER_KEY)
                s = descifrar_dato(u['api_secret'], MASTER_KEY)

                if 'binance' in u['broker_name'].lower():
                    procesar_binance(k, s, u['user_id'], db, statements)

                if 'bingx' in u['broker_name'].lower():
                    procesar_bingx(k, s, u['user_id'], db, session, statements)

            db.close()
            print(f"--- Ciclo {ciclo} OK ---")
            ciclo += 1
            time.sleep(ESPERA_CICLO_RAPIDO)

        except Exception as e:
            print(f"🔥 Error Motor: {e}")
            time.sleep(30)

if __name__ == "__main__":
    motor_maestro()

