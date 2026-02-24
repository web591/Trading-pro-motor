import mysql.connector
from binance.client import Client
import time, os, base64
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# ==========================================================
# 🛡️ SEGURIDAD Y DESCIFRADO
# ==========================================================
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

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
# 📡 LÓGICA DEL RADAR (GEMA)
# ==========================================================

def normalizador_binance(raw_symbol):
    s = raw_symbol.upper().strip()
    categoria = 'SPOT'
    if s.startswith('LD'):
        categoria = 'LENDING'
        s = s[2:]
    ticker_base = s
    for suffix in ['USDT', 'USDC', 'BUSD', 'BTC', 'ETH']:
        if s.endswith(suffix) and s != suffix:
            ticker_base = s[:-len(suffix)]
            break
    return ticker_base.replace('-', '').replace('_', ''), categoria

def ejecutar_radar_gema(conexion_db, user_id, ticker_base, info_ctx):
    cursor = conexion_db.cursor(dictionary=True)
    
    # Verificamos si ya hay una tarea pendiente o procesándose para este ticker y usuario
    cursor.execute("""
        SELECT id FROM sys_simbolos_buscados 
        WHERE user_id = %s AND ticker = %s AND status NOT IN ('ignorado', 'confirmado')
    """, (user_id, ticker_base))
    
    if not cursor.fetchone():
        print(f"    🔍 RADAR: Detectado saldo de {ticker_base}. Creando tarea de búsqueda...")
        try:
            # Insertamos como 'pendiente' para que el MOTOR MAESTRO haga su magia
            sql = """
                INSERT INTO sys_simbolos_buscados (user_id, ticker, status, info) 
                VALUES (%s, %s, 'pendiente', %s)
            """
            cursor.execute(sql, (user_id, ticker_base, f"Saldo detectado en {info_ctx}"))
            conexion_db.commit()
        except mysql.connector.Error as err:
            if err.errno != 1062:
                print(f"      [!] Error en Radar: {err}")


# ==========================================================
# 🚀 PROCESO PRINCIPAL
# ==========================================================

def actualizar_saldos(conexion_db, user_id, api_key, api_secret):
    try:
        # Conectamos a Binance
        client = Client(api_key, api_secret)
        balances = client.get_account().get('balances', [])
        cursor = conexion_db.cursor(dictionary=True)
        
        for b in balances:
            total = float(b['free']) + float(b['locked'])
            if total > 0.000001:
                raw_asset = b['asset']
                ticker_base, categoria = normalizador_binance(raw_asset)
                
                # 1. Intentar vincular con Traductor existente
                cursor.execute("""
                    SELECT id, is_active FROM sys_traductor_simbolos 
                    WHERE ticker_motor = %s AND categoria_producto = %s LIMIT 1
                """, (raw_asset, categoria))
                res_trad = cursor.fetchone()
                
                t_id = res_trad['id'] if res_trad else None
                is_active = res_trad['is_active'] if res_trad else 0

                # 2. Si es un activo huérfano o inactivo, disparamos el RADAR
                stables = ['USDT', 'USDC', 'BUSD', 'DAI', 'USD']
                if ticker_base not in stables:
                    if not t_id or is_active == 0:
                        ejecutar_radar_gema(conexion_db, user_id, ticker_base, f"Binance {categoria}")

                # 3. Guardado/Actualización de Saldo (Aislado por user_id)
                sql_saldo = """
                    INSERT INTO sys_saldos_usuarios 
                    (user_id, asset, cantidad_total, traductor_id, last_update)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE 
                        cantidad_total = VALUES(cantidad_total),
                        traductor_id = IFNULL(VALUES(traductor_id), traductor_id),
                        last_update = NOW()
                """
                cursor.execute(sql_saldo, (user_id, raw_asset, total, t_id))

        conexion_db.commit()
        print(f"   [OK] User {user_id} actualizado.")
    except Exception as e:
        # Captura errores de API Key inválida y sigue con el siguiente usuario
        print(f" [!] Saltando User {user_id}: Error en conexión (¿Es de otro broker?): {e}")

# ==========================================================
# 🆕 BINANCE OPERATIVE EXTENSION v4.2.6
# ==========================================================

def binance_operativa_extension(conexion_db, user_id, api_key, api_secret):

    cursor = conexion_db.cursor(dictionary=True)

    try:

        client = Client(api_key, api_secret)

        # --------------------------------------------
        # SPOT OPEN ORDERS
        # --------------------------------------------
        oo = client.get_open_orders()

        cursor.execute("DELETE FROM sys_ordenes_abiertas WHERE user_id=%s AND exchange='BINANCE_SPOT'",(user_id,))

        for o in oo:

            cursor.execute("""
            INSERT INTO sys_ordenes_abiertas
            (user_id,exchange,symbol,side,type,price,amount,status,fecha_utc)
            VALUES (%s,'BINANCE_SPOT',%s,%s,%s,%s,%s,%s,
            %s)
            """,(
                user_id,
                o.get("symbol"),
                o.get("side"),
                o.get("type"),
                o.get("price"),
                o.get("origQty"),
                o.get("status"),
                normalizar_fecha_mysql(o.get("time"))
            ))

        # --------------------------------------------
        # FUTURES OPEN ORDERS
        # --------------------------------------------
        oo = client.futures_get_open_orders()

        cursor.execute("DELETE FROM sys_ordenes_abiertas WHERE user_id=%s AND exchange='BINANCE_FUT'",(user_id,))

        for o in oo:

            cursor.execute("""
            INSERT INTO sys_ordenes_abiertas
            (user_id,exchange,symbol,side,type,price,amount,status,fecha_utc)
            VALUES (%s,'BINANCE_FUT',%s,%s,%s,%s,%s,%s,
            %s)
            """,(
                user_id,
                o.get("symbol"),
                o.get("side"),
                o.get("type"),
                o.get("price"),
                o.get("origQty"),
                o.get("status"),
                normalizar_fecha_mysql(o.get("updateTime"))
            ))

        # --------------------------------------------
        # SPOT TRADES → detalle_trades
        # --------------------------------------------
        trades = client.get_my_trades()

        for t in trades:

            id_ext = f"BN-SP-{t.get('id')}"

            cursor.execute("""
            INSERT IGNORE INTO detalle_trades
            (user_id,exchange,tipo_producto,id_externo_ref,
            symbol,lado,precio_ejecucion,
            cantidad_ejecutada,fecha_utc)
            VALUES (%s,'BINANCE','SPOT',%s,%s,%s,%s,%s,
            %s)
            """,(
                user_id,
                id_ext,
                t.get("symbol"),
                t.get("side"),
                t.get("price"),
                t.get("qty"),
                normalizar_fecha_mysql(t.get("time"))
            ))

        # --------------------------------------------
        # FUTURES TRADES → detalle_trades
        # --------------------------------------------
        params = binance_backfill_params(conexion_db,user_id,'BINANCE')
        trades = client.futures_account_trades(**params)

        for t in trades:

            id_ext = f"BN-FUT-{t.get('id')}"

            cursor.execute("""
            INSERT IGNORE INTO detalle_trades
            (user_id,exchange,tipo_producto,id_externo_ref,
            symbol,lado,precio_ejecucion,
            cantidad_ejecutada,fecha_utc)
            VALUES (%s,'BINANCE','FUTURES',%s,%s,%s,%s,%s,
            %s)
            """,(
                user_id,
                id_ext,
                t.get("symbol"),
                t.get("side"),
                t.get("price"),
                t.get("qty"),
                normalizar_fecha_mysql(t.get("time"))
            ))

            net = float(t.get("qty",0))
            if t.get("side") == "SELL":
                net *= -1

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BINANCE','FUTURES',
            'TRADE',%s,%s,
            %s)
            """,(
                id_ext,
                user_id,
                t.get("symbol"),
                net,
                normalizar_fecha_mysql(t.get("time"))
            ))

        # --------------------------------------------
        # FUNDING FEES → GLOBAL LEDGER
        # --------------------------------------------
        inc = client.futures_income_history(limit=50)

        for i in inc:

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BINANCE','FUTURES',
            %s,%s,%s,
            %s)
            """,(
                f"BN-FUND-{i.get('tranId')}",
                user_id,
                i.get("incomeType"),
                i.get("asset"),
                float(i.get("income",0)),
                normalizar_fecha_mysql(i.get("time"))
            ))

        # --------------------------------------------
        # POSITION RISK SNAPSHOT
        # --------------------------------------------
        pos = client.futures_position_information()

        for p in pos:

            amt = float(p.get("positionAmt",0))
            if abs(amt) <= 0: continue

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BINANCE','FUTURES',
            'POSITION_SNAPSHOT',%s,%s,NOW())
            """,(
                f"BN-POS-{p.get('symbol')}",
                user_id,
                p.get("symbol"),
                amt
            ))

        conexion_db.commit()

    except Exception as e:
        print(f"❌ Binance OPERATIVE Error {user_id}: {e}")

# ==========================================================
# 🔐 ANEXO CONTABLE FUTURES v4.2.8
# Commission + ADL + Insurance
# ==========================================================
def actualizar_fees_futures_binance(client, conexion_db, user_id):

    cursor = conexion_db.cursor(dictionary=True)

    try:
        trades = client.futures_account_trades()

        for t in trades:

            fee = float(t.get('commission', 0))
            asset = t.get('commissionAsset')

            if fee == 0:
                continue

            # 🔍 RADAR si no existe traductor
            cursor.execute("""
                SELECT id FROM sys_traductor_simbolos
                WHERE ticker_motor = %s LIMIT 1
            """, (asset,))
            res = cursor.fetchone()

            if not res:
                ejecutar_radar_gema(conexion_db, user_id, asset, "Futures Commission")

            sql = """
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BINANCE','FUTURES',
            'FEE',%s,%s,NOW())
            """

            cursor.execute(sql, (
                f"BN-FUT-FEE-{t['id']}",
                user_id,
                asset,
                -abs(fee)
            ))

        conexion_db.commit()

    except Exception as e:
        print(f"[FUTURES FEES] {e}")


# ==========================================================
# 🔁 INTERNAL TRANSFER SPOT ⇄ FUTURES
# ==========================================================
def actualizar_transfers_binance(client, conexion_db, user_id):

    cursor = conexion_db.cursor(dictionary=True)

    try:
        transfers = client.get_asset_transfer_history()

        for t in transfers.get('rows', []):

            asset = t['asset']
            qty   = float(t['amount'])

            ref = f"BN-TRANS-{t['tranId']}"

            sql = """
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BINANCE','INTERNAL',
            'TRANSFER',%s,%s,NOW())
            """
            cursor.execute(sql,(
                ref,
                user_id,
                asset,
                qty
            ))

        conexion_db.commit()

    except Exception as e:
        print(f"[TRANSFER] {e}")





def binance_ledger_extension(conexion_db, user_id, api_key, api_secret):

    cursor = conexion_db.cursor(dictionary=True)

    try:

        client = Client(api_key, api_secret)

        # --------------------------------------------
        # DEPOSITS
        # --------------------------------------------
        params = binance_backfill_params(conexion_db,user_id,'BINANCE')
        dep = client.get_deposit_history(**params)
        wd  = client.get_withdraw_history(**params)

        for d in dep:

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BINANCE','SPOT',
            'DEPOSIT',%s,%s,
            %s)
            """,(
                f"BN-DEP-{d.get('txId')}",
                user_id,
                d.get("coin"),
                float(d.get("amount",0)),
                normalizar_fecha_mysql(d.get("insertTime"))
            ))

        # --------------------------------------------
        # WITHDRAWALS
        # --------------------------------------------
        wd = client.get_withdraw_history()

        for w in wd:

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BINANCE','SPOT',
            'WITHDRAW',%s,%s,
            %s)
            """,(
                f"BN-WD-{w.get('id')}",
                user_id,
                w.get("coin"),
                -float(w.get("amount",0)),
                normalizar_fecha_mysql(w.get("applyTime"))
            ))

        # --------------------------------------------
        # EARN INTEREST
        # --------------------------------------------
        earn = client.get_lending_interest_history()

        for e in earn:

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BINANCE','EARN',
            'INTEREST',%s,%s,
            %s)
            """,(
                f"BN-EARN-{e.get('tranId')}",
                user_id,
                e.get("asset"),
                float(e.get("interest",0)),
                normalizar_fecha_mysql(e.get("time"))
            ))

        # --------------------------------------------
        # DUST CONVERSION
        # --------------------------------------------
        dust = client.get_dust_log()

        for d in dust.get("userAssetDribblets",[]):

            for det in d.get("userAssetDribbletDetails",[]):

                cursor.execute("""
                INSERT IGNORE INTO transacciones_globales
                (id_externo,user_id,exchange,cuenta_tipo,
                categoria,asset,monto_neto,fecha_utc)
                VALUES (%s,%s,'BINANCE','SPOT',
                'DUST',%s,%s,
                %s)
                """,(
                    f"BN-DUST-{det.get('transId')}",
                    user_id,
                    det.get("fromAsset"),
                    -float(det.get("amount",0)),
                    normalizar_fecha_mysql(det.get("operateTime"))
                ))

        # --------------------------------------------
        # SPOT FEES (REAL FILL COMMISSION)
        # --------------------------------------------
        trades = client.get_my_trades()

        for t in trades:

            fee = float(t.get("commission",0))
            if fee <= 0: continue

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BINANCE','SPOT',
            'FEE',%s,%s,
            %s)
            """,(
                f"BN-FEE-{t.get('id')}",
                user_id,
                t.get("commissionAsset"),
                -fee,
                normalizar_fecha_mysql(t.get("time"))
            ))

        conexion_db.commit()

    except Exception as e:
        print(f"❌ Binance LEDGER Error {user_id}: {e}")

# ==========================================================
# 🆕 MINING / LAUNCHPOOL / STAKING / AIRDROP
# Version 4.3.1
# ==========================================================
def actualizar_rewards_binance(client, conexion_db, user_id):

    cursor = conexion_db.cursor(dictionary=True)

    try:

        rewards = client.get_asset_dividend_history()

        for r in rewards.get("rows", []):

            asset = r.get("asset")
            amount = float(r.get("amount",0))
            divType = r.get("dividendType","UNKNOWN")
            tranId = r.get("tranId")

            if amount == 0:
                continue

            # -----------------------------
            # NORMALIZADOR CONTABLE
            # -----------------------------
            categoria = "MINING_REWARD"

            if "airdrop" in divType.lower():
                categoria = "AIRDROP"

            if "cash" in divType.lower():
                categoria = "CASHBACK"

            if "staking" in divType.lower():
                categoria = "INTEREST"

            if "launchpool" in divType.lower():
                categoria = "MINING_REWARD"

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BINANCE','EARN',
            %s,%s,%s,
            %s)
            """,(
                f"BN-RWD-{tranId}",
                user_id,
                categoria,
                asset,
                amount,
                normalizar_fecha_mysql(r.get("divTime"))
            ))

        conexion_db.commit()

    except Exception as e:
        print(f"[REWARDS] {e}")

# ==========================================================
# 🆕 UNIVERSAL TRANSFER (SPOT ⇄ FUTURES ⇄ MARGIN)
# v4.2.9
# ==========================================================
def actualizar_universal_transfer_binance(client,conexion_db,user_id):

    cursor=conexion_db.cursor(dictionary=True)

    try:
        data=client.get_universal_transfer_history()

        for t in data.get("rows",[]):

            asset=t["asset"]
            qty=float(t["amount"])

            if t.get("type") in ["MAIN_UMFUTURE","SPOT_UMFUTURE","MARGIN_UMFUTURE"]:
                qty *= -1

            ref=f"BN-UNI-{t['tranId']}"

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BINANCE','INTERNAL',
            'UNIVERSAL_TRANSFER',%s,%s,NOW())
            """,(ref,user_id,asset,qty))

        conexion_db.commit()

    except Exception as e:
        print(f"[UNI TRANS] {e}")





# ==========================================================
# 🆕 BINGX CORE EXTENSION v4.2.4
# ==========================================================
import hmac, hashlib, requests, json

BINGX_BASE = "https://open-api.bingx.com"

def bx_now():
    return int(time.time() * 1000)

def bx_sign(params, sec):
    params["timestamp"] = bx_now()
    qs = "&".join(f"{k}={params[k]}" for k in sorted(params))
    sig = hmac.new(sec.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs, sig

def bx_req(key, sec, path, params=None):
    params = params or {}
    qs, sig = bx_sign(params, sec)
    url = f"{BINGX_BASE}{path}?{qs}&signature={sig}"
    return requests.get(url, headers={'X-BX-APIKEY': key}).json()

# ==========================================================
# 🆕 BINGX CORE EXTENSION v4.2.4
# ==========================================================

def actualizar_bingx_extension(conexion_db, user_id, api_key, api_secret):
    cursor = conexion_db.cursor(dictionary=True)

    def guardar(asset, cantidad, tipo):
        if float(cantidad) <= 0: return
        
        cursor.execute("""
            SELECT id FROM sys_traductor_simbolos 
            WHERE ticker_motor=%s LIMIT 1
        """,(asset,))
        trad = cursor.fetchone()
        tid = trad['id'] if trad else None
        
        if not tid:
            ejecutar_radar_gema(conexion_db, user_id, asset, f"BingX {tipo}")
        
        cursor.execute("""
        INSERT INTO sys_saldos_usuarios 
        (user_id, broker_name, tipo_cuenta, asset, traductor_id, cantidad_total, last_update)
        VALUES (%s,'BingX',%s,%s,%s,%s,NOW())
        ON DUPLICATE KEY UPDATE 
        cantidad_total=VALUES(cantidad_total),
        traductor_id=IFNULL(VALUES(traductor_id),traductor_id),
        last_update=NOW()
        """,(user_id,tipo,asset,tid,cantidad))

    try:
        # SPOT
        r = bx_req(api_key,api_secret,"/openApi/spot/v1/account/balance")
        for b in r.get("data",{}).get("balances",[]):
            guardar(b.get("asset"),float(b.get("free",0))+float(b.get("locked",0)),"SPOT")

        # FUTURES STANDARD
        r = bx_req(api_key,api_secret,"/openApi/futures/v1/account/balance")
        for b in r.get("data",[]):
            guardar(b.get("asset"),b.get("balance"),"FUTURES_STD")

        # PERP WALLET
        r = bx_req(api_key,api_secret,"/openApi/swap/v2/user/balance")
        bal=r.get("data",{}).get("balance")
        if isinstance(bal,dict):
            guardar(bal.get("asset"),bal.get("balance"),"PERP")

        # PERP POSITIONS
        r = bx_req(api_key,api_secret,"/openApi/swap/v2/user/positions")
        for p in r.get("data",[]):
            guardar(p.get("symbol"),p.get("positionAmt"),"PERP_POSITION")

        # WALLET FLOW → LEDGER
        r = bx_req(api_key,api_secret,"/openApi/swap/v2/user/income",{"limit":100})
        data=r.get("data")
        items=data.get("list",[]) if isinstance(data,dict) else data
        
        for i in items:

            categoria_norm = normalizar_categoria_bingx(i.get("type"))

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,categoria,
            asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BingX','FUTURES',%s,%s,%s,
            %s)
            """,(
                f"BX-{i.get('tranId')}",
                user_id,
                categoria_norm,
                i.get("asset"),
                float(i.get("income",0)),
                normalizar_fecha_mysql(i.get("time"))
            ))

        conexion_db.commit()
    except Exception as e:
        print(f"❌ BingX EXT Error {user_id}: {e}")


               
# ==========================================================
# 🧠 NORMALIZADOR CONTABLE BINGX WALLET FLOW
# v4.3.3
# ==========================================================
def normalizar_categoria_bingx(tipo_raw):

    if not tipo_raw:
        return "UNKNOWN"

    t = tipo_raw.upper()

    MAP = {
        "REALIZED_PNL": "TRADE_PNL",
        "FUNDING_FEE": "FUNDING",
        "COMMISSION": "FEE",
        "WELCOME_BONUS": "AIRDROP",
        "AIR_DROP": "AIRDROP",
        "REBATE": "CASHBACK",
        "INSURANCE_CLEAR": "COMPENSATION",
        "ADL": "COMPENSATION",
        "BONUS": "AIRDROP",
        "MINING_REWARD": "MINING_REWARD"
    }

    return MAP.get(t, "UNKNOWN")


# ==========================================================
# 🆕 BINGX SPOT FILLS
# ==========================================================
def bingx_spot_trades_ledger(api_key,api_secret,conexion_db,user_id):

    cursor=conexion_db.cursor(dictionary=True)

    try:

        r=bx_req(api_key,api_secret,
        "/openApi/spot/v1/trade/myTrades",{"limit":100})

        for t in r.get("data",[]):

            net = float(t.get("qty",0))

            if t.get("side") == "SELL":
                net *= -1

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BingX','SPOT',
            'TRADE',%s,%s,
            %s)
            """,(
                f"BX-SP-{t['id']}",
                user_id,
                t.get("symbol"),
                net,
                normalizar_fecha_mysql(t.get("time"))
            ))

            fee=float(t.get("commission",0))

            if fee<=0:
                continue

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BingX','SPOT',
            'FEE',%s,%s,
            %s)
            """,(
                f"BX-SP-FEE-{t['id']}",
                user_id,
                t["commissionAsset"],
                -fee,
                normalizar_fecha_mysql(t["time"])
            ))

        conexion_db.commit()

    except Exception as e:
        print(f"[BX SPOT FEE] {e}")

# ==========================================================
# 🆕 BINGX OPERATIVE EXTENSION v4.2.5
# ==========================================================

def bingx_operativa_extension(conexion_db, user_id, api_key, api_secret):
    cursor = conexion_db.cursor(dictionary=True)

    try:

        # --------------------------------------------
        # SPOT OPEN ORDERS
        # --------------------------------------------
        r = bx_req(api_key,api_secret,"/openApi/spot/v1/trade/openOrders")
        cursor.execute("DELETE FROM sys_ordenes_abiertas WHERE user_id=%s AND exchange='BingX_SPOT'",(user_id,))
        
        for o in r.get("data",{}).get("orders",[]):
            cursor.execute("""
            INSERT INTO sys_ordenes_abiertas
            (user_id,exchange,symbol,side,type,price,amount,status,fecha_utc)
            VALUES (%s,'BingX_SPOT',%s,%s,%s,%s,%s,%s,
            %s)
            """,(
                user_id,
                o.get("symbol"),
                o.get("side"),
                o.get("type"),
                o.get("price"),
                o.get("origQty"),
                o.get("status"),
                normalizar_fecha_mysql(o.get("time"))
            ))

        # --------------------------------------------
        # FUTURES OPEN ORDERS
        # --------------------------------------------
        r = bx_req(api_key,api_secret,"/openApi/swap/v2/trade/openOrders")
        cursor.execute("DELETE FROM sys_ordenes_abiertas WHERE user_id=%s AND exchange='BingX_FUT'",(user_id,))
        
        for o in r.get("data",{}).get("orders",[]):
            cursor.execute("""
            INSERT INTO sys_ordenes_abiertas
            (user_id,exchange,symbol,side,type,price,amount,status,fecha_utc)
            VALUES (%s,'BingX_FUT',%s,%s,'LIMIT',%s,%s,%s,
            %s)
            """,(
                user_id,
                o.get("symbol"),
                o.get("side"),
                o.get("price"),
                o.get("origQty"),
                o.get("status"),
                normalizar_fecha_mysql(o.get("updateTime"))
            ))

        # --------------------------------------------
        # FUTURES TRADES → detalle_trades
        # --------------------------------------------
        r = bx_req(api_key,api_secret,"/openApi/swap/v2/trade/allOrders",{"limit":50})
        
        for t in r.get("data",{}).get("orders",[]):

            id_ext = f"BX-TR-{t.get('orderId')}"

            cursor.execute("""
            INSERT IGNORE INTO detalle_trades
            (user_id,exchange,tipo_producto,id_externo_ref,
            symbol,lado,precio_ejecucion,
            cantidad_ejecutada,fecha_utc)
            VALUES (%s,'BINGX','PERP',%s,%s,%s,%s,%s,
            %s)
            """,(
                user_id,
                id_ext,
                t.get("symbol"),
                t.get("side"),
                t.get("price"),
                t.get("origQty"),
                normalizar_fecha_mysql(t.get("updateTime"))
            ))

            # --------------------------------------------
            # → GLOBAL LEDGER
            # --------------------------------------------
            net = float(t.get("origQty",0))
            if t.get("side") == "SELL":
                net *= -1

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BingX','FUTURES',
            'TRADE',%s,%s,
            %s)
            """,(
                id_ext,
                user_id,
                t.get("symbol"),
                net,
                normalizar_fecha_mysql(t.get("updateTime"))
            ))

        conexion_db.commit()

    except Exception as e:
        print(f"❌ BingX OPERATIVE Error {user_id}: {e}")

# ==========================================================
# Version 4.2.12
# BINGX WALLET FLOWS + BACKFILL DINAMICO
# ANEXO CONTABLE - NO MODIFICA VERSIONES ANTERIORES
# ==========================================================

def obtener_inicio_dinamico(cursor, user_id):
    cursor.execute("""
        SELECT UNIX_TIMESTAMP(MAX(fecha_utc))*1000
        FROM transacciones_globales
        WHERE exchange = 'BINGX'
        AND user_id = %s
    """, (user_id,))
    
    last = cursor.fetchone()[0]
    
    if last:
        return int(last)
    else:
        return int((time.time() - 1440*60)*1000)



# ==========================================================
# RETIROS
# ==========================================================

def bingx_withdraws(session, db, user_id):

    inicio = obtener_inicio_dinamico(db.cursor(), user_id)

    endpoint = "/openApi/wallets/v1/capital/withdraw/history"
    params = {"startTime": inicio}

    data = session.get(endpoint, params=params).json()

    for w in data.get("data", []):

        db.cursor().execute("""
        INSERT INTO transacciones_globales
        (user_id, id_externo, exchange, categoria, subtipo,
        activo, monto, fecha, procedencia)
        VALUES (%s,%s,'BINGX','WALLET_FLOW','WITHDRAW',
        %s,%s,%s,'SPOT')
        ON DUPLICATE KEY UPDATE monto = VALUES(monto)
        """,
        (user_id,
         f"BX-WDR-{w['txId']}",
         w['coin'],
         float(w['amount']),
         normalizar_fecha_mysql(w['applyTime'])
        ))

    db.commit()
# ==========================================================
# Version 4.2.12
# ANEXO CONTABLE — TRANSFERENCIAS INTERNAS BINANCE
# FUTURES ↔ SPOT
# ==========================================================

def binance_internal_transfer_ledger(conn, user_id, api_key, api_secret):

    endpoint = "/sapi/v1/asset/transfer"
    timestamp = int(time.time() * 1000)

    params = {
        "timestamp": timestamp
    }

    query_string = urlencode(params)
    signature = hmac.new(
        api_secret.encode(),
        query_string.encode(),
        hashlib.sha256
    ).hexdigest()

    url = f"https://api.binance.com{endpoint}?{query_string}&signature={signature}"

    headers = {
        "X-MBX-APIKEY": api_key
    }

    r = requests.get(url, headers=headers)
    data = r.json()

    cursor = conn.cursor()

    for tx in data.get("rows", []):

        asset = tx["asset"]
        amount = float(tx["amount"])
        txId = tx["tranId"]
        timestamp = int(tx["timestamp"])

        from_acc = tx["fromAccountType"]
        to_acc = tx["toAccountType"]

        categoria = f"TRANSFER_{from_acc}_TO_{to_acc}"

        cursor.execute("""
        INSERT IGNORE INTO transacciones_globales
        (id_externo,user_id,exchange,cuenta_tipo,
        categoria,asset,monto_neto,fecha_utc)
        VALUES (%s,%s,'BINANCE','INTERNAL',
        %s,%s,%s,
        %s)
        """,(
            f"BN-TRANSFER-{txId}",
            user_id,
            categoria,
            asset,
            amount,
            normalizar_fecha_mysql(timestamp)
        ))

    conn.commit()



# ==========================================================
# Version 4.2.12
# ANEXO CONTABLE — RETIROS BINGX
# ==========================================================

def bingx_withdraw_ledger(conn, user_id, api_key, secret):

    endpoint = "/openApi/wallets/v1/capital/withdraw/history"

    params = {
        "timestamp": int(time.time()*1000)
    }

    query = urlencode(params)
    sign = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()

    url = f"https://open-api.bingx.com{endpoint}?{query}&signature={sign}"

    headers = {"X-BX-APIKEY": api_key}

    r = requests.get(url, headers=headers)
    data = r.json()

    cursor = conn.cursor()

    for tx in data.get("data", []):

        asset = tx["coin"]
        amount = float(tx["amount"])
        txId = tx["txId"]
        time_ = int(tx["applyTime"])

        cursor.execute("""
        INSERT IGNORE INTO transacciones_globales
        (id_externo,user_id,exchange,cuenta_tipo,
        categoria,asset,monto_neto,fecha_utc)
        VALUES (%s,%s,'BINGX','SPOT',
        'WITHDRAW',%s,%s,
        %s)
        """,(
            f"BX-WITH-{txId}",
            user_id,
            asset,
            -amount,
            normalizar_fecha_mysql(time_)
        ))

    conn.commit()

# ==========================================================
# BINGX WALLET FLOW UNIFICADO v4.4.2
# SIN DUPLICADOS
# ==========================================================
def bingx_wallet_flow_ledger(conexion_db,user_id,api_key,api_secret):

    cursor=conexion_db.cursor(dictionary=True)

    try:

        delta=obtener_backfill_desde_ledger(conexion_db,user_id)

        r=bx_req(api_key,api_secret,
        "/openApi/swap/v2/user/income",
        {"startTime":int(time.time()*1000)-delta})

        data=r.get("data")
        items=data.get("list",[]) if isinstance(data,dict) else data

        for i in items:

            categoria_raw = i.get("type")
            categoria = normalizar_categoria_evento(categoria_raw)

            ref=f"BX-INC-{i.get('tranId')}"

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BINGX','FUTURES',
            %s,%s,%s,
            %s)
            """,(
                ref,
                user_id,
                categoria,
                i.get("asset"),
                float(i.get("income",0)),
                normalizar_fecha_mysql(i.get("time"))
            ))

        conexion_db.commit()

    except Exception as e:
        print(f"BingX WALLET FLOW Error {user_id}: {e}")

# ==========================================================
# 🆕 BINGX WITHDRAW → LEDGER v4.2.12
# ==========================================================
def bingx_withdraw_ledger(conexion_db,user_id,api_key,api_secret):

    cursor=conexion_db.cursor(dictionary=True)

    try:

        delta=obtener_backfill_desde_ledger(conexion_db,user_id)

        r=bx_req(api_key,api_secret,
        "/openApi/wallets/v1/withdraw/history",
        {"startTime":int(time.time()*1000)-delta})

        for d in r.get("data",[]):

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BingX','WALLET',
            'WITHDRAW',%s,%s,
            %s)
            """,(
                f"BX-WD-{d.get('txId')}",
                user_id,
                d.get("coin"),
                -abs(float(d.get("amount",0))),
                normalizar_fecha_mysql(d.get("time"))
            ))

        conexion_db.commit()

    except Exception as e:
        print(f"BingX WD Error {user_id}: {e}")

# ==========================================================
# 🆕 BINGX INTERNAL TRANSFER v4.2.12
# ==========================================================
def bingx_internal_transfer_ledger(conexion_db,user_id,api_key,api_secret):

    cursor=conexion_db.cursor(dictionary=True)

    try:

        delta=obtener_backfill_desde_ledger(conexion_db,user_id)

        r=bx_req(api_key,api_secret,
        "/openApi/wallets/v1/transfer/history",
        {"startTime":int(time.time()*1000)-delta})

        for t in r.get("data",[]):

            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,
            categoria,asset,monto_neto,fecha_utc)
            VALUES (%s,%s,'BingX','TRANSFER',
            'INTERNAL_TRANSFER',%s,%s,
            %s)
            """,(
                f"BX-TRF-{t.get('tranId')}",
                user_id,
                t.get("coin"),
                float(t.get("amount",0)),
                normalizar_fecha_mysql(t.get("time"))
            ))

        conexion_db.commit()

    except Exception as e:
        print(f"BingX TRF Error {user_id}: {e}")

# ==========================================================
# 🆕 CONCILIACION CONTABLE v4.2.11
# ==========================================================
def conciliacion_contable(conexion_db, user_id):

    cursor = conexion_db.cursor(dictionary=True)

    try:

        cursor.execute("""
        SELECT asset, broker_name, cantidad_total
        FROM sys_saldos_usuarios
        WHERE user_id=%s
        """,(user_id,))

        saldos = cursor.fetchall()

        for s in saldos:

            asset = s["asset"]
            exch  = s["broker_name"]
            snapshot = float(s["cantidad_total"])

            cursor.execute("""
            SELECT IFNULL(SUM(monto_neto),0) AS ledger
            FROM transacciones_globales
            WHERE user_id=%s
            AND exchange=%s
            AND asset=%s
            """,(user_id,exch,asset))

            ledger = float(cursor.fetchone()["ledger"])

            diff = snapshot - ledger

            status = "OK"
            if abs(diff) > 0.00001:
                status = "ALERTA"

            cursor.execute("""
            INSERT INTO sys_conciliacion_saldos
            (user_id,asset,exchange,
            saldo_snapshot,saldo_ledger,
            diferencia,status)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,(
                user_id,
                asset,
                exch,
                snapshot,
                ledger,
                diff,
                status
            ))

        conexion_db.commit()

    except Exception as e:
        print(f"[CONCILIACION] {e}")

# ==========================================================
# 🆕 BACKFILL DINÁMICO v4.2.12
# ==========================================================
def obtener_backfill_desde_ledger(conexion_db,user_id):

    cursor = conexion_db.cursor()

    cursor.execute("""
    SELECT MAX(fecha_utc)
    FROM transacciones_globales
    WHERE user_id=%s
    """,(user_id,))

    r=cursor.fetchone()

    if not r or not r[0]:
        return int(time.time()*1000)-(7*24*60*60*1000)

    last=int(r[0].timestamp()*1000)
    now=int(time.time()*1000)

    return now-last
# ==========================================================
# Version 4.2.13
# BACKFILL DINAMICO REAL BINANCE
# ==========================================================

def binance_backfill_params(conn,user_id,exchange):

    start = obtener_last_tx_time(conn,user_id,exchange)

    return {
        "startTime": start,
        "limit":1000
    }

from datetime import datetime

# ==========================================================
# 🕒 NORMALIZADOR UNIVERSAL DE FECHAS v4.2.13
# ==========================================================

def normalizar_fecha_mysql(fecha_raw):

    if not fecha_raw:
        return None

    try:

        # Epoch ms → Binance / BingX
        if isinstance(fecha_raw,(int,float)):
            if fecha_raw > 9999999999:
                fecha = datetime.utcfromtimestamp(fecha_raw/1000)
            else:
                fecha = datetime.utcfromtimestamp(fecha_raw)

        # String CSV o Withdraw Binance
        elif isinstance(fecha_raw,str):
            try:
                fecha = datetime.strptime(fecha_raw,"%Y-%m-%d %H:%M:%S")
            except:
                fecha = datetime.fromisoformat(fecha_raw)

        elif isinstance(fecha_raw,datetime):
            fecha = fecha_raw
        else:
            return None

        # 🔒 NORMALIZADOR CONTABLE
        return fecha.replace(microsecond=0)

    except Exception as e:
        print(f"[FECHA ERROR] {fecha_raw} → {e}")
        return None


def normalizar_fecha_mysql(ts):

    if not ts:
        return None

    try:
        if ts > 1e12:
            ts = ts/1000
        return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts))
    except:
        return None

# ==========================================================
# NORMALIZADOR CONTABLE UNIFICADO v5.0
# BINANCE + BINGX + CSV
# ==========================================================

def normalizar_categoria_evento(cat_raw):

    if not cat_raw:
        return "OTRO"

    c = str(cat_raw).upper().strip()

    MAP = {

        # =========================
        # OPERACION
        # =========================
        "TRADE"            : "OPERACION",
        "REALIZED_PNL"     : "INGRESO_FINANCIERO",
        "TRADE_PNL"        : "INGRESO_FINANCIERO",

        # =========================
        # COSTOS
        # =========================
        "FEE"              : "COSTO_OPERATIVO",
        "TRADING_FEE"      : "COSTO_OPERATIVO",
        "COMMISSION"       : "COSTO_OPERATIVO",
        "COMMISSION_IN"    : "COSTO_OPERATIVO",

        # =========================
        # CAPITAL
        # =========================
        "DEPOSIT"          : "ENTRADA",
        "CAPITAL_IN"       : "ENTRADA",
        "WITHDRAW"         : "SALIDA",

        # =========================
        # TRANSFER
        # =========================
        "TRANSFER"           : "TRANSFERENCIA_INTERNA",
        "UNIVERSAL_TRANSFER" : "TRANSFERENCIA_INTERNA",
        "INTERNAL_TRANSFER"  : "TRANSFERENCIA_INTERNA",
        "P2P_TRANSFER"       : "TRANSFERENCIA_INTERNA",

        # =========================
        # RIESGO
        # =========================
        "INSURANCE_CLEAR"  : "AJUSTE_RIESGO",
        "AUTO_DELEVERAGING": "AJUSTE_RIESGO",
        "LIQUIDATION"      : "AJUSTE_RIESGO",
        "COMPENSATION"     : "AJUSTE_RIESGO",
        "ADL"              : "AJUSTE_RIESGO",

        # =========================
        # INGRESOS
        # =========================
        "FUNDING_FEE"      : "INGRESO_FINANCIERO",
        "FUNDING"          : "INGRESO_FINANCIERO",

        # =========================
        # NO OPERATIVOS
        # =========================
        "AIRDROP"          : "INGRESO_NO_OPERATIVO",
        "WELCOME_BONUS"    : "INGRESO_NO_OPERATIVO",
        "BONUS"            : "INGRESO_NO_OPERATIVO",
        "MINING_REWARD"    : "INGRESO_NO_OPERATIVO",
        "POOL"             : "INGRESO_NO_OPERATIVO",
        "LAUNCHPOOL"       : "INGRESO_NO_OPERATIVO",
        "SAVINGS"          : "INGRESO_NO_OPERATIVO",
        "INTEREST"         : "INGRESO_NO_OPERATIVO",
        "REBATE"           : "INGRESO_NO_OPERATIVO",
        "CASHBACK"         : "INGRESO_NO_OPERATIVO",

        # =========================
        # TECNICO
        # =========================
        "DUST"             : "AJUSTE_TECNICO",
        "POSITION_SNAPSHOT": "POSICION"
    }

    return MAP.get(c, "OTRO")

def iniciar_motor():
    print(f"💎 GEMA v4.4.2 - RADAR + OPERATIVE PRUEBA 1")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            
            # ✅ CORRECCIÓN: Agregamos broker_name al SELECT para que el IF funcione
            cursor.execute("""
                SELECT user_id, api_key, api_secret, broker_name 
                FROM api_keys 
                WHERE status=1 AND UPPER(broker_name) IN ('BINANCE','BINGX') 
            """)
            usuarios = cursor.fetchall()
            
            for u in usuarios:
                print(f" -> Analizando User {u['user_id']} en {u['broker_name']}...")

                k = descifrar_dato(u['api_key'], MASTER_KEY)
                s = descifrar_dato(u['api_secret'], MASTER_KEY)

                if not k or not s:
                    print(f" [!] Error al descifrar llaves para User {u['user_id']}")
                    continue

                # Normalizamos el nombre a Mayúsculas para evitar errores de escritura
                broker_actual = u.get('broker_name','').upper()

                # ==================================================
                # 🟦 BINGX
                # ==================================================
                if broker_actual == 'BINGX':
                    actualizar_bingx_extension(db, u['user_id'], k, s)
                    bingx_operativa_extension(db, u['user_id'], k, s)
                    bingx_spot_trades_ledger(k, s, db, u['user_id'])
                    
                    # 👉 NUEVOS LEDGERS BINGX (Depósitos, Retiros y Transferencias)
                    bingx_wallet_flow_ledger(db, u['user_id'], k, s)
                    bingx_withdraw_ledger(db, u['user_id'], k, s)
                    bingx_internal_transfer_ledger(db, u['user_id'], k, s)
                    
                    conciliacion_contable(db, u['user_id'])

                # ==================================================
                # 🟨 BINANCE
                # ==================================================
                elif broker_actual == 'BINANCE':
                    actualizar_saldos(db, u['user_id'], k, s)
                    binance_operativa_extension(db, u['user_id'], k, s)
                    binance_ledger_extension(db, u['user_id'], k, s)
                    actualizar_rewards_binance(client, db, u['user_id'])

                    client = Client(k, s)

                    actualizar_fees_futures_binance(client, db, u['user_id'])
                    
                    actualizar_transfers_binance(client, db, u['user_id'])
                    actualizar_universal_transfer_binance(client, db, u['user_id'])
                    

                    # 👉 INTERNAL SPOT ⇄ FUTURES BINANCE
                    binance_internal_transfer_ledger(db, u['user_id'], k, s)

                    conciliacion_contable(db, u['user_id'])
            
            db.close()
        except Exception as e:
            print(f" [CRITICAL] Error en bucle principal: {e}")
        
        espera = getattr(config, 'ESPERA_CICLO_RAPIDO', 60)
        print(f"Ciclo terminado. Esperando {espera}s...")
        time.sleep(espera)

if __name__ == "__main__":
    iniciar_motor()