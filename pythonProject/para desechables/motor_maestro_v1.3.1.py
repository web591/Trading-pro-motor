import mysql.connector
from binance.client import Client
import time, sys, os, base64, hmac, requests, hashlib, json
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# --- CONFIGURACIÓN DE TIEMPOS ---
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

ESPERA_CICLO_RAPIDO = 120   # 2 minutos (Saldos, Open Orders y Trades)
CICLOS_PARA_STATEMENTS = 240 # 240 * 2 min = 8 Horas (Funding, Intereses, etc.)

# ==============================================================================
#   FUNCIONES DE APOYO (PRECIOS Y SEGURIDAD)
# ==============================================================================
def descifrar_dato(t, m):
    try:
        r = base64.b64decode(t.strip())
        p = r.split(b"::")
        c = AES.new(sha256(m.encode()).digest(), AES.MODE_CBC, p[1])
        return unpad(c.decrypt(p[0]), AES.block_size).decode().strip()
    except: return None

def obtener_precio_db(cursor, asset):
    stables = ['USDT', 'USDC', 'BUSD', 'DAI', 'FDUSD']
    if asset.upper() in stables: return 1.0
    try:
        cursor.execute("SELECT price FROM sys_precios_activos WHERE symbol = %s OR symbol = %s LIMIT 1", (asset.upper(), f"{asset.upper()}USDT"))
        res = cursor.fetchone()
        return float(res['price']) if res else 0.0
    except: return 0.0

# ==============================================================================
#   SECCIÓN DE CONTROL (IS_ACTIVE / WATCHLIST / DISPARADOR)
# ==============================================================================
def disparador_inteligente_pro(cursor, user_id, broker, asset, cantidad, es_operativa=False):
    """
    Sincroniza el motor de saldos con el motor de precios y la watchlist.
    """
    stables = ['USDT', 'USDC', 'BUSD', 'DAI', 'FDUSD']
    asset = asset.upper()

    # 1. Ignorar Stables (Ya tienen valor 1 fijo)
    if asset in stables:
        return 

    # 2. Verificar si el activo ya es conocido por el sistema (tiene precio)
    cursor.execute("SELECT id_precio FROM sys_precios_activos WHERE symbol = %s OR symbol = %s LIMIT 1", (f"{asset}USDT", asset))
    existe_en_precios = cursor.fetchone()

    # 3. Lógica de Activación (Si tiene saldo o posición abierta)
    if cantidad != 0:
        if not existe_en_precios:
            # Si no existe precio, lo mandamos a buscar (Dispara al PRICE_SYNC)
            print(f"   🔍 Nuevo activo detectado con saldo: {asset}. Solicitando búsqueda...")
            sql_bus = "INSERT IGNORE INTO sys_simbolos_buscados (ticker, status, fecha_registro) VALUES (%s, 'pendiente', NOW())"
            cursor.execute(sql_bus, (asset,))
        
        # En ambos casos, actualizamos sys_usuarios_activos a ACTIVO
        sql_act = """INSERT INTO sys_usuarios_activos 
                     (user_id, broker_name, asset, tipo_lista, is_active, last_check) 
                     VALUES (%s, %s, %s, 'ACTIVOS', 1, NOW())
                     ON DUPLICATE KEY UPDATE tipo_lista='ACTIVOS', is_active=1, last_check=NOW()"""
        cursor.execute(sql_act, (user_id, broker, asset))

    # 4. Lógica de Desactivación (Si la operativa cerró la posición y saldo es 0)
    elif es_operativa and cantidad == 0:
        # Lo pasamos a HISTORICOS
        sql_hist = """UPDATE sys_usuarios_activos 
                      SET tipo_lista = 'HISTORICOS', is_active = 0, last_check = NOW()
                      WHERE user_id = %s AND asset = %s"""
        cursor.execute(sql_hist, (user_id, asset))


# ==========================================================
# Normalizador numérico seguro para CSV / API
# Soporta: -1,131.20 | 1131.20 | None | ''
# ==========================================================
def to_float(value):
    if value is None:
        return 0.0
    try:
        return float(str(value).replace(',', '').strip())
    except ValueError:
        return 0.0


# ==============================================================================
#   LÓGICA BINANCE
# ==============================================================================

MAPEO_INCOME_BINANCE = {
    'FUNDING_FEE': 'FUNDING',
    'REALIZED_PNL': 'PNL',
    'INSURANCE_CLEAR': 'ADJUSTMENT',
    'COMMISSION': 'COMMISSION'
}

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

        # ==========================================================
        # 1. SPOT
        # ==========================================================
        acc = client.get_account()
        for b in acc['balances']:
            f, l = float(b['free']), float(b['locked'])
            total = f + l
            if total > 0:
                p = obtener_precio_db(cur, b['asset'])
                cur.execute(
                    """INSERT INTO sys_saldos_usuarios
                    (user_id, broker_name, tipo_cuenta, asset,
                     cantidad_total, cantidad_disponible,
                     cantidad_bloqueada, precio_referencia,
                     valor_usd, last_update, is_active)
                    VALUES (%s,'Binance','SPOT',%s,%s,%s,%s,%s,%s,NOW(),1)
                    ON DUPLICATE KEY UPDATE
                        cantidad_total=%s,
                        valor_usd=%s,
                        last_update=NOW(),
                        is_active=1""",
                    (
                        user_id, b['asset'], total, f, l, p, total * p,
                        total, total * p
                    )
                )

                disparador_inteligente_pro(cur, user_id, 'Binance', b['asset'], total)

        # ==========================================================
        # 2. FUTUROS — SALDOS
        # ==========================================================
        f_acc = client.futures_account()
        for f in f_acc['assets']:
            wb = float(f['walletBalance'])
            if wb != 0:
                upnl = float(f['unrealizedProfit'])
                eq = wb + upnl

                cur.execute(
                    """INSERT INTO sys_saldos_usuarios
                    (user_id, broker_name, tipo_cuenta, asset,
                     cantidad_total, pnl_no_realizado,
                     equidad_neta, margen_disponible,
                     margen_usado, margen_mantenimiento,
                     valor_usd, last_update, is_active)
                    VALUES (%s,'Binance','PERPETUAL',%s,%s,%s,%s,
                            %s,%s,%s,%s,NOW(),1)
                    ON DUPLICATE KEY UPDATE
                        cantidad_total=%s,
                        equidad_neta=%s,
                        last_update=NOW(),
                        is_active=1""",
                    (
                        user_id, f['asset'], wb, upnl, eq,
                        float(f.get('maxWithdrawAmount', 0)),
                        float(f.get('initialMargin', 0)),
                        float(f.get('maintMargin', 0)),
                        eq,
                        wb, eq
                    )
                )

                disparador_inteligente_pro(cur, user_id, 'Binance', f['asset'], wb)

        # ==========================================================
        # 3. OPEN ORDERS
        # ==========================================================
        cur.execute(
            "DELETE FROM sys_open_orders WHERE user_id=%s AND exchange='Binance'",
            (user_id,)
        )

        for o in client.futures_get_open_orders():
            val = float(o['price']) * float(o['origQty'])
            cur.execute(
                """INSERT INTO sys_open_orders
                (user_id, exchange, symbol, side, type,
                 price, quantity, amount_usd, timestamp)
                VALUES (%s,'Binance',%s,%s,%s,%s,%s,%s,%s)""",
                (
                    user_id, o['symbol'], o['side'], o['type'],
                    o['price'], o['origQty'], val, o['time']
                )
            )

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
                (id_externo_ref, timestamp_ms, symbol,
                 lado, precio_ejecucion,
                 cantidad_ejecutada, pnl_realizado, is_maker)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    id_ext, t['time'], symbol,
                    t['side'], float(t['price']),
                    float(t['qty']), float(t['realizedPnl']),
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

        # ==========================================================
        # 7. STATEMENTS
        # ==========================================================
        if descargar_statements:
            incomes = client.futures_income_history(limit=50)
            for i in incomes:
                if i['incomeType'] == 'TRADING_COMMISSION':
                    continue

                id_ext = f"BIN-INC-{i['tranId']}"
                categoria = MAPEO_INCOME_BINANCE.get(i['incomeType'], i['incomeType'])

                cur.execute(
                    """INSERT IGNORE INTO transacciones_globales
                    (id_externo, user_id, exchange,
                     cuenta_tipo, categoria,
                     asset, monto_neto,
                     timestamp_ms, descripcion)
                    VALUES (%s,%s,'Binance','FUTURES',
                            %s,%s,%s,%s,%s)""",
                    (
                        id_ext, user_id, categoria,
                        i['asset'], i['income'],
                        i['time'], i['info']
                    )
                )

        db.commit()

    except Exception as e:
        print(f"❌ Error Binance ID {user_id}: {e}")

# ==============================================================================
#   LÓGICA BINGX
# ==============================================================================
def procesar_bingx(key, sec, user_id, db, session, descargar_statements):
    try:
        cur = db.cursor(dictionary=True)
        
        # CORREGIDO: Función interna definida correctamente
        def bingx_req(path, params={}):
            params["timestamp"] = int(time.time() * 1000)
            qs = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])
            signature = hmac.new(sec.encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
            url = f"https://open-api.bingx.com{path}?{qs}&signature={signature}"
            return session.get(url, headers={'X-BX-APIKEY': key}).json()

        # 1. SPOT BINGX
        res_spot = bingx_req("/openApi/spot/v1/account/balance")
        if res_spot.get('code') == 0:
            for b in res_spot['data']['balances']:
                total = float(b['free']) + float(b['locked'])
                if total > 0:
                    p = obtener_precio_db(cur, b['asset'])
                    cur.execute("""INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, valor_usd, last_update, is_active) 
                                   VALUES (%s, %s, %s, %s, %s, %s, NOW(), 1)
                                   ON DUPLICATE KEY UPDATE cantidad_total=%s, is_active=1""",
                                (user_id, 'BingX', 'SPOT', b['asset'], total, total * p, total))
                    
                    # CORREGIDO: Llamada correcta
                    disparador_inteligente_pro(cur, user_id, 'BingX', b['asset'], total)

        # 2. PERPETUOS BINGX
        res_swap = bingx_req("/openApi/swap/v2/user/balance")
        if res_swap.get('code') == 0:
            d = res_swap['data']['balance']
            wb = float(d['balance'])
            if wb > 0:
                 cur.execute("""INSERT INTO sys_saldos_usuarios 
                    (user_id, broker_name, tipo_cuenta, asset, cantidad_total, pnl_no_realizado, realised_profit, 
                     equidad_neta, margen_disponible, margen_usado, valor_usd, last_update) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
                    (user_id, 'BingX', 'PERPETUAL', d['asset'], wb, float(d['unrealizedProfit']), float(d['realisedProfit']),
                     float(d['equity']), float(d['availableMargin']), float(d['usedMargin']), float(d['equity'])))
                 
                 disparador_inteligente_pro(cur, user_id, 'BingX', d['asset'], wb)

        # 3. TRADES BINGX
        res_t = bingx_req("/openApi/swap/v2/trade/order/history", {'limit': 5})
        if res_t.get('code') == 0:
            for o in res_t['data']['orders']:
                id_ext = f"BX-T-{o['orderId']}"
                cur.execute("INSERT IGNORE INTO transacciones_globales ""(id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, timestamp_ms) ""VALUES (%s,%s,'BingX','PERPETUAL','TRADE','USDT',0,%s)",(id_ext, user_id, o['time']))
                cur.execute("INSERT IGNORE INTO detalle_trades ""(id_externo_ref, timestamp_ms, symbol, lado, precio_ejecucion, cantidad_ejecutada)" "VALUES (%s,%s,%s,%s,%s,%s)",(id_ext,o['time'],o['symbol'],o['side'],o['price'],o['executedQty']))

        # 4. STATEMENTS BINGX
        if descargar_statements:
            res_inc = bingx_req("/openApi/swap/v2/user/income", {'limit': 20})
            if res_inc.get('code') == 0:
                for i in res_inc['data']:
                    id_ext = f"BX-INC-{i['id']}"
                    cur.execute("INSERT IGNORE INTO transacciones_globales (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, timestamp_ms, descripcion) VALUES (%s,%s,'BingX','PERPETUAL',%s,%s,%s,%s,%s)",
                                (id_ext, user_id, i['incomeType'], i['asset'], i['income'], i['time'], i['incomeType']))

        db.commit()
    except Exception as e: print(f" ❌ Error BingX ID {user_id}: {e}")



# ==============================================================================
#   MOTOR MAESTRO
# ==============================================================================
def motor_maestro():
    print("🚀 MOTOR v1.3.1 (Corregido y Funcional)")
    session = requests.Session()
    contador_ciclos = 0

    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, broker_name, api_key, api_secret FROM api_keys WHERE status = 1")
            usuarios = cursor.fetchall()
            
            es_ciclo_statements = (contador_ciclos % CICLOS_PARA_STATEMENTS == 0)
            
            for u in usuarios:
                k = descifrar_dato(u['api_key'], MASTER_KEY)
                s = descifrar_dato(u['api_secret'], MASTER_KEY)
                broker = u['broker_name'].lower()

                if 'binance' in broker:
                    procesar_binance(k, s, u['user_id'], db, es_ciclo_statements)
                elif 'bingx' in broker:
                    procesar_bingx(k, s, u['user_id'], db, session, es_ciclo_statements)

            db.close()
            print(f"--- Ciclo {contador_ciclos} Finalizado ({'Incluye Statements' if es_ciclo_statements else 'Rápido'}) ---")
            
            contador_ciclos += 1
            time.sleep(ESPERA_CICLO_RAPIDO)

        except Exception as e:
            print(f"🔥 Error Motor: {e}")
            time.sleep(30)

if __name__ == "__main__":
    motor_maestro()