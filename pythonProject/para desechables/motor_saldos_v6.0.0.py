import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests, json
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

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
# 🎯 VINCULACIÓN MAESTRA v5.6.3 (AJUSTADA)
# ==========================================================
def obtener_traductor_id(cursor, motor_fuente, ticker):
    ticker = ticker.upper().strip()
    
    # 1. Búsqueda Directa (Exacta por motor y ticker)
    sql = "SELECT id FROM sys_traductor_simbolos WHERE motor_fuente=%s AND ticker_motor=%s AND is_active=1 LIMIT 1"
    cursor.execute(sql, (motor_fuente, ticker))
    row = cursor.fetchone()
    if row: return row['id']

    # 2. Lógica de "Limpieza de Prefijos"
    ticker_limpio = ticker
    if ticker.startswith("LD") and len(ticker) > 2:
        ticker_limpio = ticker[2:]
    elif ticker.startswith("STK") and len(ticker) > 3:
        ticker_limpio = ticker[3:]

    # 3. Búsqueda por Underlying en el mismo motor (Ej: Buscar USDT si es LDUSDT)
    sql = "SELECT id FROM sys_traductor_simbolos WHERE underlying=%s AND motor_fuente=%s AND is_active=1 LIMIT 1"
    cursor.execute(sql, (ticker_limpio, motor_fuente))
    row = cursor.fetchone()
    if row: return row['id']

    # 4. Búsqueda Global (Último recurso, pero PRIORIZANDO activos vigentes)
    # Ordenamos por is_active para no agarrar basura de Yahoo o deslistados
    sql = """
        SELECT id FROM sys_traductor_simbolos 
        WHERE underlying=%s 
        ORDER BY is_active DESC, fecha_creacion DESC 
        LIMIT 1
    """
    cursor.execute(sql, (ticker_limpio,))
    row = cursor.fetchone()
    
    return row['id'] if row else None

# --- LA FUNCIÓN QUE FALTABA ---
def disparar_radar(cursor, uid, ticker, ctx):
    sql = "INSERT IGNORE INTO sys_simbolos_buscados (user_id, ticker, status, info) VALUES (%s,%s,'pendiente',%s)"
    cursor.execute(sql, (uid, ticker, f"Detectado en {ctx}"))

# ==========================================================
# 💰 OBTENCIÓN DE PRECIO USD (SIN "TRAMPAS" DE LIKE)
# ==========================================================
def obtener_precio_usd(cursor, tid, asset_name):
    asset_name = asset_name.upper()
    
    # 1. Forzar Stables a 1.0 (Hardcoded para evitar errores de DB)
    stables = ['USDT', 'USDC', 'DAI', 'BUSD', 'PYUSD']
    clean_ticker = asset_name.replace("LD", "").replace("STK", "")
    if clean_ticker in stables:
        return 1.0
    
    try:
        # 2. Búsqueda por ID (El método más seguro)
        if tid:
            sql = "SELECT price FROM sys_precios_activos WHERE traductor_id = %s ORDER BY last_update DESC LIMIT 1"
            cursor.execute(sql, (tid,))
            row = cursor.fetchone()
            if row and row['price'] > 0:
                return float(row['price'])
        
        # 3. FALLBACK POR UNDERLYING (En lugar de LIKE)
        # Si el ID no tiene precio, buscamos el precio más reciente de CUALQUIER motor 
        # que comparta el mismo underlying (ej. Si LDONE no tiene precio, busca el de ONE)
        sql_fb = """
            SELECT p.price 
            FROM sys_precios_activos p
            JOIN sys_traductor_simbolos t ON p.traductor_id = t.id
            WHERE t.underlying = %s AND t.is_active = 1
            ORDER BY p.last_update DESC LIMIT 1
        """
        cursor.execute(sql_fb, (clean_ticker,))
        row_fb = cursor.fetchone()
        if row_fb:
            return float(row_fb['price'])

    except Exception as e:
        print(f"     [!] Error en precio para {asset_name}: {e}")
    
    return 0.0

def obtener_permiso_ejecucion(cursor, uid, proceso="motor_saldos_v6"):
    """
    Llama al procedimiento almacenado para asegurar que no haya 
    dos instancias del mismo proceso para el mismo usuario.
    """
    try:
        # Ejecutamos el procedimiento con un timeout de 5 segundos
        cursor.execute("CALL sp_get_job_lock(%s, %s, 5)", (uid, proceso))
        res = cursor.fetchone()
        # Si el SP devuelve 1, tenemos el lock. Si es 0, está ocupado.
        return res['res'] if res and 'res' in res else 0
    except Exception as e:
        print(f"    [!] Error al obtener Lock: {e}")
        return 0

def obtener_punto_inicio_sincro(cursor, broker_id, metodo):
    """
    Busca en la tabla sys_sync_estado cuándo fue la última vez 
    que sincronizamos con éxito este método específico.
    """
    sql = """
        SELECT ultima_fecha_sincro 
        FROM sys_sync_estado 
        WHERE broker_id = %s AND metodo_api = %s 
        LIMIT 1
    """
    cursor.execute(sql, (broker_id, metodo))
    row = cursor.fetchone()
    
    if row and row['ultima_fecha_sincro']:
        return int(row['ultima_fecha_sincro'])
    
    # Si es la primera vez, retrocedemos 30 días (en milisegundos)
    return int((time.time() - (30 * 24 * 60 * 60)) * 1000)

def actualizar_punto_sincro(cursor, broker_id, metodo, nuevo_ts):
    """
    Guarda el timestamp del último registro procesado para la próxima ejecución.
    """
    sql = """
        INSERT INTO sys_sync_estado (broker_id, metodo_api, ultima_fecha_sincro, last_update)
        VALUES (%s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE 
        ultima_fecha_sincro = VALUES(ultima_fecha_sincro),
        last_update = NOW()
    """
    cursor.execute(sql, (broker_id, metodo, nuevo_ts))

def normalizar_fecha_sql(ts_ms):
    """
    Convierte milisegundos (Binance/BingX) a formato DATETIME de MariaDB.
    Ejemplo: 1772668799000 -> '2026-03-04 23:59:59'
    """
    try:
        # Convertimos milisegundos a segundos
        ts_segundos = ts_ms / 1000.0
        # Formateamos a string compatible con SQL
        return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts_segundos))
    except:
        return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()) # Fallback a NOW()

def registrar_saldo(cursor, uid, tid, total, locked, asset_name, broker, tipo_cta="SPOT"):
    disponible = total - locked
    precio = obtener_precio_usd(cursor, tid, asset_name)
    valor_usd = total * precio
    
    # Mapeo para Futuros
    margen_usado = locked if tipo_cta == "FUTURES" else 0.0
    
    sql = """
        INSERT INTO sys_saldos_usuarios 
        (user_id, broker_name, asset, traductor_id, cantidad_total, cantidad_disponible, 
         cantidad_bloqueada, margen_usado, valor_usd, precio_referencia, tipo_cuenta, tipo_lista, last_update)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'ACTIVO', NOW())
        ON DUPLICATE KEY UPDATE
        traductor_id = VALUES(traductor_id),
        cantidad_total = VALUES(cantidad_total),
        cantidad_disponible = VALUES(cantidad_disponible),
        cantidad_bloqueada = VALUES(cantidad_bloqueada),
        margen_usado = VALUES(margen_usado),
        valor_usd = VALUES(valor_usd),
        precio_referencia = VALUES(precio_referencia),
        tipo_cuenta = VALUES(tipo_cuenta),
        last_update = NOW()
    """
    cursor.execute(sql, (uid, broker, asset_name, tid, total, disponible, locked, margen_usado, valor_usd, precio, tipo_cta))
# ==========================================================
# 🟨 PROCESADOR BINANCE
# ==========================================================
def procesar_binance(db, uid, k, s):
    try:
        client = Client(k, s)
        cursor = db.cursor(dictionary=True)

        # SPOT
        acc = client.get_account()
        for b in acc['balances']:
            total = float(b['free']) + float(b['locked'])
            if total <= 0.000001: continue
            ticker = b['asset']
            tid = obtener_traductor_id(cursor, "binance_spot", ticker)
            registrar_saldo(cursor, uid, tid, total, float(b['locked']), ticker, "BINANCE", "SPOT")
            if not tid: disparar_radar(cursor, uid, ticker, "BINANCE SPOT")

        # EARN / LENDING
        try:
            savings = client.get_simple_earn_account_realtime_data()
            for s_asset in savings.get('rows', []):
                total = float(s_asset['totalAmount'])
                if total <= 0: continue
                ticker = s_asset['asset']
                # Buscamos con prefijo LD para consistencia contable
                asset_ld = f"LD{ticker}"
                tid = obtener_traductor_id(cursor, "binance_spot", asset_ld)
                registrar_saldo(cursor, uid, tid, total, 0, asset_ld, "BINANCE", "LENDING")
                if not tid: disparar_radar(cursor, uid, asset_ld, "BINANCE EARN")
        except: pass 

        print(f"    [OK] Binance User {uid} procesado.")
    except Exception as e: print(f"    [!] Error Binance User {uid}: {e}")

# ==========================================================
# 🟦 PROCESADOR BINGX
# ==========================================================
def procesar_bingx(db, uid, ak, as_):
    cursor = db.cursor(dictionary=True)
    print(f"    [DEBUG] Iniciando BingX para User {uid}...")
    
    def bx_req(path, params=None):
        if params is None: params = {}
        ts = int(time.time()*1000)
        params["timestamp"] = ts
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://open-api.bingx.com{path}?{query}&signature={sig}"
        r = requests.get(url, headers={"X-BX-APIKEY": ak}, timeout=10).json()
        if r.get("code") != 0:
            print(f"    [!] Error API BingX: {r.get('msg')} (Code: {r.get('code')})")
        return r

    # --- 1. SPOT ---
    try:
        res_spot = bx_req("/openApi/spot/v1/account/balance")
        if res_spot.get("data"):
            for b in res_spot['data']['balances']:
                total = float(b['free']) + float(b['locked'])
                if total <= 0.000001: continue
                ticker = b['asset']
                tid = obtener_traductor_id(cursor, "bingx_crypto", ticker)
                registrar_saldo(cursor, uid, tid, total, float(b['locked']), ticker, "BINGX", "SPOT")
            print(f"    [OK] BingX Spot procesado.")
    except Exception as e: 
        print(f"    [!] Error crítico en BingX Spot: {e}")

    # --- 2. FUTURES PERPETUAL (Revertido a lógica v5.5.6 que funcionaba) ---
    try:
        res_perp = bx_req("/openApi/swap/v2/user/balance")
        # Accedemos directo al balance como en la versión funcional
        data_balance = res_perp.get("data", {})
        if isinstance(data_balance, list): 
            balances = data_balance
        else:
            # Si es un dict, lo buscamos en la llave 'balance' o lo metemos en lista
            balances = [data_balance.get("balance", {})] if "balance" in data_balance else [data_balance]

        for item in balances:
            ticker = item.get("asset")
            if not ticker: continue
            
            total = float(item.get("balance", 0))
            locked = float(item.get("freezedMargin", 0))
            if total <= 0: continue
            
            tid = obtener_traductor_id(cursor, "bingx_usdt_future", ticker)
            registrar_saldo(cursor, uid, tid, total, locked, ticker, "BINGX", "FUTURES")
        
        print(f"    [OK] BingX Perpetual procesado.")
    except Exception as e: 
        print(f"    [!] Error crítico en BingX Perp: {e}")



# ==========================================================
# 🚀 EJECUCIÓN
# ==========================================================
def run():
    print("💎 MOTOR SALDOS v6.0 - FASE 1: INFRAESTRUCTURA ACTIVA")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            
            # 1. Obtenemos llaves
            cursor.execute("SELECT user_id, api_key, api_secret, broker_name, broker_id FROM api_keys WHERE status=1")
            users = cursor.fetchall()

            for u in users:
                # --- PASO 1.1: SEGURIDAD (LOCK) ---
                if not obtener_permiso_ejecucion(cursor, u['user_id']):
                    print(f"    [!] Usuario {u['user_id']} ya está siendo procesado. Saltando...")
                    continue

                # Descifrado de llaves (Tu lógica v5.6.3)
                k = descifrar_dato(u['api_key'], MASTER_KEY)
                s = descifrar_dato(u['api_secret'], MASTER_KEY)
                if not k or not s: continue
                
                # --- PASO 1.2: OBTENER START_TIME ---
                # Ejemplo para Binance Spot Trades
                start_ts = obtener_punto_inicio_sincro(cursor, u['broker_id'], "binance_spot_trades")

                if u['broker_name'].upper() == "BINANCE":
                    procesar_binance(db, u['user_id'], k, s)
                elif u['broker_name'].upper() == "BINGX":
                    procesar_bingx(db, u['user_id'], k, s)
                
            db.commit()
            db.close()
        except Exception as e: 
            print(f"CRITICAL: {e}")
        time.sleep(60)


if __name__ == "__main__":
    run()