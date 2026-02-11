import mysql.connector
from binance.client import Client
import time, os, base64
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# --- CONFIGURACIÓN ---
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
ESPERA_CICLO = getattr(config, 'ESPERA_CICLO_RAPIDO', 120) 

def descifrar_dato(t, m):
    try:
        if not t: return None
        raw = base64.b64decode(t.strip())
        
        # Determinamos el separador
        sep = b":::" if b":::" in raw else b"::"
        
        # Usamos rsplit(sep, 1) para asegurar que solo separemos el ÚLTIMO trozo (el IV)
        partes = raw.rsplit(sep, 1)
        if len(partes) != 2:
            print(f" [!] Error de formato: Se encontraron {len(partes)} partes en el dato.")
            return None
            
        data, iv = partes
        
        # Verificación crítica del IV (AES CBC requiere 16 bytes)
        if len(iv) != 16:
            print(f" [!] Error de IV: El IV tiene {len(iv)} bytes (se requieren 16).")
            return None

        key_hash = sha256(m.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        
        decrypted = unpad(cipher.decrypt(data), AES.block_size)
        return decrypted.decode().strip()
    except Exception as e:
        print(f" [!] Error interno en descifrado: {e}")
        return None

# --- CEREBRO NORMALIZADOR (Modular por Broker) ---

def normalizador_binance(raw_symbol):
    """
    Plugin específico para Binance.
    Retorna: (ticker_base, categoria, moneda_pago)
    """
    s = raw_symbol.upper().strip()
    categoria = 'SPOT'
    
    # Identificar LENDING sin destruir la base
    if s.startswith('LD'):
        categoria = 'LENDING'
        s = s[2:]
    
    # Identificar FUTUROS
    if 'PERP' in s or s.endswith('_PERP'):
        categoria = 'FUTUROS'
        s = s.replace('_PERP', '').replace('PERP', '')

    # Extraer moneda de pago para obtener el ticker base
    ticker_base = s
    for suffix in ['USDT', 'USDC', 'BUSD', 'BTC', 'ETH', 'USD']:
        if s.endswith(suffix) and s != suffix:
            ticker_base = s[:-len(suffix)]
            break
            
    return ticker_base.replace('-', '').replace('_', ''), categoria

def obtener_traductor_id(cursor, ticker_base, categoria, broker, raw_ticker):
    """
    TRIANGULACIÓN: Busca la identidad exacta en el diccionario maestro.
    """
    query = """
        SELECT id FROM sys_traductor_simbolos 
        WHERE (nombre_comun = %s AND broker_name = %s AND categoria_producto = %s)
           OR (ticker_motor = %s AND broker_name = %s)
        LIMIT 1
    """
    cursor.execute(query, (ticker_base, broker, categoria, raw_ticker, broker))
    res = cursor.fetchone()
    return res['id'] if res else None

# --- LOGICA DE RADAR Y SALDOS ---

def ejecutar_radar_gema(conexion_db, user_id, lista_activos_raw, broker='Binance'):
    cursor = conexion_db.cursor(dictionary=True)
    
    for raw in lista_activos_raw:
        # 1. Normalización técnica
        ticker_base, categoria = normalizador_binance(raw)
        
        # 2. Triangulación de ID (Pre-consulta al diccionario)
        t_id = obtener_traductor_id(cursor, ticker_base, categoria, broker, raw)

        # 3. Filtro de Exclusión Inteligente
        # Si ya tiene este ID en su cartera, lo ignoramos
        if t_id:
            cursor.execute("SELECT id FROM sys_usuarios_activos WHERE user_id = %s AND traductor_id = %s", (user_id, t_id))
            if cursor.fetchone(): continue

        # 4. Evitar duplicados en el propio buzón
        cursor.execute("SELECT id FROM sys_simbolos_buscados WHERE ticker = %s AND user_id = %s", (ticker_base, user_id))
        if cursor.fetchone(): continue

        # 5. Inserción en Radar con etiqueta de producto e ID si existe
        print(f" [RADAR] Usuario {user_id} -> Detectado {ticker_base} [{categoria}]")
        sql = """
            INSERT IGNORE INTO sys_simbolos_buscados 
            (user_id, ticker, status, traductor_id, info) 
            VALUES (%s, %s, 'encontrado', %s, %s)
        """
        info_extra = f"Detectado automáticamente en {broker} {categoria}"
        cursor.execute(sql, (user_id, ticker_base, t_id, info_extra))
    
    conexion_db.commit()

def actualizar_saldos(conexion_db, user_id, api_key, api_secret):
    try:
        # 1. Inicializar cliente de Binance
        client = Client(api_key, api_secret)
        
        # Validamos conexión obteniendo balances
        try:
            account_info = client.get_account()
            balances = account_info.get('balances', [])
        except Exception as api_err:
            print(f" [!] Error de API para Usuario {user_id}: {api_err}")
            return # Saltamos este registro si la API es inválida

        cursor = conexion_db.cursor(dictionary=True)
        activos_encontrados = []

        for b in balances:
            total = float(b['free']) + float(b['locked'])
            
            if total > 0.00001:
                raw_asset = b['asset']
                activos_encontrados.append(raw_asset)
                
                # Normalización
                ticker_base, categoria = normalizador_binance(raw_asset)
                t_id = obtener_traductor_id(cursor, ticker_base, categoria, 'Binance', raw_asset)

                # SQL CORREGIDO: Usamos 'broker' en lugar de 'broker_name'
                # Y usamos 'tipo_cuenta' que es como está en tu SQL
                
                # Cambia esta línea dentro del SQL de la función actualizar_saldos:
                sql = """
                    INSERT INTO sys_saldos_usuarios 
                    (user_id, broker, tipo_cuenta, asset, cantidad_total, traductor_id, last_update)
                    VALUES (%s, 'Binance', %s, %s, %s, %s, NOW())
                    ...
                """
                cursor.execute(sql, (user_id, categoria, raw_asset, total, t_id))

        conexion_db.commit()
        print(f" [+] Usuario {user_id}: {len(activos_encontrados)} activos actualizados.")
        
        if activos_encontrados:
            ejecutar_radar_gema(conexion_db, user_id, activos_encontrados)

    except Exception as e:
        print(f" [!] Error Crítico en Proceso User {user_id}: {e}")

# --- INICIO ---

def iniciar_motor():
    print(f"\n{'='*40}\n GEMA v4.0.6 - MULTI-BROKER INTELLIGENT RADAR\n{'='*40}")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, api_key, api_secret FROM api_keys WHERE status=1")
            usuarios = cursor.fetchall()

# --- DENTRO DE iniciar_motor() ---

            for u in usuarios:
                # Identificamos el broker de la fila actual
                broker_actual = u.get('broker_name', '').upper()
                
                print(f" -> Procesando Usuario {u['user_id']} [{broker_actual}]...")
                
                k = descifrar_dato(u['api_key'], MASTER_KEY)
                s = descifrar_dato(u['api_secret'], MASTER_KEY)
                
                if k and s:
                    # SOLO ejecutamos si el broker es Binance
                    if broker_actual == 'BINANCE':
                        actualizar_saldos(db, u['user_id'], k, s)
                    else:
                        print(f" [!] Skip: El motor de {broker_actual} aún no está implementado.")
                else:
                    print(f" [!] Error: No se pudieron descifrar las llaves.")

            db.close()
        except Exception as e:
            print(f" [CRITICAL] {e}")
        
        print(f"\n Ciclo terminado. Esperando {ESPERA_CICLO}s...")
        time.sleep(ESPERA_CICLO)

if __name__ == "__main__":
    iniciar_motor()