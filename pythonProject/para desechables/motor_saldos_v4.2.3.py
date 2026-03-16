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

def iniciar_motor():
    print(f"💎 GEMA v4.2.3 - RADAR MULTIUSUARIO")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            
            # Solo traemos llaves de BINANCE para evitar errores con BingX por ahora
            cursor.execute("""
                SELECT user_id, api_key, api_secret 
                FROM api_keys 
                WHERE status=1 AND UPPER(broker_name) = 'BINANCE'
            """)
            usuarios = cursor.fetchall()
            
            for u in usuarios:
                print(f" -> Analizando User {u['user_id']}...")
                k = descifrar_dato(u['api_key'], MASTER_KEY)
                s = descifrar_dato(u['api_secret'], MASTER_KEY)
                if k and s:
                    actualizar_saldos(db, u['user_id'], k, s)
            
            db.close()
        except Exception as e:
            print(f" [CRITICAL] Error en base de datos: {e}")
        
        # SOLUCIÓN AL CRASH: Si no existe la variable en config, espera 60 segundos por defecto
        espera = getattr(config, 'ESPERA_CICLO_RAPIDO', 60)
        print(f"Ciclo terminado. Esperando {espera}s...")
        time.sleep(espera)

if __name__ == "__main__":
    iniciar_motor()