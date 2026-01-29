import mysql.connector
from binance.client import Client
import time, sys, os, base64, hmac, requests, threading, random
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# --- CONFIGURACIÓN DE SEGURIDAD ---
try:
    import config
    MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
    if not MASTER_KEY:
        print("❌ ERROR: No se encontró la ENCRYPTION_KEY")
        sys.exit(1)
except Exception as e:
    sys.exit(1)

def descifrar_dato(texto_cifrado, llave_maestra):
    try:
        r = base64.b64decode(texto_cifrado.strip())
        p = r.split(b"::")
        c = AES.new(sha256(llave_maestra.encode()).digest(), AES.MODE_CBC, p[1])
        return unpad(c.decrypt(p[0]), AES.block_size).decode().strip()
    except:
        return None

def obtener_precio_db(cursor, asset):
    if asset in ['USDT', 'USDC', 'DAI', 'BUSD']: return 1.0
    try:
        cursor.execute("SELECT price FROM sys_precios_activos WHERE symbol = %s OR symbol = %s ORDER BY last_update DESC LIMIT 1", (asset, f"{asset}USDT"))
        res = cursor.fetchone()
        return float(res['price']) if res else 0.0
    except:
        return 0.0

def guardar_saldo(user_id, broker, asset, free_amount, locked_amount):
    """Guarda o actualiza el saldo usando las columnas REALES de tu tabla"""
    try:
        db = mysql.connector.connect(**config.DB_CONFIG)
        cur = db.cursor(dictionary=True)
        
        total = float(free_amount) + float(locked_amount)
        if total <= 0: return False

        precio = obtener_precio_db(cur, asset)
        valor_usd = total * precio

        # SQL AJUSTADO A TUS COLUMNAS: broker_name, cantidad_total, cantidad_disponible, etc.
        sql = """INSERT INTO sys_saldos_usuarios 
                 (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, cantidad_bloqueada, precio_referencia, valor_usd, last_update) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                 ON DUPLICATE KEY UPDATE 
                 cantidad_total=%s, cantidad_disponible=%s, cantidad_bloqueada=%s, precio_referencia=%s, valor_usd=%s, last_update=NOW()"""
        
        # tipo_cuenta lo ponemos como 'SPOT' por defecto para Binance
        valores = (user_id, broker, 'SPOT', asset, total, free_amount, locked_amount, precio, valor_usd,
                   total, free_amount, locked_amount, precio, valor_usd)
        
        cur.execute(sql, valores)
        db.commit()
        cur.close(); db.close()
        return True
    except Exception as e:
        print(f"   ⚠️ Error DB en {asset}: {e}")
        return False

def tarea_binance(key, sec, user_id):
    try:
        client = Client(key, sec)
        db = mysql.connector.connect(**config.DB_CONFIG)
        cur = db.cursor(dictionary=True)

        # --- 1. PROCESAR SPOT, CASH Y EARN ---
        info = client.get_account()
        for asset in info.get('balances', []):
            c_free = float(asset['free'])
            c_locked = float(asset['locked'])
            qty = c_free + c_locked
            
            if qty > 0.00001:
                symbol = asset['asset']
                # Lógica de categorías
                es_estable = symbol in ['USDT', 'USDC', 'BUSD', 'DAI', 'FDUSD']
                es_earn = symbol.startswith('LD') or symbol.startswith('STAKE') or "SIMPLEEARN" in symbol

                if es_earn: tipo = "EARN"
                elif es_estable: tipo = "CASH"
                else: tipo = "SPOT"

                asset_clean = symbol.replace('LD', '').replace('STAKE', '')
                p = obtener_precio_db(cur, asset_clean)
                v_usd = qty * p

                sql = """REPLACE INTO sys_saldos_usuarios 
                         (user_id, broker_name, tipo_cuenta, asset, cantidad_total, 
                          cantidad_disponible, cantidad_bloqueada, pnl_no_realizado, 
                          equidad_neta, precio_referencia, valor_usd, last_update) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"""
                
                cur.execute(sql, (user_id, 'Binance', tipo, symbol, qty, c_free, c_locked, 0.0, v_usd, p, v_usd))

        # --- 2. PROCESAR PERPETUAL (FUTUROS) ---
        try:
            futuros = client.futures_account_balance()
            for f in futuros:
                balance_f = float(f['balance'])
                # Solo guardamos si hay dinero en la billetera de futuros (USDT o monedas)
                if balance_f > 0.01:
                    asset_f = f['asset']
                    # En futuros, la equidad neta incluye el PnL
                    equidad = float(f['withdrawAvailable']) # Lo que realmente vale la cuenta ahora
                    
                    sql_f = """REPLACE INTO sys_saldos_usuarios 
                             (user_id, broker_name, tipo_cuenta, asset, cantidad_total, 
                              cantidad_disponible, cantidad_bloqueada, pnl_no_realizado, 
                              equidad_neta, precio_referencia, valor_usd, last_update) 
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"""
                    
                    # Para Futuros, el PnL lo calculamos o lo dejamos para que el PHP lo jale de equidad
                    cur.execute(sql_f, (
                        user_id, 'Binance', 'PERPETUAL', asset_f, balance_f, 
                        balance_f, 0.0, 0.0, equidad, 1.0, equidad
                    ))
        except Exception as e_f:
            print(f"   ⚠️ Nota: No se pudo acceder a Futuros de ID {user_id} (Posiblemente no activados)")

        db.commit()
        cur.close(); db.close()
        print(f"   ✅ Binance ID {user_id}: Sincronizado (SPOT + EARN + PERPETUAL)")
        return True
    except Exception as e:
        print(f"   ❌ Error en Binance ID {user_id}: {str(e)}")
        return False

def motor():
    print("🚀 MOTOR v1.00 - CENTRO DE INTELIGENCIA DE CARTERA")
    print("🔄 Sincronizando todos los usuarios con llaves activas...")
    
    while True:
        try:
            conn = mysql.connector.connect(**config.DB_CONFIG)
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT user_id, broker_name, api_key, api_secret FROM api_keys WHERE status = 1")
            todas_las_llaves = cursor.fetchall()
            cursor.close(); conn.close()
            
            for registro in todas_las_llaves:
                u_id = registro['user_id']
                broker = registro['broker_name']
                
                print(f"👤 Procesando: ID {u_id} | {broker}")
                
                k = descifrar_dato(registro['api_key'], MASTER_KEY)
                s = descifrar_dato(registro['api_secret'], MASTER_KEY)
                
                if k and s:
                    if broker.lower() == 'binance':
                        tarea_binance(k, s, u_id)
                    # Aquí puedes añadir BingX después
                else:
                    print(f"   ❌ Llaves inválidas para usuario {u_id}")

            espera = random.randint(150, 210)
            print(f"✅ Ciclo terminado. Esperando {espera}s...")
            time.sleep(espera)

        except Exception as e:
            print(f"🔥 Error en el motor: {e}")
            time.sleep(30)

if __name__ == "__main__":
    motor()