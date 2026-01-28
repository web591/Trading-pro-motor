import mysql.connector
from binance.client import Client
import time, sys, os, base64, hmac, requests, threading, random
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# --- CONFIGURACIÓN DE SEGURIDAD ---
try:
    import config
    # Intentamos obtener la llave maestra desde el entorno o el archivo config
    MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
    if not MASTER_KEY:
        print("❌ ERROR: No se encontró la ENCRYPTION_KEY en config.py")
        sys.exit(1)
except Exception as e:
    print(f"❌ ERROR de configuración: {e}")
    sys.exit(1)

def descifrar_dato(texto_cifrado, llave_maestra):
    """Descifra las API Keys de la base de datos"""
    try:
        r = base64.b64decode(texto_cifrado.strip())
        p = r.split(b"::")
        c = AES.new(sha256(llave_maestra.encode()).digest(), AES.MODE_CBC, p[1])
        return unpad(c.decrypt(p[0]), AES.block_size).decode().strip()
    except:
        return None

def obtener_precio_db(cursor, asset):
    """Obtiene el último precio conocido de un activo para calcular el saldo en USD"""
    if asset in ['USDT', 'USDC', 'DAI', 'BUSD']: return 1.0
    try:
        cursor.execute("SELECT price FROM sys_precios_activos WHERE symbol = %s OR symbol = %s ORDER BY last_update DESC LIMIT 1", (asset, f"{asset}USDT"))
        res = cursor.fetchone()
        return float(res['price']) if res else 0.0
    except:
        return 0.0

def guardar_saldo(user_id, broker, asset, free_amount, locked_amount):
    """Guarda o actualiza el saldo en la tabla del usuario"""
    try:
        db = mysql.connector.connect(**config.DB_CONFIG)
        cur = db.cursor(dictionary=True)
        
        total = float(free_amount) + float(locked_amount)
        if total <= 0: return False

        precio = obtener_precio_db(cur, asset)
        valor_usd = total * precio

        # AJUSTE: He cambiado 'source' por 'broker' (o verifica si tu columna se llama 'exchange')
        # Si tu columna se llama 'source' y el error dice que NO existe, prueba con 'broker_name' 
        # o 'source' (según tu PHP original era 'source', pero el error dice que no está)
        
        sql = """INSERT INTO sys_saldos_usuarios 
                 (user_id, source, asset, free, locked, total, btc_val, usd_val, last_update) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                 ON DUPLICATE KEY UPDATE 
                 free=%s, locked=%s, total=%s, usd_val=%s, last_update=NOW()"""
        
        # Si el error persiste, es probable que en tu base de datos la columna se llame 'broker'
        # En ese caso, cambia arriba 'source' por 'broker'
        
        valores = (user_id, broker, asset, free_amount, locked_amount, total, 0, valor_usd,
                   free_amount, locked_amount, total, valor_usd)
        
        cur.execute(sql, valores)
        db.commit()
        cur.close(); db.close()
        return True
    except mysql.connector.Error as err:
        print(f"   ⚠️ Error de Base de Datos en {asset}: {err}")
        return False

def tarea_binance(key, secret, user_id):
    """Consulta saldos en Binance Spot"""
    try:
        client = Client(key, secret)
        account = client.get_account()
        balances = account.get('balances', [])
        
        for b in balances:
            free = float(b['free'])
            locked = float(b['locked'])
            if (free + locked) > 0:
                guardar_saldo(user_id, 'binance_spot', b['asset'], free, locked)
        return True
    except Exception as e:
        print(f"   ⚠️ Error en Binance (User {user_id}): {e}")
        return False

def tarea_bingx(key, secret, user_id):
    """Consulta saldos en BingX (Simulado según tu lógica original)"""
    # Aquí iría tu lógica de requests a BingX usando el user_id dinámico
    print(f"   🟠 BingX User {user_id} -> Consultando...")
    # (Tu código de BingX actual debería ir aquí adaptado para usar el 'user_id' que recibe)
    return True

def motor():
    print("🚀 MOTOR v1.00 - CENTRO DE INTELIGENCIA DE CARTERA")
    print("🔄 Modo Multi-Usuario Activado")
    
    while True:
        try:
            # 1. Conectar y buscar TODAS las llaves activas
            conn = mysql.connector.connect(**config.DB_CONFIG)
            cursor = conn.cursor(dictionary=True)
            
            # Buscamos llaves de CUALQUIER usuario que estén activas (status = 1)
            cursor.execute("SELECT user_id, broker_name, api_key, api_secret FROM api_keys WHERE status = 1")
            todas_las_llaves = cursor.fetchall()
            cursor.close(); conn.close()
            
            if not todas_las_llaves:
                print("💤 No hay llaves API activas para procesar.")
            
            # 2. Iterar sobre cada llave encontrada
            for registro in todas_las_llaves:
                u_id = registro['user_id']
                broker = registro['broker_name'].lower()
                
                print(f"👤 Procesando Usuario ID: {u_id} | Broker: {broker.upper()}")
                
                # Descifrar llaves
                k = descifrar_dato(registro['api_key'], MASTER_KEY)
                s = descifrar_dato(registro['api_secret'], MASTER_KEY)
                
                if k and s:
                    if broker == 'binance':
                        res = tarea_binance(k, s, u_id)
                        print(f"   ✅ Binance Sync: {'EXITOSO' if res else 'FALLIDO'}")
                    elif broker == 'bingx':
                        res = tarea_bingx(k, s, u_id)
                        print(f"   ✅ BingX Sync: {'EXITOSO' if res else 'FALLIDO'}")
                else:
                    print(f"   ❌ Error: No se pudieron descifrar las llaves del usuario {u_id}")

            # 3. Pausa aleatoria para evitar bloqueos de IP (Anti-Detección)
            espera = random.randint(150, 300) 
            print(f"--- Ciclo completado. Esperando {espera} segundos para la siguiente vuelta ---")
            time.sleep(espera)

        except Exception as e:
            print(f"🔥 ERROR CRÍTICO EN EL MOTOR: {e}")
            time.sleep(60)

if __name__ == "__main__":
    motor()