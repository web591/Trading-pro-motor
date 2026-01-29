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
    if asset in ['USDT', 'USDC', 'DAI', 'BUSD', 'FDUSD']: return 1.0
    try:
        # Limpiamos prefijos de Binance Earn para encontrar el precio real
        a_clean = asset.replace('LD', '').replace('STAKE', '')
        cursor.execute("SELECT price FROM sys_precios_activos WHERE symbol = %s OR symbol = %s ORDER BY last_update DESC LIMIT 1", (a_clean, f"{a_clean}USDT"))
        res = cursor.fetchone()
        return float(res['price']) if res else 0.0
    except:
        return 0.0

def tarea_binance(key, secret, user_id):
    """Consulta saldos en Binance y clasifica según tus etiquetas originales"""
    try:
        client = Client(key, secret)
        account = client.get_account()
        balances = account.get('balances', [])
        
        db = mysql.connector.connect(**config.DB_CONFIG)
        cur = db.cursor(dictionary=True)
        
        for b in balances:
            free = float(b['free'])
            locked = float(b['locked'])
            total = free + locked
            
            if total > 0.000001:
                symbol = b['asset']
                
                # --- TU LÓGICA DE ETIQUETAS ORIGINAL ---
                # 1. CASH: Dólares
                if symbol in ['USDT', 'USDC', 'BUSD', 'DAI', 'FDUSD']:
                    tipo = "CASH"
                # 2. EARN: Lo que viene de ahorros (LD)
                elif symbol.startswith('LD') or symbol.startswith('STAKE'):
                    tipo = "EARN"
                # 3. SPOT: El resto de monedas
                else:
                    tipo = "SPOT"

                # Obtener precio para el valor USD
                precio = obtener_precio_db(cur, symbol)
                v_usd = total * precio

                # --- ELIMINAR DUPLICADOS Y ACTUALIZAR ---
                # Esta sentencia limpia lo viejo del usuario para ese asset y broker antes de insertar
                cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND asset = %s AND broker_name = %s", (user_id, symbol, 'Binance'))

                sql = """INSERT INTO sys_saldos_usuarios 
                         (user_id, broker_name, tipo_cuenta, asset, cantidad_total, 
                          cantidad_disponible, cantidad_bloqueada, pnl_no_realizado, 
                          equidad_neta, precio_referencia, valor_usd, last_update) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"""
                
                valores = (user_id, 'Binance', tipo, symbol, total, free, locked, 0.0, v_usd, precio, v_usd)
                cur.execute(sql, valores)

        # --- LÓGICA PERPETUAL (Si existe saldo en futuros) ---
        try:
            futuros = client.futures_account_balance()
            for f in futuros:
                val_f = float(f['balance'])
                if val_f > 0.01:
                    cur.execute("DELETE FROM sys_saldos_usuarios WHERE user_id = %s AND asset = %s AND tipo_cuenta = %s", (user_id, f['asset'], 'PERPETUAL'))
                    equidad = float(f['withdrawAvailable'])
                    cur.execute(sql, (user_id, 'Binance', 'PERPETUAL', f['asset'], val_f, val_f, 0.0, 0.0, equidad, 1.0, equidad))
        except:
            pass

        db.commit()
        cur.close(); db.close()
        return True
    except Exception as e:
        print(f"   ⚠️ Error en Binance (User {user_id}): {e}")
        return False

def motor():
    print("🚀 MOTOR v1.02 - CENTRO DE INTELIGENCIA DE CARTERA")
    while True:
        try:
            conn = mysql.connector.connect(**config.DB_CONFIG)
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT user_id, broker_name, api_key, api_secret FROM api_keys WHERE status = 1")
            todas_las_llaves = cursor.fetchall()
            cursor.close(); conn.close()
            
            for registro in todas_las_llaves:
                u_id = registro['user_id']
                broker = registro['broker_name'].lower()
                print(f"👤 Procesando: ID {u_id} | {broker.upper()}")
                
                k = descifrar_dato(registro['api_key'], MASTER_KEY)
                s = descifrar_dato(registro['api_secret'], MASTER_KEY)
                
                if k and s:
                    if broker == 'binance':
                        tarea_binance(k, s, u_id)
                else:
                    print(f"   ❌ Llaves inválidas para usuario {u_id}")

            espera = random.randint(150, 210)
            print(f"✅ Ciclo terminado. Esperando {espera}s...")
            time.sleep(espera)
        except Exception as e:
            print(f"🔥 Error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    motor()