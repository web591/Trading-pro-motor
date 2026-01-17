import mysql.connector
from binance.client import Client
import time
import sys
import os
import base64
import hmac
import requests
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# --- 1. CONFIGURACIÓN ---
try:
    import config
    # Intentamos obtener la clave de cifrado desde el entorno o el archivo config
    MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
    if not MASTER_KEY:
        print("❌ ERROR: No se encontró ENCRYPTION_KEY en config.py")
        sys.exit(1)
except ImportError:
    print("❌ ERROR: No se encontró config.py")
    sys.exit(1)

# --- 2. FUNCIÓN DE DESCIFRADO ---
def descifrar_dato(texto_base64, master_key):
    try:
        if not texto_base64: return None
        raw_combined = base64.b64decode(texto_base64.strip())
        if b"::" in raw_combined:
            partes = raw_combined.split(b"::")
            cifrado, iv = partes[0], partes[1]
        else:
            # Intento de rescate si el formato viene distinto
            decoded_text = raw_combined.decode('utf-8')
            if "::" in decoded_text:
                p = decoded_text.split("::")
                cifrado, iv = base64.b64decode(p[0]), base64.b64decode(p[1])
            else: return None
            
        key_hash = sha256(master_key.encode('utf-8')).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        decrypted_raw = cipher.decrypt(cifrado)
        return unpad(decrypted_raw, AES.block_size).decode('utf-8').strip()
    except:
        return None

# --- 3. OBTENER PRECIOS ---
def obtener_precio_db(cursor, asset):
    if asset in ['USDT', 'USDC', 'DAI', 'BUSD', 'LDUSDT', 'LDUSDC']: 
        return 1.0
    # Limpiamos prefijos de Binance Earn (LD)
    search_asset = asset[2:] if asset.startswith('LD') else asset
    cursor.execute("""
        SELECT price FROM sys_precios_activos 
        WHERE symbol = %s OR symbol = %s 
        ORDER BY last_update DESC LIMIT 1
    """, (search_asset, f"{search_asset}USDT"))
    res = cursor.fetchone()
    return float(res['price']) if res else 0.0

# --- 4. MOTOR DE ACTUALIZACIÓN ---
def actualizar_saldos():
    conn = None
    try:
        conn = mysql.connector.connect(**config.DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # Solo procesamos tu usuario (ID 6) que es el que estamos configurando
        cursor.execute("SELECT broker_name, api_key, api_secret FROM api_keys WHERE user_id = 6 AND status = 1")
        keys_list = cursor.fetchall()

        for registro in keys_list:
            broker = registro['broker_name'].lower()
            key = descifrar_dato(registro['api_key'], MASTER_KEY)
            sec = descifrar_dato(registro['api_secret'], MASTER_KEY)
            
            if not key or not sec:
                print(f"⚠️ No se pudo descifrar las llaves para {broker}")
                continue

            # --- LÓGICA BINANCE ---
            if broker == 'binance':
                print("🤖 Sincronizando Binance...")
                try:
                    client = Client(key, sec)
                    # Spot Binance
                    for b in client.get_account()['balances']:
                        tot = float(b['free']) + float(b['locked'])
                        if tot > 0.0001:
                            p = obtener_precio_db(cursor, b['asset'])
                            tipo = 'CASH' if b['asset'] in ['USDT', 'USDC'] else 'SPOT'
                            cursor.execute("""
                                INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, precio_referencia, valor_usd) 
                                VALUES (6, 'binance', %s, %s, %s, %s, %s, %s) 
                                ON DUPLICATE KEY UPDATE cantidad_total=%s, cantidad_disponible=%s, precio_referencia=%s, valor_usd=%s
                            """, (tipo, b['asset'], tot, float(b['free']), p, tot*p, tot, float(b['free']), p, tot*p))
                except Exception as eb: print(f"❌ Error Binance: {eb}")

            # --- LÓGICA BINGX ---
            elif broker == 'bingx':
                print("🟠 Sincronizando BingX...")
                try:
                    def bx_req(path, params={}):
                        params['timestamp'] = int(time.time() * 1000)
                        qs = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
                        sig = hmac.new(sec.encode('utf-8'), qs.encode('utf-8'), sha256).hexdigest()
                        url = f"https://open-api.bingx.com{path}?{qs}&signature={sig}"
                        return requests.get(url, headers={'X-BX-APIKEY': key}).json()

                    # 1. SPOT BINGX ($3.38)
                    s_res = bx_req("/openApi/spot/v1/account/balance")
                    if s_res.get('code') == 0:
                        for b in s_res['data']['balances']:
                            tot = float(b['free']) + float(b['locked'])
                            if tot > 0.001:
                                p = obtener_precio_db(cursor, b['asset'])
                                cursor.execute("""
                                    INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, precio_referencia, valor_usd) 
                                    VALUES (6, 'bingx', 'SPOT', %s, %s, %s, %s) 
                                    ON DUPLICATE KEY UPDATE cantidad_total=%s, valor_usd=%s
                                """, (b['asset'], tot, p, tot*p, tot, tot*p))

                    # 2. PERPETUAL V2 ($5.00 + los $17.95 si los transfieres)
                    f_res = bx_req("/openApi/swap/v2/user/balance")
                    if f_res.get('code') == 0:
                        data = f_res.get('data', [])
                        # Normalizamos la respuesta (lista o dict)
                        items = data if isinstance(data, list) else [data.get('balance', data)]
                        for item in items:
                            if isinstance(item, dict) and 'asset' in item:
                                monto = float(item.get('balance', 0))
                                if monto > 0.001:
                                    asset = item['asset']
                                    p = 1.0 if asset == 'USDT' else obtener_precio_db(cursor, asset)
                                    cursor.execute("""
                                        INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, precio_referencia, valor_usd) 
                                        VALUES (6, 'bingx', 'PERPETUAL_V2', %s, %s, %s, %s) 
                                        ON DUPLICATE KEY UPDATE cantidad_total=%s, valor_usd=%s
                                    """, (asset, monto, p, monto*p, monto, monto*p))

                except Exception as ex: print(f"❌ Error BingX: {ex}")

        conn.commit()
        print(f"✅ Ciclo terminado: {time.strftime('%H:%M:%S')}")

    except Exception as e:
        print(f"❌ Error General: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# --- 5. EJECUCIÓN CONTINUA ---
if __name__ == "__main__":
    print("🚀 MOTOR DE SALDOS INICIADO (Ciclo de 2 minutos)")
    print("💡 Consejo: Transfiere tus USDT de 'Standard' a 'Perpetual' en BingX para ver el total.")
    
    while True:
        actualizar_saldos()
        # Aquí recuperamos los 2 minutos de espera
        print("💤 Esperando 2 minutos para la próxima actualización...")
        time.sleep(120)