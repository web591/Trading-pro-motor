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
    MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)
    if not MASTER_KEY:
        print("❌ ERROR: No se encontró ENCRYPTION_KEY en config.py")
        sys.exit(1)
except ImportError:
    print("❌ ERROR: No se encontró config.py")
    sys.exit(1)

# --- 2. FUNCIÓN DE DESCIFRADO ---
def descifrar_dato(texto_base64, master_key, nombre_campo="dato"):
    try:
        if not texto_base64: return None
        raw_combined = base64.b64decode(texto_base64.strip())
        if b"::" in raw_combined:
            partes = raw_combined.split(b"::")
            cifrado, iv = partes[0], partes[1]
        else:
            decoded_text = raw_combined.decode('utf-8')
            if "::" in decoded_text:
                p = decoded_text.split("::")
                cifrado, iv = base64.b64decode(p[0]), base64.b64decode(p[1])
            else: return None
        if len(iv) != 16: return None
        key_hash = sha256(master_key.encode('utf-8')).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        decrypted_raw = cipher.decrypt(cifrado)
        return unpad(decrypted_raw, AES.block_size).decode('utf-8').strip()
    except:
        return None

# --- 3. OBTENER PRECIOS ---
def obtener_precio_db(cursor, asset):
    if asset in ['USDT', 'USDC', 'DAI', 'BUSD', 'LDUSDT', 'LDUSDC', 'LDBUSD']: 
        return 1.0
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
        cursor.execute("SELECT broker_name, api_key, api_secret FROM api_keys WHERE user_id = 6 AND status = 1")
        keys_list = cursor.fetchall()

        for registro in keys_list:
            broker = registro['broker_name'].lower()
            key = descifrar_dato(registro['api_key'], MASTER_KEY)
            sec = descifrar_dato(registro['api_secret'], MASTER_KEY)
            if not key or not sec: continue

            # --- LÓGICA BINANCE ---
            if broker == 'binance':
                print("🤖 Sincronizando Binance...")
                try:
                    client = Client(key, sec)
                    # Spot y Cash
                    for b in client.get_account()['balances']:
                        total = float(b['free']) + float(b['locked'])
                        if total > 0.0001:
                            p = obtener_precio_db(cursor, b['asset'])
                            tipo = 'CASH' if b['asset'] in ['USDT', 'USDC'] else 'SPOT'
                            cursor.execute("""
                                INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, precio_referencia, valor_usd)
                                VALUES (6, 'binance', %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE cantidad_total=%s, cantidad_disponible=%s, precio_referencia=%s, valor_usd=%s
                            """, (tipo, b['asset'], total, float(b['free']), p, total*p, total, float(b['free']), p, total*p))
                    # Futuros Binance
                    try:
                        for f in client.futures_account()['assets']:
                            wb = float(f['walletBalance'])
                            if wb > 0.01:
                                p = 1.0 if f['asset'] == 'USDT' else obtener_precio_db(cursor, f['asset'])
                                pnl = float(f.get('unrealizedProfit', 0))
                                cursor.execute("""
                                    INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, pnl_no_realizado, precio_referencia, valor_usd)
                                    VALUES (6, 'binance', 'PERPETUAL', %s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE cantidad_total=%s, pnl_no_realizado=%s, precio_referencia=%s, valor_usd=%s
                                """, (f['asset'], wb, pnl, p, (wb+pnl)*p, wb, pnl, p, (wb+pnl)*p))
                    except: pass
                except Exception as eb: print(f"❌ Error Binance: {eb}")

            # --- LÓGICA BINGX ---
            elif broker == 'bingx':
                print("🟠 Sincronizando BingX (Todas las carteras)...")
                try:
                    def bx_req(path, params={}):
                        params['timestamp'] = int(time.time() * 1000)
                        qs = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
                        sig = hmac.new(sec.encode('utf-8'), qs.encode('utf-8'), sha256).hexdigest()
                        headers = {'X-BX-APIKEY': key, 'Content-Type': 'application/json'}
                        url = f"https://open-api.bingx.com{path}?{qs}&signature={sig}"
                        return requests.get(url, headers=headers).json()

                    # 1. SPOT BINGX
                    s_res = bx_req("/openApi/spot/v1/account/balance")
                    if s_res.get('code') == 0 and 'data' in s_res:
                        for b in s_res['data']['balances']:
                            total = float(b['free']) + float(b['locked'])
                            if total > 0.0001:
                                p = obtener_precio_db(cursor, b['asset'])
                                cursor.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, cantidad_disponible, precio_referencia, valor_usd) VALUES (6, 'bingx', 'SPOT', %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE cantidad_total=%s, cantidad_disponible=%s, precio_referencia=%s, valor_usd=%s", (b['asset'], total, float(b['free']), p, total*p, total, float(b['free']), p, total*p))

                    # 2. FUTUROS PERPETUOS (PRO/V2)
                    f_res = bx_req("/openApi/swap/v2/user/balance")
                    if f_res.get('code') == 0 and isinstance(f_res.get('data'), list):
                        for f in f_res['data']:
                            wb = float(f.get('balance', 0))
                            if wb > 0.01:
                                asset = f.get('asset', 'USDT')
                                p = 1.0 if asset == 'USDT' else obtener_precio_db(cursor, asset)
                                pnl = float(f.get('unrealizedProfit', 0))
                                cursor.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, pnl_no_realizado, precio_referencia, valor_usd) VALUES (6, 'bingx', 'PERPETUAL_PRO', %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE cantidad_total=%s, pnl_no_realizado=%s, precio_referencia=%s, valor_usd=%s", (asset, wb, pnl, p, (wb+pnl)*p, wb, pnl, p, (wb+pnl)*p))

                    # 3. FUTUROS ESTÁNDAR (TRADICIONALES)
                    st_res = bx_req("/openApi/swap/v1/user/balance")
                    if st_res.get('code') == 0 and 'data' in st_res:
                        # El formato de estándar a veces varía, validamos si es lista o objeto
                        balances = st_res['data'] if isinstance(st_res['data'], list) else [st_res['data']]
                        for st in balances:
                            wb = float(st.get('balance', 0))
                            if wb > 0.01:
                                asset = st.get('asset', 'USDT')
                                p = 1.0 if asset == 'USDT' else obtener_precio_db(cursor, asset)
                                cursor.execute("INSERT INTO sys_saldos_usuarios (user_id, broker_name, tipo_cuenta, asset, cantidad_total, precio_referencia, valor_usd) VALUES (6, 'bingx', 'STANDARD_FUT', %s, %s, %s, %s) ON DUPLICATE KEY UPDATE cantidad_total=%s, precio_referencia=%s, valor_usd=%s", (asset, wb, p, wb*p, wb, p, wb*p))

                except Exception as ex: print(f"❌ Error BingX: {ex}")

        conn.commit()
        print(f"✅ Ciclo terminado: {time.strftime('%H:%M:%S')}")
    except Exception as e: print(f"❌ Error General: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    print("🚀 Motor de Saldos Iniciado")
    while True:
        actualizar_saldos()
        print("💤 Esperando 2 minutos...")
        time.sleep(120)