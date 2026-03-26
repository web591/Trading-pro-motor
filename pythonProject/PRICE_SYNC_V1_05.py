import mysql.connector
import time
import requests
import yfinance as yf
from datetime import datetime
import os
import sys

# --- IMPORTACIÓN DUAL ---
try:
    import config
    DB_CONFIG = config.DB_CONFIG
    FINNHUB_KEY = config.FINNHUB_KEY
    PROXY_URL = getattr(config, 'PROXY_URL', None)
    if PROXY_URL:
        print(f"🌐 [SISTEMA] Proxy detectado: {PROXY_URL[:15]}...")
except ImportError:
    print("❌ [ERROR] No se encontró config.py.")
    sys.exit(1)

# --- FUNCIONES DE LOCK ---
def obtener_lock(cursor, lock_name, identifier):
    """ Intenta obtener un bloqueo en la tabla sys_locks """
    # Limpiar locks viejos (más de 10 minutos) por si hubo un crash
    cursor.execute("DELETE FROM sys_locks WHERE lock_time < NOW() - INTERVAL 10 MINUTE")
    
    try:
        cursor.execute(
            "INSERT INTO sys_locks (lock_name, locked_by, lock_time) VALUES (%s, %s, NOW())",
            (lock_name, identifier)
        )
        return True
    except:
        return False

def liberar_lock(cursor, lock_name):
    """ Libera el bloqueo """
    cursor.execute("DELETE FROM sys_locks WHERE lock_name = %s", (lock_name,))

# --- MOTOR PRINCIPAL ---
def actualizar_precios():
    conn = None
    px = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
    identificador = "GITHUB_ACTION" if os.getenv('GITHUB_ACTIONS') == 'true' else "LOCAL_PC"

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)

        # 🛡️ INTENTO DE LOCK
        if not obtener_lock(cur, 'price_sync_lock', identificador):
            print(f"⚠️ [LOCK] El motor ya está siendo ejecutado por otra instancia. Abortando...")
            return
        
        conn.commit() # Confirmamos el lock en la BD

        cur.execute("SELECT id, nombre_comun, motor_fuente, ticker_motor FROM sys_traductor_simbolos WHERE is_active = 1")
        activos = cur.fetchall()

        if not activos:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 💤 Sin activos.")
            liberar_lock(cur, 'price_sync_lock')
            conn.commit()
            return

        print(f"\n🔄 [SYNC {datetime.now().strftime('%H:%M:%S')}] Actualizando {len(activos)} pares...")

        for ac in activos:
            tid, nombre, fuente, ticker = ac['id'], ac['nombre_comun'], ac['motor_fuente'], ac['ticker_motor']
            precio, cambio, volumen = None, 0, 0
            
            try:
                # LÓGICA DE MOTORES (BINANCE, BINGX, YAHOO, FINNHUB)
                if 'binance' in fuente:
                    if "spot" in fuente: base_url = "https://api.binance.com/api/v3"
                    elif "coin_future" in fuente or "_perp" in ticker.lower(): base_url = "https://dapi.binance.com/dapi/v1"
                    else: base_url = "https://fapi.binance.com/fapi/v1"
                    res = requests.get(f"{base_url}/ticker/24hr?symbol={ticker}", proxies=px, timeout=10).json()
                    if isinstance(res, list) and len(res) > 0: res = res[0]
                    if 'lastPrice' in res:
                        precio, cambio, volumen = float(res['lastPrice']), float(res.get('priceChangePercent', 0)), float(res.get('volume', 0))

                elif 'bingx' in fuente:
                    url = f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={ticker}"
                    res = requests.get(url, proxies=px, timeout=10).json()
                    if 'data' in res and res['data']:
                        d = res['data']
                        precio, cambio, volumen = float(d.get('lastPrice', 0)), float(d.get('priceChangePercent', 0)), float(d.get('volume24h', 0))

                elif 'yahoo' in fuente:
                    tk = yf.Ticker(ticker)
                    try:
                        precio = tk.fast_info.last_price
                        volumen = tk.fast_info.last_volume
                    except:
                        hist = tk.history(period="1d")
                        if not hist.empty:
                            precio, volumen = hist['Close'].iloc[-1], hist['Volume'].iloc[-1]
                    hist_2d = tk.history(period="2d")
                    if len(hist_2d) > 1:
                        prev_close = hist_2d['Close'].iloc[-2]
                        if prev_close > 0 and precio: cambio = ((precio - prev_close) / prev_close) * 100

                # GUARDADO EN BD
                if precio is not None and precio > 0:
                    sql = """
                        INSERT INTO sys_precios_activos (traductor_id, price, change_24h, volume_24h, source, last_update)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON DUPLICATE KEY UPDATE 
                            price = VALUES(price), change_24h = VALUES(change_24h), 
                            volume_24h = VALUES(volume_24h), source = VALUES(source), last_update = NOW()
                    """
                    cur.execute(sql, (tid, precio, cambio, volumen, fuente))
                    conn.commit() # Commit por cada registro para evitar Lock Wait Timeouts
                    print(f"   ✅ {nombre[:15]:<15} | ${precio:>10.4f} | {cambio:>6.2f}% | {fuente}")

            except Exception as e:
                print(f"   ❌ Error en {nombre}: {str(e)[:50]}...")

        # 🔓 LIBERAR LOCK AL TERMINAR
        liberar_lock(cur, 'price_sync_lock')
        conn.commit()
        cur.close()

    except Exception as e:
        print(f"⚠️ Error General: {e}")
    finally:
        if conn and conn.is_connected(): conn.close()

if __name__ == "__main__":
    print("💎 MOTOR DE PRECIOS V1.03 - MODO DUAL CON LOCK")
    is_github = os.getenv('GITHUB_ACTIONS') == 'true'
    
    if is_github:
        actualizar_precios()
    else:
        while True:
            actualizar_precios()
            time.sleep(30)