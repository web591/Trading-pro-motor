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
    # Limpiamos locks de más de 10 min por si hubo un crash previo
    cursor.execute("DELETE FROM sys_locks WHERE lock_time < NOW() - INTERVAL 10 MINUTE")
    try:
        cursor.execute(
            "INSERT INTO sys_locks (lock_name, locked_by, lock_time) VALUES (%s, %s, NOW())",
            (lock_name, identifier)
        )
        return True
    except:
        return False

def liberar_lock_manual():
    """ Función de emergencia para limpiar el lock si el script se detiene """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("DELETE FROM sys_locks WHERE lock_name = 'price_sync_lock'")
        conn.commit()
        conn.close()
        print("🔓 [SISTEMA] Lock liberado correctamente.")
    except:
        pass

# --- MOTOR PRINCIPAL ---
def actualizar_precios():
    conn = None
    px = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
    identificador = "GITHUB_ACTION" if os.getenv('GITHUB_ACTIONS') == 'true' else "LOCAL_PC"

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)

        if not obtener_lock(cur, 'price_sync_lock', identificador):
            # Verificamos quién tiene el lock para informar
            cur.execute("SELECT locked_by, lock_time FROM sys_locks WHERE lock_name = 'price_sync_lock'")
            info = cur.fetchone()
            print(f"⚠️ [LOCK] Ocupado por {info['locked_by']} desde {info['lock_time']}. Abortando...")
            return False # Retornamos False para saber que no se ejecutó
        
        conn.commit() 

        cur.execute("SELECT id, nombre_comun, motor_fuente, ticker_motor FROM sys_traductor_simbolos WHERE is_active = 1")
        activos = cur.fetchall()

        if not activos:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 💤 Sin activos.")
            cur.execute("DELETE FROM sys_locks WHERE lock_name = 'price_sync_lock'")
            conn.commit()
            return True

        print(f"\n🔄 [SYNC {datetime.now().strftime('%H:%M:%S')}] Actualizando {len(activos)} pares...")

        for ac in activos:
            tid, nombre, fuente, ticker = ac['id'], ac['nombre_comun'], ac['motor_fuente'], ac['ticker_motor']
            precio, cambio, volumen = None, 0, 0
            
            try:
                # LÓGICA DE MOTORES (BINANCE, BINGX, YAHOO)
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
                    conn.commit() 
                    print(f"   ✅ {nombre[:15]:<15} | ${precio:>10.4f} | {cambio:>6.2f}% | {fuente}")

            except Exception as e:
                print(f"   ❌ Error en {nombre}: {str(e)[:50]}...")

        # 🔓 LIBERAR AL TERMINAR
        cur.execute("DELETE FROM sys_locks WHERE lock_name = 'price_sync_lock'")
        conn.commit()
        cur.close()
        return True

    except Exception as e:
        print(f"⚠️ Error General: {e}")
        return False
    finally:
        if conn and conn.is_connected(): conn.close()

if __name__ == "__main__":
    print("💎 MOTOR DE PRECIOS V1.03 - MODO DUAL ACTIVO")
    is_github = os.getenv('GITHUB_ACTIONS') == 'true'
    
    try:
        if is_github:
            actualizar_precios()
        else:
            while True:
                actualizar_precios()
                time.sleep(30)
    except KeyboardInterrupt:
        print("\n🛑 Detención manual detectada.")
    finally:
        # Esto se ejecuta SIEMPRE al cerrar (error, crash o Ctrl+C)
        print("🧹 Limpiando recursos antes de salir...")
        liberar_lock_manual()
        print("👋 Adiós.")