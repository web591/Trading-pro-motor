
# Importamos la configuración. 
# Si PROXY_URL no existe en config.py, el motor lo ignorará (ideal para tu PC)
try:
    from config import DB_CONFIG, FINNHUB_KEY, PROXY_URL
except ImportError:import mysql.connector
import time
import requests
import random
import yfinance as yf
from datetime import datetime
import os

    from config import DB_CONFIG, FINNHUB_KEY
    PROXY_URL = None

# ==========================================================
# 💎 PRICE SYNC V1.03 - MONITOR DE PRECIOS REAL (MODO DUAL)
# ==========================================================

def get_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://finance.yahoo.com/'
    }

def actualizar_precios():
    conn = None
    # Configuración de Proxy para Exchanges (Binance/BingX)
    px = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)

        cur.execute("SELECT id, nombre_comun, motor_fuente, ticker_motor FROM sys_traductor_simbolos WHERE is_active = 1")
        activos = cur.fetchall()

        if not activos:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 💤 Sin activos para monitorear.")
            return

        print(f"\n🔄 [SYNC {datetime.now().strftime('%H:%M:%S')}] Actualizando {len(activos)} pares...")

        for ac in activos:
            tid, nombre, fuente, ticker = ac['id'], ac['nombre_comun'], ac['motor_fuente'], ac['ticker_motor']
            precio, cambio, volumen = None, 0, 0
            
            try:
                # ---------------------------------------------------------
                # MOTOR A: BINANCE (Con Proxy Selectivo)
                # ---------------------------------------------------------
                if 'binance' in fuente:
                    if "spot" in fuente:
                        base_url = "https://api.binance.com/api/v3"
                    elif "coin_future" in fuente or "_perp" in ticker.lower():
                        base_url = "https://dapi.binance.com/dapi/v1"
                    else:
                        base_url = "https://fapi.binance.com/fapi/v1"
                    
                    endpoint = f"{base_url}/ticker/24hr?symbol={ticker}"
                    # Se aplica proxy solo aquí para evitar bloqueos de IP
                    res = requests.get(endpoint, proxies=px, timeout=10).json()
                    
                    if isinstance(res, list) and len(res) > 0: res = res[0]
                    if 'lastPrice' in res:
                        precio = float(res['lastPrice'])
                        cambio = float(res.get('priceChangePercent', 0))
                        volumen = float(res.get('volume', 0))

                # ---------------------------------------------------------
                # MOTOR B: BINGX (Con Proxy Selectivo)
                # ---------------------------------------------------------
                elif 'bingx' in fuente:
                    url = f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={ticker}"
                    res = requests.get(url, proxies=px, timeout=10).json()
                    if 'data' in res and res['data']:
                        d = res['data']
                        precio = float(d.get('lastPrice', 0))
                        cambio = float(d.get('priceChangePercent', 0))
                        volumen = float(d.get('volume24h', 0))

                # ---------------------------------------------------------
                # MOTOR C: YAHOO FINANCE (Conexión Directa)
                # ---------------------------------------------------------
                elif 'yahoo' in fuente:
                    tk = yf.Ticker(ticker)
                    try:
                        precio = tk.fast_info.last_price
                        volumen = tk.fast_info.last_volume
                    except:
                        hist = tk.history(period="1d")
                        if not hist.empty:
                            precio = hist['Close'].iloc[-1]
                            volumen = hist['Volume'].iloc[-1]

                    hist_2d = tk.history(period="2d")
                    if len(hist_2d) > 1:
                        prev_close = hist_2d['Close'].iloc[-2]
                        if prev_close > 0 and precio:
                            cambio = ((precio - prev_close) / prev_close) * 100

                # ---------------------------------------------------------
                # MOTOR D: FINNHUB
                # ---------------------------------------------------------
                elif 'finnhub' in fuente:
                    url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_KEY}"
                    res = requests.get(url, timeout=5).json()
                    if res.get('c') and res.get('c') != 0:
                        precio = float(res.get('c'))
                        cambio = float(res.get('dp', 0))

                # ---------------------------------------------------------
                # GUARDADO EN BD
                # ---------------------------------------------------------
                if precio is not None and precio > 0:
                    sql = """
                        INSERT INTO sys_precios_activos (traductor_id, price, change_24h, volume_24h, source, last_update)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON DUPLICATE KEY UPDATE 
                            price = VALUES(price), change_24h = VALUES(change_24h), 
                            volume_24h = VALUES(volume_24h), source = VALUES(source), last_update = NOW()
                    """
                    cur.execute(sql, (tid, precio, cambio, volumen, fuente))
                    print(f"   ✅ {nombre[:15]:<15} | ${precio:>10.4f} | {cambio:>6.2f}% | {fuente}")
                else:
                    print(f"   ⚠️ {nombre} ({ticker}) | Sin datos.")

            except Exception as e:
                print(f"   ❌ Error en {nombre}: {str(e)[:50]}...")

        conn.commit()
        cur.close()

    except Exception as e:
        print(f"⚠️ Error General: {e}")
    finally:
        if conn and conn.is_connected(): conn.close()

if __name__ == "__main__":
    print("💎 MOTOR DE PRECIOS V1.03 - MODO DUAL ACTIVO")
    is_github = os.getenv('GITHUB_ACTIONS') == 'true'
    
    if is_github:
        print("🤖 [MODO CLOUD] Ejecutando ráfaga única...")
        actualizar_precios()
        print("🏁 Ciclo Cloud finalizado.")
    else:
        print("💻 [MODO LOCAL] Bucle continuo (30s)...")
        try:
            while True:
                actualizar_precios()
                print(f"\n✅ {datetime.now().strftime('%H:%M:%S')} - Esperando 30s...")
                time.sleep(30)
        except KeyboardInterrupt:
            print("\n🛑 Detenido.")