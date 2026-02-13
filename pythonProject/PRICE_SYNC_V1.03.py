import mysql.connector
import time
import requests
import random
import yfinance as yf
from datetime import datetime
from config import DB_CONFIG, FINNHUB_KEY

# ==========================================================
# 💎 PRICE SYNC V1.02 - MONITOR DE PRECIOS REAL
# ==========================================================
# 1. Headers tipo navegador (Mozilla) para evitar bloqueos
# 2. Soporte para Binance, BingX, Yahoo, Finnhub
# 3. Lógica de Pares Únicos (Lee sys_traductor_simbolos)
# ==========================================================

def get_headers():
    # Mozilla fijo y robusto para evitar bloqueos de Yahoo/Finnhub
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://finance.yahoo.com/'
    }

def actualizar_precios():
    conn = None
    try:
        # Conexión fresca en cada ciclo para evitar "MySQL server has gone away"
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)

        # 1. LEER EL CATÁLOGO ACTIVO
        # Solo traemos lo que los usuarios realmente están siguiendo (is_active = 1)
        cur.execute("SELECT id, nombre_comun, motor_fuente, ticker_motor FROM sys_traductor_simbolos WHERE is_active = 1")
        activos = cur.fetchall()

        if not activos:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 💤 Sin activos para monitorear.")
            return

        print(f"\n🔄 [SYNC {datetime.now().strftime('%H:%M:%S')}] Actualizando {len(activos)} pares...")

        for ac in activos:
            tid = ac['id']
            nombre = ac['nombre_comun']
            fuente = ac['motor_fuente']   # Ej: 'binance_spot', 'yahoo_sym'
            ticker = ac['ticker_motor']   # Ej: 'BTCUSDT', 'AAPL'
            
            precio = None
            cambio = 0
            volumen = 0
            
            try:
                # ---------------------------------------------------------
                # MOTOR A: BINANCE (Spot, USDT-Future y COIN-Future)
                # ---------------------------------------------------------
                if 'binance' in fuente:
                    if "spot" in fuente:
                        base_url = "https://api.binance.com/api/v3"
                    elif "coin_future" in fuente or "_perp" in ticker.lower():
                        # Los pares _PERP viven en la DAPI (COIN-M)
                        base_url = "https://dapi.binance.com/dapi/v1"
                    else:
                        # Los pares USDT viven en la FAPI (USDT-M)
                        base_url = "https://fapi.binance.com/fapi/v1"
                    
                    # Para DAPI (COIN-M), el endpoint de 24h es ligeramente distinto
                    endpoint = f"{base_url}/ticker/24hr?symbol={ticker}"
                    res = requests.get(endpoint, timeout=5).json()
                    
                    # Las APIs de Futuros de Binance a veces devuelven una LISTA de un solo objeto
                    if isinstance(res, list) and len(res) > 0:
                        res = res[0]
                    
                    if 'lastPrice' in res:
                        precio = float(res['lastPrice'])
                        cambio = float(res.get('priceChangePercent', 0))
                        volumen = float(res.get('volume', 0))

                # ---------------------------------------------------------
                # MOTOR B: BINGX (Cripto Alternativo)
                # ---------------------------------------------------------
                elif 'bingx' in fuente:
                    url = f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={ticker}"
                    res = requests.get(url, timeout=5).json()
                    if 'data' in res and res['data']:
                        d = res['data']
                        precio = float(d.get('lastPrice', 0))
                        cambio = float(d.get('priceChangePercent', 0))
                        volumen = float(d.get('volume24h', 0))

                # ---------------------------------------------------------
                # MOTOR C: YAHOO FINANCE (Stocks, Forex, Indices)
                # ---------------------------------------------------------
                elif 'yahoo' in fuente:
                    # Usamos la librería yfinance que ya maneja cookies internamente, 
                    # pero si falla, tenemos los headers listos.
                    tk = yf.Ticker(ticker)
                    
                    # Intentamos obtener precio rápido
                    try:
                        precio = tk.fast_info.last_price
                        volumen = tk.fast_info.last_volume
                    except:
                        # Fallback si fast_info falla
                        hist = tk.history(period="1d")
                        if not hist.empty:
                            precio = hist['Close'].iloc[-1]
                            volumen = hist['Volume'].iloc[-1]

                    # Calculamos el cambio % manual porque Yahoo a veces da null en changePercent
                    hist_2d = tk.history(period="2d")
                    if len(hist_2d) > 1:
                        prev_close = hist_2d['Close'].iloc[-2]
                        if prev_close > 0 and precio:
                            cambio = ((precio - prev_close) / prev_close) * 100

                # ---------------------------------------------------------
                # MOTOR D: FINNHUB (Respaldo Forex/Stocks EEUU)
                # ---------------------------------------------------------
                elif 'finnhub' in fuente:
                    url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_KEY}"
                    # Aquí NO mandamos headers Mozilla para no confundir a la API key
                    res = requests.get(url, timeout=5).json()
                    
                    # Finnhub devuelve: c (current), dp (percent change)
                    if res.get('c') and res.get('c') != 0:
                        precio = float(res.get('c'))
                        cambio = float(res.get('dp', 0))
                        volumen = 0 # Finnhub gratuito no siempre da volumen realtime

                # ---------------------------------------------------------
                # GUARDADO EN BASE DE DATOS
                # ---------------------------------------------------------
                if precio is not None and precio > 0:
                    sql = """
                        INSERT INTO sys_precios_activos (traductor_id, price, change_24h, volume_24h, source, last_update)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON DUPLICATE KEY UPDATE 
                            price = VALUES(price), 
                            change_24h = VALUES(change_24h), 
                            volume_24h = VALUES(volume_24h),
                            source = VALUES(source),
                            last_update = NOW()
                    """
                    cur.execute(sql, (tid, precio, cambio, volumen, fuente))
                    
                    # Salida visual bonita en consola
                    simbolo_log = f"{nombre} ({ticker})"
                    print(f"   ✅ {simbolo_log[:20]:<20} | ${precio:>10.4f} | {cambio:>6.2f}% | {fuente}")
                else:
                    print(f"   ⚠️ {nombre} ({ticker}) | Datos vacíos o error de conexión.")

            except Exception as e:
                print(f"   ❌ Error procesando {nombre}: {str(e)[:50]}...")

        conn.commit()
        cur.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"⚠️ Error de Base de Datos: {err}")
    except Exception as e:
        print(f"⚠️ Error Crítico General: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

# ==========================================================
# BUCLE INFINITO (Ciclos)
# ==========================================================
if __name__ == "__main__":
    print("💎 MOTOR DE PRECIOS V1.02 INICIADO")
    print("   (Binance, BingX, Yahoo, Finnhub habilitados)")
    print("   Presiona Ctrl+C para detener.")
    
    while True:
        actualizar_precios()
        
        # Espera 60 segundos antes de la siguiente vuelta para no saturar
        print("\n⏳ Esperando 60 segundos...")
        time.sleep(60)