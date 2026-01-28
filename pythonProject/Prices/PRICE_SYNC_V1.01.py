import mysql.connector
import time
import requests
import random
import yfinance as yf
from datetime import datetime
from config import DB_CONFIG, FINNHUB_KEY

# ==========================================================
# 💎 PRICE SYNC V1.01 - (Corregido para estructura ID)
# ==========================================================

def get_headers():
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    ]
    return {'User-Agent': random.choice(user_agents)}

def actualizar_precios():
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)

        # 1. LEER MAPA DE ACTIVOS
        cur.execute("SELECT * FROM sys_traductor_simbolos WHERE is_active = 1")
        activos = cur.fetchall()

        print(f"\n🔄 [SYNC] Actualizando {len(activos)} activos monitoreados...")
        print("-" * 60)

        for activo in activos:
            # DATOS CLAVE: Usamos el ID para vincular, no el nombre
            nombre = activo['nombre_comun']
            tid = activo['id'] 
            
            precio_final = None
            cambio_24h = 0
            fuente_detectada = "Desconocida"

            # =========================================================
            # 🚀 1. BINANCE
            # =========================================================
            if not precio_final and (activo.get('binance_spot') or activo.get('binance_usdt_future')):
                try:
                    mkt = "binance_spot" if activo.get('binance_spot') else "binance_usdt_future"
                    tk = activo[mkt]
                    base_url = "https://api.binance.com/api/v3" if "spot" in mkt else "https://fapi.binance.com/fapi/v1"
                    res = requests.get(f"{base_url}/ticker/24hr?symbol={tk}", timeout=2).json()
                    if 'lastPrice' in res:
                        precio_final = res['lastPrice']
                        cambio_24h = res['priceChangePercent']
                        fuente_detectada = "Binance"
                except: pass

            # =========================================================
            # 🚀 2. BINGX
            # =========================================================
            if not precio_final and activo.get('bingx_perp'):
                try:
                    tk_bing = activo['bingx_perp']
                    url = f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={tk_bing}"
                    res = requests.get(url, timeout=3).json()
                    if 'data' in res and res['data']:
                        precio_final = res['data'].get('lastPrice')
                        cambio_24h = res['data'].get('priceChangePercent')
                        fuente_detectada = "BingX"
                except: pass

            # =========================================================
            # 🚀 3. YAHOO
            # =========================================================
            if not precio_final and activo.get('yahoo_sym'):
                try:
                    tk_y = activo['yahoo_sym']
                    ticker = yf.Ticker(tk_y)
                    hist = ticker.history(period="2d")
                    if not hist.empty:
                        precio_final = hist['Close'].iloc[-1]
                        if len(hist) >= 2:
                            prev = hist['Close'].iloc[-2]
                            cambio_24h = ((precio_final - prev) / prev) * 100
                        fuente_detectada = "Yahoo"
                except: pass

            # =========================================================
            # 🚀 4. FINNHUB
            # =========================================================
            if not precio_final and activo.get('finnhub_sym'):
                try:
                    tk_f = activo['finnhub_sym']
                    url = f"https://finnhub.io/api/v1/quote?symbol={tk_f}&token={FINNHUB_KEY}"
                    res = requests.get(url, timeout=3).json()
                    if res.get('c') and res.get('c') > 0:
                        precio_final = res.get('c')
                        cambio_24h = res.get('dp')
                        fuente_detectada = "Finnhub"
                except: pass

            # =========================================================
            # 💾 GUARDADO (Ahora usando traductor_id correctamente)
            # =========================================================
            if precio_final is not None:
                try:
                    p_clean = float(str(precio_final).replace(',', ''))
                    c_clean = float(str(cambio_24h).replace(',', ''))
                    
                    # AQUÍ ESTABA EL ERROR: Ahora la tabla sí tiene la columna traductor_id
                    sql = """
                        INSERT INTO sys_precios_activos (traductor_id, price, change_24h, source, last_update)
                        VALUES (%s, %s, %s, %s, NOW())
                        ON DUPLICATE KEY UPDATE 
                            price = VALUES(price), 
                            change_24h = VALUES(change_24h), 
                            source = VALUES(source), 
                            last_update = NOW()
                    """
                    cur.execute(sql, (tid, p_clean, c_clean, fuente_detectada))
                    print(f"   ✅ {nombre[:10]:<10} | ${p_clean:>10.2f} | {c_clean:>6.2f}% | Fuente: {fuente_detectada}")
                
                except Exception as e:
                    print(f"   ⚠️ Error SQL ({nombre}): {e}")
            else:
                print(f"   ❌ {nombre:<10} | Sin datos.")

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"⚠️ Error General de Conexión: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

if __name__ == "__main__":
    print("\n💎 PRICE SYNC V1.01 - ONLINE")
    while True:
        actualizar_precios()
        print("\n⏳ Esperando 60 segundos...")
        time.sleep(60)