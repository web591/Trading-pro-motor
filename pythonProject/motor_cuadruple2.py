import mysql.connector
import time
import requests
import yfinance as yf
from datetime import datetime
import config
import random

def get_db_connection():
    return mysql.connector.connect(**config.DB_CONFIG)

# --- TU ESCUDO DE ANOMALÍAS (INTACTO) ---
def validar_precio_logico(nombre, res, fuente, a):
    if not res: return None, fuente
    # Activos sensibles a colisiones
    acciones_indices = ['T', 'GOLD', 'SILVER', 'WTI', 'DAX', 'AAPL', 'NVDA']
    if nombre in acciones_indices:
        if (nombre == 'T' and res['price'] < 5.0) or \
           (nombre == 'GOLD' and res['price'] < 1000.0) or \
           (nombre == 'SILVER' and res['price'] < 5.0) or \
           (nombre == 'WTI' and res['price'] < 10.0):
            
            nuevo_res = get_yahoo_price(a['yahoo_sym'])
            if nuevo_res:
                return nuevo_res, "yahoo_shield"
    return res, fuente

# --- CAPTURA DE PRECIOS (CON SESIÓN BLINDADA) ---
def get_price_data(a, session):
    prio = a['prioridad_precio']
    res, fuente = None, ""
    try:
        # 1. Intentar por Prioridad Principal
        if prio == 'yahoo_sym':
            res = get_yahoo_price(a['yahoo_sym']); fuente = "yahoo"
        elif "binance" in prio:
            res = get_binance_public(a[prio], prio, session); fuente = prio
        elif "bingx" in prio:
            res = get_bingx_public(a[prio], session); fuente = prio

        # 2. Cascada de Respaldo
        if not res and a['yahoo_sym']:
            res = get_yahoo_price(a['yahoo_sym']); fuente = "fallback_yahoo"
        if not res and a['finnhub_sym']:
            res = get_finnhub_price(a['finnhub_sym']); fuente = "fallback_finnhub"
    except: pass
    return res, fuente

def get_binance_public(symbol, segment, session):
    base = "fapi" if "usdt_future" in segment else "dapi" if "coin_future" in segment else "api"
    url = f"https://{base}.binance.com/{base}/v1/ticker/24hr?symbol={symbol}" if base != "api" else f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
    try:
        r = session.get(url, timeout=5).json()
        d = r[0] if isinstance(r, list) else r
        return {'price': float(d['lastPrice']), 'change': float(d['priceChangePercent']), 'volume': float(d.get('quoteVolume', 0))}
    except: return None

def get_yahoo_price(symbol):
    try:
        t = yf.Ticker(symbol)
        precio_actual = t.fast_info['last_price']
        precio_ayer = t.info.get('previousClose', precio_actual)
        cambio = 0
        if precio_ayer and precio_ayer != 0:
            cambio = ((precio_actual - precio_ayer) / precio_ayer) * 100
        return {'price': precio_actual, 'change': round(cambio, 2), 'volume': t.fast_info.get('last_volume', 0)}
    except: return None

def get_bingx_public(symbol, session):
    try:
        # Blindaje con Session y headers
        url = f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={symbol}"
        r = session.get(url, timeout=10).json()
        if r['code'] == 0:
            data = r['data']
            d = data[0] if isinstance(data, list) else data
            return {'price': float(d['lastPrice']), 'change': float(d['priceChangePercent']), 'volume': float(d['volume'])}
    except: return None

def get_finnhub_price(symbol):
    try:
        url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={config.FINNHUB_KEY}"
        r = requests.get(url, timeout=5)
        data = r.json()
        if data and 'c' in data and data['c'] != 0:
            return {'price': float(data['c']), 'change': float(data['dp']), 'volume': 0}
    except: return None

# --- TU GESTIÓN DE FUNDAMENTALES (INTACTA) ---
def enriquecer_datos(conn, a):
    ahora = datetime.now()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT last_fundamental_update FROM sys_info_activos WHERE symbol = %s", (a['nombre_comun'],))
        reg = cursor.fetchone()

        if not reg or (time.time() - reg['last_fundamental_update'].timestamp() > 86400):
            sector, industry, mcap, nombre = "N/A", "N/A", 0, a['nombre_comun']
            es_cripto = "binance" in a['prioridad_precio'] or "bingx" in a['prioridad_precio']
            
            tickers_a_probar = []
            if a['yahoo_sym']: tickers_a_probar.append(a['yahoo_sym'])
            if es_cripto: tickers_a_probar.append(f"{a['nombre_comun']}-USD")
            else: tickers_a_probar.append(a['nombre_comun'])

            inf = None
            for tk in tickers_a_probar:
                try:
                    t = yf.Ticker(tk)
                    temp_inf = t.info
                    if temp_inf and len(temp_inf) > 5:
                        inf = temp_inf
                        break
                except: continue

            if inf:
                nombre = inf.get('longName') or inf.get('shortName') or a['nombre_comun']
                sector = inf.get('sector', 'Cripto/Commodity' if es_cripto else 'N/A')
                industry = inf.get('industry', 'N/A')
                mcap = inf.get('marketCap') or inf.get('totalMarketCap') or 0

            sql = """INSERT INTO sys_info_activos 
                     (symbol, nombre_comercial, sector, industry, market_cap, last_fundamental_update) 
                     VALUES (%s, %s, %s, %s, %s, NOW()) 
                     ON DUPLICATE KEY UPDATE 
                     nombre_comercial=%s, sector=%s, industry=%s, market_cap=%s, last_fundamental_update=NOW()"""
            
            cursor.execute(sql, (a['nombre_comun'], nombre, sector, industry, mcap, 
                                 nombre, sector, industry, mcap))
            conn.commit()
            print(f"    ℹ️ {a['nombre_comun']} validado como: {nombre}")
            
        cursor.close()
    except Exception as e:
        print(f"    ❌ Error en fundamentales: {e}")

# --- MOTOR PRINCIPAL ---
def motor():
    print("🚀 MOTOR V8 - BASE ORIGINAL + BLINDAJE BINGX")
    
    # Session persistente para evitar bloqueos
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })

    while True:
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE is_active = 1")
            activos = cursor.fetchall()
            cursor.close()

            print(f"\n⏰ Ciclo: {time.strftime('%H:%M:%S')}")
            for a in activos:
                res_raw, fuente_raw = get_price_data(a, session)
                res, fuente = validar_precio_logico(a['nombre_comun'], res_raw, fuente_raw, a)

                if res:
                    conn.ping(reconnect=True, attempts=3, delay=2)
                    cur = conn.cursor()
                    cur.execute("""INSERT INTO sys_precios_activos 
                                   (symbol, price, change_24h, volume_24h, source, last_update) 
                                   VALUES (%s, %s, %s, %s, %s, NOW()) 
                                   ON DUPLICATE KEY UPDATE 
                                   price=%s, change_24h=%s, volume_24h=%s, source=%s, last_update=NOW()""",
                                (a['nombre_comun'], res['price'], res['change'], res['volume'], fuente, 
                                 res['price'], res['change'], res['volume'], fuente))
                    conn.commit()
                    cur.close()
                    
                    enriquecer_datos(conn, a)
                    # Formato de 8 decimales para activos como ONE
                    print(f"   ✅ {a['nombre_comun']:7} | ${res['price']:<12.8f} | {fuente}")
                
                time.sleep(0.5) # Pausa anti-spam
                
        except Exception as e:
            print(f"❌ Error de Ciclo: {e}")
        finally:
            if conn and conn.is_connected(): conn.close()
        
        time.sleep(60)

if __name__ == "__main__":
    motor()