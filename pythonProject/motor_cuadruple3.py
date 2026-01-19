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

# --- NUEVA FUNCIÓN: CAPTURA FINNHUB ---
def get_finnhub_price(symbol):
    if not symbol: return None
    try:
        # Usa la API KEY definida en tu config.py
        url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={config.FINNHUB_KEY}"
        response = requests.get(url, timeout=5)
        data = response.json()
        
        # c = Current Price, dp = Percent Change
        if data and 'c' in data and data['c'] != 0:
            return {
                'price': float(data['c']),
                'change': float(data.get('dp', 0)),
                'volume': 0 # Finnhub Quote no da volumen en plan free
            }
    except Exception as e:
        print(f"   ⚠️ Error Finnhub ({symbol}): {e}")
    return None

# --- CAPTURA YAHOO (FOREX / STOCKS) ---
def get_yahoo_price(symbol):
    if not symbol: return None
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="2d")
        if not hist.empty:
            price = hist['Close'].iloc[-1]
            prev_price = hist['Close'].iloc[-2] if len(hist) > 1 else price
            change = ((price - prev_price) / prev_price) * 100
            volume = int(hist['Volume'].iloc[-1])
            return {'price': price, 'change': change, 'volume': volume}
    except: return None
    return None

# --- CAPTURA BINGX (CON SESIÓN) ---
def get_bingx_price(symbol, session):
    if not symbol: return None
    try:
        url = f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={symbol}"
        r = session.get(url, timeout=5)
        d = r.json()
        if d['code'] == 0:
            return {
                'price': float(d['data']['lastPrice']),
                'change': float(d['data']['priceChangePercent']),
                'volume': float(d['data']['volume'])
            }
    except: return None
    return None

# --- CAPTURA BINANCE (CON SESIÓN) ---
def get_binance_price(symbol, session):
    if not symbol: return None
    try:
        url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}"
        r = session.get(url, timeout=5)
        d = r.json()
        if 'lastPrice' in d:
            return {
                'price': float(d['lastPrice']),
                'change': float(d['priceChangePercent']),
                'volume': float(d['quoteVolume'])
            }
    except: return None
    return None

# --- ENRIQUECEDOR DE DATOS (FUNDAMENTALES) ---
def enriquecer_datos(conn, a):
    try:
        # Solo enriquecer si es Yahoo o Finnhub (Stocks)
        if a['prioridad_precio'] in ['yahoo_sym', 'finnhub_sym']:
            ticker = yf.Ticker(a['yahoo_sym'])
            info = ticker.info
            mcap = int(info.get('marketCap', 0))
            name = info.get('longName', a['nombre_comun'])
            
            cur = conn.cursor()
            cur.execute("""INSERT INTO sys_info_activos (symbol, nombre_comercial, market_cap) 
                           VALUES (%s, %s, %s) 
                           ON DUPLICATE KEY UPDATE nombre_comercial=%s, market_cap=%s""",
                        (a['nombre_comun'], name, mcap, name, mcap))
            conn.commit()
            cur.close()
    except: pass

# --- SELECTOR DE DATOS (EL MOTOR 4X4) ---
def get_price_data(a, session):
    prio = a['prioridad_precio']
    res, fuente = None, ""
    try:
        # 1. Intentar por Prioridad configurada
        if prio == 'binance_usdt_future':
            res = get_binance_price(a['binance_usdt_future'], session)
            fuente = "binance"
        elif prio == 'bingx_perp':
            res = get_bingx_price(a['bingx_perp'], session)
            fuente = "bingx"
        elif prio == 'yahoo_sym':
            res = get_yahoo_price(a['yahoo_sym'])
            fuente = "yahoo"
        elif prio == 'finnhub_sym':
            res = get_finnhub_price(a['finnhub_sym'])
            fuente = "finnhub"

        # 2. SISTEMA DE RESPALDO (Failsafe)
        if not res and prio != 'yahoo_sym':
            res = get_yahoo_price(a['yahoo_sym'])
            fuente = f"yahoo_backup"
            
    except Exception as e:
        print(f"   ⚠️ Error Crítico en captura: {e}")
        
    return res, fuente

# --- MOTOR PRINCIPAL ---
def main():
    print(f"🚀 Motor Cuádruple Iniciado [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})

    while True:
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE is_active = 1")
            activos = cursor.fetchall()
            cursor.close()

            for a in activos:
                res_raw, fuente_raw = get_price_data(a, session)
                res, fuente = validar_precio_logico(a['nombre_comun'], res_raw, fuente_raw, a)

                if res:
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
                    
                    # Cada N ciclos enriquecemos fundamentales para no saturar
                    if random.random() < 0.1: 
                        enriquecer_datos(conn, a)
                        
                    print(f"   ✅ {a['nombre_comun']:7} | ${res['price']:<12.6f} | {fuente}")
                
                time.sleep(0.5) 
            
            conn.close()
            print(f"--- Ciclo completado. Durmiendo 30s ---")
            time.sleep(30)

        except Exception as e:
            print(f"❌ Error de Ciclo: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()