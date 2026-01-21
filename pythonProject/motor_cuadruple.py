import mysql.connector
import time
import requests
import yfinance as yf
from datetime import datetime
import config
import random

def get_db_connection():
    return mysql.connector.connect(**config.DB_CONFIG)

# --- ESCUDO DE ANOMALÍAS ---
def validar_precio_logico(nombre, res, fuente, a):
    if not res: return None, fuente
    # Evita errores de Yahoo en activos de alta precisión
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

# --- FUNCIONES DE CAPTURA (Soportando tus 10 columnas) ---
HEADERS_MOZILLA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

def get_binance_price(symbol, is_future=False, is_coin_m=False):
    try:
        if is_future:
            base_url = "https://dapi.binance.com/dapi/v1/ticker/24hr" if is_coin_m else "https://fapi.binance.com/fapi/v1/ticker/24hr"
        else:
            base_url = "https://api.binance.com/api/v3/ticker/24hr"
        
        r = requests.get(f"{base_url}?symbol={symbol}", timeout=3)
        if r.status_code == 200:
            d = r.json()
            if isinstance(d, list): d = d[0]
            return {'price': float(d['lastPrice']), 'change': float(d['priceChangePercent']), 'volume': float(d['volume'])}
    except: return None

def get_bingx_price(symbol, version='v2'):
    try:
        path = "/openApi/swap/v2/quote/ticker" if version == 'v2' else "/openApi/swap/v1/ticker"
        url = f"https://open-api.bingx.com{path}?symbol={symbol}"
        r = requests.get(url, headers=HEADERS_MOZILLA, timeout=3)
        if r.status_code == 200:
            d = r.json()['data']
            if isinstance(d, list): d = d[0]
            price = d.get('lastPrice') or d.get('last_price') or d.get('price')
            change = d.get('priceChangePercent') or d.get('chg') or 0
            vol = d.get('volume') or d.get('amount') or 0
            return {'price': float(price), 'change': float(change), 'volume': float(vol)}
    except: return None

def get_bingx_spot(symbol):
    try:
        url = f"https://open-api.bingx.com/openApi/spot/v1/ticker/24hr?symbol={symbol}"
        r = requests.get(url, headers=HEADERS_MOZILLA, timeout=3)
        d = r.json()['data'][0]
        return {'price': float(d['lastPrice']), 'change': float(d['priceChangePercent']), 'volume': float(d['volume'])}
    except: return None

def get_yahoo_price(symbol):
    try:
        t = yf.Ticker(symbol)
        h = t.history(period="2d")
        if len(h) < 2: return None
        price = h['Close'].iloc[-1]
        prev_close = h['Close'].iloc[-2]
        change = ((price - prev_close) / prev_close) * 100
        return {'price': float(price), 'change': float(change), 'volume': float(h['Volume'].iloc[-1])}
    except: return None

def get_finnhub_price(symbol):
    try:
        url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={config.FINNHUB_KEY}"
        r = requests.get(url, timeout=3).json()
        return {'price': float(r['c']), 'change': float(r['dp']), 'volume': 0}
    except: return None

def get_alpha_vantage_price(symbol):
    try:
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={config.ALPHA_KEY}"
        d = requests.get(url, timeout=5).json()["Global Quote"]
        return {'price': float(d["05. price"]), 'change': float(d["10. change percent"].replace('%','')), 'volume': float(d["06. volume"])}
    except: return None

# --- ENRIQUECIMIENTO ---
def enriquecer_datos(conn, activo):
    try:
        t = yf.Ticker(activo['yahoo_sym'] if activo['yahoo_sym'] else activo['nombre_comun'])
        info = t.info
        nombre = info.get('shortName') or info.get('longName')
        mcap = info.get('marketCap', 0)
        if nombre:
            cur = conn.cursor()
            cur.execute("INSERT INTO sys_info_activos (symbol, nombre_comercial, market_cap) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE market_cap=%s",
                        (activo['nombre_comun'], nombre, mcap, mcap))
            conn.commit()
            cur.close()
    except: pass

# --- PROCESO DE BÚSQUEDA ---
def procesar_busquedas_pendientes():
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM sys_simbolos_buscados WHERE status = 'pendiente' LIMIT 5")
    busquedas = cur.fetchall()
    
    for b in busquedas:
        ticker = b['ticker']
        print(f"🔍 Buscando: {ticker}...")
        fuentes_test = [
            ('binance_usdt_future', get_binance_price(ticker + "USDT", True)),
            ('binance_spot', get_binance_price(ticker + "USDT", False)),
            ('bingx_perp', get_bingx_price(ticker + "-USDT")),
            ('yahoo_sym', get_yahoo_price(ticker))
        ]
        
        encontrado = False
        for f_name, res in fuentes_test:
            if res:
                cur.execute("""UPDATE sys_simbolos_buscados SET 
                            status='encontrado', binance_spot=%s, binance_usdt_future=%s, 
                            bingx_perp=%s, yahoo_sym=%s, prioridad_precio=%s, precio_referencia=%s 
                            WHERE id=%s""", 
                            (ticker+"USDT", ticker+"USDT", ticker+"-USDT", ticker, f_name, res['price'], b['id']))
                encontrado = True
                break
        if not encontrado:
            cur.execute("UPDATE sys_simbolos_buscados SET status='error' WHERE id=%s", (b['id'],))
            
    conn.commit()
    cur.close()
    conn.close()

# --- MOTOR PRINCIPAL ---
def motor_principal():
    print(f"🚀 Motor Cuádruple V3 - Modo Ciclo - Hostinger OK")
    while True:
        try:
            procesar_busquedas_pendientes()
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)
            cur.execute("SELECT * FROM sys_traductor_simbolos WHERE is_active = 1")
            activos = cur.fetchall()

            for a in activos:
                res_raw = None
                f_obj = a['prioridad_precio']

                # SELECTOR DINÁMICO
                if f_obj == 'binance_spot': res_raw = get_binance_price(a['binance_spot'], False)
                elif f_obj == 'binance_usdt_future': res_raw = get_binance_price(a['binance_usdt_future'], True)
                elif f_obj == 'binance_coin_future': res_raw = get_binance_price(a['binance_coin_future'], True, True)
                elif f_obj == 'bingx_perp': res_raw = get_bingx_price(a['bingx_perp'], 'v2')
                elif f_obj == 'bingx_std': res_raw = get_bingx_price(a['bingx_std'], 'v1')
                elif f_obj == 'bingx_spot': res_raw = get_bingx_spot(a['bingx_spot'])
                elif f_obj in ['yahoo_sym', 'yfinance_sym']: res_raw = get_yahoo_price(a['yahoo_sym'])
                elif f_obj == 'finnhub_sym': res_raw = get_finnhub_price(a['finnhub_sym'])
                elif f_obj == 'alpha_sym': res_raw = get_alpha_vantage_price(a['alpha_sym'])

                # RESPALDO
                if not res_raw:
                    if a['binance_usdt_future']: res_raw = get_binance_price(a['binance_usdt_future'], True); f_obj="backup_bin"
                    elif a['yahoo_sym']: res_raw = get_yahoo_price(a['yahoo_sym']); f_obj="backup_yah"

                res, fuente_final = validar_precio_logico(a['nombre_comun'], res_raw, f_obj, a)

                if res:
                    cur_upd = conn.cursor()
                    cur_upd.execute("""
                        INSERT INTO sys_precios_activos (symbol, price, change_24h, volume_24h, source, last_update)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON DUPLICATE KEY UPDATE price=%s, change_24h=%s, volume_24h=%s, source=%s, last_update=NOW()
                    """, (a['nombre_comun'], res['price'], res['change'], res['volume'], fuente_final,
                          res['price'], res['change'], res['volume'], fuente_final))
                    conn.commit()
                    cur_upd.close()
                    if random.random() < 0.05: enriquecer_datos(conn, a)
                    print(f"   📈 {a['nombre_comun']:10} | {res['price']:12.4f} | {fuente_final}")

                time.sleep(0.3) # Hostinger Friendly

            cur.close()
            conn.close()
            time.sleep(30) # Espera entre ciclos
        except Exception as e:
            print(f"❌ ERROR: {e}")
            time.sleep(10)

if __name__ == "__main__":
    motor_principal()