import mysql.connector
import time
import requests
import yfinance as yf
from datetime import datetime
import config

def get_db_connection():
    return mysql.connector.connect(**config.DB_CONFIG)

# --- ESCUDO DE ANOMALÍAS (NUEVO) ---
def validar_precio_logico(nombre, res, fuente, a):
    """
    Evita que acciones como AT&T (T) tomen precios de criptos de $0.01
    o que el Oro (GOLD) tome precios de ETFs de $40.
    """
    if not res: return None, fuente

    # Lista de activos sensibles a colisiones de nombres o símbolos incorrectos
    acciones_indices = ['T', 'GOLD', 'SILVER', 'WTI', 'DAX', 'AAPL', 'NVDA']
    
    if nombre in acciones_indices:
        # Si el precio es menor a un umbral lógico, forzamos Yahoo con el símbolo correcto
        if (nombre == 'T' and res['price'] < 5.0) or \
           (nombre == 'GOLD' and res['price'] < 1000.0) or \
           (nombre == 'SILVER' and res['price'] < 5.0) or \
           (nombre == 'WTI' and res['price'] < 10.0):
            
            # Re-intentamos específicamente con Yahoo usando el símbolo del traductor
            nuevo_res = get_yahoo_price(a['yahoo_sym'])
            if nuevo_res:
                return nuevo_res, "yahoo_shield"
            
    return res, fuente

# --- CAPTURA DE PRECIOS (JERARQUÍA REDUNDANTE) ---
def get_price_data(a):
    nombre = a['nombre_comun']
    prio = a['prioridad_precio']
    
    res, fuente = None, ""
    try:
        # 1. Intentar por Prioridad Principal
        if prio == 'yahoo_sym':
            res = get_yahoo_price(a['yahoo_sym']); fuente = "yahoo"
        elif "binance" in prio:
            res = get_binance_public(a[prio], prio); fuente = prio
        elif "bingx" in prio:
            res = get_bingx_public(a[prio]); fuente = prio

        # 2. Cascada de Respaldo si falla la prioridad
        if not res and a['yahoo_sym']:
            res = get_yahoo_price(a['yahoo_sym']); fuente = "fallback_yahoo"
        if not res and a['finnhub_sym']:
            res = get_finnhub_price(a['finnhub_sym']); fuente = "fallback_finnhub"
    except: pass
    return res, fuente

def get_binance_public(symbol, segment):
    base = "fapi" if "usdt_future" in segment else "dapi" if "coin_future" in segment else "api"
    url = f"https://{base}.binance.com/{base}/v1/ticker/24hr?symbol={symbol}" if base != "api" else f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
    try:
        r = requests.get(url, timeout=5).json()
        d = r[0] if isinstance(r, list) else r
        return {'price': float(d['lastPrice']), 'change': float(d['priceChangePercent']), 'volume': float(d.get('quoteVolume', 0))}
    except: return None

def get_yahoo_price(symbol):
    try:
        t = yf.Ticker(symbol)
        p = t.fast_info['last_price']
        return {'price': p, 'change': 0, 'volume': 0}
    except: return None

def get_bingx_public(symbol):
    try:
        r = requests.get(f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={symbol}").json()
        if r['code'] == 0:
            return {'price': float(r['data']['lastPrice']), 'change': float(r['data']['priceChangePercent']), 'volume': float(r['data']['volume'])}
    except: return None

def get_finnhub_price(symbol):
    try:
        r = requests.get(f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={config.FINNHUB_KEY}").json()
        return {'price': r['c'], 'change': r['dp'], 'volume': 0}
    except: return None

# --- GESTIÓN DE FUNDAMENTALES (CON HORARIO ALPHA) ---
def enriquecer_datos(conn, a):
    ahora = datetime.now()
    permitir_alpha = 2 <= ahora.hour <= 4
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT last_fundamental_update FROM sys_info_activos WHERE symbol = %s", (a['nombre_comun'],))
        reg = cursor.fetchone()

        if not reg or (time.time() - reg['last_fundamental_update'].timestamp() > 86400):
            sector, industry, mcap = "N/A", "N/A", 0
            try:
                t = yf.Ticker(a['yahoo_sym'])
                sector, industry, mcap = t.info.get('sector'), t.info.get('industry'), t.info.get('marketCap', 0)
            except:
                if permitir_alpha and a['alpha_sym']:
                    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={a['alpha_sym']}&apikey={config.ALPHA_VANTAGE_KEY}"
                    r = requests.get(url).json()
                    sector, mcap = r.get('Sector'), r.get('MarketCapitalization', 0)

            cursor.execute("INSERT INTO sys_info_activos (symbol, sector, industry, market_cap) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE sector=%s, industry=%s, market_cap=%s, last_fundamental_update=NOW()",
                         (a['nombre_comun'], sector, industry, mcap, sector, industry, mcap))
            conn.commit()
        cursor.close()
    except: pass

# --- MOTOR PRINCIPAL ---
def motor():
    print("🚀 MOTOR V6 INICIADO - CONEXIÓN BLINDADA + ESCUDO")
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
                # 1. Obtener datos por jerarquía original
                res_raw, fuente_raw = get_price_data(a)
                
                # 2. Aplicar Escudo de Anomalías (Solo afecta a activos en lista negra)
                res, fuente = validar_precio_logico(a['nombre_comun'], res_raw, fuente_raw, a)

                if res:
                    conn.ping(reconnect=True, attempts=3, delay=2)
                    cur = conn.cursor()
                    cur.execute("INSERT INTO sys_precios_activos (symbol, price, change_24h, volume_24h, source, last_update) VALUES (%s, %s, %s, %s, %s, NOW()) ON DUPLICATE KEY UPDATE price=%s, change_24h=%s, volume_24h=%s, source=%s, last_update=NOW()",
                                (a['nombre_comun'], res['price'], res['change'], res['volume'], fuente, res['price'], res['change'], res['volume'], fuente))
                    conn.commit()
                    cur.close()
                    
                    enriquecer_datos(conn, a)
                    print(f"   ✅ {a['nombre_comun']:7} | ${res['price']:<10.2f} | {fuente}")
                
        except Exception as e:
            print(f"❌ Error de Ciclo: {e}")
        finally:
            if conn and conn.is_connected(): conn.close()
        
        time.sleep(60)

if __name__ == "__main__":
    motor()