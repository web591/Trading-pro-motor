import mysql.connector
import time
import requests
import yfinance as yf
from datetime import datetime, timedelta
import config
import random
import logging

# --- CONFIGURACIÓN DE AUDITORÍA ---
VERSION = "44.0 - Rotación de Identidad"
UMBRAL_LIMPIEZA = 24

# Silenciar logs
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# [CORRECCIÓN] LISTA DE AGENTES ROTATIVOS
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
]

def get_header():
    """ Genera una identidad diferente en cada llamada """
    return {'User-Agent': random.choice(USER_AGENTS)}

def get_db_connection():
    return mysql.connector.connect(**config.DB_CONFIG)

# --- [NORMALIZADOR] ---
def normalizar_ticker_yahoo(symbol):
    correcciones = {
        'GOLD': 'GC=F', 'SILVER': 'SI=F', 'WTI': 'CL=F',
        'EURUSD': 'EURUSD=X', 'GBPUSD': 'GBPUSD=X', 'NZDUSD': 'NZDUSD=X', 'AUDUSD': 'AUDUSD=X',
        'SP500': '^GSPC', 'DAX': '^GDAXI'
    }
    return correcciones.get(symbol, symbol)

# --- [MANTENIMIENTO DB] ---
def limpiar_datos_viejos(conn):
    try:
        cur = conn.cursor()
        limite = datetime.now() - timedelta(hours=UMBRAL_LIMPIEZA)
        cur.execute("DELETE FROM sys_precios_activos WHERE last_update < %s", (limite,))
        conn.commit()
        cur.close()
    except: pass

# --- [AUTO-CAZADOR] ---
def auditoria_maestra(conn, activo_id, nombre):
    hallazgos = {}
    try:
        # Prueba en Binance
        test_bn = f"{nombre}USDT"
        if requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={test_bn}", timeout=5).status_code == 200:
            hallazgos['binance_spot'] = test_bn
    except: pass
    
    # Prueba en Finnhub
    try:
        k = getattr(config, 'FINNHUB_KEY', '')
        r = requests.get(f"https://finnhub.io/api/v1/search?q={nombre}&token={k}", timeout=5)
        res = r.json().get('result', [])
        if res: hallazgos['finnhub_sym'] = res[0]['symbol']
    except: pass

    if hallazgos:
        cur = conn.cursor()
        set_q = ", ".join([f"{k} = %s" for k in hallazgos.keys()])
        cur.execute(f"UPDATE sys_traductor_simbolos SET {set_q} WHERE id = %s", list(hallazgos.values()) + [activo_id])
        conn.commit()
        cur.close()
        return hallazgos
    return None

# --- [CAPTURADORES (Usa get_header() dinámico)] ---

def get_binance(symbol, sub):
    if not symbol: return None
    url = f"https://{sub}.binance.com/{'api/v3' if sub=='api' else sub+'/v1'}/ticker/24hr?symbol={symbol}"
    try:
        r = requests.get(url, headers=get_header(), timeout=12)
        if r.status_code == 200:
            d = r.json()
            if isinstance(d, list): d = d[0]
            return {'p': float(d['lastPrice']), 'c': float(d['priceChangePercent']), 'v': float(d.get('quoteVolume', 0))}
    except: pass
    return None

def get_bingx(symbol, endpoint):
    if not symbol: return None
    url = f"https://open-api.bingx.com/openApi/{endpoint}/ticker/24hr?symbol={symbol}"
    if 'swap' in endpoint: url = url.replace('ticker/24hr', 'quote/ticker')
    try:
        r = requests.get(url, headers=get_header(), timeout=12)
        if r.status_code == 200:
            d = r.json()['data']
            if isinstance(d, list): d = d[0]
            return {'p': float(d['lastPrice']), 'c': float(d['priceChangePercent']), 'v': float(d.get('volume', 0))}
    except: pass
    return None

def get_yahoo_v2(symbol):
    if not symbol: return None
    ticker_corr = normalizar_ticker_yahoo(symbol)
    try:
        tk = yf.Ticker(ticker_corr)
        # yfinance gestiona sus propios headers, pero añadimos retardo natural
        h = tk.history(period="2d", interval="1d")
        if h.empty: return None
        price = h['Close'].iloc[-1]
        prev = h['Close'].iloc[-2] if len(h) > 1 else price
        change = ((price - prev) / prev) * 100
        
        # Fundamentales (30% probabilidad)
        name, mcap = symbol, 0
        if random.random() < 0.3 and "=" not in ticker_corr:
            i = tk.info
            name = i.get('longName', symbol)
            mcap = i.get('marketCap', 0)
            
        return {'p': float(price), 'c': float(change), 'v': 0, 'n': name, 'mcap': mcap}
    except: return None

def get_finnhub_v2(symbol):
    if not symbol: return None
    k = getattr(config, 'FINNHUB_KEY', '')
    try:
        r = requests.get(f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={k}", headers=get_header(), timeout=10)
        d = r.json()
        if d.get('c'):
            return {'p': float(d['c']), 'c': float(d['dp']), 'v': 0, 'n': symbol}
    except: pass
    return None

# --- [CICLO PRINCIPAL] ---

def ejecutar_motor():
    print(f"🚀 MOTOR V44 | {VERSION} | {datetime.now().strftime('%H:%M:%S')}")
    
    while True:
        try:
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)
            limpiar_datos_viejos(conn) # Mantenimiento Hostinger

            cur.execute("SELECT * FROM sys_traductor_simbolos WHERE is_active = 1")
            activos = cur.fetchall()

            for a in activos:
                name = a['nombre_comun']

                # 1. Auditoría de Maestra (Auto-Cazador)
                if not a['binance_spot']:
                    nuevos = auditoria_maestra(conn, a['id'], name)
                    if nuevos:
                        print(f"   🔎 [AUTO-FIX] {name} -> {list(nuevos.keys())}")
                        for k,v in nuevos.items(): a[k]=v

                # 2. Mapeo de 8 URLs con Funciones Blindadas
                rutas = [
                    ('binance_spot', lambda: get_binance(a['binance_spot'], 'api')),
                    ('binance_usdt_future', lambda: get_binance(a['binance_usdt_future'], 'fapi')),
                    ('binance_coin_future', lambda: get_binance(a['binance_coin_future'], 'dapi')),
                    ('bingx_perp', lambda: get_bingx(a['bingx_perp'], 'swap/v2')),
                    ('bingx_std', lambda: get_bingx(a['bingx_std'], 'swap/v1')),
                    ('bingx_spot', lambda: get_bingx(a['bingx_spot'], 'spot/v1')),
                    ('yahoo_sym', lambda: get_yahoo_v2(a['yahoo_sym'] or name)),
                    ('finnhub_sym', lambda: get_finnhub_v2(a['finnhub_sym'] or name))
                ]

                hits = 0
                for sid, func in rutas:
                    # Filtro inteligente: no ejecutamos si está vacío y no es Yahoo/Finnhub (que prueban con el nombre común)
                    if not a.get(sid) and sid not in ['yahoo_sym', 'finnhub_sym']: continue
                    
                    res = func()
                    if res and res['p'] > 0:
                        # Guardar Precio
                        cur.execute("""
                            INSERT INTO sys_precios_activos (symbol, price, change_24h, volume_24h, source, last_update)
                            VALUES (%s, %s, %s, %s, %s, NOW())
                            ON DUPLICATE KEY UPDATE price=%s, change_24h=%s, volume_24h=%s, last_update=NOW()
                        """, (name, res['p'], res['c'], res.get('v', 0), sid, res['p'], res['c'], res.get('v', 0)))
                        
                        # Guardar Fundamental
                        if res.get('mcap'):
                            cur.execute("""
                                INSERT INTO sys_info_activos (symbol, nombre_comercial, market_cap, source_info, last_update)
                                VALUES (%s, %s, %s, %s, NOW())
                                ON DUPLICATE KEY UPDATE nombre_comercial=%s, market_cap=%s, source_info=%s, last_update=NOW()
                            """, (name, res.get('n', name), res['mcap'], sid, res.get('n', name), res['mcap'], sid))
                        hits += 1

                conn.commit()
                print(f"   ✅ {name:8} | {hits}/8 fuentes OK.")
                
                # Pausa variable para simular humano en Hostinger
                time.sleep(random.uniform(0.5, 1.2))

            cur.close()
            conn.close()
            print(f"--- Ciclo completo. Enfriamiento 60s ---")
            time.sleep(60)

        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(20)

if __name__ == "__main__":
    ejecutar_motor()