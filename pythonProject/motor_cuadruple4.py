import mysql.connector
import time
import requests
import yfinance as yf
from datetime import datetime, timedelta
import config
import random
import logging

# --- CONFIGURACIÓN DE AUDITORÍA Y LÍMITES ---
VERSION = "42.0 Final Edition"
UMBRAL_LIMPIEZA_HORAS = 24  # Borra precios viejos cada 24h para ahorrar espacio
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
}

# Silenciar logs innecesarios para Hostinger
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

def get_db_connection():
    return mysql.connector.connect(**config.DB_CONFIG)

# --- [1. MANTENIMIENTO DE BASE DE DATOS] ---

def limpiar_datos_viejos(conn):
    """ Evita que la DB de Hostinger colapse eliminando registros de más de 24 horas """
    try:
        cur = conn.cursor()
        fecha_limite = datetime.now() - timedelta(hours=UMBRAL_LIMPIEZA_HORAS)
        cur.execute("DELETE FROM sys_precios_activos WHERE last_update < %s", (fecha_limite,))
        conn.commit()
        print(f"   🧹 [MANTENIMIENTO] Registros antiguos eliminados ({UMBRAL_LIMPIEZA_HORAS}h).")
        cur.close()
    except Exception as e:
        print(f"   ⚠️ Error en limpieza: {e}")

# --- [2. CAPTURADORES DE DATOS (8 URLs)] ---

def get_binance(symbol, sub):
    if not symbol: return None
    # sub: api (spot), fapi (usdt-m), dapi (coin-m)
    url = f"https://{sub}.binance.com/{'api/v3' if sub=='api' else sub+'/v1'}/ticker/24hr?symbol={symbol}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code == 200:
            d = r.json()
            if isinstance(d, list): d = d[0]
            return {'p': float(d['lastPrice']), 'c': float(d['priceChangePercent']), 'v': float(d.get('quoteVolume', 0))}
    except: pass
    return None

def get_bingx(symbol, endpoint):
    if not symbol: return None
    # endpoint: swap/v2, swap/v1, spot/v1
    url = f"https://open-api.bingx.com/openApi/{endpoint}/ticker/24hr?symbol={symbol}"
    if 'swap' in endpoint: url = url.replace('ticker/24hr', 'quote/ticker')
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code == 200:
            d = r.json()['data']
            if isinstance(d, list): d = d[0]
            return {'p': float(d['lastPrice']), 'c': float(d['priceChangePercent']), 'v': float(d.get('volume', 0))}
    except: pass
    return None

def get_yahoo(symbol):
    if not symbol: return None
    try:
        tk = yf.Ticker(symbol)
        f = tk.fast_info
        h = tk.history(period="2d", progress=False)
        p = f.last_price
        c = ((p - h['Close'].iloc[-2]) / h['Close'].iloc[-2]) * 100 if len(h) > 1 else 0
        # Pedimos info completa (Nombre/Mcap) solo aleatoriamente para evitar baneo
        inf = tk.info if random.random() < 0.2 else {}
        return {'p': p, 'c': c, 'v': f.last_volume, 'mcap': inf.get('marketCap', 0), 'n': inf.get('longName', symbol)}
    except: pass
    return None

def get_finnhub(symbol):
    if not symbol: return None
    key = getattr(config, 'FINNHUB_KEY', '')
    try:
        r = requests.get(f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={key}", timeout=10)
        p = r.json()
        # Intentamos obtener nombre y mcap si es la primera vez
        rf = requests.get(f"https://finnhub.io/api/v1/stock/profile2?symbol={symbol}&token={key}", timeout=10)
        f = rf.json()
        return {'p': float(p['c']), 'c': float(p['dp']), 'v': 0, 'mcap': f.get('marketCapitalization', 0)*1000000, 'n': f.get('name', symbol)}
    except: pass
    return None

# --- [3. LÓGICA DE AUDITORÍA Y AUTO-ESCRITURA] ---

def auditoria_maestra(conn, activo_id, nombre_comun):
    """ Busca y escribe automáticamente tickers faltantes en la tabla maestra """
    hallazgos = {}
    # 1. Probar en Binance Spot
    try:
        test_bn = f"{nombre_comun}USDT"
        if requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={test_bn}", timeout=5).status_code == 200:
            hallazgos['binance_spot'] = test_bn
    except: pass

    # 2. Probar en Finnhub
    key = getattr(config, 'FINNHUB_KEY', '')
    try:
        r = requests.get(f"https://finnhub.io/api/v1/search?q={nombre_comun}&token={key}", timeout=5)
        res = r.json().get('result', [])
        if res: hallazgos['finnhub_sym'] = res[0]['symbol']
    except: pass

    if hallazgos:
        cur = conn.cursor()
        set_clause = ", ".join([f"{k} = %s" for k in hallazgos.keys()])
        cur.execute(f"UPDATE sys_traductor_simbolos SET {set_clause} WHERE id = %s", list(hallazgos.values()) + [activo_id])
        conn.commit()
        cur.close()
        return hallazgos
    return None

# --- [4. CICLO PRINCIPAL] ---

def ejecutar_motor():
    print(f"🚀 INICIANDO MOTOR V42 | {VERSION}")
    print(f"⏰ Hora de inicio: {datetime.now().strftime('%H:%M:%S')}")
    
    while True:
        try:
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)
            
            # Mantenimiento preventivo
            limpiar_datos_viejos(conn)

            cur.execute("SELECT * FROM sys_traductor_simbolos WHERE is_active = 1")
            activos = cur.fetchall()

            for a in activos:
                ticker_ref = a['nombre_comun']
                
                # --- AUDITORÍA DE MAESTRA ---
                # Si le faltan tickers clave, el motor los busca y los escribe solo
                if not a['binance_spot'] or not a['finnhub_sym']:
                    nuevos = auditoria_maestra(conn, a['id'], ticker_ref)
                    if nuevos:
                        print(f"   🔎 [MAESTRA ACTUALIZADA] {ticker_ref}: {list(nuevos.keys())}")
                        for k, v in nuevos.items(): a[k] = v

                # --- MAPEADO DE LAS 8 URLs ---
                rutas = [
                    ('binance_spot', lambda: get_binance(a['binance_spot'], 'api')),
                    ('binance_usdt_future', lambda: get_binance(a['binance_usdt_future'], 'fapi')),
                    ('binance_coin_future', lambda: get_binance(a['binance_coin_future'], 'dapi')),
                    ('bingx_perp', lambda: get_bingx(a['bingx_perp'], 'swap/v2')),
                    ('bingx_std', lambda: get_bingx(a['bingx_std'], 'swap/v1')),
                    ('bingx_spot', lambda: get_bingx(a['bingx_spot'], 'spot/v1')),
                    ('yahoo_sym', lambda: get_yahoo(a['yahoo_sym'])),
                    ('finnhub_sym', lambda: get_finnhub(a['finnhub_sym']))
                ]

                exitos = 0
                for source_id, func in rutas:
                    if not a.get(source_id): continue
                    
                    res = func()
                    if res and res['p'] > 0:
                        # 1. Guardar precio en tiempo real
                        cur.execute("""
                            INSERT INTO sys_precios_activos (symbol, price, change_24h, volume_24h, source, last_update)
                            VALUES (%s, %s, %s, %s, %s, NOW())
                            ON DUPLICATE KEY UPDATE price=%s, change_24h=%s, volume_24h=%s, last_update=NOW()
                        """, (ticker_ref, res['p'], res['c'], res.get('v', 0), source_id,
                              res['p'], res['c'], res.get('v', 0)))
                        
                        # 2. Guardar Fundamentales y Nombre Comercial (Si vienen de Yahoo/Finnhub)
                        if res.get('mcap'):
                            cur.execute("""
                                INSERT INTO sys_info_activos (symbol, nombre_comercial, market_cap, source_info, last_update)
                                VALUES (%s, %s, %s, %s, NOW())
                                ON DUPLICATE KEY UPDATE nombre_comercial=%s, market_cap=%s, source_info=%s, last_update=NOW()
                            """, (ticker_ref, res['n'], res['mcap'], source_id, res['n'], res['mcap'], source_id))
                        exitos += 1

                conn.commit()
                # Contador visual de hits
                print(f"   ✅ {ticker_ref:8} | {exitos}/8 fuentes sincronizadas.")
                
                # Micro-pausa Hostinger (vital)
                time.sleep(random.uniform(0.4, 1.0))

            cur.close()
            conn.close()
            print(f"--- Ciclo completado. Descanso de 60s para enfriar CPU ---")
            time.sleep(60)

        except Exception as e:
            print(f"❌ Error crítico en ciclo: {e}")
            time.sleep(20)

if __name__ == "__main__":
    ejecutar_motor()