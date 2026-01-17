import mysql.connector
import time
import requests
import yfinance as yf
from datetime import datetime
import config

def get_db_connection():
    return mysql.connector.connect(**config.DB_CONFIG)

# --- ESCUDO DE ANOMALÍAS ---
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

# --- CAPTURA DE PRECIOS ---
def get_price_data(a):
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

        # 2. Cascada de Respaldo
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
        precio_actual = t.fast_info['last_price']
        # El cambio de 24h ahora es real comparando con el cierre previo
        precio_ayer = t.info.get('previousClose', precio_actual)
        cambio = 0
        if precio_ayer and precio_ayer != 0:
            cambio = ((precio_actual - precio_ayer) / precio_ayer) * 100
        return {'price': precio_actual, 'change': round(cambio, 2), 'volume': t.fast_info.get('last_volume', 0)}
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

# --- GESTIÓN DE FUNDAMENTALES (CORREGIDA PARA EVITAR COLISIONES) ---
def enriquecer_datos(conn, a):
    ahora = datetime.now()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT last_fundamental_update FROM sys_info_activos WHERE symbol = %s", (a['nombre_comun'],))
        reg = cursor.fetchone()

        if not reg or (time.time() - reg['last_fundamental_update'].timestamp() > 86400):
            sector, industry, mcap, nombre = "N/A", "N/A", 0, a['nombre_comun']
            
            # --- LÓGICA DE DETECCIÓN DE TIPO ---
            es_cripto = "binance" in a['prioridad_precio'] or "bingx" in a['prioridad_precio']
            
            # Intentamos construir el Ticker de Yahoo de forma agresiva
            tickers_a_probar = []
            
            if a['yahoo_sym']: 
                tickers_a_probar.append(a['yahoo_sym'])
            
            if es_cripto:
                # Si es cripto, la prioridad absoluta es TICKER-USD
                tickers_a_probar.append(f"{a['nombre_comun']}-USD")
            else:
                # Si es acción, el ticker directo
                tickers_a_probar.append(a['nombre_comun'])

            # --- PROCESO DE EXTRACCIÓN ---
            inf = None
            for tk in tickers_a_probar:
                try:
                    t = yf.Ticker(tk)
                    temp_inf = t.info
                    # Validamos que el nombre no sea el mismo que el ticker (señal de fallo)
                    if temp_inf and len(temp_inf) > 5:
                        inf = temp_inf
                        break
                except: continue

            if inf:
                nombre = inf.get('longName') or inf.get('shortName') or a['nombre_comun']
                sector = inf.get('sector', 'Cripto/Commodity' if es_cripto else 'N/A')
                industry = inf.get('industry', 'N/A')
                mcap = inf.get('marketCap') or inf.get('totalMarketCap') or 0
            else:
                # Si todo falla, al menos limpiamos el nombre comercial del ticker
                nombre = a['nombre_comun']

            # --- GUARDADO ---
            sql = """INSERT INTO sys_info_activos 
                     (symbol, nombre_comercial, sector, industry, market_cap, last_fundamental_update) 
                     VALUES (%s, %s, %s, %s, %s, NOW()) 
                     ON DUPLICATE KEY UPDATE 
                     nombre_comercial=%s, sector=%s, industry=%s, market_cap=%s, last_fundamental_update=NOW()"""
            
            cursor.execute(sql, (a['nombre_comun'], nombre, sector, industry, mcap, 
                                 nombre, sector, industry, mcap))
            conn.commit()
            print(f"   ℹ️ {a['nombre_comun']} validado como: {nombre}")
            
        cursor.close()
    except Exception as e:
        print(f"   ❌ Error en fundamentales: {e}")

# --- MOTOR PRINCIPAL ---
def motor():
    print("🚀 MOTOR V6 INICIADO - CONEXIÓN BLINDADA + ESCUDO + FUNDAMENTALES")
    while True:
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            # Solo procesamos activos activos
            cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE is_active = 1")
            activos = cursor.fetchall()
            cursor.close()

            print(f"\n⏰ Ciclo: {time.strftime('%H:%M:%S')}")
            for a in activos:
                res_raw, fuente_raw = get_price_data(a)
                # Aplicamos el escudo de anomalías (T, GOLD, etc)
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
                    
                    # Actualizamos fundamentales (nombre, sector, etc)
                    enriquecer_datos(conn, a)
                    print(f"   ✅ {a['nombre_comun']:7} | ${res['price']:<10.2f} | {fuente}")
                
        except Exception as e:
            print(f"❌ Error de Ciclo: {e}")
        finally:
            if conn and conn.is_connected(): conn.close()
        
        time.sleep(60)

if __name__ == "__main__":
    motor()