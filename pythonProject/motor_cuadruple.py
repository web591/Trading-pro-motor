import mysql.connector
import time
import requests
import yfinance as yf
from binance.client import Client
import config

def get_db_connection():
    return mysql.connector.connect(**config.DB_CONFIG)

# --- CONECTORES CRYPTO ---
def get_binance_data(symbol, segment):
    try:
        if segment == 'binance_spot': url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
        elif segment == 'binance_usdt_future': url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}"
        elif segment == 'binance_coin_future': url = f"https://dapi.binance.com/dapi/v1/ticker/24hr?symbol={symbol}"
        else: return None
        r = requests.get(url, timeout=5).json()
        d = r[0] if isinstance(r, list) else r
        vol = float(d.get('quoteVolume', 0)) if segment != 'binance_coin_future' else float(d.get('baseVolume', 0))
        return {'price': float(d['lastPrice']), 'change': float(d['priceChangePercent']), 'volume': vol}
    except: return None

def get_bingx_data(symbol):
    try:
        url = f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={symbol}"
        r = requests.get(url, timeout=5).json()
        if r.get('code') == 0:
            d = r['data']
            return {'price': float(d['lastPrice']), 'change': float(d['priceChangePercent']), 'volume': float(d['volume'])}
    except: return None

# --- CONECTORES TRADICIONALES (JERARQUÍA DE RESPALDO) ---

def get_yahoo_data(symbol):
    """ NIVEL 1: Yahoo Finance """
    try:
        t = yf.Ticker(symbol)
        price = t.fast_info['last_price']
        change = 0
        try:
            hist = t.history(period="2d")
            if len(hist) > 1:
                prev = hist['Close'].iloc[-2]
                change = ((price - prev) / prev) * 100
        except: pass
        return {'price': float(price), 'change': float(change), 'volume': 0}
    except: return None

def get_finnhub_data(symbol):
    """ NIVEL 2: Finnhub (Solo para acciones) """
    try:
        url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={config.FINNHUB_KEY}"
        r = requests.get(url, timeout=5).json()
        if r.get('c'):
            return {'price': float(r['c']), 'change': float(r['dp']), 'volume': 0}
    except: return None

def get_alpha_data(symbol):
    """ NIVEL 3: Alpha Vantage (Último recurso) """
    try:
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={config.ALPHA_VANTAGE_KEY}"
        r = requests.get(url, timeout=5).json()
        if "Global Quote" in r:
            d = r["Global Quote"]
            return {'price': float(d["05. price"]), 'change': float(d["10. change percent"].replace('%','')), 'volume': 0}
    except: return None

# --- FUNDAMENTALES HÍBRIDOS ---

def actualizar_fundamentales(nombre, a):
    """ 
    Estrategia de Protección:
    1. Solo actualiza una vez cada 24 horas.
    2. Prioriza Yahoo (Gratis).
    3. Alpha Vantage es el ÚLTIMO recurso y tiene un límite estricto de 25/día.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 1. Verificar cuándo fue la última actualización exitosa
        cursor.execute("SELECT last_fundamental_update FROM sys_info_activos WHERE symbol = %s", (nombre,))
        reg = cursor.fetchone()

        # Si ya actualizamos hace menos de 24 horas, no hacemos NADA.
        if reg and (time.time() - reg['last_fundamental_update'].timestamp() < 86400):
            cursor.close()
            conn.close()
            return 

        print(f"   ℹ️ Buscando fundamentales para {nombre}...")
        sector, industry, mcap = "N/A", "N/A", 0
        exito_yahoo = False

        # Intento A: Yahoo (Ilimitado y Gratis)
        try:
            t = yf.Ticker(a['yahoo_sym'])
            info = t.info
            sector = info.get('sector', 'N/A')
            industry = info.get('industry', 'N/A')
            mcap = info.get('marketCap', 0)
            if sector != 'N/A': exito_yahoo = True
        except:
            exito_yahoo = False

        # Intento B: Alpha Vantage (SOLO si Yahoo falló y tenemos el símbolo)
        if not exito_yahoo and a['alpha_sym']:
            print(f"   ⚠️ Yahoo falló para {nombre}. Usando cuota de Alpha Vantage...")
            try:
                # Pausa de seguridad para no saturar el límite de Alpha (5 llamadas/minuto)
                time.sleep(15) 
                url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={a['alpha_sym']}&apikey={config.ALPHA_VANTAGE_KEY}"
                r = requests.get(url, timeout=10).json()
                
                # Validar que no hayamos recibido el mensaje de límite de API
                if "Note" in r:
                    print(f"   ❌ Límite de Alpha Vantage alcanzado por hoy.")
                else:
                    sector = r.get('Sector', sector)
                    industry = r.get('Industry', industry)
                    mcap = r.get('MarketCapitalization', mcap)
            except:
                pass

        # Guardar resultados
        query = """
            INSERT INTO sys_info_activos (symbol, sector, industry, market_cap) 
            VALUES (%s, %s, %s, %s) 
            ON DUPLICATE KEY UPDATE sector=%s, industry=%s, market_cap=%s, last_fundamental_update=NOW()
        """
        cursor.execute(query, (nombre, sector, industry, mcap, sector, industry, mcap))
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"   ⚠️ Error en fundamentales de {nombre}: {e}")

# --- MOTOR PRINCIPAL ---

def motor():
    print("⚡ MOTOR ULTRA-REDUNDANTE ACTIVO (Binance/BingX -> Yahoo -> Finnhub -> Alpha)")
    while True:
        print(f"\n⏰ Ciclo: {time.strftime('%H:%M:%S')}")
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE is_active = 1")
        activos = cursor.fetchall()
        cursor.close()
        conn.close()

        for a in activos:
            nombre = a['nombre_comun']
            prioridad = a['prioridad_precio']
            res, fuente_final = None, ""

            # 1. Prioridad Principal (Binance/BingX/Yahoo)
            if prioridad == 'yahoo_sym': res = get_yahoo_data(a['yahoo_sym']); fuente_final = "yahoo"
            elif "binance" in prioridad: res = get_binance_data(a[prioridad], prioridad); fuente_final = prioridad
            elif "bingx" in prioridad: res = get_bingx_data(a[prioridad]); fuente_final = prioridad

            # 2. Respaldo 1: Yahoo (si la prioridad no era Yahoo y falló)
            if not res and a['yahoo_sym']:
                res = get_yahoo_data(a['yahoo_sym']); fuente_final = "fallback_yahoo"

            # 3. Respaldo 2: Finnhub (solo para acciones)
            if not res and a['finnhub_sym']:
                res = get_finnhub_data(a['finnhub_sym']); fuente_final = "fallback_finnhub"

            # 4. Respaldo 3: Alpha Vantage
            if not res and a['alpha_sym']:
                res = get_alpha_data(a['alpha_sym']); fuente_final = "fallback_alpha"

            if res:
                # Guardar en DB
                conn = get_db_connection()
                cursor = conn.cursor()
                query = "INSERT INTO sys_precios_activos (symbol, price, change_24h, volume_24h, source, last_update) VALUES (%s, %s, %s, %s, %s, NOW()) ON DUPLICATE KEY UPDATE price=%s, change_24h=%s, volume_24h=%s, source=%s, last_update=NOW()"
                cursor.execute(query, (nombre, res['price'], res['change'], res['volume'], fuente_final, res['price'], res['change'], res['volume'], fuente_final))
                conn.commit()
                cursor.close()
                conn.close()

                actualizar_fundamentales(nombre, a)
                print(f"   ✅ {nombre:7} | ${res['price']:<10.2f} | {res['change']:>6.2f}% | {fuente_final}")
            else:
                print(f"   ❌ {nombre}: Fallo en todas las fuentes.")

        time.sleep(60)

if __name__ == "__main__":
    motor()