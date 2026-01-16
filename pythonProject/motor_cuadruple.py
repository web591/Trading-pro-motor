import mysql.connector
import time
import requests
import yfinance as yf
from binance.client import Client
import config

# --- CONECTORES (Iguales a los anteriores, no cambian) ---
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

def get_yahoo_data(symbol):
    try:
        t = yf.Ticker(symbol)
        price = None
        try: price = t.fast_info['last_price']
        except: pass
        if price is None:
            hist = t.history(period="1d")
            if not hist.empty: price = hist['Close'].iloc[-1]
        if price is None: return None
        change = 0
        try:
            hist_2d = t.history(period="2d")
            if len(hist_2d) > 1:
                prev = hist_2d['Close'].iloc[-2]
                change = ((price - prev) / prev) * 100
        except: pass
        return {'price': float(price), 'change': float(change), 'volume': 0}
    except: return None

# --- FUNCIONES DE BASE DE DATOS (AHORA RECIBEN LA CONEXIÓN) ---

def actualizar_fundamentales(conn, nombre, a):
    """ Usa la conexión existente para no crear una nueva """
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT last_fundamental_update FROM sys_info_activos WHERE symbol = %s", (nombre,))
        reg = cursor.fetchone()

        if not reg or (time.time() - reg['last_fundamental_update'].timestamp() > 86400):
            print(f"   ℹ️ Buscando fundamentales para {nombre}...")
            t = yf.Ticker(a['yahoo_sym'])
            info = t.info
            sector = info.get('sector', 'Cripto/Commodity')
            industry = info.get('industry', 'N/A')
            mcap = info.get('marketCap', 0)

            query = """INSERT INTO sys_info_activos (symbol, sector, industry, market_cap) 
                       VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE sector=%s, industry=%s, market_cap=%s, last_fundamental_update=NOW()"""
            cursor.execute(query, (nombre, sector, industry, mcap, sector, industry, mcap))
            conn.commit()
        cursor.close()
    except: pass

def guardar_datos(conn, nombre, data, fuente):
    """ Usa la conexión existente """
    try:
        cursor = conn.cursor()
        query = """INSERT INTO sys_precios_activos (symbol, price, change_24h, volume_24h, source, last_update) 
                   VALUES (%s, %s, %s, %s, %s, NOW()) ON DUPLICATE KEY UPDATE price=%s, change_24h=%s, volume_24h=%s, source=%s, last_update=NOW()"""
        cursor.execute(query, (nombre, data['price'], data['change'], data['volume'], fuente, data['price'], data['change'], data['volume'], fuente))
        conn.commit()
        cursor.close()
    except: pass

# --- MOTOR PRINCIPAL OPTIMIZADO ---

def motor():
    print("⚡ MOTOR OPTIMIZADO (Ahorro de conexiones DB activo)")
    while True:
        print(f"\n⏰ Ciclo: {time.strftime('%H:%M:%S')}")
        
        conn = None
        try:
            # 1. ABRIR UNA SOLA CONEXIÓN POR MINUTO
            conn = mysql.connector.connect(**config.DB_CONFIG)
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE is_active = 1")
            activos = cursor.fetchall()
            cursor.close()

            for a in activos:
                nombre = a['nombre_comun']
                prioridad = a['prioridad_precio']
                res, fuente_final = None, ""

                # Lógica de captura (Igual que antes)
                if prioridad == 'yahoo_sym': res = get_yahoo_data(a['yahoo_sym']); fuente_final = "yahoo"
                elif "binance" in prioridad: res = get_binance_data(a[prioridad], prioridad); fuente_final = prioridad
                elif "bingx" in prioridad: res = get_bingx_data(a[prioridad]); fuente_final = prioridad
                
                if not res and a['yahoo_sym']:
                    res = get_yahoo_data(a['yahoo_sym']); fuente_final = "fallback_yahoo"

                if res:
                    # Pasamos 'conn' a las funciones para reusar la conexión
                    guardar_datos(conn, nombre, res, fuente_final)
                    if a['yahoo_sym']:
                        actualizar_fundamentales(conn, nombre, a)
                    print(f"   ✅ {nombre:7} | ${res['price']:<10.2f} | {fuente_final}")
                else:
                    print(f"   ❌ {nombre}: Error")

        except mysql.connector.Error as err:
            print(f"❌ Error de MySQL: {err}. Reintentando en 60s...")
        finally:
            # 2. CERRAR LA CONEXIÓN AL FINAL DEL CICLO
            if conn and conn.is_connected():
                conn.close()

        time.sleep(60)

if __name__ == "__main__":
    motor()