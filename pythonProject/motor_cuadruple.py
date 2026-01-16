import mysql.connector
import time
import requests
import yfinance as yf # <-- Nuevo integrante
from binance.client import Client
import config

binance_client = Client(config.BINANCE_KEY, config.BINANCE_SECRET)

def get_db_connection():
    return mysql.connector.connect(**config.DB_CONFIG)

# --- CONECTORES ---

def get_yahoo_price(symbol):
    """ Opción A: Yahoo Finance (Gratis y Rápido) """
    try:
        ticker = yf.Ticker(symbol)
        # Intentamos obtener el precio actual (regularMarketPrice)
        price = ticker.fast_info['last_price']
        return float(price)
    except: return None

# ... (Mantenemos get_binance_price y get_bingx_price igual que antes) ...

def get_binance_price(symbol, segment):
    try:
        if segment == 'binance_spot':
            return float(binance_client.get_symbol_ticker(symbol=symbol)['price'])
        elif segment == 'binance_usdt_future':
            return float(binance_client.futures_symbol_ticker(symbol=symbol)['price'])
        elif segment == 'binance_coin_future':
            return float(binance_client.futures_coin_symbol_ticker(symbol=symbol)[0]['price'])
    except: return None

def get_bingx_price(symbol, segment):
    try:
        url = f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={symbol}"
        r = requests.get(url, timeout=5).json()
        if r.get('code') == 0: return float(r['data']['lastPrice'])
        return None
    except: return None

def get_alpha_fallback(symbol):
    """ Opción Último Recurso """
    try:
        if symbol in ['XAUUSD', 'EURUSD'] or len(symbol) == 6:
            base = symbol[:3] if len(symbol) == 6 else "XAU"
            target = symbol[3:] if len(symbol) == 6 else "USD"
            url = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={base}&to_currency={target}&apikey={config.ALPHA_VANTAGE_KEY}"
            r = requests.get(url).json()
            return float(r["Realtime Currency Exchange Rate"]["5. Exchange Rate"])
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={config.ALPHA_VANTAGE_KEY}"
        r = requests.get(url).json()
        return float(r["Global Quote"]["05. price"])
    except: return None

# --- MOTOR CON JERARQUÍA ACTUALIZADA ---

def motor():
    print("⚡ MOTOR DE 6 MERCADOS (JERARQUÍA: BINANCE/BINGX -> YAHOO -> FINNHUB -> ALPHA)")
    while True:
        print(f"\n🚀 Ciclo {time.strftime('%H:%M:%S')}")
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE is_active = 1")
        activos = cursor.fetchall()
        cursor.close()
        conn.close()

        for a in activos:
            nombre = a['nombre_comun']
            prioridad = a['prioridad_precio']
            precio = None
            fuente_final = "ninguna"

            # 1. Intentar Exchange Primario (Binance/BingX)
            if "binance" in prioridad:
                precio = get_binance_price(a[prioridad], prioridad)
                fuente_final = prioridad
            elif "bingx" in prioridad:
                precio = get_bingx_price(a[prioridad], prioridad)
                fuente_final = prioridad

            # 2. NUEVA OPCIÓN A: Yahoo Finance (Si falla el primario o es Forex/Índice)
            if not precio and a['yahoo_sym']:
                precio = get_yahoo_price(a['yahoo_sym'])
                fuente_final = "yahoo_finance"

            # 3. Opción B: Finnhub (Para acciones si Yahoo falla)
            if not precio and a['finnhub_sym']:
                try:
                    url = f"https://finnhub.io/api/v1/quote?symbol={a['finnhub_sym']}&token={config.FINNHUB_KEY}"
                    fr = requests.get(url).json()
                    if fr.get('c'): 
                        precio = float(fr['c'])
                        fuente_final = "finnhub"
                except: pass

            # 4. Opción C: Alpha Vantage (Último recurso)
            if not precio and a['alpha_sym']:
                precio = get_alpha_fallback(a['alpha_sym'])
                fuente_final = "alpha_vantage"

            if precio:
                guardar_en_db(nombre, precio, fuente_final)
            else:
                print(f"   ❌ {nombre}: Error total de captura.")

        time.sleep(60)

def guardar_en_db(nombre, precio, fuente):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "INSERT INTO sys_precios_activos (symbol, price, source) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE price=%s, source=%s, last_update=NOW()"
        cursor.execute(query, (nombre, precio, fuente, precio, fuente))
        conn.commit()
        conn.close()
        print(f"   ✅ {nombre} -> ${precio} (vía {fuente})")
    except: pass

if __name__ == "__main__":
    motor()