import mysql.connector
import time
import requests
from binance.client import Client
import finnhub
from alpha_vantage.timeseries import TimeSeries
import config

# --- CONFIGURACIÓN DE CLIENTES ---
binance_client = Client(config.BINANCE_KEY, config.BINANCE_SECRET)
finnhub_client = finnhub.Client(api_key=config.FINNHUB_KEY)

def get_db_connection():
    return mysql.connector.connect(**config.DB_CONFIG)

# --- FUNCIONES DE EXTRACCIÓN ---

def consultar_binance(symbol):
    try:
        ticker = binance_client.get_ticker(symbol=symbol.replace("/", ""))
        return {'price': float(ticker['lastPrice']), 'change': float(ticker['priceChangePercent'])}
    except: return None

def consultar_bingx(symbol):
    try:
        # Ajuste para Forex/Oro en BingX (Intentando Standard Ticker)
        # Algunos símbolos de TradFi en BingX requieren formato limpio como EURUSD
        url = f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={symbol}"
        response = requests.get(url).json()
        if response.get('code') == 0:
            data = response['data']
            return {'price': float(data['lastPrice']), 'change': float(data['priceChangePercent'])}
        return None
    except: return None

def consultar_finnhub(symbol):
    try:
        quote = finnhub_client.quote(symbol)
        if quote['c'] == 0: return None
        return {'price': float(quote['c']), 'change': float(quote['dp'])}
    except: return None

def consultar_alpha_vantage(symbol):
    try:
        ts = TimeSeries(key=config.ALPHA_VANTAGE_KEY)
        # Usamos el endpoint de intercambio de divisas para Oro/Forex
        from_c = symbol[:3]
        to_c = symbol[3:]
        data, _ = ts.get_currency_exchange_rate(from_currency=from_c, to_currency=to_c)
        return {'price': float(data['5. Exchange Rate']), 'change': 0.00}
    except: return None

# --- GUARDADO EN DB ---
def guardar_en_db(symbol, data, source):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
            INSERT INTO sys_precios_activos (symbol, price, change_24h, source) 
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE price=%s, change_24h=%s, source=%s
        """
        cursor.execute(query, (symbol, data['price'], data['change'], source, 
                               data['price'], data['change'], source))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"   ✅ {source} -> {symbol}: ${data['price']}")
    except Exception as e:
        print(f"❌ Error DB: {e}")

# --- MOTOR PRINCIPAL CON TIEMPOS ---
def motor_principal():
    minuto_actual = time.localtime().tm_min
    segundo_actual = time.localtime().tm_sec
    
    print(f"\n🚀 Ciclo {time.strftime('%H:%M:%S')}")
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT symbol, priority_source FROM sys_monedas_activas WHERE is_active = 1")
    activos = cursor.fetchall()
    conn.close()

    for item in activos:
        source = item['priority_source']
        symbol = item['symbol']
        resultado = None

        # 1. APIs de Alta Frecuencia (Binance, BingX, Finnhub)
        if source == 'BINANCE': resultado = consultar_binance(symbol)
        elif source == 'BINGX': resultado = consultar_bingx(symbol)
        elif source == 'FINNHUB': resultado = consultar_finnhub(symbol)
        
        # 2. API de Baja Frecuencia (Alpha Vantage) - Solo corre en el minuto 0 de cada hora
        elif source == 'ALPHA':
            if minuto_actual == 0:
                print(f"   ⏳ Actualizando fundamental/commodity (Alpha)...")
                resultado = consultar_alpha_vantage(symbol)
            else:
                continue # Salta este activo si no es la hora exacta

        if resultado:
            guardar_en_db(symbol, resultado, source)

if __name__ == "__main__":
    while True:
        motor_principal()
        time.sleep(60)