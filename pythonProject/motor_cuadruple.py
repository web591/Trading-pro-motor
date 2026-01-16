import mysql.connector
import time
import requests
import yfinance as yf
from binance.client import Client
import config

binance_client = Client(config.BINANCE_KEY, config.BINANCE_SECRET)

def get_db_connection():
    return mysql.connector.connect(**config.DB_CONFIG)

# --- CONECTORES ROBUSTOS ---

def get_yahoo_data(symbol):
    try:
        t = yf.Ticker(symbol)
        # Usamos fast_info para precio y volumen
        info = t.fast_info
        price = float(info['last_price'])
        vol = float(info['last_volume']) if 'last_volume' in info else 0
        
        # Para el cambio, intentamos una descarga rápida de hoy
        # Si falla, el cambio será 0 pero no romperá el precio
        change = 0
        try:
            hist = t.history(period="2d")
            if len(hist) > 1:
                prev = hist['Close'].iloc[-2]
                change = ((price - prev) / prev) * 100
        except: pass
        
        return {'price': price, 'change': change, 'volume': vol}
    except Exception as e:
        return None

def get_binance_data(symbol, segment):
    try:
        if segment == 'binance_spot':
            d = binance_client.get_ticker(symbol=symbol)
        elif segment == 'binance_usdt_future':
            d = binance_client.futures_ticker(symbol=symbol)
        elif segment == 'binance_coin_future':
            # Coin-M requiere este método específico
            d = binance_client.futures_coin_ticker(symbol=symbol)[0]
        
        return {
            'price': float(d['lastPrice']),
            'change': float(d['priceChangePercent']),
            'volume': float(d['quoteVolume'])
        }
    except: return None

def get_bingx_data(symbol):
    try:
        url = f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={symbol}"
        r = requests.get(url, timeout=5).json()
        if r.get('code') == 0:
            d = r['data']
            return {
                'price': float(d['lastPrice']),
                'change': float(d['priceChangePercent']),
                'volume': float(d['volume'])
            }
    except: pass
    return None

# --- MOTOR CON CASCADA ---

def motor():
    print("⚡ MOTOR V4: PRECIO/CAMBIO/VOLUMEN (RESISTENTE A ERRORES)")
    while True:
        print(f"\n🚀 Ciclo {time.strftime('%H:%M:%S')}")
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM sys_traductor_simbolos WHERE is_active = 1")
            activos = cursor.fetchall()
            cursor.close()
            conn.close()
        except: continue

        for a in activos:
            nombre = a['nombre_comun']
            prioridad = a['prioridad_precio']
            res = None
            fuente_final = ""

            # 1. INTENTAR PRIORIDAD
            if prioridad == 'yahoo_sym' and a['yahoo_sym']:
                res = get_yahoo_data(a['yahoo_sym'])
                fuente_final = "yahoo_finance"
            elif "binance" in prioridad:
                res = get_binance_data(a[prioridad], prioridad)
                fuente_final = prioridad
            elif "bingx" in prioridad:
                res = get_bingx_data(a[prioridad])
                fuente_final = prioridad

            # 2. CASCADA DE EMERGENCIA (Si el anterior falló)
            if not res:
                if a['yahoo_sym']:
                    res = get_yahoo_data(a['yahoo_sym'])
                    fuente_final = "yahoo_finance"
                elif a['finnhub_sym']:
                    # Simplificado para no romper el flujo
                    fuente_final = "finnhub"
                    # Aquí iría tu lógica de finnhub anterior...

            # GUARDAR Y MOSTRAR
            if res:
                guardar_en_db(nombre, res, fuente_final)
                print(f"   ✅ {nombre:7} | ${res['price']:<10.2f} | {res['change']:>6.2f}% | Vol: {res['volume']:,.0f} ({fuente_final})")
            else:
                print(f"   ❌ {nombre}: Sin datos disponibles.")

        time.sleep(60)

def guardar_en_db(nombre, data, fuente):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
            INSERT INTO sys_precios_activos (symbol, price, change_24h, volume_24h, source, last_update) 
            VALUES (%s, %s, %s, %s, %s, NOW()) 
            ON DUPLICATE KEY UPDATE 
            price=%s, change_24h=%s, volume_24h=%s, source=%s, last_update=NOW()
        """
        cursor.execute(query, (nombre, data['price'], data['change'], data['volume'], fuente, 
                               data['price'], data['change'], data['volume'], fuente))
        conn.commit()
        conn.close()
    except: pass

if __name__ == "__main__":
    motor()