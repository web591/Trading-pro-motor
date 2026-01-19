import mysql.connector
import time
import requests
import yfinance as yf
from datetime import datetime
import config
import random

def get_db_connection():
    return mysql.connector.connect(**config.DB_CONFIG)

# --- TU ESCUDO DE ANOMALÍAS (MANTENIDO INTACTO) ---
def validar_precio_logico(nombre, res, fuente, a):
    if not res: return None, fuente
    # Activos sensibles a colisiones
    acciones_indices = ['T', 'GOLD', 'SILVER', 'WTI', 'DAX', 'AAPL', 'NVDA']
    if nombre in acciones_indices:
        if (nombre == 'T' and res['price'] < 5.0) or \
           (nombre == 'GOLD' and res['price'] < 1000.0) or \
           (nombre == 'SILVER' and res['price'] < 5.0) or \
           (nombre == 'WTI' and res['price'] < 10.0):
            
            # Si falla la lógica, forzamos Yahoo como respaldo seguro
            nuevo_res = get_yahoo_price(a['yahoo_sym'])
            if nuevo_res:
                return nuevo_res, "yahoo_shield"
    return res, fuente

# --- FUNCIONES DE CAPTURA (ACTUALIZADAS PARA NUEVAS COLUMNAS) ---

def get_binance_price(symbol, is_future=True, is_coin_m=False):
    if not symbol: return None
    try:
        # Lógica para elegir el endpoint correcto según el tipo de contrato
        if is_coin_m:
            url = f"https://dapi.binance.com/dapi/v1/ticker/price?symbol={symbol}"
        elif is_future:
            url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}"
        else:
            url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
            
        r = requests.get(url, timeout=3)
        if r.status_code == 200:
            data = r.json()
            # Si es Coin-M la respuesta es una lista
            if isinstance(data, list): data = data[0]
            
            return {
                'price': float(data['price'] if 'price' in data else data['lastPrice']),
                'change': float(data.get('priceChangePercent', 0)),
                'volume': float(data.get('quoteVolume', 0))
            }
    except: return None

def get_bingx_price(symbol, version='v2'):
    if not symbol: return None
    try:
        # v2 es para Perpetual, v1 es para Standard
        endpoint = "swap/v2/quote/ticker" if version == 'v2' else "swap/v1/ticker/24hr"
        url = f"https://open-api.bingx.com/openApi/{endpoint}?symbol={symbol}"
        r = requests.get(url, timeout=3)
        if r.status_code == 200:
            d = r.json()['data']
            return {
                'price': float(d['lastPrice']),
                'change': float(d['priceChangePercent']),
                'volume': float(d['volume'] if 'volume' in d else d['amount'])
            }
    except: return None

def get_yahoo_price(symbol):
    if not symbol: return None
    try:
        tk = yf.Ticker(symbol)
        inf = tk.fast_info
        return {
            'price': inf['last_price'],
            'change': ((inf['last_price'] - inf['previous_close']) / inf['previous_close']) * 100,
            'volume': inf['last_volume']
        }
    except: return None

# --- CICLO PRINCIPAL (CON NUEVA LÓGICA DE PRIORIDADES) ---

def ciclo_principal():
    print("🚀 Motor Cuádruple V2 (Multifuente) Iniciado...")
    while True:
        try:
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)
            
            cur.execute("SELECT * FROM sys_traductor_simbolos WHERE is_active = 1")
            activos = cur.fetchall()

            for a in activos:
                res_raw = None
                fuente_raw = "none"

                # 1. Intentar Binance USDT Futures (Prioridad alta en Cripto)
                if a['binance_usdt_future']:
                    res_raw = get_binance_price(a['binance_usdt_future'], is_future=True)
                    fuente_raw = "binance_futures"

                # 2. Si falla o no hay, intentar BingX Perp
                if not res_raw and a['bingx_perp']:
                    res_raw = get_bingx_price(a['bingx_perp'], version='v2')
                    fuente_raw = "bingx_perp"

                # 3. Intentar Binance Coin-M (NUEVA COLUMNA)
                if not res_raw and a['binance_coin_future']:
                    res_raw = get_binance_price(a['binance_coin_future'], is_coin_m=True)
                    fuente_raw = "binance_coin_m"

                # 4. Fallback a Yahoo (Acciones, Metales, etc.)
                if not res_raw and a['yahoo_sym']:
                    res_raw = get_yahoo_price(a['yahoo_sym'])
                    fuente_raw = "yahoo"

                # APLICAR TU ESCUDO DE ANOMALÍAS
                res, fuente = validar_precio_logico(a['nombre_comun'], res_raw, fuente_raw, a)

                if res:
                    cur_upd = conn.cursor()
                    cur_upd.execute("""
                        INSERT INTO sys_precios_activos 
                        (symbol, price, change_24h, volume_24h, source, last_update) 
                        VALUES (%s, %s, %s, %s, %s, NOW()) 
                        ON DUPLICATE KEY UPDATE 
                        price=%s, change_24h=%s, volume_24h=%s, source=%s, last_update=NOW()
                    """, (a['nombre_comun'], res['price'], res['change'], res['volume'], fuente, 
                          res['price'], res['change'], res['volume'], fuente))
                    conn.commit()
                    cur_upd.close()
                    print(f"   ✅ {a['nombre_comun']:7} | ${res['price']:<12.4f} | {fuente}")

            cur.close()
            conn.close()
            print(f"--- Ciclo completado. Durmiendo 30s ---")
            time.sleep(30)

        except Exception as e:
            print(f"❌ Error en ciclo: {e}")
            time.sleep(10)

if __name__ == "__main__":
    ciclo_principal()