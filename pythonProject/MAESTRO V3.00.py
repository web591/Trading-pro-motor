import requests
import time
import random
import pandas as pd
import yfinance as yf
import mysql.connector
from datetime import datetime
from config import FINNHUB_KEY, ALPHA_VANTAGE_KEY, DB_CONFIG

# ==========================================================
# 🚩 CONFIGURACIÓN DE BÚSQUEDA
# ==========================================================
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1'
]

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

def get_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'application/json',
        'Referer': 'https://finance.yahoo.com/'
    }

# ==========================================================
# 🚀 TUS MOTORES DE BÚSQUEDA (SIN CAMBIOS)
# ==========================================================

def mapeo_binance(busqueda):
    tk = busqueda.upper().replace("-", "")
    encontrados = []
    hosts = [
        ("BIN_SPOT", "https://api.binance.com/api/v3/ticker/price"),
        ("BIN_USDT_F", "https://fapi.binance.com/fapi/v1/ticker/price"),
        ("BIN_COIN_F", "https://dapi.binance.com/dapi/v1/ticker/price")
    ]
    for mkt, url in hosts:
        try:
            r = requests.get(url, headers=get_headers(), timeout=7)
            if r.status_code == 200:
                df = pd.DataFrame(r.json())
                match = df[df['symbol'].str.contains(tk)]
                for _, row in match.iterrows():
                    encontrados.append({
                        "Motor": mkt, "Ticker": row['symbol'], 
                        "Precio": float(row['price']), "Info": "Crypto Pair"
                    })
            time.sleep(random.uniform(1.1, 2.5))
        except: pass
    return encontrados

def mapeo_bingx(busqueda):
    tk = busqueda.upper()
    encontrados = []
    try:
        url = "https://open-api.bingx.com/openApi/swap/v2/quote/allTicker"
        r = requests.get(url, headers=get_headers(), timeout=7)
        if r.status_code == 200:
            data = r.json().get('data', [])
            for item in data:
                if tk in item['symbol']:
                    encontrados.append({
                        "Motor": "BINGX_PERP", "Ticker": item['symbol'], 
                        "Precio": float(item['lastPrice']), "Info": "ADN Auto-Verificado"
                    })
    except: pass
    return encontrados

def mapeo_yahoo(busqueda):
    encontrados = []
    try:
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={busqueda}"
        r = requests.get(url, headers=get_headers(), timeout=7)
        if r.status_code == 200:
            quotes = r.json().get('quotes', [])
            for q in quotes[:6]:
                ticker = q['symbol']
                try:
                    time.sleep(random.uniform(0.5, 1.0))
                    tk_data = yf.Ticker(ticker)
                    hist = tk_data.history(period="1d")
                    if not hist.empty:
                        encontrados.append({
                            "Motor": "YAHOO", "Ticker": ticker, 
                            "Precio": float(hist['Close'].iloc[-1]), 
                            "Info": f"{q.get('quoteType', '')} - {q.get('shortname', '')}"
                        })
                except: continue
    except: pass
    return encontrados

def mapeo_finnhub(busqueda):
    encontrados = []
    try:
        r_cry = requests.get(f"https://finnhub.io/api/v1/crypto/symbol?exchange=binance&token={FINNHUB_KEY}", timeout=7)
        if r_cry.status_code == 200:
            for s in r_cry.json():
                if busqueda.upper() in s['symbol']:
                    encontrados.append({"Motor": "FINNHUB_CRY", "Ticker": s['symbol'], "Precio": 0, "Info": s['displaySymbol']})
    except: pass
    return encontrados

def mapeo_alpha(busqueda):
    encontrados = []
    try:
        url = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={busqueda}&apikey={ALPHA_VANTAGE_KEY}"
        r = requests.get(url, timeout=7)
        if r.status_code == 200:
            best = r.json().get('bestMatches', [])
            for match in best[:2]:
                sym = match['1. symbol']
                encontrados.append({"Motor": "ALPHA", "Ticker": sym, "Precio": 0, "Info": match['2. name']})
    except: pass
    return encontrados

# ==========================================================
# 💾 GUARDADO Y LOGICA DE CONTROL (CORREGIDO)
# ==========================================================

def guardar_resultados_db(resultados, busqueda_id, nombre_comun):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        vistos = set()
        query = "INSERT INTO sys_busqueda_resultados (busqueda_id, nombre_comun, motor, ticker, precio, info) VALUES (%s, %s, %s, %s, %s, %s)"
        for res in resultados:
            llave = f"{res['Motor']}-{res['Ticker']}"
            if llave not in vistos:
                cursor.execute(query, (busqueda_id, nombre_comun, res['Motor'], res['Ticker'], res['Precio'], res['Info']))
                vistos.add(llave)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error DB al guardar: {e}")

def ejecutar_bucle_buscador():
    print("🔍 MAESTRO V3.00 activo y esperando tareas...")
    while True:
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)
            
            # Buscamos tarea pendiente
            cur.execute("SELECT id, ticker FROM sys_simbolos_buscados WHERE status = 'pendiente' ORDER BY id ASC LIMIT 1")
            tarea = cur.fetchone()
            
            if tarea:
                id_tarea = tarea['id']
                ticker_busqueda = tarea['ticker'].upper()
                
                # Bloqueo para evitar duplicados
                cur.execute("UPDATE sys_simbolos_buscados SET status = 'buscando' WHERE id = %s", (id_tarea,))
                conn.commit()

                # --- VALIDACIÓN DE CACHÉ (Punto 8) ---
                cur.execute("""
                    SELECT COUNT(*) as total FROM sys_busqueda_resultados 
                    WHERE nombre_comun = %s AND fecha_actualizacion > NOW() - INTERVAL 1 DAY
                """, (ticker_busqueda,))
                
                if cur.fetchone()['total'] > 0:
                    print(f"♻️ Usando resultados existentes para: {ticker_busqueda}")
                    cur.execute("UPDATE sys_simbolos_buscados SET status = 'encontrado' WHERE id = %s", (id_tarea,))
                    conn.commit()
                    continue 

                print(f"🚀 Procesando búsqueda exhaustiva para: {ticker_busqueda}")
                consolidado = []
                
                # EJECUCIÓN DE MOTORES
                consolidado.extend(mapeo_binance(ticker_busqueda))
                consolidado.extend(mapeo_bingx(ticker_busqueda))
                consolidado.extend(mapeo_yahoo(ticker_busqueda))
                consolidado.extend(mapeo_finnhub(ticker_busqueda))
                consolidado.extend(mapeo_alpha(ticker_busqueda))

                # Guardamos resultados y liberamos
                guardar_resultados_db(consolidado, id_tarea, ticker_busqueda)
                
                cur.execute("UPDATE sys_simbolos_buscados SET status = 'encontrado' WHERE id = %s", (id_tarea,))
                conn.commit()
                print(f"✅ ¡Hecho! {ticker_busqueda} procesado correctamente.")
            
            cur.close()
            conn.close()
        except Exception as e:
            print(f"⚠️ Error: {e}")
            if conn: conn.close()
        
        time.sleep(10)

if __name__ == "__main__":
    ejecutar_bucle_buscador()