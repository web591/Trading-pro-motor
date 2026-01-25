import requests
import time
import random
import pandas as pd
import yfinance as yf
import mysql.connector
from datetime import datetime
from config import FINNHUB_KEY, ALPHA_VANTAGE_KEY, DB_CONFIG

# ==========================================================
# 🚩 CONFIGURACIÓN
# ==========================================================
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

def get_headers():
    return {'User-Agent': random.choice(USER_AGENTS), 'Accept': 'application/json'}

# ==========================================================
# 🚀 MOTORES DE BÚSQUEDA
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
                    encontrados.append({"Motor": mkt, "Ticker": row['symbol'], "Precio": float(row['price']), "Info": "Crypto Pair"})
        except: pass
    return encontrados

def mapeo_bingx(busqueda):
    tk_search = busqueda.upper().replace("/", "").replace("-", "").replace("=X", "")
    encontrados = []
    # Lógica de identidades simplificada para el ejemplo
    identidades = {"GOLD": ["GOLD", "XAU"], "SILVER": ["SILVER", "XAG"]}
    familia_adn = identidades.get(tk_search, [tk_search])
    
    mercados = [
        ("BINGX_PERP", "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"),
        ("BINGX_SPOT", "https://open-api.bingx.com/openApi/spot/v1/market/ticker")
    ]
    for nombre_mkt, url in mercados:
        try:
            r = requests.get(url, timeout=10).json()
            items = r.get('data', [])
            for i in items:
                sym_orig = i.get('symbol', '').upper()
                if any(adn in sym_orig for adn in familia_adn):
                    precio = i.get('lastPrice') or i.get('price')
                    if precio: encontrados.append({"Motor": nombre_mkt, "Ticker": sym_orig, "Precio": precio, "Info": "ADN BingX"})
        except: continue
    return encontrados

def mapeo_yahoo(busqueda):
    encontrados = []
    try:
        url = f"https://query1.finance.yahoo.com/v1/finance/search?q={busqueda}"
        r = requests.get(url, headers=get_headers(), timeout=10).json()
        for q in r.get('quotes', [])[:5]:
            sym = q['symbol']
            try:
                t = yf.Ticker(sym)
                p = t.fast_info['last_price']
                encontrados.append({"Motor": "YAHOO", "Ticker": sym, "Precio": f"{p:.2f}", "Info": q.get('shortname', 'N/A')})
            except: continue
    except: pass
    return encontrados

def mapeo_finnhub(busqueda):
    encontrados = []
    try:
        r = requests.get(f"https://finnhub.io/api/v1/search?q={busqueda}&token={FINNHUB_KEY}").json()
        for i in r.get('result', [])[:3]:
            sym = i['symbol']
            q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_KEY}").json()
            if q.get('c'): encontrados.append({"Motor": "FINNHUB", "Ticker": sym, "Precio": q['c'], "Info": i['description']})
    except: pass
    return encontrados

def mapeo_alpha(busqueda):
    encontrados = []
    try:
        url = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={busqueda}&apikey={ALPHA_VANTAGE_KEY}"
        r = requests.get(url).json()
        for match in r.get('bestMatches', [])[:2]:
            sym = match['1. symbol']
            encontrados.append({"Motor": "ALPHA", "Ticker": sym, "Precio": "0.00", "Info": match['2. name']})
    except: pass
    return encontrados

# ==========================================================
# 💾 PERSISTENCIA Y CACHÉ
# ==========================================================

def guardar_resultados_db(resultados, busqueda_id, nombre_comun):
    """Guarda los hallazgos en la DB para que el PHP los muestre"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """INSERT INTO sys_busqueda_resultados 
                   (busqueda_id, nombre_comun, motor, ticker, precio, info) 
                   VALUES (%s, %s, %s, %s, %s, %s)"""
        for res in resultados:
            # Aseguramos que el precio sea un float válido o 0.0
            try: precio_val = float(res['Precio'])
            except: precio_val = 0.0
            
            cursor.execute(query, (
                busqueda_id, 
                nombre_comun, 
                res['Motor'], 
                res['Ticker'], 
                precio_val, 
                res['Info']
            ))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e: 
        print(f"❌ Error al persistir en DB: {e}")

# ==========================================================
# 🧠 BUCLE PRINCIPAL: CACHÉ + ENSAMBLADOR V1.6
# ==========================================================

def ejecutar_bucle_buscador():
    print("💎 CÓDIGO MAESTRO V3.00 - SISTEMA INICIADO")
    print("📡 Escuchando peticiones desde el Dashboard...")
    
    while True:
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)
            
            # Buscamos si hay algo que el usuario escribió en la web
            cur.execute("SELECT id, ticker FROM sys_simbolos_buscados WHERE status = 'pendiente' LIMIT 1")
            tarea = cur.fetchone()
            
            if tarea:
                id_tarea = tarea['id']
                # Limpieza de entrada según tu archivo original
                tk_busqueda = tarea['ticker'].upper().replace(" ", "").replace("=X", "").strip()
                
                cur.execute("UPDATE sys_simbolos_buscados SET status = 'buscando' WHERE id = %s", (id_tarea,))
                conn.commit()

                print("\n" + "═"*115)
                print(f"💎 ENSAMBLADOR V1.6 - PROCESANDO: {tk_busqueda}")
                print("-" * 115)

                # --- 1. COMPROBACIÓN DE CACHÉ ---
                # Buscamos si ya se buscó este ticker en la última hora (o historial)
                cur.execute("""SELECT motor, ticker, precio, info 
                               FROM sys_busqueda_resultados 
                               WHERE nombre_comun = %s LIMIT 100""", (tk_busqueda,))
                cache = cur.fetchall()

                consolidado = []

                if cache:
                    print(f"🧠 CACHÉ: Se encontraron {len(cache)} resultados previos. Clonando datos...")
                    for c in cache:
                        consolidado.append({
                            "Motor": c['motor'], 
                            "Ticker": c['ticker'], 
                            "Precio": c['precio'], 
                            "Info": c['info']
                        })
                    # Guardamos la copia para esta nueva búsqueda ID
                    guardar_resultados_db(consolidado, id_tarea, tk_busqueda)
                else:
                    # --- 2. BÚSQUEDA EN VIVO (Si no hay caché) ---
                    print(f"📡 No hay caché para '{tk_busqueda}'. Iniciando interrogatorio de APIs...")
                    
                    # Aquí el script llama a todas tus funciones de mapeo
                    motores_activos = [
                        ("Binance", mapeo_binance), 
                        ("BingX", mapeo_bingx), 
                        ("Yahoo", mapeo_yahoo), 
                        ("Finnhub", mapeo_finnhub), 
                        ("AlphaVantage", mapeo_alpha)
                    ]

                    for nombre, func in motores_activos:
                        print(f"   🔎 Consultando {nombre}...")
                        try:
                            hallazgos = func(tk_busqueda)
                            if hallazgos:
                                consolidado.extend(hallazgos)
                        except Exception as e:
                            print(f"      ⚠️ Error en {nombre}: {e}")
                        time.sleep(1.2) # Respetando el delay de tu archivo original

                    if consolidado:
                        guardar_resultados_db(consolidado, id_tarea, tk_busqueda)

                # --- 3. IMPRESIÓN ESTÉTICA (PANDAS) ---
                print("\n" + "═"*115)
                print(f"📊 RESULTADO FINAL PARA: {tk_busqueda}")
                print("═"*115)
                
                if consolidado:
                    df = pd.DataFrame(consolidado)
                    # Configuración visual de Pandas como en tu Ensamblador
                    pd.set_option('display.max_colwidth', 60)
                    print(df[["Motor", "Ticker", "Precio", "Info"]].to_string(index=False, justify='left'))
                else:
                    print(f"❌ No se hallaron coincidencias en ningún mercado para '{tk_busqueda}'")
                
                print("═"*115 + "\n")

                # Finalizamos la tarea para que el Dashboard deje de mostrar el "Cargando"
                cur.execute("UPDATE sys_simbolos_buscados SET status = 'encontrado' WHERE id = %s", (id_tarea,))
                conn.commit()

            cur.close()
            conn.close()
        except Exception as e:
            print(f"⚠️ Error Crítico en el Bucle: {e}")
            if conn: conn.close()
        
        time.sleep(5) # Pausa antes de la siguiente revisión

if __name__ == "__main__":
    ejecutar_bucle_buscador()