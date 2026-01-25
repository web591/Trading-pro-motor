import requests
import time
import random
import pandas as pd
import yfinance as yf
import mysql.connector
from datetime import datetime
from config import FINNHUB_KEY, ALPHA_VANTAGE_KEY, DB_CONFIG

# ==========================================================
# 🚩 CONFIGURACIÓN Y USER AGENTS
# ==========================================================
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0'
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
# 🚀 1️⃣ BINANCE: 3 EJES
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
            r = requests.get(url, headers=get_headers(), timeout=10).json()
            data = r if isinstance(r, list) else [r]
            for i in data:
                sym = i.get('symbol','')
                if tk in sym:
                    if mkt == "BIN_SPOT" and not (sym.endswith("USDT") or sym.endswith("USDC")): continue
                    encontrados.append({
                        "Motor": mkt, "Ticker": sym, 
                        "Precio": float(i.get('price', i.get('lastPrice', 0))), 
                        "Info": "Crypto Pair"
                    })
        except: continue
    return encontrados

# ==========================================================
# 🚀 2️⃣ BINGX: INTEGRAL (ADN)
# ==========================================================
def mapeo_bingx(busqueda):
    tk_search = busqueda.upper().replace("/", "").replace("-", "").replace("=X", "")
    encontrados = []
    identidades = {
        "GOLD": ["GOLD", "XAU", "PAXG", "XAUT", "NCCOGOLD"],
        "SILVER": ["SILVER", "XAG", "NCCOSILVER"],
        "DAX": ["DAX", "GER", "DE30", "DE40", "GDAXI", "NVDAX"],
        "OIL": ["WTI", "OIL", "CRCL"]
    }
    familia_adn = identidades.get(tk_search, [tk_search])
    if len(tk_search) == 6 and not tk_search.isdigit():
        base_currency = tk_search[:3]
        if base_currency not in familia_adn: familia_adn.append(base_currency)

    mercados = [
        ("BINGX_PERP", "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"),
        ("BINGX_SPOT", "https://open-api.bingx.com/openApi/spot/v1/market/ticker")
    ]
    for nombre_mkt, url in mercados:
        try:
            r = requests.get(url, headers=get_headers(), timeout=10).json()
            items = r.get('data', [])
            for i in items:
                sym_orig = i.get('symbol', '').upper()
                sym_fix = sym_orig.replace("NCFX", "").replace("NCCO", "").replace("NCSK", "")
                sym_fix = sym_fix.replace("-USDT", "").replace("USDT", "").replace("-USDC", "").replace("USDC", "").replace("-", "")
                match_hallado = False
                for adn in familia_adn:
                    if sym_fix == adn or sym_fix == f"{adn}X" or sym_fix == tk_search:
                        match_hallado = True
                        break
                    if adn in sym_orig and ("NCFX" in sym_orig or "NCCO" in sym_orig):
                        match_hallado = True
                        break
                if match_hallado:
                    if tk_search == "SOL" and "GASOLINE" in sym_orig: continue
                    precio = i.get('lastPrice') or i.get('price')
                    if precio and float(precio) > 0:
                        encontrados.append({
                            "Motor": nombre_mkt, "Ticker": sym_orig, 
                            "Precio": float(precio), "Info": "ADN Auto-Verificado"
                        })
        except: continue
    return encontrados

# ==========================================================
# 🚀 3️⃣ YAHOO: DISCOVERY
# ==========================================================
def mapeo_yahoo(busqueda):
    encontrados = []
    try:
        url = f"https://query1.finance.yahoo.com/v1/finance/search?q={busqueda}"
        r = requests.get(url, headers=get_headers(), timeout=10).json()
        for q in r.get('quotes', [])[:5]:
            try:
                sym = q['symbol']
                t = yf.Ticker(sym)
                p = t.fast_info['last_price']
                if p:
                    encontrados.append({
                        "Motor": "YAHOO", "Ticker": sym, 
                        "Precio": float(p), 
                        "Info": f"{q.get('quoteType')} - {q.get('shortname')}"
                    })
            except: continue
    except: pass
    return encontrados

# ==========================================================
# 🚀 4️⃣ FINNHUB V1.7
# ==========================================================
def mapeo_finnhub(busqueda):
    tk = busqueda.upper()
    encontrados = []
    try:
        r_gen = requests.get(f"https://finnhub.io/api/v1/search?q={tk}&token={FINNHUB_KEY}", timeout=10).json()
        for i in r_gen.get('result', [])[:3]:
            sym = i['symbol']
            q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_KEY}").json()
            if q.get('c'):
                encontrados.append({"Motor": "FINNHUB_GEN", "Ticker": sym, "Precio": float(q['c']), "Info": i['description']})
    except: pass
    try:
        traductores = {"GOLD": "XAU_USD", "SILVER": "XAG_USD", "EURUSD": "EUR_USD"}
        target_fx = traductores.get(tk, tk)
        r_fx = requests.get(f"https://finnhub.io/api/v1/forex/symbol?exchange=oanda&token={FINNHUB_KEY}", timeout=10).json()
        for s in r_fx:
            if target_fx in s['symbol'] or tk in s['displaySymbol']:
                q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={s['symbol']}&token={FINNHUB_KEY}").json()
                if q.get('c'):
                    encontrados.append({"Motor": "FINNHUB_FX", "Ticker": s['symbol'], "Precio": float(q['c']), "Info": f"OANDA: {s['description']}"})
                    break
    except: pass
    return encontrados

# ==========================================================
# 🚀 5️⃣ ALPHA VANTAGE
# ==========================================================
def mapeo_alpha(busqueda):
    encontrados = []
    try:
        url = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={busqueda}&apikey={ALPHA_VANTAGE_KEY}"
        r = requests.get(url, timeout=10).json()
        for match in r.get('bestMatches', [])[:2]:
            sym = match['1. symbol']
            url_q = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={sym}&apikey={ALPHA_VANTAGE_KEY}"
            rq = requests.get(url_q).json().get('Global Quote', {})
            if rq and rq.get('05. price'):
                encontrados.append({"Motor": "ALPHA", "Ticker": sym, "Precio": float(rq['05. price']), "Info": match['2. name']})
            time.sleep(1.2)
    except: pass
    return encontrados

# ==========================================================
# 💾 PERSISTENCIA (PROTECCIÓN ANTI-DUPLICADOS)
# ==========================================================
def guardar_resultados_db(resultados, busqueda_id, nombre_comun):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # Borramos cualquier residuo de esta búsqueda antes de insertar
        cursor.execute("DELETE FROM sys_busqueda_resultados WHERE busqueda_id = %s", (busqueda_id,))
        query = "INSERT INTO sys_busqueda_resultados (busqueda_id, nombre_comun, motor, ticker, precio, info) VALUES (%s, %s, %s, %s, %s, %s)"
        for res in resultados:
            cursor.execute(query, (busqueda_id, nombre_comun, res['Motor'], res['Ticker'], res['Precio'], res['Info']))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e: print(f"❌ Error DB: {e}")

# ==========================================================
# 🔄 BUCLE MAESTRO (CON LIMPIEZA AUTOMÁTICA)
# ==========================================================
def ejecutar_bucle_buscador():
    print("💎 MAESTRO V3.00 - ENSAMBLADOR V1.6 ACTIVO")
    
    while True:
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)

            # --- 🧹 MANTENIMIENTO: Borrar lo que tenga más de 24 horas ---
            cur.execute("DELETE FROM sys_busqueda_resultados WHERE fecha_actualizacion < NOW() - INTERVAL 1 DAY")
            conn.commit()

            cur.execute("SELECT id, ticker FROM sys_simbolos_buscados WHERE status = 'pendiente' ORDER BY id ASC LIMIT 1")
            tarea = cur.fetchone()
            
            if tarea:
                id_tarea = tarea['id']
                tk_busqueda = tarea['ticker'].upper().strip()
                cur.execute("UPDATE sys_simbolos_buscados SET status = 'buscando' WHERE id = %s", (id_tarea,))
                conn.commit()

                print("\n" + "═"*120)
                print(f"🔍 PROCESANDO: {tk_busqueda} (Tarea #{id_tarea})")
                
                # --- LÓGICA DE CACHÉ INTELIGENTE ---
                cur.execute("SELECT motor, ticker, precio, info FROM sys_busqueda_resultados WHERE nombre_comun = %s GROUP BY motor, ticker", (tk_busqueda,))
                cache = cur.fetchall()
                consolidado = []

                if cache:
                    print(f"🧠 CACHÉ: Vinculando datos existentes a Solicitud #{id_tarea}...")
                    for c in cache:
                        consolidado.append({"Motor": c['motor'], "Ticker": c['ticker'], "Precio": float(c['precio']), "Info": c['info']})
                    # Guardamos (esto borra basura del ID actual y vincula lo nuevo)
                    guardar_resultados_db(consolidado, id_tarea, tk_busqueda)
                else:
                    print(f"📡 API: Consultando motores en vivo...")
                    consolidado.extend(mapeo_binance(tk_busqueda))
                    consolidado.extend(mapeo_bingx(tk_busqueda))
                    consolidado.extend(mapeo_yahoo(tk_busqueda))
                    consolidado.extend(mapeo_finnhub(tk_busqueda))
                    consolidado.extend(mapeo_alpha(tk_busqueda))
                    guardar_resultados_db(consolidado, id_tarea, tk_busqueda)

                # --- VISTA TERMINAL (PANDAS) ---
                print("-" * 120)
                if consolidado:
                    df = pd.DataFrame(consolidado)
                    print(df[["Motor", "Ticker", "Precio", "Info"]].to_string(index=False))
                else:
                    print(f"❌ Sin hallazgos para {tk_busqueda}")
                print("═"*120)

                cur.execute("UPDATE sys_simbolos_buscados SET status = 'encontrado' WHERE id = %s", (id_tarea,))
                conn.commit()
            
            cur.close()
            conn.close()
        except Exception as e:
            print(f"⚠️ Error: {e}")
        
        time.sleep(60) # Pausa de seguridad Hostinger

if __name__ == "__main__":
    ejecutar_bucle_buscador()