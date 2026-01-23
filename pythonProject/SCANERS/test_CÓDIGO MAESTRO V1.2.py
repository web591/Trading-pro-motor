import requests
import time
import random
import pandas as pd
import yfinance as yf
from config import FINNHUB_KEY, ALPHA_VANTAGE_KEY

# ==========================================================
# 🚩 ESPACIO PARA PRUEBAS (CONFIGURA AQUÍ)
# ==========================================================
TICKER_PARA_PRUEBA = "BTC" 
# ==========================================================

# --- CONFIGURACIÓN ANTI-BLOQUEO ---
MOZILLA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
]

def get_headers():
    return {'User-Agent': random.choice(MOZILLA_POOL)}

# ==========================================================
# 1️⃣ BINANCE: SCANNER DE 3 EJES (Spot, USDT-M, COIN-M)
# ==========================================================
def mapeo_binance_scanner(busqueda):
    tk = busqueda.upper().replace("-", "")
    encontrados = []
    # Definición de los 3 mundos de Binance
    endpoints = [
        ("SPOT", "https://api.binance.com/api/v3/ticker/price"),
        ("USDT-FUT", "https://fapi.binance.com/fapi/v1/ticker/price"),
        ("COIN-FUT", "https://dapi.binance.com/dapi/v1/ticker/price")
    ]
    
    for mercado, url in endpoints:
        try:
            r = requests.get(url, headers=get_headers(), timeout=10).json()
            # COIN-FUT (dapi) a veces devuelve una lista directamente
            data = r if isinstance(r, list) else [r]
            
            for item in data:
                sym = item.get('symbol', '')
                if tk in sym:
                    # Filtro extra para Spot: Solo queremos USDT/USDC para evitar ruido de pares raros
                    if mercado == "SPOT" and not (sym.endswith("USDT") or sym.endswith("USDC")):
                        continue
                        
                    encontrados.append({
                        "Plataforma": "BINANCE",
                        "Mercado": mercado,
                        "Ticker": sym,
                        "Precio": item.get('price', item.get('lastPrice', 'N/A'))
                    })
        except: continue
    return encontrados

# ==========================================================
# 2️⃣ BINGX: SCANNER INTEGRAL (Perp V2, Standard, Spot)
# ==========================================================
def mapeo_bingx_scanner(busqueda):
    tk = busqueda.upper()
    encontrados = []
    try:
        # A. Perpetuos V2 (NCCO/NCSK/Cripto)
        r_perp = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker", timeout=10).json()
        if r_perp.get('code') == 0:
            for item in r_perp['data']:
                if tk in item['symbol']:
                    encontrados.append({"Plataforma": "BINGX", "Mercado": "PERP_V2", "Ticker": item['symbol'], "Precio": item['lastPrice']})
        
        # B. Standard Contract (Indices/Forex Tradicional)
        r_std = requests.get("https://open-api.bingx.com/openApi/market/v1/tickers", timeout=10).json()
        if r_std.get('code') == 0:
            for s in r_std['data'].get('tickers', []):
                if tk in s['symbol']:
                    encontrados.append({"Plataforma": "BINGX", "Mercado": "STD_FUT", "Ticker": s['symbol'], "Precio": s['lastPrice']})

        # C. Spot (Inventory Scan)
        r_inv = requests.get("https://open-api.bingx.com/openApi/spot/v1/common/symbols", timeout=10).json()
        if r_inv.get('code') == 0:
            for s in r_inv['data']['symbols']:
                if tk in s['symbol']:
                    p_res = requests.get(f"https://open-api.bingx.com/openApi/spot/v1/ticker/24hr?symbol={s['symbol']}", timeout=5).json()
                    if p_res.get('code') == 0:
                        encontrados.append({"Plataforma": "BINGX", "Mercado": "SPOT", "Ticker": s['symbol'], "Precio": p_res['data'][0]['lastPrice']})
    except: pass
    return encontrados

# ==========================================================
# 3️⃣ YAHOO: DISCOVERY SCANNER (Acciones, ETFs, Futuros CME)
# ==========================================================
def mapeo_yahoo_scanner(busqueda):
    encontrados = []
    try:
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={busqueda}"
        r = requests.get(url, headers=get_headers(), timeout=10).json()
        for quote in r.get('quotes', [])[:5]:
            sym = quote['symbol']
            t = yf.Ticker(sym)
            p = t.fast_info['last_price']
            encontrados.append({"Plataforma": "YAHOO", "Mercado": quote.get('quoteType', 'N/A'), "Ticker": sym, "Precio": f"{p:.2f}" if p else "N/A"})
    except: pass
    return encontrados

# ==========================================================
# 4️⃣ FINNHUB: SCANNER GLOBAL (Oanda FX, CFDs, Brokers)
# ==========================================================
def mapeo_finnhub_scanner(busqueda):
    encontrados = []
    try:
        url = f"https://finnhub.io/api/v1/search?q={busqueda}&token={FINNHUB_KEY}"
        r = requests.get(url, timeout=10).json()
        for item in r.get('result', [])[:5]:
            sym = item['symbol']
            rq = requests.get(f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_KEY}").json()
            if rq.get('c'):
                encontrados.append({"Plataforma": "FINNHUB", "Mercado": "GLOBAL/CFD", "Ticker": sym, "Precio": rq['c']})
    except: pass
    return encontrados

# ==========================================================
# 5️⃣ ALPHA VANTAGE: SCANNER REGULADO (Bolsas Principales)
# ==========================================================
def mapeo_alpha_scanner(busqueda):
    encontrados = []
    try:
        url = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={busqueda}&apikey={ALPHA_VANTAGE_KEY}"
        r = requests.get(url, timeout=10).json()
        for match in r.get('bestMatches', [])[:2]:
            sym = match['1. symbol']
            url_q = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={sym}&apikey={ALPHA_VANTAGE_KEY}"
            rq = requests.get(url_q).json().get('Global Quote', {})
            if rq:
                encontrados.append({"Plataforma": "ALPHA", "Mercado": "STOCK/ETF", "Ticker": sym, "Precio": rq.get('05. price')})
            time.sleep(1.2) # Respeto al límite de 5 req/min
    except: pass
    return encontrados

# ==========================================================
# 🧠 ENSAMBLADOR MAESTRO V1.2
# ==========================================================
def ejecutar_maestro_v1_2():
    print(f"💎 CÓDIGO MAESTRO V1.2 - INVENTARIO DE ACTIVOS 💎")
    print(f"🔍 ESCANEANDO: {TICKER_PARA_PRUEBA}")
    print("-" * 90)
    
    consolidado = []
    motores = [
        ("Binance", mapeo_binance_scanner),
        ("BingX", mapeo_bingx_scanner),
        ("Yahoo", mapeo_yahoo_scanner),
        ("Finnhub", mapeo_finnhub_scanner),
        ("Alpha", mapeo_alpha_scanner)
    ]

    for nombre, funcion in motores:
        print(f"📡 Buscando en {nombre}...")
        try:
            res = funcion(TICKER_PARA_PRUEBA)
            if res: consolidado.extend(res)
        except Exception as e:
            print(f"   ⚠️ Error en motor {nombre}: {e}")
        time.sleep(1) # Ciclo de seguridad para Hostinger

    print("\n" + "="*100)
    print(f"📋 RESULTADOS TOTALES DEL ESCANEO PARA: {TICKER_PARA_PRUEBA}")
    print("="*100)
    
    if consolidado:
        df = pd.DataFrame(consolidado)
        # Limpieza estética del precio
        df['Precio'] = df['Precio'].apply(lambda x: f"{float(x):,.4f}" if x != "N/A" and x is not None else "N/A")
        print(df.to_string(index=False))
    else:
        print(f"❌ No se encontró rastro de '{TICKER_PARA_PRUEBA}' en los inventarios actuales.")
    
    print("="*100)
    print(f"🔔 Fin del escaneo. Total de variantes detectadas: {len(consolidado)}")

if __name__ == "__main__":
    ejecutar_maestro_v1_2()