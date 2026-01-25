import requests
import time
import random
import pandas as pd
import yfinance as yf
from config import FINNHUB_KEY, ALPHA_VANTAGE_KEY

# ==========================================================
# 🚩 CONFIGURACIÓN DE PRUEBA
# ==========================================================
TICKER_PARA_PRUEBA = "EURUSD"
# ==========================================================

def get_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://finance.yahoo.com/'
    }

# 1️⃣ BINANCE: 3 EJES (Spot, USDT-M, COIN-M)
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
            r = requests.get(url, timeout=10).json()
            data = r if isinstance(r, list) else [r]
            for i in data:
                sym = i.get('symbol','')
                if tk in sym:
                    if mkt == "BIN_SPOT" and not (sym.endswith("USDT") or sym.endswith("USDC")): continue
                    encontrados.append({"Motor": mkt, "Ticker": sym, "Precio": i.get('price', i.get('lastPrice')), "Info": "Crypto Pair"})
        except: continue
    return encontrados

# 2️⃣ BINGX: INTEGRAL (Perp V2 + Standard + Spot)
def mapeo_bingx(busqueda):
    tk = busqueda.upper()
    encontrados = []
    # Perpetuos y Standard
    urls = [
        ("BINGX_PERP", "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"),
        ("BINGX_STD", "https://open-api.bingx.com/openApi/market/v1/tickers")
    ]
    for mkt, url in urls:
        try:
            r = requests.get(url, timeout=10).json()
            items = r['data'] if 'data' in r and isinstance(r['data'], list) else r.get('data', {}).get('tickers', [])
            for i in items:
                if tk in i['symbol']:
                    encontrados.append({"Motor": mkt, "Ticker": i['symbol'], "Precio": i['lastPrice'], "Info": "Derivado/Tradicional"})
        except: continue
    return encontrados

# 3️⃣ YAHOO: DISCOVERY REFORZADO
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
                encontrados.append({
                    "Motor": "YAHOO", 
                    "Ticker": sym, 
                    "Precio": f"{p:.2f}" if p else "N/A", 
                    "Info": f"{q.get('quoteType')} - {q.get('shortname')}"
                })
            except: continue
    except: pass
    return encontrados

# 4️⃣ FINNHUB: CFD / GLOBAL
def mapeo_finnhub(busqueda):
    encontrados = []
    try:
        r = requests.get(f"https://finnhub.io/api/v1/search?q={busqueda}&token={FINNHUB_KEY}").json()
        for i in r.get('result', [])[:5]:
            q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={i['symbol']}&token={FINNHUB_KEY}").json()
            if q.get('c'):
                encontrados.append({"Motor": "FINNHUB", "Ticker": i['symbol'], "Precio": q['c'], "Info": i['description']})
    except: pass
    return encontrados

# 5️⃣ ALPHA VANTAGE: REGULADOS (Restaurado)
def mapeo_alpha(busqueda):
    encontrados = []
    try:
        url = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={busqueda}&apikey={ALPHA_VANTAGE_KEY}"
        r = requests.get(url, timeout=10).json()
        for match in r.get('bestMatches', [])[:2]:
            sym = match['1. symbol']
            url_q = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={sym}&apikey={ALPHA_VANTAGE_KEY}"
            rq = requests.get(url_q).json().get('Global Quote', {})
            if rq:
                encontrados.append({
                    "Motor": "ALPHA", 
                    "Ticker": sym, 
                    "Precio": rq.get('05. price'), 
                    "Info": match['2. name']
                })
            time.sleep(1.2) 
    except: pass
    return encontrados

# ==========================================================
# 🧠 ENSAMBLADOR FINAL V1.5
# ==========================================================
def auditor_maestro_v1_5():
    print(f"💎 MOTOR DE INVENTARIO V1.5 - FULL STACK")
    print(f"🔎 ESCANEANDO: {TICKER_PARA_PRUEBA}")
    print("-" * 120)
    
    consolidado = []
    # Lista de motores completa
    motores = [
        ("Binance", mapeo_binance), 
        ("BingX", mapeo_bingx), 
        ("Yahoo", mapeo_yahoo), 
        ("Finnhub", mapeo_finnhub),
        ("Alpha", mapeo_alpha)
    ]

    for nombre, func in motores:
        print(f"📡 Interrogando {nombre}...")
        try:
            res = func(TICKER_PARA_PRUEBA)
            if res: consolidado.extend(res)
        except Exception as e:
            print(f"   ⚠️ Error en {nombre}: {e}")
        time.sleep(1.5)

    print("\n" + "═"*125)
    print(f"📊 RESULTADO FINAL PARA: {TICKER_PARA_PRUEBA}")
    print("═"*125)
    
    if consolidado:
        df = pd.DataFrame(consolidado)
        # Formateo de columnas para que no se corten
        pd.set_option('display.max_colwidth', 50)
        print(df[["Motor", "Ticker", "Precio", "Info"]].to_string(index=False, justify='left'))
    else:
        print(f"❌ No se encontró nada para '{TICKER_PARA_PRUEBA}'")
    
    print("═"*125)

if __name__ == "__main__":
    auditor_maestro_v1_5()