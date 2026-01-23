import requests
import time
import random
import pandas as pd
import yfinance as yf
from config import FINNHUB_KEY, ALPHA_VANTAGE_KEY

# ==========================================================
# 🚩 CONFIGURACIÓN DE PRUEBA
# ==========================================================
TICKER_PARA_PRUEBA = "AAPL"
# ==========================================================

def get_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://finance.yahoo.com/'
    }

# 1️⃣ BINANCE: 3 EJES
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

# 2️⃣ BINGX: INTEGRAL
def mapeo_bingx(busqueda):
    tk = busqueda.upper()
    encontrados = []
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

# 3️⃣ YAHOO: DISCOVERY
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
                encontrados.append({"Motor": "YAHOO", "Ticker": sym, "Precio": f"{p:.2f}" if p else "N/A", "Info": f"{q.get('quoteType')} - {q.get('shortname')}"})
            except: continue
    except: pass
    return encontrados

# 4️⃣ FINNHUB V1.7 (CON FOREX OANDA Y CRYPTO BINANCE)
def mapeo_finnhub(busqueda):
    tk = busqueda.upper()
    encontrados = []
    
    # 1. 🔍 BÚSQUEDA GENERAL (No se toca, es la que trae GOLD.COM o AAPL INC)
    try:
        r_gen = requests.get(f"https://finnhub.io/api/v1/search?q={tk}&token={FINNHUB_KEY}").json()
        for i in r_gen.get('result', [])[:3]:
            q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={i['symbol']}&token={FINNHUB_KEY}").json()
            if q.get('c'):
                encontrados.append({"Motor": "FINNHUB_GEN", "Ticker": i['symbol'], "Precio": q['c'], "Info": i['description']})
    except: pass

    # 2. 🌍 CAPA FOREX/METALES (OANDA) - El "Misil Teledirigido"
    # Diccionario de traducción para que Finnhub sepa qué buscar en Oanda
    diccionario_fx = {
        "GOLD": "XAU_USD", 
        "SILVER": "XAG_USD", 
        "DAX": "GER30_EUR", # El DAX en Oanda suele ser GER30
        "EURUSD": "EUR_USD",
        "GBPUSD": "GBP_USD"
    }
    target = diccionario_fx.get(tk, tk)

    try:
        r_fx = requests.get(f"https://finnhub.io/api/v1/forex/symbol?exchange=oanda&token={FINNHUB_KEY}").json()
        for s in r_fx:
            # Si el ticker de Oanda contiene nuestro target (ej: XAU_USD)
            if target in s['symbol']:
                q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={s['symbol']}&token={FINNHUB_KEY}").json()
                if q.get('c'):
                    encontrados.append({
                        "Motor": "FINNHUB_FX", 
                        "Ticker": s['symbol'], 
                        "Precio": q['c'], 
                        "Info": f"OANDA: {s['description']}"
                    })
                break # Solo el primero más relevante de Oanda
    except: pass

    # 3. ₿ CAPA CRYPTO (BINANCE)
    try:
        r_cry = requests.get(f"https://finnhub.io/api/v1/crypto/symbol?exchange=binance&token={FINNHUB_KEY}").json()
        count = 0
        for s in r_cry:
            if tk in s['symbol'] and ("USDT" in s['symbol'] or "USDC" in s['symbol']):
                q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={s['symbol']}&token={FINNHUB_KEY}").json()
                if q.get('c'):
                    encontrados.append({"Motor": "FINNHUB_CRY", "Ticker": s['symbol'], "Precio": q['c'], "Info": f"BINANCE: {s['description']}"})
                count += 1
                if count >= 2: break
    except: pass

    return encontrados
    # --- NIVEL 3: CRYPTO (BINANCE FEED) ---
    try:
        url_cry = f"https://finnhub.io/api/v1/crypto/symbol?exchange=binance&token={FINNHUB_KEY}"
        r_cry = requests.get(url_cry, timeout=10).json()
        if isinstance(r_cry, list):
            count = 0
            for s in r_cry:
                # Filtramos para que sea el ticker buscado contra USDT o USDC
                if tk in s['symbol'] and ("USDT" in s['symbol'] or "USDC" in s['symbol']):
                    q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={s['symbol']}&token={FINNHUB_KEY}").json()
                    if q.get('c'):
                        encontrados.append({
                            "Motor": "FINNHUB_CRY", 
                            "Ticker": s['symbol'], 
                            "Precio": q['c'], 
                            "Info": f"BINANCE: {s['description']}"
                        })
                    count += 1
                    if count >= 2: break # No saturar con mil pares de crypto
    except Exception as e:
        print(f"   ⚠️ Error en Finnhub Crypto: {e}")

    return encontrados

# 5️⃣ ALPHA VANTAGE
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
                encontrados.append({"Motor": "ALPHA", "Ticker": sym, "Precio": rq.get('05. price'), "Info": match['2. name']})
            time.sleep(1.2) 
    except: pass
    return encontrados

# ==========================================================
# 🧠 ENSAMBLADOR V1.6
# ==========================================================
def ejecutor_maestro_v1_6():
    print(f"💎 CÓDIGO MAESTRO V1.71 - FILTRADO FINNHUB FX/CRYPTO")
    print(f"🔍 ESCANEANDO: {TICKER_PARA_PRUEBA}")
    print("-" * 125)
    
    consolidado = []
    motores = [
        ("Binance", mapeo_binance), ("BingX", mapeo_bingx), 
        ("Yahoo", mapeo_yahoo), ("Finnhub", mapeo_finnhub), ("Alpha", mapeo_alpha)
    ]

    for nombre, func in motores:
        print(f"📡 Interrogando {nombre}...")
        try:
            res = func(TICKER_PARA_PRUEBA)
            if res: consolidado.extend(res)
        except Exception as e: print(f"   ⚠️ Error en {nombre}: {e}")
        time.sleep(1.2)

    print("\n" + "═"*130)
    print(f"📊 RESULTADO FINAL PARA: {TICKER_PARA_PRUEBA}")
    print("═"*130)
    
    if consolidado:
        df = pd.DataFrame(consolidado)
        pd.set_option('display.max_colwidth', 60)
        print(df[["Motor", "Ticker", "Precio", "Info"]].to_string(index=False, justify='left'))
    else:
        print(f"❌ Sin resultados para '{TICKER_PARA_PRUEBA}'")
    
    print("═"*130)

if __name__ == "__main__":
    ejecutor_maestro_v1_6()