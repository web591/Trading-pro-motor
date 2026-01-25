import requests
import time
import random
import pandas as pd
import yfinance as yf
from config import FINNHUB_KEY, ALPHA_VANTAGE_KEY

# ==========================================================
# 🚩 CONFIGURACIÓN DE PRUEBA
# ==========================================================
TICKER_PARA_PRUEBA = "GOLD" 
# ==========================================================

def get_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://finance.yahoo.com/'
    }

# 1️⃣ BINANCE V1.4
def mapeo_binance(busqueda):
    tk = busqueda.upper().replace("-", "")
    encontrados = []
    hosts = [
        ("BINANCE_SPOT", "https://api.binance.com/api/v3/ticker/price"),
        ("BINANCE_USDT_F", "https://fapi.binance.com/fapi/v1/ticker/price"),
        ("BINANCE_COIN_F", "https://dapi.binance.com/dapi/v1/ticker/price")
    ]
    for mkt, url in hosts:
        try:
            r = requests.get(url, timeout=10).json()
            data = r if isinstance(r, list) else [r]
            for i in data:
                sym = i.get('symbol','')
                if tk in sym:
                    if mkt == "BINANCE_SPOT" and not (sym.endswith("USDT") or sym.endswith("USDC")): continue
                    encontrados.append({"Motor": mkt, "Ticker": sym, "Precio": i.get('price', i.get('lastPrice')), "Info": "Crypto Pair"})
        except: continue
    return encontrados

# 2️⃣ BINGX V1.4 (Doble Escaneo: Perp + Standard)
def mapeo_bingx(busqueda):
    tk = busqueda.upper()
    encontrados = []
    # A. Perpetuos (NCCO / Cripto)
    try:
        r = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker", timeout=10).json()
        if r.get('code') == 0:
            for i in r['data']:
                if tk in i['symbol']:
                    encontrados.append({"Motor": "BINGX_PERP", "Ticker": i['symbol'], "Precio": i['lastPrice'], "Info": "Perpetuo Rebelde"})
    except: pass
    # B. Standard (Indices / Forex / Commodities)
    try:
        r_std = requests.get("https://open-api.bingx.com/openApi/market/v1/tickers", timeout=10).json()
        if r_std.get('code') == 0:
            for s in r_std['data'].get('tickers', []):
                if tk in s['symbol']:
                    encontrados.append({"Motor": "BINGX_STD", "Ticker": s['symbol'], "Precio": s['lastPrice'], "Info": "Standard Contract"})
    except: pass
    return encontrados

# 3️⃣ YAHOO V1.4 (Con Fix de Headers para GOLD)
def mapeo_yahoo(busqueda):
    encontrados = []
    try:
        # Usamos el endpoint de Query v1 para mayor estabilidad
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

# 4️⃣ FINNHUB V1.4
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

# ==========================================================
# 🧠 MOTOR DE AUDITORÍA V1.4
# ==========================================================
def auditor_maestro_v1_4():
    print(f"💎 MOTOR DE INVENTARIO V1.4 - PRODUCCIÓN")
    print(f"🔎 ESCANEANDO: {TICKER_PARA_PRUEBA}")
    print("-" * 110)
    
    consolidado = []
    motores = [("Binance", mapeo_binance), ("BingX", mapeo_bingx), ("Yahoo", mapeo_yahoo), ("Finnhub", mapeo_finnhub)]

    for nombre, func in motores:
        print(f"📡 Interrogando {nombre}...")
        res = func(TICKER_PARA_PRUEBA)
        if res: consolidado.extend(res)
        time.sleep(1.5)

    print("\n" + "═"*115)
    print(f"📊 RESULTADO FINAL PARA: {TICKER_PARA_PRUEBA}")
    print("═"*115)
    
    if consolidado:
        df = pd.DataFrame(consolidado)
        print(df[["Motor", "Ticker", "Precio", "Info"]].to_string(index=False))
    else:
        print(f"❌ No se encontró nada para '{TICKER_PARA_PRUEBA}'")
    
    print("═"*115)

if __name__ == "__main__":
    auditor_maestro_v1_4()