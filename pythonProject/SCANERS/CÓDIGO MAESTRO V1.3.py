import requests
import time
import random
import pandas as pd
import yfinance as yf
from config import FINNHUB_KEY, ALPHA_VANTAGE_KEY

# ==========================================================
# 🚩 ZONA DE PRUEBAS - CAMBIA EL ACTIVO AQUÍ
# ==========================================================
TICKER_PARA_PRUEBA = "GOLD" 
# ==========================================================

# --- CONFIGURACIÓN ANTI-BLOQUEO ---
MOZILLA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/115.0"
]

def get_headers():
    return {'User-Agent': random.choice(MOZILLA_POOL)}

# ==========================================================
# 1️⃣ MAPEO BINANCE (3 HOSTS + SEPARACIÓN USDT/USDC)
# ==========================================================
def mapeo_binanceV1_3(busqueda):
    tk = busqueda.upper().replace("-", "").replace("/", "")
    encontrados = []
    endpoints = [
        ("BINANCE_SPOT", "https://api.binance.com/api/v3/ticker/price"),
        ("BINANCE_USDT_FUT", "https://fapi.binance.com/fapi/v1/ticker/price"),
        ("BINANCE_COIN_FUT", "https://dapi.binance.com/dapi/v1/ticker/price")
    ]
    
    for nombre_mkt, url in endpoints:
        try:
            r = requests.get(url, headers=get_headers(), timeout=10).json()
            data = r if isinstance(r, list) else [r]
            for item in data:
                sym = item.get('symbol', '')
                if tk in sym:
                    # Lógica de discriminación solicitada
                    sub_tipo = nombre_mkt
                    if nombre_mkt == "BINANCE_SPOT":
                        if sym.endswith("USDT"): sub_tipo = "BINANCE_SPOT_USDT"
                        elif sym.endswith("USDC"): sub_tipo = "BINANCE_SPOT_USDC"
                        else: continue # Ignorar pares menores (BTC/ETH, etc) para no ensuciar

                    encontrados.append({
                        "Motor": sub_tipo,
                        "Ticker_Real": sym,
                        "Nombre_Comun": f"{busqueda} en {nombre_mkt}",
                        "Precio": item.get('price', item.get('lastPrice'))
                    })
        except: continue
    return encontrados

# ==========================================================
# 2️⃣ MAPEO BINGX (SPOT + STD + PERP V2 - REBELDES)
# ==========================================================
def mapeo_bingxV1_3(busqueda):
    tk = busqueda.upper()
    encontrados = []
    try:
        # A. Perpetuos V2 (Incluye NCCO/NCSK)
        r_perp = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker", timeout=10).json()
        if r_perp.get('code') == 0:
            for item in r_perp['data']:
                if tk in item['symbol']:
                    encontrados.append({
                        "Motor": "BINGX_PERP_V2",
                        "Ticker_Real": item['symbol'],
                        "Nombre_Comun": "Derivado / Tradicional" if "NC" in item['symbol'] else "Cripto Perp",
                        "Precio": item['lastPrice']
                    })
        
        # B. Standard Contract (Indices y Commodities)
        r_std = requests.get("https://open-api.bingx.com/openApi/market/v1/tickers", timeout=10).json()
        if r_std.get('code') == 0:
            for s in r_std['data'].get('tickers', []):
                if tk in s['symbol']:
                    encontrados.append({
                        "Motor": "BINGX_STD_FUT",
                        "Ticker_Real": s['symbol'],
                        "Nombre_Comun": "Standard Contract",
                        "Precio": s['lastPrice']
                    })

        # C. Spot Inventory
        r_inv = requests.get("https://open-api.bingx.com/openApi/spot/v1/common/symbols", timeout=10).json()
        if r_inv.get('code') == 0:
            for s in r_inv['data']['symbols']:
                if tk in s['symbol']:
                    p_res = requests.get(f"https://open-api.bingx.com/openApi/spot/v1/ticker/24hr?symbol={s['symbol']}", timeout=5).json()
                    if p_res.get('code') == 0:
                        encontrados.append({
                            "Motor": "BINGX_SPOT",
                            "Ticker_Real": s['symbol'],
                            "Nombre_Comun": "Spot Asset",
                            "Precio": p_res['data'][0]['lastPrice']
                        })
    except: pass
    return encontrados

# ==========================================================
# 3️⃣ MAPEO YAHOO (DISCOVERY)
# ==========================================================
def mapeo_yahooV1_3(busqueda):
    encontrados = []
    try:
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={busqueda}"
        r = requests.get(url, headers=get_headers(), timeout=10).json()
        for quote in r.get('quotes', [])[:5]:
            sym = quote['symbol']
            t = yf.Ticker(sym)
            p = t.fast_info['last_price']
            encontrados.append({
                "Motor": f"YAHOO_{quote.get('quoteType', 'SEC')}",
                "Ticker_Real": sym,
                "Nombre_Comun": quote.get('shortname', 'Asset'),
                "Precio": f"{p:.2f}" if p else "N/A"
            })
    except: pass
    return encontrados

# ==========================================================
# 4️⃣ MAPEO FINNHUB (GLOBAL/FX)
# ==========================================================
def mapeo_finnhubV1_3(busqueda):
    encontrados = []
    try:
        url = f"https://finnhub.io/api/v1/search?q={busqueda}&token={FINNHUB_KEY}"
        r = requests.get(url, timeout=10).json()
        for item in r.get('result', [])[:5]:
            sym = item['symbol']
            rq = requests.get(f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_KEY}").json()
            if rq.get('c'):
                encontrados.append({
                    "Motor": "FINNHUB_CFD_FX",
                    "Ticker_Real": sym,
                    "Nombre_Comun": item['description'],
                    "Precio": rq['c']
                })
    except: pass
    return encontrados

# ==========================================================
# 5️⃣ MAPEO ALPHA VANTAGE (STOCKS REGULADOS)
# ==========================================================
def mapeo_alphaV1_3(busqueda):
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
                    "Motor": "ALPHA_STOCKS",
                    "Ticker_Real": sym,
                    "Nombre_Comun": match['2. name'],
                    "Precio": rq.get('05. price')
                })
            time.sleep(1.5) # Anti-bloqueo Alpha
    except: pass
    return encontrados

# ==========================================================
# 🛠️ EJECUTOR AUDITADO V1.3
# ==========================================================
def auditor_maestro():
    print(f"🕵️ AUDITORÍA DE INVENTARIOS V1.3")
    print(f"🔎 BUSCANDO TODAS LAS OPCIONES PARA: {TICKER_PARA_PRUEBA}")
    print("-" * 100)
    
    mapeo_total = []
    motores = [
        ("Binance", mapeo_binanceV1_3),
        ("BingX", mapeo_bingxV1_3),
        ("Yahoo", mapeo_yahooV1_3),
        ("Finnhub", mapeo_finnhubV1_3),
        ("Alpha", mapeo_alphaV1_3)
    ]

    for nombre, func in motores:
        print(f"📡 Interrogando inventario de {nombre}...")
        try:
            res = func(TICKER_PARA_PRUEBA)
            if res: mapeo_total.extend(res)
        except Exception as e:
            print(f"   ⚠️ Fallo en {nombre}: {e}")
        time.sleep(1.2) # Ciclo Hostinger

    print("\n" + "═"*110)
    print(f"📊 RESULTADO DEL MAPEO: {len(mapeo_total)} OPCIONES ENCONTRADAS")
    print("═"*110)
    
    if mapeo_total:
        df = pd.DataFrame(mapeo_total)
        # Reordenar columnas para legibilidad
        columnas = ["Motor", "Ticker_Real", "Precio", "Nombre_Comun"]
        print(df[columnas].to_string(index=False))
    else:
        print(f"❌ No se encontró ningún activo relacionado con '{TICKER_PARA_PRUEBA}'.")
    
    print("═"*110)

if __name__ == "__main__":
    auditor_maestro()