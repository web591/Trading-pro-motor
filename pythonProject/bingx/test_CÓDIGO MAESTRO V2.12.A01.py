# ==========================================================
# MOTOR SCANEO TEST v1.0 (SIN GUARDADO)
# ==========================================================

import requests
import yfinance as yf
from config import FINNHUB_KEY

# ==========================================================
# 🚩 ESPACIO PARA PRUEBAS (CONFIGURA AQUÍ)
# ==========================================================
TICKER_PARA_PRUEBA = "GOOGL"
# ==========================================================

def get_headers():
    return {
        'User-Agent': 'Mozilla/5.0',
        'Referer': 'https://finance.yahoo.com/'
    }

# ==========================================================
# 🚀 MOTORES
# ==========================================================

def mapeo_binance(busqueda):
    tk = busqueda.upper().replace("-", "")
    encontrados = []
    hosts = [
        ("binance_spot", "https://api.binance.com/api/v3/ticker/price"),
        ("binance_usdt_future", "https://fapi.binance.com/fapi/v1/ticker/price"),
        ("binance_coin_future", "https://dapi.binance.com/dapi/v1/ticker/price")
    ]
    for mkt, url in hosts:
        try:
            r = requests.get(url, timeout=10).json()
            for i in r:
                sym = i.get('symbol','')
                if tk in sym:
                    precio = i.get('price',0)
                    nm_limpio = sym.replace("USDT","").replace("USDC","").replace("BUSD","").replace("USD","")
                    if "_" in nm_limpio: nm_limpio = nm_limpio.split("_")[0]
                    encontrados.append({
                        "Motor": mkt,
                        "Ticker": sym,
                        "Nombre": nm_limpio,
                        "Precio": precio,
                        "Info": f"Crypto Pair: {sym}"
                    })
        except: continue
    return encontrados

# ==========================================================
# BINGX UNIVERSO TOTAL (CRYPTO + FOREX + STOCKS + INDICES + COMMODITIES)
# Version 2.0 INSTITUCIONAL CFD TOKEN
# ==========================================================

def mapeo_bingx (busqueda):

    tk = busqueda.upper().replace("/", "").replace("-", "")
    encontrados = []

    try:

        # --------------------------------------------------
        # 1️⃣ CATALOGO (QUE EXISTE)
        # --------------------------------------------------
        url_contracts = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
        contratos = requests.get(url_contracts, timeout=10).json().get("data", [])

        # --------------------------------------------------
        # 2️⃣ PRECIOS (CUANTO VALE)
        # --------------------------------------------------
        url_ticker = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
        precios = requests.get(url_ticker, timeout=10).json().get("data", [])

        precios_dict = {p["symbol"]:p for p in precios}

        # --------------------------------------------------
        # 3️⃣ SCAN UNIVERSAL
        # --------------------------------------------------
        for c in contratos:

            symbol       = c.get("symbol")
            underlying   = str(c.get("underlying")).replace("/","").upper()
            asset        = str(c.get("asset")).upper()
            displayName  = c.get("displayName","")

            if tk not in underlying and tk not in displayName.upper():
                continue

            precio_data = precios_dict.get(symbol)

            if not precio_data:
                continue

            precio = precio_data.get("lastPrice")

            if not precio or float(precio) <= 0:
                continue

            # --------------------------------------------------
            # 4️⃣ CLASIFICACION UNIVERSO
            # --------------------------------------------------
            if asset.startswith("NCFX"):
                universo = "FOREX"
            elif asset.startswith("NCCO"):
                universo = "COMMODITY"
            elif asset.startswith("NCSK"):
                universo = "STOCK"
            elif asset.startswith("NCSI"):
                universo = "INDEX"
            else:
                universo = "CRYPTO"

            encontrados.append({

                "Motor"  : f"bingx_{universo.lower()}",
                "Ticker" : symbol,
                "Nombre" : underlying,
                "Precio" : precio,
                "Info"   : f"{universo} CFD | {displayName}"

            })

    except Exception as e:
        print("❌ Error BingX Universal:", e)

    return encontrados

def mapeo_yahoo(busqueda):
    encontrados = []
    url = f"https://query2.finance.yahoo.com/v1/finance/search?q={busqueda}"
    try:
        r = requests.get(url, headers=get_headers(), timeout=10).json()
        for q in r.get('quotes', [])[:5]:
            sym = q.get('symbol')
            if not sym: continue
            try:
                t = yf.Ticker(sym)
                p = t.fast_info['last_price']
                nombre = q.get('shortname') or q.get('longname') or sym
                encontrados.append({
                    "Motor": "yahoo",
                    "Ticker": sym,
                    "Nombre": busqueda,
                    "Precio": f"{p:.4f}",
                    "Info": nombre
                })
            except: continue
    except: pass
    return encontrados

def mapeo_finnhub(busqueda):
    encontrados = []
    try:
        url_gen = f"https://finnhub.io/api/v1/search?q={busqueda}&token={FINNHUB_KEY}"
        r_gen = requests.get(url_gen, timeout=10).json()
        for i in r_gen.get('result', [])[:3]:
            sym = i['symbol']
            q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_KEY}").json()
            if q.get('c'):
                encontrados.append({
                    "Motor": "finnhub",
                    "Ticker": sym,
                    "Nombre": busqueda,
                    "Precio": q['c'],
                    "Info": i.get('description', sym)
                })
    except: pass
    return encontrados

# ==========================================================
# 🧪 TEST
# ==========================================================

def ejecutar_test():
    tk = TICKER_PARA_PRUEBA.upper().strip()
    print(f"\n🧪 TEST SCANEO: {tk}")
    print("="*60)

    motores = [
        mapeo_binance,
        mapeo_bingx,
        mapeo_yahoo,
        mapeo_finnhub
    ]

    total = []

    for m in motores:
        try:
            res = m(tk)
            if res:
                total.extend(res)
        except: continue

    if not total:
        print("❌ No se encontraron resultados.")
        return

    for r in total:
        print(f"{r['Motor']:20} | {r['Ticker']:15} | {r['Nombre']:10} | {r['Precio']:10} | {r['Info']}")

# ==========================================================
# MAIN
# ==========================================================

if __name__ == "__main__":
    ejecutar_test()
