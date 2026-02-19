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
# 🔥 TRADUCTOR BINGX REAL
# Version 4.0
# Descubre como BingX llama a GOLD, EURUSD, AAPL, DAX, etc
# ==========================================================



def buscar_crypto_bingx(tk):

    urls = [
        ("bingx_spot", "https://open-api.bingx.com/openApi/spot/v1/market/ticker"),
        ("bingx_perp", "https://open-api.bingx.com/openApi/swap/v2/quote/ticker")
    ]

    encontrados = []

    for nombre, url in urls:

        r = requests.get(url, timeout=10).json()
        data = r.get("data", [])

        for i in data:

            sym = i.get("symbol","")

            if tk in sym.replace("-",""):

                precio = i.get("lastPrice") or i.get("price")

                encontrados.append({
                    "Motor": nombre,
                    "Ticker": sym,
                    "Nombre": sym.split("-")[0],
                    "Precio": precio,
                    "Info": f"CRYPTO: {sym}"
                })

    return encontrados

def buscar_tokenizados_bingx(tk):

    print("\n📡 ESCANEANDO TOKENIZED MARKET...")

    encontrados = []

    url = "https://open-api.bingx.com/openApi/market/v1/tickers"

    r = requests.get(url, timeout=10).json()

    if r.get("code") == 0:

        tickers = r["data"].get("tickers", [])

        for item in tickers:

            sym = item.get("symbol","")

            limpio = sym.replace("-","").upper()

            if tk in limpio:

                precio = item.get("lastPrice")

                encontrados.append({
                    "Motor": "bingx_tokenized",
                    "Ticker": sym,
                    "Nombre": sym.split("-")[0],
                    "Precio": precio,
                    "Info": f"TOKENIZED: {sym}"
                })

    return encontrados


# ==========================================================
# BUSCAR CFD BINGX REAL (AUTH REQUIRED)
# Version 3.0 TRADEABLE CFD FIX
# ==========================================================

def buscar_cfd_bingx(tk):

    import time, hmac
    from hashlib import sha256
    import config

    print("\n📡 ESCANEANDO MARKET CFD TRADEABLE...")

    encontrados = []

    base = "https://open-api.bingx.com"
    endpoint = "/openApi/cfd/v1/quote/contracts"

    timestamp = str(int(time.time() * 1000))
    params = f"timestamp={timestamp}"

    signature = hmac.new(
        config.BINGX_SECRET.encode("utf-8"),
        params.encode("utf-8"),
        sha256
    ).hexdigest()

    url = f"{base}{endpoint}?{params}&signature={signature}"

    headers = {
        'X-BX-APIKEY': config.BINGX_APIKEY
    }

    r = requests.get(url, headers=headers).json()

    if "data" not in r:
        print("❌ CFD API SIN RESPUESTA")
        return []

    contratos = r["data"]

    tk_norm = tk.replace("/","").upper()

    for c in contratos:

        sym = c.get("symbol","")      # EUR-USD
        underlying = c.get("underlying","")

        limpio = sym.replace("-","").upper()

        if tk_norm == limpio or tk_norm in underlying.replace("/",""):

            encontrados.append({
                "Motor": "bingx_cfd",
                "Ticker": sym,
                "Nombre": underlying,
                "Precio": "CFD TRADEABLE",
                "Info": f"REAL CFD: {sym}"
            })

    return encontrados

def buscar_precio_swap(symbol):

    url = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
    r = requests.get(url, timeout=10).json()

    if r.get("code") != 0:
        return None

    for i in r["data"]:
        if i.get("symbol") == symbol:
            return float(
                i.get("markPrice")
                or i.get("indexPrice")
                or i.get("lastPrice",0)
            )

    return None


# ==========================================================
# Version 1.0 BUSQUEDA DIRECTA CFD NORMALIZADA
# ==========================================================

def buscar_swap_cfd_normalizado(tk):

    encontrados = []

    tk_limpio = limpiar_input_forex(tk)
    symbol = normalizar_cfd_bingx(tk_limpio)

    url = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
    r = requests.get(url, timeout=10).json()

    if r.get("code") != 0:
        return []

    for i in r["data"]:

        key = i.get("symbol")   # ✅ FIX REAL

        if key == symbol:

            precio = float(
                i.get("markPrice")
                or i.get("indexPrice")
                or i.get("lastPrice",0)
            )

            encontrados.append({
                "Motor":"bingx_swap_cfd",
                "Ticker":key,
                "Nombre":tk,
                "Precio":precio,
                "Info":f"CFD REAL: {key}"
            })

    return encontrados

# ==========================================================
# Version 1.0 LIMPIAR FOREX
# ==========================================================

def limpiar_input_forex(symbol):

    s = symbol.upper()

    # elimina basura común de Yahoo / TradingView
    s = s.replace("=X","")
    s = s.replace(".FX","")
    s = s.replace("/","")
    s = s.replace("-","")
    s = s.replace("_","")

    return s


# ==========================================================
# Version 2.14 BINGX CFD NORMALIZER (DINÁMICO)
# ==========================================================

def normalizar_cfd_bingx(symbol_usuario: str):

    s = symbol_usuario.upper().replace("/", "")

    # ===== FOREX =====
    if len(s) == 6:
        base = s[:3]
        quote = s[3:]

        forex = ["EUR","USD","GBP","JPY","AUD","NZD","CAD","CHF",
                 "MXN","TRY","ZAR","SEK","NOK","DKK","ILS"]

        if base in forex and quote in forex:
            return f"NCFX{base}2{quote}-USDT"

    # ===== COMMODITIES =====
    commodities = {
        "GOLD":"NCCOGOLD2USD-USDT",
        "SILVER":"NCCOSILVER2USD-USDT",
        "OILWTI":"NCCOOILWTI2USD-USDT",
        "OILBRENT":"NCCOOILBRENT2USD-USDT",
        "NATURALGAS":"NCCONATURALGAS2USD-USDT",
        "COPPER":"NCCOCOPPER2USD-USDT"
    }

    if s in commodities:
        return commodities[s]

    # ===== INDICES =====
    indices = {
        "SP500":"NCSISP5002USD-USDT",
        "NASDAQ100":"NCSINASDAQ1002USD-USDT",
        "DOWJONES":"NCSIDOWJONES2USD-USDT"
    }

    if s in indices:
        return indices[s]

    # ===== STOCKS DEFAULT =====
    return f"NCSK{s}2USD-USDT"




# ==========================================================
# MAPEADOR BINGX FINAL
# Version 6.2 INSTITUTIONAL CFD ROUTING
# (FOREX + GOLD + INDICES + STOCKS + CRYPTO)
# ==========================================================

def mapeo_bingx(busqueda):

    tk = busqueda.upper().replace("/","")

    encontrados = []

    # 1️⃣ CRYPTO
    crypto = buscar_crypto_bingx(tk)
    if crypto:
        encontrados.extend(crypto)

    # 2️⃣ CFD REAL (FOREX + GOLD + INDICES + STOCKS)
    cfd_meta = buscar_cfd_bingx(tk)

    for c in cfd_meta:

        sym = c["Ticker"]
        precio = buscar_precio_swap(sym)

        if precio:
            encontrados.append({
                "Motor":"bingx_cfd",
                "Ticker":sym,
                "Nombre":c["Nombre"],
                "Precio":precio,
                "Info":"CFD REAL"
            })

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
#if __name__ == "__main__":
#    debug_universo_swap_bingx()
