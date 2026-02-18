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
# TOKENIZED CFD REAL (FOREX / GOLD)
# Version 4.2 FIX REAL MATCH BINGX
# ==========================================================
def buscar_tokenizados_cfd_swap(tk):

    print("\n📡 ESCANEANDO SWAP TOKENIZED CFD...")

    encontrados = []

    url_catalogo = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
    url_precio   = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"

    r1 = requests.get(url_catalogo, timeout=10).json()
    r2 = requests.get(url_precio, timeout=10).json()

    if r1.get("code") != 0 or r2.get("code") != 0:
        return []

    contratos = r1["data"]

    # 🔥 Construimos MAP REAL: SYMBOL-MARGIN
    precios = {}

    for p in r2["data"]:
        key = f"{p.get('symbol')}-{p.get('marginAsset')}"
        precios[key] = p

    for c in contratos:

        sym = c.get("symbol","")
        margin = c.get("marginAsset","")

        limpio = sym.replace("-","").upper()

        if tk in limpio:

            key = f"{sym}-{margin}"

            precio_real = 0

            if key in precios:

                precio_real = float(
                    precios[key].get("markPrice") or
                    precios[key].get("indexPrice") or
                    precios[key].get("lastPrice",0)
                )

            encontrados.append({
                "Motor": "bingx_tokenized",
                "Ticker": key,
                "Nombre": sym,
                "Precio": precio_real,
                "Info": f"TOKENIZED CFD: {key}"
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



# ==========================================================
# TOKENIZED CFD VIA UNDERLYING
# Version 1.0
# ==========================================================

def buscar_tokenizados_cfd_swap_underlying(tk):

    url_catalogo = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
    url_precio   = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"

    encontrados = []

    r1 = requests.get(url_catalogo, timeout=10).json()
    r2 = requests.get(url_precio, timeout=10).json()

    if r1.get("code") != 0 or r2.get("code") != 0:
        return []

    contratos = r1["data"]

    precios = {}
    for p in r2["data"]:
        key = f"{p.get('symbol')}-{p.get('marginAsset')}"
        precios[key] = p

    for c in contratos:

        underlying = (c.get("underlying") or "").upper()
        symbol     = c.get("symbol")
        margin     = c.get("marginAsset")

        if not underlying:
            continue

        limpio = underlying.replace("/","")

        if tk in limpio:

            key = f"{symbol}-{margin}"

            precio_real = 0

            if key in precios:
                precio_real = float(
                    precios[key].get("markPrice") or
                    precios[key].get("indexPrice") or
                    precios[key].get("lastPrice",0)
                )

            encontrados.append({
                "Motor": "bingx_swap_cfd",
                "Ticker": key,
                "Nombre": underlying,
                "Precio": precio_real,
                "Info": f"REAL CFD: {underlying}"
            })

    return encontrados

# ==========================================================
# Version 1.0 BUSQUEDA DIRECTA CFD NORMALIZADA
# ==========================================================

def buscar_swap_cfd_normalizado(tk):

    print("\n📡 INTENTANDO MATCH NORMALIZADO BINGX SWAP CFD...")

    encontrados = []

    symbol_normalizado = normalizar_cfd_bingx(tk)

    url = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"

    r = requests.get(url, timeout=10).json()

    if r.get("code") != 0:
        return []

    for item in r["data"]:

        key = f"{item.get('symbol')}-{item.get('marginAsset')}"

        if key == symbol_normalizado:

            precio = float(
                item.get("markPrice") or
                item.get("indexPrice") or
                item.get("lastPrice",0)
            )

            encontrados.append({
                "Motor": "bingx_swap_cfd",
                "Ticker": key,
                "Nombre": tk,
                "Precio": precio,
                "Info": f"CFD NORMALIZED: {key}"
            })

    return encontrados

# ==========================================================
# Version 2.14 BINGX CFD NORMALIZER (DINÁMICO)
# ==========================================================

def normalizar_cfd_bingx(symbol_usuario: str):

    symbol_usuario = symbol_usuario.upper().replace("/", "")

    # FOREX REAL BINGX
    if len(symbol_usuario) == 6:

        base = symbol_usuario[:3]
        quote = symbol_usuario[3:]

        # VALIDAMOS QUE SEA FOREX REAL
        if base in ["EUR","USD","GBP","JPY","AUD","NZD","CAD","CHF"] \
        and quote in ["EUR","USD","GBP","JPY","AUD","NZD","CAD","CHF","MXN","TRY","ZAR","SEK","NOK","DKK","ILS"]:

            return f"NCFX{base}2{quote}-USDT"


    # COMMODITIES
    commodities = {
        "GOLD": "NCCOGOLD2USD-USDT",
        "SILVER": "NCCOSILVER2USD-USDT",
        "OILWTI": "NCCOOILWTI2USD-USDT",
        "OILBRENT": "NCCOOILBRENT2USD-USDT",
        "NATURALGAS": "NCCONATURALGAS2USD-USDT",
        "COPPER": "NCCOCOPPER2USD-USDT"
    }

    if symbol_usuario in commodities:
        return commodities[symbol_usuario]

    # STOCKS
    return f"NCSK{symbol_usuario}2USD-USDT"


# ==========================================================
# SCANNER UNIVERSO REAL BINGX SWAP CFD
# Version 1.0 INGENIERIA INVERSA
# ==========================================================

def debug_universo_swap_bingx():

    print("\n🧪 DESCUBRIENDO UNIVERSO REAL SWAP CFD...\n")

    url = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"

    r = requests.get(url, timeout=10).json()

    if r.get("code") != 0:
        print("❌ ERROR API")
        return

    data = r["data"]

    print(f"TOTAL CONTRATOS: {len(data)}\n")

    for c in data:

        print("--------------------------------------------------")
        print(f"SYMBOL       : {c.get('symbol')}")
        print(f"UNDERLYING   : {c.get('underlying')}")
        print(f"DISPLAY NAME : {c.get('displayName')}")
        print(f"ASSET        : {c.get('asset')}")
        print(f"MARGIN       : {c.get('marginAsset')}")
        print("--------------------------------------------------\n")
#==========================================================
# 📊 BINGX CFD TOKENIZED CONTRACTS LOADER
# Versión 2.13 CFD-READY
# ==========================================================

def cargar_catalogo_cfd_bingx():
    import requests
    import time
    import hmac
    from hashlib import sha256

    print("\n📡 CARGANDO UNIVERSO: BINGX TOKENIZED CFD...")

    base_url = "https://open-api.bingx.com"
    endpoint = "/openApi/cfd/v1/quote/contracts"

    timestamp = str(int(time.time() * 1000))

    params = f"timestamp={timestamp}"

    signature = hmac.new(
        config.BINGX_SECRET.encode("utf-8"),
        params.encode("utf-8"),
        sha256
    ).hexdigest()

    url = f"{base_url}{endpoint}?{params}&signature={signature}"

    headers = {
        'X-BX-APIKEY': config.BINGX_APIKEY
    }

    try:
        response = requests.get(url, headers=headers)
        data = response.json()

        if "data" not in data:
            print("❌ Error cargando CFD Tokenized")
            return {}

        contratos = data["data"]

        universo_cfd = {}

        for c in contratos:

            symbol = c.get("symbol", "")

            # SOLO TOKENIZED NCCO
            if not symbol.startswith("NCCO"):
                continue

            # EJEMPLO:
            # NCCOEURUSD-USDT
            # NCCOGOLD2USD-USDT
            # NCCONAS100USD-USDT

            limpio = symbol.replace("NCCO", "").replace("-USDT", "")

            # GOLD2USD → GOLD
            limpio = limpio.replace("2USD", "")

            universo_cfd[limpio] = {
                "symbol": symbol,
                "tipo": "bingx_tokenized",
                "tradeable": True
            }

        print(f"✅ CFD TOKENIZED CARGADOS: {len(universo_cfd)}")

        return universo_cfd

    except Exception as e:
        print(f"❌ Error CFD BingX: {e}")
        return {}


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

    # 2️⃣ NORMALIZED SWAP CFD 🔥🔥🔥
    swap_norm = buscar_swap_cfd_normalizado(tk)
    if swap_norm:
        encontrados.extend(swap_norm)

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
