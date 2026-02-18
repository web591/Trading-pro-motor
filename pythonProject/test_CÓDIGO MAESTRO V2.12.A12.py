# ==========================================================
# MOTOR SCANEO TEST v2.0 - UNIFICADO
# ==========================================================

import requests
import yfinance as yf
from config import FINNHUB_KEY

TICKER_PARA_PRUEBA = "COCOA"

def get_headers():
    return {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.yahoo.com/'}

# ==========================================================
# BINANCE
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
# BINGX UNIVERSAL v3.4 - FUNCIONES INTERNAS
# ==========================================================

def _bingx_forex(tk, contratos, precios_dict):
    encontrados = []
    for c in contratos:
        sym = c.get("symbol","").upper()
        underlying = str(c.get("underlying","")).replace("/","").upper()
        asset = str(c.get("asset","")).upper()
        displayName = c.get("displayName","")
        if not asset.startswith("NCFX"): continue
        if tk not in underlying and tk not in displayName.upper(): continue
        precio = precios_dict.get(sym, {}).get("lastPrice")
        if not precio or float(precio)<=0: continue
        nombre = underlying.replace("USDT","").replace("USD","")
        encontrados.append({
            "Motor": "bingx_forex",
            "Ticker": sym,
            "Nombre": nombre,
            "Precio": precio,
            "Info": f"FOREX CFD | {displayName}"
        })
    return encontrados

def _bingx_commodity(tk, contratos, precios_dict):
    encontrados = []
    for c in contratos:
        sym = c.get("symbol","").upper()
        asset = str(c.get("asset","")).upper()
        displayName = c.get("displayName","")
        if not asset.startswith("NCCO"): continue
        underlying = str(c.get("underlying","")).replace("/","").upper()
        if tk not in underlying and tk not in displayName.upper(): continue
        precio = precios_dict.get(sym, {}).get("lastPrice")
        if not precio or float(precio)<=0: continue
        nombre = underlying.replace("USDT","").replace("USD","")
        encontrados.append({
            "Motor": "bingx_commodity",
            "Ticker": sym,
            "Nombre": nombre,
            "Precio": precio,
            "Info": f"COMMODITY CFD | {displayName}"
        })
    return encontrados

def _bingx_stock(tk, contratos, precios_dict):
    encontrados = []
    for c in contratos:
        sym = c.get("symbol","").upper()
        asset = str(c.get("asset","")).upper()
        displayName = c.get("displayName","")
        underlying = str(c.get("underlying","")).replace("/","").upper()

        # Solo stock reales
        if asset.startswith("NCSK") and tk in underlying or tk in displayName.upper():
            precio = precios_dict.get(sym, {}).get("lastPrice")
            if precio and float(precio)>0:
                nombre = underlying.replace("USDT","").replace("USD","")
                encontrados.append({
                    "Motor": "bingx_stock",
                    "Ticker": sym,
                    "Nombre": nombre,
                    "Precio": precio,
                    "Info": f"STOCK CFD | {displayName}"
                })
            # Tokenizadas / sintéticas
            elif asset.startswith("NCSK"):
                nombre = underlying.replace("USDT","").replace("USD","")
                encontrados.append({
                    "Motor": "bingx_stock",
                    "Ticker": sym,
                    "Nombre": nombre,
                    "Precio": "MAPEO",
                    "Info": f"STOCK CFD (Tokenizada) | {displayName}"
                })
    return encontrados


def _bingx_index(tk, contratos, precios_dict):
    encontrados = []
    for c in contratos:
        sym = c.get("symbol","").upper()
        asset = str(c.get("asset","")).upper()
        displayName = c.get("displayName","")
        underlying = str(c.get("underlying","")).replace("/","").upper()

        # Solo índices reales
        if asset.startswith("NCSI") and tk in underlying or tk in displayName.upper():
            precio = precios_dict.get(sym, {}).get("lastPrice")
            if precio and float(precio)>0:
                nombre = underlying.replace("USDT","").replace("USD","")
                encontrados.append({
                    "Motor": "bingx_index",
                    "Ticker": sym,
                    "Nombre": nombre,
                    "Precio": precio,
                    "Info": f"INDEX CFD | {displayName}"
                })
            # Tokenizados / sintéticos
            elif asset.startswith("NCSI"):
                nombre = underlying.replace("USDT","").replace("USD","")
                encontrados.append({
                    "Motor": "bingx_index",
                    "Ticker": sym,
                    "Nombre": nombre,
                    "Precio": "MAPEO",
                    "Info": f"INDEX CFD (Tokenizado) | {displayName}"
                })
    return encontrados

def _bingx_crypto(tk, precios):
    encontrados = []
    for p in precios:
        sym = p.get("symbol","").upper()
        if tk not in sym.replace("-",""): continue
        precio = p.get("lastPrice")
        if not precio or float(precio)<=0: continue
        nombre = sym.replace("USDT","").replace("-","")
        encontrados.append({
            "Motor": "bingx_crypto",
            "Ticker": sym,
            "Nombre": nombre,
            "Precio": precio,
            "Info": f"BingX Crypto: {sym}"
        })
    return encontrados

def mapeo_bingx (busqueda):
    tk = busqueda.upper().replace("/", "").replace("-", "").replace("=X", "")
    encontrados = []

    try:
        contratos = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts", timeout=10).json().get("data", [])
        precios = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker", timeout=10).json().get("data", [])
        precios_dict = {p["symbol"]:p for p in precios}

        # 🚀 Llamadas separadas
        encontrados += _bingx_forex(tk, contratos, precios_dict)
        encontrados += _bingx_commodity(tk, contratos, precios_dict)
        encontrados += _bingx_stock(tk, contratos, precios_dict)
        encontrados += _bingx_index(tk, contratos, precios_dict)
        encontrados += _bingx_crypto(tk, precios)

    except Exception as e:
        print("❌ Error BingX Universal:", e)

    return encontrados


# ==========================================================
# YAHOO
# ==========================================================
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

# ==========================================================
# FINNHUB
# ==========================================================
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
# TEST UNIFICADO
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
        print(f"{r['Motor']:20} | {r['Ticker']:20} | {r['Nombre']:10} | {r['Precio']:10} | {r['Info']}")

if __name__ == "__main__":
    ejecutar_test()
