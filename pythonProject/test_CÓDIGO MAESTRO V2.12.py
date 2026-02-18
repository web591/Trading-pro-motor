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

def mapeo_bingx(busqueda):

    tk = busqueda.upper().replace("/", "").replace("-", "").replace("=X", "")
    encontrados = []

    prefijos = [
        "NCFX",   # Forex
        "NCCO",   # Commodities
        "NCSK",   # Stocks
        "NV",     # Index
    ]

    mercados = [
        ("bingx_perp", "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"),
        ("bingx_spot", "https://open-api.bingx.com/openApi/spot/v1/market/ticker")
    ]

    for nombre_mkt, url in mercados:
        try:
            r = requests.get(url, timeout=10).json()
            items = r.get('data', [])

            for i in items:

                sym = i.get('symbol', '').upper()
                precio = i.get('lastPrice') or i.get('price')

                if not precio or float(precio) <= 0:
                    continue

                # =====================================================
                # 1️⃣ MATCH CRYPTO NATURAL (BTC-USDT)
                # =====================================================
                sym_clean = sym.replace("-USDT","").replace("USDT","").replace("-","")

                if sym_clean == tk:
                    encontrados.append({
                        "Motor": nombre_mkt,
                        "Ticker": sym,
                        "Nombre": sym_clean,
                        "Precio": precio,
                        "Info": f"BingX CRYPTO"
                    })
                    continue

                # =====================================================
                # 2️⃣ MATCH INSTITUCIONAL (NCCOGOLD)
                # =====================================================
                root = sym_clean

                for p in prefijos:
                    if root.startswith(p):
                        root = root.replace(p, "", 1)

                if root == tk or root.startswith(tk):
                    encontrados.append({
                        "Motor": nombre_mkt,
                        "Ticker": sym,
                        "Nombre": root,
                        "Precio": precio,
                        "Info": f"BingX SYNTH"
                    })

        except:
            continue

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
