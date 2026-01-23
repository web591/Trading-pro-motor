import requests
import random

# --- CONFIGURACIÓN ---
MOZILLA_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
]

def escanear_binance(busqueda):
    busqueda = busqueda.upper().replace("-", "")
    res = {
        "motor": "BINANCE",
        "spot_usdt": [],
        "spot_usdc": [],
        "futuros_usdt": [],
        "futuros_coin": [],
        "error": None
    }
    headers = {"User-Agent": random.choice(MOZILLA_AGENTS)}

    try:
        # 1. SPOT (Trae todo el mercado de una vez para filtrar USDT y USDC)
        url_spot = "https://api.binance.com/api/v3/ticker/price"
        r_spot = requests.get(url_spot, headers=headers, timeout=10).json()
        
        for item in r_spot:
            sym = item['symbol']
            p = float(item['price'])
            # Filtro estricto: Que empiece con la búsqueda o la contenga claramente
            if busqueda in sym:
                info = {"ticker": sym, "precio": p}
                if sym.endswith("USDT"):
                    res["spot_usdt"].append(info)
                elif sym.endswith("USDC"):
                    res["spot_usdc"].append(info)

        # 2. FUTUROS USDT (FAPI)
        url_fapi = "https://fapi.binance.com/fapi/v1/ticker/price"
        r_fapi = requests.get(url_fapi, headers=headers, timeout=10).json()
        for item in r_fapi:
            sym = item['symbol']
            if busqueda in sym and sym.endswith("USDT"):
                res["futuros_usdt"].append({"ticker": sym, "precio": float(item['price'])})

        # 3. FUTUROS COIN (DAPI)
        url_dapi = "https://dapi.binance.com/dapi/v1/ticker/price"
        r_dapi = requests.get(url_dapi, headers=headers, timeout=10).json()
        for item in r_dapi:
            sym = item['symbol']
            if busqueda in sym and "USD" in sym:
                res["futuros_coin"].append({"ticker": sym, "precio": float(item['price'])})

    except Exception as e:
        res["error"] = str(e)

    return res