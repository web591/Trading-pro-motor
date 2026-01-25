import requests
import random
import time

# --- CONFIGURACIÓN ANTI-BLOQUEO ---
MOZILLA_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
]

def rastrear_binance(busqueda):
    busqueda = busqueda.upper().replace("-", "") # Limpieza básica
    res = {
        "motor": "BINANCE", "busqueda": busqueda, "nombre_comun": "N/A", "precio": "N/A",
        "binance_spot_usdt": "N/A", "binance_spot_usdc": "N/A",
        "binance_usdt_fut": "N/A", "binance_coin_fut": "N/A"
    }
    
    headers = {"User-Agent": random.choice(MOZILLA_AGENTS)}
    
    # ---------------------------------------------------------
    # 🔍 FASE 1: RASTREO EN SPOT (USDT / USDC)
    # ---------------------------------------------------------
    try:
        url_spot = "https://api.binance.com/api/v3/ticker/price"
        # Traemos TODOS los precios (más eficiente que buscar uno por uno y fallar)
        r = requests.get(url_spot, headers=headers, timeout=10).json()
        
        for item in r:
            s = item['symbol']
            # Lógica de coincidencia exacta para USDT y USDC
            if s == f"{busqueda}USDT":
                res["binance_spot_usdt"] = s
                res["precio"] = item['price'] # Priorizamos precio Spot
            if s == f"{busqueda}USDC":
                res["binance_spot_usdc"] = s
    except Exception as e: pass

    # ---------------------------------------------------------
    # 🔍 FASE 2: RASTREO EN FUTUROS (USDT-M)
    # ---------------------------------------------------------
    try:
        url_f = "https://fapi.binance.com/fapi/v1/ticker/price"
        r = requests.get(url_f, headers=headers, timeout=10).json()
        for item in r:
            if item['symbol'] == f"{busqueda}USDT":
                res["binance_usdt_fut"] = item['symbol']
    except: pass

    # ---------------------------------------------------------
    # 🔍 FASE 3: RASTREO EN COIN-M (Inversos)
    # ---------------------------------------------------------
    try:
        url_d = "https://dapi.binance.com/dapi/v1/ticker/price"
        r = requests.get(url_d, headers=headers, timeout=10).json()
        for item in r:
            # Coin-M suele ser ticker + USD_PERP
            if f"{busqueda}USD" in item['symbol']: 
                res["binance_coin_fut"] = item['symbol']
    except: pass

    return res