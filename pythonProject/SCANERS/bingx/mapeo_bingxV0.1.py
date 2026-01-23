import requests
import random

def rastrear_bingx(busqueda):
    busqueda = busqueda.upper()
    res = {
        "motor": "BINGX", "busqueda": busqueda, "nombre_comun": "N/A", "precio": "N/A",
        "bingx_spot": "N/A", "bingx_std": "N/A", "bingx_perp": "N/A"
    }
    
    # ---------------------------------------------------------
    # 🔍 FASE 1: RASTREO DE PERPETUOS (Donde está Stocks/Forex/Commodities)
    # ---------------------------------------------------------
    try:
        url_perp = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
        r = requests.get(url_perp, timeout=10).json()
        
        if r['code'] == 0:
            data = r['data']
            # Algoritmo de Búsqueda Inteligente
            for item in data:
                sym = item['symbol']
                # Caso 1: Coincidencia Exacta Cripto (BTC-USDT)
                if sym == f"{busqueda}-USDT":
                    res["bingx_perp"] = sym
                    res["precio"] = item['lastPrice']
                    break
                # Caso 2: Coincidencia "Sucia" (NCCOGOLD... contiene GOLD)
                # Verificamos que 'busqueda' esté dentro del símbolo y termine en USDT
                elif busqueda in sym and "USDT" in sym:
                    # Filtro extra: Si busco 'GOLD', no quiero 'GOLDMANSACHS' (lógica simple)
                    res["bingx_perp"] = sym
                    res["precio"] = item['lastPrice']
                    res["nombre_comun"] = "BingX Asset"
    except: pass

    # ---------------------------------------------------------
    # 🔍 FASE 2: RASTREO DE SPOT (Solo si es Crypto)
    # ---------------------------------------------------------
    try:
        url_spot = f"https://open-api.bingx.com/openApi/spot/v1/ticker/24hr?symbol={busqueda}-USDT"
        r = requests.get(url_spot, timeout=5).json()
        if r['code'] == 0 and r['data']:
             res["bingx_spot"] = f"{busqueda}-USDT"
    except: pass

    return res