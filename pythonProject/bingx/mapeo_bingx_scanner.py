import requests
import random

# --- CONFIGURACIÓN ---
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def escanear_bingx(busqueda):
    busqueda = busqueda.upper().strip() # Limpiamos espacios
    res = {
        "motor": "BINGX",
        "perpetual_v2": [],      # Aquí viven los NCCO (Oro, Stocks, Commodities)
        "standard_contract": [], # Futuros Standard
        "spot": [],              # Spot Cripto (BTC-USDT, etc)
        "error": None
    }
    
    # URLs Oficiales
    url_perp_inv = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
    url_std_inv = "https://open-api.bingx.com/openApi/market/v1/tickers"
    # Para Spot, usamos el directorio de símbolos primero (más seguro que adivinar precios)
    url_spot_inv = "https://open-api.bingx.com/openApi/spot/v1/common/symbols" 
    url_spot_price = "https://open-api.bingx.com/openApi/spot/v1/ticker/24hr"

    try:
        # ==========================================================
        # 1. ESCANEO PERPETUAL V2 (El más importante para Tradicionales)
        # ==========================================================
        r_perp = requests.get(url_perp_inv, headers=HEADERS, timeout=10).json()
        if r_perp.get('code') == 0:
            for item in r_perp['data']:
                sym = item['symbol']
                # Buscamos coincidencia flexible (Ej: "GOLD" en "NCCOGOLD...")
                if busqueda in sym:
                    desc_tipo = "Perpetuo Tradicional (BingX)" if "NC" in sym else "Perpetuo Cripto"
                    res["perpetual_v2"].append({
                        "ticker": sym,
                        "precio": float(item['lastPrice']),
                        "desc": desc_tipo
                    })

        # ==========================================================
        # 2. ESCANEO STANDARD CONTRACT (Futuros Cripto Standard)
        # ==========================================================
        r_std = requests.get(url_std_inv, headers=HEADERS, timeout=10).json()
        if r_std.get('code') == 0:
            tickers = r_std['data'].get('tickers', [])
            for item in tickers:
                sym = item['symbol']
                if busqueda in sym:
                    res["standard_contract"].append({
                        "ticker": sym,
                        "precio": float(item['lastPrice']),
                        "desc": "Futuro Standard"
                    })

        # ==========================================================
        # 3. ESCANEO SPOT (El que faltaba)
        # ==========================================================
        # Paso A: Obtener la lista de todo lo que existe en Spot
        r_spot_list = requests.get(url_spot_inv, headers=HEADERS, timeout=10).json()
        
        candidatos_spot = []
        if r_spot_list.get('code') == 0:
            for item in r_spot_list['data']['symbols']:
                sym = item['symbol']
                # Filtramos aquí para no hacer peticiones de precio innecesarias
                if busqueda in sym:
                    candidatos_spot.append(sym)
        
        # Paso B: Obtener precio SOLO de los candidatos encontrados (Para no saturar)
        # BingX Spot no suele dar precio masivo en un endpoint público fácil, pedimos uno por uno
        # Limitamos a los primeros 5 candidatos para evitar bloqueos si la busqueda es muy genérica (ej "A")
        for sym_spot in candidatos_spot[:5]:
            try:
                # El endpoint de precio requiere el parámetro 'symbol'
                r_price = requests.get(f"{url_spot_price}?symbol={sym_spot}", headers=HEADERS, timeout=5).json()
                if r_price.get('code') == 0 and r_price.get('data'):
                    data_price = r_price['data'][0]
                    res["spot"].append({
                        "ticker": sym_spot,
                        "precio": float(data_price['lastPrice']),
                        "desc": "Mercado Spot"
                    })
            except: pass

    except Exception as e:
        res["error"] = f"Error en BingX Scanner: {str(e)}"
        
    return res

# Bloque de prueba individual
if __name__ == "__main__":
    import json
    # Prueba buscando algo que exista en los 3 mundos si es posible, o BTC
    resultado = escanear_bingx("GOLD")
    print(json.dumps(resultado, indent=4))