import requests

# INYECTAR KEY DESDE EL MAESTRO
# ALPHA_KEY = "..."

def rastrear_alpha(busqueda, api_key):
    res = {
        "motor": "ALPHA", "busqueda": busqueda, "nombre_comun": "N/A", "precio": "N/A",
        "alpha_sym": "N/A"
    }
    
    # ---------------------------------------------------------
    # 🔍 FASE 1: SYMBOL SEARCH (Buscador)
    # ---------------------------------------------------------
    try:
        url_search = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={busqueda}&apikey={api_key}"
        r = requests.get(url_search, timeout=10).json()
        
        matches = r.get('bestMatches', [])
        if matches:
            # Alpha devuelve claves raras como '1. symbol'
            best = matches[0]
            sym = best.get('1. symbol')
            name = best.get('2. name')
            
            res["alpha_sym"] = sym
            res["nombre_comun"] = name
            
            # ---------------------------------------------------------
            # 🔍 FASE 2: GLOBAL QUOTE (Solo si encontramos símbolo)
            # ---------------------------------------------------------
            url_quote = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={sym}&apikey={api_key}"
            rq = requests.get(url_quote).json().get('Global Quote', {})
            if rq:
                res["precio"] = rq.get('05. price')
                
    except: pass
    return res