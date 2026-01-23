import requests
import time

def escanear_alpha(busqueda, api_key):
    res = {
        "motor": "ALPHA_VANTAGE",
        "mejor_coincidencia": None,
        "otras_opciones": [], # Solo nombres, sin precio para ahorrar API
        "error": None
    }
    
    try:
        # 1. SYMBOL SEARCH
        url_search = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={busqueda}&apikey={api_key}"
        r = requests.get(url_search, timeout=10).json()
        matches = r.get('bestMatches', [])
        
        if matches:
            # 2. SELECCIÓN
            best = matches[0]
            sym = best.get('1. symbol')
            name = best.get('2. name')
            curr = best.get('8. currency')
            
            # Guardamos las otras opciones solo como texto
            res["otras_opciones"] = [m.get('1. symbol') for m in matches[1:]]
            
            # 3. COTIZACIÓN (Solo al Top 1 para evitar ban)
            # Pausa de seguridad por si acaso
            time.sleep(1) 
            url_q = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={sym}&apikey={api_key}"
            q = requests.get(url_q, timeout=10).json().get('Global Quote', {})
            
            if q:
                res["mejor_coincidencia"] = {
                    "ticker": sym,
                    "nombre": name,
                    "moneda": curr,
                    "precio": float(q.get('05. price', 0))
                }
                
    except Exception as e:
        res["error"] = str(e)
        
    return res