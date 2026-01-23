import requests

def escanear_finnhub(busqueda, api_key):
    res = {
        "motor": "FINNHUB",
        "hallazgos": [],
        "error": None
    }
    
    try:
        # 1. BÚSQUEDA GENERAL
        url_search = f"https://finnhub.io/api/v1/search?q={busqueda}&token={api_key}"
        r = requests.get(url_search, timeout=10).json()
        
        resultados = r.get('result', [])
        
        # 2. FILTRADO Y COTIZACIÓN
        # Finnhub devuelve MUCHOS resultados. Priorizamos los que tienen precio.
        # Limitamos a 5 para cuidar la API Key.
        count = 0
        for item in resultados:
            if count >= 5: break
            
            sym = item['symbol']
            desc = item['description']
            disp = item['displaySymbol']
            
            # Solo cotizamos si parece relevante (contiene la búsqueda)
            if busqueda.upper() in sym or busqueda.upper() in desc.upper():
                try:
                    url_q = f"https://finnhub.io/api/v1/quote?symbol={sym}&token={api_key}"
                    q = requests.get(url_q, timeout=3).json()
                    
                    if 'c' in q and q['c'] != 0:
                        res["hallazgos"].append({
                            "ticker": sym,
                            "display": disp,
                            "precio": q['c'],
                            "desc": desc
                        })
                        count += 1
                except: pass

    except Exception as e:
        res["error"] = str(e)
        
    return res