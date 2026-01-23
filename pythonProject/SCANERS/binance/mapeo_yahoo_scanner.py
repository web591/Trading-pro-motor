import requests
import yfinance as yf
import random

MOZILLA_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
]

def escanear_yahoo(busqueda):
    res = {
        "motor": "YAHOO",
        "hallazgos": [], # Lista mixta (Acciones, Futuros, ETFs)
        "error": None
    }
    headers = {"User-Agent": random.choice(MOZILLA_AGENTS)}

    try:
        # 1. BÚSQUEDA DE CANDIDATOS (Discovery)
        url_search = f"https://query2.finance.yahoo.com/v1/finance/search?q={busqueda}"
        r = requests.get(url_search, headers=headers, timeout=5).json()
        
        candidatos = r.get('quotes', [])
        
        # 2. PROCESAMIENTO DE OPCIONES
        # Limitamos a los primeros 5 para no saturar la red
        for item in candidatos[:5]:
            sym = item['symbol']
            tipo = item.get('quoteType', 'N/A')
            nombre = item.get('shortname', item.get('longname', 'N/A'))
            
            # 3. COTIZACIÓN INDIVIDUAL
            try:
                # Usamos fast_info para velocidad
                ticker_obj = yf.Ticker(sym)
                precio = ticker_obj.fast_info['last_price']
                
                res["hallazgos"].append({
                    "ticker": sym,
                    "precio": float(precio) if precio else 0.0,
                    "tipo": tipo,
                    "nombre": nombre
                })
            except:
                # Si falla la cotización, agregamos sin precio para que el usuario sepa que existe
                res["hallazgos"].append({"ticker": sym, "precio": "N/A", "tipo": tipo, "nombre": nombre})

    except Exception as e:
        res["error"] = str(e)

    return res