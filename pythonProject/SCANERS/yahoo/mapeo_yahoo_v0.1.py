import requests
import yfinance as yf
import random

MOZILLA_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
]

def rastrear_yahoo(busqueda):
    res = {
        "motor": "YAHOO", "busqueda": busqueda, "nombre_comun": "N/A", "precio": "N/A",
        "yahoo_sym": "N/A"
    }
    headers = {"User-Agent": random.choice(MOZILLA_AGENTS)}

    # ---------------------------------------------------------
    # 🔍 FASE 1: API DE BÚSQUEDA (Descubrimiento)
    # ---------------------------------------------------------
    try:
        # Esta URL devuelve los tickers sugeridos por Yahoo
        url_query = f"https://query2.finance.yahoo.com/v1/finance/search?q={busqueda}"
        r = requests.get(url_query, headers=headers, timeout=5).json()
        
        candidatos = r.get('quotes', [])
        if candidatos:
            # Tomamos el primer resultado como el más probable
            mejor_opcion = candidatos[0]
            ticker_encontrado = mejor_opcion['symbol']
            
            res["yahoo_sym"] = ticker_encontrado
            res["nombre_comun"] = mejor_opcion.get('shortname', mejor_opcion.get('longname', 'N/A'))

            # ---------------------------------------------------------
            # 🔍 FASE 2: EXTRACCIÓN DE PRECIO (Confirmación)
            # ---------------------------------------------------------
            # Una vez tenemos el ticker exacto (ej. GC=F), pedimos el precio
            yt = yf.Ticker(ticker_encontrado)
            res["precio"] = f"{yt.fast_info['last_price']:.2f}"
            
    except Exception as e: pass
    
    return res