import requests

def obtener_mapeo_finnhub(ticker, api_key):
    """
    Objetivo: Mapear el 'Market Name' y la capitalización.
    """
    # Lógica de descubrimiento simplificada
    search_url = f"https://finnhub.io/api/v1/search?q={ticker}&token={api_key}"
    try:
        # Primero buscamos el símbolo exacto que Finnhub prefiere
        search = requests.get(search_url).json()
        best_match = search['result'][0]['symbol'] if search['count'] > 0 else ticker
        
        quote = f"https://finnhub.io/api/v1/quote?symbol={best_match}&token={api_key}"
        r_q = requests.get(quote).json()
        
        return {
            "symbol_finnhub": best_match,
            "precio": r_q.get('c', 'N/A'),
            "cambio": r_q.get('dp', 'N/A')
        }
    except:
        return None