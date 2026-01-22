import requests

def obtener_mapeo_alpha(ticker, api_key):
    """
    Objetivo: Usar GLOBAL_QUOTE para acciones y CURRENCY_EXCHANGE para Forex.
    """
    # Alpha Vantage requiere distinguir entre Forex y Acciones
    if len(ticker) > 5: # Asumimos par de divisas (EURUSD)
        from_c, to_c = ticker[:3], ticker[3:]
        url = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={from_c}&to_currency={to_c}&apikey={api_key}"
    else:
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={api_key}"
    
    try:
        r = requests.get(url).json()
        # Alpha Vantage devuelve nombres de llaves largos, hay que mapearlos
        return r
    except:
        return None