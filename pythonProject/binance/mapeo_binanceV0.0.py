import requests

def obtener_mapeo_binance(ticker):
    """
    Objetivo: Identificar Spot, Futuros USDT y Futuros COIN (Inversos).
    """
    headers = {'User-Agent': 'Mozilla/5.0'}
    base_spot = "https://api.binance.com/api/v3/ticker/price"
    base_usdt_f = "https://fapi.binance.com/fapi/v1/ticker/price"
    base_coin_f = "https://dapi.binance.com/dapi/v1/ticker/price"
    
    t_usdt = f"{ticker}USDT"
    t_coin = f"{ticker}USD_PERP"
    
    resultados = {}
    # Bloque de consulta secuencial
    try:
        r_s = requests.get(f"{base_spot}?symbol={t_usdt}", headers=headers).json()
        resultados['spot'] = r_s.get('price', 'N/A')
        
        r_f = requests.get(f"{base_usdt_f}?symbol={t_usdt}", headers=headers).json()
        resultados['usdt_f'] = r_f.get('price', 'N/A')
        
        r_c = requests.get(f"{base_coin_f}?symbol={t_coin}", headers=headers).json()
        # En DAPI a veces devuelve una lista
        p_c = r_c[0].get('price') if isinstance(r_c, list) else r_c.get('price', 'N/A')
        resultados['coin_f'] = p_c
    except Exception as e:
        resultados['error'] = str(e)
        
    return resultados

# TEST INDEPENDIENTE
# print(obtener_mapeo_binance("BTC"))