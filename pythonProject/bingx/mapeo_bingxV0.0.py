import requests

def obtener_mapeo_bingx(ticker):
    """
    Objetivo: Descubrir si el ticker es Cripto, Acción (NCSK), Oro (NCCO) o Forex (NCFX).
    """
    # Mapa de prefijos conocidos
    prefijos = {"GOLD": "NCCOGOLD2USD-USDT", "AAPL": "NCSKAAPL2USD-USDT", "EURUSD": "NCFXEUR2USD-USDT"}
    simbolo_perp = prefijos.get(ticker.upper(), f"{ticker.upper()}-USDT")
    
    url_perp = f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={simbolo_perp}"
    url_spot = f"https://open-api.bingx.com/openApi/spot/v1/ticker/24hr?symbol={ticker.upper()}-USDT"
    
    res = {"perp": "N/A", "spot": "N/A"}
    try:
        r_p = requests.get(url_perp, timeout=5).json()
        if r_p['code'] == 0: res['perp'] = r_p['data']['lastPrice']
        
        r_s = requests.get(url_spot, timeout=5).json()
        if r_s['code'] == 0 and r_s['data']: res['spot'] = r_s['data'][0]['lastPrice']
    except: pass
    
    return res