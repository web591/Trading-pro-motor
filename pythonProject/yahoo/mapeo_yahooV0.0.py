import yfinance as yf

def obtener_mapeo_yahoo(ticker):
    """
    Objetivo: Probar variaciones de sufijos para encontrar el ticker correcto.
    """
    variaciones = [ticker, f"{ticker}=X", f"{ticker}-USD", f"{ticker}=F"]
    for v in variaciones:
        try:
            t = yf.Ticker(v)
            price = t.fast_info['last_price']
            if price > 0:
                return {"ticker_valido": v, "precio": price, "nombre": t.info.get('shortName')}
        except:
            continue
    return {"ticker_valido": "N/A", "precio": "N/A"}