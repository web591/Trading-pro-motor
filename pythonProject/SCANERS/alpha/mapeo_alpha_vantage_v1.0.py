import requests

# --- CONFIGURACIÓN ---
TICKER_BUSCAR = "IBM"
ALPHA_KEY = "TU_KEY_AQUI"
# ---------------------

def mapeo_alpha(ticker, key):
    tk = ticker.upper()
    res = {k: "N/A" for k in ["precio", "nombre_comun", "cambio_24h", "binance_spot", "binance_usdt", "binance_coin_future", "bingx_spot", "bingx_std", "bingx_perp", "finnhub_sym", "yahoo_sym", "alpha_sym"]}
    
    # URL identificada: Global Quote
    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={tk}&apikey={key}"
    
    try:
        res["alpha_sym"] = tk
        r = requests.get(url, timeout=5).json()
        quote = r.get("Global Quote", {})
        if quote:
            res.update({
                "precio": quote.get("05. price", "N/A"),
                "cambio_24h": quote.get("10. change percent", "N/A")
            })
    except: pass
    return res