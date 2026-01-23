import requests

# --- CONFIGURACIÓN ---
TICKER_BUSCAR = "AAPL"
FINNHUB_KEY = "TU_KEY_AQUI"
# ---------------------

def mapeo_finnhub(ticker, key):
    tk = ticker.upper()
    # Variables de mapeo: Prefijos de data real
    prefijos = {"GOLD": "OANDA:XAU_USD", "EURUSD": "OANDA:EUR_USD", "BTC": "BINANCE:BTCUSDT"}
    sym = prefijos.get(tk, tk)
    
    res = {k: "N/A" for k in ["precio", "nombre_comun", "cambio_24h", "binance_spot", "binance_usdt", "binance_coin_future", "bingx_spot", "bingx_std", "bingx_perp", "finnhub_sym", "yahoo_sym", "alpha_sym"]}
    
    # URLs identificadas (Quote y Profile)
    url_quote = f"https://finnhub.io/api/v1/quote?symbol={sym}&token={key}"
    url_profile = f"https://finnhub.io/api/v1/stock/profile2?symbol={tk}&token={key}"
    
    try:
        res["finnhub_sym"] = sym
        r = requests.get(url_quote, timeout=5).json()
        if "c" in r:
            res.update({"precio": r["c"], "cambio_24h": f"{r['dp']}%"})
        
        p = requests.get(url_profile, timeout=5).json()
        res["nombre_comun"] = p.get("name", "N/A")
    except: pass
    return res