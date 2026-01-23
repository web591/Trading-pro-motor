import yfinance as yf

# --- CONFIGURACIÓN ---
TICKER_BUSCAR = "EURUSD"
# ---------------------

def mapeo_yahoo(ticker):
    tk = ticker.upper()
    # Variables de mapeo por clase de activo
    if any(x in tk for x in ["EUR", "GBP", "MXN"]): sym = f"{tk}=X"
    elif tk in ["BTC", "ETH"]: sym = f"{tk}-USD"
    elif tk == "GOLD": sym = "GC=F"
    elif tk == "WTI": sym = "CL=F"
    else: sym = tk
    
    res = {k: "N/A" for k in ["precio", "nombre_comun", "cambio_24h", "binance_spot", "binance_usdt", "binance_coin_future", "bingx_spot", "bingx_std", "bingx_perp", "finnhub_sym", "yahoo_sym", "alpha_sym"]}
    
    try:
        res["yahoo_sym"] = sym
        y = yf.Ticker(sym)
        # URL de consulta (informativa)
        # url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
        
        info = y.info
        fast = y.fast_info
        res.update({
            "precio": f"{fast['last_price']:.4f}",
            "nombre_comun": info.get("shortName", tk),
            "cambio_24h": f"{((fast['last_price']/fast['previous_close'])-1)*100:.2f}%"
        })
    except: pass
    return res