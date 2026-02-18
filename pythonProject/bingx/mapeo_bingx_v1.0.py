import requests

# --- CONFIGURACIÓN ---
TICKER_BUSCAR = "GOLD"
# ---------------------

def mapeo_bingx(ticker):
    tk = ticker.upper()
    # Variables de mapeo identificadas por categoría
    mapa_perp = {
        "GOLD": "NCCOGOLD2USD-USDT", "WTI": "NCCOOILWTI2USD-USDT", 
        "EURUSD": "NCFXEUR2USD-USDT", "AAPL": "NCSKAAPL2USD-USDT", "GOOGL": "NCSKGOOGL2USD-USDT"
    }
    
    res = {k: "N/A" for k in ["precio", "nombre_comun", "cambio_24h", "binance_spot", "binance_usdt", "binance_coin_future", "bingx_spot", "bingx_std", "bingx_perp", "finnhub_sym", "yahoo_sym", "alpha_sym"]}
    
    res["bingx_spot"] = f"{tk}-USDT"
    res["bingx_std"] = f"{tk}-USDT"
    res["bingx_perp"] = mapa_perp.get(tk, f"{tk}-USDT")
    
    # URL identificada para Swap V2
    url = f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={res['bingx_perp']}"
    
    try:
        r = requests.get(url, timeout=5).json()
        if r.get("code") == 0:
            res.update({"precio": r["data"]["lastPrice"], "cambio_24h": f"{r['data']['priceChangePercent']}%"})
    except: pass
    return res