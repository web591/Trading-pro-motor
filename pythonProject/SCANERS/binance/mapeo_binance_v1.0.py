import requests
import random

# --- CONFIGURACIÓN ---
TICKER_BUSCAR = "BTC" 
# ---------------------

def mapeo_binance(ticker):
    tk = ticker.upper().replace("-", "")
    mozi_list = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Mozilla/5.0 (Macintosh; Intel...)"]
    
    # Estructura Maestra de Retorno
    res = {k: "N/A" for k in ["precio", "nombre_comun", "cambio_24h", "binance_spot", "binance_usdt", "binance_coin_future", "bingx_spot", "bingx_std", "bingx_perp", "finnhub_sym", "yahoo_sym", "alpha_sym"]}
    
    # Identificación de Tickers (Variables de mapeo)
    res["binance_spot"] = f"{tk}USDT (o {tk}USDC)"
    res["binance_usdt"] = f"{tk}USDT"
    res["binance_coin_future"] = f"{tk}USD_PERP"
    
    # URLs de API identificadas
    url_spot = f"https://api.binance.com/api/v3/ticker/24hr?symbol={tk}USDT"
    url_futu = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={tk}USDT"
    
    try:
        r = requests.get(url_spot, headers={"User-Agent": random.choice(mozi_list)}, timeout=5).json()
        if "lastPrice" in r:
            res.update({"precio": r["lastPrice"], "cambio_24h": f"{r['priceChangePercent']}%", "nombre_comun": tk})
    except: pass
    return res

# Test
# print(mapeo_binance(TICKER_BUSCAR))