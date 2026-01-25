# 3️⃣ YAHOO: DISCOVERY mapeo_yahoo_v1.0
def mapeo_yahoo(busqueda):
    encontrados = []
    try:
        url = f"https://query1.finance.yahoo.com/v1/finance/search?q={busqueda}"
        r = requests.get(url, headers=get_headers(), timeout=10).json()
        for q in r.get('quotes', [])[:5]:
            try:
                sym = q['symbol']
                t = yf.Ticker(sym)
                p = t.fast_info['last_price']
                encontrados.append({"Motor": "YAHOO", "Ticker": sym, "Precio": f"{p:.2f}" if p else "N/A", "Info": f"{q.get('quoteType')} - {q.get('shortname')}"})
            except: continue
    except: pass
    return encontrados