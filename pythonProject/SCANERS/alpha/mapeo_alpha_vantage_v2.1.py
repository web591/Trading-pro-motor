def mapeo_alpha(busqueda):
    encontrados = []
    tk = busqueda.upper().replace("/", "").replace("-", "") # Limpiamos para el buscador
    
    try:
        # FASE 1: SEARCH (Discovery de todo: Acciones, ETFs, Divisas)
        url_search = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={tk}&apikey={ALPHA_VANTAGE_KEY}"
        r_search = requests.get(url_search, timeout=10).json()
        
        for match in r_search.get('bestMatches', [])[:3]:
            sym = match['1. symbol']
            tipo = match['3. type']
            nombre = match['2. name']
            
            # FASE 2: RUTA SEGÚN TIPO DE ACTIVO
            precio = None
            
            if "Currency" in tipo: # Caso Forex o Crypto
                # Alpha requiere separar base y cotizada. Ej: EUR/USD -> from=EUR, to=USD
                if "/" in sym:
                    base, quoted = sym.split("/")
                else:
                    # Si viene "EURUSD", intentamos partirlo o usar USD por defecto
                    base, quoted = (sym[:3], sym[3:]) if len(sym)==6 else (sym, "USD")
                
                url_fx = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={base}&to_currency={quoted}&apikey={ALPHA_VANTAGE_KEY}"
                r_fx = requests.get(url_fx).json().get('Realtime Currency Exchange Rate', {})
                precio = r_fx.get('5. Exchange Rate')
            
            else: # Caso Acciones (Equity) o ETFs
                url_q = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={sym}&apikey={ALPHA_VANTAGE_KEY}"
                rq = requests.get(url_q).json().get('Global Quote', {})
                precio = rq.get('05. price')

            if precio:
                encontrados.append({
                    "Motor": "ALPHA",
                    "Ticker": sym,
                    "Precio": f"{float(precio):.4f}",
                    "Info": f"[{tipo}] {nombre}"
                })
            
            time.sleep(1.3) # Crucial para no quemar la API Key gratuita
            
    except Exception as e:
        pass
        
    return encontrados