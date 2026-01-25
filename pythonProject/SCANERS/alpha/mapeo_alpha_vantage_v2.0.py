def mapeo_alpha(busqueda):
    encontrados = []
    tk = busqueda.upper()
    
    try:
        # FASE 1: SEARCH (Discovery)
        url_search = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={tk}&apikey={ALPHA_VANTAGE_KEY}"
        r_search = requests.get(url_search, timeout=10).json()
        
        for match in r_search.get('bestMatches', [])[:3]: # Top 3 mejores resultados
            sym = match['1. symbol']
            tipo = match['3. type'] # "Equity", "ETF", "Physical Currency", etc.
            nombre = match['2. name']
            región = match['4. region']
            
            # FASE 2: DECIDIR QUÉ PRECIO BUSCAR
            precio = "N/A"
            
            # Si es Acción o ETF
            if "Equity" in tipo or "ETF" in tipo:
                url_q = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={sym}&apikey={ALPHA_VANTAGE_KEY}"
                rq = requests.get(url_q).json().get('Global Quote', {})
                precio = rq.get('05. price', "N/A")
            
            # Si es Forex o Crypto (Alpha requiere par contra USD si no se especifica)
            elif "Currency" in tipo:
                # Intentamos obtener la tasa de cambio contra USD
                url_fx = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={sym}&to_currency=USD&apikey={ALPHA_VANTAGE_KEY}"
                r_fx = requests.get(url_fx).json().get('Realtime Currency Exchange Rate', {})
                precio = r_fx.get('5. Exchange Rate', "N/A")

            if precio != "N/A":
                encontrados.append({
                    "Motor": "ALPHA",
                    "Ticker": sym,
                    "Precio": f"{float(precio):.2f}" if precio else "N/A",
                    "Info": f"[{tipo}] {nombre} - {región}"
                })
            
            # IMPORTANTE: Alpha Vantage Free Tier tiene límite de 5 llamadas por minuto.
            # Metemos un pequeño delay para no ser bloqueados.
            time.sleep(1.2) 
            
    except Exception as e:
        print(f"   ⚠️ Error en Alpha Discovery: {e}")
        
    return encontrados