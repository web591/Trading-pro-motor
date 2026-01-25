def mapeo_alpha(busqueda):
    encontrados = []
    tk = busqueda.upper().replace("/", "")
    
    try:
        # FASE 1: SEARCH (Discovery de Tickers Reales)
        url_s = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={tk}&apikey={ALPHA_VANTAGE_KEY}"
        res_s = requests.get(url_s, timeout=10).json()
        
        # Si Alpha nos avisa del límite, lo reportamos para que sepas por qué no hay datos
        if "Note" in res_s:
            print("   ⚠️ Alpha Vantage: Límite de créditos alcanzado (Espera 1 min).")
            return encontrados

        matches = res_s.get('bestMatches', [])
        for match in matches[:3]: # Revisamos los 3 mejores
            sym = match['1. symbol']
            tipo = match['3. type']
            nombre = match['2. name']
            region = match['4. region']
            
            # PRIORIDAD: Si es Acción (Equity) o ETF, queremos ese Ticker
            precio = "N/A"
            
            # Intentamos buscar el precio para validar que el ticker está activo
            try:
                if "Currency" in tipo:
                    # Lógica de Forex/Crypto (visto en V1.96)
                    base, quoted = (sym[:3], sym[3:]) if "/" not in sym else sym.split("/")
                    url_p = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={base}&to_currency={quoted}&apikey={ALPHA_VANTAGE_KEY}"
                    r_p = requests.get(url_p).json().get('Realtime Currency Exchange Rate', {})
                    precio = r_p.get('5. Exchange Rate')
                else:
                    # Lógica de Acciones/ETFs
                    url_p = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={sym}&apikey={ALPHA_VANTAGE_KEY}"
                    r_p = requests.get(url_p).json().get('Global Quote', {})
                    precio = r_p.get('05. price')
            except:
                precio = "N/A" # Si falla el precio, no morimos, seguimos para darte el Ticker

            # AGREGAR A LA TABLA (Aunque el precio sea N/A, el Ticker es lo que te interesa)
            encontrados.append({
                "Motor": "ALPHA",
                "Ticker": sym,
                "Precio": f"{float(precio):.2f}" if precio and precio != "N/A" else "N/A",
                "Info": f"[{tipo}] {nombre} ({region})"
            })
            
            # Pausa obligatoria entre llamadas de la misma función
            time.sleep(1.5) 

    except Exception as e:
        print(f"   ⚠️ Error en Alpha: {e}")
        
    return encontrados