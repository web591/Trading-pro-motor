def mapeo_yahoo(busqueda):
    encontrados = []
    # Lista de endpoints para redundancia
    urls = [
        f"https://query2.finance.yahoo.com/v1/finance/search?q={busqueda}",
        f"https://query1.finance.yahoo.com/v1/finance/search?q={busqueda}"
    ]
    
    headers = get_headers() # Usamos tu función de headers existente
    
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=5).json()
            quotes = r.get('quotes', [])
            if not quotes: continue # Si esta URL no trae nada, probamos la siguiente
            
            # Procesamos los resultados (Limitamos a 7 para dar variedad sin saturar)
            for q in quotes[:7]:
                sym = q['symbol']
                try:
                    t = yf.Ticker(sym)
                    # Usamos fast_info para no bloquear la ejecución
                    p = t.fast_info['last_price']
                    
                    # Formateamos la INFO para que el FRONTEND sepa qué es cada cosa
                    tipo = q.get('quoteType', 'N/A')
                    exchange = q.get('exchDisp', 'Global')
                    nombre = q.get('shortname', q.get('longname', 'Asset'))
                    
                    encontrados.append({
                        "Motor": "YAHOO",
                        "Ticker": sym,
                        "Precio": f"{p:.2f}" if p else "N/A",
                        "Info": f"[{tipo}] {nombre} ({exchange})"
                    })
                except: continue
            
            if encontrados: break # Si ya hallamos datos con la primera URL, no usamos la segunda
            
        except Exception as e:
            print(f"   ⚠️ Reintentando Yahoo por bloqueo en endpoint...")
            continue
            
    return encontrados