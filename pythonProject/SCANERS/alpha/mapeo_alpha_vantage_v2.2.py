def mapeo_alpha(busqueda):
    encontrados = []
    tk = busqueda.upper().replace("/", "")
    
    # 🛡️ MEJORA: Si es un par de 6 letras (Forex), intentamos llamada directa primero
    # Esto ahorra el crédito de "SEARCH" y va directo al precio.
    if len(tk) == 6 and not tk.isdigit():
        try:
            url_fx = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={tk[:3]}&to_currency={tk[3:]}&apikey={ALPHA_VANTAGE_KEY}"
            r = requests.get(url_fx, timeout=10).json()
            
            # Alpha devuelve una "Note" si te pasaste del límite
            if "Note" in r:
                print("   ⚠️ Alpha Vantage: Límite de API alcanzado (25/día o 5/min).")
                return encontrados
                
            rate = r.get('Realtime Currency Exchange Rate', {}).get('5. Exchange Rate')
            if rate:
                encontrados.append({
                    "Motor": "ALPHA", "Ticker": tk, "Precio": f"{float(rate):.4f}",
                    "Info": f"[FOREX] {tk[:3]}/{tk[3:]} Realtime"
                })
                return encontrados # Si esto funciona, no gastamos más créditos
        except: pass

    # Si no es Forex o la llamada directa falló, usamos el buscador normal (Gama Completa)
    try:
        url_s = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={tk}&apikey={ALPHA_VANTAGE_KEY}"
        res_s = requests.get(url_s, timeout=10).json()
        
        if "Note" in res_s:
            print("   ⚠️ Alpha Vantage: Límite de API alcanzado.")
            return encontrados

        for match in res_s.get('bestMatches', [])[:2]: # Reducimos a 2 para ahorrar créditos
            sym = match['1. symbol']
            # ... (resto de la lógica de precios de la V1.95)
            time.sleep(1.5) # Pausa obligatoria
    except: pass
    return encontrados