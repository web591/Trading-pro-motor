def mapeo_bingx(busqueda):
    # Limpiamos la búsqueda: de "EUR/USD" o "EURUSD=X" a "EURUSD"
    tk_search = busqueda.upper().replace("/", "").replace("-", "").replace("=X", "")
    encontrados = []
    
    # 1. DICCIONARIO DE ALIAS (Para activos que cambian de nombre)
    identidades = {
        "GOLD": ["GOLD", "XAU", "PAXG", "XAUT", "NCCOGOLD"],
        "SILVER": ["SILVER", "XAG", "NCCOSILVER"],
        "DAX": ["DAX", "GER", "DE30", "DE40", "GDAXI", "NVDAX"],
        "OIL": ["WTI", "OIL", "CRCL"]
    }
    
    # 2. DETERMINAR RAÍCES DE BÚSQUEDA
    # Si es Forex (6 letras), buscamos tanto el par completo como la moneda base
    familia_adn = identidades.get(tk_search, [tk_search])
    if len(tk_search) == 6 and not tk_search.isdigit():
        base_currency = tk_search[:3] # Ejemplo: EUR de EURUSD
        if base_currency not in familia_adn:
            familia_adn.append(base_currency)

    mercados = [
        ("BINGX_PERP", "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"),
        ("BINGX_SPOT", "https://open-api.bingx.com/openApi/spot/v1/market/ticker")
    ]

    for nombre_mkt, url in mercados:
        try:
            r = requests.get(url, timeout=10).json()
            items = r.get('data', [])
            
            for i in items:
                sym_orig = i.get('symbol', '').upper()
                # Limpieza total del símbolo de BingX para comparar
                # Quitamos prefijos institucionales y monedas de pago
                sym_fix = sym_orig.replace("NCFX", "").replace("NCCO", "").replace("NCSK", "")
                sym_fix = sym_fix.replace("-USDT", "").replace("USDT", "").replace("-USDC", "").replace("USDC", "").replace("-", "")

                match_hallado = False
                for adn in familia_adn:
                    # REGLA MAESTRA:
                    # Si el símbolo limpio es IGUAL al ADN (EURUSD == EURUSD)
                    # O si el símbolo limpio es el par sintético (AAPLX == AAPL + X)
                    if sym_fix == adn or sym_fix == f"{adn}X" or sym_fix == tk_search:
                        match_hallado = True
                        break
                    # Caso especial para Forex en BingX (NCFXEURUSD)
                    if adn in sym_orig and ("NCFX" in sym_orig or "NCCO" in sym_orig):
                        match_hallado = True
                        break

                if match_hallado:
                    # FILTRO ANTI-RUIDO (No queremos GASOLINE si buscamos SOL)
                    if tk_search == "SOL" and "GASOLINE" in sym_orig: continue
                    
                    precio = i.get('lastPrice') or i.get('price')
                    if precio and float(precio) > 0:
                        encontrados.append({
                            "Motor": nombre_mkt,
                            "Ticker": sym_orig,
                            "Precio": precio,
                            "Info": "ADN Auto-Verificado"
                        })
        except: continue
            
    return encontrados
