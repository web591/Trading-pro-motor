# 4️⃣ FINNHUB V1.7 (CON FOREX OANDA Y CRYPTO BINANCE) mapeo_finnhub_v2.0
def mapeo_finnhub(busqueda):
    """
    Escáner de 3 niveles: 
    1. Búsqueda General (Empresas/ETFs)
    2. Forex Oanda (Divisas y Metales reales)
    3. Crypto Binance (Pares con colaterales estables)
    """
    tk = busqueda.upper()
    encontrados = []
    
    # --- NIVEL 1: BÚSQUEDA GENERAL (No se quita) ---
    try:
        url_gen = f"https://finnhub.io/api/v1/search?q={tk}&token={FINNHUB_KEY}"
        r_gen = requests.get(url_gen, timeout=10).json()
        # Tomamos los 3 resultados más relevantes del buscador general
        for i in r_gen.get('result', [])[:3]:
            sym = i['symbol']
            q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_KEY}").json()
            if q.get('c'):
                encontrados.append({
                    "Motor": "FINNHUB_GEN", 
                    "Ticker": sym, 
                    "Precio": q['c'], 
                    "Info": i['description']
                })
    except Exception as e:
        print(f"   ⚠️ Error en Finnhub General: {e}")

    # --- NIVEL 2: FOREX / METALES (OANDA) ---
    # Traductor de emergencia para activos comunes
    traductores = {"GOLD": "XAU_USD", "SILVER": "XAG_USD", "EURUSD": "EUR_USD"}
    target_fx = traductores.get(tk, tk)

    try:
        url_fx = f"https://finnhub.io/api/v1/forex/symbol?exchange=oanda&token={FINNHUB_KEY}"
        r_fx = requests.get(url_fx, timeout=10).json()
        if isinstance(r_fx, list):
            for s in r_fx:
                # Buscamos coincidencia en el símbolo (ej: XAU_USD)
                if target_fx in s['symbol'] or tk in s['displaySymbol']:
                    q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={s['symbol']}&token={FINNHUB_KEY}").json()
                    if q.get('c'):
                        encontrados.append({
                            "Motor": "FINNHUB_FX", 
                            "Ticker": s['symbol'], 
                            "Precio": q['c'], 
                            "Info": f"OANDA: {s['description']}"
                        })
                    break # Encontramos el par principal de Forex, paramos.
    except Exception as e:
        print(f"   ⚠️ Error en Finnhub Forex: {e}")

    # --- NIVEL 3: CRYPTO (BINANCE FEED) ---
    try:
        url_cry = f"https://finnhub.io/api/v1/crypto/symbol?exchange=binance&token={FINNHUB_KEY}"
        r_cry = requests.get(url_cry, timeout=10).json()
        if isinstance(r_cry, list):
            count = 0
            for s in r_cry:
                # Filtramos para que sea el ticker buscado contra USDT o USDC
                if tk in s['symbol'] and ("USDT" in s['symbol'] or "USDC" in s['symbol']):
                    q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={s['symbol']}&token={FINNHUB_KEY}").json()
                    if q.get('c'):
                        encontrados.append({
                            "Motor": "FINNHUB_CRY", 
                            "Ticker": s['symbol'], 
                            "Precio": q['c'], 
                            "Info": f"BINANCE: {s['description']}"
                        })
                    count += 1
                    if count >= 2: break # No saturar con mil pares de crypto
    except Exception as e:
        print(f"   ⚠️ Error en Finnhub Crypto: {e}")

    return encontrados