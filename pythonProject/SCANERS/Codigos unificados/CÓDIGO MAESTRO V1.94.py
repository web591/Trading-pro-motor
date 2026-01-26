import requests
import time
import random
import pandas as pd
import yfinance as yf
from config import FINNHUB_KEY, ALPHA_VANTAGE_KEY

# ==========================================================
# 🚩 CONFIGURACIÓN DE PRUEBA
# ==========================================================
TICKER_PARA_PRUEBA = "EURUSD"
# ==========================================================

def get_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://finance.yahoo.com/'
    }

# 1️⃣ BINANCE: 3 EJES mapeo_binance_v2.0
def mapeo_binance(busqueda):
    tk = busqueda.upper().replace("-", "")
    encontrados = []
    hosts = [
        ("BIN_SPOT", "https://api.binance.com/api/v3/ticker/price"),
        ("BIN_USDT_F", "https://fapi.binance.com/fapi/v1/ticker/price"),
        ("BIN_COIN_F", "https://dapi.binance.com/dapi/v1/ticker/price")
    ]
    for mkt, url in hosts:
        try:
            r = requests.get(url, timeout=10).json()
            data = r if isinstance(r, list) else [r]
            for i in data:
                sym = i.get('symbol','')
                if tk in sym:
                    if mkt == "BIN_SPOT" and not (sym.endswith("USDT") or sym.endswith("USDC")): continue
                    encontrados.append({"Motor": mkt, "Ticker": sym, "Precio": i.get('price', i.get('lastPrice')), "Info": "Crypto Pair"})
        except: continue
    return encontrados

# 2️⃣ BINGX: INTEGRAL mapeo_bingx_v2.0
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
            
# 3️⃣ YAHOO: DISCOVERY mapeo_yahoo_v2.0
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

# 5️⃣ ALPHA VANTAGE mapeo_alpha_vantage_v2.0
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

# 🧠 ENSAMBLADOR V1.94 (INTEGRAL)
# ==========================================================
def ejecutor_maestro_v1_94():
    print(f"💎 MOTOR MAESTRO V1.94 - FULL DISCOVERY & REDUNDANCY")
    print(f"🔍 BUSCANDO GAMA COMPLETA PARA: {TICKER_PARA_PRUEBA}")
    print("-" * 130)
    
    consolidado = []
    motores = [
        ("Binance", mapeo_binance), 
        ("BingX", mapeo_bingx), 
        ("Yahoo", mapeo_yahoo), 
        ("Finnhub", mapeo_finnhub), 
        ("Alpha", mapeo_alpha)
    ]

    for nombre, func in motores:
        print(f"📡 Interrogando {nombre}...")
        try:
            res = func(TICKER_PARA_PRUEBA)
            if res: consolidado.extend(res)
        except Exception as e: print(f"   ⚠️ Error en {nombre}: {e}")
        time.sleep(1)

    # MOSTRAR RESULTADOS
    if consolidado:
        df = pd.DataFrame(consolidado)
        print("\n" + "═"*130)
        print(df[["Motor", "Ticker", "Precio", "Info"]].to_string(index=False, justify='left'))
        print("═"*130)
    else:
        print(f"❌ Sin resultados para '{TICKER_PARA_PRUEBA}'")

if __name__ == "__main__":
    ejecutor_maestro_v1_94()