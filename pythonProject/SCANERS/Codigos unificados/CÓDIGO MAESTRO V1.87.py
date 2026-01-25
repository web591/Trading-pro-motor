import requests
import time
import random
import pandas as pd
import yfinance as yf
from config import FINNHUB_KEY, ALPHA_VANTAGE_KEY

# ==========================================================
# 🚩 CONFIGURACIÓN DE PRUEBA
# ==========================================================
TICKER_PARA_PRUEBA = "GOLD"
# ==========================================================

def get_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://finance.yahoo.com/'
    }

# 1️⃣ BINANCE: 3 EJES
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

# 2️⃣ BINGX: INTEGRAL
def mapeo_bingx(busqueda):
    tk = busqueda.upper().replace("/", "").replace("-", "").replace("=X", "")
    encontrados = []
    
    # 1. LISTA DE "RAÍCES" CONOCIDAS (Para mapear DAX -> GER, etc.)
    # Solo necesitamos mapear lo que cambia de nombre radicalmente.
    alias_maestros = {
        "DAX": ["GER", "DE30", "DE40", "NVDAX", "GDAXI"],
        "GOLD": ["XAU", "GOLD", "PAXG", "XAUT"],
        "SILVER": ["XAG", "SILVER"],
        "OIL": ["WTI", "CRCL", "NCSKCRCL"]
    }
    
    # Si el ticker tiene alias, los usamos; si no, usamos el ticker mismo.
    busqueda_extendida = alias_maestros.get(tk, [tk])

    mercados = [
        ("BINGX_SPOT", "https://open-api.bingx.com/openApi/spot/v1/market/ticker"),
        ("BINGX_PERP", "https://open-api.bingx.com/openApi/swap/v2/quote/ticker")
    ]

    for nombre_mkt, url in mercados:
        try:
            r = requests.get(url, timeout=10).json()
            # Esta es la "Gran Llamada" que trae TODO el catálogo
            items = r.get('data', [])
            
            for i in items:
                symbol_original = i.get('symbol', '').upper()
                
                # Normalizamos el símbolo que viene de la API para comparar
                # Quitamos sufijos de moneda para quedarnos con la raíz
                sym_root = symbol_original.replace("-USDT", "").replace("USDT", "").replace("-USDC", "").replace("USDC", "").replace("-", "")

                for raiz in busqueda_extendida:
                    es_match = False
                    
                    # REGLA 1: Match Exacto (Cripto/Forex directo)
                    if sym_root == raiz:
                        es_match = True
                    
                    # REGLA 2: Es un contrato institucional (NCFX, NCCO, NCSK + Ticker)
                    elif any(symbol_original.startswith(pre) for pre in ["NCFX", "NCCO", "NCSK"]):
                        if raiz in symbol_original:
                            # Evitamos falsos positivos: 'SOL' no debe entrar en 'GASOLINE'
                            # Solo entra si la raíz está después del prefijo
                            if any(symbol_original.startswith(pre + raiz) for pre in ["NCFX", "NCCO", "NCSK"]):
                                es_match = True

                    # REGLA 3: Es un sintético con sufijo (Ticker + X)
                    elif sym_root == f"{raiz}X":
                        es_match = True

                    if es_match:
                        precio = i.get('lastPrice') or i.get('price')
                        if precio and float(precio) > 0:
                            encontrados.append({
                                "Motor": nombre_mkt,
                                "Ticker": symbol_original,
                                "Precio": precio,
                                "Info": "Auto-Detectado"
                            })
                        break # Si ya hizo match, no probar otras raíces para este símbolo
        except Exception as e:
            continue
            
    return encontrados
            
# 3️⃣ YAHOO: DISCOVERY
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

# 4️⃣ FINNHUB V1.7 (CON FOREX OANDA Y CRYPTO BINANCE)
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

# 5️⃣ ALPHA VANTAGE
def mapeo_alpha(busqueda):
    encontrados = []
    try:
        url = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={busqueda}&apikey={ALPHA_VANTAGE_KEY}"
        r = requests.get(url, timeout=10).json()
        for match in r.get('bestMatches', [])[:2]:
            sym = match['1. symbol']
            url_q = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={sym}&apikey={ALPHA_VANTAGE_KEY}"
            rq = requests.get(url_q).json().get('Global Quote', {})
            if rq:
                encontrados.append({"Motor": "ALPHA", "Ticker": sym, "Precio": rq.get('05. price'), "Info": match['2. name']})
            time.sleep(1.2) 
    except: pass
    return encontrados

# ==========================================================
# 🧠 ENSAMBLADOR V1.6
# ==========================================================
def ejecutor_maestro_v1_6():
    print(f"💎 CÓDIGO MAESTRO V1.87 - FILTRADO FINNHUB FX/CRYPTO")
    print(f"🔍 ESCANEANDO: {TICKER_PARA_PRUEBA}")
    print("-" * 125)
    
    consolidado = []
    motores = [
        ("Binance", mapeo_binance), ("BingX", mapeo_bingx), 
        ("Yahoo", mapeo_yahoo), ("Finnhub", mapeo_finnhub), ("Alpha", mapeo_alpha)
    ]

    for nombre, func in motores:
        print(f"📡 Interrogando {nombre}...")
        try:
            res = func(TICKER_PARA_PRUEBA)
            if res: consolidado.extend(res)
        except Exception as e: print(f"   ⚠️ Error en {nombre}: {e}")
        time.sleep(1.2)

    print("\n" + "═"*130)
    print(f"📊 RESULTADO FINAL PARA: {TICKER_PARA_PRUEBA}")
    print("═"*130)
    
    if consolidado:
        df = pd.DataFrame(consolidado)
        pd.set_option('display.max_colwidth', 60)
        print(df[["Motor", "Ticker", "Precio", "Info"]].to_string(index=False, justify='left'))
    else:
        print(f"❌ Sin resultados para '{TICKER_PARA_PRUEBA}'")
    
    print("═"*130)

if __name__ == "__main__":
    ejecutor_maestro_v1_6()