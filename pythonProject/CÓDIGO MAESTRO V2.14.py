import requests
import time
import pandas as pd
import yfinance as yf
import mysql.connector
from datetime import datetime
from config import FINNHUB_KEY, DB_CONFIG, ALPHA_VANTAGE_KEY


# ==========================================================
# 🚩 CONFIGURACIÓN
# ==========================================================
def get_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Referer': 'https://finance.yahoo.com/'
    }

# ==========================================================
# 🚀 MOTORES DE BÚSQUEDA
# ==========================================================

def mapeo_binance(busqueda):
    tk = busqueda.upper().replace("-", "")
    encontrados = []
    hosts = [
        ("binance_spot", "https://api.binance.com/api/v3/ticker/price"),
        ("binance_usdt_future", "https://fapi.binance.com/fapi/v1/ticker/price"),
        ("binance_coin_future", "https://dapi.binance.com/dapi/v1/ticker/price")
    ]
    for mkt, url in hosts:
        try:
            r = requests.get(url, timeout=10).json()
            data = r if isinstance(r, list) else [r]
            for i in data:
                sym = i.get('symbol','')
                if tk in sym:
                    if mkt == "binance_spot" and not (sym.endswith("USDT") or sym.endswith("USDC")): continue
                    precio_crudo = i.get('price', i.get('lastPrice', 0))
                    
                    # --- INICIO CAMBIO: NORMALIZACIÓN DE NOMBRE ---
                    nm_limpio = sym.replace("USDT", "").replace("USDC", "").replace("BUSD", "").replace("USD", "")
                    if "_" in nm_limpio: nm_limpio = nm_limpio.split("_")[0] # Para ADAUSD_PERP -> ADA
                    # --- FIN CAMBIO ---

                    encontrados.append({
                        "Motor": mkt, 
                        "Ticker": sym, 
                        "Nombre": nm_limpio, # Nueva clave para el guardado
                        "Precio": precio_crudo, 
                        "Info": f"Crypto Pair: {sym}"
                    })
        except: continue
    return encontrados

# ==========================================================
# BINGX UNIVERSAL v3.4 - FUNCIONES INTERNAS
# ==========================================================

def _bingx_forex(tk, contratos, precios_dict):
    encontrados = []
    for c in contratos:
        sym = c.get("symbol","").upper()
        underlying = str(c.get("underlying","")).replace("/","").upper()
        asset = str(c.get("asset","")).upper()
        displayName = c.get("displayName","")
        if not asset.startswith("NCFX"): continue
        if tk not in underlying and tk not in displayName.upper(): continue
        precio = precios_dict.get(sym, {}).get("lastPrice")
        if not precio or float(precio)<=0: continue
        nombre = underlying.replace("USDT","").replace("USD","")
        encontrados.append({
            "Motor": "bingx_forex",
            "Ticker": sym,
            "Nombre": nombre,
            "Precio": precio,
            "Info": f"FOREX CFD | {displayName}"
        })
    return encontrados

def _bingx_commodity(tk, contratos, precios_dict):
    encontrados = []
    for c in contratos:
        sym = c.get("symbol","").upper()
        asset = str(c.get("asset","")).upper()
        displayName = c.get("displayName","")
        if not asset.startswith("NCCO"): continue
        underlying = str(c.get("underlying","")).replace("/","").upper()
        if tk not in underlying and tk not in displayName.upper(): continue
        precio = precios_dict.get(sym, {}).get("lastPrice")
        if not precio or float(precio)<=0: continue
        nombre = underlying.replace("USDT","").replace("USD","")
        encontrados.append({
            "Motor": "bingx_commodity",
            "Ticker": sym,
            "Nombre": nombre,
            "Precio": precio,
            "Info": f"COMMODITY CFD | {displayName}"
        })
    return encontrados

# ==========================================================
# BINGX STOCK TOKENIZADAS (NCSK)
# ==========================================================
def _bingx_stock(tk, contratos, precios_dict):
    encontrados = []
    for c in contratos:
        sym = c.get("symbol","").upper()
        asset = str(c.get("asset","")).upper()
        displayName = c.get("displayName","")
        if not asset.startswith("NCSK"): 
            continue

        underlying = str(c.get("underlying","")).replace("/","").upper()
        if tk not in underlying and tk not in displayName.upper(): 
            continue

        # Intentamos obtener lastPrice del endpoint /ticker
        precio = precios_dict.get(sym, {}).get("lastPrice")
        if not precio or float(precio) <= 0: 
            precio = "SIN PRECIO"

        nombre = underlying.replace("USDT","").replace("USD","")
        encontrados.append({
            "Motor": "bingx_stock",
            "Ticker": sym,
            "Nombre": nombre,
            "Precio": precio,
            "Info": f"STOCK CFD | {displayName}"
        })
    return encontrados

# ==========================================================
# BINGX INDEX TOKENIZADAS (NCSI)
# ==========================================================
def _bingx_index(tk, contratos, precios_dict):
    encontrados = []
    for c in contratos:
        sym = c.get("symbol","").upper()
        asset = str(c.get("asset","")).upper()
        displayName = c.get("displayName","")
        if not asset.startswith("NCSI"): 
            continue

        underlying = str(c.get("underlying","")).replace("/","").upper()
        if tk not in underlying and tk not in displayName.upper(): 
            continue

        # Intentamos obtener lastPrice del endpoint /ticker
        precio = precios_dict.get(sym, {}).get("lastPrice")
        if not precio or float(precio) <= 0: 
            precio = "SIN PRECIO"

        nombre = underlying.replace("USDT","").replace("USD","")
        encontrados.append({
            "Motor": "bingx_index",
            "Ticker": sym,
            "Nombre": nombre,
            "Precio": precio,
            "Info": f"INDEX CFD | {displayName}"
        })
    return encontrados


def _bingx_crypto(tk, precios):
    encontrados = []
    for p in precios:
        sym = p.get("symbol","").upper()
        if tk not in sym.replace("-",""): continue
        precio = p.get("lastPrice")
        if not precio or float(precio)<=0: continue
        nombre = sym.replace("USDT","").replace("-","")
        encontrados.append({
            "Motor": "bingx_crypto",
            "Ticker": sym,
            "Nombre": nombre,
            "Precio": precio,
            "Info": f"BingX Crypto: {sym}"
        })
    return encontrados

def mapeo_bingx (busqueda):
    tk = busqueda.upper().replace("/", "").replace("-", "").replace("=X", "")
    encontrados = []

    try:
        contratos = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts", timeout=10).json().get("data", [])
        precios = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker", timeout=10).json().get("data", [])
        precios_dict = {p["symbol"]:p for p in precios}

        # 🚀 Llamadas separadas
        encontrados += _bingx_forex(tk, contratos, precios_dict)
        encontrados += _bingx_commodity(tk, contratos, precios_dict)
        encontrados += _bingx_stock(tk, contratos, precios_dict)
        encontrados += _bingx_index(tk, contratos, precios_dict)
        encontrados += _bingx_crypto(tk, precios)

    except Exception as e:
        print("❌ Error BingX Universal:", e)

    return encontrados


def mapeo_yahoo(busqueda):
    encontrados = []
    url = f"https://query2.finance.yahoo.com/v1/finance/search?q={busqueda}"
    try:
        r = requests.get(url, headers=get_headers(), timeout=10).json()
        for q in r.get('quotes', [])[:7]:
            sym = q.get('symbol')
            if not sym: continue
            try:
                t = yf.Ticker(sym)
                p = t.fast_info['last_price']
                nombre = q.get('shortname') or q.get('longname') or sym
                encontrados.append({"Motor": "yahoo_sym", "Ticker": sym, "Precio": f"{p:.4f}", "Info": f"[{q.get('quoteType', 'Asset')}] {nombre}"})
            except: continue
    except: pass
    return encontrados

def mapeo_finnhub(busqueda):
    tk = busqueda.upper()
    encontrados = []
    try:
        url_gen = f"https://finnhub.io/api/v1/search?q={tk}&token={FINNHUB_KEY}"
        r_gen = requests.get(url_gen, timeout=10).json()
        for i in r_gen.get('result', [])[:3]:
            sym = i['symbol']
            q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_KEY}").json()
            if q.get('c'):
                encontrados.append({"Motor": "finnhub_sym", "Ticker": sym, "Precio": q['c'], "Info": i.get('description', sym)})
    except: pass
    return encontrados

# ==========================================================
# Version 2.13.A16
# ALPHA NORMALIZADO (MULTI-ASSET SAFE)
# ==========================================================
def mapeo_alpha(busqueda):

    encontrados = []

    try:
        url = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={busqueda}&apikey={ALPHA_VANTAGE_KEY}"
        r = requests.get(url, timeout=10).json()

        for match in r.get('bestMatches', [])[:3]:

            sym_raw = match.get('1. symbol')
            if not sym_raw:
                continue

            # 🔥 NORMALIZACIÓN IGUAL QUE BINGX
            sym_clean = sym_raw.split(".")[0]

            url_q = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={sym_clean}&apikey={ALPHA_VANTAGE_KEY}"
            rq = requests.get(url_q, timeout=10).json().get('Global Quote', {})

            precio = rq.get('05. price')

            if precio and float(precio) > 0:

                encontrados.append({
                    "Motor": "alpha_sym",
                    "Ticker": sym_clean,
                    "Nombre": sym_clean,
                    "Precio": float(precio),
                    "Info": match.get('2. name', sym_clean)
                })

            time.sleep(1.2)

    except Exception as e:
        print("❌ Alpha Error:", e)

    return encontrados

# ==========================================================
# 💾 PERSISTENCIA Y MEMORIA
# ==========================================================

def guardar_en_resultados_db(conn, hallazgos, id_tarea, busqueda_original):
    cur = conn.cursor(dictionary=True)
    try:
        tk_clean = busqueda_original.upper().strip()

        # 1. Filtro de Stables
        if tk_clean in ['USDT', 'USDC', 'DAI', 'BUSD', 'USD']:
            cur.execute("UPDATE sys_simbolos_buscados SET status = 'ignorado' WHERE id = %s", (id_tarea,))
            conn.commit()
            return

        hallazgos_limpios = [h for h in hallazgos if len(str(h.get('Ticker', ''))) <= 25]
        if not hallazgos_limpios:
            cur.execute("UPDATE sys_simbolos_buscados SET status = 'no_encontrado' WHERE id = %s", (id_tarea,))
            conn.commit()
            return

        # 2. Insertar resultados (Evitar duplicados en la misma búsqueda)
        for h in hallazgos_limpios:
            try:
                precio_val = float(str(h.get('Precio', '0')).replace(',', ''))
                # --- INICIO CAMBIO: PRIORIDAD AL NOMBRE NORMALIZADO ---
                # Si el motor nos dio un nombre limpio (como ADA), lo usamos. 
                # Si no (como en Yahoo), usamos la búsqueda original (tk_clean).
                nombre_final = h.get('Nombre', tk_clean)

                cur.execute("""
                    INSERT INTO sys_busqueda_resultados (busqueda_id, nombre_comun, motor, ticker, precio, info) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (id_tarea, nombre_final, h['Motor'], h['Ticker'], precio_val, h.get('Info', h['Ticker'])))
                # --- FIN CAMBIO ---
            except: continue

        # 3. Traductor y Status Final
        best = hallazgos_limpios[0]
        m_fuente = best['Motor'].lower()
        # Intentamos obtener el nombre normalizado (ej: ADA) si el motor lo generó, 
        # si no, usamos la búsqueda original (ej: BITCOIN)
        nombre_para_vincular = best.get('Nombre', tk_clean)
        
        # CATEGORÍA: Si es Yahoo y tiene puntos o guiones, suele ser STOCK/INDEX, si no SPOT
        cat = 'SPOT'
        if 'yahoo' in m_fuente:
            cat = 'STOCK' if ('.' in best['Ticker'] or '-' in best['Ticker']) else 'SPOT'
        elif 'future' in m_fuente or 'perp' in m_fuente:
            cat = 'PERPETUAL'

        # BUSCAMOS SI YA EXISTE EN EL TRADUCTOR
        # Importante: Buscamos por Ticker + Motor para estar seguros
        cur.execute("""
            SELECT id FROM sys_traductor_simbolos 
            WHERE ticker_motor = %s AND motor_fuente = %s 
            LIMIT 1
        """, (best['Ticker'], m_fuente))
        
        res_trad = cur.fetchone()
        trad_id = res_trad['id'] if res_trad else None

        # Si NO existe en el traductor, trad_id será None. 
        # Esto está BIEN. El usuario lo creará al darle a "Añadir" en la web.

        # 🔥 ACTUALIZAMOS LA TAREA PARA QUE APAREZCA EN EL RADAR
        cur.execute("""
            UPDATE sys_simbolos_buscados 
            SET traductor_id = %s, status = 'validar' 
            WHERE id = %s
        """, (trad_id, id_tarea))
        
        conn.commit()
        print(f"    ✅ Tarea {id_tarea} finalizada -> status: validar")
        
    except Exception as e:
        print(f"    ⚠️ Error en Guardado: {e}")
        conn.rollback()
    finally:
        cur.close()

# ==========================================================
# 🚀 ORQUESTADOR
# ==========================================================
def bucle_operativo():
    print(f"💎 MOTOR MAESTRO V2.11 - MULTIUSUARIO ONLINE (MEMORY ENABLED)")
    conn = None 

    while True:
        try:
            if conn is None or not conn.is_connected():
                conn = mysql.connector.connect(**DB_CONFIG)

            # 1. Buscar tarea
            cur = conn.cursor(dictionary=True, buffered=True)
            cur.execute("SELECT id, ticker FROM sys_simbolos_buscados WHERE status IN ('pendiente', 'encontrado') LIMIT 1")
            tarea = cur.fetchone()
            cur.close()

            if tarea:
                id_tarea = tarea['id']
                tk_busqueda = tarea['ticker'].upper().strip()
                
                # Bloqueo inmediato
                cur_upd = conn.cursor()
                cur_upd.execute("UPDATE sys_simbolos_buscados SET status = 'procesando' WHERE id = %s", (id_tarea,))
                conn.commit()
                cur_upd.close()
                
                print(f"\n🎯 PROCESANDO: {tk_busqueda} (ID: {id_tarea})")
                
                # --- PASO A: REVISAR MEMORIA 🧠 ---
                cur_mem = conn.cursor(dictionary=True)
                query_memoria = """SELECT motor, ticker, precio, info 
                                   FROM sys_busqueda_resultados 
                                   WHERE nombre_comun = %s 
                                   GROUP BY motor, ticker LIMIT 15"""
                cur_mem.execute(query_memoria, (tk_busqueda,))
                existentes = cur_mem.fetchall()
                cur_mem.close()

                if existentes:
                    print(f"🧠 MEMORIA: {tk_busqueda} ya está en caché. Sincronizando...")
                    # Convertimos memoria al formato que espera el guardado
                    hallazgos_mem = [{"Motor": f['motor'], "Ticker": f['ticker'], "Precio": f['precio'], "Info": f['info']} for f in existentes]
                    guardar_en_resultados_db(conn, hallazgos_mem, id_tarea, tk_busqueda)
                else:
                    # --- PASO B: BÚSQUEDA FRESCA 📡 ---
                    print(f"🔍 Interrogando mercados para {tk_busqueda}...")
                    consolidado = []
                    
                    # Ejecutamos motores definidos arriba
                    motores = [
                        ("Binance", mapeo_binance), 
                        ("BingX", mapeo_bingx), 
                        ("Yahoo", mapeo_yahoo),
                        ("Finnhub", mapeo_finnhub), 
                        ("AlphaVantage", mapeo_alpha)
                    ]

                    for nombre, funcion in motores:
                        try:
                            res = funcion(tk_busqueda)
                            if res: consolidado.extend(res)
                        except: continue

                    guardar_en_resultados_db(conn, consolidado, id_tarea, tk_busqueda)
                
                time.sleep(1)
            else:
                print(".", end="", flush=True)
                time.sleep(10) 

        except Exception as e:
            print(f"⚠️ Error Crítico en Bucle: {e}")
            time.sleep(5)

if __name__ == "__main__":
    bucle_operativo()