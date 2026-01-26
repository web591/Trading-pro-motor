import requests
import time
import random
import pandas as pd
import yfinance as yf
import mysql.connector
from datetime import datetime
from config import FINNHUB_KEY, ALPHA_VANTAGE_KEY, DB_CONFIG

# ==========================================================
# 🚩 CONFIGURACIÓN Y CABECERAS (V1.98)
# ==========================================================
def get_headers():
    return {
        # Identidad: Chrome 120 (más actual)
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        
        # Qué archivos aceptas (incluimos html y xml para parecer navegador)
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        
        # Idioma del "humano"
        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
        
        # De dónde vienes (Tu código ya lo tenía y es excelente)
        'Referer': 'https://finance.yahoo.com/',
        
        # Comportamiento humano
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1' # Le dice que prefieres sitios seguros
    }

# ==========================================================
# 🧠 ESPACIO PARA TUS MOTORES (Copia aquí tus mapeo_... de V1.98)
# ==========================================================

# 🚀 1️⃣ BINANCE: 3 EJES (Versión con Info Robusta)
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
                    
                    # Obtenemos el precio
                    precio_crudo = i.get('price', i.get('lastPrice', 0))
                    
                    # --- AQUÍ ESTÁ EL CAMBIO ---
                    # Si no hay una descripción, usamos "Crypto Pair: " seguido del nombre del Ticker
                    info_backup = f"Crypto Pair: {sym}"
                    
                    encontrados.append({
                        "Motor": mkt, 
                        "Ticker": sym, 
                        "Precio": precio_crudo, 
                        "Info": info_backup
                    })
        except: continue
    return encontrados

# 🚀 2️⃣ BINGX: INTEGRAL (Versión V2.02 - Corregida)
def mapeo_bingx(busqueda):
    # Limpiamos la búsqueda
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
    familia_adn = identidades.get(tk_search, [tk_search])
    if len(tk_search) == 6 and not tk_search.isdigit():
        base_currency = tk_search[:3]
        if base_currency not in familia_adn:
            familia_adn.append(base_currency)

    # 3. MERCADOS ALINEADOS A COLUMNAS SQL
    mercados = [
        ("bingx_perp", "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"),
        ("bingx_spot", "https://open-api.bingx.com/openApi/spot/v1/market/ticker")
    ]

    for nombre_mkt, url in mercados:
        try:
            r = requests.get(url, timeout=10).json()
            items = r.get('data', [])
            
            for i in items:
                sym_orig = i.get('symbol', '').upper()
                # Limpieza para comparación
                sym_fix = sym_orig.replace("NCFX", "").replace("NCCO", "").replace("NCSK", "")
                sym_fix = sym_fix.replace("-USDT", "").replace("USDT", "").replace("-USDC", "").replace("USDC", "").replace("-", "")

                match_hallado = False
                for adn in familia_adn:
                    if sym_fix == adn or sym_fix == f"{adn}X" or sym_fix == tk_search:
                        match_hallado = True
                        break
                    if adn in sym_orig and ("NCFX" in sym_orig or "NCCO" in sym_orig):
                        match_hallado = True
                        break

                if match_hallado:
                    # Filtro anti-ruido
                    if tk_search == "SOL" and "GASOLINE" in sym_orig: continue
                    
                    precio = i.get('lastPrice') or i.get('price')
                    if precio and float(precio) > 0:
                        
                        # --- BLINDAJE DE INFO (Corregido para nombres nuevos) ---
                        tipo = "Standard"
                        if "bingx_perp" in nombre_mkt: tipo = "Futures/Perp"
                        
                        sub_tipo = "Asset"
                        if "NCFX" in sym_orig: sub_tipo = "Forex"
                        elif "NCCO" in sym_orig: sub_tipo = "Commodity"
                        elif sym_orig.endswith("X"): sub_tipo = "Synthetic Stock"
                        
                        # Construcción de Info final
                        info_final = f"BingX {tipo} [{sub_tipo}]: {sym_orig}"

                        encontrados.append({
                            "Motor": nombre_mkt, # Esto insertará 'bingx_perp' o 'bingx_spot'
                            "Ticker": sym_orig,
                            "Precio": precio,
                            "Info": info_final
                        })
        except Exception as e:
            print(f"    ⚠️ Error en loop BingX: {e}")
            continue
            
    return encontrados
            
# 🚀 3️⃣ YAHOO: DISCOVERY (Versión con Info Robusta y Blindaje)
def mapeo_yahoo(busqueda):
    encontrados = []
    # Lista de endpoints para redundancia
    urls = [
        f"https://query2.finance.yahoo.com/v1/finance/search?q={busqueda}",
        f"https://query1.finance.yahoo.com/v1/finance/search?q={busqueda}"
    ]
    
    headers = get_headers()
    
    # --- 1. ESPERA INICIAL ---
    time.sleep(random.uniform(1.0, 2.0))
    
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=5).json()
            quotes = r.get('quotes', [])
            if not quotes: continue 
            
            # Procesamos los resultados (Limitamos a 7)
            for q in quotes[:7]:
                sym = q.get('symbol')
                if not sym: continue

                try:
                    # --- 2. ESPERA POR CADA TICKER (CRÍTICO) ---
                    time.sleep(random.uniform(0.5, 1.2)) 
                    
                    t = yf.Ticker(sym)
                    p = t.fast_info['last_price']
                    
                    # --- MEJORA DE INFO Y BLINDAJE ---
                    tipo = q.get('quoteType', 'Asset')
                    exchange = q.get('exchDisp', 'Global')
                    # Buscamos el nombre corto, si no el largo, si no el ticker
                    nombre = q.get('shortname') or q.get('longname') or sym
                    
                    # Construimos la descripción final
                    info_final = f"[{tipo}] {nombre} ({exchange})"
                    
                    encontrados.append({
                        "Motor": "yahoo_sym",
                        "Ticker": sym,
                        "Precio": f"{p:.4f}" if p else "0.0000",
                        "Info": info_final # <-- Garantizado que nunca sea nulo
                    })
                except: 
                    continue
            
            if encontrados: break 
            
        except Exception as e:
            time.sleep(2)
            print(f"    ⚠️ Reintentando Yahoo por bloqueo en endpoint...")
            continue
            
    return encontrados

# 🚀 4️⃣ FINNHUB V1.7 (Versión con Info Robusta y Blindaje)
def mapeo_finnhub(busqueda):
    """
    Escáner de 3 niveles: 
    1. Búsqueda General (Empresas/ETFs)
    2. Forex Oanda (Divisas y Metales reales)
    3. Crypto Binance (Pares con colaterales estables)
    """
    tk = busqueda.upper()
    encontrados = []
    
    # --- NIVEL 1: BÚSQUEDA GENERAL ---
    try:
        url_gen = f"https://finnhub.io/api/v1/search?q={tk}&token={FINNHUB_KEY}"
        r_gen = requests.get(url_gen, timeout=10).json()
        for i in r_gen.get('result', [])[:3]:
            sym = i['symbol']
            q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_KEY}").json()
            if q.get('c'):
                # Respaldo de descripción
                desc = i.get('description') or f"Asset: {sym}"
                encontrados.append({
                    "Motor": "finnhub_sym", 
                    "Ticker": sym, 
                    "Precio": q['c'], 
                    "Info": desc
                })
    except Exception as e:
        print(f"    ⚠️ Error en Finnhub General: {e}")

    # --- NIVEL 2: FOREX / METALES (OANDA) ---
    traductores = {"GOLD": "XAU_USD", "SILVER": "XAG_USD", "EURUSD": "EUR_USD"}
    target_fx = traductores.get(tk, tk)

    try:
        url_fx = f"https://finnhub.io/api/v1/forex/symbol?exchange=oanda&token={FINNHUB_KEY}"
        r_fx = requests.get(url_fx, timeout=10).json()
        if isinstance(r_fx, list):
            for s in r_fx:
                if target_fx in s['symbol'] or tk in s['displaySymbol']:
                    q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={s['symbol']}&token={FINNHUB_KEY}").json()
                    if q.get('c'):
                        # Blindaje de descripción
                        desc = s.get('description') or f"Forex Pair {s['symbol']}"
                        encontrados.append({
                            "Motor": "finnhub_sym", 
                            "Ticker": s['symbol'], 
                            "Precio": q['c'], 
                            "Info": f"OANDA: {desc}"
                        })
                    break 
    except Exception as e:
        print(f"    ⚠️ Error en Finnhub Forex: {e}")

    # --- NIVEL 3: CRYPTO (BINANCE FEED) ---
    try:
        url_cry = f"https://finnhub.io/api/v1/crypto/symbol?exchange=binance&token={FINNHUB_KEY}"
        r_cry = requests.get(url_cry, timeout=10).json()
        if isinstance(r_cry, list):
            count = 0
            for s in r_cry:
                if tk in s['symbol'] and ("USDT" in s['symbol'] or "USDC" in s['symbol']):
                    q = requests.get(f"https://finnhub.io/api/v1/quote?symbol={s['symbol']}&token={FINNHUB_KEY}").json()
                    if q.get('c'):
                        # Blindaje de descripción
                        desc = s.get('description') or f"Crypto {s['symbol']}"
                        encontrados.append({
                            "Motor": "finnhub_sym", 
                            "Ticker": s['symbol'], 
                            "Precio": q['c'], 
                            "Info": f"BINANCE: {desc}"
                        })
                    count += 1
                    if count >= 2: break 
    except Exception as e:
        print(f"    ⚠️ Error en Finnhub Crypto: {e}")

    return encontrados

# 🚀 5️⃣ ALPHA VANTAGE: DISCOVERY (Versión con Info Robusta y Blindaje)
def mapeo_alpha(busqueda):
    encontrados = []
    tk = busqueda.upper().replace("/", "")
    
    try:
        # FASE 1: SEARCH (Discovery de Tickers Reales)
        url_s = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={tk}&apikey={ALPHA_VANTAGE_KEY}"
        res_s = requests.get(url_s, timeout=10).json()
        
        if "Note" in res_s:
            print("    ⚠️ Alpha Vantage: Límite de créditos alcanzado (Espera 1 min).")
            return encontrados

        matches = res_s.get('bestMatches', [])
        for match in matches[:3]: 
            sym = match.get('1. symbol')
            if not sym: continue

            tipo = match.get('3. type', 'Equity')
            nombre = match.get('2. name') or sym
            region = match.get('4. region', 'Global')
            
            precio = 0.0
            
            # Intentamos buscar el precio
            try:
                if "Currency" in tipo:
                    # Lógica de Forex/Crypto
                    base, quoted = (sym[:3], sym[3:]) if "/" not in sym else sym.split("/")
                    url_p = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={base}&to_currency={quoted}&apikey={ALPHA_VANTAGE_KEY}"
                    r_p = requests.get(url_p).json().get('Realtime Currency Exchange Rate', {})
                    p_val = r_p.get('5. Exchange Rate')
                else:
                    # Lógica de Acciones/ETFs
                    url_p = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={sym}&apikey={ALPHA_VANTAGE_KEY}"
                    r_p = requests.get(url_p).json().get('Global Quote', {})
                    p_val = r_p.get('05. price')
                
                if p_val:
                    precio = float(str(p_val).replace(',', ''))
            except:
                precio = 0.0

            # --- MEJORA DE INFO Y BLINDAJE ---
            # Aseguramos que Info nunca sea nulo y describa bien el activo
            info_final = f"[{tipo}] {nombre} ({region})"

            encontrados.append({
                "Motor": "alpha_sym",
                "Ticker": sym,
                "Precio": precio,
                "Info": info_final # <-- Blindado
            })
            
            time.sleep(1.5) 

    except Exception as e:
        print(f"    ⚠️ Error en Alpha: {e}")
        
    return encontrados

# ==========================================================
# 💾 PERSISTENCIA EN sys_busqueda_resultados (CORREGIDO)
# ==========================================================

def guardar_en_resultados_db(conn, hallazgos, id_tarea, busqueda_original):
    try:
        cur = conn.cursor()
        # Esta instrucción inserta o actualiza si ya existe el ticker para este activo
        query = """INSERT INTO sys_busqueda_resultados 
                   (busqueda_id, nombre_comun, motor, ticker, precio, info) 
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE 
                   precio = VALUES(precio),
                   info = VALUES(info),
                   busqueda_id = VALUES(busqueda_id),
                   fecha_hallazgo = CURRENT_TIMESTAMP"""
        
        for h in hallazgos:
            try:
                # Limpiamos el precio: quitamos comas y convertimos a número
                p_str = str(h['Precio']).replace(',', '').strip()
                precio_clean = float(p_str)
            except:
                precio_clean = 0.0

            cur.execute(query, (
                id_tarea, 
                busqueda_original.upper(), 
                h['Motor'], 
                h['Ticker'], 
                precio_clean, 
                h['Info'] if h['Info'] else h['Ticker'] # Respaldo de Info
            ))
            
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"    ⚠️ Error en DB: {e}")

# ==========================================================
# 🧹 FUNCIÓN DE MANTENIMIENTO
# ==========================================================
def limpiar_resultados_antiguos(conn):
    """Borra registros de la tabla resultados que tengan más de 15 minutos"""
    try:
        cur = conn.cursor()
        # Nota: La columna fecha_hallazgo debe existir en tu tabla
        query = "DELETE FROM sys_busqueda_resultados WHERE fecha_hallazgo < NOW() - INTERVAL 15 MINUTE"
        cur.execute(query)
        registros_borrados = cur.rowcount
        conn.commit()
        if registros_borrados > 0:
            print(f"🧹 MANTENIMIENTO: Se eliminaron {registros_borrados} registros obsoletos (>15 min).")
        cur.close()
    except Exception as e:
        print(f"⚠️ Error en limpieza: {e}")

# ==========================================================
# 🚀 ORQUESTADOR V2.00 - EL CEREBRO CON MEMORIA Y AUTOLIMPIEZA
# ==========================================================

def bucle_operativo():
    print(f"💎 MOTOR MAESTRO V2.00 - ONLINE (Memoria Inteligente + Sync Web)")
    print(f"📡 Escaneando tareas en sys_simbolos_buscados...")
    
    conn = None 

    while True:
        try:
            # 1. GESTIÓN DE CONEXIÓN ÚNICA
            if conn is None or not conn.is_connected():
                if conn: 
                    try: conn.close()
                    except: pass
                conn = mysql.connector.connect(**DB_CONFIG)

            # Usamos un cursor que se asegure de limpiar resultados previos
            cur = conn.cursor(dictionary=True, buffered=True)
            
            # 2. BUSCAR TAREA PENDIENTE
            cur.execute("SELECT id, ticker FROM sys_simbolos_buscados WHERE status = 'pendiente' LIMIT 1")
            tarea = cur.fetchone()

            if tarea:
                id_tarea = tarea['id']
                tk_busqueda = tarea['ticker'].upper().strip()
                
                # --- [CORRECCIÓN CRÍTICA] ---
                # Marcamos como 'procesando' de inmediato. 
                # Si no hacemos esto, el siguiente ciclo del loop volverá a leer esta misma tarea.
                cur.execute("UPDATE sys_simbolos_buscados SET status = 'procesando' WHERE id = %s", (id_tarea,))
                conn.commit() 
                
                print(f"\n🎯 TAREA RECIBIDA: {tk_busqueda} (ID: {id_tarea})")
                
                # --- LÓGICA DE MEMORIA V3.16 ---
                # Buscamos si ya tenemos este ticker guardado de búsquedas de otros usuarios
                query_memoria = "SELECT motor, ticker, precio, info FROM sys_busqueda_resultados WHERE nombre_comun = %s LIMIT 20"
                cur.execute(query_memoria, (tk_busqueda,))
                existentes = cur.fetchall()

                if existentes:
                    print(f"🧠 MEMORIA: {tk_busqueda} encontrada. Sincronizando con Web...")
                    
                    for fila in existentes:
                        sql_insert = """INSERT INTO sys_busqueda_resultados 
                                       (busqueda_id, nombre_comun, motor, ticker, precio, info) 
                                       VALUES (%s, %s, %s, %s, %s, %s)"""
                        cur.execute(sql_insert, (
                            id_tarea, tk_busqueda, fila['motor'], fila['ticker'], fila['precio'], fila['info']
                        ))
                    
                    # Actualizar a encontrado
                    cur.execute("UPDATE sys_simbolos_buscados SET status = 'encontrado' WHERE id = %s", (id_tarea,))
                    conn.commit() 
                    print(f"✅ Web Actualizada vía Memoria.")
                
                else:
                    # --- BÚSQUEDA NUEVA EN MERCADOS ---
                    print(f"🔍 No hay registros para {tk_busqueda}. Interrogando mercados...")
                    
                    consolidado = []
                    # Lista de funciones de búsqueda
                    motores = [
                        ("Binance", mapeo_binance), ("BingX", mapeo_bingx), 
                        ("Yahoo", mapeo_yahoo), ("Finnhub", mapeo_finnhub), ("Alpha", mapeo_alpha)
                    ]

                    for nombre, funcion in motores:
                        print(f"📡 Interrogando {nombre: <12}", end=" ", flush=True)
                        try:
                            res = funcion(tk_busqueda)
                            if res and len(res) > 0:
                                consolidado.extend(res)
                                print("✅")
                            else: print("❌")
                        except Exception as e: 
                            print(f"⚠️ Error: {e}")

                    if consolidado:
                        # Guardar resultados nuevos en la BD
                        guardar_en_resultados_db(conn, consolidado, id_tarea, tk_busqueda)
                        cur.execute("UPDATE sys_simbolos_buscados SET status = 'encontrado' WHERE id = %s", (id_tarea,))
                        print(f"✅ Sincronización exitosa.")
                    else:
                        cur.execute("UPDATE sys_simbolos_buscados SET status = 'error' WHERE id = %s", (id_tarea,))
                
                # --- FINALIZACIÓN ---
                conn.commit()
                print(f"⏳ Tarea finalizada. Esperando nueva orden...")

            else:
                # No hay tareas: Limpieza y espera
                limpiar_resultados_antiguos(conn)
                print(".", end="", flush=True)
                time.sleep(5) # Espera 5 segundos antes de volver a mirar la base de datos

            cur.close()

        except mysql.connector.Error as err:
            print(f"\n❌ Error de Base de Datos: {err}")
            conn = None # Forzar reconexión en el siguiente ciclo
            time.sleep(5)
        except Exception as e:
            print(f"\n⚠️ Error inesperado: {e}")
            time.sleep(5)

# ==========================================================
# 🏁 LANZADOR
# ==========================================================
if __name__ == "__main__":
    bucle_operativo()