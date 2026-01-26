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
# 3️⃣ YAHOO: DISCOVERY mapeo_yahoo_v2.0
def mapeo_yahoo(busqueda):
    encontrados = []
    # Lista de endpoints para redundancia
    urls = [
        f"https://query2.finance.yahoo.com/v1/finance/search?q={busqueda}",
        f"https://query1.finance.yahoo.com/v1/finance/search?q={busqueda}"
    ]
    
    headers = get_headers()
    
    # --- 1. ESPERA INICIAL ---
    # Un pequeño respiro antes de empezar para que no parezca ráfaga
    time.sleep(random.uniform(1.0, 2.0))
    
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=5).json()
            quotes = r.get('quotes', [])
            if not quotes: continue 
            
            # Procesamos los resultados (Limitamos a 7)
            for q in quotes[:7]:
                sym = q['symbol']
                try:
                    # --- 2. ESPERA POR CADA TICKER (CRÍTICO) ---
                    # Esto evita que Yahoo vea 7 peticiones en el mismo milisegundo
                    time.sleep(random.uniform(0.5, 1.2)) 
                    
                    t = yf.Ticker(sym)
                    p = t.fast_info['last_price']
                    
                    tipo = q.get('quoteType', 'N/A')
                    exchange = q.get('exchDisp', 'Global')
                    nombre = q.get('shortname', q.get('longname', 'Asset'))
                    
                    encontrados.append({
                        "Motor": "YAHOO",
                        "Ticker": sym,
                        "Precio": f"{p:.4f}" if p else "N/A", # Usamos 4 decimales como acordamos
                        "Info": f"[{tipo}] {nombre} ({exchange})"
                    })
                except: 
                    continue
            
            if encontrados: break 
            
        except Exception as e:
            # Si hay error, esperamos un poco más antes de intentar con la siguiente URL
            time.sleep(2)
            print(f"    ⚠️ Reintentando Yahoo por bloqueo en endpoint...")
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

# 5️⃣ ALPHA VANTAGE mapeo_alpha_vantage_v2.3
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

# ==========================================================
# 💾 PERSISTENCIA EN sys_busqueda_resultados (CORREGIDO)
# ==========================================================

def guardar_en_resultados_db(conn, hallazgos, id_tarea, busqueda_original):
    """
    Inserta o ACTUALIZA los hallazgos. 
    Maneja limpieza de decimales y evita duplicados en la misma ventana de tiempo.
    """
    try:
        cur = conn.cursor()
        
        # Usamos ON DUPLICATE KEY UPDATE para que si el motor y el ticker ya existen
        # para esa búsqueda, simplemente actualice el precio y la info.
        query = """INSERT INTO sys_busqueda_resultados 
                   (busqueda_id, nombre_comun, motor, ticker, precio, info) 
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE 
                   precio = VALUES(precio),
                   info = VALUES(info),
                   busqueda_id = VALUES(busqueda_id),
                   fecha_hallazgo = CURRENT_TIMESTAMP"""
        
        for h in hallazgos:
            # --- LIMPIEZA DE DECIMALES ---
            try:
                # Quitamos comas y nos aseguramos de que sea un float puro
                p_str = str(h['Precio']).replace(',', '').strip()
                precio_clean = float(p_str)
            except (ValueError, TypeError):
                precio_clean = 0.00000000 # Mantener precisión de 8 decimales

            # Ejecución
            cur.execute(query, (
                id_tarea, 
                busqueda_original.upper(), 
                h['Motor'], 
                h['Ticker'], 
                precio_clean, 
                h['Info']
            ))
            
        conn.commit()
        cur.close()
        print(f"   ✅ DB: {len(hallazgos)} resultados procesados (Modo: Inteligente)")
        
    except Exception as e:
        print(f"    ⚠️ Error al insertar en sys_busqueda_resultados: {e}")
        
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
# 🚀 ORQUESTADOR V1.99 - EL CEREBRO CON MEMORIA Y AUTOLIMPIEZA
# ==========================================================

def bucle_operativo():
    print(f"💎 MOTOR MAESTRO V1.99 - ONLINE (Memoria Inteligente + Sync Web)")
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

            cur = conn.cursor(dictionary=True)
            
            # 2. BUSCAR TAREA PENDIENTE
            cur.execute("SELECT id, ticker FROM sys_simbolos_buscados WHERE status = 'pendiente' LIMIT 1")
            tarea = cur.fetchone()

            if tarea:
                id_tarea = tarea['id']
                tk_busqueda = tarea['ticker'].upper().strip()
                
                print(f"\n🎯 TAREA RECIBIDA: {tk_busqueda} (ID: {id_tarea})")
                
                # --- LÓGICA DE MEMORIA V3.16 ---
                query_memoria = "SELECT motor, ticker, precio, info FROM sys_busqueda_resultados WHERE nombre_comun = %s LIMIT 20"
                cur.execute(query_memoria, (tk_busqueda,))
                existentes = cur.fetchall()

                if existentes:
                    print(f"🧠 MEMORIA: {tk_busqueda} encontrada. Sincronizando con Web (ID: {id_tarea})...")
                    
                    for fila in existentes:
                        sql_insert = """INSERT INTO sys_busqueda_resultados 
                                       (busqueda_id, nombre_comun, motor, ticker, precio, info) 
                                       VALUES (%s, %s, %s, %s, %s, %s)"""
                        cur.execute(sql_insert, (
                            id_tarea, tk_busqueda, fila['motor'], fila['ticker'], fila['precio'], fila['info']
                        ))
                    
                    print("\n" + "═"*110)
                    df_p = pd.DataFrame(existentes)
                    df_p.columns = [c.upper() for c in df_p.columns]
                    df_p['PRECIO'] = df_p['PRECIO'].apply(lambda x: f"{float(x):.4f}")
                    print(df_p.to_string(index=False))
                    print("═"*110)
                    
                    cur.execute("UPDATE sys_simbolos_buscados SET status = 'encontrado' WHERE id = %s", (id_tarea,))
                    conn.commit() 
                    print(f"✅ Web Actualizada vía Memoria.")
                
                else:
                    # --- BÚSQUEDA FRESKA ---
                    print(f"🔍 No hay registros para {tk_busqueda}. Interrogando mercados...")
                    print("-" * 110)
                    
                    cur.execute("UPDATE sys_simbolos_buscados SET status = 'buscando' WHERE id = %s", (id_tarea,))
                    conn.commit()

                    consolidado = []
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
                        except: print("⚠️")

                    if consolidado:
                        print("\n" + "═"*110)
                        df = pd.DataFrame(consolidado)
                        df['Precio'] = df['Precio'].apply(lambda x: f"{float(str(x).replace(',', '')):.4f}")
                        print(df[["Motor", "Ticker", "Precio", "Info"]].to_string(index=False))
                        print("═"*110)
                        
                        guardar_en_resultados_db(conn, consolidado, id_tarea, tk_busqueda)
                        cur.execute("UPDATE sys_simbolos_buscados SET status = 'encontrado' WHERE id = %s", (id_tarea,))
                        print(f"✅ Sincronización exitosa (Nuevos datos).")
                    else:
                        cur.execute("UPDATE sys_simbolos_buscados SET status = 'error' WHERE id = %s", (id_tarea,))
                
                # --- FIN DE TAREA: LIMPIEZA POST-OPERATIVA ---
                conn.commit()
                limpiar_resultados_antiguos(conn) # <--- Aquí limpia después de cada ticker
                
                print(f"⏳ Esperando 10 segundos...")
                time.sleep(10)

            else:
                # --- MODO REPOSO: LIMPIEZA PERIÓDICA ---
                limpiar_resultados_antiguos(conn) # <--- Aquí limpia mientras espera tareas
                print(".", end="", flush=True)
                time.sleep(60) 

            cur.close()

        except mysql.connector.Error as err:
            if err.errno == 1226:
                print("\n⚠️ Límite Hostinger. Pausa 5 min...")
                time.sleep(300)
            else:
                conn = None
                time.sleep(10)
        except Exception as e:
            print(f"\n⚠️ Error crítico: {e}")
            time.sleep(10)

# ==========================================================
# 🏁 LANZADOR
# ==========================================================
if __name__ == "__main__":
    bucle_operativo()