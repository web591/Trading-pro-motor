import requests
import time
import pandas as pd
import yfinance as yf
import mysql.connector
from datetime import datetime
from config import FINNHUB_KEY, DB_CONFIG

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
                    encontrados.append({"Motor": mkt, "Ticker": sym, "Precio": precio_crudo, "Info": f"Crypto Pair: {sym}"})
        except: continue
    return encontrados

def mapeo_bingx(busqueda):
    tk_search = busqueda.upper().replace("/", "").replace("-", "").replace("=X", "")
    encontrados = []
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
                if tk_search in sym_orig:
                    precio = i.get('lastPrice') or i.get('price')
                    if precio and float(precio) > 0:
                        encontrados.append({"Motor": nombre_mkt, "Ticker": sym_orig, "Precio": precio, "Info": f"BingX: {sym_orig}"})
        except: continue
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

        hallazgos_limpios = [h for h in hallazgos if len(str(h.get('Ticker', ''))) <= 12]
        if not hallazgos_limpios:
            cur.execute("UPDATE sys_simbolos_buscados SET status = 'no_encontrado' WHERE id = %s", (id_tarea,))
            conn.commit()
            return

        # 2. Insertar resultados (Evitar duplicados en la misma búsqueda)
        for h in hallazgos_limpios:
            try:
                precio_val = float(str(h.get('Precio', '0')).replace(',', ''))
                cur.execute("""
                    INSERT INTO sys_busqueda_resultados (busqueda_id, nombre_comun, motor, ticker, precio, info) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (id_tarea, tk_clean, h['Motor'], h['Ticker'], precio_val, h.get('Info', h['Ticker'])))
            except: continue

        # 3. Traductor y Status Final
        best = hallazgos_limpios[0]
        m_fuente = best['Motor'].lower()
        cat = 'STOCK' if '.' in best['Ticker'] else 'SPOT'

        #cur.execute("""
        #    INSERT IGNORE INTO sys_traductor_simbolos 
        #    (nombre_comun, motor_fuente, ticker_motor, categoria_producto, is_active) 
        #    VALUES (%s, %s, %s, %s, '0')
        #""", (tk_clean, m_fuente, best['Ticker'], cat))
        
        cur.execute("SELECT id FROM sys_traductor_simbolos WHERE ticker_motor = %s AND motor_fuente = %s LIMIT 1", (best['Ticker'], m_fuente))
        res_trad = cur.fetchone()
        trad_id = res_trad['id'] if res_trad else None

        # 🔥 FORZADO A 'validar'
        cur.execute("UPDATE sys_simbolos_buscados SET traductor_id = %s, status = 'validar' WHERE id = %s", (trad_id, id_tarea))
        
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
                        ("Finnhub", mapeo_finnhub)
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