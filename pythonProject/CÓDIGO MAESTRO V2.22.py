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
                    nm_limpio = sym.replace("USDT","").replace("USDC","").replace("BUSD","").replace("USD","")
                    if "_" in nm_limpio: nm_limpio = nm_limpio.split("_")[0]
                    encontrados.append({
                        "Motor": mkt, 
                        "Ticker": sym, 
                        "Nombre": nm_limpio,
                        "Precio": precio_crudo, 
                        "Info": f"Crypto Pair: {sym}",
                        "Tipo": "CRYPTO" if "spot" in mkt else "PERPETUAL"
                    })
        except: continue
    return encontrados

# ==========================================================
# BINGX UNIVERSAL v3.4
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
        encontrados.append({
            "Motor": "bingx_forex",
            "Ticker": sym,
            "Nombre": displayName,
            "Precio": precio,
            "Info": f"FOREX CFD | {displayName}",
            "Tipo": "FOREX"
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
        encontrados.append({
            "Motor": "bingx_commodity",
            "Ticker": sym,
            "Nombre": displayName,
            "Precio": precio,
            "Info": f"COMMODITY CFD | {displayName}"
        })
    return encontrados

def _bingx_stock(tk, contratos, precios_dict):
    encontrados = []
    for c in contratos:
        sym = c.get("symbol","").upper()
        asset = str(c.get("asset","")).upper()
        displayName = c.get("displayName","")
        if not asset.startswith("NCSK"): continue
        underlying = str(c.get("underlying","")).replace("/","").upper()
        if tk not in underlying and tk not in displayName.upper(): continue
        precio = precios_dict.get(sym, {}).get("lastPrice") or 0
        encontrados.append({
            "Motor": "bingx_stock",
            "Ticker": sym,
            "Nombre": displayName,
            "Precio": precio,
            "Info": f"STOCK CFD | {displayName}",
            "Tipo": "CFD"
        })
    return encontrados

def _bingx_index(tk, contratos, precios_dict):
    encontrados = []
    for c in contratos:
        sym = c.get("symbol","").upper()
        asset = str(c.get("asset","")).upper()
        displayName = c.get("displayName","")
        if not asset.startswith("NCSI"): continue
        underlying = str(c.get("underlying","")).replace("/","").upper()
        if tk not in underlying and tk not in displayName.upper(): continue
        precio = precios_dict.get(sym, {}).get("lastPrice") or 0
        encontrados.append({
            "Motor": "bingx_index",
            "Ticker": sym,
            "Nombre": displayName,
            "Precio": precio,
            "Info": f"INDEX CFD | {displayName}",
            "Tipo": "INDEX"
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
            "Info": f"BingX Crypto: {sym}",
            "Tipo": "CRYPTO"
        })
    return encontrados

def mapeo_bingx(busqueda):
    tk = busqueda.upper().replace("/", "").replace("-", "").replace("=X", "")
    encontrados = []
    try:
        contratos = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts", timeout=10).json().get("data", [])
        precios = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker", timeout=10).json().get("data", [])
        precios_dict = {p["symbol"]:p for p in precios}
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
                tipo_map = {
                    "EQUITY": "STOCK",
                    "ETF": "ETF",
                    "INDEX": "INDEX",
                    "MUTUALFUND": "FUND",
                    "CURRENCY": "FOREX",
                    "CRYPTOCURRENCY": "CRYPTO"
                }
                qt = q.get('quoteType', 'UNKNOWN').upper()
                tipo_final = tipo_map.get(qt, "UNKNOWN")
                encontrados.append({
                    "Motor": "yahoo_sym",
                    "Ticker": sym,
                    "Nombre": nombre,
                    "Precio": f"{p:.4f}",
                    "Info": f"[{qt}] {nombre}",
                    "Tipo": tipo_final
                })
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
                encontrados.append({
                    "Motor": "finnhub_sym",
                    "Ticker": sym,
                    "Nombre": i.get('description', sym),
                    "Precio": q['c'],
                    "Info": i.get('description', sym),
                    "Tipo": "STOCK"
                })
    except: pass
    return encontrados

def mapeo_alpha(busqueda):
    encontrados = []
    try:
        url = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={busqueda}&apikey={ALPHA_VANTAGE_KEY}"
        r = requests.get(url, timeout=10).json()
        for match in r.get('bestMatches', [])[:3]:
            sym_raw = match.get('1. symbol')
            if not sym_raw: continue
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
                    "Info": match.get('2. name', sym_clean),
                    "Tipo": "STOCK"
                })
            time.sleep(1.2)
    except Exception as e:
        print("❌ Alpha Error:", e)
    return encontrados

def inferir_tipo_desde_info(info):
    if not info: return "UNKNOWN"
    txt = info.upper()
    if "[ETF]" in txt: return "ETF"
    elif "[EQUITY]" in txt: return "STOCK"
    elif "[OPTION]" in txt: return "OPTION"
    elif "[INDEX]" in txt: return "INDEX"
    elif "FUND" in txt: return "FUND"
    elif "REIT" in txt: return "REIT"
    elif "ADR" in txt: return "ADR"
    return "UNKNOWN"

# ==========================================================
# 💾 PERSISTENCIA
# ==========================================================
def guardar_en_resultados_db(conn, hallazgos, id_tarea, busqueda_original, underlying_forzado):
    cur = conn.cursor(dictionary=True)
    try:
        tk_clean = busqueda_original.upper().strip()
        if tk_clean in ['USDT','USDC','DAI','BUSD','USD']:
            cur.execute("UPDATE sys_simbolos_buscados SET status = 'ignorado' WHERE id = %s", (id_tarea,))
            conn.commit()
            return
        hallazgos_limpios = [h for h in hallazgos if len(str(h.get('Ticker', ''))) <= 25]
        if not hallazgos_limpios:
            cur.execute("UPDATE sys_simbolos_buscados SET status = 'no_encontrado' WHERE id = %s", (id_tarea,))
            conn.commit()
            return
        for h in hallazgos_limpios:
            try:
                precio_val = float(str(h.get('Precio', '0')).replace(',', ''))
                nombre_final = h.get('Nombre', tk_clean)
                underlying_real = underlying_forzado
                segmento = "SPOT"
                motor_l = h.get('Motor', '').lower()
                if "future" in motor_l or "perp" in motor_l: segmento = "FUTURES"
                elif "bingx_stock" in motor_l or "bingx_forex" in motor_l or "bingx_index" in motor_l: segmento = "CFD"
                tipo_final = h.get("Tipo", "UNKNOWN")
                if tipo_final == "UNKNOWN": tipo_final = inferir_tipo_desde_info(h.get("Info"))
                cur.execute("""
                    INSERT INTO sys_busqueda_resultados 
                    (busqueda_id, nombre_comun, underlying, motor, ticker, precio, info, tipo_investment, market_segment)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (id_tarea, nombre_final, underlying_real, h['Motor'], h['Ticker'], precio_val, h.get('Info', h['Ticker']), tipo_final, segmento))
            except: continue
        best = hallazgos_limpios[0]
        m_fuente = best['Motor'].lower()
        nombre_para_vincular = best.get('Nombre', tk_clean)
        cat = 'SPOT'
        if 'yahoo' in m_fuente: cat = 'STOCK'
        elif 'future' in m_fuente or 'perp' in m_fuente: cat = 'PERPETUAL'
        cur.execute("SELECT id FROM sys_traductor_simbolos WHERE ticker_motor = %s AND motor_fuente = %s LIMIT 1", (best['Ticker'], m_fuente))
        res_trad = cur.fetchone()
        trad_id = res_trad['id'] if res_trad else None
        cur.execute("UPDATE sys_simbolos_buscados SET traductor_id = %s, status = 'validar' WHERE id = %s", (trad_id, id_tarea))
        conn.commit()
        print(f"✅ Tarea {id_tarea} finalizada -> status: validar")
    except Exception as e:
        print(f"⚠️ Error en Guardado: {e}")
        conn.rollback()
    finally:
        cur.close()

# ==========================================================
# 🧹 LIMPIEZA
# ==========================================================
def limpiar_resultados_antiguos(conn):
    try:
        cur = conn.cursor()
        query = "DELETE FROM sys_busqueda_resultados WHERE fecha_hallazgo < NOW() - INTERVAL 24 HOUR"
        cur.execute(query)
        registros_borrados = cur.rowcount
        conn.commit()
        if registros_borrados > 0:
            print(f"🧹 MANTENIMIENTO: Se eliminaron {registros_borrados} registros obsoletos.")
        cur.close()
    except Exception as e:
        print(f"⚠️ Error en limpieza automática: {e}")

def validar_tarea_existente(conn, id_tarea, underlying_consulta, tipo_investment):
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id FROM sys_traductor_simbolos WHERE underlying = %s AND tipo_instrumento = %s LIMIT 1", (underlying_consulta, tipo_investment))
        res = cur.fetchone()
        trad_id = res['id'] if res else None
        cur.execute("UPDATE sys_simbolos_buscados SET traductor_id = %s, status = 'validar' WHERE id = %s", (trad_id, id_tarea))
        conn.commit()
        print(f"🧠 CACHE SAFE -> {underlying_consulta} ({tipo_investment}) validado sin conflicto")
    except Exception as e:
        print(f"⚠️ Error en validar_tarea_existente: {e}")
        conn.rollback()
    finally:
        cur.close()

# ==========================================================
# 🚀 ORQUESTADOR V2.21 - MULTIUSUARIO ONLINE + CACHE + TRAD_ID CONSISTENTE
# ==========================================================
def bucle_operativo():
    print("💎 MOTOR MAESTRO V2.21 - MULTIUSUARIO ONLINE (MEMORY + TRAD_ID ENABLED)")
    conn = None
    while True:
        try:
            # Conexion a DB
            if conn is None or not conn.is_connected():
                conn = mysql.connector.connect(**DB_CONFIG)

            if int(time.time()) % 300 == 0:
                limpiar_resultados_antiguos(conn)   

            cur = conn.cursor(dictionary=True, buffered=True)
            cur.execute("SELECT id, ticker FROM sys_simbolos_buscados WHERE status IN ('pendiente','encontrado') LIMIT 1")
            tarea = cur.fetchone()
            cur.close()

            if tarea:
                id_tarea = tarea['id']
                tk_busqueda = tarea['ticker'].upper().strip()

                # Marcar como procesando
                cur_upd = conn.cursor()
                cur_upd.execute("UPDATE sys_simbolos_buscados SET status = 'procesando' WHERE id = %s", (id_tarea,))
                conn.commit()
                cur_upd.close()
                print(f"\n🎯 PROCESANDO: {tk_busqueda} (ID: {id_tarea})")

                # ==========================================================
                # 🔹 DETERMINAR UNDERLYING Y TRADUCTOR_ID - mejora 2.16
                # ==========================================================
                cur_u = conn.cursor(dictionary=True)

                # Buscar directamente por underlying primero
                cur_u.execute("""
                    SELECT id, underlying 
                    FROM sys_traductor_simbolos 
                    WHERE underlying = %s 
                    LIMIT 1
                """, (tk_busqueda,))

                row = cur_u.fetchone()

                if row:
                    underlying_consulta = row["underlying"].upper()
                    trad_id = row["id"]
                else:
                    underlying_consulta = tk_busqueda.upper()
                    trad_id = None

                cur_u.close()


                # ==========================================================
                # 🔹 MEMORIA GLOBAL
                # ==========================================================
                cur_cache = conn.cursor()
                cur_cache.execute(
                    "SELECT 1 FROM sys_busqueda_resultados WHERE underlying = %s LIMIT 1",
                    (underlying_consulta,)
                )
                cache_hit = cur_cache.fetchone()
                cur_cache.close()

                if cache_hit:
                    print(f"🧠 MEMORIA GLOBAL: {underlying_consulta} ya descubierto. Reutilizando catálogo...")
                    cur_mem = conn.cursor(dictionary=True)
                    cur_mem.execute("SELECT motor, ticker, precio, info, nombre_comun, tipo_investment FROM sys_busqueda_resultados WHERE underlying = %s AND fecha_hallazgo >= NOW() - INTERVAL 24 HOUR LIMIT 20", (underlying_consulta,))
                    existentes = cur_mem.fetchall()
                    cur_mem.close()
                    hallazgos_mem = [{"Motor": f['motor'], "Ticker": f['ticker'], "Nombre": f['nombre_comun'], "Precio": f['precio'], "Info": f['info'], "Tipo": f['tipo_investment']}   for f in existentes]
                    # 🔹 Guardado usando memoria + actualización traductor_id aunque no haya hallazgos
                    guardar_en_resultados_db(conn, hallazgos_mem, id_tarea, tk_busqueda, underlying_consulta)
                    # Si no hay hallazgos_mem, aun así se actualiza traductor_id
                    if not hallazgos_mem:
                        cur_tid = conn.cursor()
                        cur_tid.execute("UPDATE sys_simbolos_buscados SET traductor_id = %s, status='validar' WHERE id = %s", (trad_id, id_tarea))
                        conn.commit()
                        cur_tid.close()
                        print(f"🧠 Traductor ID actualizado a {trad_id} (sin hallazgos)")
                else:
                    print(f"🔍 Interrogando mercados para {tk_busqueda}...")
                    consolidado = []
                    motores = [("Binance", mapeo_binance), ("BingX", mapeo_bingx), ("Yahoo", mapeo_yahoo), ("Finnhub", mapeo_finnhub), ("AlphaVantage", mapeo_alpha)]
                    for nombre, funcion in motores:
                        try:
                            res = funcion(tk_busqueda)
                            if res: consolidado.extend(res)
                        except: continue
                    # 🔹 Guardado final, maneja sin hallazgos y actualiza traductor_id siempre
                    guardar_en_resultados_db(conn, consolidado, id_tarea, tk_busqueda, underlying_consulta)
                    if not consolidado:
                        cur_tid2 = conn.cursor()
                        cur_tid2.execute("UPDATE sys_simbolos_buscados SET traductor_id = %s, status='validar' WHERE id = %s", (trad_id, id_tarea))
                        conn.commit()
                        cur_tid2.close()
                        print(f"🧠 Traductor ID actualizado a {trad_id} (sin hallazgos)")

            else:
                # No hay tareas pendientes
                print(".", end="", flush=True)
                time.sleep(10)
        except Exception as e:
            print(f"⚠️ Error Crítico en Bucle: {e}")
            time.sleep(5)

if __name__ == "__main__":
    bucle_operativo()
