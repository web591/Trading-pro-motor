import mysql.connector
import requests
import yfinance as yf
import time
import config

def buscar_mapeo_profundo(ticker_raw):
    ticker_raw = ticker_raw.strip().upper()
    
    # CASO ESPECIAL: BTCUSDC o similares
    es_par_estable = ticker_raw.endswith('USDC')
    if es_par_estable:
        clean = ticker_raw
    else:
        clean = ticker_raw.replace("USDT", "").replace("USDC", "")

    info = {
        'binance_spot': None,
        'binance_usdt_future': None,
        'bingx_perp': None,
        'yahoo_sym': None,
        'prioridad': 'yahoo_sym'
    }

    # 1. CASO AT&T y Acciones de 1 letra: Forzar Yahoo primero para evitar conflictos con Cripto
    if len(clean) <= 2 and not es_par_estable:
        print(f"   ⚠️ Detectada acción/índice corto: {clean}. Priorizando Yahoo.")
        try:
            t = yf.Ticker(clean)
            if t.fast_info['last_price'] > 0:
                info['yahoo_sym'] = clean
                info['prioridad'] = 'yahoo_sym'
                return info
        except: pass

    # 2. Búsqueda en Binance (Solo si no es una acción protegida)
    try:
        s_binance = clean if es_par_estable else f"{clean}USDT"
        r = requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={s_binance}", timeout=3).json()
        if 'price' in r:
            info['binance_spot'] = s_binance
            info['prioridad'] = 'binance_spot'
    except: pass

    # 3. Yahoo Finance para Commodities e Índices
    mapeos_fijos = {
        'GOLD': 'GC=F', 'SILVER': 'SI=F', 'WTI': 'CL=F', 
        'DAX': '^GDAXI', 'SP500': '^GSPC', 'EURUSD': 'EURUSD=X'
    }
    
    if clean in mapeos_fijos:
        info['yahoo_sym'] = mapeos_fijos[clean]
        info['prioridad'] = 'yahoo_sym'
    else:
        # Búsqueda genérica en Yahoo si no se encontró en Binance
        if not info['binance_spot']:
            try:
                t = yf.Ticker(clean)
                if t.fast_info['last_price'] > 0:
                    info['yahoo_sym'] = clean
            except: pass

    return info

def procesar_pendientes():
    conn = mysql.connector.connect(**config.DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, nombre_comun FROM sys_traductor_simbolos WHERE is_active = 0")
    for p in cursor.fetchall():
        m = buscar_mapeo_profundo(p['nombre_comun'])
        cursor.execute("""
            UPDATE sys_traductor_simbolos SET 
            binance_spot=%s, binance_usdt_future=%s, yahoo_sym=%s, prioridad_precio=%s, is_active=1 
            WHERE id=%s""", (m['binance_spot'], m['binance_usdt_future'], m['yahoo_sym'], m['prioridad'], p['id']))
        conn.commit()
        print(f"✅ {p['nombre_comun']} activado vía {m['prioridad']}")
    conn.close()

if __name__ == "__main__":
    procesar_pendientes()