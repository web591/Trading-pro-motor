import mysql.connector
import requests
import yfinance as yf
import time
import config

def get_db_connection():
    return mysql.connector.connect(**config.DB_CONFIG)

def buscar_mapeo_profundo(ticker_raw):
    """
    Intenta encontrar el mejor mapeo para un ticker nuevo.
    """
    clean = ticker_raw.replace("USDT", "").replace("USDC", "").strip().upper()
    
    info = {
        'binance_spot': None,
        'binance_usdt_future': None,
        'binance_coin_future': None,
        'bingx_perp': None,
        'yahoo_sym': None,
        'prioridad': 'yahoo_sym'
    }

    # 1. Intento en Binance (Spot y Futuros)
    try:
        s_spot = f"{clean}USDT"
        r = requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={s_spot}", timeout=3).json()
        if 'price' in r:
            info['binance_spot'] = s_spot
            info['binance_usdt_future'] = s_spot
            info['prioridad'] = 'binance_usdt_future'
        
        # Probar Coin-M si es una crypto mayor
        s_coin = f"{clean}USD_PERP"
        r_c = requests.get(f"https://dapi.binance.com/dapi/v1/ticker/price?symbol={s_coin}", timeout=3).json()
        if isinstance(r_c, list) or 'price' in r_c:
            info['binance_coin_future'] = s_coin
    except: pass

    # 2. Intento en BingX (Perpetuos Acciones/Cripto)
    try:
        s_bingx = f"{clean}-USDT"
        r = requests.get(f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={s_bingx}", timeout=3).json()
        if r.get('code') == 0:
            info['bingx_perp'] = s_bingx
            if not info['binance_spot']: info['prioridad'] = 'bingx_perp'
    except: pass

    # 3. Intento en Yahoo Finance (Forex, Índices, Acciones)
    variaciones = [clean, f"{clean}-USD", f"{clean}=F", f"^{clean}", f"{clean}.MX"]
    for v in variaciones:
        try:
            t = yf.Ticker(v)
            if t.fast_info['last_price'] > 0:
                info['yahoo_sym'] = v
                break
        except: continue
    
    return info

def procesar_pendientes():
    print("🚀 Iniciando escaneo de activos pendientes (is_active = 0)...")
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Buscar activos que se añadieron desde la web pero no tienen datos
    cursor.execute("SELECT id, nombre_comun FROM sys_traductor_simbolos WHERE is_active = 0")
    pendientes = cursor.fetchall()

    if not pendientes:
        print("☕ Todo al día. No hay activos pendientes.")
        cursor.close()
        conn.close()
        return

    for p in pendientes:
        ticker = p['nombre_comun']
        print(f"🛠 Procesando: {ticker}...")
        
        mapeo = buscar_mapeo_profundo(ticker)
        
        # Actualizamos la fila con los datos encontrados y activamos el activo
        sql = """
            UPDATE sys_traductor_simbolos 
            SET binance_spot = %s, 
                binance_usdt_future = %s, 
                binance_coin_future = %s, 
                bingx_perp = %s, 
                yahoo_sym = %s, 
                prioridad_precio = %s, 
                is_active = 1 
            WHERE id = %s
        """
        valores = (
            mapeo['binance_spot'], mapeo['binance_usdt_future'],
            mapeo['binance_coin_future'], mapeo['bingx_perp'],
            mapeo['yahoo_sym'], mapeo['prioridad'], p['id']
        )
        
        cursor.execute(sql, valores)
        conn.commit()
        print(f"   ✅ {ticker} validado y activado. Prioridad: {mapeo['prioridad']}")
        time.sleep(1) # Cortesía para las APIs

    cursor.close()
    conn.close()
    print("🏁 Fin del proceso de validación.")

if __name__ == "__main__":
    procesar_pendientes()