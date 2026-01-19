import mysql.connector
import time
import requests
import yfinance as yf
import config

def get_db_connection():
    return mysql.connector.connect(**config.DB_CONFIG)

def procesar_busqueda_exhaustiva(ticker):
    """Busca en todas las fuentes sin detenerse para llenar la tabla"""
    res = {
        'binance_spot': None, 'binance_usdt_future': None,
        'bingx_perp': None, 'yahoo_sym': None, 
        'finnhub_sym': None, 'prioridad': 'yahoo_sym', 'price': None
    }

    # --- 1. BINANCE (Spot y Futuros) ---
    b_sym = f"{ticker}USDT"
    try:
        r = requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={b_sym}", timeout=3)
        if r.status_code == 200:
            res['binance_spot'] = b_sym
            res['binance_usdt_future'] = b_sym # Usamos el mismo para fapi
            res['price'] = float(r.json()['price'])
            res['prioridad'] = 'binance_usdt_future'
    except: pass

    # --- 2. BINGX (Perpetuos) ---
    bx_sym = f"{ticker}-USDT"
    try:
        r = requests.get(f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={bx_sym}", timeout=3)
        if r.status_code == 200:
            res['bingx_perp'] = bx_sym
            if not res['price']: # Si Binance falló, usamos este precio
                res['price'] = float(r.json()['data']['lastPrice'])
                res['prioridad'] = 'bingx_perp'
    except: pass

    # --- 3. YAHOO FINANCE (Varios sufijos) ---
    # Probamos: Acciones (Ticker), Forex (=X), Commodities (=F), Cripto (-USD)
    variantes = [ticker, f"{ticker}=X", f"{ticker}=F", f"{ticker}-USD"]
    for v in variantes:
        try:
            tk = yf.Ticker(v)
            hist = tk.history(period="1d")
            if not hist.empty:
                res['yahoo_sym'] = v
                if not res['price']: # Si no es cripto, Yahoo da el precio
                    res['price'] = hist['Close'].iloc[-1]
                    res['prioridad'] = 'yahoo_sym'
                break # Si encontramos una variante válida en Yahoo, paramos Yahoo
        except: continue

    # --- 4. FINNHUB (Acciones USA) ---
    try:
        r = requests.get(f"https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={config.FINNHUB_TOKEN}", timeout=3)
        data = r.json()
        if r.status_code == 200 and data and 'name' in data:
            res['finnhub_sym'] = ticker
    except: pass

    return res

def ejecutar_motor():
    print("🛰️  Radar de Activos Iniciado... (Conexión Única)")
    while True:
        conn = None
        try:
            # Abrimos la conexión una sola vez al inicio del ciclo
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)
            
            cur.execute("SELECT id, ticker FROM sys_simbolos_buscados WHERE status = 'pendiente' LIMIT 1")
            row = cur.fetchone()

            if row:
                t_id = row['id']
                ticker = row['ticker']
                print(f"🔎 Analizando: {ticker}...")
                
                cur.execute("UPDATE sys_simbolos_buscados SET status = 'buscando' WHERE id = %s", (t_id,))
                conn.commit()

                datos = procesar_busqueda_exhaustiva(ticker)

                if datos['binance_spot'] or datos['yahoo_sym'] or datos['bingx_perp']:
                    sql = """UPDATE sys_simbolos_buscados SET 
                             binance_spot=%s, binance_usdt_future=%s, 
                             bingx_perp=%s, finnhub_sym=%s, yahoo_sym=%s, 
                             prioridad_precio=%s, precio_referencia=%s, status='encontrado'
                             WHERE id=%s"""
                    cur.execute(sql, (datos['binance_spot'], datos['binance_usdt_future'], 
                                      datos['bingx_perp'], datos['finnhub_sym'], datos['yahoo_sym'], 
                                      datos['prioridad'], datos['price'], t_id))
                else:
                    cur.execute("UPDATE sys_simbolos_buscados SET status = 'error', mensaje_error = 'No encontrado' WHERE id = %s", (t_id,))
                
                conn.commit()
            
            cur.close()
            conn.close() # Cerramos después de procesar el ticker
        except Exception as e:
            print(f"⚠️ Error: {e}")
            if conn: conn.close()
        
        time.sleep(5) # Esperamos 5 segundos antes de volver a conectar

if __name__ == "__main__":
    iniciar_motor()