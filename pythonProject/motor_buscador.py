import mysql.connector
import time
import requests
import yfinance as yf
import config

def get_db_connection():
    return mysql.connector.connect(**config.DB_CONFIG)

def procesar_busqueda_exhaustiva(ticker):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    """Busca en todas las fuentes sin detenerse para llenar la tabla"""
    res = {
        'binance_spot': None, 'binance_usdt_future': None,
        'bingx_perp': None, 'yahoo_sym': None, 
        'finnhub_sym': None, 'prioridad': 'yahoo_sym', 'price': None
    }

    # --- 1. BINANCE (Spot y Futuros) ---
    b_sym = f"{ticker}USDT"
    try:
        r = requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={b_sym}", headers=headers, timeout=3)
        if r.status_code == 200:
            res['binance_spot'] = b_sym
            res['binance_usdt_future'] = b_sym # Usamos el mismo para fapi
            res['price'] = float(r.json()['price'])
            res['prioridad'] = 'binance_usdt_future'
    except: pass

    # --- 2. BINGX (Perpetuos) ---
    bx_sym = f"{ticker}-USDT"
    try:
        r = requests.get(f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={bx_sym}", headers=headers, timeout=3)
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
    print("🛰️ Radar de Activos Iniciado... (Modo Ultra-Ahorro: 60s)")
    
    # Configuramos el disfraz de Mozilla para las búsquedas externas
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    while True:
        conn = None
        try:
            # 1. Abrimos conexión única para esta pregunta
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)
            
            # Buscamos si hay algo pendiente
            cur.execute("SELECT id, ticker FROM sys_simbolos_buscados WHERE status = 'pendiente' LIMIT 1")
            item = cur.fetchone()

            if item:
                ticker = item['ticker']
                t_id = item['id']
                
                # Marcamos que estamos trabajando en ello
                cur.execute("UPDATE sys_simbolos_buscados SET status = 'buscando' WHERE id = %s", (t_id,))
                conn.commit()
                
                print(f"🔍 Analizando activo: {ticker}...")
                
                # Realizamos la búsqueda exhaustiva (Asegúrate que esta función use los headers)
                data = procesar_busqueda_exhaustiva(ticker)

                # Guardamos los resultados
                cur.execute("""
                    UPDATE sys_simbolos_buscados SET 
                    status = 'encontrado', binance_spot = %s, binance_usdt_future = %s,
                    bingx_perp = %s, yahoo_sym = %s, finnhub_sym = %s,
                    prioridad_precio = %s, precio_referencia = %s
                    WHERE id = %s
                """, (data['binance_spot'], data['binance_usdt_future'], data['bingx_perp'], 
                      data['yahoo_sym'], data['finnhub_sym'], data['prioridad'], 
                      data['price'], t_id))
                conn.commit()
                
                print(f"✅ ¡Hecho! {ticker} procesado correctamente.")
                espera_final = 5  # Si encontró uno, esperamos poco por si hay más en cola
            else:
                # Si NO hay nada, cerramos y esperamos 1 minuto
                espera_final = 60 

            cur.close()
            conn.close() # Cerramos la puerta de la DB
            
        except Exception as e:
            print(f"⚠️ Error en motor buscador: {e}")
            if conn: conn.close()
            espera_final = 30 # Si hubo error, esperamos 30s para reintentar

        # El motor "duerme" para no saturar Hostinger
        time.sleep(espera_final)

if __name__ == "__main__":
    print("🔍 Motor Buscador activo y esperando tareas...")
    while True:
        try:
            # Aquí llamamos a la función que revisa la tabla sys_simbolos_buscados
            # Según tu código anterior, la función se llama 'ejecutar_motor'
            ejecutar_motor() 
            
            # El sleep de 60s ya está dentro de ejecutar_motor, 
            # pero ponemos este por si la función falla y sale.
            time.sleep(10) 
        except Exception as e:
            print(f"⚠️ Reintentando motor por error: {e}")
            time.sleep(10)