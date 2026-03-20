# Version 1.1

import mysql.connector
import requests
import yfinance as yf
import time
from config import DB_CONFIG, ALPHA_VANTAGE_KEY

def conectar_db():
    return mysql.connector.connect(**DB_CONFIG)

def motor_actualizacion_activos():
    try:
        conn = conectar_db()
        cursor = conn.cursor(dictionary=True)
        print(">>> Conexión exitosa a la base de datos.")
    except Exception as e:
        print(f"!!! ERROR DE CONEXIÓN: {e}")
        return

    # 1. PASO YAHOO
    print("\n--- INICIANDO BARRIDO YAHOO FINANCE ---")
    cursor.execute("SELECT underlying, ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente = 'yahoo_sym'")
    activos_yahoo = cursor.fetchall()
    total_yahoo = len(activos_yahoo)
    print(f"Se encontraron {total_yahoo} activos para actualizar vía Yahoo.")

    cont_y = 0
    for activo in activos_yahoo:
        cont_y += 1
        symbol_real = activo['underlying']
        ticker_yahoo = activo['ticker_motor']
        
        print(f"[{cont_y}/{total_yahoo}] Actualizando {symbol_real} ({ticker_yahoo})...", end="\r")
        
        try:
            ticker = yf.Ticker(ticker_yahoo)
            info = ticker.info
            
            sql = """
                INSERT INTO sys_info_activos (symbol, nombre_comercial, market_cap, last_update, source_info)
                VALUES (%s, %s, %s, NOW(), 'yahoo_sym')
                ON DUPLICATE KEY UPDATE 
                nombre_comercial = VALUES(nombre_comercial),
                market_cap = VALUES(market_cap),
                last_update = NOW()
            """
            cursor.execute(sql, (symbol_real, info.get('longName'), info.get('marketCap')))
            conn.commit()
            time.sleep(0.5) 
            
        except Exception as e:
            print(f"\n! Error en Yahoo para {symbol_real}: {e}")
            time.sleep(2)

    print(f"\n>>> Fin del barrido Yahoo. {cont_y} procesados.")

    # 2. PASO ALPHA VANTAGE
    print("\n--- INICIANDO FUNDAMENTALES ALPHA VANTAGE ---")
    query_alpha = """
        SELECT t.underlying, t.ticker_motor 
        FROM sys_traductor_simbolos t
        JOIN sys_info_activos i ON t.underlying = i.symbol
        WHERE t.motor_fuente = 'alpha_sym'
        AND (i.alpha_fail = 0 OR i.alpha_fail IS NULL)
        ORDER BY 
            (i.sector IS NULL) DESC, 
            i.last_update ASC 
        LIMIT 1
    """
    cursor.execute(query_alpha)
    target = cursor.fetchone()

    if target:
        symbol_real = target['underlying']
        ticker_alpha = target['ticker_motor']
        print(f"Seleccionado para Alpha Vantage: {symbol_real} (Ticker: {ticker_alpha})")
        
        try:
            url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker_alpha}&apikey={ALPHA_VANTAGE_KEY}'
            print(f"Llamando a la API de Alpha Vantage...")
            r = requests.get(url).json()

            if r and "Symbol" in r:
                sql_update = """
                    UPDATE sys_info_activos SET 
                    sector = %s, industry = %s, description = %s, 
                    pe_ratio = %s, dividend_yield = %s, ebitda = %s,
                    book_value = %s, profit_margin = %s, analyst_target_price = %s,
                    dividendo_fecha = %s, source_info = 'alpha_vantage', last_update = NOW()
                    WHERE symbol = %s
                """
                valores = (
                    r.get("Sector"), r.get("Industry"), r.get("Description"),
                    r.get("PERatio") if r.get("PERatio") != 'None' else None,
                    r.get("DividendYield") if r.get("DividendYield") != 'None' else None,
                    r.get("EBITDA") if r.get("EBITDA") != 'None' else None,
                    r.get("BookValue") if r.get("BookValue") != 'None' else None,
                    r.get("ProfitMargin") if r.get("ProfitMargin") != 'None' else None,
                    r.get("AnalystTargetPrice") if r.get("AnalystTargetPrice") != 'None' else None,
                    r.get("DividendDate") if r.get("DividendDate") != 'None' else None,
                    symbol_real
                )
                cursor.execute(sql_update, valores)
                conn.commit()
                print(f"√ ÉXITO: {symbol_real} actualizado con fundamentales profundos.")
            else:
                print(f"x AVISO: Alpha Vantage no devolvió datos para {ticker_alpha}. Posible límite de API o ticker no encontrado.")
                
                cursor.execute(
                    "UPDATE sys_info_activos SET alpha_fail = 1 WHERE symbol = %s",
                    (symbol_real,)
                )
                conn.commit()


                # 🔥 MARCAR COMO FALLIDO
                cursor.execute(
                    "UPDATE sys_info_activos SET alpha_fail = 1 WHERE symbol = %s",
                    (symbol_real,)
                )
                conn.commit()

                if "Note" in r:
                    print(f"Mensaje de API: {r['Note']}")

        except Exception as e:
            print(f"!!! Error en Alpha Vantage: {e}")
    else:
        print("No hay candidatos para actualización en Alpha Vantage (revisa el traductor).")

    print("\n--- PROCESO COMPLETADO ---")
    cursor.close()
    conn.close()


# ==========================================================
# BUCLE HORARIO
# ==========================================================
if __name__ == "__main__":
    
    while True:

        print("\n================================================")
        print("INICIANDO CICLO DEL MOTOR DE ACTIVOS")
        print("================================================")

        motor_actualizacion_activos()

        print("\nCiclo terminado. Esperando 1 hora...")
        
        time.sleep(360)