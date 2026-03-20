# Version 1.3

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

    # =========================
    # 1. YAHOO (NO TOCAR)
    # =========================
    print("\n--- INICIANDO BARRIDO YAHOO FINANCE ---")
    cursor.execute("SELECT underlying, ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente = 'yahoo_sym'")
    activos_yahoo = cursor.fetchall()

    for i, activo in enumerate(activos_yahoo, 1):
        symbol_real = activo['underlying']
        ticker_yahoo = activo['ticker_motor']

        print(f"[{i}/{len(activos_yahoo)}] {symbol_real}", end="\r")

        try:
            ticker = yf.Ticker(ticker_yahoo)
            info = ticker.info

            cursor.execute("""
                INSERT INTO sys_info_activos (symbol, nombre_comercial, market_cap, last_update, source_info)
                VALUES (%s, %s, %s, NOW(), 'yahoo_sym')
                ON DUPLICATE KEY UPDATE 
                nombre_comercial = VALUES(nombre_comercial),
                market_cap = VALUES(market_cap),
                last_update = NOW()
            """, (symbol_real, info.get('longName'), info.get('marketCap')))

            conn.commit()
            time.sleep(0.5)

        except Exception as e:
            print(f"\nError Yahoo {symbol_real}: {e}")
            time.sleep(2)

    print("\n>>> Yahoo completo.")

    cursor.close()
    conn.close()


# =========================
# ALPHA INTELIGENTE
# =========================

def obtener_candidato_alpha(cursor):
    cursor.execute("""
        SELECT 
            i.symbol,
            COALESCE(t.ticker_motor, i.symbol) AS ticker_alpha
        FROM sys_info_activos i
        LEFT JOIN sys_traductor_simbolos t 
            ON i.symbol = t.underlying 
            AND t.motor_fuente = 'alpha_sym'
        WHERE 
            (i.alpha_fail IS NULL OR i.alpha_fail = 0)
            AND i.symbol NOT LIKE '%USDT%'
            AND i.symbol NOT LIKE '%USD%'
        ORDER BY 
            (i.sector IS NULL) DESC,
            i.alpha_intentos ASC,
            i.last_update ASC
        LIMIT 1
    """)
    return cursor.fetchone()

def marcar_intento(cursor, symbol):
    cursor.execute("""
        UPDATE sys_info_activos 
        SET alpha_intentos = alpha_intentos + 1,
            alpha_last_try = NOW()
        WHERE symbol = %s
    """, (symbol,))

def marcar_fail(cursor, symbol):
    cursor.execute("""
        UPDATE sys_info_activos 
        SET alpha_fail = 1
        WHERE symbol = %s
    """, (symbol,))

def actualizar_fundamentales(cursor, symbol, r):
    cursor.execute("""
        UPDATE sys_info_activos SET 
            sector = %s,
            industry = %s,
            description = %s,
            pe_ratio = %s,
            dividend_yield = %s,
            ebitda = %s,
            book_value = %s,
            profit_margin = %s,
            analyst_target_price = %s,
            dividendo_fecha = %s,
            source_info = 'alpha_vantage',
            last_update = NOW()
        WHERE symbol = %s
    """, (
        r.get("Sector"),
        r.get("Industry"),
        r.get("Description"),
        None if r.get("PERatio") in ['None', None] else r.get("PERatio"),
        None if r.get("DividendYield") in ['None', None] else r.get("DividendYield"),
        None if r.get("EBITDA") in ['None', None] else r.get("EBITDA"),
        None if r.get("BookValue") in ['None', None] else r.get("BookValue"),
        None if r.get("ProfitMargin") in ['None', None] else r.get("ProfitMargin"),
        None if r.get("AnalystTargetPrice") in ['None', None] else r.get("AnalystTargetPrice"),
        None if r.get("DividendDate") in ['None', None] else r.get("DividendDate"),
        symbol
    ))

def motor_alpha_inteligente():

    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)

    candidato = obtener_candidato_alpha(cursor)

    if not candidato:
        print("No hay candidatos Alpha.")
        return

    symbol = candidato['symbol']
    ticker = candidato['ticker_alpha']

    print(f"Alpha → {symbol} ({ticker})")

    try:
        marcar_intento(cursor, symbol)
        conn.commit()

        r = requests.get(
            f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={ALPHA_VANTAGE_KEY}',
            timeout=10
        ).json()

        if "Note" in r:
            print("RATE LIMIT")
            return

        if not r or "Symbol" not in r:
            print(f"FAIL real: {symbol}")
            marcar_fail(cursor, symbol)
            conn.commit()
            return

        actualizar_fundamentales(cursor, symbol, r)
        conn.commit()

        print(f"OK: {symbol}")

        time.sleep(12)

    except Exception as e:
        print(f"Error Alpha: {e}")

    finally:
        cursor.close()
        conn.close()


# =========================
# LOOP
# =========================
if __name__ == "__main__":

    while True:

        print("\n=== CICLO ===")

        motor_actualizacion_activos()
        motor_alpha_inteligente()

        print("Esperando 1 hora...")
        time.sleep(3600)