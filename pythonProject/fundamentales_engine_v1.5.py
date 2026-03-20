# Version 1.5

import mysql.connector
import requests
import yfinance as yf
import time
from config import DB_CONFIG, ALPHA_VANTAGE_KEY

def conectar_db():
    return mysql.connector.connect(**DB_CONFIG)

# =========================
# YAHOO (SIN CAMBIOS)
# =========================

def motor_actualizacion_activos():
    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)

    print("\n--- YAHOO ---")

    cursor.execute("SELECT underlying, ticker_motor FROM sys_traductor_simbolos WHERE motor_fuente = 'yahoo_sym'")
    activos = cursor.fetchall()

    for i, activo in enumerate(activos, 1):
        symbol = activo['underlying']
        ticker = activo['ticker_motor']

        try:
            info = yf.Ticker(ticker).info

            cursor.execute("""
                INSERT INTO sys_info_activos (symbol, nombre_comercial, market_cap, last_update, source_info)
                VALUES (%s, %s, %s, NOW(), 'yahoo_sym')
                ON DUPLICATE KEY UPDATE 
                nombre_comercial = VALUES(nombre_comercial),
                market_cap = VALUES(market_cap),
                last_update = NOW()
            """, (symbol, info.get('longName'), info.get('marketCap')))

            conn.commit()
            time.sleep(0.5)

        except Exception as e:
            print(f"Error Yahoo {symbol}: {e}")
            time.sleep(2)

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
            AND (
                i.last_update IS NULL
                OR i.last_update < NOW() - INTERVAL 7 DAY
            )
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


# =========================
# 🔥 AUTO-CLASIFICACIÓN
# =========================

def clasificar_asset(r):
    asset_type = (r.get("AssetType") or "").upper()

    if "STOCK" in asset_type or "ETF" in asset_type:
        return "STOCK", "SPOT"
    elif "DIGITAL" in asset_type or "CRYPTO" in asset_type:
        return "CRYPTO", "SPOT"
    elif "FOREX" in asset_type or "CURRENCY" in asset_type:
        return "FIAT", "SPOT"
    else:
        return "STOCK", "SPOT"  # fallback seguro


# =========================
# TRADUCTOR DINÁMICO
# =========================

# Version 2.0 - guardar_traductor_alpha dinámico

def guardar_traductor_alpha(cursor, symbol, ticker, tipo, categoria):
    cursor.execute("""
        INSERT INTO sys_traductor_simbolos (
            user_id,
            nombre_comun,
            motor_fuente,
            tipo_investment,
            ticker_motor,
            underlying,
            categoria_producto,
            is_active
        )
        VALUES (
            NULL,
            %s,
            'alpha_sym',
            %s,
            %s,
            %s,
            %s,
            0
        )
        ON DUPLICATE KEY UPDATE
            ticker_motor = VALUES(ticker_motor),
            categoria_producto = VALUES(categoria_producto),
            tipo_investment = VALUES(tipo_investment),
            is_active = 0
    """, (
        symbol,
        tipo,
        ticker,
        symbol,
        categoria
    ))


# =========================
# FUNDAMENTALES FULL
# =========================

def actualizar_fundamentales(cursor, symbol, r):

    def clean(x):
        return None if x in ['None', None, ''] else x

    cursor.execute("""
        UPDATE sys_info_activos SET 
            sector = %s,
            industry = %s,
            description = %s,
            pe_ratio = %s,
            dividend_yield = %s,
            eps = %s,
            ebitda = %s,
            book_value = %s,
            dividend_per_share = %s,
            profit_margin = %s,
            operating_margin_ttm = %s,
            analyst_target_price = %s,
            trailing_pe = %s,
            forward_pe = %s,
            price_to_book_ratio = %s,
            dividendo_fecha = %s,
            ex_dividendo_fecha = %s,
            source_info = 'alpha_vantage',
            last_update = NOW()
        WHERE symbol = %s
    """, (
        clean(r.get("Sector")),
        clean(r.get("Industry")),
        clean(r.get("Description")),
        clean(r.get("PERatio")),
        clean(r.get("DividendYield")),
        clean(r.get("EPS")),
        clean(r.get("EBITDA")),
        clean(r.get("BookValue")),
        clean(r.get("DividendPerShare")),
        clean(r.get("ProfitMargin")),
        clean(r.get("OperatingMarginTTM")),
        clean(r.get("AnalystTargetPrice")),
        clean(r.get("TrailingPE")),
        clean(r.get("ForwardPE")),
        clean(r.get("PriceToBookRatio")),
        clean(r.get("DividendDate")),
        clean(r.get("ExDividendDate")),
        symbol
    ))


# =========================
# MOTOR PRINCIPAL
# =========================

def motor_alpha_inteligente():

    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)

    candidato = obtener_candidato_alpha(cursor)

    if not candidato:
        print("No hay candidatos Alpha.")
        cursor.close()
        conn.close()
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

        # RATE LIMIT
        if "Note" in r:
            print("RATE LIMIT")
            return

        # FAIL REAL
        if not r or "Symbol" not in r:
            print(f"FAIL real: {symbol}")
            marcar_fail(cursor, symbol)
            conn.commit()
            return

        # 🔥 CLASIFICACIÓN AUTOMÁTICA
        tipo, categoria = clasificar_asset(r)

        # FUNDAMENTALES
        actualizar_fundamentales(cursor, symbol, r)

        # 🔥 AUTO-APRENDIZAJE INTELIGENTE
        guardar_traductor_alpha(cursor, symbol, ticker, tipo, categoria)

        conn.commit()

        print(f"OK + aprendido: {symbol} | {tipo} - {categoria}")

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

        print("Esperando 1 hora y pico.22 Ejecuciones diarias..")
        time.sleep(3900)