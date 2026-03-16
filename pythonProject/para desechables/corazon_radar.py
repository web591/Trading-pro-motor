import mysql.connector

def radar_deteccion_activos(conexion_db, exchange_api, user_id):
    """
    Escanea saldos y órdenes para detectar tickers nuevos.
    """
    cursor = conexion_db.cursor(dictionary=True)
    
    # 1. Obtener tickers brutos del Exchange (Ejemplo con Binance/BingX)
    # Aquí el motor ya trajo la data de: /account y /openOrders
    activos_detectados = exchange_api.get_all_assets_in_use() 
    # Ejemplo: ['BTC', 'ETH', 'JUP', 'SOL']

    for ticker_raw in activos_detectados:
        # Limpiar el ticker (quitar USDT, PERP, etc.)
        ticker = limpiar_ticker(ticker_raw) 
        
        # A. ¿Ya tiene un traductor asignado?
        cursor.execute("""
            SELECT id FROM sys_traductor_simbolos 
            WHERE (ticker_motor = %s OR nombre_comun = %s) AND is_active = 1
        """, (ticker, ticker))
        
        if cursor.fetchone():
            continue  # Ya lo conocemos, no hacemos nada.

        # B. ¿Está ya en el buzón (Pendiente o Ignorado)?
        cursor.execute("""
            SELECT status FROM sys_simbolos_buscados 
            WHERE ticker = %s AND user_id = %s
        """, (ticker, user_id))
        
        registro_buzon = cursor.fetchone()

        if registro_buzon:
            # Si ya está pendiente, encontrado o ignorado (la vacuna), lo saltamos
            continue 
        
        # C. ¡NUEVO HALLAZGO! Lo insertamos en el Buzón
        print(f"Detectado nuevo activo: {ticker}. Enviando al buzón...")
        
        sql_insert = """
            INSERT INTO sys_simbolos_buscados (ticker, origen, status, user_id)
            VALUES (%s, 'MOTOR', 'pendiente', %s)
        """
        cursor.execute(sql_insert, (ticker, user_id))
        conexion_db.commit()

def limpiar_ticker(symbol):
    """
    Lógica para extraer la moneda base.
    Ej: JUPUSDT -> JUP, BTC-PERP -> BTC
    """
    for suffix in ['USDT', 'BTC', 'ETH', 'PERP', '-']:
        symbol = symbol.replace(suffix, '')
    return symbol.strip()