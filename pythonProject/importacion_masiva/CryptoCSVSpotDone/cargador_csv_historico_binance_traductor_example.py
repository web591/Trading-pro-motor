import pandas as pd
import mysql.connector
import json
import os
from datetime import datetime

# ==========================================================
# ⚙️ CONFIGURACIÓN DE BASE DE DATOS (Pon tus datos aquí)
# ==========================================================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'u800112681_dashboard'
}

def buscar_traductor_id(cursor, symbol, broker):
    # CORRECCIÓN: Usamos tu tabla 'sys_traductor_simbolos' 
    # y comparamos contra 'ticker_motor' y 'motor_fuente'
    sql = """
        SELECT id, categoria_producto, tipo_investment, motor_fuente 
        FROM sys_traductor_simbolos 
        WHERE ticker_motor = %s AND motor_fuente = %s
    """
    cursor.execute(sql, (symbol, broker.lower())) # 'binance' en minúsculas como en tu tabla
    return cursor.fetchone()

def registrar_trade_limpio(cursor, uid, t_data, info_traductor, broker_nombre, tipo_mercado):
    try:
        # Si encuentra el traductor lo usa, si no, usa los valores por defecto del archivo
        t_id = info_traductor['id'] if info_traductor else None
        cat_prod = info_traductor['categoria_producto'] if info_traductor else tipo_mercado
        tipo_inv = info_traductor['tipo_investment'] if info_traductor else 'CRYPTO'
        motor = info_traductor['motor_fuente'] if info_traductor else broker_nombre.lower()

        id_vinculo = f"BN-TRD-{t_data['tradeId']}"
        lado = t_data.get('side', 'BUY') 

        # Lógica de flujo de caja (Conciliación)
        if tipo_mercado == 'SPOT':
            monto_neto_global = float(t_data.get('quoteQty', 0))
            if lado == 'BUY': monto_neto_global = -monto_neto_global 
        else:
            monto_neto_global = float(t_data.get('realizedPnl', 0))

        # 1. transacciones_globales
        sql_global = """
            INSERT IGNORE INTO transacciones_globales
            (id_externo, user_id, tipo_investment, cuenta_tipo, categoria, asset,
             traductor_id, monto_neto, comision, fecha_utc, broker, descripcion, raw_json_backup)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cursor.execute(sql_global, (
            id_vinculo, uid, tipo_inv, cat_prod, 'TRADE', 
            t_data['symbol'], t_id, monto_neto_global, 
            t_data.get('commission', 0), t_data['fecha_sql'], broker_nombre,
            f"Trade {lado} {t_data['symbol']} ({tipo_mercado})",
            json.dumps(t_data)
        ))

        # 2. detalle_trades
        sql_detalle = """
            INSERT IGNORE INTO detalle_trades (
                user_id, traductor_id, broker, categoria_producto,
                motor_fuente, tipo_investment, id_externo_ref, fecha_utc, 
                symbol, lado, position_side, reduce_only, precio_ejecucion, 
                cantidad_ejecutada, commission, commission_asset,
                quote_qty, pnl_realizado, is_maker, trade_id_externo, raw_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cursor.execute(sql_detalle, (
            uid, t_id, broker_nombre, cat_prod, motor, tipo_inv,
            id_vinculo, t_data['fecha_sql'], t_data['symbol'],
            lado, t_data.get('positionSide'), t_data.get('reduceOnly', 0),
            float(t_data.get('price', 0)), float(t_data.get('qty', 0)),
            float(t_data.get('commission', 0)), t_data.get('commissionAsset'),
            float(t_data.get('quoteQty', 0)), float(t_data.get('realizedPnl', 0)),
            t_data.get('isMaker', 0), f"TRD-{t_data['tradeId']}",
            json.dumps(t_data)
        ))        
        return True
    except Exception as e:
        print(f"   [!] Error en registro: {e}")
        return False

def procesar_csv_ada(nombre_archivo, user_id, tipo_mercado):
    ruta_completa = os.path.join(os.getcwd(), nombre_archivo)
    
    if not os.path.exists(ruta_completa):
        print(f"⚠️ Archivo no encontrado: {nombre_archivo}")
        return

    db = mysql.connector.connect(**DB_CONFIG)
    cursor = db.cursor(dictionary=True)
    df = pd.read_csv(ruta_completa)
    
    print(f"\n🚀 PROCESANDO {tipo_mercado}: {nombre_archivo}")
    count = 0

    for _, row in df.iterrows():
        # Fechas
        time_str = str(row['time'])
        for fmt in ('%d-%m-%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S'):
            try:
                dt = datetime.strptime(time_str, fmt)
                break
            except: continue
        f_sql = dt.strftime('%Y-%m-%d %H:%M:%S')

        # Side
        if 'side' in row:
            lado = str(row['side']).upper()
        else:
            lado = 'BUY' if str(row.get('isBuyer')).upper() == 'TRUE' else 'SELL'

        t_data = {
            'symbol': row['symbol'],
            'orderId': row['orderId'],
            'tradeId': row.get('id', row['orderId']),
            'price': row['price'],
            'qty': row['qty'],
            'quoteQty': row.get('quoteQty', row.get('baseQty', 0)),
            'commission': row.get('commission', 0),
            'commissionAsset': row.get('commissionAsset', 'USDT'),
            'fecha_sql': f_sql,
            'side': lado,
            'positionSide': row.get('positionSide', 'BOTH'),
            'realizedPnl': row.get('realizedPnl', 0),
            'isMaker': 1 if str(row.get('isMaker', row.get('maker', ''))).upper() == 'TRUE' else 0,
            'reduceOnly': 1 if str(row.get('reduceOnly', 'FALSE')).upper() == 'TRUE' else 0
        }

        # Búsqueda en TU tabla real
        info_t = buscar_traductor_id(cursor, t_data['symbol'], 'BINANCE')
        
        if registrar_trade_limpio(cursor, user_id, t_data, info_t, "BINANCE", tipo_mercado):
            count += 1

    db.commit()
    db.close()
    print(f"✅ Finalizado: {count} registros.")

if __name__ == "__main__":
    ID_USUARIO = 6
    # Asegúrate de estar en la carpeta correcta
    procesar_csv_ada('trades_VARIOSUSDT.csv', ID_USUARIO, 'SPOT')
    #procesar_csv_ada('DoneUM_ETCUSDT.csv', ID_USUARIO, 'UM_FUTURES')
    #procesar_csv_ada('DoneCM_ETCUSD1_PERP.csv', ID_USUARIO, 'CM_FUTURES')