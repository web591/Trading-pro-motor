import pandas as pd
import mysql.connector
import json
import os
from datetime import datetime

# ==========================================================
# ⚙️ CONFIGURACIÓN DE BASE DE DATOS
# ==========================================================
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "tu_usuario",
    "password": "tu_password",
    "database": "u800112681_dashboard"
}

# ==========================================================
# 🧠 LÓGICA REPLICADA DEL MOTOR v6.6.6.23 (FIXED)
# ==========================================================

def buscar_traductor_id(cursor, symbol, broker, categoria):
    # Replicamos exactamente cómo tu motor busca los IDs
    sql = "SELECT id, categoria_producto, tipo_investment, motor_fuente FROM sys_traductor_activos WHERE activo_broker = %s AND broker = %s"
    cursor.execute(sql, (symbol, broker))
    return cursor.fetchone()

def registrar_trade(cursor, uid, t_data, info_traductor, broker_nombre):
    try:
        t_id = info_traductor['id'] if info_traductor else None
        cat_prod = info_traductor['categoria_producto'] if info_traductor else 'SPOT'
        tipo_inv = info_traductor['tipo_investment'] if info_traductor else 'CRYPTO'
        motor = info_traductor['motor_fuente'] if info_traductor else broker_nombre.lower()

        id_vinculo = f"{uid}-{t_data['orderId']}"
        lado = t_data.get('side', 'BUY') 

        # 1. Registro en transacciones_globales
        sql_global = """
            INSERT IGNORE INTO transacciones_globales
            (id_externo, user_id, tipo_investment, cuenta_tipo, categoria, asset,
             traductor_id, monto_neto, comision, fecha_utc, broker)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cursor.execute(sql_global, (
            id_vinculo, uid, tipo_inv, cat_prod, 'TRADE', 
            t_data['symbol'], t_id, t_data.get('quoteQty', 0), 
            t_data.get('commission', 0), t_data['fecha_sql'], broker_nombre
        ))

        # 2. Registro en detalle_trades (CORREGIDO CON 21 PARÁMETROS)
        sql_detalle = """
            INSERT IGNORE INTO detalle_trades (
                user_id, traductor_id, broker, categoria_producto,
                motor_fuente, tipo_investment, id_externo_ref, fecha_utc, 
                symbol, lado, position_side, reduce_only, precio_ejecucion, 
                cantidad_ejecutada, commission, commission_asset,
                quote_qty, pnl_realizado, is_maker, trade_id_externo, raw_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE raw_json = VALUES(raw_json)
        """
        
        cursor.execute(sql_detalle, (
            uid, t_id, broker_nombre, cat_prod, motor, tipo_inv,
            id_vinculo, t_data['fecha_sql'], t_data['symbol'],
            lado, t_data.get('positionSide'), 1 if t_data.get('reduceOnly') else 0,
            float(t_data.get('price', 0)), float(t_data.get('qty', 0)),
            float(t_data.get('commission', 0)), t_data.get('commissionAsset'),
            float(t_data.get('quoteQty', 0)), float(t_data.get('realizedPnl', 0)),
            1 if t_data.get('isMaker') else 0, f"TRD-{t_data.get('tradeId', t_data['orderId'])}",
            json.dumps(t_data)
        ))        
        return True
    except Exception as e:
        print(f"❌ Error en registro: {e}")
        return False

# ==========================================================
# 📂 PROCESADOR DE CSV
# ==========================================================

def procesar_archivo(file_path, user_id, tipo_mercado):
    if not os.path.exists(file_path):
        print(f"⚠️ No existe: {file_path}")
        return

    db = mysql.connector.connect(**DB_CONFIG)
    cursor = db.cursor(dictionary=True)
    df = pd.read_csv(file_path)
    
    print(f"⌛ Iniciando {file_path}...")
    count = 0

    for _, row in df.iterrows():
        # Normalización de tiempo
        try:
            dt = datetime.strptime(str(row['time']), '%d-%m-%Y %H:%M:%S')
        except:
            dt = datetime.strptime(str(row['time']), '%Y-%m-%d %H:%M:%S')
        
        f_sql = dt.strftime('%Y-%m-%d %H:%M:%S')

        # Diccionario compatible con la lógica del motor
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
            'side': row.get('side', 'BUY' if str(row.get('isBuyer')).upper() == 'TRUE' else 'SELL'),
            'positionSide': row.get('positionSide', 'BOTH'),
            'realizedPnl': row.get('realizedPnl', 0),
            'isMaker': 1 if str(row.get('maker')).upper() == 'TRUE' or str(row.get('isMaker')).upper() == 'TRUE' else 0,
            'reduceOnly': 1 if str(row.get('reduceOnly')).upper() == 'TRUE' else 0
        }

        info_t = buscar_traductor_id(cursor, t_data['symbol'], 'BINANCE', tipo_mercado)
        
        if registrar_trade(cursor, user_id, t_data, info_t, "BINANCE"):
            count += 1

    db.commit()
    db.close()
    print(f"✅ User {user_id}: {count} trades cargados desde {file_path}")

# ==========================================================
# 🏁 EJECUCIÓN
# ==========================================================
if __name__ == "__main__":
    U6 = 6
    # Ejecutar las cargas para ADA como ejemplo:
    procesar_archivo('trades_ADAUSDT.csv', U6, 'SPOT')
    procesar_archivo('DoneUM_ADAUSDT.csv', U6, 'UM_FUTURES')
    procesar_archivo('DoneCM_ADAUSD_PERP.csv', U6, 'CM_FUTURES')