import os
import pandas as pd
import mysql.connector
import re
from datetime import datetime, timedelta
import config

def conectar_db():
    return mysql.connector.connect(**config.DB_CONFIG)

def clean_num(val):
    if pd.isna(val) or str(val).strip().lower() == 'nan': return 0.0
    limpio = re.sub(r'[^\d.-]', '', str(val).replace(',', ''))
    try: return float(limpio)
    except: return 0.0

def arreglar_id_excel(val):
    if pd.isna(val): return ""
    v = str(val).strip()
    if 'E+' in v.upper():
        try: return "{:.0f}".format(float(v))
        except: return v
    return v.split('.')[0]

def normalizar_fecha_final(fecha_raw):
    if pd.isna(fecha_raw) or str(fecha_raw).strip() == '': return None
    f_str = str(fecha_raw).strip()
    formatos = ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M:%S"]
    for fmt in formatos:
        try:
            dt = datetime.strptime(f_str, fmt)
            return (dt - timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
        except: continue
    return None

def importar_bingx_estilo_motor(base_name, user_id, cuenta_tipo):
    file_path = base_name if os.path.exists(base_name) else base_name.replace("Conso", "Consolidado")
    
    if not os.path.exists(file_path):
        print(f"⚠️ Saltando {cuenta_tipo}: No se encontró el archivo '{file_path}'")
        return

    db = conectar_db()
    cursor = db.cursor(dictionary=True)
    print(f"\n🚀 PROCESANDO: {file_path} ({cuenta_tipo})")
    
    try:
        df = pd.read_csv(file_path, encoding='latin1', quotechar='"', sep=None, engine='python')
        df.columns = [c.strip() for c in df.columns]
    except Exception as e:
        print(f"❌ Error crítico leyendo CSV: {e}")
        return

    nuevos = 0
    for i, row in df.iterrows():
        try:
            oid_raw = row.get('Order No.', row.get('Order No', None))
            if oid_raw is None: continue
            order_id = arreglar_id_excel(oid_raw)

            raw_pair = str(row.get('Pair', row.get('category', ''))).strip()
            if not raw_pair or raw_pair == 'nan': continue

            # Buscamos en traductor para obtener ID y Tipo Investment real
            cursor.execute("""
                SELECT id, ticker_motor, tipo_investment FROM sys_traductor_simbolos 
                WHERE ticker_motor = %s AND motor_fuente LIKE '%bingx%' LIMIT 1
            """, (raw_pair,))
            traductor = cursor.fetchone()
            
            if not traductor:
                continue

            t_id = traductor['id']
            t_inv = traductor['tipo_investment']
            t_ticker = traductor['ticker_motor']

            col_f = 'Time(UTC+8)' if 'Time(UTC+8)' in row else ('closeTime(UTC+8)' if 'closeTime(UTC+8)' in row else 'openTime(UTC+8)')
            fecha = normalizar_fecha_final(row.get(col_f))
            if not fecha: continue

            side = str(row.get('Type', row.get('direction', ''))).upper()
            precio = clean_num(row.get('DealPrice', row.get('closePrice', row.get('Price', 0))))
            qty = clean_num(row.get('Quantity', row.get('Amount', 0)))
            pnl = clean_num(row.get('Realized PNL', row.get('Realized Pnl', 0)))
            fee = clean_num(row.get('Fee', row.get('fees', 0)))

            if cuenta_tipo == 'SPOT':
                monto_neto = clean_num(row.get('Order Value', 0))
                if "SELL" not in side: monto_neto = -abs(monto_neto)
            else:
                monto_neto = pnl - abs(fee)

            # 1. Inserción en detalle_trades
            cursor.execute("""
                INSERT IGNORE INTO detalle_trades 
                (user_id, traductor_id, broker, categoria_producto, motor_fuente, tipo_investment, 
                 id_externo_ref, fecha_utc, symbol, lado, precio_ejecucion, cantidad_ejecutada, 
                 commission, pnl_realizado, trade_id_externo)
                VALUES (%s, %s, 'BINGX', %s, 'CSV_IMPORT', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, t_id, cuenta_tipo, t_inv, f"BX-{order_id}", fecha, t_ticker, side, precio, qty, fee, pnl, order_id))

            # 2. Inserción en transacciones_globales
            cursor.execute("""
                INSERT IGNORE INTO transacciones_globales
                (id_externo, user_id, tipo_investment, cuenta_tipo, categoria, asset, traductor_id, 
                 monto_neto, comision, fecha_utc, descripcion, broker)
                VALUES (%s, %s, %s, %s, 'TRADE', %s, %s, %s, %s, %s, %s, 'BINGX')
            """, (f"BX-{order_id}", user_id, t_inv, cuenta_tipo, t_ticker, t_id, monto_neto, fee, fecha, f"Trade {side} {t_ticker}"))

            if cursor.rowcount > 0: nuevos += 1

        except Exception as e:
            print(f"❌ Error en fila {i}: {e}")

    db.commit()
    db.close()
    print(f"✅ FINALIZADO {cuenta_tipo}: {nuevos} registros nuevos cargados.")

if __name__ == "__main__":
    u_id = 6
    importar_bingx_estilo_motor('R.Edo BingX  Conso - Done orders Spot BingX.csv', u_id, 'SPOT')
    importar_bingx_estilo_motor('R.Edo BingX  Conso - Done Order Perpetual.csv', u_id, 'PERPETUAL')
    importar_bingx_estilo_motor('R.Edo BingX  Conso - Done Order Standar Future.csv', u_id, 'STANDARD')