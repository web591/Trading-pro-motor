import pandas as pd
import mysql.connector
import os
import re
from datetime import datetime
from config import DB_CONFIG

def conectar_db():
    return mysql.connector.connect(**DB_CONFIG)

def clean_num(val):
    """Limpia cualquier rastro de comillas, comas y espacios antes de convertir a float."""
    if pd.isna(val) or str(val).strip().lower() == 'nan' or str(val).strip() == '':
        return 0.0
    # Elimina todo lo que no sea número, punto o signo menos
    limpio = re.sub(r'[^\d.-]', '', str(val))
    try:
        return float(limpio)
    except:
        return 0.0

def normalizar_fecha_bingx(fecha_str):
    if pd.isna(fecha_str): return None
    formatos = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d-%m-%Y %H:%M:%S"]
    for fmt in formatos:
        try:
            return datetime.strptime(str(fecha_str).strip(), fmt).strftime("%Y-%m-%d %H:%M:%S")
        except: continue
    return str(fecha_str).strip()

def importar_bingx(file_path, user_id):
    db = conectar_db()
    cursor = db.cursor()
    
    try:
        # Usamos engine='python' para evitar errores de parseo con comas dentro de comillas
        df = pd.read_csv(file_path, dtype=str, engine='python', skipinitialspace=True)
        df.columns = [c.strip().replace('"', '') for c in df.columns]
    except Exception as e:
        print(f"❌ Error leyendo {file_path}: {e}")
        return

    print(f"\n🚀 Procesando: {os.path.basename(file_path)} ({len(df)} filas)")
    nuevos = 0

    for i, row in df.iterrows():
        try:
            # Reporte de progreso cada 50 filas para saber que no está colgado
            if i % 50 == 0 and i > 0:
                print(f"⏳ Procesadas {i} filas...")

            id_raw = row.get('Order No.', row.get('Order No', None))
            if not id_raw: continue
            id_ext = f"BX-{id_raw}"

            symbol = str(row.get('Pair', row.get('category', 'UNKNOWN'))).replace('/', '').replace('-', '')
            fecha_raw = row.get('Time(UTC+8)', row.get('closeTime(UTC+8)', row.get('Time', None)))
            fecha = normalizar_fecha_bingx(fecha_raw)
            side_raw = str(row.get('Type', row.get('direction', 'UNKNOWN'))).upper()

            # Limpieza profunda de montos
            precio = clean_num(row.get('DealPrice', row.get('closePrice', row.get('Price', 0))))
            qty = clean_num(row.get('Quantity', row.get('margin', row.get('Amount', 0))))
            comision = clean_num(row.get('Fee', row.get('fees', 0)))
            pnl = clean_num(row.get('Realized PNL', row.get('Realized Pnl', 0)))

            if 'category' in df.columns: tipo_cuenta = "FUTURES_STD"
            elif 'Leverage' in df.columns: tipo_cuenta = "FUTURES_PERP"
            else: tipo_cuenta = "SPOT"

            monto_neto = qty if any(x in side_raw for x in ["BUY", "LONG", "OPEN"]) else -qty
            
            # 1. Global
            cursor.execute("""INSERT IGNORE INTO transacciones_globales 
                            (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, comision, fecha_utc) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
                            (id_ext, user_id, "BingX", tipo_cuenta, "TRADE", symbol, monto_neto, comision, fecha))

            # 2. Detalle
            cursor.execute("""INSERT IGNORE INTO detalle_trades 
                             (id_externo_ref, fecha_utc, symbol, lado, precio_ejecucion, cantidad_ejecutada, pnl_realizado) 
                             VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
                             (id_ext, fecha, symbol, side_raw, precio, qty, pnl))
            
            if cursor.rowcount > 0:
                nuevos += 1

        except Exception as e:
            continue

    db.commit()
    db.close()
    print(f"✅ Finalizado: {nuevos} registros nuevos.")

# --- EJECUCIÓN ---
UID = 6
archivos_bingx = [
    "R.Edo BingX  Consolidado - Done Order Perpetual.csv",
    "R.Edo BingX  Consolidado - Done Order Standar Future.csv",
    "R.Edo BingX  Consolidado - Done orders Spot BingX.csv"
]

if __name__ == "__main__":
    for f in archivos_bingx:
        importar_bingx(f, UID)