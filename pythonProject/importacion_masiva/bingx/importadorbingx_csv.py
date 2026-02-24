import pandas as pd
import mysql.connector
import os
import re
from datetime import datetime, timedelta
from config import DB_CONFIG

def conectar_db():
    return mysql.connector.connect(**DB_CONFIG)

# ==========================================================
# NUMERICO SEGURO
# ==========================================================
def clean_num(val):
    if pd.isna(val) or str(val).strip().lower() == 'nan':
        return 0.0
    limpio = re.sub(r'[^\d.-]', '', str(val))
    try:
        return float(limpio)
    except:
        return 0.0

# ==========================================================
# ⏰ NORMALIZADOR UTC (ALINEADO MOTOR)
# BingX viene en UTC+8
# ==========================================================
def normalizar_fecha_bingx(fecha_raw):

    if pd.isna(fecha_raw):
        return None

    formatos = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y %H:%M:%S"
    ]

    for fmt in formatos:
        try:
            fecha_local = datetime.strptime(str(fecha_raw).strip(), fmt)
            fecha_utc = fecha_local - timedelta(hours=8)
            return fecha_utc.replace(microsecond=0)
        except:
            continue

    return None

# ==========================================================
# ID INSTITUCIONAL (ANTI DUPLICADO MULTI USER)
# ==========================================================
def generar_id_institucional(exchange, cuenta, order_id):

    if not order_id:
        return None

    return f"{exchange}-{cuenta}-{str(order_id).strip()}"

# ==========================================================
# IMPORTADOR
# ==========================================================
def importar_bingx(file_path, user_id):

    db = conectar_db()
    cursor = db.cursor()

    df = pd.read_csv(file_path, dtype=str, engine='python')
    df.columns = [c.strip().replace('"','') for c in df.columns]

    print(f"\n🚀 Procesando {os.path.basename(file_path)}")

    nuevos = 0

    for _, row in df.iterrows():

        try:

            id_raw = row.get('Order No.', row.get('Order No', None))
            if not id_raw:
                continue

            # Detectar cuenta
            if 'category' in df.columns:
                cuenta = "FUTURES_STD"
            elif 'Leverage' in df.columns:
                cuenta = "FUTURES_PERP"
            else:
                cuenta = "SPOT"

            id_ext = generar_id_institucional("BINGX", cuenta, id_raw)

            symbol = str(row.get('Pair','UNKNOWN')).replace('/','').replace('-','')

            fecha_raw = row.get('Time(UTC+8)', row.get('closeTime(UTC+8)', None))
            fecha = normalizar_fecha_bingx(fecha_raw)

            side = str(row.get('Type','UNKNOWN')).upper()

            precio = clean_num(row.get('DealPrice',0))
            qty = clean_num(row.get('Quantity',0))
            comision = clean_num(row.get('Fee',0))
            pnl = clean_num(row.get('Realized PNL',0))

            monto = qty if any(x in side for x in ["BUY","LONG","OPEN"]) else -qty

            # GLOBAL
            cursor.execute("""
            INSERT IGNORE INTO transacciones_globales
            (id_externo,user_id,exchange,cuenta_tipo,categoria,asset,monto_neto,comision,fecha_utc)
            VALUES (%s,%s,'BingX',%s,'TRADE',%s,%s,%s,%s)
            """,(id_ext,user_id,cuenta,symbol,monto,comision,fecha))

            # DETALLE
            cursor.execute("""
            INSERT IGNORE INTO detalle_trades
            (id_externo_ref,fecha_utc,symbol,lado,precio_ejecucion,cantidad_ejecutada,pnl_realizado)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,(id_ext,fecha,symbol,side,precio,qty,pnl))

            if cursor.rowcount > 0:
                nuevos += 1

        except:
            continue

    db.commit()
    db.close()

    print(f"✅ {nuevos} nuevos registros")

# ==========================================================
# EJECUCION
# ==========================================================
UID = 6

archivos = [
"R.Edo BingX  Consolidado - Done Order Perpetual.csv",
"R.Edo BingX  Consolidado - Done Order Standar Future.csv",
"R.Edo BingX  Consolidado - Done orders Spot BingX.csv"
]

if __name__ == "__main__":
    for f in archivos:
        importar_bingx(f, UID)