import os  # Asegúrate de tener esta línea arriba del todo
import pandas as pd
import mysql.connector
from datetime import datetime
import config
from config import DB_CONFIG

def conectar_db():
    return mysql.connector.connect(**DB_CONFIG)

def normalizar_fecha(fecha_str):
    for fmt in ["%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M"]:
        try:
            return datetime.strptime(str(fecha_str).strip(), fmt).strftime("%Y-%m-%d %H:%M:%S")
        except: continue
    return None

def importar_trades_sincronizado(file_path, user_id):
    db = conectar_db()
    cursor = db.cursor()
    
    try:
        df = pd.read_csv(file_path, dtype=str, low_memory=False)
        df.columns = [c.strip() for c in df.columns]
    except Exception as e:
        print(f"❌ Error leyendo {file_path}: {e}")
        return

    print(f"📡 Sincronizando con tablas SQL: {file_path}...")
    nuevos = 0

    for _, row in df.iterrows():
        try:
            symbol = row['symbol']
            # ID que se usará como enlace entre ambas tablas
            id_ext_comun = f"BIN-{row['id']}" 
            fecha = normalizar_fecha(row['time'])
            
            if 'isBuyer' in row:
                side = "BUY" if row['isBuyer'].upper() == 'TRUE' else "SELL"
                tipo_cuenta = "SPOT"
            else:
                side = row['side'].upper()
                tipo_cuenta = "FUTURES"

            # 1. INSERTAR EN transacciones_globales (Nombres confirmados en tu SQL)
            qty = float(row['qty'])
            monto_neto = qty if side == "BUY" else -qty

            sql_global = """INSERT IGNORE INTO transacciones_globales 
                            (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, comision, fecha_utc) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            
            cursor.execute(sql_global, (
                id_ext_comun, user_id, "Binance", tipo_cuenta, "TRADE", symbol, 
                monto_neto, float(row['commission']), fecha
            ))

            # 2. INSERTAR EN detalle_trades (Ajustado a tus nombres: id_externo_ref, lado, precio_ejecucion, etc.)
            sql_detalle = """INSERT IGNORE INTO detalle_trades 
                 (id_externo_ref, fecha_utc, symbol, lado, precio_ejecucion, 
                  cantidad_ejecutada, pnl_realizado, is_maker) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

            pnl = float(row.get('realizedPnl', 0))
            is_maker = 1 if str(row.get('maker', 'False')).upper() == 'TRUE' else 0

            cursor.execute(sql_detalle, (
                id_ext_comun, fecha, symbol, side, float(row['price']), 
                qty, pnl, is_maker
            ))
            
            if cursor.rowcount > 0:
                nuevos += 1

        except Exception as e:
            # Descomenta la siguiente línea si quieres ver el error detallado por fila
            # print(f"Error en fila: {e}")
            continue

    db.commit()
    cursor.close()
    db.close()
    print(f"✅ ¡Éxito! {nuevos} registros vinculados correctamente.")

# --- CONFIGURACIÓN DE CARGA AUTOMÁTICA ---
UID = 6
# Usamos exactamente la ruta que me pasaste
RUTA_ARCHIVOS = "C:/Users/ajafa/Mi unidad/sublime_Texts_Github/Trading-pro-motor/pythonProject/importacion_masiva/"

if __name__ == "__main__":
    print(f"🚀 Iniciando escaneo masivo en: {RUTA_ARCHIVOS}")
    
    try:
        # Obtenemos la lista de todos los archivos en esa carpeta
        contenido = os.listdir(RUTA_ARCHIVOS)
    except Exception as e:
        print(f"❌ Error al acceder a la ruta: {e}")
        contenido = []

    archivos_procesados = 0
    
    for nombre_archivo in contenido:
        # Filtros de seguridad:
        # 1. Que sea un archivo CSV
        # 2. Que no sea el consolidado (porque ya lo subimos con el otro script)
        if nombre_archivo.endswith(".csv") and nombre_archivo != "binance_conso.csv":
            
            # Construimos la ruta completa (une la carpeta con el nombre del archivo)
            ruta_completa = os.path.join(RUTA_ARCHIVOS, nombre_archivo)
            
            # Llamamos a tu función de importación
            importar_trades_sincronizado(ruta_completa, UID)
            archivos_procesados += 1

    print(f"\n🏆 ¡CARGA MASIVA FINALIZADA!")
    print(f"📊 Se procesaron un total de {archivos_procesados} archivos CSV.")