import os  # Asegúrate de tener esta línea arriba del todo
import pandas as pd
import mysql.connector
from datetime import datetime
import config
from config import DB_CONFIG

def conectar_db():
    return mysql.connector.connect(**DB_CONFIG)

def normalizar_fecha_motor(fecha_raw):

    if not fecha_raw:
        return None

    try:
        if isinstance(fecha_raw,(int,float)):
            if fecha_raw > 9999999999:
                fecha = datetime.utcfromtimestamp(int(fecha_raw)/1000)
            else:
                fecha = datetime.utcfromtimestamp(int(fecha_raw))

        elif isinstance(fecha_raw,str):
            for fmt in [
                "%d-%m-%Y %H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
                "%d/%m/%Y %H:%M",
                "%Y-%m-%dT%H:%M:%S"
            ]:
                try:
                    fecha = datetime.strptime(fecha_raw.strip(),fmt)
                    break
                except: continue
            else:
                return None

        elif isinstance(fecha_raw,datetime):
            fecha = fecha_raw
        else:
            return None

        return fecha.replace(microsecond=0)

    except:
        return None

def normalizar_categoria_motor(cat):
    """
    Mantenemos el nombre original pero con lógica Ledger v4.4.2
    """
    op = str(cat).upper().strip()
    
    # Lógica unificada
    if any(x in op for x in ['LAUNCHPOOL', 'DISTRIBUTION', 'AIRDROP', 'DIVIDEND']): return "AIRDROP"
    if any(x in op for x in ['EARN', 'SAVINGS', 'STAKING', 'INTEREST']): return "INTEREST"
    if any(x in op for x in ['MINING', 'POOL REWARDS']): return "MINING"
    if any(x in op for x in ['VOUCHER', 'BONUS']): return "BONUS"
    if any(x in op for x in ['REBATE', 'COMMISSION REBATE']): return "REBATE"
    if 'CASHBACK' in op: return "CASHBACK"
    if any(x in op for x in ['FEE', 'TRANSACTION FEE']): return "FEE"
    if 'FUNDING' in op: return "FUNDING"
    if any(x in op for x in ['DEPOSIT', 'INITIAL BALANCE']): return "DEPOSIT"
    if any(x in op for x in ['WITHDRAW', 'SEND']): return "WITHDRAW"
    if any(x in op for x in ['TRANSFER', 'P2P', 'INTERNAL']): return "TRANSFER_INTERNAL"
    if any(x in op for x in ['TRADE', 'BUY', 'SELL', 'TRANSACTION', 'PNL']): return "TRADE"
    
    return "UNKNOWN"



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
            
            # --- DETECCIÓN INTELIGENTE DE MERCADO ---
            if 'isBuyer' in row:
                tipo_mercado = "SPOT"
                side = "BUY" if str(row['isBuyer']).upper() == 'TRUE' else "SELL"
            elif 'Market' in row:
                if row['Market'] == 'FUM': tipo_mercado = "USDT-M"
                elif row['Market'] == 'FCM': tipo_mercado = "COIN-M"
                else: tipo_mercado = "FUTURES"
                side = row['side'].upper()
            else:
                tipo_mercado = "UNKNOWN"
                side = row.get('side', 'BUY').upper()

            # ID Único con trazabilidad de mercado
            id_ext_comun = f"CSV-BN-{tipo_mercado}-{symbol}-{row['id']}" 
            fecha = normalizar_fecha_motor(row['time'])

            # Inserción Global (Dashboard)
            sql_global = """INSERT IGNORE INTO transacciones_globales 
                            (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, comision, fecha_utc) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            
            cursor.execute(sql_global, (
                id_ext_comun, user_id, "Binance", tipo_mercado, "TRADE", symbol, 
                float(row['qty']) if side in ["BUY", "LONG"] else -float(row['qty']), 
                float(row.get('commission', 0)), fecha
            ))

            # Inserción Detalle (Filtros PHP)
            sql_detalle = """INSERT IGNORE INTO detalle_trades 
                 (id_externo_ref, user_id, exchange, tipo_mercado, symbol, lado, precio_ejecucion, 
                  cantidad_ejecutada, pnl_realizado, fecha_utc) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            cursor.execute(sql_detalle, (
                id_ext_comun, user_id, "Binance", tipo_mercado, symbol, side, 
                float(row['price']), float(row['qty']), float(row.get('realizedPnl', 0)), fecha
            ))
             
            if cursor.rowcount > 0:
                nuevos += 1

        except Exception as e:
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