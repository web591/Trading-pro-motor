import mysql.connector
import yfinance as yf
import pandas as pd
import config
from datetime import timedelta

def reparacion_comisiones_no_stables():
    db = mysql.connector.connect(**config.DB_CONFIG)
    cursor = db.cursor(dictionary=True)
    
    # Añadimos symbol y precio_ejecucion a la consulta para comparar
    sql = """
        SELECT id_detalle, fecha_utc, commission, commission_asset, symbol, precio_ejecucion 
        FROM detalle_trades 
        WHERE commission > 0 
          AND commission_asset NOT IN ('USDT', 'USDC', 'BUSD', 'DAI', 'FDUSD', 'TUSD', 'PYUSD')
          AND broker = 'BINANCE'
    """
    cursor.execute(sql)
    filas = cursor.fetchall()
    
    total = len(filas)
    print(f"🚀 Analizando {total} filas...")

    cache_precios = {}
    procesados = 0
    errores = 0

    for fila in filas:
        id_det = fila['id_detalle']
        asset = fila['commission_asset'].strip().replace('"', '').replace("'", "")
        symbol = fila['symbol']
        precio_ejecucion = float(fila['precio_ejecucion'])
        comision_qty = float(fila['commission'])
        
        comision_calculada_usd = 0
        metodo = ""

        # --- NIVEL 1: COINCIDENCIA CON EL SYMBOL (EXACTITUD TOTAL) ---
        # Si compras ADA/USDT y la comisión es ADA, el precio es el del trade.
        if asset in symbol and precio_ejecucion > 0:
            comision_calculada_usd = comision_qty * precio_ejecucion
            metodo = "PRECIO_EJECUCION (Exacto)"
        
        # --- NIVEL 2: YFINANCE (PRECIO DIARIO) ---
        else:
            fecha_dt = fila['fecha_utc']
            fecha_str = fecha_dt.strftime('%Y-%m-%d')
            key = f"{asset}_{fecha_str}"
            
            if key not in cache_precios:
                ticker_str = f"{asset}-USD"
                try:
                    data = yf.download(ticker_str, start=fecha_str, end=(fecha_dt + timedelta(days=1)).strftime('%Y-%m-%d'), progress=False)
                    if not data.empty:
                        precio = data['Close'].iloc[0]
                        cache_precios[key] = float(precio) if not isinstance(precio, pd.Series) else float(precio.iloc[0])
                    else:
                        cache_precios[key] = None
                except:
                    cache_precios[key] = None
            
            precio_hist = cache_precios.get(key)
            if precio_hist:
                comision_calculada_usd = comision_qty * precio_hist
                metodo = "YFINANCE (Estimado)"

        # --- APLICAR EL UPDATE SI TENEMOS PRECIO ---
        if comision_calculada_usd > 0:
            try:
                cursor.execute(
                    "UPDATE detalle_trades SET commission_usd = %s WHERE id_detalle = %s",
                    (comision_calculada_usd, id_det)
                )
                procesados += 1
                if metodo == "PRECIO_EJECUCION (Exacto)":
                    print(f"✅ ID {id_det}: Reparado con precio del trade (${comision_calculada_usd:.4f})")
            except Exception as e:
                errores += 1

        if procesados % 50 == 0 and procesados > 0:
            db.commit()

    db.commit()
    db.close()
    print(f"\n✅ PROCESO TERMINADO")
    print(f"📊 Total analizado: {total}")
    print(f"📊 Reparados con éxito: {procesados}")
    print(f"❌ Fallidos: {total - procesados}")

if __name__ == "__main__":
    reparacion_comisiones_no_stables()
