import mysql.connector
import yfinance as yf
import pandas as pd
import config

def reparacion_total_final():
    db = mysql.connector.connect(**config.DB_CONFIG)
    cursor = db.cursor(dictionary=True)
    
    # Seleccionamos TODOS los que tengan comisión pero el USD esté en 0 (recién reseteados)
    sql = """
        SELECT id_detalle, fecha_utc, commission, commission_asset 
        FROM detalle_trades 
        WHERE commission > 0 
          AND (commission_usd < 0.000001 OR commission_usd IS NULL)
          AND commission_asset NOT IN ('USDT', 'USDC', 'BUSD', 'DAI', 'FDUSD', 'TUSD')
    """
    cursor.execute(sql)
    filas = cursor.fetchall()
    
    total = len(filas)
    print(f"🚀 Iniciando recálculo real de {total} filas...")

    cache_precios = {}
    procesados = 0

    for fila in filas:
        fecha_str = fila['fecha_utc'].strftime('%Y-%m-%d')
        asset = fila['commission_asset'].strip().replace('"', '')
        
        # Ignorar si el asset es vacío
        if not asset or asset == 'None': continue
            
        key = f"{asset}_{fecha_str}"
        
        if key not in cache_precios:
            ticker = f"{asset}-USD"
            try:
                data = yf.download(ticker, start=fecha_str, end=pd.to_datetime(fecha_str) + pd.Timedelta(days=1), progress=False)
                if not data.empty:
                    precio = data['Close'].iloc[0]
                    if isinstance(precio, pd.Series): precio = precio.iloc[0]
                    cache_precios[key] = float(precio)
                else:
                    cache_precios[key] = None
            except:
                cache_precios[key] = None
        
        precio_final = cache_precios.get(key)
        if precio_final:
            comision_real_usd = float(fila['commission']) * precio_final
            
            # ACTUALIZACIÓN QUIRÚRGICA POR ID
            cursor.execute(
                "UPDATE detalle_trades SET commission_usd = %s WHERE id_detalle = %s",
                (comision_real_usd, fila['id_detalle'])
            )
            procesados += 1
            if procesados % 50 == 0:
                db.commit()
                print(f"⏳ Procesados {procesados}/{total}...")

    db.commit()
    db.close()
    print(f"✅ ¡LISTO! Se han recalculado {procesados} comisiones con precios reales.")

if __name__ == "__main__":
    reparacion_total_final()