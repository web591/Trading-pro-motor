import mysql.connector
import yfinance as yf
import pandas as pd
import config
from datetime import timedelta

def reparacion_comisiones_no_stables():
    db = mysql.connector.connect(**config.DB_CONFIG)
    cursor = db.cursor(dictionary=True)
    
    # 1. Buscamos trades donde la comisión NO sea una stablecoin
    # y que necesiten reparación (puedes ajustar el filtro de commission_usd si quieres forzar todos)
    sql = """
        SELECT id_detalle, fecha_utc, commission, commission_asset 
        FROM detalle_trades 
        WHERE commission > 0 
          AND commission_asset NOT IN ('USDT', 'USDC', 'BUSD', 'DAI', 'FDUSD', 'TUSD', 'PYUSD')
          AND broker = 'BINANCE'
    """
    cursor.execute(sql)
    filas = cursor.fetchall()
    
    total = len(filas)
    print(f"🚀 Analizando {total} filas con comisiones en cripto/altcoins...")

    cache_precios = {}
    procesados = 0
    errores = 0

    for fila in filas:
        fecha_dt = fila['fecha_utc']
        fecha_str = fecha_dt.strftime('%Y-%m-%d')
        # Limpieza de asset (quitar comillas o espacios)
        asset = fila['commission_asset'].strip().replace('"', '').replace("'", "")
        
        if not asset or asset == 'None': continue
            
        key = f"{asset}_{fecha_str}"
        
        if key not in cache_precios:
            # Formato Yahoo Finance para Cripto es BTC-USD
            ticker_str = f"{asset}-USD"
            try:
                # Descargamos el día exacto
                data = yf.download(ticker_str, start=fecha_str, end=(fecha_dt + timedelta(days=1)).strftime('%Y-%m-%d'), progress=False)
                if not data.empty:
                    # Intentamos obtener el precio de cierre
                    precio = data['Close'].iloc[0]
                    # En versiones nuevas de yf, el resultado puede ser un float directo o una serie
                    val_precio = float(precio) if not isinstance(precio, pd.Series) else float(precio.iloc[0])
                    cache_precios[key] = val_precio
                else:
                    cache_precios[key] = None
            except Exception as e:
                print(f"⚠️ Error buscando {ticker_str}: {e}")
                cache_precios[key] = None
        
        precio_historico = cache_precios.get(key)
        
        if precio_historico:
            comision_calculada_usd = float(fila['commission']) * precio_historico
            
            # 2. UPDATE QUIRÚRGICO
            try:
                cursor.execute(
                    "UPDATE detalle_trades SET commission_usd = %s WHERE id_detalle = %s",
                    (comision_calculada_usd, fila['id_detalle'])
                )
                procesados += 1
            except Exception as e:
                print(f"❌ Error en UPDATE id {fila['id_detalle']}: {e}")
                errores += 1
        else:
            # Si no hay precio, podrías dejar un log para revisar manualmente
            pass

        if procesados % 25 == 0 and procesados > 0:
            db.commit()
            print(f"⏳ Avance: {procesados}/{total} reparados... (Errores: {errores})")

    db.commit()
    db.close()
    print(f"\n✅ PROCESO TERMINADO")
    print(f"📊 Reparados con éxito: {procesados}")
    print(f"❌ No se pudo encontrar precio para: {total - procesados}")

if __name__ == "__main__":
    reparacion_comisiones_no_stables()