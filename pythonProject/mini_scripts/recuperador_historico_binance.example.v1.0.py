import requests
import time
import hmac
import hashlib
import mysql.connector
import json
from urllib.parse import urlencode

# === CONFIGURACIÓN ===
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'u800112681_dashboard'
}

def binance_sign(secret, query):
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()

# ... (mantener imports y config de base de datos) ...

def registrar_transaccion_global(cursor, data):
    """ Mismo motor que el v1.2.2 para alimentar el dashboard principal """
    # 1. Intentamos obtener el traductor_id (para asset y broker)
    sql_trad = "SELECT id, categoria_producto, tipo_investment FROM sys_traductor_simbolos WHERE motor_fuente = %s AND ticker_motor = %s LIMIT 1"
    cursor.execute(sql_trad, (data["broker"], data["asset"]))
    res = cursor.fetchone()
    
    traductor_id = res['id'] if res else None
    cuenta_tipo = res['categoria_producto'] if res else "SPOT"
    tipo_inv = res['tipo_investment'] if res else "CRYPTO"

    # ID final para transacciones globales
    id_final = f"{data['user_id']}-CASH-{data['id_externo']}"
    
    sql = """
    INSERT INTO transacciones_globales 
    (id_externo, user_id, tipo_investment, cuenta_tipo, categoria, asset, 
     traductor_id, monto_neto, fecha_utc, broker, raw_json_backup)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE monto_neto = VALUES(monto_neto)
    """
    cursor.execute(sql, (
        id_final, data["user_id"], tipo_inv, cuenta_tipo, data["tipo_evento"], 
        data["asset"], traductor_id, data["cantidad"], data["fecha"], 
        data["broker"], data.get("raw", "{}")
    ))

def registrar_historia_completa(cursor, data):
    """ Inserta en ambas tablas para que aparezca en el Dashboard """
    # A. Insertar en sys_cashflows
    sql_cash = """
    INSERT INTO sys_cashflows (user_id, broker, tipo_evento, asset, cantidad, ticker_motor, fecha_utc, id_externo, raw_json)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE id_externo=id_externo
    """
    cursor.execute(sql_cash, (
        data["user_id"], data["broker"], data["tipo_evento"], data["asset"],
        data["cantidad"], data["ticker_motor"], data["fecha"], 
        data["id_externo"], data["raw"]
    ))
    
    # B. Insertar en transacciones_globales
    registrar_transaccion_global(cursor, data)

# === DENTRO DEL BUCLE DE BARRER_HISTORICO ===
# Cambia la llamada a la función:
# Antes: registrar_en_db(cursor, data_norm)
# Ahora: registrar_historia_completa(cursor, data_norm)

def barrer_historico(db, user_id, key, secret, endpoint_type, start_date_str):
    cursor = db.cursor(dictionary=True)
    start_ts = int(time.mktime(time.strptime(start_date_str, "%Y-%m-%d")) * 1000)
    end_ts_final = int(time.time() * 1000)
    
    # Ventana de 90 días (Binance limit)
    window_ms = 90 * 24 * 60 * 60 * 1000 
    current_start = start_ts
    
    total_procesado = 0

    while current_start < end_ts_final:
        current_end = current_start + window_ms
        if current_end > end_ts_final: current_end = end_ts_final
        
        fecha_h = time.strftime('%Y-%m-%d', time.gmtime(current_start/1000))
        print(f"[*] {endpoint_type}: Consultando bloque desde {fecha_h}...")
        
        params = {"startTime": current_start, "endTime": current_end, "timestamp": int(time.time() * 1000)}
        
        # Selección de URL y Parámetros según tipo
        if endpoint_type == "DIVIDEND":
            url = "https://api.binance.com/sapi/v1/asset/assetDividend"
            params["limit"] = 500
        elif endpoint_type == "INCOME":
            url = "https://fapi.binance.com/fapi/v1/income"
            params["limit"] = 1000
        elif endpoint_type == "DEPOSIT":
            url = "https://api.binance.com/sapi/v1/capital/deposit/hisrec"
        elif endpoint_type == "WITHDRAW":
            url = "https://api.binance.com/sapi/v1/capital/withdraw/history"
        elif endpoint_type == "TRANSFER":
            url = "https://api.binance.com/sapi/v1/asset/transfer"
            params["limit"] = 100
        elif endpoint_type == "MINING":
            url = "https://api.binance.com/sapi/v1/mining/payment/list"
            params["algo"] = "ethash" # O el algoritmo que uses (etcash para ETC)
            params["userName"] = "EthJafa01" # Tu nombre de trabajador en Binance Pool    

        query = urlencode(params)
        sig = binance_sign(secret, query)
        full_url = f"{url}?{query}&signature={sig}"
        
        try:
            res = requests.get(full_url, headers={"X-MBX-APIKEY": key}).json()
            
            # Normalizar respuesta (algunos vienen en 'rows', otros son listas directas)
            if isinstance(res, dict) and "rows" in res: items = res["rows"]
            elif isinstance(res, list): items = res
            else: items = []

            bloque_count = 0
            for i in items:
                data_norm = {"user_id": user_id, "broker": "BINANCE", "raw": json.dumps(i)}
                
                if endpoint_type == "DIVIDEND":
                    data_norm.update({
                        "tipo_evento": "DIVIDEND", "asset": i["asset"], "cantidad": i["amount"],
                        "ticker_motor": i.get("enInfo"), "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(i["divTime"]/1000)),
                        "id_externo": f"BN-DIV-{i.get('tranId', i.get('id'))}"
                    })
                elif endpoint_type == "INCOME":
                    data_norm.update({
                        "tipo_evento": i["incomeType"], "asset": i["asset"], "cantidad": i["income"],
                        "ticker_motor": i.get("symbol"), "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(i["time"]/1000)),
                        "id_externo": f"BN-INC-{i['tranId']}"
                    })
                elif endpoint_type == "DEPOSIT":
                    data_norm.update({
                        "tipo_evento": "DEPOSIT", "asset": i["coin"], "cantidad": i["amount"],
                        "ticker_motor": None, "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(i["insertTime"]/1000)),
                        "id_externo": f"BN-DEP-{i['txId']}"
                    })
                elif endpoint_type == "WITHDRAW":
                    data_norm.update({
                        "tipo_evento": "WITHDRAW", "asset": i["coin"], "cantidad": -float(i["amount"]),
                        "ticker_motor": None, "fecha": i["applyTime"],
                        "id_externo": f"BN-WITH-{i['id']}"
                    })
                elif endpoint_type == "TRANSFER":
                    data_norm.update({
                        "tipo_evento": "TRANSFER", "asset": i["asset"], "cantidad": i["amount"],
                        "ticker_motor": f"{i['type']}", "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(i["timestamp"]/1000)),
                        "id_externo": f"BN-TR-{i['tranId']}"
                    })
                elif endpoint_type == "MINING":
                    data_norm.update({
                        "tipo_evento": "MINING_PAYMENT", 
                        "asset": i["coinName"], 
                        "cantidad": i["amount"],
                        "ticker_motor": "POOL", 
                        "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(i["time"]/1000)),
                        "id_externo": f"BN-MINE-{params['userName']}-{i['time']}"
                    })    

                registrar_historia_completa(cursor, data_norm)
                bloque_count += 1
            
            db.commit()
            total_procesado += bloque_count
            if bloque_count > 0:
                print(f"    [OK] Bloque procesado: {bloque_count} registros encontrados.")
            
        except Exception as e:
            print(f"    [!] Error en bloque: {e}")

        current_start = current_end + 1
        time.sleep(2) # Respetar rate limit
    
    print(f"--- FINALIZADO {endpoint_type}: Total {total_procesado} registros revisados ---\n")

# === EJECUCIÓN ===
# Reemplaza con tus credenciales para la prueba
API_KEY = "TU_KEY"
API_SECRET = "TU_SECRET"

# === EJECUCIÓN MANUAL POR PARTES ===
conn = mysql.connector.connect(**DB_CONFIG)
print("INICIANDO RECUPERACIÓN HISTÓRICA...")

# PROCESO 1: Solo Dividendos (Lánzalo, espera a que termine y luego haz el siguiente)
barrer_historico(conn, 6, API_KEY, API_SECRET, "DIVIDEND", "2021-10-01")

# Estos los dejamos "apagados" (con #) por ahora
# barrer_historico(conn, 6, API_KEY, API_SECRET, "INCOME", "2021-10-01")
# barrer_historico(conn, 6, API_KEY, API_SECRET, "DEPOSIT", "2021-10-01")
# barrer_historico(conn, 6, API_KEY, API_SECRET, "WITHDRAW", "2021-10-01")
# barrer_historico(conn, 6, API_KEY, API_SECRET, "TRANSFER", "2021-10-01")
# barrer_historico(conn, 6, API_KEY, API_SECRET, "MINING", "2021-10-01")

conn.close()