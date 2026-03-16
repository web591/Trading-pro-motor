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

def registrar_transaccion_global(cursor, data):
    sql_trad = "SELECT id, categoria_producto, tipo_investment FROM sys_traductor_simbolos WHERE motor_fuente = %s AND ticker_motor = %s LIMIT 1"
    cursor.execute(sql_trad, (data["broker"], data["asset"]))
    res = cursor.fetchone()
    traductor_id = res['id'] if res else None
    cuenta_tipo = res['categoria_producto'] if res else "SPOT"
    tipo_inv = res['tipo_investment'] if res else "CRYPTO"
    id_final = f"{data['user_id']}-CASH-{data['id_externo']}"
    sql = """INSERT INTO transacciones_globales (id_externo, user_id, tipo_investment, cuenta_tipo, categoria, asset, traductor_id, monto_neto, fecha_utc, broker, raw_json_backup)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE monto_neto = VALUES(monto_neto)"""
    cursor.execute(sql, (id_final, data["user_id"], tipo_inv, cuenta_tipo, data["tipo_evento"], data["asset"], traductor_id, data["cantidad"], data["fecha"], data["broker"], data.get("raw", "{}")))

def registrar_historia_completa(cursor, data):
    sql_cash = """INSERT INTO sys_cashflows (user_id, broker, tipo_evento, asset, cantidad, ticker_motor, fecha_utc, id_externo, raw_json)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE id_externo=id_externo"""
    cursor.execute(sql_cash, (data["user_id"], data["broker"], data["tipo_evento"], data["asset"], data["cantidad"], data["ticker_motor"], data["fecha"], data["id_externo"], data["raw"]))
    registrar_transaccion_global(cursor, data)

def barrer_historico_bingx(db, user_id, key, secret, endpoint_type, start_date_str):
    cursor = db.cursor(dictionary=True)
    current_start = int(time.mktime(time.strptime(start_date_str, "%Y-%m-%d")) * 1000)
    end_ts_final = int(time.time() * 1000)
    window_ms = 15 * 24 * 60 * 60 * 1000 
    total_procesado = 0
    base_url = "https://open-api.bingx.com" 

    while current_start < end_ts_final:
        current_end = min(current_start + window_ms, end_ts_final)
        fecha_h = time.strftime('%Y-%m-%d', time.gmtime(current_start/1000))
        
        # --- RUTAS DEFINITIVAS ---
        if endpoint_type == "ASSET_RECORD":
            path = "/openApi/v3/asset/getLog"
        else:
            path = "/openApi/swap/v2/user/income"

        params = {
            "timestamp": int(time.time() * 1000),
            "startTime": current_start,
            "endTime": current_end
        }
        query = urlencode(params)
        signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"{base_url}{path}?{query}&signature={signature}"
        
        headers = {'X-BX-APIKEY': key, 'User-Agent': 'Mozilla/5.0'}
        print(f"[*] {fecha_h} | {endpoint_type} | Llamando...", end=" ", flush=True)
        
        try:
            r = requests.get(url, headers=headers, timeout=12)
            res = r.json()
            
            if res.get("code") == 0:
                # La V3 devuelve los datos en res['data'] o res['data']['list']
                raw_data = res.get("data", [])
                items = raw_data.get("list", []) if isinstance(raw_data, dict) else raw_data
                
                if items:
                    for i in items:
                        # Unificamos campos (V3 usa 'time' y 'amount')
                        tm = i.get("time", i.get("tm", 0))
                        asset = i.get("asset", i.get("coin", "USDT"))
                        amt = i.get("amount", i.get("income", i.get("change", 0)))
                        
                        data_norm = {
                            "user_id": user_id, "broker": "BINGX", "raw": json.dumps(i),
                            "tipo_evento": i.get("type", endpoint_type), 
                            "asset": asset, "cantidad": amt,
                            "ticker_motor": i.get("info", i.get("symbol", "N/A")),
                            "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(tm/1000)),
                            "id_externo": f"BX-{tm}-{asset}-{amt}"
                        }
                        registrar_historia_completa(cursor, data_norm)
                    db.commit()
                    total_procesado += len(items)
                    print(f"OK ({len(items)})")
                else:
                    print("Vacío.")
            else:
                print(f"Error: {res.get('msg')} ({res.get('code')})")
                
        except Exception as e:
            print(f"Fallo: {e}")

        current_start += window_ms
        time.sleep(0.5)
    print(f"--- FINALIZADO {endpoint_type}: {total_procesado} ---")

# === EJECUCIÓN ===
MI_API_KEY = "TU_KEY"
MI_API_SECRET = "TU_SECRET"

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    print(">>> CONECTADO A DB.")
    barrer_historico_bingx(conn, 6, MI_API_KEY, MI_API_SECRET, "ASSET_RECORD", "2024-08-01")
    barrer_historico_bingx(conn, 6, MI_API_KEY, MI_API_SECRET, "FUTURES_INCOME", "2024-08-01")
    conn.close()
    print(">>> PROCESO TERMINADO.")
except Exception as e:
    print(f"ERROR: {e}")