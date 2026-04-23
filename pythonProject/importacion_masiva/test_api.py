import time, hmac, hashlib, requests
from datetime import datetime

API_KEY = "TU_API_KEY"
API_SECRET = "TU_API_SECRET"

def call(method, endpoint, params):
    params["timestamp"] = int(time.time() * 1000)
    query = "&".join([f"{k}={v}" for k, v in params.items()])
    signature = hmac.new(API_SECRET.encode('utf-8'), query.encode('utf-8'), hashlib.sha256).hexdigest()
    url = f"https://api.binance.com{endpoint}?{query}&signature={signature}"
    return requests.get(url, headers={"X-MBX-APIKEY": API_KEY}).json()

print("--- 🚨 OPERACIÓN RESCATE: ARCHIVO HISTÓRICO 2021 🚨 ---")

# Rango del 18 de Octubre 2021 (Día completo)
s = 1634515200000 
e = 1634601600000 

# PRUEBA A: SAPI All Orders (Historial de Sistema)
# Este endpoint es distinto al que usamos antes (V3)
print("\n[1] Consultando SAPI All Orders (ADAUSDT)...")
res_a = call("GET", "/sapi/v1/margin/allOrders", {"symbol": "ADAUSDT", "startTime": s, "endTime": e})
print(f"Respuesta SAPI Margin: {res_a}")

# PRUEBA B: MyTrades V3 con Rango Estricto pero con de-listing check
# Probaremos ADABUSD por si el reporte CSV agrupó pares.
print("\n[2] Consultando ADABUSD (Por si acaso)...")
res_b = call("GET", "/api/v3/myTrades", {"symbol": "ADABUSD", "startTime": s, "endTime": e})
print(f"Respuesta ADABUSD: {res_b}")

# PRUEBA C: CAPITAL FLOW (El más importante)
# Este endpoint muestra entradas y salidas de dinero por trade
print("\n[3] Consultando Capital Flow (Libro Contable)...")
# Nota: Este endpoint usa 'fromId' o 'startTime'. Probamos 24h.
res_c = call("GET", "/sapi/v1/asset/assetLog", {"startTime": s, "endTime": e})
# Si da 404, probamos el endpoint alternativo de 'Universal Transfer'
if isinstance(res_c, dict) and res_c.get('code') == -1002 or 'error' in str(res_c):
    print("    Intentando con Universal Transfer History...")
    res_c = call("GET", "/sapi/v1/asset/transfer/GetTransactionHistory", {"type": "MAIN_UMFUTURE", "startTime": s})

print(f"Respuesta Contable: {res_c}")

# PRUEBA D: Trade Fee (Ver si hay un registro de que alguna vez pagaste comisión de ADA)
print("\n[4] Consultando historial de comisiones pagadas...")
res_d = call("GET", "/sapi/v1/asset/tradeFee", {"symbol": "ADAUSDT"})
print(f"Respuesta Fees: {res_d}")