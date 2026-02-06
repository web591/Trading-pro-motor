# ==========================================================
# Mini-script diagnóstico Binance Earn Flexible (últimos 7 días)
# ==========================================================

import time, hmac, hashlib, requests, datetime

# ---------------- CONFIG ----------------
USER_ID = 6   # solo para referencia
API_KEY = "TU API_KEY"       # nueva API Key solo lectura
API_SECRET = "TU API_SECRET"

# ---------------- TIEMPOS ----------------
hoy_utc = datetime.datetime.utcnow().date()
start_dt = hoy_utc - datetime.timedelta(days=7)
end_dt = hoy_utc
start_ts = int(time.mktime(start_dt.timetuple()) * 1000)
end_ts = int(time.mktime(end_dt.timetuple()) * 1000 + 86399999)  # hasta 23:59:59 del día de hoy

print("DEBUG - Start Timestamp:", start_ts)
print("DEBUG - End Timestamp:", end_ts)

# ---------------- PETICIÓN EARN FLEXIBLE ----------------
current = 1
size = 100
all_records = []

while True:
    query = f"startTime={start_ts}&endTime={end_ts}&type=ALL&size={size}&current={current}&timestamp={int(time.time()*1000)}"
    signature = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    url = f"https://api.binance.com/sapi/v1/simple-earn/flexible/history/rewardsRecord?{query}&signature={signature}"
    headers = {"X-MBX-APIKEY": API_KEY}

    print(f"\nDEBUG - Página {current}")
    print("URL:", url)

    res = requests.get(url, headers=headers, timeout=10)
    print("Status Code:", res.status_code)
    print("Content-Type:", res.headers.get("Content-Type"))

    try:
        data = res.json()
    except:
        print("⚠️ No es JSON")
        break

    rows = data.get("rows", [])
    if not rows:
        print("⚠️ No hay registros Earn en este rango")
        break

    for r in rows:
        ts = int(r["time"])
        date_utc = time.strftime('%Y-%m-%d', time.gmtime(ts/1000))
        asset = r["asset"]
        amount = float(r["rewards"])
        type_reward = r["type"]
        print(f"{date_utc} | {asset} | {amount:.8f} | {type_reward}")
        all_records.append((date_utc, asset, amount, type_reward))

    if len(rows) < size:
        break
    current += 1

print("\n✅ Diagnóstico completado")
