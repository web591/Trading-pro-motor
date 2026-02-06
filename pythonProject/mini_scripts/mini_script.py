# ==========================================================
# Mini-script diagnóstico Binance Earn Flexible
# Solo terminal, muestra query y firma HMAC
# ==========================================================

import time
import hmac
import hashlib
import requests

# ---------------- CONFIG ----------------
API_KEY = "TU API_KEY"       # nueva API Key solo lectura
API_SECRET = "TU API_SECRET"

# ---------------- TIEMPOS ----------------
# últimos 3 días
hoy_utc = time.gmtime()
start_ts = int(time.time() - 3*24*60*60) * 1000  # 3 días atrás en ms
end_ts   = int(time.time()) * 1000                # ahora en ms

print("DEBUG - Start Timestamp:", start_ts)
print("DEBUG - End Timestamp:", end_ts)

# ---------------- PETICIÓN ----------------
current = 1
size = 100
earn_type = "ALL"

while True:
    # construimos la query string EXACTA
    query = f"startTime={start_ts}&endTime={end_ts}&type={earn_type}&size={size}&current={current}&timestamp={end_ts}"
    # calculamos la firma HMAC
    signature = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    # URL final
    url = f"https://api.binance.com/sapi/v1/simple-earn/flexible/history/rewardsRecord?{query}&signature={signature}"

    print(f"\nDEBUG - Página {current}")
    print("URL:", url)
    print("Signature HMAC:", signature)

    headers = {"X-MBX-APIKEY": API_KEY}

    try:
        res = requests.get(url, headers=headers, timeout=10)
        print("Status Code:", res.status_code)
        print("Content-Type:", res.headers.get("Content-Type"))

        # intentar mostrar JSON si es válido
        try:
            print("JSON Response:", res.json())
        except:
            print("⚠️ No es JSON:", res.text)
    except Exception as e:
        print("⚠️ Error en request:", str(e))

    # solo una página para diagnóstico
    break

print("\n✅ Diagnóstico completado")
