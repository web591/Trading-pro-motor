import time, hmac, hashlib, requests, json

# ======================================================
# CONFIGURACIÓN DE PRUEBA (Pon tus llaves aquí)
# ======================================================
API_KEY = "TU API_KEY"
API_SECRET = "TU API_SECRET"

def bingx_req(path, params=None, method="GET"):
    if params is None: params = {}
    params["timestamp"] = int(time.time() * 1000)
    qs = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])
    signature = hmac.new(API_SECRET.encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
    url = f"https://open-api.bingx.com{path}?{qs}&signature={signature}"
    headers = {'X-BX-APIKEY': API_KEY}
    return requests.get(url, headers=headers).json()

def debug_bingx():
    print(f"\n{'='*60}\nDEBUG BINGX v1.7.3 - REVISIÓN TOTAL\n{'='*60}")

    # 1. SPOT
    print("\n--- [1] SPOT BALANCES ---")
    print(json.dumps(bingx_req("/openApi/spot/v1/account/balance"), indent=2))
    
    # 2. PERPETUAL BALANCES (Aquí estaba el error del dict)
    print("\n--- [2] PERPETUAL BALANCES ---")
    res_swap = bingx_req("/openApi/swap/v2/user/balance")
    print(json.dumps(res_swap, indent=2))
    if res_swap.get('code') == 0:
        # Nota: BingX devuelve un dict 'balance', no una lista.
        print(f"✅ Data detectada como: {type(res_swap['data'].get('balance'))}")

    # 3. OPEN ORDERS
    print("\n--- [3] OPEN ORDERS (PERPETUAL) ---")
    print(json.dumps(bingx_req("/openApi/swap/v2/trade/openOrders"), indent=2))

    # 4. TRADES / ALL ORDERS (Corregido)
    print("\n--- [4] RECENT TRADES (ALL ORDERS) ---")
    # Nota: A veces allOrders requiere un 'symbol' si la cuenta tiene mucha actividad
    # Si te sigue dando error, prueba poner {"symbol": "BTC-USDT"}
    res_trades = bingx_req("/openApi/swap/v2/trade/allOrders", {"limit": 5})
    print(json.dumps(res_trades, indent=2))

    # 5. INCOME (STATEMENTS)
    print("\n--- [5] STATEMENTS / INCOME ---")
    print(json.dumps(bingx_req("/openApi/swap/v2/user/income", {"limit": 5}), indent=2))

if __name__ == "__main__":
    debug_bingx()