import time, hmac, hashlib, requests, json

# ======================================================
# CONFIGURACIÓN BINANCE
# ======================================================
API_KEY = "TU API_KEY"
API_SECRET = "TU API_SECRET"

def binance_req(base_url, path, params=None):
    if params is None: params = {}
    params["timestamp"] = int(time.time() * 1000)
    
    # IMPORTANTE: Ordenar parámetros alfabéticamente para la firma
    ordered_params = sorted(params.items())
    query_string = "&".join([f"{k}={v}" for k, v in ordered_params])
    
    sig = hmac.new(API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
    url = f"{base_url}{path}?{query_string}&signature={sig}"
    
    res = requests.get(url, headers={'X-MBX-APIKEY': API_KEY})
    return res.json()

def debug_binance():
    print(f"\n{'='*70}\nDEBUG BINANCE v1.7.3 - SINCRONIZACIÓN TOTAL\n{'='*70}")
    SPOT = "https://api.binance.com"
    FUT  = "https://fapi.binance.com"

    # --- [1] SALDOS ---
    print("\n--- [1.1] SPOT BALANCES ---")
    res_s = binance_req(SPOT, "/api/v3/account")
    print(json.dumps([b for b in res_s.get('balances', []) if float(b['free'])+float(b['locked']) > 0], indent=2))
    
    print("\n--- [1.2] PERPETUAL BALANCES ---")
    res_f = binance_req(FUT, "/fapi/v2/account")
    print(json.dumps([a for a in res_f.get('assets', []) if float(a['walletBalance']) > 0], indent=2))

    # --- [2] OPEN ORDERS ---
    print("\n--- [2.1] SPOT OPEN ORDERS ---")
    print(json.dumps(binance_req(SPOT, "/api/v3/openOrders"), indent=2))
    
    print("\n--- [2.2] PERPETUAL OPEN ORDERS ---")
    print(json.dumps(binance_req(FUT, "/fapi/v1/openOrders"), indent=2))

    # --- [3] TRADES (Ejemplos con BTCUSDT) ---
    print("\n--- [3.1] SPOT TRADES (BTCUSDT) ---")
    print(json.dumps(binance_req(SPOT, "/api/v3/myTrades", {"symbol": "BTCUSDT", "limit": 5}), indent=2))
    
    print("\n--- [3.2] PERPETUAL TRADES (BTCUSDT) ---")
    print(json.dumps(binance_req(FUT, "/fapi/v1/userTrades", {"symbol": "BTCUSDT", "limit": 5}), indent=2))

    # --- [4] MOVIMIENTOS DE WALLET (Deposits, Withdrawals, Dust) ---
    print("\n--- [4.1] DEPOSIT HISTORY ---")
    print(json.dumps(binance_req(SPOT, "/sapi/v1/capital/deposit/hisrec", {"limit": 5}), indent=2))

    print("\n--- [4.2] WITHDRAW HISTORY ---")
    print(json.dumps(binance_req(SPOT, "/sapi/v1/capital/withdraw/history", {"limit": 5}), indent=2))

    print("\n--- [4.3] DUST LOG (BNB Conversions) ---")
    print(json.dumps(binance_req(SPOT, "/sapi/v1/asset/dribblet", {}), indent=2))

    # --- [5] DISTRIBUCIONES (Earn, Airdrops, Staking, Pool) ---
    print("\n--- [5] EARN / AIRDROPS / DISTRIBUTIONS ---")
    print(json.dumps(binance_req(SPOT, "/sapi/v1/asset/assetDividend", {"limit": 10}), indent=2))

    # --- [6] FUTURES INCOME (PNL, Funding, Commissions) ---
    print("\n--- [6] FUTURES INCOME (Statements) ---")
    print(json.dumps(binance_req(FUT, "/fapi/v1/income", {"limit": 10}), indent=2))

if __name__ == "__main__":
    debug_binance()