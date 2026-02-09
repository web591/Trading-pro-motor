# ==========================================================
# BINGX SCRIPT6 DEBUG MASTER
# Version 6.6 - FULL LOCKED + JSON RAW (NO CAMBIOS DE ENDPOINTS)
# ==========================================================

import time
import hmac
import hashlib
import requests
import json
from datetime import datetime, timezone

API_KEY = "TU_API_KEY"
API_SECRET = "TU_API_SECRET"

BASE = "https://open-api.bingx.com"
EXCHANGE = "BINGX"

# ==========================================================
# HELPERS
# ==========================================================

def now_ms():
    return int(time.time() * 1000)

def motor_time(ms):
    if not ms:
        ms = now_ms()
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()

def sign_params(params):
    params["timestamp"] = now_ms()
    query = "&".join(f"{k}={params[k]}" for k in sorted(params))
    signature = hmac.new(
        API_SECRET.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()
    return query, signature

def bingx_req(path, params=None):
    if params is None:
        params = {}
    query, sig = sign_params(params)
    url = f"{BASE}{path}?{query}&signature={sig}"
    headers = {"X-BX-APIKEY": API_KEY}
    r = requests.get(url, headers=headers)

    try:
        return r.json()
    except:
        return {}

def jdump(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except:
        return str(obj)

def print_table(title, rows, cols):
    print(f"\n--- {title} ---")
    if not rows:
        print("Sin datos")
        return

    header = " | ".join(cols)
    print(header)
    print("-" * len(header))

    for r in rows:
        print(" | ".join(str(r.get(c, "")) for c in cols))

# ==========================================================
# SPOT BALANCES
# ==========================================================

def spot_balances():
    data = bingx_req("/openApi/spot/v1/account/balance")
    rows = []

    for a in data.get("data", {}).get("balances", []):
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "SPOT",
            "asset": a.get("asset"),
            "free": a.get("free"),
            "locked": a.get("locked"),
            "id_externo": None,
            "motor_event_time_normalized": motor_time(None),
            "json_raw": jdump(a)
        })

    print_table(
        "SPOT BALANCES",
        rows,
        ["exchange","cuenta_tipo","asset","free","locked",
         "id_externo","motor_event_time_normalized","json_raw"]
    )

# ==========================================================
# FUTURES BALANCES PERPETUAL
# ==========================================================

def futures_balances_perpetual():
    data = bingx_req("/openApi/swap/v2/user/balance")
    rows = []

    balance = data.get("data", {}).get("balance")

    if isinstance(balance, dict):
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "FUTURES_PERPETUAL",
            "asset": balance.get("asset"),
            "walletBalance": balance.get("balance"),
            "unrealizedProfit": balance.get("unrealizedProfit"),
            "id_externo": balance.get("userId"),
            "motor_event_time_normalized": motor_time(None),
            "json_raw": jdump(balance)
        })

    print_table(
        "FUTURES BALANCES (PERPETUAL)",
        rows,
        ["exchange","cuenta_tipo","asset","walletBalance",
         "unrealizedProfit","id_externo","motor_event_time_normalized","json_raw"]
    )

# ==========================================================
# FUTURES BALANCES STANDARD
# ==========================================================

def futures_balances_standard():
    data = bingx_req("/openApi/futures/v1/account/balance")
    rows = []

    balances = data.get("data", [])

    if isinstance(balances, list):
        for b in balances:
            rows.append({
                "exchange": EXCHANGE,
                "cuenta_tipo": "FUTURES_STANDARD",
                "asset": b.get("asset"),
                "walletBalance": b.get("balance"),
                "unrealizedProfit": b.get("unrealizedProfit"),
                "id_externo": None,
                "motor_event_time_normalized": motor_time(None),
                "json_raw": jdump(b)
            })

    print_table(
        "FUTURES BALANCES (STANDARD)",
        rows,
        ["exchange","cuenta_tipo","asset","walletBalance",
         "unrealizedProfit","id_externo","motor_event_time_normalized","json_raw"]
    )

# ==========================================================
# SPOT OPEN ORDERS
# ==========================================================

def spot_open_orders():
    data = bingx_req("/openApi/spot/v1/trade/openOrders")
    rows = []

    for o in data.get("data", {}).get("orders", []):
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "SPOT",
            "symbol": o.get("symbol"),
            "side": o.get("side"),
            "type": o.get("type"),
            "price": o.get("price"),
            "origQty": o.get("origQty"),
            "executedQty": o.get("executedQty"),
            "status": o.get("status"),
            "id_externo": o.get("orderId"),
            "motor_event_time_normalized": motor_time(o.get("time")),
            "json_raw": jdump(o)
        })

    print_table(
        "SPOT OPEN ORDERS",
        rows,
        ["exchange","cuenta_tipo","symbol","side","type",
         "price","origQty","executedQty","status",
         "id_externo","motor_event_time_normalized","json_raw"]
    )

# ==========================================================
# FUTURES OPEN ORDERS
# ==========================================================

def futures_open_orders():
    data = bingx_req("/openApi/swap/v2/trade/openOrders")
    rows = []

    for o in data.get("data", {}).get("orders", []):
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "FUTURES",
            "symbol": o.get("symbol"),
            "side": o.get("side"),
            "price": o.get("price"),
            "origQty": o.get("origQty"),
            "status": o.get("status"),
            "id_externo": o.get("orderId"),
            "motor_event_time_normalized": motor_time(o.get("updateTime")),
            "json_raw": jdump(o)
        })

    print_table(
        "FUTURES OPEN ORDERS",
        rows,
        ["exchange","cuenta_tipo","symbol","side","price",
         "origQty","status","id_externo","motor_event_time_normalized","json_raw"]
    )

# ==========================================================
# FUTURES TRADES
# ==========================================================

def futures_trades():
    data = bingx_req("/openApi/swap/v2/trade/allOrders", {"limit": 50})
    rows = []

    for t in data.get("data", {}).get("orders", []):
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "FUTURES",
            "symbol": t.get("symbol"),
            "price": t.get("price"),
            "qty": t.get("origQty"),
            "id_externo": t.get("orderId"),
            "time": t.get("updateTime"),
            "motor_event_time_normalized": motor_time(t.get("updateTime")),
            "json_raw": jdump(t)
        })

    print_table(
        "FUTURES TRADES",
        rows,
        ["exchange","cuenta_tipo","symbol","price","qty",
         "id_externo","time","motor_event_time_normalized","json_raw"]
    )

# ==========================================================
# FUTURES INCOME
# ==========================================================

def futures_income():
    data = bingx_req("/openApi/swap/v2/user/income", {"limit": 50})
    rows = []

    raw = data.get("data")

    if isinstance(raw, dict):
        items = raw.get("list", [])
    elif isinstance(raw, list):
        items = raw
    else:
        items = []

    for i in items:
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "FUTURES",
            "symbol": i.get("symbol"),
            "asset": i.get("asset"),
            "income": i.get("income"),
            "incomeType": i.get("type"),
            "id_externo": i.get("tranId"),
            "time": i.get("time"),
            "motor_event_time_normalized": motor_time(i.get("time")),
            "json_raw": jdump(i)
        })

    print_table(
        "FUTURES INCOME",
        rows,
        ["exchange","cuenta_tipo","symbol","asset","income",
         "incomeType","id_externo","time","motor_event_time_normalized","json_raw"]
    )

# ==========================================================
# FASE 2 — WALLET FLOWS (FUND FLOW REAL)
# ==========================================================

def wallet_flows():
    """
    BingX NO separa deposit / withdraw / transfer
    Todo viene aquí (fund flow / income)
    """
    data = bingx_req("/openApi/swap/v2/user/income", {"limit": 100})
    rows = []

    raw = data.get("data")
    if isinstance(raw, dict):
        items = raw.get("list", [])
    elif isinstance(raw, list):
        items = raw
    else:
        items = []

    for i in items:
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "WALLET_FLOW",
            "flow_type": i.get("type"),   # FUNDING / TRANSFER / PNL / FEE / etc
            "symbol": i.get("symbol"),
            "asset": i.get("asset"),
            "amount": i.get("income"),
            "id_externo": i.get("tranId"),
            "time": i.get("time"),
            "motor_event_time_normalized": motor_time(i.get("time")),
            "json_raw": jdump(i)
        })

    print_table(
        "WALLET FLOWS (DEPOSIT / WITHDRAW / TRANSFER / FUNDING)",
        rows,
        [
            "exchange","cuenta_tipo","flow_type","symbol","asset",
            "amount","id_externo","time",
            "motor_event_time_normalized","json_raw"
        ]
    )

# ==========================================================
# FASE 3 — RISK STATE (POSITIONS)
# ==========================================================

def futures_positions():
    data = bingx_req("/openApi/swap/v2/user/positions")
    rows = []

    positions = data.get("data", [])

    if isinstance(positions, list):
        for p in positions:
            rows.append({
                "exchange": EXCHANGE,
                "cuenta_tipo": "FUTURES_POSITION",
                "symbol": p.get("symbol"),
                "side": p.get("positionSide"),
                "positionAmt": p.get("positionAmt"),
                "entryPrice": p.get("entryPrice"),
                "markPrice": p.get("markPrice"),
                "unrealizedPnl": p.get("unrealizedProfit"),
                "leverage": p.get("leverage"),
                "id_externo": p.get("positionId"),
                "motor_event_time_normalized": motor_time(None),
                "json_raw": jdump(p)
            })

    print_table(
        "FUTURES POSITIONS (RISK STATE)",
        rows,
        [
            "exchange","cuenta_tipo","symbol","side",
            "positionAmt","entryPrice","markPrice",
            "unrealizedPnl","leverage","id_externo",
            "motor_event_time_normalized","json_raw"
        ]
    )

# ==========================================================
# MAIN
# ==========================================================

def run():
    print("\n====================================================")
    print("BINGX SCRIPT6 DEBUG MASTER v6.6")
    print("FULL LOCKED + JSON RAW")
    print("====================================================")

    spot_balances()
    futures_balances_perpetual()
    futures_balances_standard()
    spot_open_orders()
    futures_open_orders()
    futures_trades()
    futures_income()
    wallet_flows()
    futures_positions()

if __name__ == "__main__":
    run()
