# ==========================================================
# BINANCE SCRIPT6 DEBUG MASTER
# Version 6.6 - FULL ORIGINAL + JSON_RAW EN TODAS LAS TABLAS
# ==========================================================

import time
import hmac
import hashlib
import requests
import json
from datetime import datetime, timezone

# ==========================================================
# CONFIG
# ==========================================================

API_KEY = "TU_API_KEY"
API_SECRET = "TU_API_SECRET"

SPOT = "https://api.binance.com"
FUT  = "https://fapi.binance.com"

EXCHANGE = "BINANCE"

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

def binance_req(base, path, params=None):
    if params is None:
        params = {}

    query, sig = sign_params(params)
    url = f"{base}{path}?{query}&signature={sig}"
    headers = {"X-MBX-APIKEY": API_KEY}

    r = requests.get(url, headers=headers)

    try:
        return r.json()
    except:
        return {}

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
# BALANCES
# ==========================================================

def spot_balances():
    data = binance_req(SPOT, "/api/v3/account")
    rows = []

    for b in data.get("balances", []):
        total = float(b["free"]) + float(b["locked"])
        if total == 0:
            continue

        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "SPOT",
            "asset": b["asset"],
            "free": b["free"],
            "locked": b["locked"],
            "id_externo": None,
            "motor_event_time_normalized": motor_time(None),
            "json_raw": json.dumps(b, ensure_ascii=False)
        })

    print_table("SPOT BALANCES", rows,
    ["exchange","cuenta_tipo","asset","free","locked","id_externo","motor_event_time_normalized","json_raw"])

def futures_balances():
    data = binance_req(FUT, "/fapi/v2/account")
    rows = []

    for a in data.get("assets", []):
        if float(a["walletBalance"]) == 0:
            continue

        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "FUTURES",
            "asset": a["asset"],
            "walletBalance": a["walletBalance"],
            "unrealizedProfit": a["unrealizedProfit"],
            "id_externo": None,
            "motor_event_time_normalized": motor_time(None),
            "json_raw": json.dumps(a, ensure_ascii=False)
        })

    print_table("FUTURES BALANCES", rows,
    ["exchange","cuenta_tipo","asset","walletBalance","unrealizedProfit","id_externo","motor_event_time_normalized","json_raw"])

# ==========================================================
# ORDERS
# ==========================================================

def spot_open_orders():
    data = binance_req(SPOT, "/api/v3/openOrders")
    rows = []

    for o in data:
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "SPOT",
            "symbol": o["symbol"],
            "side": o["side"],
            "price": o["price"],
            "origQty": o["origQty"],
            "status": o["status"],
            "id_externo": o["orderId"],
            "motor_event_time_normalized": motor_time(o.get("time")),
            "json_raw": json.dumps(o, ensure_ascii=False)
        })

    print_table("SPOT OPEN ORDERS", rows,
    ["exchange","cuenta_tipo","symbol","side","price","origQty","status","id_externo","motor_event_time_normalized","json_raw"])

def futures_open_orders():
    data = binance_req(FUT, "/fapi/v1/openOrders")
    rows = []

    for o in data:
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "FUTURES",
            "symbol": o["symbol"],
            "side": o["side"],
            "price": o["price"],
            "origQty": o["origQty"],
            "status": o["status"],
            "id_externo": o["orderId"],
            "motor_event_time_normalized": motor_time(o.get("time")),
            "json_raw": json.dumps(o, ensure_ascii=False)
        })

    print_table("FUTURES OPEN ORDERS", rows,
    ["exchange","cuenta_tipo","symbol","side","price","origQty","status","id_externo","motor_event_time_normalized","json_raw"])

# ==========================================================
# TRADES / INCOME
# ==========================================================

def spot_trades(symbol="BTCUSDT"):
    data = binance_req(SPOT, "/api/v3/myTrades", {"symbol": symbol, "limit": 20})
    rows = []

    for t in data:
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "SPOT",
            "symbol": t["symbol"],
            "price": t["price"],
            "qty": t["qty"],
            "commission": t["commission"],
            "id_externo": t["id"],
            "time": t["time"],
            "motor_event_time_normalized": motor_time(t["time"]),
            "json_raw": json.dumps(t, ensure_ascii=False)
        })

    print_table("SPOT TRADES", rows,
    ["exchange","cuenta_tipo","symbol","price","qty","commission","id_externo","time","motor_event_time_normalized","json_raw"])

def futures_income():
    data = binance_req(FUT, "/fapi/v1/income", {"limit": 100})
    rows = []

    for i in data:
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "FUTURES",
            "symbol": i.get("symbol"),
            "asset": i.get("asset"),
            "income": i.get("income"),
            "incomeType": i.get("incomeType"),
            "id_externo": i.get("tranId"),
            "time": i.get("time"),
            "motor_event_time_normalized": motor_time(i.get("time")),
            "json_raw": json.dumps(i, ensure_ascii=False)
        })

    print_table("FUTURES INCOME", rows,
    ["exchange","cuenta_tipo","symbol","asset","income","incomeType","id_externo","time","motor_event_time_normalized","json_raw"])

# ==========================================================
# EARN
# ==========================================================

def earn_flexible():
    data = binance_req(SPOT, "/sapi/v1/simple-earn/flexible/position")
    rows = []

    for p in data.get("rows", []):
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "EARN_FLEX",
            "asset": p.get("asset"),
            "amount": p.get("totalAmount"),
            "id_externo": p.get("productId"),
            "motor_event_time_normalized": motor_time(None),
            "json_raw": json.dumps(p, ensure_ascii=False)
        })

    print_table("EARN FLEXIBLE", rows,
    ["exchange","cuenta_tipo","asset","amount","id_externo","motor_event_time_normalized","json_raw"])

def earn_locked():
    data = binance_req(SPOT, "/sapi/v1/simple-earn/locked/position")
    rows = []

    for p in data.get("rows", []):
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "EARN_LOCKED",
            "asset": p.get("asset"),
            "amount": p.get("amount"),
            "id_externo": p.get("projectId"),
            "motor_event_time_normalized": motor_time(None),
            "json_raw": json.dumps(p, ensure_ascii=False)
        })

    print_table("EARN LOCKED", rows,
    ["exchange","cuenta_tipo","asset","amount","id_externo","motor_event_time_normalized","json_raw"])

# ==========================================================
# DIVIDENDS / DUST
# ==========================================================

def dividends():
    data = binance_req(SPOT, "/sapi/v1/asset/assetDividend", {"limit": 50})
    rows = []

    for d in data.get("rows", []):
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "DIVIDEND",
            "asset": d.get("asset"),
            "amount": d.get("amount"),
            "id_externo": d.get("tranId"),
            "time": d.get("divTime"),
            "motor_event_time_normalized": motor_time(d.get("divTime")),
            "json_raw": json.dumps(d, ensure_ascii=False)
        })

    print_table("DIVIDENDS", rows,
    ["exchange","cuenta_tipo","asset","amount","id_externo","time","motor_event_time_normalized","json_raw"])

def dust_log():
    data = binance_req(SPOT, "/sapi/v1/asset/dribblet")
    rows = []

    for d in data.get("userAssetDribblets", []):
        for det in d.get("userAssetDribbletDetails", []):
            rows.append({
                "exchange": EXCHANGE,
                "cuenta_tipo": "DUST",
                "asset": det.get("fromAsset"),
                "amount": det.get("amount"),
                "id_externo": det.get("transId"),
                "motor_event_time_normalized": motor_time(None),
                "json_raw": json.dumps(det, ensure_ascii=False)
            })

    print_table("DUST LOG", rows,
    ["exchange","cuenta_tipo","asset","amount","id_externo","motor_event_time_normalized","json_raw"])

# ==========================================================
# TRANSFERS / DEPOSITS / WITHDRAW / SNAPSHOT
# ==========================================================

def transfers():
    data = binance_req(SPOT, "/sapi/v1/futures/transfer", {"size": 50})
    rows = []

    for t in data.get("rows", []):
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "TRANSFER",
            "asset": t["asset"],
            "amount": t["amount"],
            "id_externo": t.get("tranId"),
            "time": t["timestamp"],
            "motor_event_time_normalized": motor_time(t["timestamp"]),
            "json_raw": json.dumps(t, ensure_ascii=False)
        })

    print_table("TRANSFERS", rows,
    ["exchange","cuenta_tipo","asset","amount","id_externo","time","motor_event_time_normalized","json_raw"])

def deposits():
    data = binance_req(SPOT, "/sapi/v1/capital/deposit/hisrec", {"limit": 50})
    rows = []

    for d in data:
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "DEPOSIT",
            "asset": d.get("coin"),
            "amount": d.get("amount"),
            "id_externo": d.get("txId"),
            "time": d.get("insertTime"),
            "motor_event_time_normalized": motor_time(d.get("insertTime")),
            "json_raw": json.dumps(d, ensure_ascii=False)
        })

    print_table("DEPOSITS", rows,
    ["exchange","cuenta_tipo","asset","amount","id_externo","time","motor_event_time_normalized","json_raw"])

def withdraws():
    data = binance_req(SPOT, "/sapi/v1/capital/withdraw/history", {"limit": 50})
    rows = []

    for w in data:
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "WITHDRAW",
            "asset": w.get("coin"),
            "amount": w.get("amount"),
            "id_externo": w.get("txId"),
            "time": w.get("applyTime"),
            "motor_event_time_normalized": motor_time(None),
            "json_raw": json.dumps(w, ensure_ascii=False)
        })

    print_table("WITHDRAWALS", rows,
    ["exchange","cuenta_tipo","asset","amount","id_externo","time","motor_event_time_normalized","json_raw"])

def snapshot():
    data = binance_req(SPOT, "/sapi/v1/accountSnapshot", {"type":"SPOT","limit":5})
    rows = []

    for s in data.get("snapshotVos", []):
        rows.append({
            "exchange": EXCHANGE,
            "cuenta_tipo": "SNAPSHOT",
            "type": s.get("type"),
            "updateTime": s.get("updateTime"),
            "motor_event_time_normalized": motor_time(s.get("updateTime")),
            "json_raw": json.dumps(s, ensure_ascii=False)
        })

    print_table("ACCOUNT SNAPSHOT", rows,
    ["exchange","cuenta_tipo","type","updateTime","motor_event_time_normalized","json_raw"])

# ==========================================================
# MAIN
# ==========================================================

def run():
    print("\n====================================================")
    print("BINANCE SCRIPT6 DEBUG MASTER v6.6")
    print("FULL ORIGINAL + JSON RAW EN TODO")
    print("====================================================")

    spot_balances()
    futures_balances()
    spot_open_orders()
    futures_open_orders()
    spot_trades()
    futures_income()
    earn_flexible()
    earn_locked()
    dividends()
    dust_log()
    transfers()
    deposits()
    withdraws()
    snapshot()

if __name__ == "__main__":
    run()
