from binance.um_futures import UMFutures
from binance.cm_futures import CMFutures

API_KEY = "TU_KEY"
API_SECRET = "TU_SECRET"

print("======================================")
print("🟦 TEST UM FUTURES (USDT-M)")
print("======================================")

um = UMFutures(key=API_KEY, secret=API_SECRET)

um_symbols = ["BTCUSDT", "BTCUSDC"]

for symbol in um_symbols:
    try:
        orders = um.get_orders(symbol=symbol)

        abiertas = [o for o in orders if o["status"] in ["NEW", "PARTIALLY_FILLED"]]

        print(f"\n🔹 {symbol}")
        print("Total órdenes:", len(orders))
        print("Abiertas:", len(abiertas))
        print(abiertas)

    except Exception as e:
        print(f"[UM ERROR {symbol}] {e}")

print("\n======================================")
print("🟧 TEST CM FUTURES (COIN-M)")
print("======================================")

cm = CMFutures(key=API_KEY, secret=API_SECRET)

cm_symbols = ["ETCUSD_PERP", "BTCUSD_PERP"]

for symbol in cm_symbols:
    try:
        orders = cm.get_orders(symbol=symbol)

        abiertas = [o for o in orders if o["status"] in ["NEW", "PARTIALLY_FILLED"]]

        print(f"\n🔹 {symbol}")
        print("Total órdenes:", len(orders))
        print("Abiertas:", len(abiertas))
        print(abiertas)

    except Exception as e:
        print(f"[CM ERROR {symbol}] {e}")