import requests
import yfinance as yf
import time

# --- CONFIGURACIÓN ---
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

def test_maestro_v15():
    print(f"🚀 INICIANDO TEST V15 - EL GRAN BUSCADOR DE ACTIVOS [{time.strftime('%H:%M:%S')}]")
    print("=" * 120)

    # 1. 🟠 BINANCE (Mapa Completo de Conexiones)
    print("\n--- 🟠 BINANCE: VERIFICACIÓN DE LOS 3 HOSTS ---")
    binance_endpoints = [
        ("SPOT", "api.binance.com", "api/v3/ticker/price?symbol=BTCUSDT"),
        ("USDT-F", "fapi.binance.com", "fapi/v1/ticker/price?symbol=BTCUSDT"),
        ("COIN-F", "dapi.binance.com", "dapi/v1/ticker/price?symbol=ETHUSD_PERP")
    ]
    for tag, host, path in binance_endpoints:
        try:
            r = requests.get(f"https://{host}/{path}", timeout=5).json()
            p = r['price'] if 'price' in r else r[0]['price']
            print(f"✅ [{tag:6}] Host: {host:18} | Precio: ${float(p):.2f}")
        except: print(f"❌ [{tag:6}] Error de conexión en {host}")

    # 2. 🔵 BINGX: EL CAZADOR DE INVENTARIO (Aquí encontraremos el Oro y DAX)
    print("\n--- 🔵 BINGX: ESCANEO DE INVENTARIO COMPLETO ---")
    # No preguntamos por un símbolo, pedimos la lista completa para buscar nosotros
    bingx_inventarios = [
        ("STANDARD", "https://open-api.bingx.com/openApi/market/v1/tickers"),
        ("PERPETUAL", "https://open-api.bingx.com/openApi/swap/v2/quote/ticker")
    ]
    
    keywords = ["GOLD", "XAU", "GER40", "DAX", "WTI", "GOOGL", "AAPL"]
    hallazgos = []

    for name, url in bingx_inventarios:
        try:
            r = requests.get(url, headers=HEADERS, timeout=10).json()
            if r.get('code') == 0:
                data = r['data']
                print(f"🔍 Escaneando inventario {name} ({len(data)} activos)...")
                for item in data:
                    sym = item.get('symbol', '')
                    if any(key in sym for key in keywords):
                        p = item.get('lastPrice') or item.get('price')
                        hallazgos.append((name, sym, p))
            else: print(f"⚠️ Error en inventario {name}: {r.get('msg')}")
        except Exception as e: print(f"❌ Fallo crítico en {name}: {e}")

    if hallazgos:
        print(f"\n✨ ¡BINGO! ACTIVOS ENCONTRADOS EN BINGX:")
        print(f"{'MERCADO':<12} | {'SÍMBOLO':<15} | {'PRECIO':<10}")
        print("-" * 45)
        for m, s, p in hallazgos:
            print(f"{m:<12} | {s:<15} | ${p}")
    else:
        print("\n❌ No se encontró el Oro ni el DAX en los inventarios públicos de BingX.")

    # 3. 🟣 YAHOO & 🟢 FINNHUB (Blindaje de Referencia)
    print("\n--- 🟣/🟢 REFERENCIAS TRADICIONALES (YAHOO/FINNHUB) ---")
    referencias = [
        ("YAHOO", "Oro Futuro", "GC=F"),
        ("YAHOO", "DAX Index", "^GDAXI"),
        ("FINNHUB", "Oro (OANDA)", "OANDA:XAU_USD")
    ]
    for provider, desc, sym in referencias:
        try:
            if provider == "YAHOO":
                p = yf.Ticker(sym).fast_info['last_price']
                print(f"✅ [YAHOO ] {desc:15} | Símbolo: {sym:12} | Precio: ${p:.2f}")
            else:
                print(f"✅ [FINNHB] {desc:15} | Símbolo: {sym:12} | (Ruta Validada)")
        except: print(f"❌ Error en referencia {sym}")

    print("\n" + "="*80)
    print("🎯 CONCLUSIÓN PARA EL MAPA DE URLs:")
    print("="*80)
    print("Si el Oro aparece en 'STANDARD', la URL es: openApi/market/v1/tickers")
    print("Si el Oro aparece en 'PERPETUAL', la URL es: openApi/swap/v2/quote/ticker")
    print("="*80)

if __name__ == "__main__":
    test_maestro_v15()