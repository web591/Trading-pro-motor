import requests
import yfinance as yf

# ==============================================================================
# 🎛️ PANEL DE CONTROL
# ==============================================================================
TICKER_A_PROBAR = "IBM" # <--- CAMBIA POR: BTC, GOLD, WTI, AAPL, GOOGL
FINNHUB_KEY = "" 

class MaestroV1_1:
    def __init__(self):
        # Mapeo BINGX PERP (Ajustado con tus correcciones: NCSK para Stocks)
        self.BINGX_MAP = {
            "GOLD": "NCCOGOLD2USD-USDT", "WTI": "NCCOOILWTI2USD-USDT",
            "EURUSD": "NCFXEUR2USD-USDT", "GBPUSD": "NCFXGBP2USD-USDT",
            "AAPL": "NCSKAAPL2USD-USDT", "GOOGL": "NCSKGOOGL2USD-USDT", # <--- Corregido
            "TSLA": "NCSKTSLA2USD-USDT", "DAX": "NCCOGER402EUR-USDT"
        }
        # Mapeo FINNHUB
        self.FINNHUB_MAP = {
            "GOLD": "OANDA:XAU_USD", "WTI": "OANDA:WTICO_USD",
            "EURUSD": "OANDA:EUR_USD", "BTC": "BINANCE:BTCUSDT"
        }
        # Mapeo YAHOO
        self.YAHOO_MAP = {
            "GOLD": "GC=F", "WTI": "CL=F", "EURUSD": "EURUSD=X", "BTC": "BTC-USD"
        }

    def ejecutar(self, tk):
        tk = tk.upper()
        print(f"\n💎 CÓDIGO MAESTRO V1.1: REPORTE FINAL [{tk}]")
        print("=" * 150)
        print(f"{'PLATAFORMA':<20} | {'TICKER REAL':<22} | {'PRECIO':<12} | {'VOLUMEN':<15} | {'URL / ESTADO'}")
        print("-" * 150)

        # 1. 🟠 BINANCE (Corregido Coin-M)
        s_bin_t = tk.replace("-", "") + "USDT"
        s_bin_perp = f"{tk}USD_PERP" # <--- Tu corrección: Sin la 'T'
        
        bn_endpoints = [
            ("binance_spot", f"https://api.binance.com/api/v3/ticker/price?symbol={s_bin_t}"),
            ("binance_usdt_f", f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={s_bin_t}"),
            ("binance_coin_f", f"https://dapi.binance.com/dapi/v1/ticker/price?symbol={s_bin_perp}")
        ]
        for label, url in bn_endpoints:
            try:
                r = requests.get(url, timeout=2).json()
                p = r.get('price', r.get('lastPrice', 'N/A'))
                val = f"${float(p):.2f}" if p != 'N/A' else "N/A"
                print(f"{label:<20} | {url.split('=')[-1]:<22} | {val:<12} | {'-':<15} | {url}")
            except: print(f"{label:<20} | {'N/A':<22} | {'N/A':<12} | {'-':<15} | {url}")

        # 2. 🔵 BINGX (Spot y Perp sin exclusiones)
        # Intentamos Spot con ticker normal o mapeado
        s_bx_spot = f"{tk}-USDT"
        url_s = f"https://open-api.bingx.com/openApi/spot/v1/ticker/24hr?symbol={s_bx_spot}"
        try:
            r = requests.get(url_s, timeout=2).json()
            if r['code'] == 0 and r['data']:
                d = r['data'][0]
                print(f"{'bingx_spot':<20} | {s_bx_spot:<22} | ${float(d['lastPrice']):<11.2f} | {d['volume']:<15} | {url_s}")
            else: print(f"{'bingx_spot':<20} | {s_bx_spot:<22} | {'N/A':<12} | {'-':<15} | {url_s}")
        except: pass

        # Perpetuo (Usa el mapa técnico NCSK/NCCO)
        s_bx_p = self.BINGX_MAP.get(tk, f"{tk}-USDT")
        url_p = f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={s_bx_p}"
        try:
            r = requests.get(url_p, timeout=2).json()
            p = f"${float(r['data']['lastPrice']):.2f}" if r['code'] == 0 else "N/A"
            v = r['data'].get('volume', '-') if r['code'] == 0 else "-"
            print(f"{'bingx_perp':<20} | {s_bx_p:<22} | {p:<12} | {v:<15} | {url_p}")
        except: pass

        # 3. 🟣 YAHOO & 🟢 FINNHUB (Ficha Técnica)
        y_tk = self.YAHOO_MAP.get(tk, tk)
        f_tk = self.FINNHUB_MAP.get(tk, tk)
        y_info = {"name": "N/A", "cap": "N/A", "price": "N/A"}
        
        # Yahoo Data
        try:
            yt = yf.Ticker(y_tk)
            y_info['price'] = f"${yt.fast_info['last_price']:.2f}"
            y_info['name'] = yt.info.get('shortName', tk)
            y_info['cap'] = yt.info.get('marketCap', 'N/A')
            print(f"{'yahoo_finance':<20} | {y_tk:<22} | {y_info['price']:<12} | {'-':<15} | https://query1.finance.yahoo.com/v8/finance/chart/{y_tk}")
        except: pass

        # Finnhub Data
        fh_info = {"name": "N/A", "cap": "N/A", "price": "N/A"}
        try:
            rq = requests.get(f"https://finnhub.io/api/v1/quote?symbol={f_tk}&token={FINNHUB_KEY}").json()
            if 'c' in rq and rq['c'] != 0:
                fh_info['price'] = f"${rq['c']:.2f}"
            
            # Perfil para Ficha
            ep = "crypto/profile" if "BINANCE" in f_tk else "stock/profile2"
            rp = requests.get(f"https://finnhub.io/api/v1/{ep}?symbol={f_tk}&token={FINNHUB_KEY}").json()
            fh_info['name'] = rp.get('name', rp.get('longName', 'N/A'))
            fh_info['cap'] = rp.get('marketCapitalization', rp.get('marketCap', 'N/A'))
            print(f"{'finnhub_api':<20} | {f_tk:<22} | {fh_info['price']:<12} | {'(Ficha)':<15} | URL: HIDDEN_TOKEN")
        except: pass

        print("-" * 150)
        print(f"📋 FICHA TÉCNICA CONSOLIDADA: {fh_info['name'] if fh_info['name'] != 'N/A' else y_info['name']}")
        print(f"   🔹 Capitalización: {fh_info['cap'] if fh_info['cap'] != 'N/A' else y_info['cap']}")
        print(f"   🔹 Origen de Datos: Finnhub API + Yahoo Respaldo")
        print("=" * 150)

if __name__ == "__main__":
    MaestroV1_1().ejecutar(TICKER_A_PROBAR)