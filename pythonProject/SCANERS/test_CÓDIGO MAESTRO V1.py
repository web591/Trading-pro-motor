import requests
import yfinance as yf
import time

# ==============================================================================
# 🎛️ PANEL DE CONTROL (USER INPUT)
# ==============================================================================
TICKER_A_PROBAR = "GOOGL"  # <--- CAMBIA POR: BTC, GOLD, WTI, AAPL, GOOGL
FINNHUB_KEY = ""  # <--- PEGA TU KEY AQUÍ

class MaestroConectividad:
    def __init__(self):
        # 1. DICCIONARIO BINGX (Traducción a Contratos V2 y Spot)
        self.BINGX_MAP = {
            "GOLD": "NCCOGOLD2USD-USDT", 
            "WTI": "NCCOOILWTI2USD-USDT",
            "EURUSD": "NCFXEUR2USD-USDT",
            "GBPUSD": "NCFXGBP2USD-USDT",
            "DAX": "NCCOGER402EUR-USDT",
            "AAPL": "NCSKAAPL2USD-USDT",
            "TSLA": "NCSKTSLA2USD-USDT"
        }

        # 2. DICCIONARIO FINNHUB (Prefijos de Proveedor)
        # Acciones no llevan prefijo. Cripto = BINANCE:. Forex/Metales = OANDA:
        self.FINNHUB_MAP = {
            "GOLD": "OANDA:XAU_USD",
            "WTI": "OANDA:WTICO_USD",
            "EURUSD": "OANDA:EUR_USD",
            "GBPUSD": "OANDA:GBP_USD",
            "BTC": "BINANCE:BTCUSDT",
            "ETH": "BINANCE:ETHUSDT",
            "DAX": "INDEX:GDAXI" 
        }

        # 3. DICCIONARIO YAHOO (Símbolos Financieros Clásicos)
        self.YAHOO_MAP = {
            "GOLD": "GC=F",
            "WTI": "CL=F",
            "EURUSD": "EURUSD=X",
            "GBPUSD": "GBPUSD=X",
            "BTC": "BTC-USD",
            "DAX": "^GDAXI"
        }

    def es_cripto(self, tk):
        # Lógica simple para saber si buscamos en Spot de BingX
        return tk in ["BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "DOGE"]

    def ejecutar_peritaje(self, tk):
        tk = tk.upper()
        print(f"\n💎 CÓDIGO MAESTRO V1: REPORTE DE IDENTIDAD PARA [{tk}]")
        print("=" * 145)
        print(f"{'PLATAFORMA':<20} | {'TICKER REAL':<22} | {'PRECIO':<12} | {'VOLUMEN':<15} | {'URL / ESTADO'}")
        print("-" * 145)

        # -----------------------------------------------------------
        # 🟠 BLOQUE 1: BINANCE (Referencia Líder Cripto)
        # -----------------------------------------------------------
        s_bin = tk.replace("-", "") + "USDT"
        endpoints = [
            ("binance_spot", f"https://api.binance.com/api/v3/ticker/price?symbol={s_bin}"),
            ("binance_usdt_fut", f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={s_bin}"),
            ("binance_coin_fut", f"https://dapi.binance.com/dapi/v1/ticker/price?symbol={tk}USD_PERP")
        ]
        for label, url in endpoints:
            try:
                r = requests.get(url, timeout=2).json()
                price = r.get('price', r.get('lastPrice', 'N/A')) # Binance varía la key entre endpoints
                if price != "N/A": price = f"${float(price):.2f}"
                print(f"{label:<20} | {url.split('=')[-1]:<22} | {price:<12} | {'-':<15} | {url}")
            except: 
                print(f"{label:<20} | {'N/A':<22} | {'N/A':<12} | {'-':<15} | {url}")

        # -----------------------------------------------------------
        # 🔵 BLOQUE 2: BINGX (Spot vs Perpetuos)
        # -----------------------------------------------------------
        # A) BINGX SPOT (Solo Cripto)
        if self.es_cripto(tk):
            s_bx_s = f"{tk}-USDT"
            url_bx_s = f"https://open-api.bingx.com/openApi/spot/v1/ticker/24hr?symbol={s_bx_s}"
            try:
                r = requests.get(url_bx_s, timeout=2).json()
                if r['code'] == 0 and r['data']:
                    d = r['data'][0]
                    print(f"{'bingx_spot':<20} | {s_bx_s:<22} | ${float(d['lastPrice']):<11.2f} | {d['volume']:<15} | {url_bx_s}")
                else: print(f"{'bingx_spot':<20} | {s_bx_s:<22} | {'N/A':<12} | {'-':<15} | {url_bx_s}")
            except: pass
        else:
             print(f"{'bingx_spot':<20} | {'(No Aplica)':<22} | {'-':<12} | {'-':<15} | ACTIVO TRADICIONAL (NO EXISTE EN SPOT)")

        # B) BINGX PERPETUAL (Todo: Cripto + Tradicionales)
        s_bx_p = self.BINGX_MAP.get(tk, f"{tk}-USDT")
        url_bx_p = f"https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol={s_bx_p}"
        try:
            r = requests.get(url_bx_p, timeout=2).json()
            if r['code'] == 0:
                d = r['data']
                print(f"{'bingx_perp':<20} | {s_bx_p:<22} | ${float(d['lastPrice']):<11.2f} | {d['volume']:<15} | {url_bx_p}")
            else:
                 print(f"{'bingx_perp':<20} | {s_bx_p:<22} | {'N/A':<12} | {'-':<15} | {url_bx_p}")
        except: pass

        # -----------------------------------------------------------
        # 🟣 BLOQUE 3: YAHOO FINANCE (Respaldo)
        # -----------------------------------------------------------
        y_tk = self.YAHOO_MAP.get(tk, tk)
        url_y = f"https://query1.finance.yahoo.com/v8/finance/chart/{y_tk}"
        y_data = {"name": "N/A", "cap": "N/A"}
        try:
            yt = yf.Ticker(y_tk)
            hist = yt.history(period="1d")
            p_y = f"${hist['Close'].iloc[-1]:.2f}" if not hist.empty else "N/A"
            v_y = f"{hist['Volume'].iloc[-1]}" if not hist.empty else "-"
            # Guardamos info para ficha técnica
            y_data['name'] = yt.info.get('shortName', yt.info.get('longName', 'N/A'))
            y_data['cap'] = yt.info.get('marketCap', 'N/A')
            
            print(f"{'yahoo_finance':<20} | {y_tk:<22} | {p_y:<12} | {v_y:<15} | {url_y}")
        except: 
            print(f"{'yahoo_finance':<20} | {y_tk:<22} | {'N/A':<12} | {'-':<15} | {url_y}")

        # -----------------------------------------------------------
        # 🟢 BLOQUE 4: FINNHUB (Autoridad de Metadatos)
        # -----------------------------------------------------------
        fh_sym = self.FINNHUB_MAP.get(tk, tk)
        # URL 1: Quote (Precio)
        url_fh_q = f"https://finnhub.io/api/v1/quote?symbol={fh_sym}&token={FINNHUB_KEY}"
        # URL 2: Profile (Info) - Detectar si es Crypto o Stock
        is_crypto_fh = "BINANCE" in fh_sym
        endpoint_p = "crypto/profile" if is_crypto_fh else "stock/profile2"
        url_fh_p = f"https://finnhub.io/api/v1/{endpoint_p}?symbol={fh_sym}&token={FINNHUB_KEY}"
        
        # Obtener Precio
        try:
            r = requests.get(url_fh_q).json()
            if 'c' in r and r['c'] != 0:
                 print(f"{'finnhub_api':<20} | {fh_sym:<22} | ${r['c']:<12.2f} | {'(Ver Ficha)':<15} | {url_fh_q.split('token=')[0]}token=HIDDEN")
            else:
                 print(f"{'finnhub_api':<20} | {fh_sym:<22} | {'N/A':<12} | {'-':<15} | {url_fh_q.split('token=')[0]}token=HIDDEN")
        except: pass

        # Obtener Ficha Técnica
        fh_meta = {"name": "N/A", "cap": "N/A"}
        try:
            r_p = requests.get(url_fh_p).json()
            if r_p:
                fh_meta['name'] = r_p.get('longName', r_p.get('name', 'N/A'))
                fh_meta['cap']  = r_p.get('marketCap', r_p.get('marketCapitalization', 'N/A'))
        except: pass

        # -----------------------------------------------------------
        # 📝 FICHA TÉCNICA CONSOLIDADA
        # -----------------------------------------------------------
        # Prioridad: Finnhub > Yahoo > "N/A"
        final_name = fh_meta['name'] if fh_meta['name'] != "N/A" else y_data['name']
        final_cap  = fh_meta['cap'] if fh_meta['cap'] != "N/A" else y_data['cap']
        
        print("-" * 145)
        print("📋 FICHA TÉCNICA DEL ACTIVO")
        print(f"   🔹 Nombre Oficial:   {final_name}")
        print(f"   🔹 Capitalización:   {final_cap}")
        print(f"   🔹 Fuente Principal: {'Finnhub' if fh_meta['name'] != 'N/A' else 'Yahoo Finance'}")
        print("=" * 145)

if __name__ == "__main__":
    motor = MaestroConectividad()
    motor.ejecutar_peritaje(TICKER_A_PROBAR)