import requests
import json

def investigar_bingx():
    print("🔍 INVESTIGANDO MERCADOS DE BINGX...\n")
    
    # 1. Escaneo de Perpetuos (V2)
    url_perp = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
    # 2. Escaneo de Standard (V1)
    url_std = "https://open-api.bingx.com/openApi/market/v1/tickers"

    filtros = ["GOLD", "XAU", "EUR", "USD", "DAX", "GER", "WTI", "OIL"]

    # --- BUSCAR EN PERPETUOS ---
    try:
        r_perp = requests.get(url_perp).json()
        print("--- [ MERCADO PERPETUAL ] ---")
        if r_perp.get('code') == 0:
            for contrato in r_perp['data']:
                sym = contrato['symbol']
                if any(f in sym for f in filtros):
                    print(f"✅ Encontrado en PERP: {sym}")
        else:
            print("❌ Error accediendo a Perpetuos")
    except Exception as e:
        print(f"❌ Error de red en Perp: {e}")

    print("\n" + "-"*30 + "\n")

    # --- BUSCAR EN STANDARD ---
    try:
        r_std = requests.get(url_std).json()
        print("--- [ MERCADO STANDARD ] ---")
        if r_std.get('code') == 0:
            for ticker in r_std['data']['tickers']:
                sym = ticker['symbol']
                if any(f in sym for f in filtros):
                    print(f"✅ Encontrado en STANDARD: {sym}")
        else:
            print(f"❌ Error en Standard: {r_std.get('msg')}")
    except Exception as e:
        print(f"❌ Error de red en Standard: {e}")

if __name__ == "__main__":
    investigar_bingx()