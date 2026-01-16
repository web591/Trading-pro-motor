import requests


def scannear_bingx_rebeldes():
    print("🔍 INTERROGANDO A BINGX... buscando nombres reales de contratos.")
    url_perp = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"

    try:
        r_perp = requests.get(url_perp).json()
        if r_perp.get('code') == 0:
            all_symbols = [c['symbol'] for c in r_perp['data']]

            # Filtros de búsqueda basados en lo que operamos
            filtros = {
                "FOREX": ["EUR", "USD", "GBP"],
                "INDICES": ["DAX", "GER", "30", "DE"],
                "PETROLEO": ["WTI", "OIL", "CRUDE"],
                "TESLA": ["TSLA", "TESLA"]
            }

            for categoria, claves in filtros.items():
                encontrados = [s for s in all_symbols if any(k in s for k in claves)]
                print(f"\n📌 {categoria}:")
                if encontrados:
                    print(f"   Posibles: {encontrados}")
                else:
                    print(f"   ❌ No se encontró nada para {categoria}")
        else:
            print(f"❌ Error API: {r_perp.get('msg')}")

    except Exception as e:
        print(f"❌ Error de red: {e}")


if __name__ == "__main__":
    scannear_bingx_rebeldes()