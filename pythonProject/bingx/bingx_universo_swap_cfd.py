# ==========================================================
# BINGX UNIVERSO CFD SWAP
# Version 1.0 CATALOGO MAESTRO
# ==========================================================

import requests
import pandas as pd

def construir_catalogo_swap():

    print("\n📡 DESCARGANDO UNIVERSO CFD SWAP BINGX...\n")

    url = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"

    r = requests.get(url, timeout=15).json()

    if r.get("code") != 0:
        print("❌ ERROR API")
        return

    contratos = r["data"]

    filas = []

    for c in contratos:

        filas.append({

            "symbol"       : c.get("symbol"),
            "underlying"   : c.get("underlying"),
            "asset"        : c.get("asset"),
            "displayName"  : c.get("displayName"),
            "marginAsset"  : c.get("marginAsset"),
            "quoteAsset"   : c.get("quoteAsset"),
            "contractType" : c.get("contractType"),
            "status"       : c.get("status"),
            "pricePrecision": c.get("pricePrecision")

        })

    df = pd.DataFrame(filas)

    df["underlying_limpio"] = (
        df["underlying"]
        .astype(str)
        .str.replace("/","", regex=False)
        .str.upper()
    )

    df["asset_limpio"] = df["asset"].astype(str).str.upper()

    df.to_csv("BINGX_SWAP_CFD_CATALOGO.csv", index=False)

    print("\n✅ CATALOGO GUARDADO:")
    print("BINGX_SWAP_CFD_CATALOGO.csv")
    print(f"TOTAL FILAS: {len(df)}")

# ==========================================================
# MAIN
# ==========================================================

if __name__ == "__main__":
    construir_catalogo_swap()
