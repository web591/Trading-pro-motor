import requests
import json
import os
from datetime import datetime, timedelta
from config import ALPHA_VANTAGE_KEY

DB_FILE = "db_fundamentales.json"

def cargar_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, 'r') as f: return json.load(f)
    return {}

def guardar_db(db):
    with open(DB_FILE, 'w') as f: json.dump(db, f, indent=4)

def obtener_detalles_alpha(ticker, forzar=False):
    db = cargar_db()
    ahora = datetime.now()

    # 1. Verificar si ya lo tenemos y si es reciente (menos de 30 días)
    if ticker in db and not forzar:
        fecha_upd = datetime.strptime(db[ticker]['last_updated'], '%Y-%m-%d')
        if ahora - fecha_upd < timedelta(days=30):
            print(f"✅ {ticker} recuperado de base de datos local.")
            return db[ticker]

    # 2. Si no está o es viejo, llamar a Alpha Vantage
    print(f"📡 Consultando Alpha Vantage para {ticker}...")
    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={ALPHA_VANTAGE_KEY}"
    
    try:
        r = requests.get(url, timeout=15).json()
        if "Note" in r:
            return {"error": "Límite de API alcanzado. Intenta mañana."}
        if not r or "Symbol" not in r:
            return {"error": "No hay datos fundamentales para este activo."}

        # Estructura limpia
        datos = {
            "name": r.get("Name"),
            "sector": r.get("Sector"),
            "industry": r.get("Industry"),
            "market_cap": r.get("MarketCapitalization"),
            "per": r.get("PERatio"),
            "dividend": r.get("DividendYield"),
            "description": r.get("Description"),
            "last_updated": ahora.strftime('%Y-%m-%d')
        }
        
        # 3. Guardar en DB para la próxima vez
        db[ticker] = datos
        guardar_db(db)
        return datos

    except Exception as e:
        return {"error": str(e)}

# --- LOGICA DE ACTUALIZADOR MENSUAL ---
def mantenimiento_mensual():
    db = cargar_db()
    for ticker in list(db.keys()):
        # Aquí llamaríamos a obtener_detalles_alpha(ticker, forzar=True)
        # pero con un time.sleep(15) entre cada uno para no morir por límites.
        pass