import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests
from datetime import datetime, timezone, timedelta
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import config

# ==========================================================
# 🛡️ CAPA 1: SEGURIDAD, RADAR Y NORMALIZACIÓN (GEMA CORE)
# ==========================================================
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

def descifrar_dato(t, m):
    try:
        if not t: return None
        raw = base64.b64decode(t.strip())
        partes = raw.rsplit(b":::", 1) if b":::" in raw else raw.rsplit(b"::", 1)
        if len(partes) != 2: return None
        data, iv = partes
        key_hash = sha256(m.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

def format_date_sql(ts_ms):
    """Formato ISO YYYY-MM-DD HH:MM:SS para PHPMyAdmin"""
    return datetime.fromtimestamp(int(ts_ms)/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

def normalizar_categoria(cat_raw):
    c = str(cat_raw).upper().strip()
    mapa = {
        "REALIZED_PNL": "PNL", "FUNDING_FEE": "FUNDING", "COMMISSION": "FEE",
        "SIMPLE_EARN_INTEREST": "EARN", "STAKING_REWARDS": "EARN", 
        "POOL_DISTRIBUTION": "POOL", "DISTRIBUTION": "BONUS", "AIRDROP": "BONUS",
        "DUST_CONVERT": "DUST", "CONVERT": "CONVERT", "LIQUIDATION": "LIQUIDATION",
        "TRADE": "TRADE", "DEPOSIT": "DEPOSIT", "WITHDRAW": "WITHDRAW",
        "INTERNAL_TRANSFER": "TRANSFER_INTERNAL", "AUTO_EXCHANGE": "CONVERT"
    }
    return mapa.get(c, c)

def ejecutar_radar_gema(db, uid, ticker, ctx):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT id FROM sys_simbolos_buscados WHERE user_id=%s AND ticker=%s AND status NOT IN ('ignorado','confirmado')", (uid, ticker))
    if not cursor.fetchone():
        cursor.execute("INSERT INTO sys_simbolos_buscados (user_id, ticker, status, info) VALUES (%s, %s, 'pendiente', %s)", (uid, ticker, ctx))
        db.commit()

# ==========================================================
# 📊 CAPA 2: INYECTORES DUALES (OPERATIVO + LEDGER)
# ==========================================================

def inyector_detalle_trade(db, uid, exch, tipo, id_ext, symbol, lado, precio, cantidad, fecha, comision=0, asset_fee=""):
    cursor = db.cursor()
    sql = """INSERT IGNORE INTO detalle_trades 
             (user_id, exchange, tipo_producto, id_externo_ref, symbol, lado, precio_ejecucion, cantidad_ejecutada, comision_valor, comision_asset, fecha_utc)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cursor.execute(sql, (uid, exch.upper(), tipo.upper(), id_ext, symbol, lado, precio, cantidad, comision, asset_fee, fecha))

def inyector_ledger_global(db, uid, exch, cuenta, cat_raw, asset, monto, fecha, id_ext, desc=""):
    if float(monto) == 0: return
    cat = normalizar_categoria(cat_raw)
    cursor = db.cursor()
    sql = """INSERT IGNORE INTO transacciones_globales 
             (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, fecha_utc, descripcion)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cursor.execute(sql, (id_ext, uid, exch.upper(), cuenta.upper(), cat, asset.upper(), float(monto), fecha, desc))
    db.commit()

def registrar_saldo_snapshot(db, uid, exch, asset, total, cuenta_tipo):
    cursor = db.cursor()
    sql = "REPLACE INTO sys_saldos_usuarios (user_id, exchange, asset, saldo_total, cuenta_tipo, last_update) VALUES (%s, %s, %s, %s, %s, NOW())"
    cursor.execute(sql, (uid, exch.upper(), asset.upper(), float(total), cuenta_tipo.upper()))
    db.commit()

# ==========================================================
# 🟨 CAPA 3: BINANCE - AUDITORÍA 360°
# ==========================================================

def binance_sync_total(db, uid, k, s):
    client = Client(k, s)
    
    # 1. SALDOS Y RADAR (Spot + Futures + Earn)
    acc = client.get_account()
    for b in acc['balances']:
        total = float(b['free']) + float(b['locked'])
        if total > 0.00000001:
            registrar_saldo_snapshot(db, uid, "BINANCE", b['asset'], total, "SPOT")
            ejecutar_radar_gema(db, uid, b['asset'], "Binance Spot Balance")
    
    f_acc = client.futures_account()
    for f in f_acc['assets']:
        if float(f['walletBalance']) > 0:
            registrar_saldo_snapshot(db, uid, "BINANCE", f['asset'], f['walletBalance'], "FUTURES")

    # 2. TRADES SPOT + FEES + DETALLE (Paginado)
    for b in acc['balances']:
        asset = b['asset']
        if (float(b['free']) + float(b['locked'])) > 0 and asset not in ['USDT', 'FDUSD', 'BNB']:
            try:
                trades = client.get_my_trades(symbol=f"{asset}USDT", limit=1000)
                for t in trades:
                    id_ext = f"BN-SP-{t['id']}"
                    fecha = format_date_sql(t['time'])
                    inyector_detalle_trade(db, uid, 'BINANCE', 'SPOT', id_ext, t['symbol'], 
                                           ('BUY' if t['isBuyer'] else 'SELL'), t['price'], t['qty'], fecha, t['commission'], t['commissionAsset'])
                    
                    monto_neto = float(t['qty']) if t['isBuyer'] else -float(t['qty'])
                    inyector_ledger_global(db, uid, 'BINANCE', 'SPOT', 'TRADE', asset, monto_neto, fecha, id_ext)
                    
                    if float(t['commission']) > 0:
                        inyector_ledger_global(db, uid, 'BINANCE', 'SPOT', 'FEE', t['commissionAsset'], -float(t['commission']), fecha, f"FEE-{id_ext}")
            except: continue

    # 3. EARN, POOL & DISTRIBUTIONS (El flujo pasivo)
    try:
        # Simple Earn Interest
        for e in client.get_simple_earn_interest_history(limit=100)['rows']:
            inyector_ledger_global(db, uid, "BINANCE", "EARN", "EARN", e['asset'], e['interest'], format_date_sql(e['time']), f"BN-EARN-{e['id']}")
        
        # Airdrops / Staking / Pool Distribution
        for d in client.get_asset_distribution_history(limit=100)['rows']:
            inyector_ledger_global(db, uid, "BINANCE", "SPOT", "BONUS", d['asset'], d['amount'], format_date_sql(d['divTime']), f"BN-DIST-{d['divTime']}", d['enName'])
    except: pass

    # 4. DUST (Migajas a BNB)
    dust = client.get_dust_log()
    if 'userAssetDribblets' in dust:
        for entry in dust['userAssetDribblets']:
            fecha = format_date_sql(entry['operateTime'])
            for d in entry['userAssetDribbletDetails']:
                id_e = f"BN-DUST-{d['transId']}"
                inyector_ledger_global(db, uid, 'BINANCE', 'SPOT', 'DUST', d['fromAsset'], -float(d['amount']), fecha, f"{id_e}-OUT")
                inyector_ledger_global(db, uid, 'BINANCE', 'SPOT', 'DUST', 'BNB', d['transferAmount'], fecha, f"{id_e}-IN")

    # 5. FUTURES INCOME (PNL, Funding, Fees, Liquidaciones)
    income = client.futures_income_history(limit=1000)
    for i in income:
        fecha = format_date_sql(i['time'])
        inyector_ledger_global(db, uid, 'BINANCE', 'FUTURES', i['incomeType'], i['asset'], i['income'], fecha, f"BN-FUT-{i['tranId']}")

# ==========================================================
# 🟦 CAPA 4: BINGX - AUDITORÍA DE DERIVADOS Y WALLET
# ==========================================================

def bingx_sync_total(db, uid, ak, as_):
    def bx_req(path, params=None):
        params = params or {}; params["timestamp"] = int(time.time() * 1000)
        qs = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(as_.encode(), qs.encode(), hashlib.sha256).hexdigest()
        return requests.get(f"https://open-api.bingx.com{path}?{qs}&signature={sig}", headers={"X-BX-APIKEY": ak}).json()

    # 1. SALDOS BINGX
    r_s = bx_req("/openApi/spot/v1/account/balance")
    for b in r_s.get("data", {}).get("balances", []):
        registrar_saldo_snapshot(db, uid, "BINGX", b['asset'], float(b['free'])+float(b['locked']), "SPOT")
    
    # 2. SWAP INCOME (PNL + Funding)
    r_i = bx_req("/openApi/swap/v2/user/income")
    for i in r_i.get("data", []):
        inyector_ledger_global(db, uid, "BINGX", "FUTURES", i['incomeType'], i['asset'], i['income'], format_date_sql(i['time']), f"BX-INC-{i['tranId']}")

# ==========================================================
# 🚀 CAPA 5: CONCILIACIÓN FINAL Y LOOP
# ==========================================================

def conciliacion_maestra(db, uid):
    cursor = db.cursor(dictionary=True)
    # Comprobación de integridad: Snapshot (Real) vs Ledger (Calculado)
    sql = """
    SELECT s.asset, s.saldo_total, IFNULL(SUM(l.monto_neto),0) as total_ledger,
    (s.saldo_total - IFNULL(SUM(l.monto_neto),0)) as diferencia
    FROM sys_saldos_usuarios s
    LEFT JOIN transacciones_globales l ON s.user_id = l.user_id AND s.asset = l.asset
    WHERE s.user_id = %s GROUP BY s.asset
    """
    cursor.execute(sql, (uid,))
    # Esta tabla permite generar el reporte CSV de discrepancias
    for row in cursor.fetchall():
        if abs(row['diferencia']) > 0.00001:
            pass # Aquí puedes insertar en una tabla de alertas sys_discrepancias

def iniciar_motor():
    print("💎 GEMA MOTOR v4.5.0 - UNIFICADO E INSTITUCIONAL")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG)
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status = 'active'")
            for u in cursor.fetchall():
                k, s = descifrar_dato(u['api_key'], MASTER_KEY), descifrar_dato(u['api_secret'], MASTER_KEY)
                broker = u['broker_name'].upper()
                if broker == "BINANCE": binance_sync_total(db, u['user_id'], k, s)
                elif broker == "BINGX": bingx_sync_total(db, u['user_id'], k, s)
                conciliacion_maestra(db, u['user_id'])
            db.close()
        except Exception as e: print(f" [CRITICAL] {e}")
        time.sleep(60)

if __name__ == "__main__": iniciar_motor()