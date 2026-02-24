import mysql.connector
from binance.client import Client
import time, os, base64, hmac, hashlib, requests
from datetime import datetime, timezone
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from hashlib import sha256
import config

# ==========================================================
# 🛡️ SEGURIDAD Y NORMALIZACIÓN (ALINEADO CON IMPORTADORES)
# ==========================================================
MASTER_KEY = os.getenv('APP_ENCRYPTION_KEY') or getattr(config, 'ENCRYPTION_KEY', None)

def descifrar_dato(t, m):
    try:
        if not t: return None
        raw = base64.b64decode(t.strip())
        partes = raw.rsplit(b":::", 1) if b":::" in raw else raw.rsplit(b"::", 1)
        data, iv = partes; key_hash = sha256(m.encode()).digest()
        cipher = AES.new(key_hash, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(data), AES.block_size).decode().strip()
    except: return None

def format_date_sql(ts_ms):
    return datetime.fromtimestamp(int(ts_ms)/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

# ==========================================================
# 🧠 LÓGICA DE CATEGORÍAS (ESPEJO DE binance_carga_consolidada.py)
# ==========================================================
# --- PASO A: ACTUALIZA LA FUNCIÓN DE CATEGORÍAS ---
def normalizar_categoria_total(op_raw):
    op = str(op_raw).upper().strip()
    if any(x in op for x in ['LAUNCHPOOL', 'DISTRIBUTION', 'AIRDROP', 'DIVIDEND']): return "AIRDROP"
    if any(x in op for x in ['EARN', 'SAVINGS', 'STAKING', 'INTEREST']): return "INTEREST"
    if any(x in op for x in ['MINING', 'POOL REWARDS']): return "MINING"
    if any(x in op for x in ['VOUCHER', 'BONUS']): return "BONUS"
    if any(x in op for x in ['REBATE', 'COMMISSION REBATE']): return "REBATE"
    if 'CASHBACK' in op: return "CASHBACK"
    if any(x in op for x in ['FEE', 'TRANSACTION FEE']): return "FEE"
    if 'FUNDING' in op: return "FUNDING"
    if any(x in op for x in ['DEPOSIT', 'INITIAL BALANCE']): return "DEPOSIT"
    if any(x in op for x in ['WITHDRAW', 'SEND']): return "WITHDRAW"
    if any(x in op for x in ['TRANSFER', 'P2P', 'INTERNAL']): return "TRANSFER_INTERNAL"
    if any(x in op for x in ['TRADE', 'BUY', 'SELL', 'TRANSACTION', 'PNL']): return "TRADE"
    return "UNKNOWN"


# ==========================================================
# 📊 REGISTROS Y SINCRO (CON PREFIJOS DE SEGURIDAD)
# ==========================================================
def registrar_en_sistema(db, uid, exch, mercado, cat_raw, asset, monto, comi, pnl, fecha, id_ext, symbol=None):
    cursor = db.cursor()
    cat = normalizar_categoria_total(cat_raw)
    
    # Inserción Global (Evita duplicados con CSV usando id_ext único)
    sql_global = """INSERT IGNORE INTO transacciones_globales 
                    (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, comision, pnl_realizado, fecha_utc)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cursor.execute(sql_global, (id_ext, uid, exch, mercado, cat, asset.upper(), float(monto), float(comi), float(pnl), fecha))
    
    # Detalle de Mercado (Para Filtros PHP)
    if cat in ["TRADE", "FEE", "FUNDING"]:
        sql_detalle = """INSERT IGNORE INTO detalle_trades 
                        (id_externo_ref, user_id, exchange, tipo_mercado, symbol, fecha_utc, pnl_realizado, cantidad_ejecutada)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(sql_detalle, (id_ext, uid, exch, mercado, symbol or asset, fecha, float(pnl), float(monto)))
    db.commit()

# ==========================================================
# 🟨 BINANCE SYNC (FULL ENDPOINTS)
# ==========================================================
def binance_full_sync(db, uid, k, s):
    client = Client(k, s); cursor = db.cursor(dictionary=True)
    
    # A. Balances y Radar Gema v4.2.3
    acc = client.get_account()
    for b in acc['balances']:
        total = float(b['free']) + float(b['locked'])
        if total > 0.000001:
            asset = b['asset'].upper()
            if asset not in ['USDT', 'USDC', 'FDUSD']:
                cursor.execute("SELECT id FROM sys_traductor_simbolos WHERE ticker_motor=%s LIMIT 1", (asset,))
                if not cursor.fetchone():
                    cursor.execute("INSERT IGNORE INTO sys_simbolos_buscados (user_id, ticker, info) VALUES (%s, %s, 'Radar API Binance')", (uid, asset))
            cursor.execute("REPLACE INTO sys_saldos_usuarios (user_id, exchange, asset, cantidad_total, saldo_bloqueado, last_update) VALUES (%s, 'BINANCE', %s, %s, %s, NOW())", 
                           (uid, asset, total, b['locked']))

    # B. Open Orders (Sincronización Espejo)
    vivas = []
    for o in client.get_open_orders():
        id_ref = f"BN-SPOT-{o['orderId']}"; vivas.append(id_ref)
        cursor.execute("REPLACE INTO sys_open_order_spot (user_id, exchange, symbol, side, price, qty, id_externo_ref, last_update) VALUES (%s, 'BINANCE', %s, %s, %s, %s, %s, NOW())", (uid, o['symbol'], o['side'], o['price'], o['origQty'], id_ref))
    # Limpieza: Borrar lo que ya no está abierto
    if vivas:
        format_strings = ','.join(['%s'] * len(vivas))
        cursor.execute(f"DELETE FROM sys_open_order_spot WHERE user_id = %s AND exchange = 'BINANCE' AND id_externo_ref NOT IN ({format_strings})", (uid, *vivas))
    
    # C. Historial (Dividends, Dust, Income)

    for div in client.get_asset_dividend_history()['rows']:
        # Usamos la info del dividendo para saber si es Airdrop, Interés o Mining
        cat_final = normalizar_categoria_total(div['enInfo']) 
        
        registrar_en_sistema(
            db, uid, "BINANCE", "SPOT", cat_final, 
            div['asset'], div['amount'], 0, 0, 
            format_date_sql(div['divTime']), 
            f"BN-DIV-{div['id']}"
        )

# ==========================================================
# 🚀 MOTOR DE EJECUCIÓN CONTINUA
# ==========================================================
def iniciar_motor():
    print("💎 GEMA v5.2.1 - MOTOR DE PRUEBAS ACTIVO")
    while True:
        try:
            db = mysql.connector.connect(**config.DB_CONFIG); cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT user_id, api_key, api_secret, broker_name FROM api_keys WHERE status=1")
            for u in cursor.fetchall():
                k, s = descifrar_dato(u['api_key'], MASTER_KEY), descifrar_dato(u['api_secret'], MASTER_KEY)
                if k and s:
                    if u['broker_name'].upper() == 'BINANCE': binance_full_sync(db, u['user_id'], k, s)
                    # (BingX Sync Capa 5 igual al bloque anterior...)
            db.close(); print(f"[{datetime.now()}] Ciclo de auditoría completado.")
        except Exception as e: print(f" [CRITICAL ERROR] {e}")
        time.sleep(300)

if __name__ == "__main__": iniciar_motor()