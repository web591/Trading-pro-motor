import pandas as pd
import mysql.connector
import hashlib
import config
from datetime import datetime

# --- DICCIONARIO MAESTRO EXTENDIDO ---
# Mapeamos TODO lo que Binance puede enviar en el consolidado
MAPEO_OPERACIONES = {
    # 1. INGRESOS Y PREMIOS (Afectan PnL +)
    'Simple Earn Flexible Interest': 'INTEREST',
    'Simple Earn Flexible Airdrop': 'AIRDROP',
    'Staking Rewards': 'INTEREST',
    'Savings Interest': 'INTEREST',
    'BNB Vault Rewards': 'INTEREST',
    'Airdrop Assets': 'AIRDROP',
    'Distribution': 'AIRDROP',
    'Launchpool Earnings Withdrawal': 'INTEREST',
    'Launchpool Airdrop': 'AIRDROP',
    'Launchpool Airdrop - System Distribution': 'AIRDROP',
    'HODLer Airdrops Distribution': 'AIRDROP',
    'Pool Distribution': 'MINING_REWARD',
    'Crypto Box': 'GIFT',
    'Referral Commission': 'COMMISSION_IN',
    'Commission Rebate': 'COMMISSION_IN',
    'Cash Voucher': 'CASHBACK',
    'Insurance Fund Compensation': 'COMPENSATION',
    'Small Assets Exchange BNB': 'TRADE_SPOT',
    'Binance Convert': 'TRADE_SPOT',

    # 2. GASTOS Y COSTOS (Afectan PnL -)
    'Fee': 'FEE',
    'Funding Fee': 'FUNDING_FEE',
    
    # 3. MOVIMIENTOS DE CAPITAL (Entradas/Salidas Externas)
    'Initial Balance': 'CAPITAL_IN',
    'Deposit': 'DEPOSIT',
    'Withdraw': 'WITHDRAW',
    'Send': 'WITHDRAW',
    'P2P Trading': 'P2P_TRANSFER',

    # 4. TRASVASES INTERNOS (Neutrales - Se ignoran o marcan como INTERNAL)
    'Simple Earn Flexible Subscription': 'INTERNAL_TRANSFER',
    'Simple Earn Flexible Redemption': 'INTERNAL_TRANSFER',
    'Transfer Between Main and Funding Wallet': 'INTERNAL_TRANSFER',
    'Transfer Between Spot Account and UM Futures Account': 'INTERNAL_TRANSFER',
    'Transfer Between Spot Account and CM Futures Account': 'INTERNAL_TRANSFER',
    'Transfer Between UM Futures and Funding Account': 'INTERNAL_TRANSFER',
    'Transfer Between CM Futures and Funding Account': 'INTERNAL_TRANSFER',
    'Transfer Between UM and CM Account': 'INTERNAL_TRANSFER',
    'Transfer Funds to Spot': 'INTERNAL_TRANSFER',
    'Transfer Funds to Funding Wallet': 'INTERNAL_TRANSFER',
    'Copy Portfolio (UM) - Create/Close Transfer': 'INTERNAL_TRANSFER',
    'Copy Portfolio (UM) - Create': 'INTERNAL_TRANSFER',
    'Copy Portfolio (UM) - Deposit': 'INTERNAL_TRANSFER',
    'Copy Portfolio (UM) - Withdraw': 'INTERNAL_TRANSFER',
    'Copy Portfolio (UM) - Close': 'INTERNAL_TRANSFER',
    'Transaction Revenue': 'INTERNAL_TRANSFER',
    'Transaction Spend': 'INTERNAL_TRANSFER',
    'Realized Profit and Loss': 'INTERNAL_TRANSFER',   

    # 5. TRADES (¡CRÍTICO! Estos vendrán de los archivos de órdenes)
    # Los categorizamos como TRADE_SPOT para filtrarlos al final    
    'Transaction Buy': 'TRADE_SPOT',
    'Transaction Sold': 'TRADE_SPOT',
    'Transaction Fee': 'TRADE_SPOT',    
}

# ==========================================================
# Normalizador numérico seguro para CSV / API
# Soporta: -1,131.20 | 1131.20 | None | ''
# ==========================================================
def to_float(value):
    if value is None:
        return 0.0
    try:
        return float(str(value).replace(',', '').strip())
    except ValueError:
        return 0.0


def cargar_consolidado_auditado(archivo_csv, user_id_interno):
    try:
        df = pd.read_csv(archivo_csv)
        print(f"📊 Analizando {len(df)} registros con el nuevo mapeo completo...")

        registros_para_bd = []
        conteo_ignorable = 0
        
        for _, row in df.iterrows():
            op_raw = row['Operation']
            categoria = MAPEO_OPERACIONES.get(op_raw, 'UNKNOWN')

            # --- LÓGICA DE EXCLUSIÓN SOLICITADA ---
            # Si la operación es un TRADE, la quitamos porque vendrá de los otros archivos.
            if categoria == 'TRADE_SPOT':
                conteo_ignorable += 1
                continue
            
            # Generar ID único para evitar duplicados si corres el script 2 veces
            raw_id = f"{user_id_interno}{row['UTC_Time']}{row['Change']}{row['Coin']}{op_raw}"
            id_externo = hashlib.md5(raw_id.encode()).hexdigest()

            # Normalizar fecha
            try:
                fecha_dt = datetime.strptime(row['UTC_Time'], "%d/%m/%Y %H:%M")
            except:
                fecha_dt = datetime.now() # Fallback por si acaso

            registros_para_bd.append((
                f"CONSO-{id_externo}", 
                user_id_interno,
                'Binance',
                row['Account'],
                categoria,
                row['Coin'],
                to_float(row['Change']),
                fecha_dt,
                f"Op: {op_raw} | Ref: {row['Remark']}"
            ))

        # Inserción
        conn = mysql.connector.connect(**config.DB_CONFIG)
        cursor = conn.cursor()
        
        sql = """
            INSERT IGNORE INTO transacciones_globales 
            (id_externo, user_id, exchange, cuenta_tipo, categoria, asset, monto_neto, fecha_utc, descripcion) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(sql, registros_para_bd)
        conn.commit()
        
        print(f"✅ Proceso completado.")
        print(f"📥 Insertados/Verificados: {len(registros_para_bd)} movimientos (Intereses, Airdrops, etc.)")
        print(f"🚫 Ignorados: {conteo_ignorable} registros de TRADE/Convert (se asume que vienen en otros archivos)")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    # Asegúrate de que el nombre del archivo coincida
    cargar_consolidado_auditado('binance_conso.csv', user_id_interno=6)