import pandas as pd
import mysql.connector
import hashlib
import config
from datetime import datetime

# ==========================================================
# MAPEO EXTENDIDO DE BINANCE CONSOLIDADO
# ==========================================================

MAPEO_OPERACIONES = {

    # INGRESOS
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
    'Referral Commission': 'COMMISSION',
    'Commission Rebate': 'COMMISSION',
    'Cash Voucher': 'CASHBACK',
    'Insurance Fund Compensation': 'COMPENSATION',

    # COSTOS
    'Fee': 'FEE',
    'Funding Fee': 'FUNDING',

    # CAPITAL
    'Initial Balance': 'DEPOSIT',
    'Deposit': 'DEPOSIT',
    'Withdraw': 'WITHDRAW',
    'Send': 'WITHDRAW',
    'P2P Trading': 'TRANSFER_INTERNAL',

    # INTERNOS
    'Simple Earn Flexible Subscription': 'TRANSFER_INTERNAL',
    'Simple Earn Flexible Redemption': 'TRANSFER_INTERNAL',
    'Transfer Between Main and Funding Wallet': 'TRANSFER_INTERNAL',
    'Transfer Between Spot Account and UM Futures Account': 'TRANSFER_INTERNAL',
    'Transfer Between Spot Account and CM Futures Account': 'TRANSFER_INTERNAL',
    'Transfer Between UM Futures and Funding Account': 'TRANSFER_INTERNAL',
    'Transfer Between CM Futures and Funding Account': 'TRANSFER_INTERNAL',
    'Transfer Between UM and CM Account': 'TRANSFER_INTERNAL',
    'Transfer Funds to Spot': 'TRANSFER_INTERNAL',
    'Transfer Funds to Funding Wallet': 'TRANSFER_INTERNAL',
    'Copy Portfolio (UM) - Create/Close Transfer': 'TRANSFER_INTERNAL',
    'Copy Portfolio (UM) - Create': 'TRANSFER_INTERNAL',
    'Copy Portfolio (UM) - Deposit': 'TRANSFER_INTERNAL',
    'Copy Portfolio (UM) - Withdraw': 'TRANSFER_INTERNAL',
    'Copy Portfolio (UM) - Close': 'TRANSFER_INTERNAL',
    'Transaction Revenue': 'TRANSFER_INTERNAL',
    'Transaction Spend': 'TRANSFER_INTERNAL',

    # TRADES (se ignoran porque vienen del importador trades)
    'Transaction Buy': 'TRADE',
    'Transaction Sold': 'TRADE',
    'Transaction Fee': 'TRADE',
    'Small Assets Exchange BNB': 'TRADE',
    'Binance Convert': 'TRADE',
}

# ==========================================================
# NORMALIZADOR NUMÉRICO
# ==========================================================

def to_float(value):
    if value is None:
        return 0.0
    try:
        return float(str(value).replace(',', '').strip())
    except:
        return 0.0

# ==========================================================
# NORMALIZADOR DE FECHA (MISMO ESTÁNDAR DEL MOTOR)
# ==========================================================

def normalizar_fecha_motor(fecha_raw):

    if not fecha_raw:
        return None

    try:
        for fmt in [
            "%d/%m/%Y %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S"
        ]:
            try:
                fecha = datetime.strptime(str(fecha_raw).strip(), fmt)
                return fecha.replace(microsecond=0)
            except:
                continue
        return None
    except:
        return None

# ==========================================================
# NORMALIZADOR DE CATEGORÍA (ESTÁNDAR MOTOR SALDOS)
# ==========================================================


def normalizar_ledger_v4(texto_operacion):
    op = str(texto_operacion).upper().strip()
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

# --- DENTRO DEL BUCLE FOR, BUSCA DONDE SE ASIGNA 'categoria' Y CÁMBIALO POR: ---


# ==========================================================
# GENERADOR ID EXTERNO INSTITUCIONAL (SHA256)
# ==========================================================

def generar_id_externo(exchange, asset, monto, fecha, categoria):

    if not fecha:
        return None

    base = f"{exchange}|{asset}|{round(float(monto),8)}|{fecha.strftime('%Y-%m-%d %H:%M:%S')}|{categoria}"
    return hashlib.sha256(base.encode()).hexdigest()

# ==========================================================
# CARGADOR CONSOLIDADO ALINEADO AL MOTOR
# ==========================================================

def cargar_consolidado_auditado(archivo_csv, user_id_interno):

    try:
        df = pd.read_csv(archivo_csv)
        print(f"📊 Analizando {len(df)} registros del consolidado...")

        registros_para_bd = []
        conteo_ignorado_trades = 0

        for _, row in df.iterrows():

            op_raw = row['Operation']
            categoria_raw = MAPEO_OPERACIONES.get(op_raw, 'UNKNOWN')
            categoria = normalizar_ledger_v4(op_raw)

            # Ignorar trades (vendrán del otro importador)
            if categoria == "TRADE":
                conteo_ignorado_trades += 1
                continue

            fecha_dt = normalizar_fecha_motor(row['UTC_Time'])
            monto = to_float(row['Change'])
            asset = row['Coin']

            id_hash = generar_id_externo(
                "BINANCE",
                asset,
                monto,
                fecha_dt,
                categoria
            )

            registros_para_bd.append((
                f"CONSO-{id_hash}",
                user_id_interno,
                "Binance",
                row['Account'],
                categoria,
                asset,
                monto,
                fecha_dt,
                f"Op: {op_raw} | Ref: {row.get('Remark','')}"
            ))

        conn = mysql.connector.connect(**config.DB_CONFIG)
        cursor = conn.cursor()

        sql = """
            INSERT IGNORE INTO transacciones_globales
            (id_externo, user_id, exchange, cuenta_tipo, categoria,
             asset, monto_neto, fecha_utc, descripcion)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        cursor.executemany(sql, registros_para_bd)
        conn.commit()

        print("✅ Consolidado alineado insertado correctamente.")
        print(f"📥 Movimientos procesados: {len(registros_para_bd)}")
        print(f"🚫 Trades ignorados: {conteo_ignorado_trades}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")

# ==========================================================
# EJECUCIÓN
# ==========================================================

if __name__ == "__main__":
    cargar_consolidado_auditado("binance_conso.csv", user_id_interno=6)