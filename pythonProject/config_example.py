# config.py
# --- ARCHIVO DE CREDENCIALES PRIVADAS ---
# ¡NO SUBIR A GITHUB!

# 1. Base de Datos Hostinger (Tu Bóveda)
DB_CONFIG = {
    'host': '195.35.61.47',
    'user': 'u800112681_dashboard',
    'password': 'TU_CONTRASEÑA_DE_HOSTINGER',  # <--- Pon aquí tu pass real
    'database': 'u800112681_dashboard',
    'port': 3306
}

# Esta llave DEBE ser la misma que está en /config/security.php de PHP
ENCRYPTION_KEY = "AQUI LA LLAVE DE ENCRYPTION"

# FINNHUB (Acciones USA / Forex Respaldo)
FINNHUB_KEY = "AQUI LA LLAVE DE FINNHUB_KEY"

# ALPHA VANTAGE (Commodities / Fundamentales)
ALPHA_VANTAGE_KEY = "AQUI LA LLAVE DE ALPHA_VANTAGE_KEY"  