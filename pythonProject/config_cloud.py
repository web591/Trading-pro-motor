# Version 1.0 - CLOUD CONFIG

import os

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'),
    'database': os.getenv('DB_NAME'),
    'port': int(os.getenv('DB_PORT', 3306))
}

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
FINNHUB_KEY = os.getenv("FINNHUB_KEY")
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")