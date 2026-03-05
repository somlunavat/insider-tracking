import os
from dotenv import load_dotenv

load_dotenv()

# --- SEC EDGAR ---
SEC_USER_NAME = os.getenv("SEC_USER_NAME", "Insider Tracker Bot")
SEC_USER_EMAIL = os.getenv("SEC_USER_EMAIL", "user@example.com")

# --- Database ---
DB_PATH = os.getenv("DB_PATH", "insider_tracker.db")

# --- Financial Modeling Prep (fallback for shares outstanding history) ---
FMP_API_KEY = os.getenv("FMP_API_KEY", "")

# --- Logging ---
LOG_FILE = os.getenv("LOG_FILE", "logs/insider_tracker.log")

# --- Scoring thresholds ---
MAX_MARKET_CAP = 500_000_000       # Criteria 1: $500M cap
MIN_TRADE_VALUE = 1_000_000        # Criteria 2: $1M minimum trade
MIN_POSITION_INCREASE_PCT = 10.0   # Criteria 2: must increase position by 10%+
MIN_SHARE_REDUCTION_PCT = 2.0      # Criteria 4: cannibal requires 2%+ annual reduction
URGENT_FILING_HOURS = 3            # Criteria 5: flag as urgent if filed within 3 hours

# --- Scoring weights (sum to 100) ---
WEIGHTS = {
    "small_cap":   15,
    "materiality": 30,
    "sector":      20,
    "cannibal":    20,
    "timing":      15,
}

# --- Sector classification ---
# STRONG PASS if company is in one of these sectors
STRONG_PASS_SECTORS = [
    "Biotechnology",
    "Gold",
    "Mining",
    "Basic Materials",
    "Healthcare",
]

# --- Scheduler ---
SCREENER_HOUR = 20    # 8 PM local time
SCREENER_MINUTE = 0
