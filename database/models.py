# SQL schema definitions. Each module adds its own table to ALL_TABLES.

FILINGS_TABLE = """
CREATE TABLE IF NOT EXISTS filings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    accession_number    TEXT    UNIQUE NOT NULL,
    cik                 TEXT    NOT NULL,
    company_name        TEXT    NOT NULL,
    ticker              TEXT,
    insider_name        TEXT    NOT NULL,
    insider_title       TEXT,
    is_director         INTEGER DEFAULT 0,
    is_officer          INTEGER DEFAULT 0,
    is_ten_pct_owner    INTEGER DEFAULT 0,
    date_filed          TEXT    NOT NULL,
    period_of_report    TEXT,
    filing_url          TEXT,
    created_at          TEXT    DEFAULT (datetime('now'))
)
"""

TRANSACTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS transactions (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id               INTEGER NOT NULL REFERENCES filings(id),
    security_title          TEXT,
    transaction_date        TEXT,
    transaction_code        TEXT    NOT NULL,
    transaction_type        TEXT,
    shares                  REAL,
    price_per_share         REAL,
    total_value             REAL,
    acquired_disposed       TEXT,
    shares_owned_after      REAL,
    shares_owned_before     REAL,
    is_open_market_purchase INTEGER DEFAULT 0,
    created_at              TEXT    DEFAULT (datetime('now'))
)
"""

# Populated by Module 2 (enricher.py)
ENRICHED_DATA_TABLE = """
CREATE TABLE IF NOT EXISTS enriched_data (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id                   INTEGER UNIQUE NOT NULL REFERENCES filings(id),
    market_cap                  REAL,
    sector                      TEXT,
    industry                    TEXT,
    shares_outstanding_current  REAL,
    shares_outstanding_12mo_ago REAL,
    share_count_change_pct      REAL,
    enriched_at                 TEXT    DEFAULT (datetime('now'))
)
"""

# Populated by Module 3 (scoring engine)
SCORED_TRADES_TABLE = """
CREATE TABLE IF NOT EXISTS scored_trades (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id          INTEGER UNIQUE NOT NULL REFERENCES transactions(id),
    score_small_cap         TEXT,
    score_materiality       TEXT,
    score_sector            TEXT,
    score_cannibal          TEXT,
    score_timing            TEXT,
    reasoning_small_cap     TEXT,
    reasoning_materiality   TEXT,
    reasoning_sector        TEXT,
    reasoning_cannibal      TEXT,
    reasoning_timing        TEXT,
    overall_signal          TEXT,
    confidence_score        INTEGER,
    is_urgent               INTEGER DEFAULT 0,
    alert_sent              INTEGER DEFAULT 0,
    scored_at               TEXT    DEFAULT (datetime('now'))
)
"""

ALL_TABLES = [
    FILINGS_TABLE,
    TRANSACTIONS_TABLE,
    ENRICHED_DATA_TABLE,
    SCORED_TRADES_TABLE,
]
