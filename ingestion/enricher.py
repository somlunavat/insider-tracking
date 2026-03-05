"""
Module 2: Data Enrichment

For each filing that has at least one open market purchase:
  1. Fetch market cap + sector/industry from yfinance
  2. Fetch 12-month shares outstanding history from yfinance (fallback: FMP)
  3. Calculate share count change % (for Cannibal criterion)
  4. Store in enriched_data table

Data sources:
  - yfinance   — market cap, sector, industry, shares history (primary)
  - FMP        — shares outstanding quarterly history (fallback, requires API key)
"""

import time
import logging
import requests
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf

import config
from database.db import get_db

logger = logging.getLogger(__name__)

FMP_BASE = "https://financialmodelingprep.com/api/v3"

# Space requests to stay below yfinance's informal rate limit
YF_DELAY = 0.5

# Cache enriched ticker data within a single run to avoid redundant API calls
_ticker_cache: dict[str, dict] = {}


# ---------------------------------------------------------------------------
# yfinance helpers
# ---------------------------------------------------------------------------

def fetch_market_data(ticker: str) -> dict:
    """
    Fetch market cap, sector, and industry for a ticker via yfinance.
    All values may be None if the data is unavailable.
    """
    try:
        info = yf.Ticker(ticker).info
        time.sleep(YF_DELAY)
        return {
            "market_cap": info.get("marketCap"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
        }
    except Exception as e:
        logger.warning("yfinance market data failed [%s]: %s", ticker, e)
        return {"market_cap": None, "sector": None, "industry": None}


def fetch_shares_history_yfinance(ticker: str) -> tuple[Optional[float], Optional[float]]:
    """
    Return (shares_now, shares_12_months_ago) from yfinance.
    Uses get_shares_full() which returns a dated Series of share counts.
    Returns (None, None) if data is unavailable or the ticker is too small.
    """
    try:
        obj = yf.Ticker(ticker)
        end = datetime.now()
        start = end - timedelta(days=400)   # a little past 12 months for buffer

        series = obj.get_shares_full(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
        )
        time.sleep(YF_DELAY)

        if series is None or series.empty:
            return None, None

        current = float(series.iloc[-1])

        # yfinance returns a timezone-aware Series index; build a matching cutoff
        cutoff_naive = end - timedelta(days=365)
        tz = getattr(series.index, "tz", None)
        cutoff = pd.Timestamp(cutoff_naive, tz=tz) if tz else pd.Timestamp(cutoff_naive)

        older = series[series.index <= cutoff]
        year_ago = float(older.iloc[-1]) if not older.empty else float(series.iloc[0])

        return current, year_ago
    except Exception as e:
        logger.warning("yfinance shares history failed [%s]: %s", ticker, e)
        return None, None


# ---------------------------------------------------------------------------
# FMP fallback
# ---------------------------------------------------------------------------

def fetch_shares_history_fmp(ticker: str) -> tuple[Optional[float], Optional[float]]:
    """
    Fallback: fetch shares outstanding from FMP quarterly balance sheet.
    Requires config.FMP_API_KEY.
    Returns (shares_now, shares_12_months_ago), both may be None.
    """
    if not config.FMP_API_KEY:
        return None, None

    try:
        resp = requests.get(
            f"{FMP_BASE}/balance-sheet-statement/{ticker}",
            params={"period": "quarter", "limit": 6, "apikey": config.FMP_API_KEY},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        if not data or not isinstance(data, list):
            return None, None

        # FMP returns quarters most-recent first
        def _shares(entry: dict) -> Optional[float]:
            val = entry.get("weightedAverageShsOut") or entry.get("sharesOutstanding")
            return float(val) if val else None

        current = _shares(data[0])
        # Prefer 4 quarters ago for a clean annual comparison
        year_ago = _shares(data[4]) if len(data) > 4 else _shares(data[-1])

        return current, year_ago
    except Exception as e:
        logger.warning("FMP shares history failed [%s]: %s", ticker, e)
        return None, None


# ---------------------------------------------------------------------------
# Per-ticker enrichment (with in-run cache)
# ---------------------------------------------------------------------------

def enrich_ticker(ticker: str) -> dict:
    """
    Collect all enrichment data for a ticker.
    Cached within the current process run to avoid duplicate API calls.

    Returns a dict with keys:
        market_cap, sector, industry,
        shares_outstanding_current, shares_outstanding_12mo_ago,
        share_count_change_pct
    """
    if ticker in _ticker_cache:
        return _ticker_cache[ticker]

    market = fetch_market_data(ticker)

    shares_current, shares_12mo_ago = fetch_shares_history_yfinance(ticker)
    if shares_current is None:
        logger.debug("yfinance shares unavailable for %s, trying FMP", ticker)
        shares_current, shares_12mo_ago = fetch_shares_history_fmp(ticker)

    change_pct: Optional[float] = None
    if shares_current is not None and shares_12mo_ago and shares_12mo_ago > 0:
        change_pct = ((shares_current - shares_12mo_ago) / shares_12mo_ago) * 100

    result = {
        "market_cap": market["market_cap"],
        "sector": market["sector"],
        "industry": market["industry"],
        "shares_outstanding_current": shares_current,
        "shares_outstanding_12mo_ago": shares_12mo_ago,
        "share_count_change_pct": change_pct,
    }
    _ticker_cache[ticker] = result
    return result


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_unenriched_filings() -> list[tuple[int, str]]:
    """
    Return (filing_id, ticker) for filings that:
    - Have at least one open market purchase
    - Have not been enriched yet
    - Have a non-empty ticker symbol
    """
    with get_db() as conn:
        rows = conn.execute("""
            SELECT DISTINCT f.id, f.ticker
            FROM   filings f
            JOIN   transactions t  ON t.filing_id  = f.id
            LEFT JOIN enriched_data e ON e.filing_id = f.id
            WHERE  t.is_open_market_purchase = 1
              AND  e.id IS NULL
              AND  f.ticker IS NOT NULL
              AND  f.ticker != ''
              AND  f.ticker != 'N/A'
            ORDER BY f.date_filed DESC
        """).fetchall()
    return [(row["id"], row["ticker"]) for row in rows]


def store_enriched_data(filing_id: int, data: dict) -> None:
    with get_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO enriched_data
                (filing_id, market_cap, sector, industry,
                 shares_outstanding_current, shares_outstanding_12mo_ago,
                 share_count_change_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            filing_id,
            data["market_cap"],
            data["sector"],
            data["industry"],
            data["shares_outstanding_current"],
            data["shares_outstanding_12mo_ago"],
            data["share_count_change_pct"],
        ))


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def enrich_new_filings() -> int:
    """
    Enrich all unenriched filings that contain open market purchases.
    Returns the count of filings successfully enriched.
    """
    _ticker_cache.clear()

    to_enrich = get_unenriched_filings()
    total = len(to_enrich)
    logger.info("Enrichment starting: %d filings to process", total)

    count = errors = 0
    for i, (filing_id, ticker) in enumerate(to_enrich, start=1):
        if i % 20 == 0 or i == total:
            logger.info("Enrichment progress: %d/%d | enriched=%d errors=%d", i, total, count, errors)
        try:
            data = enrich_ticker(ticker)
            store_enriched_data(filing_id, data)
            count += 1
            logger.debug(
                "Enriched filing %d | %-6s | cap=$%.0fM | sector=%-20s | share_chg=%s%%",
                filing_id, ticker,
                (data["market_cap"] or 0) / 1e6,
                data["sector"] or "n/a",
                f"{data['share_count_change_pct']:.1f}" if data["share_count_change_pct"] is not None else "n/a",
            )
        except Exception as e:
            logger.error("Failed to enrich filing %d (%s): %s", filing_id, ticker, e)
            errors += 1

    logger.info("Enrichment complete: %d enriched, %d errors (of %d)", count, errors, total)
    return count
