"""
Module 3: Scoring Engine

Implements all 5 criteria from the investment thesis and combines them into
an overall signal rating with a 0-100 confidence score.

Each criterion function accepts plain Python values and returns a
CriterionResult(score, reasoning) so the engine is fully testable in isolation.

Scoring tiers:
  STRONG   — C1 + C2 + C3 all pass, plus C4 or C5
  MODERATE — C1 + C2 pass, plus at least one of C3/C4/C5
  WEAK     — C1 + C2 pass only
  NO_SIGNAL — C1 or C2 fail
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import config
from database.db import get_db

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Criterion result
# ---------------------------------------------------------------------------

class CriterionResult:
    __slots__ = ("score", "reasoning")

    def __init__(self, score: str, reasoning: str):
        self.score = score
        self.reasoning = reasoning

    def __repr__(self):
        return f"CriterionResult(score={self.score!r})"


# ---------------------------------------------------------------------------
# Criterion 1: Small Cap Filter
# ---------------------------------------------------------------------------

def score_small_cap(market_cap: Optional[float]) -> CriterionResult:
    """PASS if market_cap < $500M."""
    if market_cap is None:
        return CriterionResult("UNKNOWN", "Market cap data unavailable")

    cap_m = market_cap / 1_000_000
    if market_cap < config.MAX_MARKET_CAP:
        return CriterionResult(
            "PASS",
            f"Market cap ${cap_m:.0f}M is under the $500M threshold"
            " — less algorithmic and institutional coverage",
        )
    return CriterionResult(
        "FAIL",
        f"Market cap ${cap_m:.0f}M exceeds the $500M threshold",
    )


# ---------------------------------------------------------------------------
# Criterion 2: Materiality
# ---------------------------------------------------------------------------

def score_materiality(
    total_value: Optional[float],
    shares_owned_before: Optional[float],
    shares_bought: Optional[float],
    is_open_market_purchase: int,
) -> CriterionResult:
    """
    PASS if open market purchase AND trade value >= $1M AND position increase >= 10%.
    PARTIAL if only one condition met.
    FAIL if not an open market purchase or both conditions fail.
    """
    if not is_open_market_purchase:
        return CriterionResult("FAIL", "Not an open market purchase")

    value_ok = total_value is not None and total_value >= config.MIN_TRADE_VALUE

    position_pct: Optional[float] = None
    if (
        shares_owned_before is not None
        and shares_owned_before > 0
        and shares_bought is not None
    ):
        position_pct = (shares_bought / shares_owned_before) * 100

    position_ok = (
        position_pct is not None
        and position_pct >= config.MIN_POSITION_INCREASE_PCT
    )

    val_str = f"${total_value / 1e6:.2f}M" if total_value is not None else "unknown"
    pos_str = f"{position_pct:.1f}%" if position_pct is not None else "unknown"

    if value_ok and position_ok:
        return CriterionResult(
            "PASS",
            f"Trade value {val_str} meets $1M minimum; "
            f"position increased by {pos_str} (≥10% required)",
        )
    if value_ok:
        return CriterionResult(
            "PARTIAL",
            f"Trade value {val_str} meets threshold but position increase "
            f"of {pos_str} is below the 10% minimum",
        )
    if position_ok:
        return CriterionResult(
            "PARTIAL",
            f"Position increased by {pos_str} but trade value {val_str} "
            f"is below the $1M minimum",
        )
    return CriterionResult(
        "FAIL",
        f"Trade value {val_str} below $1M and position increase {pos_str} "
        f"is below 10% — insufficient materiality",
    )


# ---------------------------------------------------------------------------
# Criterion 3: Information Asymmetry / Sector Filter
# ---------------------------------------------------------------------------

_STRONG_KEYWORDS = {
    "biotech", "biotechnology", "gold", "mining", "silver",
    "precious metal", "pharmaceutical", "drug", "bioscience",
}


def score_sector(sector: Optional[str], industry: Optional[str]) -> CriterionResult:
    """
    STRONG_PASS for biotech/gold/mining (high information asymmetry).
    PASS for any other known sector.
    UNKNOWN if sector data is missing.
    """
    combined = " ".join(filter(None, [sector, industry])).lower()

    for kw in _STRONG_KEYWORDS:
        if kw in combined:
            label = sector or industry or kw
            return CriterionResult(
                "STRONG_PASS",
                f"Sector '{label}' is in the high-asymmetry category"
                " (biotech/gold/mining) — insiders likely hold non-public data",
            )

    if sector or industry:
        return CriterionResult(
            "PASS",
            f"Sector '{sector or 'unknown'}' — not a priority sector"
            " but trade is not disqualified",
        )

    return CriterionResult("UNKNOWN", "Sector data unavailable")


# ---------------------------------------------------------------------------
# Criterion 4: The Cannibal Trait
# ---------------------------------------------------------------------------

def score_cannibal(share_count_change_pct: Optional[float]) -> CriterionResult:
    """
    PASS if share count reduced >= 2% annually.
    PARTIAL if reducing but < 2%.
    FAIL if flat or increasing.
    UNKNOWN if data is missing.
    """
    if share_count_change_pct is None:
        return CriterionResult("UNKNOWN", "Share count history unavailable")

    reduction_pct = -share_count_change_pct  # positive = reduction

    if reduction_pct >= config.MIN_SHARE_REDUCTION_PCT:
        return CriterionResult(
            "PASS",
            f"Share count reduced by {reduction_pct:.1f}% over 12 months"
            " — management is buying back stock, signalling undervaluation",
        )
    if reduction_pct > 0:
        return CriterionResult(
            "PARTIAL",
            f"Share count reduced by {reduction_pct:.1f}%"
            f" — positive but below the {config.MIN_SHARE_REDUCTION_PCT}% threshold",
        )

    growth_pct = share_count_change_pct
    if abs(growth_pct) < 0.1:
        trend = "flat"
    else:
        trend = f"increased by {growth_pct:.1f}%"
    return CriterionResult(
        "FAIL",
        f"Share count {trend} over 12 months — no cannibal signal",
    )


# ---------------------------------------------------------------------------
# Criterion 5: Aftermarket Timing
# ---------------------------------------------------------------------------

def score_timing(date_filed: str) -> CriterionResult:
    """
    URGENT if filed within config.URGENT_FILING_HOURS hours of now.
    RECENT if filed within the same day (< 24 h).
    NORMAL otherwise.
    """
    try:
        try:
            filed_dt = datetime.strptime(date_filed, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            filed_dt = datetime.strptime(date_filed, "%Y-%m-%d")

        hours_since = (datetime.now() - filed_dt).total_seconds() / 3600

        if hours_since <= config.URGENT_FILING_HOURS:
            return CriterionResult(
                "URGENT",
                f"Filed {hours_since:.1f}h ago — within the"
                f" {config.URGENT_FILING_HOURS}h window;"
                " actionable in aftermarket trading",
            )
        if hours_since <= 24:
            return CriterionResult(
                "RECENT",
                f"Filed {hours_since:.1f}h ago — same-day filing, still timely",
            )
        days_ago = hours_since / 24
        return CriterionResult(
            "NORMAL",
            f"Filed {days_ago:.0f} day(s) ago — no timing edge",
        )
    except (ValueError, TypeError):
        return CriterionResult(
            "UNKNOWN", f"Could not parse filing date: {date_filed!r}"
        )


# ---------------------------------------------------------------------------
# Confidence score (0-100)
# ---------------------------------------------------------------------------

# Multiplier per score value per criterion
_SCORE_MULTIPLIERS: dict[str, dict[str, float]] = {
    "small_cap":   {"PASS": 1.0, "FAIL": 0.0, "UNKNOWN": 0.0},
    "materiality": {"PASS": 1.0, "PARTIAL": 0.5, "FAIL": 0.0, "UNKNOWN": 0.0},
    "sector":      {"STRONG_PASS": 1.0, "PASS": 0.5, "UNKNOWN": 0.0},
    "cannibal":    {"PASS": 1.0, "PARTIAL": 0.5, "FAIL": 0.0, "UNKNOWN": 0.0},
    "timing":      {"URGENT": 1.0, "RECENT": 0.6, "NORMAL": 0.3, "UNKNOWN": 0.0},
}


def calculate_confidence(
    s_small_cap: str,
    s_materiality: str,
    s_sector: str,
    s_cannibal: str,
    s_timing: str,
) -> int:
    """Return 0-100 confidence score weighted by config.WEIGHTS."""
    weights = config.WEIGHTS
    score = (
        weights["small_cap"]   * _SCORE_MULTIPLIERS["small_cap"].get(s_small_cap, 0.0)
        + weights["materiality"] * _SCORE_MULTIPLIERS["materiality"].get(s_materiality, 0.0)
        + weights["sector"]      * _SCORE_MULTIPLIERS["sector"].get(s_sector, 0.0)
        + weights["cannibal"]    * _SCORE_MULTIPLIERS["cannibal"].get(s_cannibal, 0.0)
        + weights["timing"]      * _SCORE_MULTIPLIERS["timing"].get(s_timing, 0.0)
    )
    return round(score)


# ---------------------------------------------------------------------------
# Overall signal
# ---------------------------------------------------------------------------

def calculate_signal(
    s_small_cap: str,
    s_materiality: str,
    s_sector: str,
    s_cannibal: str,
    s_timing: str,
) -> str:
    """
    STRONG   — C1 + C2 + C3 all pass, plus C4 or C5
    MODERATE — C1 + C2 pass, plus at least one of C3/C4/C5
    WEAK     — C1 + C2 pass only
    NO_SIGNAL — C1 or C2 fail
    """
    c1 = s_small_cap == "PASS"
    c2 = s_materiality == "PASS"
    c3 = s_sector in ("PASS", "STRONG_PASS")
    c4 = s_cannibal == "PASS"
    c5 = s_timing in ("URGENT", "RECENT")

    if not (c1 and c2):
        return "NO_SIGNAL"

    if c3 and (c4 or c5):
        return "STRONG"
    if c3 or c4 or c5:
        return "MODERATE"
    return "WEAK"


# ---------------------------------------------------------------------------
# Main scoring function
# ---------------------------------------------------------------------------

def score_trade(transaction: dict, enriched: dict, filing: dict) -> dict:
    """
    Score a single trade.

    Parameters
    ----------
    transaction : dict — row from the transactions table
    enriched    : dict — row from the enriched_data table (values may be None)
    filing      : dict — row from the filings table

    Returns
    -------
    dict matching the scored_trades table columns.
    """
    r_small_cap   = score_small_cap(enriched.get("market_cap"))
    r_materiality = score_materiality(
        total_value=transaction.get("total_value"),
        shares_owned_before=transaction.get("shares_owned_before"),
        shares_bought=transaction.get("shares"),
        is_open_market_purchase=transaction.get("is_open_market_purchase", 0),
    )
    r_sector   = score_sector(enriched.get("sector"), enriched.get("industry"))
    r_cannibal = score_cannibal(enriched.get("share_count_change_pct"))
    r_timing   = score_timing(filing.get("date_filed", ""))

    signal     = calculate_signal(
        r_small_cap.score, r_materiality.score,
        r_sector.score, r_cannibal.score, r_timing.score,
    )
    confidence = calculate_confidence(
        r_small_cap.score, r_materiality.score,
        r_sector.score, r_cannibal.score, r_timing.score,
    )

    return {
        "transaction_id":        transaction["id"],
        "score_small_cap":       r_small_cap.score,
        "score_materiality":     r_materiality.score,
        "score_sector":          r_sector.score,
        "score_cannibal":        r_cannibal.score,
        "score_timing":          r_timing.score,
        "reasoning_small_cap":   r_small_cap.reasoning,
        "reasoning_materiality": r_materiality.reasoning,
        "reasoning_sector":      r_sector.reasoning,
        "reasoning_cannibal":    r_cannibal.reasoning,
        "reasoning_timing":      r_timing.reasoning,
        "overall_signal":        signal,
        "confidence_score":      confidence,
        "is_urgent":             1 if r_timing.score == "URGENT" else 0,
    }


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_unscored_transactions() -> list[dict]:
    """
    Return all open-market-purchase transactions that have enriched data
    but have not yet been scored.
    """
    with get_db() as conn:
        rows = conn.execute("""
            SELECT
                t.id, t.filing_id, t.total_value, t.shares,
                t.shares_owned_before, t.is_open_market_purchase,
                e.market_cap, e.sector, e.industry, e.share_count_change_pct,
                f.date_filed, f.ticker, f.company_name,
                f.insider_name, f.insider_title
            FROM   transactions t
            JOIN   filings f       ON f.id = t.filing_id
            JOIN   enriched_data e ON e.filing_id = t.filing_id
            LEFT JOIN scored_trades s ON s.transaction_id = t.id
            WHERE  t.is_open_market_purchase = 1
              AND  s.id IS NULL
            ORDER  BY f.date_filed DESC
        """).fetchall()
    return [dict(row) for row in rows]


def store_scored_trade(result: dict) -> None:
    with get_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO scored_trades (
                transaction_id,
                score_small_cap, score_materiality, score_sector,
                score_cannibal,  score_timing,
                reasoning_small_cap, reasoning_materiality, reasoning_sector,
                reasoning_cannibal,  reasoning_timing,
                overall_signal, confidence_score, is_urgent
            ) VALUES (
                :transaction_id,
                :score_small_cap, :score_materiality, :score_sector,
                :score_cannibal,  :score_timing,
                :reasoning_small_cap, :reasoning_materiality, :reasoning_sector,
                :reasoning_cannibal,  :reasoning_timing,
                :overall_signal, :confidence_score, :is_urgent
            )
        """, result)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def score_new_trades() -> int:
    """
    Score all unscored open-market-purchase transactions that have enriched data.
    Returns the count of trades successfully scored.
    """
    rows = get_unscored_transactions()
    total = len(rows)
    logger.info("Scoring starting: %d trades to process", total)

    count = errors = 0
    for row in rows:
        try:
            transaction = {
                "id":                   row["id"],
                "total_value":          row["total_value"],
                "shares":               row["shares"],
                "shares_owned_before":  row["shares_owned_before"],
                "is_open_market_purchase": row["is_open_market_purchase"],
            }
            enriched = {
                "market_cap":            row["market_cap"],
                "sector":                row["sector"],
                "industry":              row["industry"],
                "share_count_change_pct": row["share_count_change_pct"],
            }
            filing = {"date_filed": row["date_filed"]}

            result = score_trade(transaction, enriched, filing)
            store_scored_trade(result)
            count += 1

            logger.debug(
                "Scored txn %d | %-6s | %s | signal=%-10s | confidence=%d",
                row["id"], row["ticker"], row["company_name"],
                result["overall_signal"], result["confidence_score"],
            )
        except Exception as e:
            logger.error("Failed to score transaction %d: %s", row["id"], e)
            errors += 1

    logger.info(
        "Scoring complete: %d scored, %d errors (of %d)", count, errors, total
    )
    return count
