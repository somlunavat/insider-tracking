"""
Unit tests for scoring/engine.py

Run with:  python -m pytest tests/test_scoring.py -v
"""

import pytest
from unittest.mock import patch
from datetime import datetime, timedelta

from scoring.engine import (
    score_small_cap,
    score_materiality,
    score_sector,
    score_cannibal,
    score_timing,
    calculate_confidence,
    calculate_signal,
    score_trade,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_minus(hours: float) -> str:
    """Return a datetime string for N hours ago."""
    dt = datetime.now() - timedelta(hours=hours)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _days_ago(days: int) -> str:
    dt = datetime.now() - timedelta(days=days)
    return dt.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Criterion 1: Small Cap
# ---------------------------------------------------------------------------

class TestScoreSmallCap:

    def test_pass_under_threshold(self):
        r = score_small_cap(200_000_000)
        assert r.score == "PASS"
        assert "200" in r.reasoning

    def test_pass_just_under(self):
        r = score_small_cap(499_999_999)
        assert r.score == "PASS"

    def test_fail_at_threshold(self):
        r = score_small_cap(500_000_000)
        assert r.score == "FAIL"

    def test_fail_over_threshold(self):
        r = score_small_cap(2_000_000_000)
        assert r.score == "FAIL"

    def test_unknown_when_none(self):
        r = score_small_cap(None)
        assert r.score == "UNKNOWN"

    def test_reasoning_is_nonempty(self):
        assert score_small_cap(100_000_000).reasoning


# ---------------------------------------------------------------------------
# Criterion 2: Materiality
# ---------------------------------------------------------------------------

class TestScoreMateriality:

    def test_pass_both_conditions(self):
        r = score_materiality(
            total_value=2_000_000,
            shares_owned_before=50_000,
            shares_bought=10_000,   # +20% position
            is_open_market_purchase=1,
        )
        assert r.score == "PASS"

    def test_fail_not_open_market(self):
        r = score_materiality(
            total_value=5_000_000,
            shares_owned_before=10_000,
            shares_bought=5_000,
            is_open_market_purchase=0,
        )
        assert r.score == "FAIL"
        assert "not an open market" in r.reasoning.lower()

    def test_partial_value_ok_position_below_10pct(self):
        r = score_materiality(
            total_value=1_500_000,
            shares_owned_before=200_000,
            shares_bought=5_000,   # +2.5%
            is_open_market_purchase=1,
        )
        assert r.score == "PARTIAL"

    def test_partial_position_ok_value_below_1m(self):
        r = score_materiality(
            total_value=500_000,   # below $1M
            shares_owned_before=10_000,
            shares_bought=5_000,   # +50%
            is_open_market_purchase=1,
        )
        assert r.score == "PARTIAL"

    def test_fail_both_conditions_miss(self):
        r = score_materiality(
            total_value=100_000,
            shares_owned_before=100_000,
            shares_bought=100,    # 0.1%
            is_open_market_purchase=1,
        )
        assert r.score == "FAIL"

    def test_position_zero_before_treated_gracefully(self):
        # shares_owned_before = 0 → cannot compute pct → partial or fail
        r = score_materiality(
            total_value=2_000_000,
            shares_owned_before=0,
            shares_bought=10_000,
            is_open_market_purchase=1,
        )
        # position_pct = None → position_ok = False → value_ok=True → PARTIAL
        assert r.score == "PARTIAL"

    def test_none_values_handled(self):
        r = score_materiality(
            total_value=None,
            shares_owned_before=None,
            shares_bought=None,
            is_open_market_purchase=1,
        )
        assert r.score == "FAIL"

    def test_exactly_1m_passes_value_check(self):
        r = score_materiality(
            total_value=1_000_000,
            shares_owned_before=10_000,
            shares_bought=2_000,   # +20%
            is_open_market_purchase=1,
        )
        assert r.score == "PASS"

    def test_exactly_10pct_passes_position_check(self):
        r = score_materiality(
            total_value=1_000_000,
            shares_owned_before=10_000,
            shares_bought=1_000,   # +10%
            is_open_market_purchase=1,
        )
        assert r.score == "PASS"


# ---------------------------------------------------------------------------
# Criterion 3: Sector
# ---------------------------------------------------------------------------

class TestScoreSector:

    def test_strong_pass_biotech(self):
        r = score_sector("Biotechnology", "Drug Manufacturers")
        assert r.score == "STRONG_PASS"

    def test_strong_pass_gold(self):
        r = score_sector("Basic Materials", "Gold Mining")
        assert r.score == "STRONG_PASS"

    def test_strong_pass_mining(self):
        r = score_sector("Basic Materials", "Silver & Mining")
        assert r.score == "STRONG_PASS"

    def test_strong_pass_pharmaceutical(self):
        r = score_sector("Healthcare", "Pharmaceutical")
        assert r.score == "STRONG_PASS"

    def test_pass_other_known_sector(self):
        r = score_sector("Technology", "Software")
        assert r.score == "PASS"

    def test_pass_financials(self):
        r = score_sector("Financial Services", "Banks")
        assert r.score == "PASS"

    def test_unknown_when_both_none(self):
        r = score_sector(None, None)
        assert r.score == "UNKNOWN"

    def test_unknown_when_empty_strings(self):
        r = score_sector("", "")
        assert r.score == "UNKNOWN"

    def test_case_insensitive_match(self):
        r = score_sector("BIOTECHNOLOGY", None)
        assert r.score == "STRONG_PASS"

    def test_reasoning_nonempty(self):
        assert score_sector("Technology", "Software").reasoning


# ---------------------------------------------------------------------------
# Criterion 4: Cannibal
# ---------------------------------------------------------------------------

class TestScoreCannibal:

    def test_pass_reduction_above_threshold(self):
        r = score_cannibal(-3.5)   # 3.5% reduction
        assert r.score == "PASS"
        assert "3.5" in r.reasoning

    def test_pass_exactly_at_threshold(self):
        r = score_cannibal(-2.0)
        assert r.score == "PASS"

    def test_partial_reduction_below_threshold(self):
        r = score_cannibal(-1.0)
        assert r.score == "PARTIAL"

    def test_partial_tiny_reduction(self):
        r = score_cannibal(-0.1)
        assert r.score == "PARTIAL"

    def test_fail_flat(self):
        r = score_cannibal(0.0)
        assert r.score == "FAIL"
        assert "flat" in r.reasoning.lower()

    def test_fail_increasing(self):
        r = score_cannibal(5.0)
        assert r.score == "FAIL"
        assert "increased" in r.reasoning.lower()

    def test_unknown_when_none(self):
        r = score_cannibal(None)
        assert r.score == "UNKNOWN"


# ---------------------------------------------------------------------------
# Criterion 5: Timing
# ---------------------------------------------------------------------------

class TestScoreTiming:

    def test_urgent_within_window(self):
        r = score_timing(_now_minus(1.5))
        assert r.score == "URGENT"

    def test_urgent_at_boundary(self):
        r = score_timing(_now_minus(2.9))
        assert r.score == "URGENT"

    def test_recent_same_day_outside_window(self):
        r = score_timing(_now_minus(6))
        assert r.score == "RECENT"

    def test_normal_older_than_24h(self):
        r = score_timing(_days_ago(3))
        assert r.score == "NORMAL"

    def test_unknown_invalid_date(self):
        r = score_timing("not-a-date")
        assert r.score == "UNKNOWN"

    def test_unknown_none_date(self):
        r = score_timing(None)
        assert r.score == "UNKNOWN"

    def test_date_only_format_parsed(self):
        # date-only strings filed today should be RECENT or URGENT
        today = datetime.now().strftime("%Y-%m-%d")
        r = score_timing(today)
        assert r.score in ("URGENT", "RECENT")

    def test_old_date_is_normal(self):
        r = score_timing("2020-01-01")
        assert r.score == "NORMAL"


# ---------------------------------------------------------------------------
# Confidence score
# ---------------------------------------------------------------------------

class TestCalculateConfidence:

    def test_perfect_score(self):
        # All best outcomes → 100
        score = calculate_confidence("PASS", "PASS", "STRONG_PASS", "PASS", "URGENT")
        assert score == 100

    def test_all_fail(self):
        score = calculate_confidence("FAIL", "FAIL", "UNKNOWN", "FAIL", "UNKNOWN")
        assert score == 0

    def test_partial_materiality(self):
        # small_cap=15, materiality=15 (half of 30), sector=10 (half of 20), cannibal=0, timing=0
        score = calculate_confidence("PASS", "PARTIAL", "PASS", "FAIL", "UNKNOWN")
        assert score == 15 + 15 + 10  # = 40

    def test_recent_timing(self):
        # timing = RECENT → 0.6 * 15 = 9
        score = calculate_confidence("FAIL", "FAIL", "UNKNOWN", "UNKNOWN", "RECENT")
        assert score == round(0.6 * 15)

    def test_normal_timing(self):
        score = calculate_confidence("FAIL", "FAIL", "UNKNOWN", "UNKNOWN", "NORMAL")
        assert score == round(0.3 * 15)

    def test_strong_pass_sector_full_weight(self):
        score = calculate_confidence("FAIL", "FAIL", "STRONG_PASS", "UNKNOWN", "UNKNOWN")
        assert score == 20  # full sector weight

    def test_pass_sector_half_weight(self):
        score = calculate_confidence("FAIL", "FAIL", "PASS", "UNKNOWN", "UNKNOWN")
        assert score == 10  # half of 20


# ---------------------------------------------------------------------------
# Overall signal
# ---------------------------------------------------------------------------

class TestCalculateSignal:

    def test_strong_c1_c2_c3_c4(self):
        sig = calculate_signal("PASS", "PASS", "PASS", "PASS", "NORMAL")
        assert sig == "STRONG"

    def test_strong_c1_c2_c3_c5(self):
        sig = calculate_signal("PASS", "PASS", "STRONG_PASS", "FAIL", "URGENT")
        assert sig == "STRONG"

    def test_strong_all_five(self):
        sig = calculate_signal("PASS", "PASS", "STRONG_PASS", "PASS", "URGENT")
        assert sig == "STRONG"

    def test_moderate_c1_c2_c3_only(self):
        sig = calculate_signal("PASS", "PASS", "PASS", "FAIL", "NORMAL")
        assert sig == "MODERATE"

    def test_moderate_c1_c2_c4_only(self):
        sig = calculate_signal("PASS", "PASS", "UNKNOWN", "PASS", "NORMAL")
        assert sig == "MODERATE"

    def test_moderate_c1_c2_c5_only(self):
        sig = calculate_signal("PASS", "PASS", "UNKNOWN", "FAIL", "RECENT")
        assert sig == "MODERATE"

    def test_weak_c1_c2_only(self):
        sig = calculate_signal("PASS", "PASS", "UNKNOWN", "FAIL", "NORMAL")
        assert sig == "WEAK"

    def test_no_signal_c1_fails(self):
        sig = calculate_signal("FAIL", "PASS", "STRONG_PASS", "PASS", "URGENT")
        assert sig == "NO_SIGNAL"

    def test_no_signal_c2_fails(self):
        sig = calculate_signal("PASS", "FAIL", "STRONG_PASS", "PASS", "URGENT")
        assert sig == "NO_SIGNAL"

    def test_no_signal_both_fail(self):
        sig = calculate_signal("FAIL", "FAIL", "STRONG_PASS", "PASS", "URGENT")
        assert sig == "NO_SIGNAL"

    def test_partial_materiality_is_no_signal(self):
        # PARTIAL on C2 is not a PASS — should be NO_SIGNAL
        sig = calculate_signal("PASS", "PARTIAL", "STRONG_PASS", "PASS", "URGENT")
        assert sig == "NO_SIGNAL"


# ---------------------------------------------------------------------------
# score_trade integration
# ---------------------------------------------------------------------------

class TestScoreTrade:

    def _strong_trade(self):
        transaction = {
            "id": 1,
            "total_value": 2_000_000,
            "shares": 20_000,
            "shares_owned_before": 50_000,    # +40% position
            "is_open_market_purchase": 1,
        }
        enriched = {
            "market_cap": 100_000_000,         # $100M — small cap
            "sector": "Biotechnology",
            "industry": "Drug Manufacturers",
            "share_count_change_pct": -3.0,    # 3% reduction
        }
        filing = {"date_filed": _now_minus(1)}  # filed 1 hour ago → URGENT
        return transaction, enriched, filing

    def test_strong_signal_returned(self):
        t, e, f = self._strong_trade()
        result = score_trade(t, e, f)
        assert result["overall_signal"] == "STRONG"

    def test_is_urgent_flag_set(self):
        t, e, f = self._strong_trade()
        result = score_trade(t, e, f)
        assert result["is_urgent"] == 1

    def test_confidence_above_80_for_strong(self):
        t, e, f = self._strong_trade()
        result = score_trade(t, e, f)
        assert result["confidence_score"] >= 80

    def test_all_score_keys_present(self):
        t, e, f = self._strong_trade()
        result = score_trade(t, e, f)
        expected_keys = {
            "transaction_id",
            "score_small_cap", "score_materiality", "score_sector",
            "score_cannibal", "score_timing",
            "reasoning_small_cap", "reasoning_materiality", "reasoning_sector",
            "reasoning_cannibal", "reasoning_timing",
            "overall_signal", "confidence_score", "is_urgent",
        }
        assert expected_keys == set(result.keys())

    def test_no_signal_large_cap(self):
        t, e, f = self._strong_trade()
        e["market_cap"] = 10_000_000_000   # $10B — large cap
        result = score_trade(t, e, f)
        assert result["overall_signal"] == "NO_SIGNAL"
        assert result["score_small_cap"] == "FAIL"

    def test_no_signal_non_open_market(self):
        t, e, f = self._strong_trade()
        t["is_open_market_purchase"] = 0
        result = score_trade(t, e, f)
        assert result["overall_signal"] == "NO_SIGNAL"
        assert result["score_materiality"] == "FAIL"

    def test_reasonings_are_strings(self):
        t, e, f = self._strong_trade()
        result = score_trade(t, e, f)
        for key in ("reasoning_small_cap", "reasoning_materiality",
                    "reasoning_sector", "reasoning_cannibal", "reasoning_timing"):
            assert isinstance(result[key], str)
            assert result[key]

    def test_unknown_enriched_data(self):
        """Missing enrichment data should degrade gracefully, not raise."""
        t = {
            "id": 99,
            "total_value": 2_000_000,
            "shares": 10_000,
            "shares_owned_before": 50_000,
            "is_open_market_purchase": 1,
        }
        e = {
            "market_cap": None,
            "sector": None,
            "industry": None,
            "share_count_change_pct": None,
        }
        f = {"date_filed": _days_ago(5)}
        result = score_trade(t, e, f)
        assert result["score_small_cap"] == "UNKNOWN"
        assert result["score_sector"] == "UNKNOWN"
        assert result["score_cannibal"] == "UNKNOWN"
        # C1 unknown → NO_SIGNAL
        assert result["overall_signal"] == "NO_SIGNAL"
