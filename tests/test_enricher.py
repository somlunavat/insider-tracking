"""
Unit tests for ingestion/enricher.py

Run with: python -m pytest tests/ -v
"""

import pytest
import sqlite3
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd

import config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_series(values: dict, tz=None) -> pd.Series:
    """Build a pandas Series keyed by datetime for shares history tests."""
    index = pd.to_datetime(list(values.keys()))
    if tz:
        index = index.tz_localize(tz)
    return pd.Series(list(values.values()), index=index, dtype=float)


def _days_ago(n: int) -> str:
    """Return an ISO date string for N days before today."""
    return (datetime.now() - timedelta(days=n)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Tests: share count change calculation
# ---------------------------------------------------------------------------

class TestShareCountChange:

    def test_reduction_calculates_negative_pct(self):
        """Shares went from 10M to 9M → -10% change."""
        shares_current = 9_000_000.0
        shares_12mo_ago = 10_000_000.0
        change = ((shares_current - shares_12mo_ago) / shares_12mo_ago) * 100
        assert change == pytest.approx(-10.0)

    def test_increase_calculates_positive_pct(self):
        shares_current = 11_000_000.0
        shares_12mo_ago = 10_000_000.0
        change = ((shares_current - shares_12mo_ago) / shares_12mo_ago) * 100
        assert change == pytest.approx(10.0)

    def test_flat_is_zero(self):
        shares_current = 10_000_000.0
        shares_12mo_ago = 10_000_000.0
        change = ((shares_current - shares_12mo_ago) / shares_12mo_ago) * 100
        assert change == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Tests: fetch_market_data (yfinance mocked)
# ---------------------------------------------------------------------------

class TestFetchMarketData:

    def test_returns_correct_fields(self):
        from ingestion.enricher import fetch_market_data
        mock_info = {
            "marketCap": 250_000_000,
            "sector": "Biotechnology",
            "industry": "Drug Manufacturers",
        }
        with patch("ingestion.enricher.yf.Ticker") as mock_ticker:
            mock_ticker.return_value.info = mock_info
            result = fetch_market_data("ACME")

        assert result["market_cap"] == 250_000_000
        assert result["sector"] == "Biotechnology"
        assert result["industry"] == "Drug Manufacturers"

    def test_returns_nones_on_exception(self):
        from ingestion.enricher import fetch_market_data
        # Make yf.Ticker() itself raise so the except block is triggered
        with patch("ingestion.enricher.yf.Ticker", side_effect=Exception("API down")):
            result = fetch_market_data("BADTICK")

        assert result == {"market_cap": None, "sector": None, "industry": None}

    def test_handles_missing_keys_gracefully(self):
        from ingestion.enricher import fetch_market_data
        with patch("ingestion.enricher.yf.Ticker") as mock_ticker:
            mock_ticker.return_value.info = {}   # no marketCap, sector, etc.
            result = fetch_market_data("SPARSE")

        assert result["market_cap"] is None
        assert result["sector"] is None
        assert result["industry"] is None


# ---------------------------------------------------------------------------
# Tests: fetch_shares_history_yfinance (mocked)
# ---------------------------------------------------------------------------

class TestFetchSharesHistoryYfinance:

    def _patch_get_shares_full(self, series):
        mock = MagicMock()
        mock.get_shares_full.return_value = series
        return patch("ingestion.enricher.yf.Ticker", return_value=mock)

    def test_returns_current_and_year_ago(self):
        from ingestion.enricher import fetch_shares_history_yfinance
        # Use relative dates so the test doesn't break as time passes
        series = _make_series({
            _days_ago(400): 10_000_000,   # > 1yr ago — should become year_ago
            _days_ago(30):  9_200_000,
            _days_ago(1):   9_100_000,    # most recent — should become current
        })
        with self._patch_get_shares_full(series):
            current, year_ago = fetch_shares_history_yfinance("ACME")

        assert current == pytest.approx(9_100_000.0)
        assert year_ago == pytest.approx(10_000_000.0)

    def test_returns_none_none_on_empty_series(self):
        from ingestion.enricher import fetch_shares_history_yfinance
        with self._patch_get_shares_full(pd.Series([], dtype=float)):
            current, year_ago = fetch_shares_history_yfinance("EMPTY")

        assert current is None
        assert year_ago is None

    def test_returns_none_none_on_none_series(self):
        from ingestion.enricher import fetch_shares_history_yfinance
        with self._patch_get_shares_full(None):
            current, year_ago = fetch_shares_history_yfinance("NONE")

        assert current is None
        assert year_ago is None

    def test_returns_none_none_on_exception(self):
        from ingestion.enricher import fetch_shares_history_yfinance
        mock = MagicMock()
        mock.get_shares_full.side_effect = Exception("network error")
        with patch("ingestion.enricher.yf.Ticker", return_value=mock):
            current, year_ago = fetch_shares_history_yfinance("ERR")

        assert current is None
        assert year_ago is None

    def test_falls_back_to_first_value_when_no_data_older_than_1yr(self):
        """If all data is recent (< 1yr old), year_ago should be the oldest available."""
        from ingestion.enricher import fetch_shares_history_yfinance
        series = _make_series({
            _days_ago(60): 9_500_000,   # both < 1yr ago
            _days_ago(30): 9_300_000,
        })
        with self._patch_get_shares_full(series):
            current, year_ago = fetch_shares_history_yfinance("NEW")

        assert current == pytest.approx(9_300_000.0)
        assert year_ago == pytest.approx(9_500_000.0)  # oldest available

    def test_handles_timezone_aware_series(self):
        """yfinance returns TZ-aware Series; comparison must not raise."""
        from ingestion.enricher import fetch_shares_history_yfinance
        series = _make_series({
            _days_ago(400): 10_000_000,
            _days_ago(1):   9_000_000,
        }, tz="America/New_York")
        with self._patch_get_shares_full(series):
            current, year_ago = fetch_shares_history_yfinance("TZ")

        assert current == pytest.approx(9_000_000.0)
        assert year_ago == pytest.approx(10_000_000.0)


# ---------------------------------------------------------------------------
# Tests: fetch_shares_history_fmp (requests mocked)
# ---------------------------------------------------------------------------

class TestFetchSharesHistoryFmp:

    def test_returns_none_none_without_api_key(self, monkeypatch):
        from ingestion.enricher import fetch_shares_history_fmp
        monkeypatch.setattr(config, "FMP_API_KEY", "")
        current, year_ago = fetch_shares_history_fmp("ACME")
        assert current is None
        assert year_ago is None

    def test_parses_fmp_response(self, monkeypatch):
        from ingestion.enricher import fetch_shares_history_fmp
        monkeypatch.setattr(config, "FMP_API_KEY", "test_key")

        fmp_data = [
            {"date": "2025-01-01", "weightedAverageShsOut": 9_000_000},
            {"date": "2024-10-01", "weightedAverageShsOut": 9_100_000},
            {"date": "2024-07-01", "weightedAverageShsOut": 9_300_000},
            {"date": "2024-04-01", "weightedAverageShsOut": 9_500_000},
            {"date": "2024-01-01", "weightedAverageShsOut": 10_000_000},
        ]

        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = fmp_data

        with patch("ingestion.enricher.requests.get", return_value=mock_resp):
            current, year_ago = fetch_shares_history_fmp("ACME")

        assert current == pytest.approx(9_000_000.0)
        assert year_ago == pytest.approx(10_000_000.0)  # index [4] = 4 quarters ago

    def test_returns_none_none_on_empty_response(self, monkeypatch):
        from ingestion.enricher import fetch_shares_history_fmp
        monkeypatch.setattr(config, "FMP_API_KEY", "test_key")

        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = []

        with patch("ingestion.enricher.requests.get", return_value=mock_resp):
            current, year_ago = fetch_shares_history_fmp("ACME")

        assert current is None
        assert year_ago is None

    def test_returns_none_none_on_request_exception(self, monkeypatch):
        from ingestion.enricher import fetch_shares_history_fmp
        monkeypatch.setattr(config, "FMP_API_KEY", "test_key")

        with patch("ingestion.enricher.requests.get", side_effect=Exception("timeout")):
            current, year_ago = fetch_shares_history_fmp("ERR")

        assert current is None
        assert year_ago is None


# ---------------------------------------------------------------------------
# Tests: enrich_ticker (cache + orchestration)
# ---------------------------------------------------------------------------

class TestEnrichTicker:

    def setup_method(self):
        """Clear the in-run cache before each test."""
        import ingestion.enricher as mod
        mod._ticker_cache.clear()

    def _mock_enricher(self, market, yf_shares, fmp_shares=None):
        """Patch the three data-fetching functions and return context."""
        from unittest.mock import patch
        patches = [
            patch("ingestion.enricher.fetch_market_data", return_value=market),
            patch("ingestion.enricher.fetch_shares_history_yfinance", return_value=yf_shares),
        ]
        if fmp_shares is not None:
            patches.append(
                patch("ingestion.enricher.fetch_shares_history_fmp", return_value=fmp_shares)
            )
        return patches

    def test_combines_market_and_shares_data(self):
        from ingestion.enricher import enrich_ticker
        market = {"market_cap": 300_000_000, "sector": "Biotechnology", "industry": "Drug Dev"}
        yf_shares = (9_000_000.0, 10_000_000.0)

        with patch("ingestion.enricher.fetch_market_data", return_value=market), \
             patch("ingestion.enricher.fetch_shares_history_yfinance", return_value=yf_shares):
            result = enrich_ticker("ACME")

        assert result["market_cap"] == 300_000_000
        assert result["sector"] == "Biotechnology"
        assert result["shares_outstanding_current"] == 9_000_000.0
        assert result["shares_outstanding_12mo_ago"] == 10_000_000.0
        assert result["share_count_change_pct"] == pytest.approx(-10.0)

    def test_falls_back_to_fmp_when_yfinance_has_no_shares(self):
        from ingestion.enricher import enrich_ticker
        market = {"market_cap": 200_000_000, "sector": "Gold", "industry": "Mining"}

        with patch("ingestion.enricher.fetch_market_data", return_value=market), \
             patch("ingestion.enricher.fetch_shares_history_yfinance", return_value=(None, None)), \
             patch("ingestion.enricher.fetch_shares_history_fmp", return_value=(8_000_000.0, 9_000_000.0)):
            result = enrich_ticker("GOLD")

        assert result["shares_outstanding_current"] == 8_000_000.0
        assert result["share_count_change_pct"] == pytest.approx(-11.111, abs=0.01)

    def test_change_pct_is_none_when_shares_unavailable(self):
        from ingestion.enricher import enrich_ticker
        market = {"market_cap": 100_000_000, "sector": "Tech", "industry": "Software"}

        with patch("ingestion.enricher.fetch_market_data", return_value=market), \
             patch("ingestion.enricher.fetch_shares_history_yfinance", return_value=(None, None)), \
             patch("ingestion.enricher.fetch_shares_history_fmp", return_value=(None, None)):
            result = enrich_ticker("NOSHARES")

        assert result["share_count_change_pct"] is None

    def test_caches_result_for_same_ticker(self):
        from ingestion.enricher import enrich_ticker
        market = {"market_cap": 100_000_000, "sector": "Tech", "industry": "SW"}
        yf_shares = (5_000_000.0, 5_500_000.0)

        with patch("ingestion.enricher.fetch_market_data", return_value=market) as m_market, \
             patch("ingestion.enricher.fetch_shares_history_yfinance", return_value=yf_shares):
            enrich_ticker("CACHED")
            enrich_ticker("CACHED")   # second call should hit cache

        # Market data should only be fetched once
        assert m_market.call_count == 1

    def test_different_tickers_are_cached_separately(self):
        from ingestion.enricher import enrich_ticker
        market_a = {"market_cap": 100_000_000, "sector": "Gold", "industry": "Mining"}
        market_b = {"market_cap": 200_000_000, "sector": "Biotech", "industry": "Drug Dev"}

        with patch("ingestion.enricher.fetch_market_data", side_effect=[market_a, market_b]), \
             patch("ingestion.enricher.fetch_shares_history_yfinance", return_value=(None, None)), \
             patch("ingestion.enricher.fetch_shares_history_fmp", return_value=(None, None)):
            result_a = enrich_ticker("GOLD")
            result_b = enrich_ticker("BIO")

        assert result_a["sector"] == "Gold"
        assert result_b["sector"] == "Biotech"
