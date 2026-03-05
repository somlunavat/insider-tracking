"""
Unit tests for ingestion/edgar.py

Run with:  python -m pytest tests/ -v
"""

import pytest
from ingestion.edgar import parse_form4_xml, _text, _float
import xml.etree.ElementTree as ET


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_FORM4_XML = """<?xml version="1.0"?>
<ownershipDocument>
  <schemaVersion>X0508</schemaVersion>
  <documentType>4</documentType>
  <periodOfReport>2025-01-14</periodOfReport>
  <issuer>
    <issuerCik>0001234567</issuerCik>
    <issuerName>Acme Biotech Inc</issuerName>
    <issuerTradingSymbol>ACME</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0000987654</rptOwnerCik>
      <rptOwnerName>Jane Smith</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>0</isDirector>
      <isOfficer>1</isOfficer>
      <officerTitle>Chief Executive Officer</officerTitle>
      <isTenPercentOwner>0</isTenPercentOwner>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2025-01-14</value></transactionDate>
      <transactionCoding>
        <transactionFormType>4</transactionFormType>
        <transactionCode>P</transactionCode>
        <equitySwapInvolved>0</equitySwapInvolved>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>50000</value></transactionShares>
        <transactionPricePerShare><value>22.50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>150000</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2025-01-10</value></transactionDate>
      <transactionCoding>
        <transactionFormType>4</transactionFormType>
        <transactionCode>F</transactionCode>
        <equitySwapInvolved>0</equitySwapInvolved>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>5000</value></transactionShares>
        <transactionPricePerShare><value>21.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>100000</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>"""


SAMPLE_FORM4_NO_TRANSACTIONS = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2025-01-14</periodOfReport>
  <issuer>
    <issuerCik>0001111111</issuerCik>
    <issuerName>Empty Corp</issuerName>
    <issuerTradingSymbol>EMPT</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerName>Bob Jones</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>1</isDirector>
      <isOfficer>0</isOfficer>
      <isTenPercentOwner>0</isTenPercentOwner>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
  </nonDerivativeTable>
</ownershipDocument>"""


MALFORMED_XML = "this is not xml <<<"


SAMPLE_NAMESPACED_XML = """<?xml version="1.0"?>
<ns:ownershipDocument xmlns:ns="http://www.sec.gov/form4">
  <ns:documentType>4</ns:documentType>
  <ns:periodOfReport>2025-01-14</ns:periodOfReport>
  <ns:issuer>
    <ns:issuerCik>0009999999</ns:issuerCik>
    <ns:issuerName>Namespaced Mining Co</ns:issuerName>
    <ns:issuerTradingSymbol>NMC</ns:issuerTradingSymbol>
  </ns:issuer>
  <ns:reportingOwner>
    <ns:reportingOwnerId>
      <ns:rptOwnerName>Alice Miner</ns:rptOwnerName>
    </ns:reportingOwnerId>
    <ns:reportingOwnerRelationship>
      <ns:isDirector>1</ns:isDirector>
      <ns:isOfficer>0</ns:isOfficer>
      <ns:isTenPercentOwner>0</ns:isTenPercentOwner>
    </ns:reportingOwnerRelationship>
  </ns:reportingOwner>
  <ns:nonDerivativeTable>
    <ns:nonDerivativeTransaction>
      <ns:securityTitle><ns:value>Common Stock</ns:value></ns:securityTitle>
      <ns:transactionDate><ns:value>2025-01-14</ns:value></ns:transactionDate>
      <ns:transactionCoding>
        <ns:transactionCode>P</ns:transactionCode>
      </ns:transactionCoding>
      <ns:transactionAmounts>
        <ns:transactionShares><ns:value>10000</ns:value></ns:transactionShares>
        <ns:transactionPricePerShare><ns:value>5.00</ns:value></ns:transactionPricePerShare>
        <ns:transactionAcquiredDisposedCode><ns:value>A</ns:value></ns:transactionAcquiredDisposedCode>
      </ns:transactionAmounts>
      <ns:postTransactionAmounts>
        <ns:sharesOwnedFollowingTransaction><ns:value>10000</ns:value></ns:sharesOwnedFollowingTransaction>
      </ns:postTransactionAmounts>
    </ns:nonDerivativeTransaction>
  </ns:nonDerivativeTable>
</ns:ownershipDocument>"""


# ---------------------------------------------------------------------------
# Tests: parse_form4_xml
# ---------------------------------------------------------------------------

class TestParseForm4Xml:

    def test_returns_none_on_malformed_xml(self):
        result = parse_form4_xml(MALFORMED_XML, "0000000000-25-000001", "2025-01-14")
        assert result is None

    def test_returns_none_when_no_transactions(self):
        result = parse_form4_xml(SAMPLE_FORM4_NO_TRANSACTIONS, "0000000000-25-000002", "2025-01-14")
        assert result is None

    def test_parses_issuer_fields(self):
        result = parse_form4_xml(SAMPLE_FORM4_XML, "0000987654-25-000001", "2025-01-15")
        assert result is not None
        f = result["filing"]
        assert f["company_name"] == "Acme Biotech Inc"
        assert f["ticker"] == "ACME"
        assert f["cik"] == "0001234567"

    def test_parses_insider_fields(self):
        result = parse_form4_xml(SAMPLE_FORM4_XML, "0000987654-25-000001", "2025-01-15")
        f = result["filing"]
        assert f["insider_name"] == "Jane Smith"
        assert f["insider_title"] == "Chief Executive Officer"
        assert f["is_officer"] == 1
        assert f["is_director"] == 0
        assert f["is_ten_pct_owner"] == 0

    def test_parses_all_transactions(self):
        result = parse_form4_xml(SAMPLE_FORM4_XML, "0000987654-25-000001", "2025-01-15")
        assert len(result["transactions"]) == 2

    def test_flags_open_market_purchase(self):
        result = parse_form4_xml(SAMPLE_FORM4_XML, "0000987654-25-000001", "2025-01-15")
        txns = result["transactions"]
        purchase = next(t for t in txns if t["transaction_code"] == "P")
        tax_withhold = next(t for t in txns if t["transaction_code"] == "F")
        assert purchase["is_open_market_purchase"] == 1
        assert tax_withhold["is_open_market_purchase"] == 0

    def test_open_market_purchase_values(self):
        result = parse_form4_xml(SAMPLE_FORM4_XML, "0000987654-25-000001", "2025-01-15")
        purchase = next(t for t in result["transactions"] if t["transaction_code"] == "P")
        assert purchase["shares"] == 50_000.0
        assert purchase["price_per_share"] == 22.50
        assert purchase["total_value"] == pytest.approx(1_125_000.0)
        assert purchase["acquired_disposed"] == "A"
        assert purchase["shares_owned_after"] == 150_000.0
        assert purchase["shares_owned_before"] == 100_000.0  # 150k - 50k

    def test_shares_before_calculated_for_disposal(self):
        result = parse_form4_xml(SAMPLE_FORM4_XML, "0000987654-25-000001", "2025-01-15")
        withhold = next(t for t in result["transactions"] if t["transaction_code"] == "F")
        # shares_after=100k, shares=5k, disposed → shares_before = 100k + 5k = 105k
        assert withhold["shares_owned_before"] == 105_000.0

    def test_filing_url_constructed_correctly(self):
        accession = "0000987654-25-000001"
        result = parse_form4_xml(SAMPLE_FORM4_XML, accession, "2025-01-15")
        url = result["filing"]["filing_url"]
        assert "edgar/data/0001234567" in url
        assert "000098765425000001" in url  # no dashes
        assert url.endswith("-index.htm")

    def test_handles_xml_namespace(self):
        result = parse_form4_xml(SAMPLE_NAMESPACED_XML, "0000000001-25-000001", "2025-01-14")
        assert result is not None
        assert result["filing"]["company_name"] == "Namespaced Mining Co"
        assert len(result["transactions"]) == 1
        assert result["transactions"][0]["is_open_market_purchase"] == 1

    def test_director_title_fallback(self):
        result = parse_form4_xml(SAMPLE_FORM4_NO_TRANSACTIONS, "0000000000-25-000002", "2025-01-14")
        # returns None because no transactions — but we can test the title logic
        # by creating a version with a transaction
        xml_with_txn = SAMPLE_FORM4_NO_TRANSACTIONS.replace(
            "<ns:nonDerivativeTable>\n  </ns:nonDerivativeTable>", ""
        ).replace(
            "<nonDerivativeTable>\n  </nonDerivativeTable>",
            """<nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2025-01-14</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1000</value></transactionShares>
        <transactionPricePerShare><value>10.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>1000</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>"""
        )
        result = parse_form4_xml(xml_with_txn, "0000000000-25-000003", "2025-01-14")
        assert result is not None
        assert result["filing"]["insider_title"] == "Director"

    def test_shares_owned_before_never_negative(self):
        """First purchase — insider had 0 shares before; should not go negative."""
        xml = SAMPLE_NAMESPACED_XML  # insider buys 10k, post=10k → pre=0
        result = parse_form4_xml(xml, "0000000001-25-000001", "2025-01-14")
        assert result["transactions"][0]["shares_owned_before"] == 0.0

    def test_period_of_report_parsed(self):
        result = parse_form4_xml(SAMPLE_FORM4_XML, "0000987654-25-000001", "2025-01-15")
        assert result["filing"]["period_of_report"] == "2025-01-14"


# ---------------------------------------------------------------------------
# Tests: helper functions
# ---------------------------------------------------------------------------

class TestHelpers:

    def _el(self, xml_str: str):
        return ET.fromstring(xml_str)

    def test_text_reads_value_child(self):
        el = self._el("<root><child><value>hello</value></child></root>")
        assert _text(el, "child") == "hello"

    def test_text_returns_empty_on_missing_path(self):
        el = self._el("<root></root>")
        assert _text(el, "missing") == ""

    def test_float_converts_string(self):
        el = self._el("<root><num><value>42.5</value></num></root>")
        assert _float(el, "num") == pytest.approx(42.5)

    def test_float_returns_none_on_empty(self):
        el = self._el("<root><num><value></value></num></root>")
        assert _float(el, "num") is None

    def test_float_returns_none_on_missing(self):
        el = self._el("<root></root>")
        assert _float(el, "missing") is None
