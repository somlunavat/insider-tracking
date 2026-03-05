"""
EDGAR Form 4 ingestion pipeline.

Flow:
  1. fetch_form4_metadata()  — EDGAR EFTS search API → list of {accession_number, filer_cik, ...}
  2. fetch_form4_xml()       — fetch + locate the XML document inside each filing
  3. parse_form4_xml()       — parse XML into filing + transactions dicts
  4. store_filing()          — write to SQLite (idempotent via UNIQUE on accession_number)
  5. ingest_new_filings()    — orchestrates 1-4 for a date range
"""

import re
import time
import logging
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from typing import Optional

import requests

import config
from database.db import get_db

logger = logging.getLogger(__name__)

# --- Constants -----------------------------------------------------------

EFTS_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_DATA_URL = "https://www.sec.gov/Archives/edgar/data"

# SEC enforces <10 req/s; 0.12 s gives ~8 req/s with headroom
REQUEST_DELAY = 0.12

TRANSACTION_CODE_MAP = {
    "P": "Open Market Purchase",
    "S": "Open Market Sale",
    "A": "Grant/Award/Other Acquisition",
    "D": "Sale Back to Company",
    "F": "Tax Withholding (Share Surrender)",
    "G": "Gift",
    "M": "Exercise of Derivative",
    "X": "Exercise of In-the-Money Derivative",
    "J": "Other Acquisition or Disposition",
    "K": "Equity Swap",
    "U": "Disposition via Tender Offer",
}


# --- HTTP helpers ---------------------------------------------------------

def _headers() -> dict:
    """SEC requires a User-Agent with real contact info."""
    return {
        "User-Agent": f"{config.SEC_USER_NAME} {config.SEC_USER_EMAIL}",
        "Accept-Encoding": "gzip, deflate",
    }


def _get(url: str, params: dict = None) -> Optional[requests.Response]:
    try:
        resp = requests.get(url, params=params, headers=_headers(), timeout=30)
        resp.raise_for_status()
        time.sleep(REQUEST_DELAY)
        return resp
    except requests.RequestException as e:
        logger.error("HTTP error [%s]: %s", url, e)
        return None


# --- Step 1: Fetch filing metadata from EDGAR EFTS search ----------------

def fetch_form4_metadata(since_date: date, until_date: date = None) -> list[dict]:
    """
    Search EDGAR EFTS for Form 4 filings in a date range.

    Returns list of:
        {accession_number, filer_cik, entity_name, date_filed}

    The filer_cik is extracted from the accession number prefix (first 10 digits),
    which for Form 4 is the reporting owner's CIK — used to build the filing URL.
    """
    if until_date is None:
        until_date = date.today()

    results = []
    page_size = 100
    from_offset = 0

    while True:
        resp = _get(EFTS_SEARCH_URL, params={
            "forms": "4",
            "dateRange": "custom",
            "startdt": since_date.isoformat(),
            "enddt": until_date.isoformat(),
            "from": from_offset,
        })
        if resp is None:
            break

        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        total = data.get("hits", {}).get("total", {}).get("value", 0)

        if not hits:
            break

        logger.debug("EFTS page offset=%d total=%d returned=%d", from_offset, total, len(hits))

        for hit in hits:
            source = hit.get("_source", {})

            # Prefer the clean `adsh` field; fall back to stripping `:doc.xml` from _id
            accession_number = source.get("adsh") or hit.get("_id", "").split(":")[0]
            if not accession_number:
                continue

            # First 10 digits of the accession number (no dashes) are the filer CIK.
            # For Form 4 this is the issuer CIK — used to build the filing index URL.
            filer_cik = str(int(accession_number.replace("-", "")[:10]))

            # display_names contains both company and insider names
            # e.g. ["Smith John  (CIK 0001406033)", "ACME CORP  (CIK 0000104918)"]
            display_names = source.get("display_names", [])
            entity_name = "; ".join(display_names) if display_names else ""

            results.append({
                "accession_number": accession_number,
                "filer_cik": filer_cik,
                "entity_name": entity_name,
                "date_filed": source.get("file_date", ""),
            })

        from_offset += len(hits)
        if from_offset >= total or len(hits) < page_size:
            break

    logger.info("EFTS search [%s → %s]: found %d Form 4 filings", since_date, until_date, len(results))
    return results


# --- Step 2: Fetch the Form 4 XML document --------------------------------

def _find_xml_url_in_index(filer_cik: str, accession_number: str, index_html: str) -> Optional[str]:
    """
    Scan the filing index HTML for the raw Form 4 XML document.

    EDGAR index pages use absolute paths (e.g. /Archives/edgar/data/...).
    Two XML links are typically present: a raw .xml and an XSL-styled version
    inside an xslXXXX/ subdirectory. We want the raw one.
    """
    accession_nodash = accession_number.replace("-", "")
    base = f"{EDGAR_DATA_URL}/{filer_cik}/{accession_nodash}"

    for href in re.findall(r'href="([^"]+\.xml)"', index_html, re.IGNORECASE):
        lower = href.lower()
        # Skip schema files and XSL-transformed styled views
        if any(skip in lower for skip in ("xsd", "schema", "/xsl")):
            continue
        if href.startswith("http"):
            return href
        elif href.startswith("/"):
            return f"https://www.sec.gov{href}"
        else:
            return f"{base}/{href}"

    return None


def fetch_form4_xml(filer_cik: str, accession_number: str) -> Optional[str]:
    """
    Locate and download the Form 4 XML document for a given filing.

    Strategy:
      1. Fetch the filing index HTML (standard EDGAR index page).
      2. Extract the .xml document link.
      3. Fetch the XML.
    """
    accession_nodash = accession_number.replace("-", "")
    index_url = (
        f"{EDGAR_DATA_URL}/{filer_cik}/{accession_nodash}/{accession_number}-index.htm"
    )

    resp = _get(index_url)
    if resp is None:
        return None

    xml_url = _find_xml_url_in_index(filer_cik, accession_number, resp.text)
    if not xml_url:
        logger.warning("No XML document found in index for %s", accession_number)
        return None

    xml_resp = _get(xml_url)
    return xml_resp.text if xml_resp else None


# --- Step 3: Parse Form 4 XML into structured data ------------------------

def _text(element, path: str) -> str:
    """
    Navigate to `path` and return the text of its <value> child (or the element
    itself if no <value> child exists). Returns "" on any miss.
    """
    node = element.find(path)
    if node is None:
        return ""
    value = node.find("value")
    text = (value.text if value is not None else node.text) or ""
    return text.strip()


def _float(element, path: str) -> Optional[float]:
    raw = _text(element, path)
    try:
        return float(raw) if raw else None
    except ValueError:
        return None


def parse_form4_xml(xml_content: str, accession_number: str, date_filed: str) -> Optional[dict]:
    """
    Parse Form 4 XML into a dict with keys:
        filing      — dict matching the filings table columns
        transactions — list of dicts matching the transactions table columns

    Returns None if:
        - XML is malformed
        - Required issuer or owner elements are missing
        - The filing has zero transactions

    All transaction types are stored (not just open market purchases).
    The `is_open_market_purchase` flag marks the ones that matter for scoring.
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        logger.error("XML parse error [%s]: %s", accession_number, e)
        return None

    # Strip XML namespace if present (some older Form 4s have one)
    # e.g., {http://...}ownershipDocument → ownershipDocument
    tag = root.tag
    if tag.startswith("{"):
        ns = tag[1:tag.index("}")]
        for elem in root.iter():
            elem.tag = elem.tag.replace(f"{{{ns}}}", "")

    # --- Issuer ---
    issuer = root.find("issuer")
    if issuer is None:
        return None

    company_name = (issuer.findtext("issuerName") or "").strip()
    cik = (issuer.findtext("issuerCik") or "").strip()
    ticker_raw = (issuer.findtext("issuerTradingSymbol") or "").strip()
    ticker = ticker_raw.upper() if ticker_raw else None

    if not company_name:
        return None

    # --- Reporting owner (take first if multiple) ---
    owner = root.find("reportingOwner")
    if owner is None:
        return None

    owner_id_el = owner.find("reportingOwnerId")
    insider_name = (owner_id_el.findtext("rptOwnerName") or "").strip() if owner_id_el else ""

    owner_rel = owner.find("reportingOwnerRelationship")
    insider_title = ""
    is_director = is_officer = is_ten_pct_owner = 0

    if owner_rel is not None:
        is_director = 1 if (owner_rel.findtext("isDirector") or "").strip() == "1" else 0
        is_officer = 1 if (owner_rel.findtext("isOfficer") or "").strip() == "1" else 0
        is_ten_pct_owner = 1 if (owner_rel.findtext("isTenPercentOwner") or "").strip() == "1" else 0
        insider_title = (owner_rel.findtext("officerTitle") or "").strip()

    if not insider_title:
        if is_director:
            insider_title = "Director"
        elif is_ten_pct_owner:
            insider_title = "10% Owner"

    accession_nodash = accession_number.replace("-", "")
    filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodash}/{accession_number}-index.htm"

    filing = {
        "accession_number": accession_number,
        "cik": cik,
        "company_name": company_name,
        "ticker": ticker,
        "insider_name": insider_name,
        "insider_title": insider_title,
        "is_director": is_director,
        "is_officer": is_officer,
        "is_ten_pct_owner": is_ten_pct_owner,
        "date_filed": date_filed,
        "period_of_report": (root.findtext("periodOfReport") or "").strip(),
        "filing_url": filing_url,
    }

    # --- Non-derivative transactions ---
    transactions = []
    non_deriv_table = root.find("nonDerivativeTable")

    if non_deriv_table is not None:
        for txn in non_deriv_table.findall("nonDerivativeTransaction"):
            code_node = txn.find("transactionCoding/transactionCode")
            transaction_code = (code_node.text or "").strip() if code_node is not None else ""

            acquired_disposed = _text(txn, "transactionAmounts/transactionAcquiredDisposedCode")
            shares = _float(txn, "transactionAmounts/transactionShares") or 0.0
            price = _float(txn, "transactionAmounts/transactionPricePerShare") or 0.0
            shares_after = _float(txn, "postTransactionAmounts/sharesOwnedFollowingTransaction") or 0.0
            transaction_date = _text(txn, "transactionDate")
            security_title = _text(txn, "securityTitle")

            # Derive shares_before from post-transaction total
            if acquired_disposed == "A":
                shares_before = shares_after - shares
            else:
                shares_before = shares_after + shares

            is_open_market_purchase = int(
                transaction_code == "P" and acquired_disposed == "A"
            )

            transactions.append({
                "security_title": security_title,
                "transaction_date": transaction_date,
                "transaction_code": transaction_code,
                "transaction_type": TRANSACTION_CODE_MAP.get(transaction_code, "Unknown"),
                "shares": shares,
                "price_per_share": price,
                "total_value": shares * price,
                "acquired_disposed": acquired_disposed,
                "shares_owned_after": shares_after,
                "shares_owned_before": max(shares_before, 0.0),
                "is_open_market_purchase": is_open_market_purchase,
            })

    if not transactions:
        return None

    return {"filing": filing, "transactions": transactions}


# --- Step 4: Persist to SQLite --------------------------------------------

def store_filing(filing_data: dict) -> Optional[int]:
    """
    Insert filing and its transactions into SQLite.
    Idempotent: returns None (without error) if accession_number already exists.
    Returns the new filing_id on success.
    """
    f = filing_data["filing"]
    accession = f["accession_number"]

    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM filings WHERE accession_number = ?", (accession,)
        ).fetchone()
        if existing:
            return None

        cursor = conn.execute(
            """
            INSERT INTO filings
                (accession_number, cik, company_name, ticker, insider_name, insider_title,
                 is_director, is_officer, is_ten_pct_owner, date_filed, period_of_report, filing_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f["accession_number"], f["cik"], f["company_name"], f["ticker"],
                f["insider_name"], f["insider_title"], f["is_director"], f["is_officer"],
                f["is_ten_pct_owner"], f["date_filed"], f["period_of_report"], f["filing_url"],
            ),
        )
        filing_id = cursor.lastrowid

        for txn in filing_data["transactions"]:
            conn.execute(
                """
                INSERT INTO transactions
                    (filing_id, security_title, transaction_date, transaction_code,
                     transaction_type, shares, price_per_share, total_value,
                     acquired_disposed, shares_owned_after, shares_owned_before,
                     is_open_market_purchase)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    filing_id, txn["security_title"], txn["transaction_date"],
                    txn["transaction_code"], txn["transaction_type"], txn["shares"],
                    txn["price_per_share"], txn["total_value"], txn["acquired_disposed"],
                    txn["shares_owned_after"], txn["shares_owned_before"],
                    txn["is_open_market_purchase"],
                ),
            )

    return filing_id


# --- Step 5: Orchestrator -------------------------------------------------

def ingest_new_filings(since_date: date = None) -> int:
    """
    Fetch, parse, and store all Form 4 filings since `since_date`.
    Defaults to yesterday if not provided.
    Returns the count of newly stored filings.
    """
    if since_date is None:
        since_date = date.today() - timedelta(days=1)

    logger.info("Ingestion started for filings since %s", since_date)

    metadata_list = fetch_form4_metadata(since_date)
    total = len(metadata_list)
    new_count = skipped = errors = 0

    for i, meta in enumerate(metadata_list, start=1):
        accession_number = meta["accession_number"]
        filer_cik = meta["filer_cik"]
        date_filed = meta["date_filed"]

        # Quick duplicate check before making HTTP requests
        with get_db() as conn:
            if conn.execute(
                "SELECT id FROM filings WHERE accession_number = ?", (accession_number,)
            ).fetchone():
                skipped += 1
                continue

        if i % 50 == 0 or i == total:
            logger.info(
                "Progress: %d/%d | new=%d skipped=%d errors=%d",
                i, total, new_count, skipped, errors,
            )

        xml_content = fetch_form4_xml(filer_cik, accession_number)
        if not xml_content:
            errors += 1
            continue

        parsed = parse_form4_xml(xml_content, accession_number, date_filed)
        if not parsed:
            # No transactions or parse failure — not an error, just skip
            continue

        filing_id = store_filing(parsed)
        if filing_id:
            new_count += 1
            logger.debug(
                "Stored filing %s | %s | %s",
                accession_number,
                parsed["filing"]["ticker"],
                parsed["filing"]["insider_name"],
            )

    logger.info(
        "Ingestion complete: %d new | %d skipped | %d errors (of %d total)",
        new_count, skipped, errors, total,
    )
    return new_count


# --- Utility: query local DB for reactive mode (used by Module 4) --------

def get_filings_by_ticker(ticker: str) -> list[dict]:
    """Return all stored filings + transactions for a given ticker."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT f.accession_number, f.company_name, f.ticker,
                   f.insider_name, f.insider_title, f.date_filed, f.filing_url,
                   t.id as transaction_id, t.transaction_code, t.transaction_type,
                   t.transaction_date, t.shares, t.price_per_share, t.total_value,
                   t.acquired_disposed, t.shares_owned_after, t.shares_owned_before,
                   t.is_open_market_purchase
            FROM filings f
            JOIN transactions t ON t.filing_id = f.id
            WHERE f.ticker = ?
            ORDER BY f.date_filed DESC, t.transaction_date DESC
            """,
            (ticker.upper(),),
        ).fetchall()
    return [dict(row) for row in rows]
