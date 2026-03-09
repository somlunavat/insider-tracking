"""
Module 4 · Page 1 — Reactive Ticker Lookup

Enter a ticker to fetch, enrich, score, and display a full insider report.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from collections import Counter
from datetime import date, timedelta
from typing import Optional

import streamlit as st
import pandas as pd

from database.db import init_db, get_db
from ingestion.edgar import ingest_ticker_filings
from ingestion.enricher import enrich_new_filings
from scoring.engine import score_new_trades
from dashboard._utils import (
    SIGNAL_CSS, SIGNAL_ORDER, fmt_cap, fmt_money, fmt_shares,
    render_signal_banner, render_scorecard,
)

st.set_page_config(page_title="Ticker Lookup — Insider Tracker", page_icon="🔍", layout="wide")
init_db()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_ticker_data(ticker: str, months: int) -> dict:
    since = (date.today() - timedelta(days=months * 30)).isoformat()

    with get_db() as conn:
        rows = conn.execute("""
            SELECT
                f.company_name, f.insider_name, f.insider_title,
                f.date_filed,   f.filing_url,
                t.id            AS transaction_id,
                t.transaction_date, t.shares, t.price_per_share,
                t.total_value,  t.shares_owned_before, t.shares_owned_after,
                s.score_small_cap,   s.score_materiality, s.score_sector,
                s.score_cannibal,    s.score_timing,
                s.reasoning_small_cap,   s.reasoning_materiality,
                s.reasoning_sector,      s.reasoning_cannibal,
                s.reasoning_timing,
                s.overall_signal, s.confidence_score, s.is_urgent,
                e.market_cap, e.sector, e.industry, e.share_count_change_pct
            FROM   filings f
            JOIN   transactions t    ON t.filing_id  = f.id
            LEFT JOIN scored_trades s  ON s.transaction_id = t.id
            LEFT JOIN enriched_data e  ON e.filing_id   = f.id
            WHERE  f.ticker = ?
              AND  t.is_open_market_purchase = 1
              AND  f.date_filed >= ?
            ORDER  BY f.date_filed DESC, t.transaction_date DESC
        """, (ticker, since)).fetchall()

    if not rows:
        return {}

    trades  = [dict(r) for r in rows]
    latest  = trades[0]
    scored  = [t for t in trades if t.get("overall_signal")]

    best_scored = None
    if scored:
        best_scored = max(
            scored,
            key=lambda t: (
                SIGNAL_ORDER.get(t["overall_signal"], 0),
                t.get("confidence_score") or 0,
            ),
        )

    return {
        "trades":      trades,
        "scored":      scored,
        "best_scored": best_scored,
        "best_signal":     best_scored["overall_signal"]  if best_scored else None,
        "best_confidence": best_scored["confidence_score"] if best_scored else None,
        "company_name":    latest["company_name"],
        "market_cap":      latest.get("market_cap"),
        "sector":          latest.get("sector"),
        "industry":        latest.get("industry"),
        "share_count_change_pct": latest.get("share_count_change_pct"),
        "patterns":        _detect_patterns(trades),
    }


def _detect_patterns(trades: list[dict]) -> dict:
    unique_buyers = set(t["insider_name"] for t in trades)
    buyer_counts  = Counter(t["insider_name"] for t in trades)
    top_buyer, top_count = buyer_counts.most_common(1)[0] if buyer_counts else ("—", 0)

    cluster = False
    dated: list[tuple[date, str]] = []
    for t in trades:
        try:
            dated.append((date.fromisoformat(t["date_filed"][:10]), t["insider_name"]))
        except (ValueError, TypeError):
            pass
    dated.sort()
    for i, (d, name) in enumerate(dated):
        window = {name}
        for d2, n2 in dated[i + 1:]:
            if (d2 - d).days <= 30:
                window.add(n2)
            else:
                break
        if len(window) >= 2:
            cluster = True
            break

    oldest: Optional[date] = dated[0][0] if dated else None

    return {
        "unique_buyers":  len(unique_buyers),
        "total_purchases": len(trades),
        "top_buyer":      top_buyer,
        "top_buyer_count": top_count,
        "cluster_buying": cluster,
        "oldest_date":    oldest,
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render_company_metrics(data: dict) -> None:
    chg = data.get("share_count_change_pct")
    chg_s = (
        f"{'▼' if chg < 0 else '▲'}{abs(chg):.1f}% YoY"
        if chg is not None else "N/A"
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Market Cap",          fmt_cap(data.get("market_cap")))
    c2.metric("Sector",              data.get("sector") or "Unknown")
    c3.metric("Industry",            data.get("industry") or "—")
    c4.metric("Share Count Change",  chg_s)


def render_transactions_table(trades: list[dict]) -> None:
    rows = []
    for t in trades:
        rows.append({
            "Filed":        (t.get("date_filed") or "")[:10],
            "Txn Date":     (t.get("transaction_date") or "")[:10],
            "Insider":      t.get("insider_name", "—"),
            "Title":        t.get("insider_title", "—"),
            "Shares":       fmt_shares(t.get("shares")),
            "Price":        f"${t.get('price_per_share') or 0:.2f}",
            "Total Value":  fmt_money(t.get("total_value")),
            "Post-Pos.":    fmt_shares(t.get("shares_owned_after")),
            "Signal":       t.get("overall_signal") or "—",
            "Conf":         t.get("confidence_score"),
        })

    df = pd.DataFrame(rows)

    def _color_signal(val):
        for sig, (_, text_color, bg_color) in SIGNAL_CSS.items():
            if sig in str(val):
                return f"background-color:{bg_color}"
        return ""

    styled = (
        df.style
        .applymap(_color_signal, subset=["Signal"])
        .bar(subset=["Conf"], color="#b6d7a8", vmin=0, vmax=100)
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)


def render_patterns(p: dict) -> None:
    items = []
    if p["cluster_buying"]:
        items.append("🔥 **Cluster buying** — multiple insiders purchased within a 30-day window")
    if p["top_buyer_count"] >= 3:
        items.append(
            f"🔁 **Repeated buying** — {p['top_buyer']} has made "
            f"{p['top_buyer_count']} purchases in this window"
        )
    if p["unique_buyers"] >= 3:
        items.append(f"👥 **{p['unique_buyers']} unique insiders** purchased in this period")
    if p["oldest_date"]:
        months_since = (date.today() - p["oldest_date"]).days // 30
        items.append(
            f"📅 Oldest purchase on record: **{p['oldest_date']}** ({months_since} month(s) ago)"
        )
    if not items:
        items.append("No notable patterns detected in this window.")
    for item in items:
        st.markdown(f"- {item}")


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.title("🔍 Ticker Lookup")
st.caption("Fetch, score, and review all insider activity for a specific company")

with st.sidebar:
    st.header("Lookup")
    ticker_input = st.text_input(
        "Ticker Symbol", placeholder="e.g. ACME", key="ticker"
    ).strip().upper()

    months_back = st.slider("Look-back window (months)", 1, 24, 12, key="months")

    fetch_btn = st.button("🔄 Fetch & Score from EDGAR", use_container_width=True)

    st.divider()
    st.caption(
        "**Fetch & Score** ingests the latest EDGAR Form 4 filings for this ticker, "
        "enriches with market data, then scores all 5 criteria."
    )

if not ticker_input:
    st.info("Enter a ticker symbol in the sidebar to get started.")
    st.stop()

if fetch_btn:
    with st.status(f"Fetching {ticker_input} from EDGAR…", expanded=True) as status:
        st.write("Ingesting Form 4 filings…")
        n_in  = ingest_ticker_filings(ticker_input, months=months_back)
        st.write(f"✓ {n_in} new filing(s) stored")

        st.write("Enriching with market data…")
        n_en  = enrich_new_filings()
        st.write(f"✓ {n_en} filing(s) enriched")

        st.write("Scoring trades…")
        n_sc  = score_new_trades()
        st.write(f"✓ {n_sc} trade(s) scored")

        status.update(label="Done!", state="complete")

data = load_ticker_data(ticker_input, months_back)

if not data:
    st.warning(
        f"No open-market purchase data found for **{ticker_input}** "
        f"in the last {months_back} months."
    )
    st.info('Click **"Fetch & Score from EDGAR"** in the sidebar to pull data.')
    st.stop()

# Report
st.subheader(f"{ticker_input} — {data['company_name']}")
render_signal_banner(data["best_signal"], data["best_confidence"])
render_company_metrics(data)
st.divider()

col_left, col_right = st.columns([3, 2])

with col_left:
    st.subheader("Scorecard")
    if data["best_scored"]:
        st.caption(
            f"Based on the highest-signal trade "
            f"(filed {data['best_scored'].get('date_filed', '')[:10]})"
        )
        render_scorecard(data["best_scored"])
    else:
        st.info("Trades have not been scored yet. Run **Fetch & Score** to generate scores.")

with col_right:
    st.subheader("Notable Patterns")
    p = data["patterns"]
    st.metric("Purchases (this window)", p["total_purchases"])
    st.metric("Unique Insiders",         p["unique_buyers"])
    render_patterns(p)

st.divider()
st.subheader("Open-Market Purchases")

n_unscored = len(data["trades"]) - len(data["scored"])
if n_unscored:
    st.caption(
        f"⚠️ {n_unscored} transaction(s) not yet scored — "
        "run **Fetch & Score** to process them."
    )

render_transactions_table(data["trades"])
st.divider()
st.caption(f"Data from local DB · Look-back {months_back} months · Today: {date.today()}")
