"""
Module 4: Reactive Ticker Dashboard

Run with:
    streamlit run dashboard/app.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collections import Counter
from datetime import date, timedelta, datetime
from typing import Optional

import streamlit as st
import pandas as pd

import config
from database.db import init_db, get_db
from ingestion.edgar import ingest_ticker_filings
from ingestion.enricher import enrich_new_filings
from scoring.engine import score_new_trades

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Insider Tracker",
    page_icon="📈",
    layout="wide",
)

init_db()

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

_SIGNAL_ORDER = {"STRONG": 4, "MODERATE": 3, "WEAK": 2, "NO_SIGNAL": 1}


def load_ticker_data(ticker: str, months: int) -> dict:
    """
    Query all open-market-purchase data for a ticker from the DB.
    Returns a rich dict ready for rendering, or {} if no data found.
    """
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

    trades = [dict(r) for r in rows]
    latest = trades[0]

    scored = [t for t in trades if t.get("overall_signal")]
    best_scored = None
    if scored:
        best_scored = max(
            scored,
            key=lambda t: (
                _SIGNAL_ORDER.get(t["overall_signal"], 0),
                t.get("confidence_score") or 0,
            ),
        )

    return {
        "trades":               trades,
        "scored":               scored,
        "best_scored":          best_scored,
        "best_signal":          best_scored["overall_signal"] if best_scored else None,
        "best_confidence":      best_scored["confidence_score"] if best_scored else None,
        "company_name":         latest["company_name"],
        "market_cap":           latest.get("market_cap"),
        "sector":               latest.get("sector"),
        "industry":             latest.get("industry"),
        "share_count_change_pct": latest.get("share_count_change_pct"),
        "patterns":             _detect_patterns(trades),
    }


def _detect_patterns(trades: list[dict]) -> dict:
    unique_buyers = set(t["insider_name"] for t in trades)
    buyer_counts  = Counter(t["insider_name"] for t in trades)
    top_buyer, top_count = buyer_counts.most_common(1)[0] if buyer_counts else ("—", 0)

    # Cluster buying: >= 2 unique insiders buying within any 30-day window
    cluster = False
    dates_by_buyer: list[tuple[date, str]] = []
    for t in trades:
        try:
            d = date.fromisoformat(t["date_filed"][:10])
            dates_by_buyer.append((d, t["insider_name"]))
        except (ValueError, TypeError):
            pass

    dates_by_buyer.sort()
    for i, (d, name) in enumerate(dates_by_buyer):
        window_names = {name}
        for d2, n2 in dates_by_buyer[i + 1:]:
            if (d2 - d).days <= 30:
                window_names.add(n2)
            else:
                break
        if len(window_names) >= 2:
            cluster = True
            break

    # Earliest purchase in DB for this ticker (all-time)
    oldest_date: Optional[date] = None
    for t in sorted(trades, key=lambda x: x.get("date_filed") or ""):
        try:
            oldest_date = date.fromisoformat(t["date_filed"][:10])
            break
        except (ValueError, TypeError):
            pass

    return {
        "unique_buyers": len(unique_buyers),
        "total_purchases": len(trades),
        "top_buyer": top_buyer,
        "top_buyer_count": top_count,
        "cluster_buying": cluster,
        "oldest_date": oldest_date,
    }


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

_SIGNAL_CSS = {
    "STRONG":    ("🟢", "#1a7a1a", "#d4edda"),
    "MODERATE":  ("🟡", "#7a5500", "#fff3cd"),
    "WEAK":      ("🟠", "#7a3a00", "#fde8cc"),
    "NO_SIGNAL": ("🔴", "#7a1a1a", "#f8d7da"),
}

_SCORE_ICON = {
    "PASS":        "✅",
    "STRONG_PASS": "✅✅",
    "PARTIAL":     "⚠️",
    "FAIL":        "❌",
    "UNKNOWN":     "❓",
    "URGENT":      "🚨",
    "RECENT":      "🕒",
    "NORMAL":      "⏰",
}


def _fmt_cap(cap: Optional[float]) -> str:
    if cap is None:
        return "N/A"
    if cap >= 1e9:
        return f"${cap / 1e9:.1f}B"
    return f"${cap / 1e6:.0f}M"


def _fmt_money(val: Optional[float]) -> str:
    if val is None:
        return "—"
    if abs(val) >= 1e6:
        return f"${val / 1e6:.2f}M"
    if abs(val) >= 1e3:
        return f"${val / 1e3:.0f}K"
    return f"${val:.2f}"


def _fmt_shares(n: Optional[float]) -> str:
    if n is None:
        return "—"
    return f"{n:,.0f}"


def render_signal_banner(signal: Optional[str], confidence: Optional[int]) -> None:
    if not signal:
        st.warning("No scored open-market purchases found. Run a refresh to fetch and score data.")
        return

    icon, text_color, bg_color = _SIGNAL_CSS.get(signal, ("⚪", "#333", "#eee"))
    conf_str = f"  ·  Confidence {confidence}/100" if confidence is not None else ""

    st.markdown(
        f"""
        <div style="
            background:{bg_color};
            border-left: 6px solid {text_color};
            padding: 18px 24px;
            border-radius: 6px;
            margin-bottom: 16px;
        ">
            <span style="font-size:2rem; color:{text_color}; font-weight:700;">
                {icon} {signal.replace("_", " ")}
            </span>
            <span style="font-size:1rem; color:{text_color};">
                {conf_str}
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_scorecard(best: dict) -> None:
    criteria = [
        ("1 · Small Cap",         "score_small_cap",   "reasoning_small_cap"),
        ("2 · Materiality",       "score_materiality", "reasoning_materiality"),
        ("3 · Sector",            "score_sector",      "reasoning_sector"),
        ("4 · Cannibal",          "score_cannibal",    "reasoning_cannibal"),
        ("5 · Timing",            "score_timing",      "reasoning_timing"),
    ]

    for label, score_key, reason_key in criteria:
        score   = best.get(score_key) or "UNKNOWN"
        reason  = best.get(reason_key) or "No reasoning available."
        icon    = _SCORE_ICON.get(score, "❓")

        with st.expander(f"{icon} **{label}** — {score}"):
            st.caption(reason)


def render_transactions_table(trades: list[dict]) -> None:
    rows = []
    for t in trades:
        rows.append({
            "Date Filed":      t.get("date_filed", "")[:10],
            "Txn Date":        t.get("transaction_date", "")[:10],
            "Insider":         t.get("insider_name", "—"),
            "Title":           t.get("insider_title", "—"),
            "Shares":          _fmt_shares(t.get("shares")),
            "Price":           f"${t.get('price_per_share') or 0:.2f}",
            "Total Value":     _fmt_money(t.get("total_value")),
            "Post-Position":   _fmt_shares(t.get("shares_owned_after")),
            "Signal":          t.get("overall_signal") or "—",
            "Score":           t.get("confidence_score"),
        })

    df = pd.DataFrame(rows)

    def _color_signal(val):
        colors = {
            "STRONG":    "background-color:#d4edda",
            "MODERATE":  "background-color:#fff3cd",
            "WEAK":      "background-color:#fde8cc",
            "NO_SIGNAL": "background-color:#f8d7da",
        }
        return colors.get(val, "")

    styled = df.style.applymap(_color_signal, subset=["Signal"])
    st.dataframe(styled, use_container_width=True, hide_index=True)


def render_patterns(p: dict) -> None:
    items = []

    if p["cluster_buying"]:
        items.append("🔥 **Cluster buying** — multiple insiders purchased within a 30-day window")

    if p["top_buyer_count"] >= 3:
        items.append(
            f"🔁 **Repeated buying** — {p['top_buyer']} has made"
            f" {p['top_buyer_count']} purchases in this window"
        )

    if p["unique_buyers"] >= 3:
        items.append(f"👥 **{p['unique_buyers']} unique insiders** purchased in this period")

    if p["oldest_date"]:
        days_since = (date.today() - p["oldest_date"]).days
        months_since = days_since // 30
        items.append(
            f"📅 Oldest purchase on record: **{p['oldest_date']}**"
            f" ({months_since} month(s) ago)"
        )

    if not items:
        items.append("No notable patterns detected in this window.")

    for item in items:
        st.markdown(f"- {item}")


def render_company_metrics(data: dict) -> None:
    cap = _fmt_cap(data.get("market_cap"))
    sector = data.get("sector") or "Unknown"
    industry = data.get("industry") or ""
    change = data.get("share_count_change_pct")

    if change is not None:
        arrow = "▼" if change < 0 else "▲"
        change_str = f"{arrow} {abs(change):.1f}% YoY"
    else:
        change_str = "N/A"

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Market Cap", cap)
    col2.metric("Sector", sector)
    col3.metric("Industry", industry or "—")
    col4.metric("Share Count Change", change_str)


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

st.title("📈 Insider Tracker")
st.caption("Reactive ticker lookup — open-market purchase signals only")

# Sidebar controls
with st.sidebar:
    st.header("Lookup")
    ticker_input = st.text_input(
        "Ticker Symbol", placeholder="e.g. ACME", key="ticker"
    ).strip().upper()

    months_back = st.slider("Look-back window (months)", 1, 24, 12, key="months")

    fetch_btn = st.button("🔄 Fetch & Score from EDGAR", use_container_width=True)

    st.divider()
    st.caption(
        "**Fetch & Score** ingests the latest EDGAR Form 4 filings for the ticker, "
        "enriches with market data, and scores against the 5 criteria."
    )

if not ticker_input:
    st.info("Enter a ticker symbol in the sidebar to get started.")
    st.stop()

# Run pipeline if requested
if fetch_btn:
    with st.status(f"Fetching {ticker_input} from EDGAR…", expanded=True) as status:
        st.write("Ingesting Form 4 filings…")
        n_ingested = ingest_ticker_filings(ticker_input, months=months_back)
        st.write(f"✓ {n_ingested} new filing(s) stored")

        st.write("Enriching with market data…")
        n_enriched = enrich_new_filings()
        st.write(f"✓ {n_enriched} filing(s) enriched")

        st.write("Scoring trades…")
        n_scored = score_new_trades()
        st.write(f"✓ {n_scored} trade(s) scored")

        status.update(label="Done!", state="complete")

# Load data from DB
data = load_ticker_data(ticker_input, months_back)

if not data:
    st.warning(
        f"No open-market purchase data found for **{ticker_input}** "
        f"in the last {months_back} months."
    )
    st.info('Click **"Fetch & Score from EDGAR"** in the sidebar to pull data.')
    st.stop()

# ---- Report header -------------------------------------------------------
st.subheader(f"{ticker_input} — {data['company_name']}")

render_signal_banner(data["best_signal"], data["best_confidence"])

render_company_metrics(data)

st.divider()

# ---- Two-column layout: scorecard + patterns -----------------------------
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
        st.info(
            "Trades have not been scored yet. "
            "Run **Fetch & Score** to generate scores."
        )

with col_right:
    st.subheader("Notable Patterns")
    p = data["patterns"]
    st.metric("Purchases (this window)", p["total_purchases"])
    st.metric("Unique Insiders", p["unique_buyers"])
    render_patterns(p)

st.divider()

# ---- Transactions table --------------------------------------------------
st.subheader("Open-Market Purchases")
n_unscored = len(data["trades"]) - len(data["scored"])
if n_unscored:
    st.caption(
        f"⚠️ {n_unscored} transaction(s) not yet scored — "
        "run **Fetch & Score** to process them."
    )

render_transactions_table(data["trades"])

# ---- Footer --------------------------------------------------------------
st.divider()
st.caption(
    f"Data from local DB · Look-back {months_back} months · "
    f"Today: {date.today()}"
)
