"""
Module 4 · Home Page — Signals Dashboard

Shows all tickers with STRONG or MODERATE signals from recent scored trades.

Run with:
    streamlit run dashboard/app.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date, timedelta, datetime

import streamlit as st
import pandas as pd

from database.db import init_db, get_db
from dashboard._utils import (
    SIGNAL_CSS, SIGNAL_ORDER, SCORE_ICON, fmt_cap, fmt_money, scorecard_icons,
)

st.set_page_config(
    page_title="Insider Tracker",
    page_icon="📈",
    layout="wide",
)

init_db()

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

def load_signals(days: int, signals: list[str]) -> list[dict]:
    """
    Return the best-scored trade per ticker for tickers that have at least one
    STRONG or MODERATE signal in the last `days` days.

    Uses a window function to pick the highest-confidence trade per ticker,
    then joins in the full scorecard, enrichment, and trade-count data.
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    placeholders = ",".join("?" * len(signals))

    with get_db() as conn:
        rows = conn.execute(f"""
            WITH ranked AS (
                SELECT
                    s.transaction_id,
                    f.ticker,
                    ROW_NUMBER() OVER (
                        PARTITION BY f.ticker
                        ORDER BY
                            CASE s.overall_signal
                                WHEN 'STRONG'   THEN 1
                                WHEN 'MODERATE' THEN 2
                                ELSE 3
                            END,
                            s.confidence_score DESC,
                            f.date_filed DESC
                    ) AS rn
                FROM scored_trades s
                JOIN transactions t ON t.id = s.transaction_id
                JOIN filings      f ON f.id = t.filing_id
                WHERE s.overall_signal IN ({placeholders})
                  AND f.date_filed >= ?
            ),
            trade_counts AS (
                SELECT f.ticker, COUNT(*) AS trade_count
                FROM scored_trades s
                JOIN transactions t ON t.id = s.transaction_id
                JOIN filings      f ON f.id = t.filing_id
                WHERE s.overall_signal IN ({placeholders})
                  AND f.date_filed >= ?
                GROUP BY f.ticker
            )
            SELECT
                f.ticker, f.company_name,
                f.insider_name, f.insider_title, f.date_filed,
                s.overall_signal, s.confidence_score, s.is_urgent,
                s.score_small_cap,   s.reasoning_small_cap,
                s.score_materiality, s.reasoning_materiality,
                s.score_sector,      s.reasoning_sector,
                s.score_cannibal,    s.reasoning_cannibal,
                s.score_timing,      s.reasoning_timing,
                t.total_value,
                e.market_cap, e.sector, e.share_count_change_pct,
                tc.trade_count
            FROM ranked r
            JOIN scored_trades   s  ON s.transaction_id = r.transaction_id
            JOIN transactions    t  ON t.id = s.transaction_id
            JOIN filings         f  ON f.id = t.filing_id
            LEFT JOIN enriched_data e   ON e.filing_id = f.id
            LEFT JOIN trade_counts  tc  ON tc.ticker    = f.ticker
            WHERE r.rn = 1
            ORDER BY
                CASE s.overall_signal
                    WHEN 'STRONG'   THEN 1
                    WHEN 'MODERATE' THEN 2
                    ELSE 3
                END,
                s.confidence_score DESC
        """, (*signals, since, *signals, since)).fetchall()

    return [dict(r) for r in rows]


def last_pipeline_run() -> str:
    """Return the timestamp of the most recently scored trade."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT MAX(scored_at) FROM scored_trades"
        ).fetchone()
    val = row[0] if row else None
    return val[:16] if val else "never"


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.title("📈 Insider Tracker")
st.caption("Signal dashboard — open-market purchases scored against all 5 criteria")

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    days_back = st.slider("Look-back window (days)", 7, 90, 30, key="days")

    signal_filter = st.multiselect(
        "Signal level",
        options=["STRONG", "MODERATE"],
        default=["STRONG", "MODERATE"],
        key="signal_filter",
    )

    st.divider()
    st.caption(f"DB last updated: **{last_pipeline_run()}**")
    if st.button("🔄 Refresh display", use_container_width=True):
        st.rerun()

if not signal_filter:
    st.info("Select at least one signal level in the sidebar.")
    st.stop()

signals = load_signals(days_back, signal_filter)

# Top-level metrics
n_strong   = sum(1 for s in signals if s["overall_signal"] == "STRONG")
n_moderate = sum(1 for s in signals if s["overall_signal"] == "MODERATE")
n_urgent   = sum(1 for s in signals if s.get("is_urgent"))

c1, c2, c3, c4 = st.columns(4)
c1.metric("🟢 Strong Signals",   n_strong)
c2.metric("🟡 Moderate Signals", n_moderate)
c3.metric("🚨 Urgent",           n_urgent)
c4.metric("📅 Window",           f"{days_back} days")

st.divider()

if not signals:
    st.info(
        f"No {'/ '.join(signal_filter)} signals found in the last {days_back} days.\n\n"
        "Run `python main.py --schedule` to ingest new filings, or use the "
        "**Ticker Lookup** page to fetch data for a specific company."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Signals table
# ---------------------------------------------------------------------------

st.subheader(f"Passing Tickers — {len(signals)} found")

rows = []
for s in signals:
    sig   = s["overall_signal"]
    icon  = SIGNAL_CSS.get(sig, ("⚪",))[0]
    chg   = s.get("share_count_change_pct")
    chg_s = f"{'▼' if chg and chg < 0 else '▲'}{abs(chg):.1f}%" if chg is not None else "N/A"

    rows.append({
        "Signal":     f"{icon} {sig}",
        "Conf":       s.get("confidence_score"),
        "Ticker":     s["ticker"],
        "Company":    s["company_name"],
        "Sector":     s.get("sector") or "—",
        "Cap":        fmt_cap(s.get("market_cap")),
        "Sharebacks": chg_s,
        "Scorecard":  scorecard_icons(s),
        "Insider":    s.get("insider_name", "—"),
        "Title":      s.get("insider_title", "—"),
        "Value":      fmt_money(s.get("total_value")),
        "Filed":      (s.get("date_filed") or "")[:10],
        "# Trades":   s.get("trade_count", 1),
        "Urgent":     "🚨" if s.get("is_urgent") else "",
    })

df = pd.DataFrame(rows)


def _color_signal(val: str) -> str:
    for sig, (_, text_color, bg_color) in SIGNAL_CSS.items():
        if sig in val:
            return f"background-color:{bg_color};color:{text_color};font-weight:600"
    return ""


styled = (
    df.style
    .applymap(_color_signal, subset=["Signal"])
    .bar(subset=["Conf"], color="#b6d7a8", vmin=0, vmax=100)
)
st.dataframe(styled, use_container_width=True, hide_index=True)

st.caption(
    "**Scorecard icons:** "
    + "  ".join(f"{v} = {k}" for k, v in SCORE_ICON.items()
                if k in ("PASS", "STRONG_PASS", "PARTIAL", "FAIL", "UNKNOWN"))
    + "  |  Columns: 1·Cap  2·Materiality  3·Sector  4·Cannibal  5·Timing"
)

st.divider()

# ---------------------------------------------------------------------------
# Expandable detail cards for each ticker
# ---------------------------------------------------------------------------

st.subheader("Details")

for s in signals:
    sig        = s["overall_signal"]
    icon, text_color, bg_color = SIGNAL_CSS.get(sig, ("⚪", "#333", "#eee"))
    urgent_tag = " 🚨 URGENT" if s.get("is_urgent") else ""

    with st.expander(
        f"{icon} **{s['ticker']}** — {s['company_name']} "
        f"· Conf {s.get('confidence_score')}/100{urgent_tag}"
    ):
        col1, col2, col3 = st.columns(3)
        col1.metric("Signal",       f"{icon} {sig}")
        col2.metric("Market Cap",   fmt_cap(s.get("market_cap")))
        col3.metric("Trade Value",  fmt_money(s.get("total_value")))

        col4, col5, col6 = st.columns(3)
        col4.metric("Insider",  s.get("insider_name", "—"))
        col5.metric("Title",    s.get("insider_title", "—"))
        col6.metric("Filed",    (s.get("date_filed") or "")[:10])

        st.markdown("**Scorecard**")
        criteria = [
            ("1 · Small Cap",   "score_small_cap",   "reasoning_small_cap"),
            ("2 · Materiality", "score_materiality", "reasoning_materiality"),
            ("3 · Sector",      "score_sector",      "reasoning_sector"),
            ("4 · Cannibal",    "score_cannibal",    "reasoning_cannibal"),
            ("5 · Timing",      "score_timing",      "reasoning_timing"),
        ]
        for label, score_key, reason_key in criteria:
            score  = s.get(score_key) or "UNKNOWN"
            reason = s.get(reason_key) or "—"
            sc_icon = SCORE_ICON.get(score, "❓")
            st.markdown(f"- {sc_icon} **{label}** ({score}) — {reason}")

        st.caption(
            f"Use the **Ticker Lookup** page for the full report on {s['ticker']}."
        )
