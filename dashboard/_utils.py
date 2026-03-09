"""Shared formatting and rendering helpers for the dashboard."""

from typing import Optional
import streamlit as st

SIGNAL_ORDER = {"STRONG": 4, "MODERATE": 3, "WEAK": 2, "NO_SIGNAL": 1}

SIGNAL_CSS = {
    "STRONG":    ("🟢", "#1a7a1a", "#d4edda"),
    "MODERATE":  ("🟡", "#7a5500", "#fff3cd"),
    "WEAK":      ("🟠", "#7a3a00", "#fde8cc"),
    "NO_SIGNAL": ("🔴", "#7a1a1a", "#f8d7da"),
}

SCORE_ICON = {
    "PASS":        "✅",
    "STRONG_PASS": "✅✅",
    "PARTIAL":     "⚠️",
    "FAIL":        "❌",
    "UNKNOWN":     "❓",
    "URGENT":      "🚨",
    "RECENT":      "🕒",
    "NORMAL":      "⏰",
}

CRITERIA_KEYS = [
    ("score_small_cap",   "reasoning_small_cap",   "1·Cap"),
    ("score_materiality", "reasoning_materiality",  "2·Mat"),
    ("score_sector",      "reasoning_sector",       "3·Sec"),
    ("score_cannibal",    "reasoning_cannibal",     "4·Can"),
    ("score_timing",      "reasoning_timing",       "5·Time"),
]


def fmt_cap(cap: Optional[float]) -> str:
    if cap is None:
        return "N/A"
    if cap >= 1e9:
        return f"${cap / 1e9:.1f}B"
    return f"${cap / 1e6:.0f}M"


def fmt_money(val: Optional[float]) -> str:
    if val is None:
        return "—"
    if abs(val) >= 1e6:
        return f"${val / 1e6:.2f}M"
    if abs(val) >= 1e3:
        return f"${val / 1e3:.0f}K"
    return f"${val:.2f}"


def fmt_shares(n: Optional[float]) -> str:
    if n is None:
        return "—"
    return f"{n:,.0f}"


def scorecard_icons(row: dict) -> str:
    """Return a single string of icons for the 5 criteria, for compact display."""
    return " ".join(
        SCORE_ICON.get(row.get(score_key) or "UNKNOWN", "❓")
        for score_key, _, _ in CRITERIA_KEYS
    )


def render_signal_banner(signal: Optional[str], confidence: Optional[int]) -> None:
    if not signal:
        st.warning("No scored open-market purchases found. Run a refresh to fetch and score data.")
        return
    icon, text_color, bg_color = SIGNAL_CSS.get(signal, ("⚪", "#333", "#eee"))
    conf_str = f"  ·  Confidence {confidence}/100" if confidence is not None else ""
    st.markdown(
        f"""
        <div style="
            background:{bg_color};
            border-left:6px solid {text_color};
            padding:18px 24px;
            border-radius:6px;
            margin-bottom:16px;
        ">
            <span style="font-size:2rem;color:{text_color};font-weight:700;">
                {icon} {signal.replace("_", " ")}
            </span>
            <span style="font-size:1rem;color:{text_color};">{conf_str}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_scorecard(best: dict) -> None:
    criteria = [
        ("1 · Small Cap",   "score_small_cap",   "reasoning_small_cap"),
        ("2 · Materiality", "score_materiality", "reasoning_materiality"),
        ("3 · Sector",      "score_sector",      "reasoning_sector"),
        ("4 · Cannibal",    "score_cannibal",    "reasoning_cannibal"),
        ("5 · Timing",      "score_timing",      "reasoning_timing"),
    ]
    for label, score_key, reason_key in criteria:
        score  = best.get(score_key) or "UNKNOWN"
        reason = best.get(reason_key) or "No reasoning available."
        icon   = SCORE_ICON.get(score, "❓")
        with st.expander(f"{icon} **{label}** — {score}"):
            st.caption(reason)
