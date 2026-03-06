# Cursor Planning Prompt — Insider Activity Tracker

## Instructions
Paste this entire document into Cursor as your first prompt. Tell Cursor to plan before building.

---

## The Prompt

I want to build a personal stock insider activity tracker. This is a personal tool — no auth, no multi-user, no scalability concerns. Keep the code simple and focused.

Before writing any code, help me plan the full architecture, file structure, and module breakdown. Then we will build module by module.

---

## What the Tool Does

### Mode 1: Proactive Screener (background, scheduled)
- Runs every evening after market close (Form 4 filings are published after hours)
- Ingests all new SEC Form 4 filings from EDGAR
- Scores each trade against my 5 criteria (defined below)
- Sends me an alert (email or Telegram) ONLY when a trade scores highly
- Stores all processed filings in a local database to avoid reprocessing

### Mode 2: Reactive Ticker Lookup (on demand)
- I input a stock ticker
- It pulls all recent and historical insider activity for that company
- Runs the same scoring logic against my 5 criteria
- Returns a clean plain-English summarized report including:
  - List of recent insider transactions (last 12 months)
  - Pass/fail + reasoning for each of the 5 criteria
  - Notable patterns (repeated buying, cluster buying, first purchase in X months)
  - An overall signal rating: STRONG / MODERATE / WEAK / NO SIGNAL

Both modes share the same data pipeline and scoring engine. They are just two interfaces on top of the same logic.

---

## The 5 Scoring Criteria

### Criteria 1: Small Cap Filter
- Company market cap must be under $500M
- Smaller is better — the smaller the cap, the less likely algorithms and institutions are trading on this signal
- Score: PASS if under $500M, FAIL if over

### Criteria 2: Materiality
- The trade must be an open market PURCHASE only — ignore option exercises, tax filings, gifts, or any non-cash transactions
- Trade value must be above $1,000,000
- The purchase must increase the insider's total position by more than 10%
- Best signal: insider dramatically increasing their position or going "all in"
- Score: PASS if both conditions met, PARTIAL if only one met, FAIL if neither

### Criteria 3: Information Asymmetry (Sector Filter)
- Prioritize trades in Biotechnology and Gold/Mining companies
- Biotech insiders know interim drug trial data before public disclosure
- Gold insiders know assay results and new discoveries before announcement
- Score: STRONG PASS if biotech or gold, PASS for any other sector (do not disqualify, just weight lower)

### Criteria 4: The Cannibal Trait
- The company must be actively reducing its net share count by at least 2-3% annually
- This confirms management views the stock as undervalued
- Requires comparing shares outstanding over the last 12 months
- Score: PASS if reducing by 2%+, PARTIAL if reducing but less than 2%, FAIL if flat or increasing

### Criteria 5: Aftermarket Timing
- Most Form 4 filings are published after market close
- Price reaction is typically delayed until market open the next day
- Flag trades that were JUST filed (within the last few hours) as HIGH PRIORITY for immediate alerts
- The alert must be fast enough for me to act in aftermarket trading if needed
- Score: flag as URGENT if filed within 3 hours of current time

---

## Scoring Logic

Each trade gets an overall signal based on criteria scores:
- STRONG SIGNAL: Criteria 1 + 2 + 3 all pass, plus 4 or 5
- MODERATE SIGNAL: Criteria 1 + 2 pass, plus one of 3, 4, or 5
- WEAK SIGNAL: Criteria 1 + 2 pass only
- NO SIGNAL: Criteria 1 or 2 fail

Only STRONG and MODERATE signals should trigger an alert.

---

## Data Sources

- **SEC EDGAR API** — https://efts.sec.gov/LATEST/search-index?q=%22form+4%22 — free, no API key needed, official source for Form 4 filings
- **yfinance** — for market cap, current price, shares outstanding history
- **Financial Modeling Prep (free tier)** — backup for share count history and fundamentals
- **OpenInsider** — can be used as a supplementary source or cross-reference

---

## Tech Stack

- **Language**: Python
- **Database**: SQLite (local, simple, no setup needed)
- **Scheduler**: APScheduler or a simple cron job
- **Dashboard/Reactive UI**: Streamlit (simple personal dashboard)
- **Alerts**: Email (SMTP) or Telegram bot — I will decide later, build it modular so I can switch
- **Environment**: .env file for any config (alert credentials, thresholds)

---

## Build Order (Module by Module)

Build in this exact order. Do not move to the next module until the current one is tested and working.

1. **Module 1: EDGAR Data Ingestion**
   - Fetch new Form 4 filings from SEC EDGAR
   - Parse key fields: insider name, title, company, ticker, transaction type, shares, price, total value, date filed
   - Filter to open market purchases only
   - Store raw filings in SQLite

2. **Module 2: Data Enrichment**
   - For each filing, fetch market cap and sector from yfinance
   - Fetch shares outstanding history (last 12 months) for cannibal criterion
   - Store enriched data back to SQLite

3. **Module 3: Scoring Engine**
   - Implement all 5 criteria as individual functions
   - Each function takes a trade + enriched data and returns a score + reasoning string
   - Combine into an overall signal rating
   - This module should be completely independent and testable in isolation

4. **Module 4: Reactive Ticker Report**
   - Streamlit UI with a ticker input field
   - Pulls all insider activity for that ticker from EDGAR + local DB
   - Runs scoring engine on each trade
   - Displays a clean formatted report with overall signal and per-criteria breakdown

5. **Module 5: Proactive Screener + Alerts**
   - Scheduler that runs every evening (configurable time)
   - Processes all new filings since last run
   - Scores each one
   - Sends alert for STRONG and MODERATE signals only
   - Alert includes: ticker, insider name/title, trade details, criteria scorecard, overall signal

---

## File Structure to Plan For

```
insider-tracker/
├── main.py                  # Entry point, runs scheduler
├── config.py                # Settings, thresholds, constants
├── .env                     # Credentials (gitignored)
├── database/
│   ├── db.py                # SQLite connection and setup
│   └── models.py            # Table schemas
├── ingestion/
│   ├── edgar.py             # EDGAR Form 4 fetcher and parser
│   └── enrichment.py        # yfinance enrichment
├── scoring/
│   └── engine.py            # All 5 criteria + overall signal logic
├── alerts/
│   └── notifier.py          # Email/Telegram alert sender
├── dashboard/
│   └── app.py               # Streamlit reactive ticker lookup
└── tests/
    └── test_scoring.py      # Unit tests for scoring engine
```

---

## Additional Notes

- Always fetch insider's current total holding from EDGAR to calculate position increase % for Criteria 2
- For Criteria 5, compare filing timestamp to current time to flag URGENT trades
- The Streamlit dashboard is for the reactive mode only — the proactive screener runs headlessly
- Log everything to a local log file so I can debug alert history
- Use a confidence score (0-100) in addition to the STRONG/MODERATE/WEAK label so I can tune thresholds over time

---

## First Task for Cursor

Review everything above. Then:
1. Confirm the architecture and file structure make sense
2. Flag any gaps or issues you see before we start building
3. Ask me any clarifying questions
4. Then start with Module 1: EDGAR Data Ingestion
