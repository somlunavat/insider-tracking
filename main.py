"""
Entry point for the Insider Tracker pipeline.

Usage:
  python main.py                           # run once (yesterday's filings)
  python main.py --since 2025-01-01        # run once from a specific date
  python main.py --since 2025-01-01 --until 2025-01-31
  python main.py --schedule                # run nightly at config.SCREENER_HOUR
  python main.py --schedule --interval-hours 4   # run every 4 hours
"""

import argparse
import logging
import os
import sys
from datetime import date

from apscheduler.schedulers.blocking import BlockingScheduler

import config
from database.db import init_db
from ingestion.edgar import ingest_new_filings
from ingestion.enricher import enrich_new_filings
from scoring.engine import score_new_trades

# Ensure log directory exists before configuring handlers
os.makedirs(os.path.dirname(config.LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_pipeline(since_date: date = None) -> None:
    """Run the full ingest → enrich → score pipeline once."""
    logger.info("=== Pipeline starting ===")
    init_db()

    ingested = ingest_new_filings(since_date=since_date)
    logger.info("%d new filing(s) ingested.", ingested)

    enriched = enrich_new_filings()
    scored   = score_new_trades()

    logger.info(
        "=== Pipeline done. %d ingested | %d enriched | %d scored. ===",
        ingested, enriched, scored,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Insider Tracker — EDGAR Form 4 pipeline")
    parser.add_argument(
        "--since",
        type=date.fromisoformat,
        default=None,
        help="Start date (YYYY-MM-DD). Defaults to yesterday for one-shot runs.",
    )
    parser.add_argument(
        "--until",
        type=date.fromisoformat,
        default=date.today(),
        help="End date (YYYY-MM-DD). Defaults to today. Ignored in scheduled mode.",
    )
    parser.add_argument(
        "--schedule",
        action="store_true",
        help=(
            "Keep running on a schedule instead of exiting after one run. "
            "Defaults to daily at config.SCREENER_HOUR:SCREENER_MINUTE."
        ),
    )
    parser.add_argument(
        "--interval-hours",
        type=float,
        default=None,
        metavar="N",
        help="With --schedule: run every N hours instead of once daily.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if not args.schedule:
        # One-shot run
        run_pipeline(since_date=args.since)
        sys.exit(0)

    # --- Scheduled mode ---
    scheduler = BlockingScheduler(timezone="America/New_York")

    if args.interval_hours:
        scheduler.add_job(
            run_pipeline,
            trigger="interval",
            hours=args.interval_hours,
            id="pipeline",
        )
        logger.info(
            "Scheduler: running every %.1f hour(s). Starting now…",
            args.interval_hours,
        )
    else:
        scheduler.add_job(
            run_pipeline,
            trigger="cron",
            hour=config.SCREENER_HOUR,
            minute=config.SCREENER_MINUTE,
            id="pipeline",
        )
        logger.info(
            "Scheduler: running daily at %02d:%02d ET. Starting now…",
            config.SCREENER_HOUR,
            config.SCREENER_MINUTE,
        )

    # Run once immediately so the DB is populated before the first scheduled run
    run_pipeline()

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
