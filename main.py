"""
Entry point for the Insider Tracker.

Usage:
  python main.py                        # ingest yesterday's filings
  python main.py --since 2025-01-01     # ingest from a specific date
  python main.py --since 2025-01-01 --until 2025-01-31
"""

import argparse
import logging
import os
import sys
from datetime import date

import config
from database.db import init_db
from ingestion.edgar import ingest_new_filings
from ingestion.enricher import enrich_new_filings

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


def parse_args():
    parser = argparse.ArgumentParser(description="Insider Tracker — EDGAR Form 4 ingestion")
    parser.add_argument(
        "--since",
        type=date.fromisoformat,
        default=None,
        help="Start date (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--until",
        type=date.fromisoformat,
        default=date.today(),
        help="End date (YYYY-MM-DD). Defaults to today.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    logger.info("=== Insider Tracker starting ===")
    init_db()

    ingested = ingest_new_filings(since_date=args.since)
    logger.info("%d new filing(s) ingested.", ingested)

    enriched = enrich_new_filings()
    logger.info("=== Done. %d ingested | %d enriched. ===", ingested, enriched)
