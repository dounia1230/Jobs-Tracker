"""
============================================================
Moroccan Job Market Analyzer — Run All Scrapers
============================================================
Master script that runs all scrapers in sequence.
Called by Docker container, Airflow DAG, or directly.

Usage:
  python run_all_scrapers.py
  python run_all_scrapers.py --sources rekrute emploi
  python run_all_scrapers.py --run-id my-pipeline-run-001
============================================================
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime

from scraper_rekrute import RekruteScraper
from scraper_emploi import EmploiScraper
from scraper_indeed import IndeedScraper

# ── Logging setup ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger("run_all_scrapers")

# ── Scraper registry ───────────────────────────────────────────
SCRAPERS = {
    "rekrute": RekruteScraper,
    "emploi": EmploiScraper,
    "indeed": IndeedScraper,
}


def run_all(sources: list[str] = None, pipeline_run_id: str = None) -> dict:
    """
    Run all (or specified) scrapers in sequence.

    Args:
        sources: List of source names to run (default: all)
        pipeline_run_id: Airflow DAG run ID for lineage tracking

    Returns:
        dict: {source: job_count} summary
    """
    if sources is None:
        sources = list(SCRAPERS.keys())

    run_id = pipeline_run_id or f"manual_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    summary = {}

    logger.info(f"═══════════════════════════════════════════════════")
    logger.info(f"  Moroccan Job Market Analyzer — Scraping Run")
    logger.info(f"  Run ID : {run_id}")
    logger.info(f"  Sources: {', '.join(sources)}")
    logger.info(f"  Time   : {datetime.utcnow().isoformat()}")
    logger.info(f"═══════════════════════════════════════════════════")

    total_jobs = 0

    for source_name in sources:
        if source_name not in SCRAPERS:
            logger.warning(f"Unknown source: '{source_name}', skipping")
            continue

        logger.info(f"\n{'─'*50}")
        logger.info(f"  Starting scraper: {source_name.upper()}")
        logger.info(f"{'─'*50}")

        scraper_class = SCRAPERS[source_name]
        scraper = scraper_class()

        start_time = time.time()
        try:
            jobs = scraper.run(pipeline_run_id=run_id)
            scraper.flush_kafka()

            elapsed = time.time() - start_time
            job_count = len(jobs)
            total_jobs += job_count

            summary[source_name] = {
                "status": "success",
                "job_count": job_count,
                "elapsed_seconds": round(elapsed, 2),
            }
            logger.info(f"✅ {source_name}: {job_count} jobs in {elapsed:.1f}s")

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"❌ {source_name} failed after {elapsed:.1f}s: {e}", exc_info=True)
            summary[source_name] = {
                "status": "failed",
                "job_count": 0,
                "error": str(e),
                "elapsed_seconds": round(elapsed, 2),
            }

        # Pause between scrapers to avoid IP rate limiting
        if source_name != sources[-1]:
            logger.info("Pausing 5s before next scraper...")
            time.sleep(5)

    logger.info(f"\n{'═'*50}")
    logger.info(f"  RUN COMPLETE — Total jobs: {total_jobs}")
    for src, info in summary.items():
        status_icon = "✅" if info["status"] == "success" else "❌"
        logger.info(f"  {status_icon} {src:12s}: {info.get('job_count', 0):5d} jobs  ({info['elapsed_seconds']}s)")
    logger.info(f"{'═'*50}\n")

    return summary


def parse_args():
    parser = argparse.ArgumentParser(
        description="Moroccan Job Market Analyzer — Scraper Runner"
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=list(SCRAPERS.keys()),
        default=list(SCRAPERS.keys()),
        help="Sources to scrape (default: all)"
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Pipeline run ID for lineage tracking"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    summary = run_all(sources=args.sources, pipeline_run_id=args.run_id)

    # Exit with error code if any scraper failed
    failed = [s for s, info in summary.items() if info["status"] == "failed"]
    if failed:
        logger.error(f"Some scrapers failed: {failed}")
        sys.exit(1)
    else:
        sys.exit(0)
