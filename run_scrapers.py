"""
Entry point.

Usage:
    python run_scrapers.py                                         # run all 3 stores
    python run_scrapers.py --store woolworths                      # run one store
    python run_scrapers.py --store woolworths --search milk eggs   # search specific terms
    python run_scrapers.py --seed-products                         # seed initial products from master list
"""
from __future__ import annotations

import asyncio
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from supabase import create_client

from config import SUPABASE_URL, SUPABASE_SERVICE_KEY
from newworld_scraper import NewWorldScraper
from paknsave_scraper import PakNSaveScraper
from woolworths_scraper import WoolworthsScraper
from matching.master_products import MASTER_PRODUCTS

LOG_DIR = Path(__file__).parent / "logs"
LOG_FORMAT = "%(asctime)s  %(levelname)-8s  %(message)s"
LOG_DATEFMT = "%H:%M:%S"


def _setup_logging(store_label: str) -> None:
    """Configure root logger to write to console and a timestamped log file."""
    LOG_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = LOG_DIR / f"{store_label}_{timestamp}.txt"

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fmt = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(fmt)

    root.addHandler(stream_handler)
    root.addHandler(file_handler)

    logging.getLogger(__name__).info(f"Logging to {log_path}")


logger = logging.getLogger(__name__)

SCRAPERS = {
    "newworld": NewWorldScraper,
    "paknsave": PakNSaveScraper,
    "woolworths": WoolworthsScraper,
}


async def run_all() -> None:
    tasks = [cls().run() for cls in SCRAPERS.values()]
    await asyncio.gather(*tasks)


async def run_one(store: str, search_terms: list | None = None, branch_name: str | None = None, categories: list | None = None) -> None:
    cls = SCRAPERS.get(store)
    if cls is None:
        logger.error(f"Unknown store '{store}'. Choose from: {list(SCRAPERS)}")
        sys.exit(1)
    await cls(search_terms=search_terms, branch_name=branch_name, categories=categories).run()


def seed_products() -> None:
    """
    Seed the initial master product list into the Supabase 'products' table.
    This is optional — products are also created automatically when scrapers run.
    """
    if not SUPABASE_SERVICE_KEY:
        raise RuntimeError("SUPABASE_SERVICE_KEY missing. Must be service_role key.")

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    payload = [{"name": p["name"], "source": "scraped"} for p in MASTER_PRODUCTS]
    client.table("products").upsert(payload, on_conflict="name").execute()
    logger.info(f"Seeded {len(payload)} products into Supabase.")


def main() -> None:
    parser = argparse.ArgumentParser(description="NZ Grocery scraper runner")
    parser.add_argument("--store", choices=list(SCRAPERS), help="Run one store only")
    parser.add_argument(
        "--search", nargs="+", metavar="TERM",
        help="Search for specific terms instead of scraping all categories (requires --store)",
    )
    parser.add_argument(
        "--branch-name",
        help="Override the branch name from config (e.g. 'Woolworths Ponsonby')",
    )
    parser.add_argument(
        "--categories", nargs="+", metavar="SLUG",
        help="Only scrape categories whose URL contains these slugs (e.g. fridge-deli)",
    )
    parser.add_argument(
        "--seed-products",
        action="store_true",
        help="Seed initial products from master list (optional — scrapers also create products automatically)",
    )
    args = parser.parse_args()

    if args.search and not args.store:
        parser.error("--search requires --store (e.g. --store woolworths --search milk eggs)")

    if args.branch_name and not args.store:
        parser.error("--branch-name requires --store (e.g. --store woolworths --branch-name 'Woolworths Ponsonby')")

    if args.seed_products:
        _setup_logging("seed")
        seed_products()
        return

    store_label = args.store if args.store else "all"
    _setup_logging(store_label)

    try:
        if args.store:
            asyncio.run(run_one(args.store, search_terms=args.search, branch_name=args.branch_name, categories=args.categories))
        else:
            asyncio.run(run_all())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Exiting gracefully...")


if __name__ == "__main__":
    main()
