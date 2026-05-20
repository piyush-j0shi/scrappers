#!/usr/bin/env python3
"""
run_all_branches.py — multi-branch scraper orchestrator.

Fetches all active branches from Supabase and runs the appropriate scraper
for each one, subject to concurrency limits and retry logic.

Usage:
    python run_all_branches.py [options]

Options:
    --concurrency INT   Max parallel scrapers (default: 6, max 32 with proxy)
    --chain SLUG        Filter to one chain: woolworths | new-world | paknsave
    --branch-id UUID    Run a single branch only
    --stale-only        Only branches with last_scraped_at NULL or > 24h ago
    --proxy URL         Proxy gateway URL passed to each scraper
    --max-retries INT   Per-branch retry limit (default: 3)
    --dry-run           Print branches that would be scraped, then exit
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from supabase import create_client, Client

from config import SUPABASE_URL, SUPABASE_SERVICE_KEY
from woolworths_scraper import WoolworthsScraper
from newworld_scraper import NewWorldScraper
from paknsave_scraper import PakNSaveScraper

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Chain slug → scraper class
# ---------------------------------------------------------------------------

CHAIN_SCRAPERS: dict[str, type] = {
    "woolworths": WoolworthsScraper,
    "new-world": NewWorldScraper,
    "paknsave": PakNSaveScraper,
}

# ---------------------------------------------------------------------------
# Branch result tracking
# ---------------------------------------------------------------------------

RESULT_SUCCESS = "success"
RESULT_PARTIAL = "partial"
RESULT_FAILED  = "failed"


# ---------------------------------------------------------------------------
# Fetch branches
# ---------------------------------------------------------------------------

def fetch_branches(
    client: Client,
    chain_slug: Optional[str],
    branch_id: Optional[str],
    stale_only: bool,
) -> list[dict]:
    """Return active branches from store_branches, filtered per CLI args."""

    # If a single branch UUID was requested, fetch just that one.
    if branch_id:
        result = (
            client.table("store_branches")
            .select("id, name, api_store_id, chain_id, store_chains(slug)")
            .eq("id", branch_id)
            .eq("is_active", True)
            .execute()
        )
        branches = result.data or []
        if not branches:
            logger.error(f"Branch {branch_id} not found or is inactive.")
        return branches

    query = (
        client.table("store_branches")
        .select("id, name, api_store_id, chain_id, last_scraped_at, store_chains(slug)")
        .eq("is_active", True)
    )

    # Filter by chain slug — resolve chain_id via join alias rather than hardcoding UUIDs.
    if chain_slug:
        # Supabase PostgREST foreign-table filter syntax: store_chains.slug=eq.<slug>
        query = query.eq("store_chains.slug", chain_slug)

    if stale_only:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        # last_scraped_at IS NULL OR last_scraped_at < cutoff
        # PostgREST: use .or_ with is.null and lt filters
        query = query.or_(f"last_scraped_at.is.null,last_scraped_at.lt.{cutoff}")

    # Never-scraped branches first, then oldest scrape first.
    query = query.order("last_scraped_at", desc=False, nullsfirst=True)

    result = query.execute()
    branches = result.data or []

    # PostgREST may return rows where the join filter (store_chains.slug) didn't match
    # but the row itself wasn't excluded — filter in Python to be safe.
    if chain_slug:
        branches = [
            b for b in branches
            if (b.get("store_chains") or {}).get("slug") == chain_slug
        ]

    return branches


# ---------------------------------------------------------------------------
# Per-branch scrape task
# ---------------------------------------------------------------------------

async def scrape_branch(
    branch: dict,
    semaphore: asyncio.Semaphore,
    proxy_url: Optional[str],
    max_retries: int,
    shutdown_flag: asyncio.Event,
) -> tuple[str, str]:
    """
    Run the scraper for one branch with retry logic.

    Returns (branch_name, result) where result is one of the RESULT_* constants.
    """
    chain_slug: str = (branch.get("store_chains") or {}).get("slug", "")
    branch_name: str = branch.get("name", branch["id"])
    scraper_class = CHAIN_SCRAPERS.get(chain_slug)

    if scraper_class is None:
        logger.error(f"[{branch_name}] Unknown chain slug '{chain_slug}' — skipping.")
        return branch_name, RESULT_FAILED

    for attempt in range(max_retries + 1):
        if shutdown_flag.is_set():
            logger.info(f"[{branch_name}] Shutdown requested — not starting attempt {attempt + 1}.")
            return branch_name, RESULT_FAILED

        try:
            scraper = scraper_class(
                proxy_url=proxy_url,
                concurrency_semaphore=semaphore,
            )
            # Override the class-level branch_name so _ensure_chain_and_branch()
            # looks up the correct DB row (and its api_store_id) for this branch.
            scraper.branch_name = branch_name

            await scraper.run()
            return branch_name, RESULT_SUCCESS

        except Exception as exc:
            if attempt < max_retries:
                wait_s = 60 * (2 ** attempt)
                logger.warning(
                    f"[{branch_name}] Attempt {attempt + 1}/{max_retries + 1} failed: {exc}. "
                    f"Retrying in {wait_s}s..."
                )
                # Interruptible sleep so shutdown is noticed quickly.
                try:
                    await asyncio.wait_for(
                        shutdown_flag.wait(),
                        timeout=float(wait_s),
                    )
                    # shutdown_flag was set during the wait
                    logger.info(f"[{branch_name}] Shutdown during retry wait — aborting.")
                    return branch_name, RESULT_FAILED
                except asyncio.TimeoutError:
                    pass  # Normal path: wait elapsed, proceed to retry
            else:
                logger.error(
                    f"[{branch_name}] All {max_retries + 1} attempts failed. "
                    f"Last error: {exc}"
                )
                return branch_name, RESULT_FAILED

    # Should not be reached, but satisfy the type checker.
    return branch_name, RESULT_FAILED


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def run(args: argparse.Namespace) -> None:
    client: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    branches = fetch_branches(
        client,
        chain_slug=args.chain,
        branch_id=args.branch_id,
        stale_only=args.stale_only,
    )

    logger.info(f"Found {len(branches)} branches to scrape")

    if not branches:
        return

    if args.dry_run:
        print("\nDry run — branches that would be scraped:")
        for b in branches:
            slug = (b.get("store_chains") or {}).get("slug", "unknown")
            last = b.get("last_scraped_at") or "never"
            print(f"  [{slug}] {b['name']}  (last scraped: {last})")
        return

    concurrency = args.concurrency
    semaphore = asyncio.Semaphore(concurrency)
    shutdown_flag = asyncio.Event()

    # Signal handling — set shutdown flag, let in-flight tasks finish.
    loop = asyncio.get_running_loop()

    def _handle_signal(sig: signal.Signals) -> None:
        if not shutdown_flag.is_set():
            logger.warning(f"Received {sig.name} — finishing in-flight scrapers, not starting new ones.")
            shutdown_flag.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig)
        except (NotImplementedError, OSError):
            # Windows doesn't support add_signal_handler for all signals.
            pass

    start_time = datetime.now(timezone.utc)
    tasks: list[asyncio.Task] = []

    for index, branch in enumerate(branches):
        if shutdown_flag.is_set():
            break

        # Stagger the first `concurrency` launches by 10s each; after that,
        # tasks begin as semaphore slots free up (no extra delay needed).
        if index < concurrency and index > 0:
            await asyncio.sleep(10)

        if shutdown_flag.is_set():
            break

        task = asyncio.create_task(
            scrape_branch(
                branch=branch,
                semaphore=semaphore,
                proxy_url=args.proxy,
                max_retries=args.max_retries,
                shutdown_flag=shutdown_flag,
            ),
            name=branch.get("name", branch["id"]),
        )
        tasks.append(task)

    # Wait for all spawned tasks to complete.
    results: list[tuple[str, str]] = []
    if tasks:
        done = await asyncio.gather(*tasks, return_exceptions=False)
        results = list(done)

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    end_time = datetime.now(timezone.utc)
    duration = end_time - start_time
    total_seconds = int(duration.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    succeeded = [name for name, status in results if status == RESULT_SUCCESS]
    partial   = [name for name, status in results if status == RESULT_PARTIAL]
    failed    = [name for name, status in results if status == RESULT_FAILED]
    skipped   = len(branches) - len(results)

    print("\n" + "=" * 60)
    print("SCRAPE SUMMARY")
    print("=" * 60)
    print(f"  Total branches : {len(branches)}")
    print(f"  Succeeded      : {len(succeeded)}")
    print(f"  Partial        : {len(partial)}")
    print(f"  Failed         : {len(failed)}")
    if skipped:
        print(f"  Skipped        : {skipped}  (shutdown before launch)")
    print(f"  Duration       : {hours}h {minutes}m")

    if partial:
        print("\nPartial branches:")
        for name in partial:
            print(f"    - {name}")

    if failed:
        print("\nFailed branches:")
        for name in failed:
            print(f"    - {name}")

    print("=" * 60)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run scrapers for all (or filtered) store branches in parallel.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=6,
        metavar="INT",
        help="Max parallel scrapers (default: 6; max 32 when --proxy is set)",
    )
    parser.add_argument(
        "--chain",
        metavar="SLUG",
        choices=["woolworths", "new-world", "paknsave"],
        help="Filter to a single chain",
    )
    parser.add_argument(
        "--branch-id",
        metavar="UUID",
        help="Run a single branch by its UUID",
    )
    parser.add_argument(
        "--stale-only",
        action="store_true",
        help="Only branches with last_scraped_at NULL or older than 24h",
    )
    parser.add_argument(
        "--proxy",
        metavar="URL",
        help="Proxy gateway URL passed to each scraper",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        metavar="INT",
        help="Per-branch retry limit (default: 3)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print branches that would be scraped without running them",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.proxy and args.concurrency > 32:
        logger.warning("--concurrency capped at 32 when --proxy is set.")
        args.concurrency = 32
    elif not args.proxy and args.concurrency > 32:
        logger.warning("--concurrency > 32 without a proxy is not recommended.")

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        # asyncio.run() re-raises KeyboardInterrupt after the event loop exits;
        # suppress the traceback since we already printed a summary.
        sys.exit(0)


if __name__ == "__main__":
    main()
