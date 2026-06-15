"""
Shared HTTP client used by all scrapers to POST branch reports to scraper_api.py.
Synchronous (urllib) so it can be run in an executor from async scraper code.
Never raises — failures are logged and silently ignored.
"""
from __future__ import annotations

import json
import logging
import urllib.request
from datetime import datetime, timezone
from typing import Optional

SCRAPER_API_URL = "http://127.0.0.1:8765"

logger = logging.getLogger("report_client")


def post_branch_report(
    *,
    chain: str,
    branch_name: str,
    branch_id: Optional[str],
    store_id: Optional[str],
    status: str,
    total_products: int,
    categories: list[dict],
    price_changes: int,
    specials: int,
    out_of_stock: int,
) -> bool:
    """POST a branch completion report to the scraper API. Returns True on success."""
    payload = {
        "chain": chain,
        "branch_name": branch_name,
        "branch_id": branch_id,
        "store_id": store_id,
        "status": status,
        "total_products": total_products,
        "categories": categories,
        "price_changes": price_changes,
        "specials": specials,
        "out_of_stock": out_of_stock,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        req = urllib.request.Request(
            f"{SCRAPER_API_URL}/branch-complete",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
        return True
    except Exception as e:
        logger.warning(f"[report] failed to post branch report: {e}")
        return False
