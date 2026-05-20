"""
Multi-branch Foodstuffs scraper (Pak'nSave + New World).

Does the Cloudflare bypass ONCE per chain, then reuses the captured
session headers across all branches. Each branch is a POST with a
swapped storeId — no further browser navigation needed.

Usage:
    python run_foodstuffs_branches.py --chain paknsave --workers 3
    python run_foodstuffs_branches.py --chain new_world --workers 5
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import logging
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, BrowserContext, Page
from supabase import create_client, Client

from base_scraper import (
    BaseScraper,
    EXTRA_HEADERS,
    STEALTH_SCRIPT,
    USER_AGENT,
)
from matching.product_matcher import _parse_weight_fields
from config import (
    CHAIN_CONFIG,
    HEADLESS,
    REQUEST_TIMEOUT_MS,
    SLOW_MO_MS,
    SUPABASE_SERVICE_KEY,
    SUPABASE_URL,
)
from newworld_scraper import CATEGORY_URLS as NW_CATEGORY_URLS
from newworld_scraper import NewWorldScraper
from newworld_scraper import _category_from_url as _nw_category_from_url
from paknsave_scraper import CATEGORY_URLS as PNS_CATEGORY_URLS
from paknsave_scraper import PakNSaveScraper
from paknsave_scraper import _category_from_url as _pns_category_from_url

logger = logging.getLogger(__name__)

LOG_DIR = Path(__file__).parent / "logs"

# URL slug → category_hierarchy display value used inside algoliaQuery.filters.
# Shared across Pak'nSave + New World (Foodstuffs platform).
SLUG_TO_HIERARCHY: dict[str, str] = {
    "fruit-and-vegetables": "Fruit & Vegetables",
    "meat-poultry-and-seafood": "Meat, Poultry & Seafood",
    "fridge-deli-and-eggs": "Fridge, Deli & Eggs",
    "bakery": "Bakery",
    "frozen": "Frozen",
    "pantry": "Pantry",
    "hot-and-cold-drinks": "Hot & Cold Drinks",
    "snacks-treats-and-easy-meals": "Snacks, Treats & Easy Meals",
    "health-and-body": "Health & Body",
    "household-and-cleaning": "Household & Cleaning",
}


def _slug_from_url(url: str) -> str:
    m = re.search(r"/shop/category/([^/?#]+)", url)
    return m.group(1) if m else ""


def _derive_post_data(template: dict, src_hierarchy: str, tgt_hierarchy: str) -> dict:
    """Clone the captured template and swap the category_hierarchy value
    inside algoliaQuery.filters. Tries quoted form first, then bare."""
    body = copy.deepcopy(template)
    alg = body.get("algoliaQuery")
    if isinstance(alg, dict) and isinstance(alg.get("filters"), str):
        filters = alg["filters"]
        quoted_src = f'"{src_hierarchy}"'
        quoted_tgt = f'"{tgt_hierarchy}"'
        if quoted_src in filters:
            alg["filters"] = filters.replace(quoted_src, quoted_tgt)
        elif src_hierarchy in filters:
            alg["filters"] = filters.replace(src_hierarchy, tgt_hierarchy)
    return body


CHAINS = {
    "paknsave": {
        "scraper_cls": PakNSaveScraper,
        "category_urls": PNS_CATEGORY_URLS,
        "category_from_url": _pns_category_from_url,
        "barcode_host": "api-prod.paknsave.co.nz",
        "chain_slug": CHAIN_CONFIG["paknsave"]["slug"],
    },
    "new_world": {
        "scraper_cls": NewWorldScraper,
        "category_urls": NW_CATEGORY_URLS,
        "category_from_url": _nw_category_from_url,
        "barcode_host": "api-prod.newworld.co.nz",
        "chain_slug": CHAIN_CONFIG["new_world"]["slug"],
    },
}


def _setup_logging(chain: str) -> None:
    LOG_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = LOG_DIR / f"{chain}_branches_{timestamp}.txt"
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
    sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt)
    fh = logging.FileHandler(log_path, encoding="utf-8"); fh.setFormatter(fmt)
    root.addHandler(sh); root.addHandler(fh)
    logger.info(f"Logging to {log_path}")


def _fetch_branches(supabase: Client, chain_slug: str) -> list[dict]:
    """Fetch all branches for chain with non-null api_store_id."""
    chain_row = (
        supabase.table("store_chains").select("id").eq("slug", chain_slug).limit(1).execute()
    )
    if not chain_row.data:
        raise RuntimeError(f"Chain slug '{chain_slug}' not found in store_chains")
    chain_id = chain_row.data[0]["id"]

    branches: list[dict] = []
    offset = 0
    page_size = 1000
    while True:
        result = (
            supabase.table("store_branches")
            .select("id,chain_id,name,api_store_id")
            .eq("chain_id", chain_id)
            .not_.is_("api_store_id", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        branches.extend(result.data or [])
        if len(result.data or []) < page_size:
            break
        offset += page_size
    return branches


async def _bootstrap_chain(
    context: BrowserContext,
    category_urls: list[str],
) -> tuple[dict[str, str], str, str, dict[str, dict]]:
    """
    Visit each category URL on a fresh page (same context, so Cloudflare
    cookies persist after the first challenge). Each fresh page guarantees
    a clean cache / no service worker replay, so paginated/products fires
    for every category. Returns:
        headers, api_paginated_url, method, {category_url: post_data}
    """
    shared = {"headers": {}, "api_url": "", "method": "POST"}
    category_post_data: dict[str, dict] = {}

    async def _capture_one(cat_url: str) -> bool:
        """Open a fresh page, navigate, scroll, wait up to 5s for post_data."""
        page = await context.new_page()
        page.set_default_timeout(REQUEST_TIMEOUT_MS)

        async def intercept(route, request):
            try:
                if "paginated/products" in request.url:
                    if not shared["headers"]:
                        shared["headers"] = dict(request.headers)
                    if not shared["api_url"]:
                        shared["api_url"] = request.url
                        shared["method"] = request.method
                    if cat_url not in category_post_data:
                        try:
                            pd = request.post_data_json
                        except Exception:
                            pd = None
                        if pd:
                            category_post_data[cat_url] = pd
            except Exception:
                pass
            try:
                response = await route.fetch()
                await route.fulfill(response=response)
            except Exception:
                try:
                    await route.continue_()
                except Exception:
                    pass

        await page.route("**/paginated/products**", intercept)
        try:
            await page.goto(cat_url, wait_until="load", timeout=90_000)
            await page.wait_for_timeout(2000)
            # Scroll to trigger pagination firing
            prev_h = 0
            for _ in range(8):
                h = await page.evaluate("document.body.scrollHeight")
                if h == prev_h:
                    break
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(800)
                prev_h = h
                if cat_url in category_post_data:
                    break
            # Poll up to 5s
            for _ in range(10):
                if cat_url in category_post_data:
                    break
                await page.wait_for_timeout(500)
        except Exception as e:
            logger.warning(f"[bootstrap] navigation error for {cat_url}: {e}")
        finally:
            try:
                await page.close()
            except Exception:
                pass
        return cat_url in category_post_data

    # Pak'nSave / New World use Next.js client-side routing, so only the
    # first category load actually fires the paginated/products XHR. Capture
    # one template, then derive the rest by swapping the category_hierarchy
    # value inside algoliaQuery.filters.
    if not category_urls:
        raise RuntimeError("Bootstrap failed: no category URLs configured")

    first_url = category_urls[0]
    logger.info(f"[bootstrap] Navigating (1/{len(category_urls)}): {first_url}")
    ok = False
    for attempt in range(1, 3):
        ok = await _capture_one(first_url)
        if ok:
            logger.info(f"[bootstrap] Captured post_data for {first_url}")
            break
        logger.warning(
            f"[bootstrap] No post_data captured for {first_url} (attempt {attempt}/2)"
        )
        await asyncio.sleep(2)
    if not ok:
        raise RuntimeError(
            f"Bootstrap failed: could not capture post_data for {first_url} after 2 retries"
        )

    first_template = category_post_data[first_url]
    _alg = first_template.get("algoliaQuery") or {}
    _filters = _alg.get("filters") if isinstance(_alg, dict) else None
    logger.info(f"[bootstrap] category 1 algoliaQuery.filters = {_filters!r}")

    first_slug = _slug_from_url(first_url)
    src_hierarchy = SLUG_TO_HIERARCHY.get(first_slug)
    if not src_hierarchy:
        raise RuntimeError(
            f"Bootstrap failed: no SLUG_TO_HIERARCHY entry for first-category slug '{first_slug}'"
        )
    if _filters and src_hierarchy not in _filters:
        logger.warning(
            f"[bootstrap] Expected hierarchy {src_hierarchy!r} not found in filters — "
            "derivation may be incorrect; inspect the logged filters and update SLUG_TO_HIERARCHY"
        )

    for i, url in enumerate(category_urls[1:], start=2):
        slug = _slug_from_url(url)
        tgt_hierarchy = SLUG_TO_HIERARCHY.get(slug)
        if not tgt_hierarchy:
            raise RuntimeError(
                f"Bootstrap failed: no SLUG_TO_HIERARCHY entry for slug '{slug}' ({url})"
            )
        category_post_data[url] = _derive_post_data(first_template, src_hierarchy, tgt_hierarchy)
        logger.info(f"[bootstrap] Derived post_data for {url} ({i}/{len(category_urls)})")

    if not shared["headers"] or not shared["api_url"]:
        raise RuntimeError("Bootstrap failed: no headers or api URL captured")

    logger.info(
        f"[bootstrap] Cloudflare bypass complete. Headers captured. "
        f"Categories captured: {len(category_post_data)}/{len(category_urls)}"
    )
    return shared["headers"], shared["api_url"], shared["method"], category_post_data


def _override_store_in_post_data(post_data: dict, new_store_id: str) -> dict:
    """Deep-copy a post_data template and swap storeId + algoliaQuery.filters."""
    body = copy.deepcopy(post_data)
    store_field = next((f for f in ("storeId", "store_id") if f in body), None)
    if store_field:
        old_id = body.get(store_field) or ""
        body[store_field] = new_store_id
        alg = body.get("algoliaQuery") or {}
        if isinstance(alg, dict) and "filters" in alg and old_id and old_id in alg["filters"]:
            alg["filters"] = alg["filters"].replace(old_id, new_store_id)
    return body


async def _fetch_category_pages(
    page: Page,
    api_url: str,
    method: str,
    post_data: dict,
    headers: dict,
    branch_label: str,
) -> list[dict]:
    """Fetch all pages of a single category via page.request. Returns list of raw API items."""
    items: list[dict] = []
    hdrs = {k: v for k, v in headers.items() if k.lower() not in ("host", "content-length")}

    # Page 1
    total_pages = 1
    try:
        if method == "POST":
            body = copy.deepcopy(post_data); body["page"] = 1
            resp = await page.request.post(api_url, headers=hdrs, data=body, timeout=60_000)
        else:
            page_url = re.sub(r"\bpage=\d+\b", "page=1", api_url)
            if "page=" not in api_url:
                sep = "&" if "?" in page_url else "?"
                page_url = f"{page_url}{sep}page=1"
            resp = await page.request.get(page_url, headers=hdrs, timeout=60_000)
        if not resp.ok:
            logger.warning(f"  [{branch_label}] page 1 HTTP {resp.status}")
            return items
        data = await resp.json()
        items.extend(data.get("products") or data.get("items") or [])
        total_pages = max(1, int(data.get("totalPages") or 1))
    except Exception as e:
        logger.warning(f"  [{branch_label}] page 1 failed: {e}")
        return items

    # Pages 2..N
    for page_num in range(2, total_pages + 1):
        await asyncio.sleep(random.uniform(0.5, 2.0))
        try:
            if method == "POST":
                body = copy.deepcopy(post_data); body["page"] = page_num
                resp = await page.request.post(api_url, headers=hdrs, data=body, timeout=60_000)
            else:
                page_url = re.sub(r"\bpage=\d+\b", f"page={page_num}", api_url)
                if "page=" not in api_url:
                    sep = "&" if "?" in page_url else "?"
                    page_url = f"{page_url}{sep}page={page_num}"
                resp = await page.request.get(page_url, headers=hdrs, timeout=60_000)
            if resp.ok:
                data = await resp.json()
                items.extend(data.get("products") or data.get("items") or [])
            elif resp.status in (429, 403):
                logger.warning(
                    f"  [{branch_label}] page {page_num} HTTP {resp.status} — stopping this category"
                )
                break
            else:
                logger.warning(f"  [{branch_label}] page {page_num} HTTP {resp.status}")
                if resp.status >= 500:
                    break
        except Exception as e:
            logger.warning(f"  [{branch_label}] page {page_num} failed: {e}")
    return items


async def _scrape_branch(
    branch: dict,
    chain_cfg: dict,
    api_url: str,
    method: str,
    headers: dict,
    category_post_data: dict[str, dict],
    page_pool: asyncio.Queue,
    summary: dict,
) -> None:
    """Scrape one branch using a page from the pool. Writes results to Supabase."""
    branch_label = branch["name"]
    page: Page = await page_pool.get()
    scraper_cls = chain_cfg["scraper_cls"]
    category_from_url = chain_cfg["category_from_url"]

    # Instantiate scraper without _setup (we manage browser externally).
    scraper: BaseScraper = scraper_cls(branch_name=branch_label)
    scraper._page = page
    scraper._api_headers = headers
    scraper._api_paginated_url = api_url
    scraper._api_method = method
    scraper._branch_id = branch["id"]
    scraper._chain_id = branch["chain_id"]
    scraper._api_store_id = branch["api_store_id"]

    run_id: Optional[str] = None
    stats = {"records_updated": 0, "records_failed": 0, "new_products": 0, "price_changes": 0}
    try:
        # Ensure chain row exists (branch row already exists — we fetched it).
        scraper._supabase.table("store_chains").upsert(
            {"slug": scraper.chain_slug, "name": scraper.chain_name},
            on_conflict="slug",
        ).execute()
        run_id = scraper._start_run()

        logger.info(f"[{branch_label}] Starting (branch_id={branch['id']}, api_store_id={branch['api_store_id']})")

        products = []
        for cat_url, template in category_post_data.items():
            category = category_from_url(cat_url)
            post_data = _override_store_in_post_data(template, branch["api_store_id"])
            # Stash per-category post_data so _enrich_with_barcodes / any helpers see it.
            scraper._api_post_data = post_data
            try:
                items = await _fetch_category_pages(
                    page, api_url, method, post_data, headers, branch_label
                )
            except Exception as e:
                logger.warning(f"[{branch_label}] Category {cat_url} failed: {e}")
                continue
            found = 0
            for item in items:
                p = scraper._parse_api_product(item, category)
                if p:
                    products.append(p)
                    found += 1
            logger.info(f"[{branch_label}] {category}: {found} products (from {len(items)} items)")

        logger.info(f"[{branch_label}] Scraped {len(products)} products. Enriching barcodes...")

        if run_id:
            try:
                scraper._supabase.table("scraper_runs").update(
                    {"total_scraped": len(products)}
                ).eq("id", run_id).execute()
            except Exception:
                pass

        logger.info(f"[{branch_label}] Barcode enrichment skipped in bulk mode")

        await _bulk_save(scraper, products, stats, branch_label)

        if stats["records_failed"] > 0 and stats["records_updated"] > 0:
            status = "partial"
        elif stats["records_failed"] > 0 and stats["records_updated"] == 0:
            status = "failed"
        else:
            status = "success"
        scraper._end_run(run_id, status, stats)

        summary["branches_ok"] += 1
        summary["products"] += stats["records_updated"]
        summary["price_changes"] += stats["price_changes"]
        logger.info(
            f"[{branch_label}] Done: records_updated={stats['records_updated']}, "
            f"price_changes={stats['price_changes']}, new={stats['new_products']}"
        )
    except Exception as e:
        logger.warning(f"[{branch_label}] FAILED: {e}")
        summary["branches_failed"] += 1
        if run_id:
            try:
                scraper._end_run(run_id, "failed", stats, error=str(e))
            except Exception:
                pass
    finally:
        await page_pool.put(page)


async def _bulk_save(
    scraper: BaseScraper,
    scraped: list,
    stats: dict,
    branch_label: str,
) -> None:
    """Exact-name bulk save: no fuzzy matching, no [MATCH] logs.

    - Barcoded items: upsert products on barcode conflict.
    - Non-barcoded items: exact match on products.name; insert new rows for misses.
    - Then batch-upsert store_products for this branch and insert price_history.
    """
    if not scraper._branch_id:
        logger.error(f"[{branch_label}] ABORT: branch_id is empty")
        return

    supabase = scraper._supabase
    branch_id = scraper._branch_id
    CHUNK_SIZE = 200

    with_barcode = [it for it in scraped if it.barcode]
    without_barcode = [it for it in scraped if not it.barcode]
    logger.info(
        f"[{branch_label}] bulk_save: {len(with_barcode)} with barcode, "
        f"{len(without_barcode)} without barcode"
    )

    barcode_to_pid: dict[str, str] = {}
    name_to_pid: dict[str, str] = {}

    def _row_for(item) -> dict:
        weight_value, weight_unit = _parse_weight_fields(item.weight)
        row: dict = {"name": item.raw_name, "source": "scraped"}
        if item.barcode:
            row["barcode"] = item.barcode
        if item.brand:
            row["brand"] = item.brand
        if item.image_url:
            row["image_url"] = item.image_url
        if weight_value is not None:
            row["weight_value"] = weight_value
        if weight_unit is not None:
            row["weight_unit"] = weight_unit
        return row

    # --- Barcoded items: upsert by barcode, then bulk-fetch ids ---
    if with_barcode:
        upsert_rows: dict[str, dict] = {}
        for item in with_barcode:
            upsert_rows[item.barcode] = _row_for(item)
        deduped = list(upsert_rows.values())
        for i in range(0, len(deduped), CHUNK_SIZE):
            chunk = deduped[i : i + CHUNK_SIZE]
            try:
                supabase.table("products").upsert(
                    chunk, on_conflict="barcode", ignore_duplicates=True
                ).execute()
            except Exception as e:
                logger.warning(f"[{branch_label}] barcode upsert chunk {i // CHUNK_SIZE + 1} failed: {e}")

        all_barcodes = list(upsert_rows.keys())
        for i in range(0, len(all_barcodes), 500):
            chunk = all_barcodes[i : i + 500]
            try:
                result = (
                    supabase.table("products")
                    .select("id,barcode")
                    .in_("barcode", chunk)
                    .execute()
                )
                for row in (result.data or []):
                    if row.get("barcode"):
                        barcode_to_pid[row["barcode"]] = row["id"]
            except Exception as e:
                logger.warning(f"[{branch_label}] barcode fetch chunk failed: {e}")
        logger.info(f"[{branch_label}] Resolved {len(barcode_to_pid)}/{len(all_barcodes)} by barcode")

    # --- Non-barcoded items: exact-name lookup, insert missing ---
    if without_barcode:
        names = list({it.raw_name for it in without_barcode if it.raw_name})
        for i in range(0, len(names), 300):
            chunk = names[i : i + 300]
            try:
                result = (
                    supabase.table("products")
                    .select("id,name")
                    .in_("name", chunk)
                    .execute()
                )
                for row in (result.data or []):
                    name_to_pid[row["name"]] = row["id"]
            except Exception as e:
                logger.warning(f"[{branch_label}] name fetch chunk failed: {e}")

        missing_rows: dict[str, dict] = {}
        for item in without_barcode:
            if item.raw_name and item.raw_name not in name_to_pid:
                missing_rows.setdefault(item.raw_name, _row_for(item))

        if missing_rows:
            to_insert = list(missing_rows.values())
            logger.info(
                f"[{branch_label}] Inserting {len(to_insert)} new no-barcode products by exact name"
            )
            for i in range(0, len(to_insert), CHUNK_SIZE):
                chunk = to_insert[i : i + CHUNK_SIZE]
                try:
                    ins = supabase.table("products").upsert(
                        chunk, on_conflict="name", ignore_duplicates=True
                    ).execute()
                    for r in (ins.data or []):
                        if r.get("name"):
                            name_to_pid[r["name"]] = r["id"]
                except Exception as e:
                    logger.warning(f"[{branch_label}] new product insert chunk failed: {e}")

            # Any still-unresolved names (insert returned nothing due to ignore_duplicates):
            # re-fetch by name.
            still_missing = [n for n in missing_rows if n not in name_to_pid]
            for i in range(0, len(still_missing), 300):
                chunk = still_missing[i : i + 300]
                try:
                    result = (
                        supabase.table("products")
                        .select("id,name")
                        .in_("name", chunk)
                        .execute()
                    )
                    for row in (result.data or []):
                        name_to_pid[row["name"]] = row["id"]
                except Exception as e:
                    logger.warning(f"[{branch_label}] name refetch chunk failed: {e}")

        logger.info(f"[{branch_label}] Resolved {len(name_to_pid)} no-barcode products by exact name")

    # --- Resolve each scraped item to a product_id ---
    matched: list[tuple[str, object]] = []  # (product_id, item)
    for item in scraped:
        pid: Optional[str] = None
        if item.barcode:
            pid = barcode_to_pid.get(item.barcode)
        else:
            pid = name_to_pid.get(item.raw_name)
        if not pid:
            stats["records_failed"] += 1
            continue
        matched.append((pid, item))

    # --- Fetch existing store_products for this branch ---
    existing_map: dict[str, dict] = {}
    _offset = 0
    while True:
        try:
            rows = (
                supabase.table("store_products")
                .select("id,product_id,current_price,unit_price")
                .eq("store_id", branch_id)
                .range(_offset, _offset + 999)
                .execute()
            ).data or []
        except Exception as e:
            logger.warning(f"[{branch_label}] existing store_products fetch failed: {e}")
            break
        for row in rows:
            existing_map[row["product_id"]] = row
        if len(rows) < 1000:
            break
        _offset += 1000

    # --- Build store_products + price_history rows ---
    sp_rows: list[dict] = []
    ph_rows: list[dict] = []
    for product_id, item in matched:
        eff = item.special_price if item.special_price else item.price
        existing = existing_map.get(product_id)
        if existing and existing.get("current_price") is not None:
            old = float(existing["current_price"])
            if abs(old - eff) > 0.001:
                ph_rows.append({
                    "store_product_id": existing["id"],
                    "old_price": old,
                    "new_price": eff,
                    "old_unit_price": existing.get("unit_price"),
                    "new_unit_price": None,
                })
                stats["price_changes"] += 1

        sp_rows.append({
            "product_id": product_id,
            "store_id": branch_id,
            "sku": item.barcode,
            "current_price": eff,
            "unit_price": None,
            "unit_label": None,
            "in_stock": True,
            "scraped_at": item.scraped_at.isoformat(),
        })

    sp_rows = list({r["product_id"]: r for r in sp_rows}.values())
    ph_rows = list({r["store_product_id"]: r for r in ph_rows}.values())

    total_upserted = 0
    for i in range(0, len(sp_rows), CHUNK_SIZE):
        chunk = sp_rows[i : i + CHUNK_SIZE]
        try:
            result = supabase.table("store_products").upsert(
                chunk, on_conflict="product_id,store_id"
            ).execute()
            total_upserted += len(result.data or [])
        except Exception as e:
            logger.error(f"[{branch_label}] store_products upsert chunk failed: {e}")
            stats["records_failed"] += len(chunk)
    stats["records_updated"] = total_upserted

    # New products count (barcoded: any barcode whose row did not exist before in matcher cache is
    # expensive to compute here without fuzzy matching; approximate by 0 or skip).
    stats["new_products"] = 0

    if ph_rows:
        for i in range(0, len(ph_rows), CHUNK_SIZE):
            chunk = ph_rows[i : i + CHUNK_SIZE]
            try:
                supabase.table("price_history").insert(chunk).execute()
            except Exception as e:
                logger.warning(f"[{branch_label}] price_history insert chunk failed: {e}")

    logger.info(
        f"[{branch_label}] bulk_save done: records_updated={stats['records_updated']}, "
        f"price_changes={stats['price_changes']}, records_failed={stats['records_failed']}"
    )


async def run(chain: str, workers: int) -> None:
    chain_cfg = CHAINS[chain]
    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    branches = _fetch_branches(supabase, chain_cfg["chain_slug"])
    logger.info(f"[{chain}] Found {len(branches)} branches with api_store_id")
    if not branches:
        logger.warning("No branches to scrape — exiting")
        return

    t0 = time.time()
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=HEADLESS,
            slow_mo=SLOW_MO_MS,
            args=[
                "--disable-http2",
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="en-NZ",
            timezone_id="Pacific/Auckland",
            java_script_enabled=True,
            accept_downloads=False,
            extra_http_headers=EXTRA_HEADERS,
            service_workers="block",
        )
        await context.add_init_script(STEALTH_SCRIPT)

        try:
            headers, api_url, method, cat_post_data = await _bootstrap_chain(
                context, chain_cfg["category_urls"]
            )
        except Exception as e:
            logger.error(f"[{chain}] Bootstrap failed: {e}")
            await browser.close()
            return

        # Create pool of worker pages
        page_pool: asyncio.Queue[Page] = asyncio.Queue()
        pool_pages: list[Page] = []
        for _ in range(workers):
            p = await context.new_page()
            p.set_default_timeout(REQUEST_TIMEOUT_MS)
            pool_pages.append(p)
            await page_pool.put(p)

        summary = {"branches_ok": 0, "branches_failed": 0, "products": 0, "price_changes": 0}
        tasks = [
            _scrape_branch(branch, chain_cfg, api_url, method, headers, cat_post_data, page_pool, summary)
            for branch in branches
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

        for p in pool_pages:
            try:
                await p.close()
            except Exception:
                pass
        await browser.close()

    elapsed = int(time.time() - t0)
    mins, secs = divmod(elapsed, 60)
    logger.info(
        f"Done: {summary['branches_ok']} branches, {summary['products']} products, "
        f"{summary['price_changes']} price_changes, {summary['branches_failed']} failed "
        f"in {mins}m {secs}s"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-branch Foodstuffs scraper")
    parser.add_argument("--chain", choices=list(CHAINS), required=True)
    parser.add_argument("--workers", type=int, default=5)
    args = parser.parse_args()

    _setup_logging(args.chain)
    try:
        asyncio.run(run(args.chain, args.workers))
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")


if __name__ == "__main__":
    main()
