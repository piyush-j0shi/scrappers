# base_scraper.py
"""
Base scraper class. All store scrapers inherit from this.
"""
from __future__ import annotations

import asyncio
import random
import re
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional

from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from supabase import create_client, Client

from config import SUPABASE_URL, SUPABASE_SERVICE_KEY, HEADLESS, SLOW_MO_MS, REQUEST_TIMEOUT_MS
from matching.product_matcher import ProductMatcher, _parse_weight_fields

logger = logging.getLogger(__name__)

# Realistic Windows/Chrome user agent (update periodically)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

# Headers that a real Chrome sends
EXTRA_HEADERS = {
    "Accept-Language": "en-NZ,en-GB;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Upgrade-Insecure-Requests": "1",
}

# Injected into every page before any script runs
STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

Object.defineProperty(navigator, 'plugins', {
    get: () => {
        const arr = [
            { filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
            { filename: 'internal-nacl-plugin', description: 'Native Client' },
        ];
        arr.item = (i) => arr[i];
        arr.namedItem = (n) => arr.find(p => p.filename === n);
        return arr;
    },
});

Object.defineProperty(navigator, 'languages', { get: () => ['en-NZ', 'en-GB', 'en'] });

if (!window.chrome) window.chrome = {};
if (!window.chrome.runtime) window.chrome.runtime = {};
window.chrome.loadTimes = function() { return {}; };
window.chrome.csi = function() { return {}; };

const _origPermQuery = navigator.permissions.query.bind(navigator.permissions);
navigator.permissions.query = (params) =>
    params.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : _origPermQuery(params);
"""


class ScrapedProduct:
    """Raw product data as scraped from a store website."""
    def __init__(
        self,
        store_name: str,
        raw_name: str,
        price: float,
        category: str = "other",
        special_price: Optional[float] = None,
        image_url: Optional[str] = None,
        source_url: Optional[str] = None,
        brand: Optional[str] = None,
        weight: Optional[str] = None,
        clean_name: Optional[str] = None,
        barcode: Optional[str] = None,
        product_id: Optional[str] = None,
    ):
        self.store_name = store_name
        self.raw_name = raw_name
        self.price = price
        self.category = category
        self.special_price = special_price
        self.image_url = image_url
        self.source_url = source_url
        self.brand = brand
        self.weight = weight
        self.clean_name = clean_name
        self.barcode = barcode
        self.product_id = product_id  # Foodstuffs internal ID — not stored in DB
        self.scraped_at = datetime.now(timezone.utc)


class BaseScraper(ABC):
    """
    Abstract base class for store scrapers.

    Subclasses must implement:
        scrape() -> list[ScrapedProduct]

    Subclasses must define class attributes:
        store_name: str   — display name, e.g. "New World"
        chain_slug: str   — matches store_chains.slug, e.g. "new-world"
        chain_name: str   — matches store_chains.name, e.g. "New World"
        branch_name: str  — matches store_branches.name, e.g. "New World New Lynn"
    """

    store_name: str
    chain_slug: str
    chain_name: str
    branch_name: str

    def __init__(
        self,
        search_terms: Optional[list] = None,
        proxy_url: Optional[str] = None,
        concurrency_semaphore: Optional[asyncio.Semaphore] = None,
        branch_name: Optional[str] = None,
        categories: Optional[list] = None,
    ) -> None:
        if branch_name:
            self.branch_name = branch_name
        self._supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        self._matcher = ProductMatcher()
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._search_terms: Optional[list] = search_terms
        self._proxy_url: Optional[str] = proxy_url
        self._semaphore: Optional[asyncio.Semaphore] = concurrency_semaphore
        self._categories: Optional[list] = categories
        self._chain_id: Optional[str] = None
        self._branch_id: Optional[str] = None
        self._api_store_id: Optional[str] = None

    async def _setup(self) -> None:
        self._playwright = await async_playwright().start()
        launch_kwargs: dict = {
            "headless": HEADLESS,
            "slow_mo": SLOW_MO_MS,
            "args": [
                "--disable-http2",
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        }
        if self._proxy_url:
            launch_kwargs["proxy"] = {"server": self._proxy_url}
        self._browser = await self._playwright.chromium.launch(**launch_kwargs)
        self._context = await self._browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="en-NZ",
            timezone_id="Pacific/Auckland",
            java_script_enabled=True,
            accept_downloads=False,
            extra_http_headers=EXTRA_HEADERS,
        )
        await self._context.add_init_script(STEALTH_SCRIPT)
        self._page = await self._context.new_page()
        self._page.set_default_timeout(REQUEST_TIMEOUT_MS)

    async def _teardown(self) -> None:
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    @abstractmethod
    async def scrape(self) -> list[ScrapedProduct]:
        ...

    async def run(self) -> None:
        logger.info(f"[{self.store_name}] Starting scrape...")
        await self._setup()
        run_id: Optional[str] = None
        stats: dict = {
            "records_updated": 0,
            "records_failed": 0,
            "new_products": 0,
            "price_changes": 0,
        }

        async def _do_run() -> None:
            nonlocal run_id
            try:
                self._ensure_chain_and_branch()
                run_id = self._start_run()
                products = await self.scrape()
                logger.info(f"[{self.store_name}] Scraped {len(products)} products")

                # Write total_scraped immediately — non-fatal if it fails
                if run_id:
                    try:
                        self._supabase.table("scraper_runs").update(
                            {"total_scraped": len(products)}
                        ).eq("id", run_id).execute()
                    except Exception as _e:
                        logger.warning(f"[{self.store_name}] Could not write total_scraped: {_e}")

                await self._process_and_save(products, stats)
                if stats["records_failed"] > 0 and stats["records_updated"] > 0:
                    status = "partial"
                elif stats["records_failed"] > 0 and stats["records_updated"] == 0:
                    status = "failed"
                else:
                    status = "success"
                self._end_run(run_id, status, stats)
            except Exception as e:
                if run_id:
                    self._end_run(run_id, "failed", stats, error=str(e))
                raise
            finally:
                await self._teardown()

        if self._semaphore:
            async with self._semaphore:
                await _do_run()
        else:
            await _do_run()
        logger.info(f"[{self.store_name}] Done.")

    def _ensure_chain_and_branch(self) -> None:
        """Upsert store_chains and store_branches rows, set self._chain_id and self._branch_id."""
        chain_result = self._supabase.table("store_chains").upsert(
            {"slug": self.chain_slug, "name": self.chain_name},
            on_conflict="slug",
        ).execute()
        self._chain_id = chain_result.data[0]["id"]
        logger.info(f"[{self.store_name}] chain_id={self._chain_id}")

        branch_result = self._supabase.table("store_branches").upsert(
            {"chain_id": self._chain_id, "name": self.branch_name},
            on_conflict="chain_id,name",
        ).execute()
        self._branch_id = branch_result.data[0]["id"]
        self._api_store_id = branch_result.data[0].get("api_store_id")
        logger.info(f"[{self.store_name}] branch_id={self._branch_id} api_store_id={self._api_store_id}")

    def _start_run(self) -> str:
        """Insert a scraper_runs row with status='running', return its id."""
        result = self._supabase.table("scraper_runs").insert({
            "chain_id": self._chain_id,
            "branch_id": self._branch_id,
            "status": "running",
            "started_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
        run_id = result.data[0]["id"]
        logger.info(f"[{self.store_name}] scraper_run_id={run_id}")
        return run_id

    def _end_run(
        self,
        run_id: str,
        status: str,
        stats: dict,
        error: Optional[str] = None,
    ) -> None:
        """Update the scraper_runs row with final status and counters."""
        payload: dict = {
            "status": status,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "records_updated": stats["records_updated"],
            "records_failed": stats["records_failed"],
            "new_products": stats["new_products"],
            "price_changes": stats["price_changes"],
        }
        if error:
            payload["error_log"] = error
        try:
            self._supabase.table("scraper_runs").update(payload).eq("id", run_id).execute()
            logger.info(f"[{self.store_name}] scraper_run {run_id} → {status}")
        except Exception as e:
            logger.error(f"[{self.store_name}] Failed to update scraper_run {run_id}: {e}")

        if status in ("success", "partial") and self._branch_id:
            try:
                self._supabase.table("store_branches").update(
                    {"last_scraped_at": datetime.now(timezone.utc).isoformat()}
                ).eq("id", self._branch_id).execute()
                logger.info(f"[{self.store_name}] Updated store_branches.last_scraped_at for branch {self._branch_id}")
            except Exception as e:
                logger.error(f"[{self.store_name}] Failed to update store_branches.last_scraped_at: {e}")

    async def _process_and_save(self, scraped: list[ScrapedProduct], stats: dict) -> None:
        if not self._branch_id:
            logger.error(
                f"[{self.store_name}] ABORT: branch_id is empty. "
                f"Check that store_chains/store_branches upsert succeeded."
            )
            return

        logger.info(f"[{self.store_name}] branch_id = {self._branch_id}")
        logger.info(f"[{self.store_name}] Processing {len(scraped)} scraped products...")

        CHUNK_SIZE = 200

        # Phase 1: Bulk-resolve barcoded products; serial fallback for no-barcode items.
        with_barcode = [item for item in scraped if item.barcode]
        without_barcode = [item for item in scraped if not item.barcode]
        logger.info(
            f"[{self.store_name}] Phase 1: {len(with_barcode)} with barcode, "
            f"{len(without_barcode)} without barcode"
        )

        # Snapshot barcodes already cached before this run — used to detect new products.
        known_barcodes: set[str] = {
            p["barcode"] for p in self._matcher._products if p.get("barcode")
        }

        barcode_to_product: dict[str, dict] = {}

        if with_barcode:
            # Build upsert payload (deduplicated by barcode — last writer wins).
            upsert_rows: dict[str, dict] = {}
            for item in with_barcode:
                weight_value, weight_unit = _parse_weight_fields(item.weight)
                row: dict = {"name": item.raw_name, "barcode": item.barcode, "source": "scraped"}
                if item.brand:
                    row["brand"] = item.brand
                if item.image_url:
                    row["image_url"] = item.image_url
                if weight_value is not None:
                    row["weight_value"] = weight_value
                if weight_unit is not None:
                    row["weight_unit"] = weight_unit
                upsert_rows[item.barcode] = row

            deduped_upsert = list(upsert_rows.values())
            for i in range(0, len(deduped_upsert), CHUNK_SIZE):
                chunk = deduped_upsert[i : i + CHUNK_SIZE]
                try:
                    self._supabase.table("products").upsert(
                        chunk, on_conflict="barcode", ignore_duplicates=True
                    ).execute()
                    logger.info(
                        f"[{self.store_name}] products upsert chunk "
                        f"{i // CHUNK_SIZE + 1}/{(len(deduped_upsert) - 1) // CHUNK_SIZE + 1}: "
                        f"submitted {len(chunk)} rows"
                    )
                except Exception as e:
                    logger.error(
                        f"[{self.store_name}] ERROR upserting products chunk "
                        f"{i // CHUNK_SIZE + 1}: {e} — retrying per-row via name lookup"
                    )
                    # Per-row fallback: chunk failed (likely a name-conflict 409).
                    # Resolve each row by name so barcode_to_product stays populated.
                    # Bulk name-fetch: one query for all names in the failed chunk.
                    chunk_names = [row["name"] for row in chunk]
                    try:
                        name_fetch = (
                            self._supabase.table("products")
                            .select("id,name,brand,barcode")
                            .in_("name", chunk_names)
                            .execute()
                        )
                        name_map: dict[str, dict] = {r["name"]: r for r in (name_fetch.data or [])}
                    except Exception as nf_e:
                        logger.error(
                            f"[{self.store_name}] Bulk name fetch failed: {nf_e} "
                            f"— chunk rows will fall through to serial match"
                        )
                        name_map = {}

                    # Assign barcode_to_product for found rows; collect new rows for batch insert.
                    new_rows: list[dict] = []
                    for row in chunk:
                        existing = name_map.get(row["name"])
                        if existing:
                            # Populate barcode_to_product before backfill so a failed update
                            # cannot block downstream resolution.
                            barcode_to_product[row["barcode"]] = existing
                            if not existing.get("barcode"):
                                try:
                                    self._supabase.table("products").update(
                                        {"barcode": row["barcode"]}
                                    ).eq("id", existing["id"]).execute()
                                    existing["barcode"] = row["barcode"]
                                    logger.info(
                                        f"[{self.store_name}] Backfilled barcode="
                                        f"{row['barcode']} onto '{row['name']}' "
                                        f"({existing['id']})"
                                    )
                                except Exception as upd_e:
                                    logger.warning(
                                        f"[{self.store_name}] Failed to backfill barcode "
                                        f"for '{row['name']}': {upd_e}"
                                    )
                        else:
                            new_rows.append(row)

                    logger.info(
                        f"[{self.store_name}] 409 fallback chunk: "
                        f"{len(name_map)} existing, {len(new_rows)} genuinely new"
                    )

                    # Batch-insert all genuinely new rows in one upsert.
                    if new_rows:
                        try:
                            ins = self._supabase.table("products").upsert(
                                new_rows, on_conflict="barcode", ignore_duplicates=False
                            ).execute()
                            for r in (ins.data or []):
                                if r.get("barcode"):
                                    barcode_to_product[r["barcode"]] = r
                            logger.info(
                                f"[{self.store_name}] Batch-inserted "
                                f"{len(ins.data or [])} new products"
                            )
                        except Exception as ins_e:
                            logger.error(
                                f"[{self.store_name}] Failed to batch-insert "
                                f"{len(new_rows)} new products: {ins_e}"
                            )

            # Bulk fetch all barcoded rows (chunk to avoid URL length limits).
            all_barcodes = list(dict.fromkeys(item.barcode for item in with_barcode))
            _fetch_chunk = 500
            for i in range(0, len(all_barcodes), _fetch_chunk):
                barcode_chunk = all_barcodes[i : i + _fetch_chunk]
                try:
                    result = (
                        self._supabase.table("products")
                        .select("id,name,brand,barcode")
                        .in_("barcode", barcode_chunk)
                        .execute()
                    )
                    for row in result.data:
                        barcode_to_product[row["barcode"]] = row
                except Exception as e:
                    logger.error(
                        f"[{self.store_name}] ERROR fetching products by barcode "
                        f"(chunk {i // _fetch_chunk + 1}): {e}"
                    )

            logger.info(
                f"[{self.store_name}] Fetched {len(barcode_to_product)}/{len(all_barcodes)} "
                f"products by barcode"
            )

            # Backfill matcher cache with rows not yet in memory.
            cached_ids = {p["id"] for p in self._matcher._products}
            new_cache_rows = 0
            for row in barcode_to_product.values():
                if row["id"] not in cached_ids:
                    self._matcher._products.append({
                        "id": row["id"],
                        "name": row["name"],
                        "brand": row.get("brand"),
                        "barcode": row["barcode"],
                    })
                    cached_ids.add(row["id"])
                    new_cache_rows += 1
            if new_cache_rows:
                logger.info(
                    f"[{self.store_name}] Backfilled {new_cache_rows} rows into matcher cache"
                )

        # Build matched list in original scraped order.
        matched: list[tuple[str, bool, ScrapedProduct]] = []
        for item in scraped:
            if item.barcode:
                product_row = barcode_to_product.get(item.barcode)
                if product_row is None:
                    # Upsert succeeded but fetch returned nothing — fall back to serial.
                    logger.warning(
                        f"[{self.store_name}] Barcode {item.barcode} not found after upsert, "
                        f"falling back to serial match for '{item.raw_name}'"
                    )
                    try:
                        product_id, _, is_new = self._matcher.match_or_create(
                            item.raw_name, item.category, item.image_url,
                            brand=item.brand, weight=item.weight, clean_name=item.clean_name,
                            barcode=item.barcode,
                        )
                        if is_new:
                            stats["new_products"] += 1
                        matched.append((product_id, is_new, item))
                    except ValueError as e:
                        logger.info(f"    SKIP: {e}")
                        stats["records_failed"] += 1
                    except Exception as e:
                        logger.error(f"    ERROR matching '{item.raw_name}': {e}")
                        stats["records_failed"] += 1
                else:
                    is_new = item.barcode not in known_barcodes
                    if is_new:
                        stats["new_products"] += 1
                    matched.append((product_row["id"], is_new, item))
            else:
                # Serial fallback for no-barcode items.
                logger.info(f"  Scraped (no barcode): '{item.raw_name}' @ ${item.price:.2f}")
                try:
                    product_id, _, is_new = self._matcher.match_or_create(
                        item.raw_name, item.category, item.image_url,
                        brand=item.brand, weight=item.weight, clean_name=item.clean_name,
                        barcode=item.barcode,
                    )
                    if is_new:
                        stats["new_products"] += 1
                    matched.append((product_id, is_new, item))
                except ValueError as e:
                    logger.info(f"    SKIP: {e}")
                    stats["records_failed"] += 1
                except Exception as e:
                    logger.error(f"    ERROR matching '{item.raw_name}': {e}")
                    stats["records_failed"] += 1

        logger.info(
            f"[{self.store_name}] Matched {len(matched)} products. "
            f"Fetching existing store_products for branch in one query..."
        )

        # Phase 2: Paginated fetch — all existing store_products for this branch
        # PostgREST caps .execute() at 1 000 rows by default; loop until exhausted.
        existing_map: dict[str, dict] = {}  # product_id -> {id, current_price, unit_price}
        _page_size = 1000
        _offset = 0
        try:
            while True:
                result = (
                    self._supabase.table("store_products")
                    .select("id,product_id,current_price,unit_price")
                    .eq("store_id", self._branch_id)
                    .range(_offset, _offset + _page_size - 1)
                    .execute()
                )
                for row in result.data:
                    existing_map[row["product_id"]] = row
                if len(result.data) < _page_size:
                    break
                _offset += _page_size
            logger.info(f"[{self.store_name}] Fetched {len(existing_map)} existing store_products")
        except Exception as e:
            logger.warning(f"[{self.store_name}] Could not fetch existing store_products: {e}")

        # Phase 3: Build batch rows in memory, detect price changes
        store_products_rows: list[dict] = []
        price_history_rows: list[dict] = []
        specials_by_product_id: dict[str, tuple[float, float]] = {}  # product_id -> (special_price, original_price)

        for product_id, _is_new, item in matched:
            existing = existing_map.get(product_id)
            if existing and existing.get("current_price") is not None:
                old_price = float(existing["current_price"])
                if abs(old_price - item.price) > 0.001:
                    price_history_rows.append({
                        "store_product_id": existing["id"],
                        "old_price": old_price,
                        "new_price": item.price,
                        "old_unit_price": existing.get("unit_price"),
                        "new_unit_price": None,
                    })
                    stats["price_changes"] += 1
                    logger.info(
                        f"  PRICE CHANGE '{item.raw_name}': ${old_price:.2f} → ${item.price:.2f}"
                    )

            store_products_rows.append({
                "product_id": product_id,
                "store_id": self._branch_id,
                "sku": item.barcode,
                "current_price": item.price,
                "unit_price": None,
                "unit_label": None,
                "in_stock": True,
                "scraped_at": item.scraped_at.isoformat(),
            })

            if item.special_price is not None:
                specials_by_product_id[product_id] = (item.special_price, item.price)

        # Deduplicate by product_id / store_product_id (keep last occurrence).
        # The API can return the same item in both a promo slot and its regular position.
        store_products_rows = list({r["product_id"]: r for r in store_products_rows}.values())
        price_history_rows = list({r["store_product_id"]: r for r in price_history_rows}.values())

        # Phase 4: Batch upsert store_products in chunks of 200
        store_product_id_map: dict[str, str] = {}  # product_id -> store_product_id
        total_upserted = 0
        for i in range(0, len(store_products_rows), CHUNK_SIZE):
            chunk = store_products_rows[i : i + CHUNK_SIZE]
            try:
                result = self._supabase.table("store_products").upsert(
                    chunk, on_conflict="product_id,store_id"
                ).execute()
                total_upserted += len(result.data)
                for row in result.data:
                    store_product_id_map[row["product_id"]] = row["id"]
                logger.info(
                    f"[{self.store_name}] store_products chunk "
                    f"{i // CHUNK_SIZE + 1}: upserted {len(result.data)} rows"
                )
            except Exception as e:
                logger.error(
                    f"[{self.store_name}] ERROR upserting store_products chunk "
                    f"{i // CHUNK_SIZE + 1}: {e}"
                )
                stats["records_failed"] += len(chunk)

        stats["records_updated"] = total_upserted

        # Phase 4.5: Upsert specials — deactivate stale ones, insert active ones
        if store_product_id_map:
            all_sp_ids = list(store_product_id_map.values())
            try:
                for i in range(0, len(all_sp_ids), 500):
                    chunk = all_sp_ids[i : i + 500]
                    self._supabase.table("specials").update(
                        {"is_active": False}
                    ).in_("store_product_id", chunk).eq("is_active", True).execute()
                logger.info(f"[{self.store_name}] Deactivated stale specials for this branch")
            except Exception as e:
                logger.warning(f"[{self.store_name}] Failed to deactivate stale specials: {e}")

            if specials_by_product_id:
                specials_rows = []
                for product_id, (sp_price, orig_price) in specials_by_product_id.items():
                    sp_id = store_product_id_map.get(product_id)
                    if sp_id:
                        specials_rows.append({
                            "store_product_id": sp_id,
                            "special_price": sp_price,
                            "original_price": orig_price,
                            "is_active": True,
                            "source": "scraped",
                            "card_required": False,
                        })
                for i in range(0, len(specials_rows), CHUNK_SIZE):
                    chunk = specials_rows[i : i + CHUNK_SIZE]
                    try:
                        self._supabase.table("specials").insert(chunk).execute()
                        logger.info(
                            f"[{self.store_name}] specials chunk "
                            f"{i // CHUNK_SIZE + 1}: inserted {len(chunk)} rows"
                        )
                    except Exception as e:
                        logger.warning(
                            f"[{self.store_name}] Failed to insert specials chunk "
                            f"{i // CHUNK_SIZE + 1}: {e}"
                        )
                logger.info(
                    f"[{self.store_name}] Saved {len(specials_rows)} active specials"
                )
            else:
                logger.info(f"[{self.store_name}] No specials found this run")

        # Phase 5: Mark products not seen in this run as out of stock
        matched_product_ids = [product_id for product_id, _, _ in matched]
        if matched_product_ids:
            try:
                if len(matched_product_ids) <= 500:
                    oos_result = (
                        self._supabase.table("store_products")
                        .update({"in_stock": False})
                        .eq("store_id", self._branch_id)
                        .not_.in_("product_id", matched_product_ids)
                        .eq("in_stock", True)
                        .execute()
                    )
                    logger.info(
                        f"[{self.store_name}] Marked {len(oos_result.data)} products out of stock"
                    )
                else:
                    # matched set too large for a single NOT IN — fetch in-stock IDs,
                    # diff in Python, then batch-update with IN
                    matched_set = set(matched_product_ids)
                    in_stock_ids: set[str] = set()
                    _oos_offset = 0
                    while True:
                        rows = (
                            self._supabase.table("store_products")
                            .select("product_id")
                            .eq("store_id", self._branch_id)
                            .eq("in_stock", True)
                            .range(_oos_offset, _oos_offset + _page_size - 1)
                            .execute()
                        ).data or []
                        for row in rows:
                            in_stock_ids.add(row["product_id"])
                        if len(rows) < _page_size:
                            break
                        _oos_offset += _page_size
                    to_mark_oos = list(in_stock_ids - matched_set)
                    oos_count = 0
                    for i in range(0, len(to_mark_oos), 500):
                        chunk = to_mark_oos[i : i + 500]
                        oos_result = (
                            self._supabase.table("store_products")
                            .update({"in_stock": False})
                            .eq("store_id", self._branch_id)
                            .in_("product_id", chunk)
                            .execute()
                        )
                        oos_count += len(oos_result.data)
                    logger.info(
                        f"[{self.store_name}] Marked {oos_count} products out of stock (batched)"
                    )
            except Exception as e:
                logger.warning(f"[{self.store_name}] Failed to mark out-of-stock products: {e}")

        # Phase 6: Batch insert price_history in chunks of 200
        if price_history_rows:
            for i in range(0, len(price_history_rows), CHUNK_SIZE):
                chunk = price_history_rows[i : i + CHUNK_SIZE]
                try:
                    self._supabase.table("price_history").insert(chunk).execute()
                    logger.info(
                        f"[{self.store_name}] price_history chunk "
                        f"{i // CHUNK_SIZE + 1}: inserted {len(chunk)} rows"
                    )
                except Exception as e:
                    logger.warning(
                        f"[{self.store_name}] Failed to insert price_history chunk "
                        f"{i // CHUNK_SIZE + 1}: {e}"
                    )

        logger.info(
            f"[{self.store_name}] Done. records_updated={stats['records_updated']}, "
            f"new_products={stats['new_products']}, price_changes={stats['price_changes']}, "
            f"records_failed={stats['records_failed']}"
        )

    async def _fresh_context(self) -> None:
        """Close the current page+context and open a fresh one.

        Call this after making direct API requests outside the browser (e.g. pagination
        bursts) so that Cloudflare sees a clean session on the next page.goto().
        """
        if self._page:
            await self._page.close()
        if self._context:
            await self._context.close()
        assert self._browser
        self._context = await self._browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="en-NZ",
            timezone_id="Pacific/Auckland",
            java_script_enabled=True,
            accept_downloads=False,
            extra_http_headers=EXTRA_HEADERS,
        )
        await self._context.add_init_script(STEALTH_SCRIPT)
        self._page = await self._context.new_page()
        self._page.set_default_timeout(REQUEST_TIMEOUT_MS)

    async def _random_delay(self, min_s: float = 2.0, max_s: float = 5.0) -> None:
        delay = random.uniform(min_s, max_s)
        logger.debug(f"  Waiting {delay:.1f}s...")
        await asyncio.sleep(delay)

    @staticmethod
    def parse_price(price_str: str) -> Optional[float]:
        if not price_str:
            return None
        cleaned = re.sub(r"[^\d.]", "", price_str.strip())
        if price_str.strip().endswith("c") and "." not in cleaned:
            try:
                return float(cleaned) / 100
            except ValueError:
                return None
        try:
            return float(cleaned)
        except ValueError:
            return None

    async def _scroll_to_bottom(self) -> None:
        assert self._page
        prev_height = 0
        while True:
            height = await self._page.evaluate("document.body.scrollHeight")
            if height == prev_height:
                break
            await self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await self._page.wait_for_timeout(800)
            prev_height = height
