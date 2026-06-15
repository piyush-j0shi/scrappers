"""
Yogiji's Food Mart scraper — Claude build.

Scrapes yogijis.co.nz (WooCommerce) into Supabase.
No bot protection — Playwright browser navigation + JS evaluation.
No extra parser dependencies (BeautifulSoup/requests) needed.
Self-contained — does not import from any other scraper.

CLI:
  python3 yogijis_claude.py                    # scrape all categories
  python3 yogijis_claude.py --test             # first category only
  python3 yogijis_claude.py --dry-run          # parse only, no DB writes
  python3 yogijis_claude.py --categories biscuitskhari,snacks
  python3 yogijis_claude.py --no-headless      # show browser window
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import random
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from patchright.async_api import async_playwright, Browser, BrowserContext, Page
from supabase import create_client, Client
from report_client import post_branch_report

# ---------------------------------------------------------------------------
# Paths & env
# ---------------------------------------------------------------------------

THIS_DIR = Path(__file__).resolve().parent
SCRAPERS_DIR = THIS_DIR.parent
ENV_PATH = SCRAPERS_DIR / ".env"

load_dotenv(ENV_PATH)
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL       = "https://yogijis.co.nz"
CHAIN_SLUG     = "yogijis"
CHAIN_NAME     = "Yogiji's Food Mart"
BRANCH_NAME    = "Yogiji's Food Mart Online"

PRODUCTS_PER_PAGE  = 96          # site supports up to 160
DELAY_MIN          = 1.5
DELAY_MAX          = 3.0
REQUEST_TIMEOUT_MS = 30_000

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("yogijis_claude")

_log_dir = THIS_DIR / "logs"
_log_dir.mkdir(exist_ok=True)


def _setup_file_logging() -> None:
    from datetime import date
    log_file = _log_dir / f"yogijis_{date.today()}.log"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    logging.getLogger().addHandler(fh)
    logger.info(f"logging to {log_file}")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class ScrapedProduct:
    raw_name:      str
    clean_name:    str
    price:         float
    category:      str
    special_price: Optional[float]   = None
    image_url:     Optional[str]     = None
    barcode:       Optional[str]     = None
    in_stock:      bool              = True
    product_id:    Optional[str]     = None
    product_url:   Optional[str]     = None
    scraped_at:    datetime          = field(default_factory=lambda: datetime.now(timezone.utc))


_PRICE_RE = re.compile(r"[\d,]+\.?\d*")


def _parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    m = _PRICE_RE.search(text.replace(",", ""))
    return float(m.group()) if m else None


# ---------------------------------------------------------------------------
# JavaScript evaluated in-browser — no HTML parser library needed
# ---------------------------------------------------------------------------

# Extracts all unique /product-category/ slugs from the page.
_JS_DISCOVER_CATEGORIES = """
() => {
    const seen = new Set();
    const cats = [];
    document.querySelectorAll('a[href]').forEach(a => {
        const m = a.href.match(/\\/product-category\\/([^/?#]+)\\//);
        if (!m || seen.has(m[1])) return;
        seen.add(m[1]);
        const raw = (a.innerText || a.textContent || '').trim();
        const name = raw.replace(/\\s+\\d+$/, '').trim();  // strip trailing badge count
        cats.push({ slug: m[1], name: name || m[1], url: a.href.split('?')[0] });
    });
    return cats;
}
"""

# Extracts all product cards from a category listing page.
_JS_EXTRACT_PRODUCTS = """
() => {
    const cards = document.querySelectorAll('li.product');
    return Array.from(cards).map(card => {
        // Name: WooCommerce puts it in h2/h3 with this class, or a bare <h3><a>
        const titleEl = card.querySelector(
            '.woocommerce-loop-product__title, h3 > a, h2 > a'
        );
        const name = (titleEl?.innerText || titleEl?.textContent || '').trim();

        // Product URL
        const linkEl = card.querySelector('a[href*="/product/"]');
        const product_url = linkEl?.href || '';

        // WooCommerce product ID lives in the add-to-cart query param
        const cartLink = card.querySelector('a[href*="add-to-cart"]');
        const cartHref = cartLink?.getAttribute('href') || '';
        const idMatch  = cartHref.match(/add-to-cart=(\\d+)/);
        const product_id = idMatch ? idMatch[1] : '';

        // Image — prefer lazy-loaded data-src
        const imgEl    = card.querySelector('img');
        const raw_img  = imgEl?.getAttribute('data-src') || imgEl?.getAttribute('src') || '';
        const image_url = raw_img.includes('placeholder') ? '' : raw_img;

        // Price: sale products use <del> (original) + <ins> (sale)
        const priceEl  = card.querySelector('.price');
        const insEl    = priceEl?.querySelector('ins');
        const delEl    = priceEl?.querySelector('del');
        const amounts  = Array.from(
            priceEl?.querySelectorAll('.woocommerce-Price-amount') || []
        );

        let price = '', sale_price = '';
        if (insEl && delEl) {
            price      = (delEl.innerText || delEl.textContent || '').trim();
            sale_price = (insEl.innerText || insEl.textContent || '').trim();
        } else if (amounts.length) {
            // Variable products show a range — take the first (lower) amount
            price = (amounts[0].innerText || amounts[0].textContent || '').trim();
        } else {
            price = (priceEl?.innerText || priceEl?.textContent || '').trim();
        }

        const in_stock = !card.classList.contains('outofstock');

        return { name, price, sale_price, image_url, product_url, product_id, in_stock };
    }).filter(p => p.name && p.price);
}
"""

# True if a "next page" link is present.
_JS_HAS_NEXT_PAGE = """
() => !!(
    document.querySelector('.woocommerce-pagination a.next') ||
    document.querySelector('nav.woocommerce-pagination a[rel="next"]')
)
"""


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class YogijisScraper:

    def __init__(
        self,
        category_slugs: Optional[list[str]] = None,
        dry_run:        bool = False,
        test_mode:      bool = False,
        headless:       bool = True,
    ) -> None:
        if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
            raise RuntimeError(f"Missing Supabase env vars — check {ENV_PATH}")
        self.supabase:        Client                   = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        self.category_slugs:  Optional[list[str]]      = category_slugs
        self.dry_run:         bool                     = dry_run
        self.test_mode:       bool                     = test_mode
        self.headless:        bool                     = headless
        self.branch_id:       Optional[str]            = None
        self.chain_id:        Optional[str]            = None
        self._playwright                               = None
        self._browser:        Optional[Browser]        = None
        self._context:        Optional[BrowserContext] = None
        self._page:           Optional[Page]           = None

    # ---- Browser ------------------------------------------------------------

    async def _start_browser(self) -> None:
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        self._context = await self._browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="en-NZ",
            timezone_id="Pacific/Auckland",
        )
        self._page = await self._context.new_page()
        self._page.set_default_timeout(REQUEST_TIMEOUT_MS)

    async def _close_browser(self) -> None:
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    # ---- Branch resolution --------------------------------------------------

    def _resolve_branch(self) -> None:
        chain = (
            self.supabase.table("store_chains")
            .select("id").eq("slug", CHAIN_SLUG).execute().data
        )
        if not chain:
            chain = (
                self.supabase.table("store_chains")
                .upsert({"slug": CHAIN_SLUG, "name": CHAIN_NAME}, on_conflict="slug")
                .execute().data
            )
        self.chain_id = chain[0]["id"]

        r = (
            self.supabase.table("store_branches")
            .select("id").eq("chain_id", self.chain_id).eq("name", BRANCH_NAME)
            .execute().data
        )
        if r:
            self.branch_id = r[0]["id"]
            return

        r = (
            self.supabase.table("store_branches")
            .upsert({"chain_id": self.chain_id, "name": BRANCH_NAME}, on_conflict="chain_id,name")
            .execute().data
        )
        self.branch_id = r[0]["id"]

    # ---- Run tracking -------------------------------------------------------

    def _start_run(self) -> Optional[str]:
        if self.dry_run:
            return None
        try:
            r = self.supabase.table("scraper_runs").insert({
                "chain_id":   self.chain_id,
                "branch_id":  self.branch_id,
                "status":     "running",
                "started_at": datetime.now(timezone.utc).isoformat(),
            }).execute()
            return r.data[0]["id"]
        except Exception as e:
            logger.warning(f"could not insert scraper_runs row: {e}")
            return None

    def _update_run(self, run_id: Optional[str], **fields) -> None:
        if not run_id:
            return
        try:
            self.supabase.table("scraper_runs").update(fields).eq("id", run_id).execute()
        except Exception as e:
            logger.warning(f"scraper_runs update failed: {e}")

    def _end_run(
        self, run_id: Optional[str], status: str, stats: dict, error: Optional[str] = None
    ) -> None:
        if not run_id:
            return
        payload = {
            "status":          status,
            "finished_at":     datetime.now(timezone.utc).isoformat(),
            "records_updated": stats["records_updated"],
            "records_failed":  stats["records_failed"],
            "new_products":    stats["new_products"],
            "price_changes":   stats["price_changes"],
        }
        if error:
            payload["error_log"] = error[:2000]
        try:
            self.supabase.table("scraper_runs").update(payload).eq("id", run_id).execute()
        except Exception as e:
            logger.warning(f"scraper_runs finalise failed: {e}")
        if status in ("success", "partial") and self.branch_id:
            try:
                self.supabase.table("store_branches").update(
                    {"last_scraped_at": datetime.now(timezone.utc).isoformat()}
                ).eq("id", self.branch_id).execute()
            except Exception:
                pass

    # ---- Category discovery -------------------------------------------------

    async def _discover_categories(self) -> list[dict]:
        logger.info("discovering categories from homepage ...")
        await self._page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60_000)
        await self._page.wait_for_timeout(1500)
        cats: list[dict] = await self._page.evaluate(_JS_DISCOVER_CATEGORIES)
        cats = [c for c in cats if c["slug"] and c["name"]]
        logger.info(f"found {len(cats)} categories: {[c['slug'] for c in cats]}")
        return cats

    # ---- Product scraping ---------------------------------------------------

    async def _scrape_category(self, cat: dict) -> list[ScrapedProduct]:
        slug     = cat["slug"]
        base_url = cat["url"].rstrip("/") + "/"
        all_products: list[ScrapedProduct] = []
        page_num = 1

        while True:
            url = (
                f"{base_url}?per_page={PRODUCTS_PER_PAGE}"
                if page_num == 1
                else f"{base_url}page/{page_num}/?per_page={PRODUCTS_PER_PAGE}"
            )
            logger.info(f"  [{slug}] page {page_num} ...")

            resp = await self._page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            if resp and resp.status == 404:
                logger.info(f"  [{slug}] 404 on page {page_num} — done")
                break
            await self._page.wait_for_timeout(1000)

            raw: list[dict] = await self._page.evaluate(_JS_EXTRACT_PRODUCTS)
            products: list[ScrapedProduct] = []
            for item in raw:
                price = _parse_price(item["price"])
                if not price or not item["name"]:
                    continue
                sale = _parse_price(item["sale_price"]) if item.get("sale_price") else None
                # sale_price must be lower than regular price to be valid
                if sale and sale >= price:
                    sale = None
                products.append(ScrapedProduct(
                    raw_name=item["name"],
                    clean_name=" ".join(item["name"].split()),
                    price=price,
                    special_price=sale,
                    category=slug,
                    image_url=item["image_url"] or None,
                    in_stock=item["in_stock"],
                    product_id=item["product_id"] or None,
                    product_url=item["product_url"] or None,
                ))

            logger.info(f"  [{slug}] page {page_num}: {len(products)} products")
            all_products.extend(products)

            has_next: bool = await self._page.evaluate(_JS_HAS_NEXT_PAGE)
            if not has_next or not products:
                break
            page_num += 1
            await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        return all_products

    # ---- Supabase write pipeline --------------------------------------------

    def _fetch_price_snapshot(self) -> dict[str, dict]:
        """Fetch all existing store_products for this branch once before the run.
        Keyed by product_id so each category save can detect real price changes
        without re-hitting the DB 39 times."""
        existing_map: dict[str, dict] = {}
        if not self.branch_id:
            return existing_map
        page_size, offset = 1000, 0
        try:
            while True:
                r = (
                    self.supabase.table("store_products")
                    .select("id,product_id,current_price,unit_price")
                    .eq("store_id", self.branch_id)
                    .range(offset, offset + page_size - 1)
                    .execute()
                )
                for row in r.data:
                    existing_map[row["product_id"]] = row
                if len(r.data) < page_size:
                    break
                offset += page_size
            logger.info(f"price snapshot: {len(existing_map)} existing store_products loaded")
        except Exception as e:
            logger.warning(f"store_products snapshot failed: {e}")
        return existing_map

    def _save_category(
        self,
        products: list[ScrapedProduct],
        stats: dict,
        price_snapshot: dict[str, dict],
        saved_names: set[str],
    ) -> None:
        """Save one category's products immediately after scraping.

        price_snapshot  — pre-fetched map of product_id → store_products row,
                          updated in-place so subsequent categories see correct old prices.
        saved_names     — names already written this run (cross-category dedup),
                          updated in-place.
        """
        if not products:
            return
        CHUNK = 500

        # Skip products already saved in a previous category this run
        deduped = [p for p in products if p.raw_name not in saved_names]
        if not deduped:
            logger.info(f"  [save] all {len(products)} products already saved — skipping")
            return
        logger.info(f"  [save] {len(deduped)} new products to save ({len(products) - len(deduped)} cross-category dupes skipped)")

        # Fetch which of these names already exist in the products table
        all_names = [p.raw_name for p in deduped]
        existing_by_name: dict[str, dict] = {}
        for i in range(0, len(all_names), 500):
            try:
                r = (
                    self.supabase.table("products")
                    .select("id,name")
                    .in_("name", all_names[i: i + 500])
                    .execute()
                )
                for row in r.data:
                    existing_by_name[row["name"]] = row
            except Exception as e:
                logger.warning(f"products fetch chunk {i // 500 + 1}: {e}")

        # Insert genuinely new products
        new_rows: list[dict] = []
        for p in deduped:
            if p.raw_name not in existing_by_name:
                row: dict = {"name": p.raw_name, "source": "scraped"}
                if p.image_url:
                    row["image_url"] = p.image_url
                new_rows.append(row)

        for i in range(0, len(new_rows), CHUNK):
            chunk = new_rows[i: i + CHUNK]
            try:
                r = self.supabase.table("products").insert(chunk).execute()
                for row in (r.data or []):
                    existing_by_name[row["name"]] = row
            except Exception as e:
                logger.error(f"products insert chunk {i // CHUNK + 1}: {e}")

        # Re-fetch IDs for anything not returned by insert
        still_missing = [p.raw_name for p in deduped if p.raw_name not in existing_by_name]
        for i in range(0, len(still_missing), 500):
            try:
                r = (
                    self.supabase.table("products")
                    .select("id,name")
                    .in_("name", still_missing[i: i + 500])
                    .execute()
                )
                for row in r.data:
                    existing_by_name[row["name"]] = row
            except Exception as e:
                logger.warning(f"products re-fetch chunk: {e}")

        # Resolve product_id for every scraped product
        matched: list[tuple[str, ScrapedProduct]] = []
        for p in deduped:
            row = existing_by_name.get(p.raw_name)
            if row:
                matched.append((row["id"], p))
                saved_names.add(p.raw_name)  # mark as saved for future categories
            else:
                stats["records_failed"] += 1
                logger.warning(f"unresolved: {p.raw_name!r}")

        stats["new_products"] += len(new_rows)

        # Build store_products rows
        sp_rows: list[dict] = []
        new_price_map: dict[str, float] = {}
        for product_id, p in matched:
            effective = p.special_price if p.special_price else p.price
            new_price_map[product_id] = effective
            sp_rows.append({
                "product_id":    product_id,
                "store_id":      self.branch_id,
                "sku":           p.barcode,
                "current_price": effective,
                "unit_price":    None,
                "unit_label":    None,
                "in_stock":      p.in_stock,
                "scraped_at":    p.scraped_at.isoformat(),
            })
        sp_rows = list({r["product_id"]: r for r in sp_rows}.values())

        # Detect price changes against the pre-fetched snapshot
        ph_rows: list[dict] = []
        for product_id, effective in new_price_map.items():
            existing = price_snapshot.get(product_id)
            if existing and existing.get("current_price") is not None:
                old_price = float(existing["current_price"])
                if abs(old_price - effective) > 0.001:
                    ph_rows.append({
                        "store_product_id": existing["id"],
                        "old_price":        old_price,
                        "new_price":        effective,
                        "old_unit_price":   existing.get("unit_price"),
                        "new_unit_price":   None,
                    })
                    stats["price_changes"] += 1
        ph_rows = list({r["store_product_id"]: r for r in ph_rows}.values())

        total = 0
        for i in range(0, len(sp_rows), CHUNK):
            chunk = sp_rows[i: i + CHUNK]
            try:
                r = self.supabase.table("store_products").upsert(
                    chunk, on_conflict="product_id,store_id"
                ).execute()
                total += len(r.data)
                # Update snapshot in-place so next categories see the latest price
                for row in (r.data or []):
                    pid = row.get("product_id")
                    if pid:
                        price_snapshot[pid] = row
            except Exception as e:
                logger.error(f"store_products upsert chunk {i // CHUNK + 1}: {e}")
                stats["records_failed"] += len(chunk)
        stats["records_updated"] += total

        if ph_rows:
            for i in range(0, len(ph_rows), CHUNK):
                chunk = ph_rows[i: i + CHUNK]
                try:
                    self.supabase.table("price_history").insert(chunk).execute()
                except Exception as e:
                    logger.warning(f"price_history insert chunk {i // CHUNK + 1}: {e}")
            logger.info(f"  [save] {total} store_products upserted  {len(ph_rows)} price changes  {len(new_rows)} new products")
        else:
            logger.info(f"  [save] {total} store_products upserted  {len(new_rows)} new products")

    # ---- Main run -----------------------------------------------------------

    async def run(self) -> dict:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._resolve_branch)
        logger.info(f"chain={CHAIN_NAME}  branch={BRANCH_NAME}  branch_id={self.branch_id}")

        await self._start_browser()
        try:
            all_categories = await self._discover_categories()
            if not all_categories:
                raise RuntimeError("No categories found — site structure may have changed")

            if self.category_slugs:
                cats = [c for c in all_categories if c["slug"] in self.category_slugs]
                if not cats:
                    raise ValueError(f"None of {self.category_slugs} matched discovered categories")
            elif self.test_mode:
                cats = all_categories[:1]
            else:
                cats = all_categories

            run_id = self._start_run()
            stats = {
                "records_updated": 0, "records_failed": 0, "new_products": 0,
                "price_changes": 0, "categories_failed": 0, "category_results": [],
            }
            all_products: list[ScrapedProduct] = []

            # Fetch price snapshot once before the loop — passed to every _save_category
            # call so we never re-fetch the same data 39 times.
            price_snapshot: dict[str, dict] = {}
            saved_names: set[str] = set()
            if not self.dry_run:
                price_snapshot = await loop.run_in_executor(None, self._fetch_price_snapshot)

            for cat_idx, cat in enumerate(cats, 1):
                logger.info(f"[{cat_idx}/{len(cats)}] {cat['name']} ({cat['slug']})")
                exc: Optional[Exception] = None
                products: list[ScrapedProduct] = []

                for attempt in range(1, 4):
                    try:
                        products = await self._scrape_category(cat)
                        if products:
                            break
                        logger.warning(f"  attempt {attempt}: 0 products")
                    except Exception as e:
                        exc = e
                        logger.warning(f"  attempt {attempt} failed: {e}")
                    if attempt < 3:
                        await asyncio.sleep(3 * attempt)

                if not products and exc is not None:
                    stats["categories_failed"] += 1
                    stats["category_results"].append({
                        "name": cat["slug"], "status": "failed", "products": 0,
                        "reason": f"{type(exc).__name__}: {exc}",
                    })
                elif not products:
                    stats["category_results"].append({
                        "name": cat["slug"], "status": "empty", "products": 0,
                    })
                else:
                    stats["category_results"].append({
                        "name": cat["slug"], "status": "success", "products": len(products),
                    })
                    if not self.dry_run:
                        await loop.run_in_executor(
                            None, self._save_category, products, stats, price_snapshot, saved_names
                        )

                all_products.extend(products)
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

            logger.info(f"TOTAL scraped: {len(all_products)} products")
            self._update_run(run_id, total_scraped=len(all_products))

            if self.dry_run:
                logger.info("DRY RUN — skipping Supabase writes")

            status = (
                "failed"  if (stats["records_failed"] and not stats["records_updated"])
                else "partial" if stats["records_failed"]
                else "success"
            )
            self._end_run(run_id, status, stats)

            specials     = sum(1 for p in all_products if p.special_price is not None)
            out_of_stock = sum(1 for p in all_products if not p.in_stock)

            if specials:
                sample = [
                    f"{p.clean_name!r} ${p.price:.2f}→${p.special_price:.2f}"
                    for p in all_products if p.special_price is not None
                ][:5]
                logger.info(f"[specials] {specials}/{len(all_products)} — sample: {'; '.join(sample)}")
            if out_of_stock:
                sample_oos = [p.clean_name for p in all_products if not p.in_stock][:5]
                logger.info(f"[oos] {out_of_stock}/{len(all_products)} — sample: {sample_oos}")

            post_branch_report(
                chain=CHAIN_NAME,
                branch_name=BRANCH_NAME,
                branch_id=str(self.branch_id) if self.branch_id else None,
                store_id=None,
                status=status,
                total_products=len(all_products),
                categories=stats["category_results"],
                price_changes=stats["price_changes"],
                specials=specials,
                out_of_stock=out_of_stock,
            )
            return stats

        except Exception as e:
            logger.exception("run failed")
            raise
        finally:
            await self._close_browser()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    _setup_file_logging()
    parser = argparse.ArgumentParser(description="Scrape Yogiji's Food Mart into Supabase")
    parser.add_argument("--test",        action="store_true", help="first category only")
    parser.add_argument("--dry-run",     action="store_true", help="no DB writes")
    parser.add_argument("--categories",  help="comma-separated slugs, e.g. biscuitskhari,snacks")
    parser.add_argument("--no-headless", action="store_true", help="show browser window")
    args = parser.parse_args()

    category_slugs = (
        [s.strip() for s in args.categories.split(",")] if args.categories else None
    )

    scraper = YogijisScraper(
        category_slugs=category_slugs,
        dry_run=args.dry_run,
        test_mode=args.test,
        headless=not args.no_headless,
    )
    stats = asyncio.run(scraper.run())
    logger.info(f"done: {stats}")


if __name__ == "__main__":
    main()
