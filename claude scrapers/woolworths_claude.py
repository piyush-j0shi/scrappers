"""
Woolworths NZ scraper — Claude build.

Self-contained scraper for Woolworths NZ (woolworths.co.nz). Captures the
/api/v1/products responses the storefront fires while browsing categories,
then paginates the same endpoint with the captured auth headers.

Data extracted per product:
  - name (raw + cleaned)
  - brand
  - barcode
  - category (from URL)
  - quantity / weight (volumeSize)
  - price (originalPrice)
  - special_price (salePrice if < price)
  - in_stock (from availabilityStatus)
  - image_url

Upserts into the live Supabase schema (auto-detected):
  products, store_chains, store_branches, store_products, price_history, scraper_runs.

Matching strategy:
  1. Barcode-first: bulk upsert products on conflict=barcode. Fetch back by barcode.
  2. Name-fallback: for items without a barcode (or after a 409 conflict),
     resolve by exact name; create a new product row if none exists.

CLI:
  python3 woolworths_claude.py --test                       # 3 categories, default branch
  python3 woolworths_claude.py --branch "Woolworths Ponsonby"
  python3 woolworths_claude.py --branch-id <uuid>
  python3 woolworths_claude.py --all-branches               # loop every branch with a session
  python3 woolworths_claude.py --categories fruit-veg,bakery
  python3 woolworths_claude.py --no-headless --dry-run

Reads SUPABASE_URL and SUPABASE_SERVICE_KEY from ../scrapers/.env.
Does not modify or import existing scraper modules.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import logging
import math
import os
import random
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from dotenv import load_dotenv
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Paths & env
# ---------------------------------------------------------------------------

THIS_DIR = Path(__file__).resolve().parent
SCRAPERS_DIR = THIS_DIR.parent                    # ../scrapers
SESSIONS_DIR = SCRAPERS_DIR / "sessions" / "woolworths"
ENV_PATH = SCRAPERS_DIR / ".env"

load_dotenv(ENV_PATH)
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://www.woolworths.co.nz"

# Categories cover the full /shop/browse top-level taxonomy (verified 2026-05).
CATEGORY_URLS: list[str] = [
    "https://www.woolworths.co.nz/shop/browse/fruit-veg",
    "https://www.woolworths.co.nz/shop/browse/meat-poultry",
    "https://www.woolworths.co.nz/shop/browse/fish-seafood",
    "https://www.woolworths.co.nz/shop/browse/fridge-deli",
    "https://www.woolworths.co.nz/shop/browse/bakery",
    "https://www.woolworths.co.nz/shop/browse/frozen",
    "https://www.woolworths.co.nz/shop/browse/pantry",
    "https://www.woolworths.co.nz/shop/browse/drinks",
    "https://www.woolworths.co.nz/shop/browse/beer-wine",
    "https://www.woolworths.co.nz/shop/browse/health-body",
    "https://www.woolworths.co.nz/shop/browse/household",
    "https://www.woolworths.co.nz/shop/browse/baby-child",
    "https://www.woolworths.co.nz/shop/browse/pet",
]

URL_TO_CATEGORY: dict[str, str] = {
    "fruit-veg": "produce",
    "meat-poultry": "meat",
    "fish-seafood": "meat",
    "fridge-deli": "dairy",
    "bakery": "bakery",
    "frozen": "frozen",
    "pantry": "pantry",
    "drinks": "drinks",
    "beer-wine": "alcohol",
    "health-body": "health",
    "household": "household",
    "baby-child": "baby",
    "pet": "pet",
}

DEFAULT_BRANCH_NAME = "Woolworths Ponsonby"
WOOLWORTHS_CHAIN_SLUG = "woolworths"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

EXTRA_HEADERS = {
    "Accept-Language": "en-NZ,en-GB;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Upgrade-Insecure-Requests": "1",
}

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

REQUEST_TIMEOUT_MS = 30_000
DELAY_MIN = 2.0
DELAY_MAX = 5.0

# Session staleness — Woolworths' cw-lrkswrdjp / _abck cookies expire after ~2h
DEFAULT_MAX_SESSION_AGE_MIN = 90

# Block-detection: if a category page returns the Cloudflare/Akamai challenge,
# the title contains "Just a moment..." or the body contains "Access Denied".
BLOCK_TITLE_PATTERNS = ("Just a moment", "Access Denied", "Attention Required")
BOOTSTRAP_SCRIPT = "bootstrap_woolworths_sessions.py"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("woolworths_claude")

_log_dir = Path(__file__).parent / "logs"
_log_dir.mkdir(exist_ok=True)

def _setup_file_logging() -> None:
    from datetime import date
    log_file = _log_dir / f"woolworths_{date.today()}.log"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    logging.getLogger().addHandler(fh)
    logger.info(f"logging to {log_file}")

# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class ScrapedProduct:
    raw_name: str
    clean_name: str
    price: float
    category: str
    special_price: Optional[float] = None
    image_url: Optional[str] = None
    brand: Optional[str] = None
    weight: Optional[str] = None       # "100g", "1.5L", "2 x 50g" etc — quantity/size string
    barcode: Optional[str] = None
    in_stock: bool = True
    sku: Optional[str] = None          # Woolworths internal SKU
    scraped_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


_WEIGHT_PARSE_RE = re.compile(r"^(\d+\.?\d*)\s*(ml|l|kg|g|oz|lb|pk)$", re.I)


def parse_weight_fields(weight_str: Optional[str]) -> tuple[Optional[float], Optional[str]]:
    """Split a simple weight like '100g' / '1.5kg' into (value, unit). Returns (None, None) on complex strings."""
    if not weight_str:
        return None, None
    m = _WEIGHT_PARSE_RE.match(weight_str.strip())
    if m:
        return float(m.group(1)), m.group(2).lower()
    return None, None


def category_from_url(url: str) -> str:
    for slug, cat in URL_TO_CATEGORY.items():
        if f"/{slug}" in url:
            return cat
    return "other"


def _parse_proxy(url: str) -> dict:
    """Parse a proxy URL like 'http://user:pass@host:port' into Playwright's proxy dict."""
    from urllib.parse import urlparse
    p = urlparse(url)
    if not p.scheme or not p.hostname:
        raise ValueError(f"bad proxy URL: {url!r} (need scheme://host:port)")
    out: dict = {"server": f"{p.scheme}://{p.hostname}:{p.port or 8080}"}
    if p.username:
        out["username"] = p.username
    if p.password:
        out["password"] = p.password
    return out


class ProxyPool:
    """Thread/coroutine-safe round-robin pool of proxy URLs.

    Reads `proxies.txt` (one URL per line, comments allowed). Each call to
    `next()` returns the next proxy in rotation. Used by --all-branches with
    --proxy-file so each branch scraper gets a different IP.
    """

    def __init__(self, file_path: Optional[Path] = None) -> None:
        self._proxies: list[str] = []
        self._lock = asyncio.Lock()
        self._idx = 0
        if file_path:
            for line in file_path.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # Bare host:port → prepend http://
                if "://" not in line:
                    line = f"http://{line}"
                self._proxies.append(line)

    def __len__(self) -> int:
        return len(self._proxies)

    async def next(self) -> Optional[str]:
        if not self._proxies:
            return None
        async with self._lock:
            p = self._proxies[self._idx % len(self._proxies)]
            self._idx += 1
            return p


def _is_block_signal(title: str | None, body: str | None) -> bool:
    """Return True if the page text/title looks like a Cloudflare/Akamai challenge."""
    t = (title or "")
    b = (body or "")[:500]
    return any(p in t or p in b for p in BLOCK_TITLE_PATTERNS)


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class WoolworthsClaudeScraper:
    def __init__(
        self,
        branch_name: str = DEFAULT_BRANCH_NAME,
        branch_id: Optional[str] = None,
        category_urls: Optional[list[str]] = None,
        headless: bool = True,
        dry_run: bool = False,
        proxy_url: Optional[str] = None,
        auto_bootstrap: bool = True,
        max_session_age_min: int = DEFAULT_MAX_SESSION_AGE_MIN,
    ) -> None:
        if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
            raise RuntimeError(
                f"Missing SUPABASE_URL / SUPABASE_SERVICE_KEY in {ENV_PATH}"
            )
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        self.branch_name = branch_name
        self.branch_id = branch_id  # filled in by _resolve_branch if not provided
        self.chain_id: Optional[str] = None
        self.category_urls = category_urls or CATEGORY_URLS
        self.headless = headless
        self.dry_run = dry_run
        self.proxy_url = proxy_url
        self.auto_bootstrap = auto_bootstrap
        self.max_session_age_min = max_session_age_min
        self.fast_categories: bool = False
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._api_headers: dict[str, str] = {}
        self._api_paginated_url: str = ""
        self._fast_template_url: Optional[str] = None
        self._fast_headers: dict[str, str] = {}

    # ---- Browser setup ---------------------------------------------------

    async def _start_browser(self) -> None:
        self._playwright = await async_playwright().start()
        launch_kwargs: dict = {
            "headless": self.headless,
            "args": [
                "--disable-http2",
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        }
        if self.proxy_url:
            launch_kwargs["proxy"] = _parse_proxy(self.proxy_url)
            logger.info(f"using proxy: {launch_kwargs['proxy'].get('server')}")
        self._browser = await self._playwright.chromium.launch(**launch_kwargs)

    async def _new_context(self, storage_state: Optional[str] = None) -> None:
        assert self._browser
        kwargs: dict = dict(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="en-NZ",
            timezone_id="Pacific/Auckland",
            java_script_enabled=True,
            accept_downloads=False,
            extra_http_headers=EXTRA_HEADERS,
        )
        if storage_state:
            kwargs["storage_state"] = storage_state
        self._context = await self._browser.new_context(**kwargs)
        await self._context.add_init_script(STEALTH_SCRIPT)
        self._page = await self._context.new_page()
        self._page.set_default_timeout(REQUEST_TIMEOUT_MS)

    async def _refresh_context(self) -> None:
        """Drop the current context+page and open a fresh one — used after API bursts
        so the next page.goto() sees a clean session against Cloudflare."""
        session_path = self._session_path_for_branch()
        if self._page:
            try:
                await self._page.unroute_all(behavior="ignoreErrors")
            except Exception:
                pass
            try:
                await self._page.close()
            except Exception:
                pass
        if self._context:
            try:
                await self._context.close()
            except Exception:
                pass
        await self._new_context(storage_state=str(session_path) if session_path else None)

    async def _close_browser(self) -> None:
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    # ---- Branch / session resolution ------------------------------------

    def _session_path_for_branch(self) -> Optional[Path]:
        if not self.branch_id:
            return None
        p = SESSIONS_DIR / f"{self.branch_id}.json"
        return p if p.exists() else None

    def _session_age_min(self) -> Optional[float]:
        """Age of the saved session JSON in minutes, or None if missing."""
        sp = self._session_path_for_branch()
        if not sp:
            return None
        import time as _t
        return (_t.time() - sp.stat().st_mtime) / 60.0

    def _bootstrap_session_sync(self) -> bool:
        """Shell out to bootstrap_woolworths_sessions.py to refresh THIS branch's session.

        Sync because the bootstrap script is itself an asyncio app — running it
        in a subprocess avoids nested event-loop pain. Takes ~20-40s per branch.
        Returns True on success.
        """
        import subprocess
        if not self.branch_id:
            return False
        script_path = SCRAPERS_DIR / BOOTSTRAP_SCRIPT
        if not script_path.exists():
            logger.warning(f"  [bootstrap] {script_path} not found, skipping refresh")
            return False
        cmd = ["python3", str(script_path), "--branch-id", self.branch_id]
        logger.info(f"  [bootstrap] refreshing session for branch {self.branch_id}...")
        try:
            r = subprocess.run(
                cmd, cwd=str(SCRAPERS_DIR),
                capture_output=True, text=True, timeout=120,
            )
            if r.returncode == 0:
                logger.info(f"  [bootstrap] OK in {r.stdout.count(chr(10))} log lines")
                return True
            logger.warning(f"  [bootstrap] failed (rc={r.returncode}): {r.stderr[:200]}")
            return False
        except subprocess.TimeoutExpired:
            logger.warning(f"  [bootstrap] timeout after 120s")
            return False
        except Exception as e:
            logger.warning(f"  [bootstrap] exception: {e}")
            return False

    async def _ensure_fresh_session(self) -> bool:
        """If auto_bootstrap is on AND the session is older than threshold,
        refresh it before scraping. Returns True if a fresh session is available.

        IMPORTANT: when a proxy is in use, we DO NOT bootstrap from this machine —
        because bootstrapping from home IP creates session cookies tied to the
        home IP's Akamai trust profile, and using them from a different proxy IP
        within a short window triggers a silent challenge (Akamai's anti-session-
        hijacking heuristic). Old saved sessions survive the IP switch better than
        brand-new ones.
        """
        if not self.auto_bootstrap or not self.branch_id:
            return self._session_path_for_branch() is not None
        if self.proxy_url:
            # Skip refresh when proxying — trust existing session.
            age = self._session_age_min()
            if age is None:
                logger.warning(
                    f"  [session] no saved session for branch {self.branch_id} AND proxy in use — "
                    f"won't bootstrap (would create IP-mismatched session). Run "
                    f"bootstrap_woolworths_sessions.py without proxy first."
                )
                return False
            logger.info(f"  [session] {age:.1f} min old, proxy in use — skipping refresh (IP mismatch risk)")
            return True
        age = self._session_age_min()
        if age is None:
            logger.info(f"  [session] no saved session — bootstrapping branch {self.branch_id}")
            return await asyncio.get_event_loop().run_in_executor(None, self._bootstrap_session_sync)
        if age > self.max_session_age_min:
            logger.info(f"  [session] {age:.1f} min old > {self.max_session_age_min} — refreshing")
            return await asyncio.get_event_loop().run_in_executor(None, self._bootstrap_session_sync)
        logger.info(f"  [session] {age:.1f} min old — fresh enough")
        return True

    def _resolve_branch(self) -> None:
        """Look up chain_id and branch_id from store_chains / store_branches."""
        chain = (
            self.supabase.table("store_chains")
            .select("id")
            .eq("slug", WOOLWORTHS_CHAIN_SLUG)
            .execute()
            .data
        )
        if not chain:
            # First-ever scrape — create the chain row.
            chain = (
                self.supabase.table("store_chains")
                .upsert({"slug": WOOLWORTHS_CHAIN_SLUG, "name": "Woolworths"}, on_conflict="slug")
                .execute()
                .data
            )
        self.chain_id = chain[0]["id"]

        if self.branch_id:
            r = (
                self.supabase.table("store_branches")
                .select("id,name")
                .eq("id", self.branch_id)
                .execute()
                .data
            )
            if r:
                self.branch_name = r[0]["name"]
            return

        r = (
            self.supabase.table("store_branches")
            .select("id,name")
            .eq("chain_id", self.chain_id)
            .eq("name", self.branch_name)
            .execute()
            .data
        )
        if r:
            self.branch_id = r[0]["id"]
            return

        # Branch row missing — create it. (Sessions key on the new branch_id, so a
        # session may not yet exist; scrape will fall back to no-session pricing.)
        r = (
            self.supabase.table("store_branches")
            .upsert(
                {"chain_id": self.chain_id, "name": self.branch_name},
                on_conflict="chain_id,name",
            )
            .execute()
            .data
        )
        self.branch_id = r[0]["id"]

    # ---- Network capture -------------------------------------------------

    def _make_intercept_handler(self, captured: list[dict]):
        async def intercept(route, request):
            # Capture auth headers + canonical URL once.
            if not self._api_headers:
                self._api_headers = dict(request.headers)
            if not self._api_paginated_url:
                self._api_paginated_url = request.url
            response = await route.fetch()
            try:
                body = await response.json()
                captured.append({"url": request.url, "data": body})
            except Exception:
                pass
            await route.fulfill(response=response)

        return intercept

    async def _scroll_to_bottom(self) -> None:
        assert self._page
        prev = 0
        for _ in range(20):
            h = await self._page.evaluate("document.body.scrollHeight")
            if h == prev:
                break
            await self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await self._page.wait_for_timeout(800)
            prev = h

    async def _random_delay(self) -> None:
        await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # ---- Scrape ----------------------------------------------------------

    async def scrape_one_category(self, url: str) -> tuple[list[ScrapedProduct], bool, int]:
        """Scrape one category. Returns (products, did_paginate, num_responses_captured).

        did_paginate is True iff we issued direct-API pagination calls — caller can
        use this to decide whether to refresh the browser context (Cloudflare-friendly).
        num_responses_captured = how many /api/v1/products responses the route handler
        intercepted; 0 typically means Akamai blocked the page from making the API call.
        """
        assert self._page
        captured: list[dict] = []
        self._api_headers = {}
        self._api_paginated_url = ""
        did_paginate = False

        intercept = self._make_intercept_handler(captured)
        await self._page.route("**/api/v1/products**", intercept)

        category = category_from_url(url)
        logger.info(f"  → {url}  [{category}]")

        try:
            await self._random_delay()
            await self._page.goto(url, wait_until="load", timeout=60_000)
            await self._page.wait_for_timeout(3000)
            await self._scroll_to_bottom()
            await self._page.wait_for_timeout(800)

            # Determine total pages from the first products response.
            total_pages = 1
            page_size = 48
            total_items = 0
            for entry in captured:
                data = entry.get("data")
                if isinstance(data, dict):
                    products_node = data.get("products")
                    if isinstance(products_node, dict):
                        total_items = products_node.get("totalItems") or 0
                        page_size = data.get("currentPageSize") or 48
                        if total_items:
                            total_pages = math.ceil(total_items / page_size)
                            break

            if total_items:
                logger.info(f"    pagination: {total_items} items / page_size={page_size} → {total_pages} pages")

            # Save API template for --fast-categories on first successful browser capture.
            if not self._fast_template_url and self._api_paginated_url and self._api_headers:
                self._fast_template_url = self._api_paginated_url
                self._fast_headers = {
                    k: v for k, v in self._api_headers.items()
                    if k.lower() not in ("host", "content-length")
                }
                logger.info(f"  [fast] template captured: {self._fast_template_url[:120]}")

            # Direct-API pagination using captured headers.
            if total_pages > 1 and self._api_paginated_url and self._api_headers:
                did_paginate = True
                headers = {
                    k: v for k, v in self._api_headers.items()
                    if k.lower() not in ("host", "content-length")
                }
                for page_num in range(2, total_pages + 1):
                    page_url = re.sub(r"\bpage=\d+\b", f"page={page_num}", self._api_paginated_url)
                    if "page=" not in self._api_paginated_url:
                        sep = "&" if "?" in page_url else "?"
                        page_url = f"{page_url}{sep}page={page_num}"
                    try:
                        resp = await self._page.request.get(page_url, headers=headers)
                        if resp.ok:
                            captured.append({"url": page_url, "data": await resp.json()})
                        elif resp.status == 500:
                            # one retry — Cloudflare sometimes coughs on burst
                            await asyncio.sleep(3)
                            resp2 = await self._page.request.get(page_url, headers=headers)
                            if resp2.ok:
                                captured.append({"url": page_url, "data": await resp2.json()})
                            else:
                                logger.warning(f"    page {page_num}: HTTP {resp2.status} after retry — stopping")
                                break
                        else:
                            logger.warning(f"    page {page_num}: HTTP {resp.status} — stopping")
                            break
                    except Exception as e:
                        logger.warning(f"    page {page_num}: {e} — stopping")
                        break
                    await asyncio.sleep(0.4)
        finally:
            try:
                await self._page.unroute_all(behavior="ignoreErrors")
            except Exception:
                pass

        # Parse all captured responses.
        products: list[ScrapedProduct] = []
        for entry in captured:
            data = entry.get("data")
            if not isinstance(data, dict):
                continue
            products_node = data.get("products")
            if isinstance(products_node, dict):
                items = products_node.get("items") or []
            elif isinstance(products_node, list):
                items = products_node
            else:
                items = data.get("items") or []
            for item in items:
                p = self._parse_item(item, category)
                if p:
                    products.append(p)

        logger.info(f"    parsed {len(products)} products from {len(captured)} responses")
        return products, did_paginate, len(captured)

    async def _scrape_category_direct(self, url: str) -> Optional[list[ScrapedProduct]]:
        """Skip browser nav — substitute slug in captured API template and paginate directly.

        Returns None on any failure so the caller falls back to the browser path.
        """
        if not self._fast_template_url or not self._fast_headers:
            return None
        slug = url.rstrip("/").split("/")[-1]
        # Reset to page 1 and substitute the category slug in the dasFilter param.
        base = re.sub(r"\bpage=\d+\b", "page=1", self._fast_template_url)
        direct_url = re.sub(
            r"dasFilter=Department%3[Bb]%3[Bb][^%&]+%3[Bb]false",
            f"dasFilter=Department%3B%3B{quote(slug)}%3Bfalse",
            base,
            flags=re.IGNORECASE,
        )
        if direct_url == base:
            logger.warning(f"  [fast] could not substitute '{slug}' in template — browser fallback")
            return None

        category = category_from_url(url)
        try:
            resp = await self._page.request.get(direct_url, headers=self._fast_headers)
            if not resp.ok:
                logger.warning(f"  [fast] HTTP {resp.status} for {slug} — browser fallback")
                return None
            data = await resp.json()
        except Exception as e:
            logger.warning(f"  [fast] {e} — browser fallback")
            return None

        products_node = data.get("products") or {}
        total_items = 0
        page_size = 48
        if isinstance(products_node, dict):
            total_items = products_node.get("totalItems") or 0
            page_size = data.get("currentPageSize") or 48
        total_pages = math.ceil(total_items / page_size) if total_items else 1

        all_data = [data]
        if total_pages > 1:
            _sem = asyncio.Semaphore(3)

            async def _fetch_page(page_num: int) -> tuple[int, Optional[dict]]:
                page_url = re.sub(r"\bpage=\d+\b", f"page={page_num}", direct_url)
                async with _sem:
                    try:
                        r = await self._page.request.get(page_url, headers=self._fast_headers)
                        if r.ok:
                            return page_num, await r.json()
                        if r.status == 500:
                            await asyncio.sleep(3)
                            r2 = await self._page.request.get(page_url, headers=self._fast_headers)
                            if r2.ok:
                                return page_num, await r2.json()
                            logger.warning(f"  [fast] page {page_num}: HTTP {r2.status} after retry")
                        else:
                            logger.warning(f"  [fast] page {page_num}: HTTP {r.status}")
                    except Exception as e:
                        logger.warning(f"  [fast] page {page_num}: {e}")
                return page_num, None

            page_results = await asyncio.gather(
                *[_fetch_page(p) for p in range(2, total_pages + 1)]
            )
            for _, page_data in sorted(page_results, key=lambda x: x[0]):
                if page_data is not None:
                    all_data.append(page_data)

        products: list[ScrapedProduct] = []
        for d in all_data:
            pn = d.get("products")
            if isinstance(pn, dict):
                items = pn.get("items") or []
            elif isinstance(pn, list):
                items = pn
            else:
                items = d.get("items") or []
            for item in items:
                p = self._parse_item(item, category)
                if p:
                    products.append(p)

        if not products:
            logger.warning(f"  [fast] {url} → 0 products — browser fallback")
            return None

        pages_label = f"{total_pages}p" if total_pages > 1 else ""
        logger.info(f"  [fast] {url}  {len(products)} products {pages_label}".rstrip())
        return products

    def _parse_item(self, item: dict, category: str) -> Optional[ScrapedProduct]:
        if item.get("type") != "Product":
            return None
        raw_name = item.get("name")
        if not raw_name:
            return None

        brand_raw = item.get("brand") or None
        brand = brand_raw.title() if brand_raw else None

        clean = raw_name
        if brand_raw and clean.lower().startswith(brand_raw.lower()):
            clean = clean[len(brand_raw):].strip()
        clean_name = (clean or raw_name).title()

        price_data = item.get("price") or {}
        price = price_data.get("originalPrice")
        if not price or float(price) <= 0:
            return None
        try:
            price_f = float(price)
        except (TypeError, ValueError):
            return None

        sale = price_data.get("salePrice")
        try:
            special = float(sale) if sale and float(sale) < price_f else None
        except (TypeError, ValueError):
            special = None

        size = item.get("size") or {}
        weight = size.get("volumeSize") or None

        # Stock — Woolworths exposes a few possible signals; treat anything explicitly OOS as False.
        availability = (
            item.get("availabilityStatus")
            or (item.get("stockLevel") if isinstance(item.get("stockLevel"), str) else None)
            or "In Stock"
        )
        in_stock = "out" not in str(availability).lower()
        # Some payloads include a numeric stockLevel where 0 means OOS.
        sl = item.get("stockLevel")
        if isinstance(sl, (int, float)) and sl == 0:
            in_stock = False

        # Embed weight in raw_name so name-fallback matching can disambiguate sizes.
        titled = raw_name.title()
        raw_name_full = f"{titled} {weight}" if weight else titled

        return ScrapedProduct(
            raw_name=raw_name_full,
            clean_name=clean_name,
            price=price_f,
            category=category,
            special_price=special,
            image_url=(item.get("images") or {}).get("big"),
            brand=brand,
            weight=weight,
            barcode=item.get("barcode") or None,
            in_stock=in_stock,
            sku=str(item.get("sku") or item.get("productId") or "") or None,
        )

    # ---- Run loop --------------------------------------------------------

    async def run(self) -> dict:
        self._resolve_branch()
        logger.info(f"chain_id={self.chain_id}  branch_id={self.branch_id}  branch={self.branch_name}")

        # Auto-bootstrap if enabled — refreshes saved session before scraping.
        await self._ensure_fresh_session()

        run_id = self._start_run()
        stats = {"records_updated": 0, "records_failed": 0, "new_products": 0,
                 "price_changes": 0, "blocks_detected": 0, "retries": 0}

        await self._start_browser()
        session_path = self._session_path_for_branch()
        if session_path:
            logger.info(f"using saved session: {session_path.name}")
            await self._new_context(storage_state=str(session_path))
        else:
            logger.warning(
                f"no saved session for branch {self.branch_id} — prices may default to a generic store"
            )
            await self._new_context()

        all_products: list[ScrapedProduct] = []
        try:
            for url in self.category_urls:
                did_paginate = False
                products: list[ScrapedProduct] = []
                num_responses = 0
                used_fast = False

                # Fast path: skip browser nav if template is captured and flag is set.
                if self.fast_categories and self._fast_template_url:
                    try:
                        fast_prods = await self._scrape_category_direct(url)
                        if fast_prods is not None:
                            products = fast_prods
                            used_fast = True
                    except Exception as e:
                        logger.warning(f"  [fast] unexpected error: {e} — browser fallback")

                if not used_fast:
                    try:
                        products, did_paginate, num_responses = await self.scrape_one_category(url)
                    except Exception as e:
                        logger.warning(f"category failed: {url}: {e}")

                    # Block detection: 3 signals, any one triggers recovery
                    #   1. Visible challenge page (Akamai/Cloudflare HTML)  — `_is_block_signal`
                    #   2. Silent challenge: 0 products AND 0 captured XHRs — page loaded but JS suppressed
                    #   3. Empty result with no responses = same as #2
                    # Recovery depends on whether we're proxying:
                    #   - No proxy: re-bootstrap from home IP (fresh session) and retry
                    #   - Proxy:    refresh context + slight delay + retry once (proxy stays the same;
                    #               new TLS handshake + jitter usually carries enough trust to pass)
                    silent_block = (not products) and (num_responses == 0)
                    visible_block = False
                    if (not products) and session_path:
                        try:
                            title = await self._page.title()
                            body = await self._page.evaluate(
                                "document.body ? document.body.innerText.substring(0, 500) : ''"
                            )
                            visible_block = _is_block_signal(title, body)
                        except Exception:
                            pass

                    if (silent_block or visible_block) and self.auto_bootstrap and session_path:
                        stats["blocks_detected"] += 1
                        label = "visible challenge" if visible_block else "silent challenge (0 XHRs captured)"
                        if self.proxy_url:
                            logger.warning(f"  [block] {label} on {url} (proxy in use) — refresh+jitter+retry")
                            await self._refresh_context()
                            await asyncio.sleep(random.uniform(5.0, 12.0))
                        else:
                            logger.warning(f"  [block] {label} on {url} — re-bootstrap+retry")
                            ok = await asyncio.get_event_loop().run_in_executor(
                                None, self._bootstrap_session_sync
                            )
                            if ok:
                                await self._refresh_context()
                        try:
                            products, did_paginate, num_responses = await self.scrape_one_category(url)
                            stats["retries"] += 1
                            if products:
                                logger.info(f"  [block] retry succeeded ({len(products)} products)")
                            else:
                                logger.warning(f"  [block] retry STILL empty ({num_responses} XHRs) — moving on")
                        except Exception as e:
                            logger.warning(f"  retry failed: {e}")

                    if did_paginate:
                        await self._refresh_context()
                    await self._random_delay()

                all_products.extend(products)
            logger.info(f"TOTAL scraped: {len(all_products)} products")
            self._update_run(run_id, total_scraped=len(all_products))

            if not self.dry_run:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._save_to_supabase, all_products, stats)
            else:
                logger.info("DRY RUN — skipping Supabase writes")

            status = (
                "failed" if (stats["records_failed"] and not stats["records_updated"])
                else "partial" if stats["records_failed"]
                else "success"
            )
            self._end_run(run_id, status, stats)
        except Exception as e:
            logger.exception("run failed")
            self._end_run(run_id, "failed", stats, error=str(e))
            raise
        finally:
            await self._close_browser()

        return stats

    # ---- Supabase writes -------------------------------------------------

    def _start_run(self) -> Optional[str]:
        if self.dry_run:
            return None
        try:
            r = (
                self.supabase.table("scraper_runs")
                .insert({
                    "chain_id": self.chain_id,
                    "branch_id": self.branch_id,
                    "status": "running",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                })
                .execute()
            )
            return r.data[0]["id"]
        except Exception as e:
            logger.warning(f"could not start scraper_runs row: {e}")
            return None

    def _update_run(self, run_id: Optional[str], **fields) -> None:
        if not run_id:
            return
        try:
            self.supabase.table("scraper_runs").update(fields).eq("id", run_id).execute()
        except Exception as e:
            logger.warning(f"could not update scraper_runs: {e}")

    def _end_run(self, run_id: Optional[str], status: str, stats: dict, error: Optional[str] = None) -> None:
        if not run_id:
            return
        payload = {
            "status": status,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "records_updated": stats["records_updated"],
            "records_failed": stats["records_failed"],
            "new_products": stats["new_products"],
            "price_changes": stats["price_changes"],
        }
        if error:
            payload["error_log"] = error[:2000]
        try:
            self.supabase.table("scraper_runs").update(payload).eq("id", run_id).execute()
        except Exception as e:
            logger.warning(f"could not finalise scraper_runs: {e}")
        if status in ("success", "partial") and self.branch_id:
            try:
                self.supabase.table("store_branches").update(
                    {"last_scraped_at": datetime.now(timezone.utc).isoformat()}
                ).eq("id", self.branch_id).execute()
            except Exception:
                pass

    def _save_to_supabase(self, products: list[ScrapedProduct], stats: dict) -> None:
        """Barcode-first upsert with name fallback. Idempotent."""
        if not products:
            return
        CHUNK = 200

        # Deduplicate scraped list — same SKU can appear in promo + grid slots.
        seen_keys: set[str] = set()
        deduped: list[ScrapedProduct] = []
        for p in products:
            key = p.barcode or f"name:{p.raw_name}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(p)

        with_barcode = [p for p in deduped if p.barcode]
        without_barcode = [p for p in deduped if not p.barcode]
        logger.info(
            f"saving: {len(deduped)} unique  ({len(with_barcode)} barcoded, "
            f"{len(without_barcode)} need name match)"
        )

        # ---------- Phase 1: barcode upserts ----------
        # Snapshot existing barcodes BEFORE upsert so we can count truly-new products.
        known_barcodes: set[str] = set()
        if with_barcode:
            all_codes = [p.barcode for p in with_barcode]
            for i in range(0, len(all_codes), 500):
                chunk = all_codes[i : i + 500]
                try:
                    r = self.supabase.table("products").select("barcode").in_("barcode", chunk).execute()
                    for row in r.data:
                        if row.get("barcode"):
                            known_barcodes.add(row["barcode"])
                except Exception as e:
                    logger.warning(f"snapshot barcodes chunk failed: {e}")

        barcode_to_product: dict[str, dict] = {}
        if with_barcode:
            rows: dict[str, dict] = {}
            for p in with_barcode:
                wv, wu = parse_weight_fields(p.weight)
                row = {"name": p.raw_name, "barcode": p.barcode, "source": "scraped"}
                if p.brand:
                    row["brand"] = p.brand
                if p.image_url:
                    row["image_url"] = p.image_url
                if wv is not None:
                    row["weight_value"] = wv
                if wu is not None:
                    row["weight_unit"] = wu
                rows[p.barcode] = row

            payload = list(rows.values())
            for i in range(0, len(payload), CHUNK):
                chunk = payload[i : i + CHUNK]
                try:
                    self.supabase.table("products").upsert(
                        chunk, on_conflict="barcode", ignore_duplicates=True
                    ).execute()
                except Exception as e:
                    # Likely a name UNIQUE conflict — fall back per-row to resolve.
                    logger.warning(
                        f"products upsert chunk {i // CHUNK + 1} failed ({str(e)[:100]}) — "
                        f"falling back to per-row resolve"
                    )
                    self._resolve_chunk_by_name(chunk, barcode_to_product)

            # Fetch back by barcode.
            all_codes = list(rows.keys())
            for i in range(0, len(all_codes), 500):
                chunk = all_codes[i : i + 500]
                try:
                    r = (
                        self.supabase.table("products")
                        .select("id,name,brand,barcode")
                        .in_("barcode", chunk)
                        .execute()
                    )
                    for row in r.data:
                        barcode_to_product[row["barcode"]] = row
                except Exception as e:
                    logger.warning(f"products barcode fetch chunk {i // 500 + 1}: {e}")

        # ---------- Phase 2: resolve to product_ids ----------
        matched: list[tuple[str, ScrapedProduct]] = []
        new_products = 0
        for p in deduped:
            if p.barcode and p.barcode in barcode_to_product:
                if p.barcode not in known_barcodes:
                    new_products += 1
                matched.append((barcode_to_product[p.barcode]["id"], p))
            else:
                # Name-fallback path (no barcode, or barcode upsert didn't return a row).
                pid, was_new = self._resolve_by_name(p)
                if pid:
                    if was_new:
                        new_products += 1
                    matched.append((pid, p))
                else:
                    stats["records_failed"] += 1
        stats["new_products"] = new_products
        logger.info(f"matched {len(matched)} products  (new: {new_products})")

        # ---------- Phase 3: existing store_products snapshot for price-change detection ----------
        existing_map: dict[str, dict] = {}
        page_size = 1000
        offset = 0
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
        except Exception as e:
            logger.warning(f"could not paginate existing store_products: {e}")

        # ---------- Phase 4: build store_products + price_history rows ----------
        sp_rows: list[dict] = []
        ph_rows: list[dict] = []
        for product_id, p in matched:
            effective = p.special_price if p.special_price else p.price
            existing = existing_map.get(product_id)
            if existing and existing.get("current_price") is not None:
                old_price = float(existing["current_price"])
                if abs(old_price - effective) > 0.001:
                    ph_rows.append({
                        "store_product_id": existing["id"],
                        "old_price": old_price,
                        "new_price": effective,
                        "old_unit_price": existing.get("unit_price"),
                        "new_unit_price": None,
                    })
                    stats["price_changes"] += 1
            sp_rows.append({
                "product_id": product_id,
                "store_id": self.branch_id,
                "sku": p.barcode,
                "current_price": effective,
                "unit_price": None,
                "unit_label": None,
                "in_stock": p.in_stock,
                "scraped_at": p.scraped_at.isoformat(),
            })

        sp_rows = list({r["product_id"]: r for r in sp_rows}.values())
        ph_rows = list({r["store_product_id"]: r for r in ph_rows}.values())

        # ---------- Phase 5: upsert store_products in chunks ----------
        total_upserted = 0
        for i in range(0, len(sp_rows), CHUNK):
            chunk = sp_rows[i : i + CHUNK]
            try:
                r = self.supabase.table("store_products").upsert(
                    chunk, on_conflict="product_id,store_id"
                ).execute()
                total_upserted += len(r.data)
            except Exception as e:
                logger.error(f"store_products upsert chunk {i // CHUNK + 1}: {e}")
                stats["records_failed"] += len(chunk)
        stats["records_updated"] = total_upserted
        logger.info(f"upserted {total_upserted} store_products rows")

        # ---------- Phase 6: insert price_history ----------
        if ph_rows:
            for i in range(0, len(ph_rows), CHUNK):
                chunk = ph_rows[i : i + CHUNK]
                try:
                    self.supabase.table("price_history").insert(chunk).execute()
                except Exception as e:
                    logger.warning(f"price_history insert chunk {i // CHUNK + 1}: {e}")
            logger.info(f"recorded {len(ph_rows)} price changes")

    def _resolve_chunk_by_name(self, chunk: list[dict], barcode_to_product: dict[str, dict]) -> None:
        """Per-row fallback when a barcode-upsert chunk hits a name UNIQUE conflict."""
        names = [row["name"] for row in chunk]
        try:
            r = (
                self.supabase.table("products")
                .select("id,name,brand,barcode")
                .in_("name", names)
                .execute()
            )
            name_map = {row["name"]: row for row in r.data}
        except Exception as e:
            logger.warning(f"name fetch failed: {e}")
            name_map = {}

        new_rows: list[dict] = []
        for row in chunk:
            existing = name_map.get(row["name"])
            if existing:
                barcode_to_product[row["barcode"]] = existing
                if not existing.get("barcode"):
                    try:
                        self.supabase.table("products").update(
                            {"barcode": row["barcode"]}
                        ).eq("id", existing["id"]).execute()
                        existing["barcode"] = row["barcode"]
                    except Exception:
                        pass
            else:
                new_rows.append(row)
        if new_rows:
            try:
                ins = self.supabase.table("products").upsert(
                    new_rows, on_conflict="barcode", ignore_duplicates=False
                ).execute()
                for r in (ins.data or []):
                    if r.get("barcode"):
                        barcode_to_product[r["barcode"]] = r
            except Exception as e:
                logger.warning(f"insert new chunk rows failed: {e}")

    def _resolve_by_name(self, p: ScrapedProduct) -> tuple[Optional[str], bool]:
        """Look up a product by exact raw_name; insert if missing. Returns (id, was_new)."""
        try:
            r = (
                self.supabase.table("products")
                .select("id,barcode")
                .eq("name", p.raw_name)
                .limit(1)
                .execute()
            )
            if r.data:
                row = r.data[0]
                # Backfill barcode if we have one and the row didn't.
                if p.barcode and not row.get("barcode"):
                    try:
                        self.supabase.table("products").update(
                            {"barcode": p.barcode}
                        ).eq("id", row["id"]).execute()
                    except Exception:
                        pass
                return row["id"], False
        except Exception as e:
            logger.warning(f"name lookup failed for '{p.raw_name}': {e}")
            return None, False

        wv, wu = parse_weight_fields(p.weight)
        row: dict = {"name": p.raw_name, "source": "scraped"}
        if p.barcode:
            row["barcode"] = p.barcode
        if p.brand:
            row["brand"] = p.brand
        if p.image_url:
            row["image_url"] = p.image_url
        if wv is not None:
            row["weight_value"] = wv
        if wu is not None:
            row["weight_unit"] = wu
        try:
            ins = self.supabase.table("products").insert(row).execute()
            return ins.data[0]["id"], True
        except Exception as e:
            logger.warning(f"insert by name failed for '{p.raw_name}': {e}")
            return None, False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Woolworths NZ scraper (Claude build)")
    ap.add_argument("--branch", default=DEFAULT_BRANCH_NAME, help="Branch name in store_branches.name")
    ap.add_argument("--branch-id", default=None, help="Branch UUID (overrides --branch)")
    ap.add_argument("--all-branches", action="store_true",
                    help="Loop every Woolworths branch with a saved session")
    ap.add_argument("--categories", default=None,
                    help="Comma-separated list of slugs (e.g. fruit-veg,bakery). Default = all categories.")
    ap.add_argument("--test", action="store_true",
                    help="Run only 3 categories: fruit-veg, bakery, drinks. Used for sanity checks.")
    ap.add_argument("--no-headless", action="store_true",
                    help="Show the browser window (default = headless).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Scrape but do not write to Supabase.")
    ap.add_argument("--proxy", default=None,
                    help="Proxy URL (e.g. http://USER:PASS@gateway.iproyal.com:12321). "
                         "All browser traffic and direct API calls route through it.")
    ap.add_argument("--proxy-file", default=None,
                    help="Path to a text file with one proxy URL per line. "
                         "Proxies are round-robin'd across branches. Mutually exclusive with --proxy.")
    ap.add_argument("--concurrency", type=int, default=1,
                    help="Number of branches to scrape in parallel (default 1)")
    ap.add_argument("--max-session-age", type=int, default=DEFAULT_MAX_SESSION_AGE_MIN,
                    help=f"Refresh saved session if older than N minutes (default {DEFAULT_MAX_SESSION_AGE_MIN})")
    ap.add_argument("--no-auto-bootstrap", action="store_true",
                    help="Disable automatic session refresh on stale / block detection")
    ap.add_argument("--skip-recently-done", type=float, default=0.0,
                    help="If >0, query scraper_runs and skip branches with status=success "
                         "in the last N hours. Use to resume after a partial run.")
    ap.add_argument("--fast-categories", action="store_true",
                    help="After the first category captures the API template, skip browser "
                         "navigation for all remaining categories and call the API directly. "
                         "~4-5x faster per branch.")
    return ap.parse_args()


def categories_for(args: argparse.Namespace) -> list[str]:
    if args.test:
        slugs = ["fruit-veg", "bakery", "drinks"]
    elif args.categories:
        slugs = [s.strip() for s in args.categories.split(",") if s.strip()]
    else:
        return CATEGORY_URLS
    return [f"https://www.woolworths.co.nz/shop/browse/{s}" for s in slugs]


async def main_async() -> int:
    args = parse_args()
    _setup_file_logging()
    if args.proxy and args.proxy_file:
        logger.error("--proxy and --proxy-file are mutually exclusive")
        return 2
    headless = not args.no_headless
    cats = categories_for(args)
    auto_bootstrap = not args.no_auto_bootstrap

    proxy_pool: Optional[ProxyPool] = None
    if args.proxy_file:
        proxy_pool = ProxyPool(Path(args.proxy_file))
        if len(proxy_pool) == 0:
            logger.error(f"no proxies in {args.proxy_file}")
            return 2
        logger.info(f"loaded {len(proxy_pool)} proxies from {args.proxy_file}")

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    branches: list[dict] = []
    if args.all_branches:
        chain = sb.table("store_chains").select("id").eq("slug", WOOLWORTHS_CHAIN_SLUG).execute().data
        if not chain:
            logger.error("no Woolworths chain row")
            return 1
        all_branches = (
            sb.table("store_branches")
            .select("id,name").eq("chain_id", chain[0]["id"]).execute().data
        )
        session_uuids = {p.stem for p in SESSIONS_DIR.glob("*.json")}
        branches = [b for b in all_branches if b["id"] in session_uuids]
        logger.info(f"--all-branches: {len(branches)} branches with saved sessions")

        # Skip branches that already completed successfully in the last N hours
        if args.skip_recently_done > 0:
            from datetime import timedelta
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=args.skip_recently_done)).isoformat()
            try:
                done_rows = (
                    sb.table("scraper_runs")
                    .select("branch_id")
                    .eq("chain_id", chain[0]["id"])
                    .eq("status", "success")
                    .gte("started_at", cutoff)
                    .execute().data
                )
                done_ids = {r["branch_id"] for r in done_rows if r.get("branch_id")}
                pre = len(branches)
                branches = [b for b in branches if b["id"] not in done_ids]
                logger.info(
                    f"--skip-recently-done {args.skip_recently_done}h: filtered out "
                    f"{pre - len(branches)} branches (was {pre}, now {len(branches)})"
                )
            except Exception as e:
                logger.warning(f"skip-recently-done query failed: {e}")
    else:
        if args.branch_id:
            branches = [{"id": args.branch_id, "name": None}]
        else:
            branches = [{"id": None, "name": args.branch}]

    overall = {"branches": 0, "scraped": 0, "updated": 0, "new": 0, "failed": 0,
               "price_changes": 0, "blocks": 0, "retries": 0}
    t0 = time.time()

    sem = asyncio.Semaphore(max(1, args.concurrency))
    completed = 0
    total = len(branches)
    completed_lock = asyncio.Lock()

    async def run_one(b: dict) -> None:
        nonlocal completed
        async with sem:
            proxy = await proxy_pool.next() if proxy_pool else args.proxy
            scraper = WoolworthsClaudeScraper(
                branch_name=b.get("name") or DEFAULT_BRANCH_NAME,
                branch_id=b.get("id"),
                category_urls=cats,
                headless=headless,
                dry_run=args.dry_run,
                proxy_url=proxy,
                auto_bootstrap=auto_bootstrap,
                max_session_age_min=args.max_session_age,
            )
            scraper.fast_categories = args.fast_categories
            try:
                stats = await scraper.run()
                overall["branches"] += 1
                overall["updated"] += stats["records_updated"]
                overall["new"] += stats["new_products"]
                overall["failed"] += stats["records_failed"]
                overall["price_changes"] += stats["price_changes"]
                overall["blocks"] += stats.get("blocks_detected", 0)
                overall["retries"] += stats.get("retries", 0)
            except Exception as e:
                logger.error(f"branch {b.get('name') or b.get('id')} failed: {e}")
                overall["failed"] += 1
            async with completed_lock:
                completed += 1
                logger.info(f"=== progress: {completed}/{total} branches done ({(completed/total*100):.1f}%) ===")

    await asyncio.gather(*[run_one(b) for b in branches], return_exceptions=True)

    dt = time.time() - t0
    logger.info(
        f"DONE  branches={overall['branches']}/{total}  updated={overall['updated']}  "
        f"new={overall['new']}  changes={overall['price_changes']}  "
        f"failed={overall['failed']}  blocks={overall['blocks']}  retries={overall['retries']}  "
        f"elapsed={dt:.1f}s"
    )
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
