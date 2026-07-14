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
import json
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
from report_client import post_branch_report
from jsonl_export import write_jsonl, to_cents, clean_record

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
# Per-product detail endpoint — source of the full card (origins/nutrition/
# health-star/breadcrumb) which the listing does not carry.
WW_DETAIL_API = BASE_URL + "/api/v1/products/{sku}"
WW_DETAIL_CACHE_PATH = Path(__file__).resolve().parent / ".woolworths_detail_cache.json"
WW_DETAIL_CONCURRENCY = 50  # warm-only; pushed hard for speed. WW tolerated 5 with ~0 fails. If fails climb, dial back.
WW_DETAIL_SAVE_EVERY = 500  # flush the warm cache to disk every N SKUs (crash-safe/resumable)

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
logging.getLogger("httpx").setLevel(logging.WARNING)  # suppress Supabase HTTP request logs
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
    # --- Scraper Data Contract / rich detail fields (for the JSONL export) ---
    comparison_price: Optional[float] = None   # was/original price
    unit_price: Optional[float] = None         # price per unit_label (cupPrice)
    unit_label: Optional[str] = None           # cupMeasure e.g. "100g", "1L"
    category_path: Optional[str] = None        # full breadcrumb "Dept > Aisle > Shelf"
    promo_text: Optional[str] = None
    promo_type: Optional[str] = None
    card_required: bool = False
    loyalty_program_code: Optional[str] = None # everyday-rewards
    source_promotion_id: Optional[str] = None  # retailer promo id (WW: usually none)
    promo_starts_at: Optional[str] = None      # ISO from price.promotionStartDate
    promo_ends_at: Optional[str] = None        # ISO from price.promotionEndDate
    multibuy_quantity: Optional[int] = None
    multibuy_price_cents: Optional[int] = None
    promo_metadata: dict = field(default_factory=dict)  # productTags badges / raw multiBuy
    detail: dict = field(default_factory=dict) # rich raw_row (country, nutrition, ...)


_WEIGHT_PARSE_RE = re.compile(r"^(\d+\.?\d*)\s*(ml|l|kg|g|oz|lb|pk)$", re.I)


def breadcrumb_to_path(bc: object) -> Optional[str]:
    """Build "Department > Aisle > Shelf" from a Woolworths breadcrumb object."""
    if not isinstance(bc, dict):
        return None
    parts = []
    for level in ("department", "aisle", "shelf", "subShelf"):
        node = bc.get(level)
        if isinstance(node, dict) and node.get("name"):
            parts.append(str(node["name"]))
    return " > ".join(parts) if parts else None


def parse_ww_detail(data: dict) -> dict:
    """Extract WW static rich card (country/nutrition/breadcrumb) from a detail
    response. Returns {"category_path": str|None, "rich": {...}}."""
    d = data or {}
    rich: dict = {}
    origins = d.get("origins")
    if origins:
        rich["country_of_origin"] = "; ".join(origins) if isinstance(origins, list) else origins
    for src, key in (
        ("nutrition", "nutrition"),
        ("ingredients", "ingredients"),
        ("allergens", "allergens"),
        ("claims", "claims"),
        ("healthStarRating", "health_star_rating"),
        ("warnings", "warnings"),
        ("servingSuggestion", "serving_suggestion"),
    ):
        v = d.get(src)
        if v not in (None, "", [], {}):
            rich[key] = v
    return {"category_path": breadcrumb_to_path(d.get("breadcrumb")), "rich": rich}


class WWDetailCache:
    """Persistent cache of WW STATIC rich fields keyed by stockcode. The listing
    already carries fresh per-store price/special, so detail is fetched once per
    product (cache-miss only) purely for the static card — keeps cost bounded."""

    def __init__(self, path: Path = WW_DETAIL_CACHE_PATH) -> None:
        self.path = path
        self._data: dict[str, dict] = {}
        if path.exists():
            try:
                self._data = json.loads(path.read_text())
            except Exception as e:
                logger.warning(f"ww detail cache read failed ({e}) — starting empty")
        logger.info(f"ww detail cache: {len(self._data)} entries loaded")

    def get(self, sku: str) -> dict:
        return self._data.get(sku) or {}

    def has(self, sku: str) -> bool:
        return sku in self._data

    def put(self, sku: str, det: dict) -> None:
        self._data[sku] = det

    def save(self, *, quiet: bool = False) -> None:
        # Atomic write: dump to a temp file in the same dir, then os.replace() —
        # a crash mid-write can never corrupt or truncate the real cache file, so
        # the long one-time warm is safe to interrupt and resume.
        try:
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(json.dumps(self._data, ensure_ascii=False))
            os.replace(tmp, self.path)
            if not quiet:
                logger.info(f"ww detail cache: saved {len(self._data)} entries")
        except Exception as e:
            logger.warning(f"ww detail cache save failed: {e}")


_WW_DETAIL_CACHE: Optional["WWDetailCache"] = None


def get_ww_detail_cache() -> "WWDetailCache":
    """Process-wide singleton so concurrent branch workers share one cache file."""
    global _WW_DETAIL_CACHE
    if _WW_DETAIL_CACHE is None:
        _WW_DETAIL_CACHE = WWDetailCache()
    return _WW_DETAIL_CACHE


_WW_DETAIL_LOCK: Optional[asyncio.Lock] = None


def get_ww_detail_lock() -> asyncio.Lock:
    """One process-wide lock so only ONE branch live-fetches detail cards at a
    time. Detail is product-global (identical per store), so the first branch
    warms the shared cache and the rest read it for free — this lock stops
    concurrent branches from stampeding WW's Akamai detail endpoint."""
    global _WW_DETAIL_LOCK
    if _WW_DETAIL_LOCK is None:
        _WW_DETAIL_LOCK = asyncio.Lock()
    return _WW_DETAIL_LOCK


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


def url_with_page(url: str, page_num: int) -> str:
    """Return ``url`` with its ``page`` query param set to ``page_num``.

    The Woolworths browse template carries no ``page=`` param (page 1 is implicit),
    so a bare ``re.sub`` would be a no-op and every "page" would re-request page 1.
    This appends ``page=`` when it is absent so pagination actually advances.
    """
    if re.search(r"\bpage=\d+\b", url):
        return re.sub(r"\bpage=\d+\b", f"page={page_num}", url)
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}page={page_num}"


def dedupe_by_sku(products: list) -> list:
    """Drop duplicate products (same Woolworths SKU/stockcode), keeping first seen.

    A safety net against pagination returning the same page repeatedly: if the
    page param ever fails to advance again, this caps the damage and surfaces it
    via the caller's duplicate-ratio warning instead of silently inflating counts.
    """
    seen: set = set()
    out = []
    for p in products:
        key = getattr(p, "sku", None) or getattr(p, "barcode", None)
        if key is None:
            out.append(p)
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


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
        self._detail_cache = get_ww_detail_cache()  # shared static rich-card cache
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
        """READ-ONLY: look up chain_id / branch_id from store_chains / store_branches.
        The scraper must not WRITE — chain/branch creation is the importer's job.
        Create-if-missing upserts are commented out below."""
        chain = (
            self.supabase.table("store_chains")
            .select("id")
            .eq("slug", WOOLWORTHS_CHAIN_SLUG)
            .execute()
            .data
        )
        if not chain:
            # DATABASE WRITE DISABLED — do not create the chain row from the scraper.
            # chain = (
            #     self.supabase.table("store_chains")
            #     .upsert({"slug": WOOLWORTHS_CHAIN_SLUG, "name": "Woolworths"}, on_conflict="slug")
            #     .execute()
            #     .data
            # )
            logger.error(
                f"store_chains row missing for slug={WOOLWORTHS_CHAIN_SLUG} — "
                f"cannot resolve chain (scraper no longer creates it). Aborting branch."
            )
            return
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

        # DATABASE WRITE DISABLED — do not create the branch row from the scraper.
        # r = (
        #     self.supabase.table("store_branches")
        #     .upsert(
        #         {"chain_id": self.chain_id, "name": self.branch_name},
        #         on_conflict="chain_id,name",
        #     )
        #     .execute()
        #     .data
        # )
        # self.branch_id = r[0]["id"]
        logger.error(
            f"store_branches row missing for {self.branch_name!r} (chain {self.chain_id}) — "
            f"scraper no longer creates it; branch unresolved, will be skipped."
        )

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
                    page_url = url_with_page(self._api_paginated_url, page_num)
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

        products = dedupe_by_sku(products)
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
                page_url = url_with_page(direct_url, page_num)
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

        raw_count = len(products)
        products = dedupe_by_sku(products)
        if raw_count > len(products) * 1.5 and total_pages > 1:
            logger.warning(
                f"  [fast] {url}: {raw_count} rows collapsed to {len(products)} unique "
                f"— pagination may not be advancing")
            return None  # fall back to the browser path rather than trust bad data

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

        # Promo type + member tag from the listing price flags
        promo_type = promo_text = loyalty_code = None
        card_required = False
        comparison = None
        source_promotion_id = None  # Woolworths does not expose a stable promo id
        promo_starts_at = price_data.get("promotionStartDate") or None
        promo_ends_at = price_data.get("promotionEndDate") or None
        multibuy_quantity = multibuy_price_cents = None
        promo_metadata: dict = {}

        # Retailer badges drive classification — never infer half-price from the
        # 50%-off maths (a deep clearance is not a half-price promo).
        tags = item.get("productTags") or []
        tag_types = [str(t.get("tagType")).strip() for t in tags
                     if isinstance(t, dict) and t.get("tagType")]
        if tag_types:
            promo_metadata["badges"] = tag_types
        tag_blob = " ".join(tag_types).lower()
        multibuy_raw = next((t.get("multiBuy") for t in tags
                             if isinstance(t, dict) and t.get("multiBuy")), None)
        if isinstance(multibuy_raw, dict):
            promo_metadata["multiBuy"] = multibuy_raw
            q = multibuy_raw.get("quantity") or multibuy_raw.get("multiBuyForQuantity")
            amt = multibuy_raw.get("price") or multibuy_raw.get("value") or multibuy_raw.get("amount")
            try:
                multibuy_quantity = int(q) if q else None
            except (TypeError, ValueError):
                multibuy_quantity = None
            multibuy_price_cents = to_cents(float(amt)) if amt not in (None, "") else None

        if special is not None:
            comparison = price_f  # original / was price
            is_club = bool(price_data.get("isClubPrice"))
            if is_club:
                promo_type = "member_price"
                card_required = True
                loyalty_code = "everyday-rewards"
            elif "clearance" in tag_blob:
                promo_type = "clearance"
            elif "half" in tag_blob:               # only when the retailer says so
                promo_type = "half_price"
            elif multibuy_quantity:
                promo_type = "multibuy_fixed_price"
            else:
                promo_type = "special"
            save = price_data.get("savePrice")
            if save:
                promo_text = f"Save ${float(save):.2f}"
        elif multibuy_quantity:
            promo_type = "multibuy_fixed_price"

        if promo_type is None:
            # No active promotion — don't attach stray promo dates/badges.
            promo_starts_at = promo_ends_at = None
            multibuy_quantity = multibuy_price_cents = None
            promo_metadata = {}

        size = item.get("size") or {}
        weight = size.get("volumeSize") or None
        # Unit price + label (free from the listing's cup price)
        try:
            cup = size.get("cupPrice")
            unit_price = float(cup) if cup else None
        except (TypeError, ValueError):
            unit_price = None
        unit_label = size.get("cupMeasure") or None

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
            comparison_price=comparison,
            unit_price=unit_price,
            unit_label=unit_label,
            promo_type=promo_type,
            promo_text=promo_text,
            card_required=card_required,
            loyalty_program_code=loyalty_code,
            source_promotion_id=source_promotion_id,
            promo_starts_at=promo_starts_at,
            promo_ends_at=promo_ends_at,
            multibuy_quantity=multibuy_quantity,
            multibuy_price_cents=multibuy_price_cents,
            promo_metadata=promo_metadata,
        )

    # ---- Detail enrichment (static rich card, cache-backed) --------------

    def attach_cached_details(self, products: list[ScrapedProduct]) -> None:
        """Attach the static rich card (origins/nutrition/health-star/breadcrumb)
        from the shared cache. Read-only and instant — NEVER live-fetches, so the
        JSONL export is never blocked behind detail requests. Whatever the shared
        cache has been warmed with so far is applied; the rest is filled in by
        warm_detail_cache() after the export (and by later branches / re-runs)."""
        skus = {p.sku for p in products if p.sku}
        applied = 0
        for p in products:
            if not p.sku:
                continue
            det = self._detail_cache.get(p.sku)
            if det.get("category_path"):
                p.category_path = det["category_path"]
            if det.get("rich"):
                p.detail = det["rich"]
                applied += 1
        cached = sum(1 for s in skus if self._detail_cache.has(s))
        logger.info(
            f"  [detail] attached rich card to {applied} products "
            f"({cached}/{len(skus)} SKUs cached)"
        )

    async def warm_detail_cache(self, products: list[ScrapedProduct], *, block: bool = False) -> None:
        """Live-fetch the static detail card for cache-miss SKUs into the shared
        on-disk cache. Only ONE branch fetches at a time (global lock) so concurrent
        branches never stampede WW's Akamai detail endpoint. Detail is product-global,
        so this warms the cache for all branches rather than re-fetching per store.

        block=False (best-effort): if another branch already holds the lock, skip.
        block=True: wait for the lock and warm before returning — used pre-export so
        THIS branch's JSONL carries the full breadcrumb category + origin/nutrition."""
        skus = {p.sku for p in products if p.sku}
        to_fetch = [s for s in skus if not self._detail_cache.has(s)]
        if not to_fetch or not self._page:
            return
        headers = {
            k: v for k, v in (self._api_headers or self._fast_headers or {}).items()
            if k.lower() not in ("host", "content-length")
        }
        fetched = failed = 0
        lock = get_ww_detail_lock()
        if lock.locked() and not block:
            # Another branch is already warming the shared, product-global cache.
            # Best-effort mode: don't queue behind it — return and free this worker
            # slot. These SKUs fill from the shared cache once the warm finishes.
            logger.info(
                f"  [detail] shared cache already warming elsewhere — skipping "
                f"({len(to_fetch)} SKUs)"
            )
            return
        async with lock:
            # Re-check under the lock — an earlier branch may have warmed these.
            to_fetch = [s for s in to_fetch if not self._detail_cache.has(s)]
            if not to_fetch:
                return
            logger.info(f"  [detail] warming shared cache: {len(to_fetch)} SKUs need API fetch")
            i = 0
            saved_at = 0  # SKUs fetched since the last incremental flush to disk
            try:
                while i < len(to_fetch):
                    batch = to_fetch[i : i + WW_DETAIL_CONCURRENCY]
                    i += len(batch)

                    async def fetch_one(sku: str) -> tuple[str, Optional[dict]]:
                        try:
                            assert self._page
                            resp = await self._page.request.get(
                                WW_DETAIL_API.format(sku=sku), headers=headers,
                                timeout=15000)  # cap per-request so one hang can't stall the batch
                            if resp.ok:
                                return sku, parse_ww_detail(await resp.json())
                            return sku, None
                        except Exception:
                            return sku, None

                    for sku, det in await asyncio.gather(*[fetch_one(s) for s in batch]):
                        if det is not None:
                            self._detail_cache.put(sku, det)
                            fetched += 1
                        else:
                            failed += 1
                    # Incrementally persist every ~500 SKUs so an interruption of this
                    # long one-time warm never throws away hours of fetched detail;
                    # the next run resumes from the on-disk cache (atomic write).
                    if fetched - saved_at >= WW_DETAIL_SAVE_EVERY:
                        self._detail_cache.save(quiet=True)
                        saved_at = fetched
                    if i % 1000 == 0:
                        logger.info(
                            f"  [detail] warming… {min(i, len(to_fetch))}/{len(to_fetch)} "
                            f"({fetched} ok, {failed} fail)"
                        )
                    await asyncio.sleep(random.uniform(0.1, 0.3))
            finally:
                # Always flush whatever we fetched — even on Ctrl-C / crash / cancel.
                self._detail_cache.save()
        logger.info(f"  [detail] warm done: fetched {fetched}, failed {failed}")

    # ---- JSONL export (Scraper Data Contract — replaces DB writes) --------

    def _build_observation(self, p: ScrapedProduct) -> Optional[dict]:
        if not p.raw_name or p.price is None or p.price <= 0:
            return None
        on_special = bool(p.special_price and p.special_price < p.price)
        current = p.special_price if on_special else p.price
        comparison = p.comparison_price if on_special else None
        product_url = (
            f"{BASE_URL}/shop/productdetails?stockcode={p.sku}" if p.sku else None
        )
        rec = {
            "source_product_id": p.sku,
            "retailer_sku": p.sku,
            "barcode": p.barcode,
            "raw_name": p.raw_name,
            "clean_name": p.clean_name,
            "brand": p.brand,
            "category_path": p.category_path or p.category,
            "size": p.weight,
            "current_price_cents": to_cents(current),
            "comparison_price_cents": to_cents(comparison),
            "unit_price_cents": to_cents(p.unit_price),
            "unit_label": p.unit_label,
            "stock_status": "in_stock" if p.in_stock else "out_of_stock",
            "promo_text": p.promo_text,
            "promo_type": p.promo_type,
            "card_required": p.card_required or None,
            "required_loyalty_program_code": p.loyalty_program_code,
            "source_promotion_id": p.source_promotion_id,
            "promo_starts_at": p.promo_starts_at,
            "promo_ends_at": p.promo_ends_at,
            "multibuy_quantity": p.multibuy_quantity,
            "multibuy_price_cents": p.multibuy_price_cents,
            "promo_metadata": p.promo_metadata or None,
            "product_url": product_url,
            "image_url": p.image_url,
            "observed_at": p.scraped_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if p.detail:
            rec["raw_row"] = p.detail
        return clean_record(rec)

    def _export_jsonl(self, products: list[ScrapedProduct]) -> None:
        """Stage 1 of the contract: write one JSONL file for this branch+run."""
        records = [r for r in (self._build_observation(p) for p in products) if r]
        write_jsonl("woolworths", self.branch_name, records)

    # ---- Run loop --------------------------------------------------------

    async def run(self) -> dict:
        self._resolve_branch()
        logger.info(f"chain_id={self.chain_id}  branch_id={self.branch_id}  branch={self.branch_name}")

        # Auto-bootstrap if enabled — refreshes saved session before scraping.
        await self._ensure_fresh_session()

        run_id = self._start_run()
        stats = {"records_updated": 0, "records_failed": 0, "new_products": 0,
                 "price_changes": 0, "blocks_detected": 0, "retries": 0, "category_results": []}

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
            for cat_idx, url in enumerate(self.category_urls, 1):
                did_paginate = False
                products: list[ScrapedProduct] = []
                num_responses = 0
                used_fast = False
                exc: Optional[Exception] = None
                silent_block = False
                visible_block = False

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
                        exc = e
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
                            exc = e
                            logger.warning(f"  retry failed: {e}")

                    if did_paginate:
                        await self._refresh_context()
                    await self._random_delay()

                _cat_name = url.split("/browse/")[-1]
                if not products and (exc is not None or visible_block or silent_block):
                    _reason = (
                        "visible block (challenge page)" if visible_block
                        else "silent block (0 XHRs captured)" if silent_block
                        else f"{type(exc).__name__}: {exc}" if exc
                        else "empty result after retry"
                    )
                    stats["category_results"].append(
                        {"name": _cat_name, "status": "failed", "products": 0, "reason": _reason}
                    )
                elif not products:
                    stats["category_results"].append(
                        {"name": _cat_name, "status": "empty", "products": 0}
                    )
                else:
                    stats["category_results"].append(
                        {"name": _cat_name, "status": "success", "products": len(products)}
                    )
                all_products.extend(products)
            logger.info(f"TOTAL scraped: {len(all_products)} products")
            self._update_run(run_id, total_scraped=len(all_products))

            # Warm the shared product-global detail cache BEFORE the export so this
            # branch's JSONL carries the full breadcrumb category (Dept > Aisle >
            # Shelf) and the rich card (origin/nutrition/allergens/health-star).
            # Serialized across branches by a global lock; detail is cached product-
            # globally, so the first branch pays the fetch and later branches mostly
            # hit the warm cache. This intentionally blocks the export until detail
            # is fetched — completeness over speed.
            if all_products:
                await self.warm_detail_cache(all_products, block=True)
                self.attach_cached_details(all_products)

            # --- DATABASE WRITES DISABLED (two-stage contract) ---
            # The scraper no longer writes to Supabase. It emits one JSONL file per
            # branch+run; pico-prod/import_products.py owns all DB writes. The old
            # direct write is kept (commented) for reference, do not re-enable.
            # if not self.dry_run:
            #     loop = asyncio.get_event_loop()
            #     await loop.run_in_executor(None, self._save_to_supabase, all_products, stats)
            # else:
            #     logger.info("DRY RUN — skipping Supabase writes")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._export_jsonl, all_products)

            status = (
                "failed" if (stats["records_failed"] and not stats["records_updated"])
                else "partial" if stats["records_failed"]
                else "success"
            )
            self._end_run(run_id, status, stats)
            specials = sum(1 for p in all_products if p.special_price is not None)
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
                chain="Woolworths",
                branch_name=self.branch_name,
                branch_id=str(self.branch_id) if self.branch_id else None,
                store_id=None,
                status=status,
                total_products=len(all_products),
                categories=stats["category_results"],
                price_changes=stats["price_changes"],
                specials=specials,
                out_of_stock=out_of_stock,
            )
        except Exception as e:
            logger.exception("run failed")
            self._end_run(run_id, "failed", stats, error=str(e))
            raise
        finally:
            await self._close_browser()

        return stats

    # ---- Supabase writes -------------------------------------------------

    def _start_run(self) -> Optional[str]:
        # DATABASE WRITE DISABLED — run tracking moves to ingest.import_runs on the
        # import side. Returning None makes _update_run/_end_run no-op safely.
        return None
        # try:
        #     r = (
        #         self.supabase.table("scraper_runs")
        #         .insert({
        #             "chain_id": self.chain_id,
        #             "branch_id": self.branch_id,
        #             "status": "running",
        #             "started_at": datetime.now(timezone.utc).isoformat(),
        #         })
        #         .execute()
        #     )
        #     return r.data[0]["id"]
        # except Exception as e:
        #     logger.warning(f"could not start scraper_runs row: {e}")
        #     return None

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
                if p.category:
                    row["tags"] = [p.category]   # simple category tag
                if wv is not None:
                    row["weight_value"] = wv
                if wu is not None:
                    row["weight_unit"] = wu
                rows[p.barcode] = row

            payload = list(rows.values())
            for i in range(0, len(payload), CHUNK):
                chunk = payload[i : i + CHUNK]
                try:
                    # ignore_duplicates=False so image_url / brand / tags refresh on
                    # existing products (the True setting left them stale forever).
                    self.supabase.table("products").upsert(
                        chunk, on_conflict="barcode", ignore_duplicates=False
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
            retailer_url = (
                f"{BASE_URL}/shop/productdetails?stockcode={p.sku}"
                if p.sku else None
            )
            sp_rows.append({
                "product_id": product_id,
                "store_id": self.branch_id,
                "sku": p.barcode,
                "current_price": effective,
                "unit_label": p.weight,          # size/pack label e.g. "100g"
                "retailer_url": retailer_url,
                "in_stock": p.in_stock,
                "scraped_at": p.scraped_at.isoformat(),
            })

        sp_rows = list({r["product_id"]: r for r in sp_rows}.values())
        ph_rows = list({r["store_product_id"]: r for r in ph_rows}.values())

        # ---------- Phase 5: upsert store_products in chunks ----------
        # product_id -> store_product_id (seed from existing rows, then upsert responses)
        sp_id_map: dict[str, str] = {
            pid: row["id"] for pid, row in existing_map.items() if row.get("id")
        }
        total_upserted = 0
        for i in range(0, len(sp_rows), CHUNK):
            chunk = sp_rows[i : i + CHUNK]
            try:
                r = self.supabase.table("store_products").upsert(
                    chunk, on_conflict="product_id,store_id"
                ).execute()
                total_upserted += len(r.data)
                for rr in (r.data or []):
                    if rr.get("product_id") and rr.get("id"):
                        sp_id_map[rr["product_id"]] = rr["id"]
            except Exception as e:
                logger.error(f"store_products upsert chunk {i // CHUNK + 1}: {e}")
                stats["records_failed"] += len(chunk)
        stats["records_updated"] = total_upserted
        logger.info(f"upserted {total_upserted} store_products rows")

        # ---------- Phase 5b: specials ----------
        self._save_specials(matched, sp_id_map)

        # ---------- Phase 6: insert price_history ----------
        if ph_rows:
            for i in range(0, len(ph_rows), CHUNK):
                chunk = ph_rows[i : i + CHUNK]
                try:
                    self.supabase.table("price_history").insert(chunk).execute()
                except Exception as e:
                    logger.warning(f"price_history insert chunk {i // CHUNK + 1}: {e}")
            logger.info(f"recorded {len(ph_rows)} price changes")

    def _save_specials(self, matched: list[tuple[str, ScrapedProduct]], sp_id_map: dict[str, str]) -> None:
        """Write specials for products currently on special; deactivate ended ones.
        Simple is_active model — one active special per store_product."""
        CHUNK = 200
        current: dict[str, tuple[float, float]] = {}
        for product_id, p in matched:
            if p.special_price and p.special_price < p.price:
                spid = sp_id_map.get(product_id)
                if spid:
                    current[spid] = (p.special_price, p.price)

        active: dict[str, str] = {}  # store_product_id -> special id
        all_spids = list(sp_id_map.values())
        for i in range(0, len(all_spids), 100):
            chunk = all_spids[i : i + 100]
            try:
                r = (self.supabase.table("specials")
                     .select("id,store_product_id")
                     .in_("store_product_id", chunk).eq("is_active", True).execute())
                for row in r.data:
                    active[row["store_product_id"]] = row["id"]
            except Exception as e:
                logger.warning(f"specials fetch chunk {i // 100 + 1}: {e}")

        to_deactivate = [active[spid] for spid in active if spid not in current]
        for i in range(0, len(to_deactivate), 200):
            chunk = to_deactivate[i : i + 200]
            try:
                self.supabase.table("specials").update(
                    {"is_active": False}).in_("id", chunk).execute()
            except Exception as e:
                logger.warning(f"specials deactivate chunk {i // 200 + 1}: {e}")

        to_insert = [
            {
                "store_product_id": spid,
                "special_price": sp,
                "original_price": orig,
                "label": f"Save ${orig - sp:.2f}",
                "is_active": True,
                "source": "scraped",
            }
            for spid, (sp, orig) in current.items() if spid not in active
        ]
        for i in range(0, len(to_insert), CHUNK):
            chunk = to_insert[i : i + CHUNK]
            try:
                self.supabase.table("specials").insert(chunk).execute()
            except Exception as e:
                logger.warning(f"specials insert chunk {i // CHUNK + 1}: {e}")
        logger.info(
            f"specials: {len(to_insert)} new, {len(to_deactivate)} deactivated, "
            f"{len(current)} on special now"
        )

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
