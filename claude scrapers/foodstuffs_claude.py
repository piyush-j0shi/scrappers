"""
Foodstuffs NZ scraper — Claude build.

Covers New World and Pak'nSave (same Foodstuffs backend, only domain differs).
Self-contained — does not modify or import any existing scraper code.

Capabilities:
  - Captures the /paginated/products POST API the storefront fires
  - Overrides storeId in the POST body so we can pin to any branch via api_store_id
    (no saved sessions required, unlike Woolworths)
  - Paginates page 2..N directly through page.request.post
  - Parallel barcode enrichment with adaptive concurrency
    (downscales 12 → 8 → 4 → 1 on rate-limit)
  - Persistent productId→barcode cache at .foodstuffs_cache.json
    so the second daily run is near-instant
  - Auto-detects live Supabase schema (same tables as Woolworths build)
  - Barcode-first matching with name fallback

CLI (same shape as woolworths_claude.py):
  python3 foodstuffs_claude.py --chain newworld --test
  python3 foodstuffs_claude.py --chain paknsave --test
  python3 foodstuffs_claude.py --chain newworld --branch "New World New Lynn"
  python3 foodstuffs_claude.py --chain paknsave --branch-id <uuid>
  python3 foodstuffs_claude.py --chain newworld --all-branches
  python3 foodstuffs_claude.py --chain newworld --categories fruit-and-vegetables,bakery
  python3 foodstuffs_claude.py --chain paknsave --no-headless --dry-run
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import json
import logging
import os
import random
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
# from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from patchright.async_api import async_playwright, Browser, BrowserContext, Page
from supabase import create_client, Client
# from report_client import post_branch_report  # disabled for server deploy (no monitor UI)
from jsonl_export import write_jsonl, to_cents, clean_record, _slug
from run_log import ScraperRunLog, category_record

# ---------------------------------------------------------------------------
# Paths & env
# ---------------------------------------------------------------------------

THIS_DIR = Path(__file__).resolve().parent
SCRAPERS_DIR = THIS_DIR.parent
ENV_PATH = SCRAPERS_DIR / ".env"
CACHE_PATH = THIS_DIR / ".foodstuffs_cache.json"


def _template_path(chain_key: str) -> Path:
    return THIS_DIR / f".{chain_key}_direct_template.json"


# A JWT is three base64url segments; the first two start with "eyJ" (base64 of '{"').
_JWT_RE = re.compile(r"eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")


def _find_jwt(obj):
    """Return the first JWT-looking string anywhere in a decoded-JSON object, else None.
    Tolerates wrappers like `Bearer `/`JWT `/quotes/byte-strings (we just search for the pattern)."""
    if isinstance(obj, str):
        m = _JWT_RE.search(obj)
        return m.group(0) if m else None
    if isinstance(obj, dict):
        for v in obj.values():
            t = _find_jwt(v)
            if t:
                return t
    elif isinstance(obj, list):
        for v in obj:
            t = _find_jwt(v)
            if t:
                return t
    return None


def _swap_store_in_body(body: dict, old_sid: str, new_sid: str) -> None:
    """In-place: replace the capture store id with this branch's store id, wherever it appears
    (top-level storeId/store_id and inside the algoliaQuery.filters string)."""
    if not (old_sid and new_sid and old_sid != new_sid):
        return
    for f in ("storeId", "store_id"):
        if f in body:
            body[f] = new_sid
    alg = body.get("algoliaQuery") or {}
    if isinstance(alg, dict) and isinstance(alg.get("filters"), str) and old_sid in alg["filters"]:
        alg["filters"] = alg["filters"].replace(old_sid, new_sid)


load_dotenv(ENV_PATH)
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# CapSolver — AntiCloudflareTask routed through a local proxy.py exposed via ngrok,
# so the challenge is solved (and the resulting cf_clearance is bound) to YOUR home IP.
# CAPSOLVER_PROXY is the ngrok forwarding address, e.g. http://0.tcp.ngrok.io:12345
CAPSOLVER_API_KEY = os.getenv("CAPSOLVER_API_KEY", "")
CAPSOLVER_PROXY = os.getenv("CAPSOLVER_PROXY", "")

# ---------------------------------------------------------------------------
# Chain config
# ---------------------------------------------------------------------------

CHAINS: dict[str, dict] = {
    "newworld": {
        "slug": "new-world",
        "name": "New World",
        "default_branch": "New World New Lynn",
        "base_url": "https://www.newworld.co.nz",
        # Verified live 2026-05-06 against the side-nav on newworld.co.nz.
        # Excludes seasonal / charity / aliases: "mother", "family2family-donation",
        # "fresh-foods-and-bakery", "chilled-frozen-and-desserts", "drinks",
        # "beer-cider-and-wine" (alias of beer-wine-and-cider), "featured".
        "categories": [
            "fruit-and-vegetables", "meat-poultry-and-seafood", "fridge-deli-and-eggs",
            "bakery", "frozen", "pantry", "hot-and-cold-drinks",
            "snacks-treats-and-easy-meals",
            "health-and-body", "household-and-cleaning",
            "baby-and-toddler", "pets", "beer-wine-and-cider",
            "meat-and-seafood-deals",  # added 2026-05-07 from screenshot — verify it has unique products
        ],
    },
    "paknsave": {
        "slug": "paknsave",
        "name": "Pak'nSave",
        # Note: matches the row in store_branches that has api_store_id populated
        # (there's also a duplicate "Pak'nSave Sylvia Park" with no api_store_id)
        "default_branch": "PAK'nSAVE Sylvia Park",
        "base_url": "https://www.paknsave.co.nz",
        # Verified live 2026-05-06 against the side-nav on paknsave.co.nz.
        "categories": [
            "fruit-and-vegetables", "meat-poultry-and-seafood", "fridge-deli-and-eggs",
            "bakery", "frozen", "pantry", "hot-and-cold-drinks",
            "snacks-treats-and-easy-meals", "health-and-body", "household-and-cleaning",
            "baby-and-toddler", "pets", "beer-wine-and-cider",
        ],
    },
}

URL_TO_CATEGORY = {
    "fruit-and-vegetables": "produce",
    "meat-poultry-and-seafood": "meat",
    "fridge-deli-and-eggs": "dairy",
    "bakery": "bakery",
    "frozen": "frozen",
    "pantry": "pantry",
    "hot-and-cold-drinks": "drinks",
    "snacks-treats-and-easy-meals": "pantry",
    "health-and-body": "health",
    "household-and-cleaning": "household",
    "baby-and-toddler": "baby",
    "pets": "pet",
    "beer-wine-and-cider": "alcohol",
    "meat-and-seafood-deals": "meat",
}

# Maps URL slug → Algolia category0SI display name used in the filters field.
# Captured by running --filters-probe against a live branch.
SLUG_TO_DISPLAY_NAME: dict[str, str] = {
    "fruit-and-vegetables":       "Fruit & Vegetables",
    "meat-poultry-and-seafood":   "Meat, Poultry & Seafood",
    "fridge-deli-and-eggs":       "Fridge, Deli & Eggs",
    "bakery":                     "Bakery",
    "frozen":                     "Frozen",
    "pantry":                     "Pantry",
    "hot-and-cold-drinks":        "Hot & Cold Drinks",
    "snacks-treats-and-easy-meals": "Snacks, Treats & Easy Meals",
    "health-and-body":            "Health & Body",
    "household-and-cleaning":     "Household & Cleaning",
    "baby-and-toddler":           "Baby & Toddler",
    "pets":                       "Pets",
    "beer-wine-and-cider":        "Beer, Wine & Cider",
    "meat-and-seafood-deals":     "Meat & Seafood Deals",
}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

EXTRA_HEADERS = {
    "Accept-Language": "en-NZ,en-GB;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Upgrade-Insecure-Requests": "1",
}

# STEALTH_SCRIPT commented out — patchright handles Runtime.enable at the CDP level,
# making JS-based webdriver detection redundant.
# STEALTH_SCRIPT = """
# Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
# Object.defineProperty(navigator, 'plugins', {
#     get: () => {
#         const arr = [
#             { filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
#             { filename: 'internal-nacl-plugin', description: 'Native Client' },
#         ];
#         arr.item = (i) => arr[i]; arr.namedItem = (n) => arr.find(p => p.filename === n);
#         return arr;
#     },
# });
# Object.defineProperty(navigator, 'languages', { get: () => ['en-NZ', 'en-GB', 'en'] });
# if (!window.chrome) window.chrome = {};
# if (!window.chrome.runtime) window.chrome.runtime = {};
# window.chrome.loadTimes = function() { return {}; };
# window.chrome.csi = function() { return {}; };
# """
STEALTH_SCRIPT = ""

REQUEST_TIMEOUT_MS = 30_000
DELAY_MIN = 1.0
DELAY_MAX = 3.0
ENRICH_CONCURRENCY_INITIAL = 12
# Global request ceiling (req/sec) shared by every worker, overridable with
# --rate. Raised 12 -> 18 on 2026-07-16: the 2026-07-13/14 full runs both
# finished with blocks=0 at 12, so there was headroom. 18 is a step up, not a
# proven ceiling — watch blocks=/429s in the DONE line and back off if they climb.
DEFAULT_RATE_LIMIT = 18.0

# promoId -> {"start","end","suspended"}, harvested from detail promotionList[].
# Process-wide (like BarcodeCache) because a FoodstuffsScraper is built PER
# BRANCH: scoped to the instance, all 148 branches would re-fetch the same
# national deal. Promotions are promotionClass=MASS, so one lookup serves every
# branch. In-memory only and never persisted — promo weeks roll over, and a
# stale date is worse than no date. Each chain runs as its own process, so NW
# and PnS promoIds can't collide here.
_PROMO_DATE_CACHE: dict[str, dict] = {}

# Cloudflare page-title fragments that indicate a challenge/block
_CF_BLOCK_TITLES = ("access denied", "just a moment", "security check",
                    "attention required", "error 403", "403 forbidden")

ADAPTIVE_BLOCK_THRESHOLD = 20  # cumulative blocked categories before dropping one level


class AdaptiveSemaphore:
    """asyncio.Semaphore with a runtime-adjustable limit.

    Call downgrade() to reduce the target by 1 (floors at min_level).
    Already-running tasks are not interrupted; the new limit takes effect
    as soon as a slot is released.
    """

    def __init__(self, initial: int, min_level: int = 1) -> None:
        self._target = max(min_level, initial)
        self._max = self._target
        self._min = min_level
        self._active = 0
        self._cond = asyncio.Condition(asyncio.Lock())

    @property
    def current(self) -> int:
        return self._target

    async def acquire(self) -> None:
        async with self._cond:
            while self._active >= self._target:
                await self._cond.wait()
            self._active += 1

    async def release(self) -> None:
        async with self._cond:
            self._active -= 1
            self._cond.notify_all()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, *_):
        await self.release()

    def downgrade(self) -> tuple[int, int]:
        """Reduce target by 1. Returns (old, new). No-op if already at min."""
        old = self._target
        if self._target > self._min:
            self._target -= 1
        return old, self._target

    async def upgrade(self) -> tuple[int, int]:
        """Restore target to original max. Returns (old, new). No-op if already at max."""
        async with self._cond:
            old = self._target
            if self._target < self._max:
                self._target = self._max
                self._cond.notify_all()
            return old, self._target
ENRICH_CONCURRENCY_MIN = 1
ENRICH_429_THRESHOLD = 3   # consecutive 429s before downscale
BARCODE_API = "https://api-prod.newworld.co.nz/v1/edge/store/{store_id}/product/{pid}"

# Algolia hard-caps paginated results at 20 pages * 50/page = 1000 hits (this is the
# retailer's own website limit too, not something we impose). When a category query
# lands on exactly this ceiling, category1SI/brand facet counts >1000 combined
# mean real products are hidden beyond hit 1000 — split the query by facet value to
# recover them. See _recover_capped_category / _fetch_filtered_category.
ALGOLIA_PAGE_CAP = 20

# Asset-blocking patterns to cut bandwidth (~3x reduction)
ASSET_BLOCK_RE = re.compile(
    r"\.(png|jpg|jpeg|gif|webp|svg|ico|woff2?|ttf|otf|mp4|mp3|css)(\?|$)|"
    r"(google-analytics|googletagmanager|doubleclick|facebook|hotjar)",
    re.I,
)



logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logging.getLogger("httpx").setLevel(logging.WARNING)  # suppress Supabase HTTP request logs
logger = logging.getLogger("foodstuffs_claude")

# File logging — writes to logs/<chain>_YYYY-MM-DD.log (set up in main() once chain is known)
_log_dir = Path(__file__).parent / "logs"
_log_dir.mkdir(exist_ok=True)

def _setup_file_logging(chain_slug: str) -> None:
    from datetime import date
    log_file = _log_dir / f"{chain_slug}_{date.today()}.log"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    logging.getLogger().addHandler(fh)
    logger.info(f"logging to {log_file}")

# ---------------------------------------------------------------------------
# ScrapedProduct
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
    weight: Optional[str] = None
    barcode: Optional[str] = None
    in_stock: bool = True
    product_id: Optional[str] = None  # Foodstuffs internal productId (used for barcode lookup)
    scraped_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    # --- Scraper Data Contract / rich detail fields (for the JSONL export) ---
    comparison_price: Optional[float] = None   # was/original/non-member price
    unit_price: Optional[float] = None         # price per unit_label
    unit_label: Optional[str] = None           # e.g. "1kg", "100g", "1L", "ea"
    category_path: Optional[str] = None        # full hierarchy "A > B > C"
    promo_text: Optional[str] = None           # exact visible promo wording
    promo_type: Optional[str] = None           # contract promo_type code
    card_required: bool = False                # loyalty/member card needed?
    loyalty_program_code: Optional[str] = None # club-plus / everyday-rewards
    source_promotion_id: Optional[str] = None  # retailer promoId (stable deal id)
    multibuy_quantity: Optional[int] = None    # threshold when >1 (e.g. 3 for $6)
    multibuy_price_cents: Optional[int] = None # total price for multibuy_quantity units
    promo_starts_at: Optional[str] = None      # only if FS exposes it (see _parse_item)
    promo_ends_at: Optional[str] = None
    promo_metadata: dict = field(default_factory=dict)  # badges + raw_promotion audit copy
    detail: dict = field(default_factory=dict) # rich raw_row (country, nutrition, ...)


_WEIGHT_PARSE_RE = re.compile(r"^(\d+\.?\d*)\s*(ml|l|kg|g|oz|lb|pk)$", re.I)


def parse_weight_fields(weight_str: Optional[str]) -> tuple[Optional[float], Optional[str]]:
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


def extract_fs_image(data: dict) -> Optional[str]:
    """Real product image from the detail-API response (images.primaryImages).
    The listing carries no image, so this is the only robust source. Returns
    None when the product genuinely has no image (primaryImages absent)."""
    imgs = (data or {}).get("images") or {}
    primary = imgs.get("primaryImages") or {}
    for size in ("400px", "500px", "300px", "200px", "100px"):
        url = primary.get(size)
        if url:
            return url
    return None


def category_path_from_trees(trees: object) -> Optional[str]:
    """Build "level0 > level1 > level2 ..." from a Foodstuffs categoryTrees list.
    Present in both the listing item and the detail response."""
    if not isinstance(trees, list) or not trees:
        return None
    node = trees[0] or {}
    parts = []
    for i in range(6):  # level0..level5, stop at first gap
        v = node.get(f"level{i}")
        if not v:
            break
        parts.append(str(v))
    return " > ".join(parts) if parts else None


def extract_fs_detail(data: dict) -> dict:
    """Pull the rich 'product card' fields from a Foodstuffs detail response into
    a raw_row dict (country/nutrition/ingredients/allergens, per the contract)."""
    d = data or {}
    out: dict = {}
    origin = d.get("originStatement") or d.get("countryOfOrigin")
    if origin:
        out["country_of_origin"] = origin
    for src, key in (
        ("nutritionalInfo", "nutrition"),
        ("ingredientStatement", "ingredients"),
        ("fsIngredientStatement", "ingredients"),
        ("allergenStatement", "allergens"),
        ("fsSupplementaryAllergenStatement", "allergen_may_contain"),
        ("healthStarRating", "health_star_rating"),
    ):
        v = d.get(src)
        if v not in (None, "", [], {}):
            out.setdefault(key, v)

    # Free-text blurb the site renders as data-testid="product-description". Already in
    # this response, so no extra request. For the ~25% of products Foodstuffs ships with
    # no structured card, it is the only detail there is (and sometimes carries the
    # "May contain ..." line). Stored verbatim — never parsed into allergen fields.
    desc = d.get("description")
    if desc:
        out["description"] = desc
    return out


def _parse_proxy(url: str) -> dict:
    """Parse 'http://user:pass@host:port' → Playwright proxy dict."""
    from urllib.parse import urlparse
    p = urlparse(url)
    if not p.scheme or not p.hostname:
        raise ValueError(f"bad proxy URL: {url!r}")
    out: dict = {"server": f"{p.scheme}://{p.hostname}:{p.port or 8080}"}
    if p.username:
        out["username"] = p.username
    if p.password:
        out["password"] = p.password
    return out


_SPOOF_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
)


async def _solve_cf_via_headless(cfg: dict) -> tuple[list[dict], Optional[str]]:
    """UA-spoofed headless Chrome — CF auto-passes on a normal IP, no paid solver needed."""
    import nodriver as uc
    import nodriver.cdp.network as _cdn
    try:
        _orig_fj = _cdn.Cookie.from_json.__func__
        _cdn.Cookie.from_json = classmethod(
            lambda cls, j: _orig_fj(cls, {**j, "sameParty": j.get("sameParty", False)})
        )
    except Exception:
        pass
    base_url = cfg["base_url"]
    target_url = f"{base_url}/shop/category/fruit-and-vegetables"
    logger.info(f"[cf] headless Chrome (UA-spoof) on {base_url} ...")
    nd_browser = None
    cookies: list[dict] = []
    user_agent: Optional[str] = None
    try:
        nd_browser = await uc.start(
            headless=True,
            browser_args=[
                "--disable-http2",
                "--lang=en-NZ",
                "--disable-dev-shm-usage",
                f"--user-agent={_SPOOF_UA}",
            ],
        )
        page = await nd_browser.get(target_url)
        for _ in range(20):
            await asyncio.sleep(1)
            title = await page.evaluate("document.title")
            if title and "just a moment" not in title.lower():
                break
        await asyncio.sleep(5)
        user_agent = await page.evaluate("navigator.userAgent")
        logger.info(f"[cf] UA: {user_agent}")
        try:
            all_cookies = await asyncio.wait_for(nd_browser.cookies.get_all(), timeout=8)
        except asyncio.TimeoutError:
            all_cookies = await asyncio.wait_for(page.send(_cdn.get_all_cookies()), timeout=8)
        cookies = [
            {"name": c.name, "value": c.value, "domain": c.domain,
             "path": c.path or "/", "secure": bool(c.secure), "httpOnly": bool(c.http_only)}
            for c in all_cookies
            if c.name in ("cf_clearance", "__cf_bm", "_cfuvid")
        ]
        if cookies:
            logger.info(f"[cf] got {len(cookies)} CF cookies (headless)")
        else:
            logger.warning("[cf] no CF cookies — Turnstile may not have resolved")
    except Exception as e:
        logger.warning(f"[cf] headless solve failed: {e} — proceeding without CF clearance")
    finally:
        if nd_browser:
            try:
                nd_browser.stop()
            except Exception:
                pass
    return cookies, user_agent


def _capsolver_proxy_str(url: str) -> str:
    """Convert 'http://user:pass@host:port' (ngrok address) → CapSolver proxy string.

    CapSolver wants 'scheme:host:port' or 'scheme:host:port:user:pass'.
    """
    from urllib.parse import urlparse
    p = urlparse(url if "://" in url else f"http://{url}")
    scheme = (p.scheme or "http").lower()
    parts = [scheme, p.hostname or "", str(p.port or 8080)]
    if p.username:
        parts += [p.username, p.password or ""]
    return ":".join(parts)


# Toggled on by the --capsolver CLI flag. When off, CF is solved by the UA-spoofed
# headless browser (free, no tunnel). When on, CapSolver AntiCloudflareTask is used.
CAPSOLVER_ENABLED = False


def _capsolver_configured() -> bool:
    """Creds present — CapSolver can be used (as primary or as auto-fallback)."""
    return bool(CAPSOLVER_API_KEY and CAPSOLVER_PROXY)


def _capsolver_active() -> bool:
    """CapSolver explicitly requested via --capsolver (and creds present)."""
    return bool(CAPSOLVER_ENABLED and _capsolver_configured())


def _capsolver_solve_blocking(target_url: str, domain: str, bind_ua: str) -> tuple[list[dict], Optional[str]]:
    """Synchronous CapSolver AntiCloudflareTask call. Run inside asyncio.to_thread.

    Solves the Cloudflare challenge through the ngrok→home-IP proxy, so the returned
    cf_clearance is bound to your home IP (matches subsequent requests routed the same way).

    cf_clearance is also bound to the User-Agent used to solve it. We send a fixed UA
    (`bind_ua`) so the binding is deterministic, and return the UA the cookie is bound to
    (CapSolver's echoed UA if present, else the one we sent) so the caller can set the
    browser context's UA to exactly match. Mismatched UA → CF may reject the cookie.
    """
    import urllib.request

    def _post(path: str, payload: dict) -> dict:
        req = urllib.request.Request(
            f"https://api.capsolver.com/{path}",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())

    proxy = _capsolver_proxy_str(CAPSOLVER_PROXY)
    logger.info(f"[cf] CapSolver AntiCloudflareTask via proxy {proxy.split(':',1)[0]}:***  bind_ua={bind_ua}")
    create = _post("createTask", {
        "clientKey": CAPSOLVER_API_KEY,
        "task": {
            "type": "AntiCloudflareTask",
            "websiteURL": target_url,
            "proxy": proxy,
            "userAgent": bind_ua,
        },
    })
    if create.get("errorId"):
        logger.warning(f"[cf] CapSolver createTask error: {create.get('errorDescription')}")
        return [], None
    task_id = create.get("taskId")
    if not task_id:
        logger.warning("[cf] CapSolver returned no taskId")
        return [], None

    # Poll up to ~120s
    for _ in range(40):
        time.sleep(3)
        res = _post("getTaskResult", {"clientKey": CAPSOLVER_API_KEY, "taskId": task_id})
        if res.get("errorId"):
            logger.warning(f"[cf] CapSolver getTaskResult error: {res.get('errorDescription')}")
            return [], None
        if res.get("status") == "ready":
            sol = res.get("solution", {}) or {}
            # cf_clearance is bound to the UA used to solve. Prefer CapSolver's echoed UA;
            # fall back to the UA we sent so the browser context always has a matching UA.
            ua = sol.get("userAgent") or bind_ua
            if sol.get("userAgent") and sol["userAgent"] != bind_ua:
                logger.info(f"[cf] CapSolver used a different UA than requested — binding to: {ua}")
            raw = sol.get("cookies") or {}
            # cookies may be a dict {name: value} or a list of cookie dicts
            cookies: list[dict] = []
            if isinstance(raw, dict):
                items = raw.items()
            else:
                items = [(c.get("name"), c.get("value")) for c in raw if isinstance(c, dict)]
            for name, value in items:
                if not name:
                    continue
                cookies.append({
                    "name": name, "value": value, "domain": domain,
                    "path": "/", "secure": True, "httpOnly": True,
                })
            if cookies:
                logger.info(f"[cf] CapSolver solved — {len(cookies)} cookies "
                            f"({', '.join(c['name'] for c in cookies)})  ua-bound={ua}")
            else:
                logger.warning("[cf] CapSolver ready but returned no cookies "
                               "(may have returned only a Turnstile token)")
            return cookies, ua
    logger.warning("[cf] CapSolver timed out after ~120s")
    return [], None


async def _solve_cf_via_capsolver(cfg: dict) -> tuple[list[dict], Optional[str]]:
    """CapSolver AntiCloudflareTask routed through ngrok→proxy.py→home IP."""
    from urllib.parse import urlparse
    base_url = cfg["base_url"]
    target_url = f"{base_url}/shop/category/fruit-and-vegetables"
    host = (urlparse(base_url).hostname or "").removeprefix("www.")
    domain = f".{host}" if host else ""
    # Bind the cookie to the SAME UA the patchright context uses (_SPOOF_UA), so CF's
    # UA-binding check passes. _new_context() sets user_agent=self._cf_user_agent (this UA).
    return await asyncio.to_thread(_capsolver_solve_blocking, target_url, domain, _SPOOF_UA)


# A headless solve is only trusted if it yields cf_clearance AND at least this many CF
# cookies. A weak solve (often just 1 cookie) tends to 403 mid-scrape; at/below this we
# escalate to CapSolver. A healthy CF solve sets cf_clearance + __cf_bm + _cfuvid (3).
MIN_HEADLESS_CF_COOKIES = 3


def _cf_solve_ok(cookies: list[dict]) -> bool:
    """True only if the headless solve looks strong enough to trust (else escalate)."""
    names = {c.get("name") for c in cookies}
    return "cf_clearance" in names and len(cookies) >= MIN_HEADLESS_CF_COOKIES


async def _solve_cf_clearance(cfg: dict) -> tuple[list[dict], Optional[str]]:
    """Get CF clearance for the one-time template capture, with automatic fallback.

    - Default: free UA-spoofed headless solve first; if it yields a WEAK result
      (no cf_clearance, or fewer than MIN_HEADLESS_CF_COOKIES cookies — a weak solve tends
      to 403 mid-scrape), AUTO-SHIFT to CapSolver when creds are configured
      (CAPSOLVER_API_KEY + CAPSOLVER_PROXY in .env).
    - With --capsolver: CapSolver first, headless as the backup.

    The result is stored on the shared CfState, so whichever solve wins is reused by ALL
    workers — this applies at startup and mid-run (a 403 triggers CfState.ensure_fresh,
    which re-runs this same logic).

    Once the API template is on disk, daily runs POST directly to api-prod (API-key gated,
    not Cloudflare-challenged) and this is not called.
    """
    if _capsolver_active():
        # Explicitly requested → CapSolver first, headless as backup.
        cookies, ua = await _solve_cf_via_capsolver(cfg)
        if cookies:
            return cookies, ua
        logger.warning("[cf] CapSolver produced no cookies — falling back to headless solve")
        return await _solve_cf_via_headless(cfg)

    # Default → free headless UA-spoof first.
    cookies, ua = await _solve_cf_via_headless(cfg)
    if _cf_solve_ok(cookies):
        return cookies, ua

    # Weak/empty headless solve → escalate to CapSolver if creds are configured.
    if _capsolver_configured():
        logger.warning(f"[cf] headless solve weak ({len(cookies)} cookies, "
                       f"need cf_clearance + >={MIN_HEADLESS_CF_COOKIES}) — auto-shifting to CapSolver")
        cs_cookies, cs_ua = await _solve_cf_via_capsolver(cfg)
        if cs_cookies:
            return cs_cookies, cs_ua
        logger.warning("[cf] CapSolver auto-fallback also produced no cookies — using headless result")
    else:
        logger.warning(f"[cf] headless solve weak ({len(cookies)} cookies) and CapSolver not configured — "
                       "set CAPSOLVER_API_KEY + CAPSOLVER_PROXY in .env to enable auto-shift")
    return cookies, ua


class CfState:
    """Shared CF clearance for all workers. Solve once; re-solve behind a lock on 403."""

    def __init__(self) -> None:
        self.cookies: list[dict] = []
        self.user_agent: Optional[str] = None
        self._lock = asyncio.Lock()
        self._solved_at: float = 0.0

    async def solve(self, cfg: dict) -> None:
        self.cookies, self.user_agent = await _solve_cf_clearance(cfg)
        self._solved_at = time.time()

    async def ensure_fresh(self, cfg: dict) -> None:
        """If another worker re-solved within 300s, reuse; otherwise re-solve."""
        async with self._lock:
            if self.cookies and (time.time() - self._solved_at) < 300:
                logger.info("[cf-shared] reusing recent CF solve")
                return
            await self.solve(cfg)


class TemplateState:
    """Shared direct-POST API template. Persisted to disk so future runs skip browser nav."""

    def __init__(self, chain_key: str) -> None:
        self._path = _template_path(chain_key)
        self._lock = asyncio.Lock()
        self.body: Optional[dict] = None             # legacy single body (old format / fallback)
        self.bodies: dict[str, dict] = {}            # per-category slug -> request body (verbatim replay)
        self.url: str = ""
        self.headers: dict[str, str] = {}
        self.store_id: str = ""                      # storeId the bodies were captured under
        self.token_request: Optional[dict] = None    # how to re-mint the Bearer (mid-run 401)
        self.token_version: int = 0                  # bumped on each refresh; workers adopt newest
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text())
            self.url = data["url"]
            self.headers = data["headers"]
            self.store_id = data.get("template_store_id", "")
            self.bodies = data.get("bodies", {}) or {}
            self.body = data.get("body_template")     # may be None in new format
            self.token_request = data.get("token_request")
            extra = f", {len(self.bodies)} categories" if self.bodies else ""
            tok = ", token-refresh ready" if self.token_request else ""
            logger.info(f"[template] loaded from {self._path.name}{extra}{tok}")
        except Exception as e:
            logger.warning(f"[template] load failed: {e}")

    def _persist(self) -> None:
        try:
            self._path.write_text(json.dumps({
                "url": self.url,
                "headers": self.headers,
                "template_store_id": self.store_id,
                "bodies": self.bodies,
                "token_request": self.token_request,
                "body_template": self.body or next(iter(self.bodies.values()), None),
            }, ensure_ascii=False, indent=2))
        except Exception as e:
            logger.warning(f"[template] save failed: {e}")

    def save_category(self, slug: str, body: dict, url: str, headers: dict, store_id: str) -> None:
        """Record one category's working request body (verbatim replay — no field parsing)."""
        self.url = url
        self.headers = headers
        self.store_id = store_id
        first = not self.bodies
        self.bodies[slug] = body
        self._persist()
        if first:
            logger.info(f"[template] saved to {self._path.name} (direct-POST: {url})")
        logger.info(f"[template] captured category '{slug}' ({len(self.bodies)} total)")

    def save_token_request(self, token_request: dict) -> None:
        if self.token_request:
            return
        self.token_request = token_request
        self._persist()
        logger.info(f"[template] token-mint request captured ({token_request.get('url')}) — "
                    f"mid-run 401s can refresh the Bearer")

    def set_token(self, new_token: str) -> None:
        """Patch the Bearer in the shared headers after a refresh, and persist."""
        self.headers = {**self.headers, "authorization": f"Bearer {new_token}"}
        self.token_version += 1
        self._persist()

    def invalidate(self) -> None:
        self.body = None
        self.bodies = {}
        self.url = ""
        self.headers = {}
        self.store_id = ""
        self.token_request = None
        try:
            self._path.unlink(missing_ok=True)
            logger.info("[template] invalidated (deleted from disk)")
        except Exception:
            pass

    @property
    def ready(self) -> bool:
        return bool((self.bodies or self.body) and self.url and self.headers)


class TokenBucketLimiter:
    """Token-bucket rate limiter: max `rate` req/s globally across all workers.
    Also acts as a global 429 freeze: any worker that hits a 429 can pause all workers.
    """

    def __init__(self, rate: float, burst: Optional[int] = None) -> None:
        self._rate = rate
        self._tokens = float(burst or rate)
        self._max = float(burst or rate)
        self._lock = asyncio.Lock()
        self._last = time.monotonic()
        self._pause_event = asyncio.Event()
        self._pause_event.set()  # initially unpaused
        self._pause_lock = asyncio.Lock()

    async def acquire(self) -> None:
        was_paused = not self._pause_event.is_set()
        await self._pause_event.wait()  # block if a 429 freeze is active
        if was_paused:
            await asyncio.sleep(random.uniform(0, 2))  # jitter only after a freeze
        async with self._lock:
            now = time.monotonic()
            self._tokens = min(self._max, self._tokens + (now - self._last) * self._rate)
            self._last = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
            else:
                wait = (1.0 - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._tokens = 0.0
                self._last = time.monotonic()

    async def pause(self, seconds: int = 5) -> None:
        """Freeze all workers for `seconds`. Only the first caller triggers the sleep."""
        async with self._pause_lock:
            if not self._pause_event.is_set():
                return  # already paused by another worker
            self._pause_event.clear()
            logger.warning(f"[429-freeze] pausing ALL workers for {seconds}s ...")
        await asyncio.sleep(seconds)
        logger.info("[429-freeze] resuming all workers")
        self._pause_event.set()


# ---------------------------------------------------------------------------
# Persistent productId→barcode cache
# ---------------------------------------------------------------------------

class BarcodeCache:
    """JSON-file cache: {productId: {"bc": barcode, "img": image_url}}.
    Shared across NW + PS runs. Legacy entries may be a bare barcode string
    (no image) — those are treated as missing an image so the detail call
    re-runs once and backfills the real image URL from the API."""

    def __init__(self, path: Path = CACHE_PATH) -> None:
        self.path = path
        self._data: dict[str, object] = {}
        if path.exists():
            try:
                self._data = json.loads(path.read_text())
            except Exception as e:
                logger.warning(f"cache read failed ({e}) — starting empty")
                self._data = {}
        logger.info(f"barcode cache: {len(self._data)} entries loaded from {path.name}")

    def get(self, product_id: str) -> Optional[str]:
        """Barcode (handles legacy string entries and new dict entries)."""
        v = self._data.get(product_id)
        if isinstance(v, dict):
            return v.get("bc")
        return v if isinstance(v, str) else None

    def get_image(self, product_id: str) -> Optional[str]:
        v = self._data.get(product_id)
        return v.get("img") if isinstance(v, dict) else None

    def get_detail(self, product_id: str) -> dict:
        """Cached STATIC rich fields (country/nutrition/category_path/unit) used as
        a fallback when this run's detail fetch fails. Never holds volatile price."""
        v = self._data.get(product_id)
        return (v.get("det") or {}) if isinstance(v, dict) else {}

    def needs_detail(self, product_id: str) -> bool:
        """True until we've fetched the static card (barcode + rich detail) once.
        Specials come from the listing, so detail is fetched cache-miss-only.

        'rich_checked' marks a card parsed by the current extractor. Entries cached
        before it existed are re-fetched exactly once, so they pick up 'description'
        whether their card was empty or already populated. After that a product is
        settled — including the genuinely bare ones Foodstuffs ships with no
        nutrition/origin/description at all, which must not re-fetch forever."""
        v = self._data.get(product_id)
        if not isinstance(v, dict):
            return True
        det = v.get("det") or {}
        if not v.get("bc") or not det:
            return True
        return not det.get("rich_checked")

    def needs_fetch(self, product_id: str) -> bool:
        """True if we haven't done a detail fetch yet (absent, or legacy
        barcode-only string entry). Once fetched, the entry is a dict and is
        settled even if the product has no image — avoids perpetual re-fetch."""
        v = self._data.get(product_id)
        if not isinstance(v, dict):
            return True
        return not v.get("bc")

    def put(self, product_id: str, barcode: str) -> None:
        """Barcode only (back-compat); preserves any cached image."""
        v = self._data.get(product_id)
        if isinstance(v, dict):
            v["bc"] = barcode
        else:
            self._data[product_id] = {"bc": barcode}

    def put_detail(self, product_id: str, barcode: Optional[str], image: Optional[str],
                   static: Optional[dict] = None) -> None:
        entry = self._data.get(product_id)
        if not isinstance(entry, dict):
            entry = {}
        if barcode:
            entry["bc"] = barcode
        if image:
            entry["img"] = image
        if static:  # static rich fields only (country/nutrition/category/unit)
            entry["det"] = static
        self._data[product_id] = entry

    def save(self) -> None:
        # Atomic write: dump to a temp file in the same dir, then os.replace() —
        # a crash mid-write can never corrupt or truncate the real cache file, so
        # the long one-time re-enrichment is safe to interrupt and resume.
        try:
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(json.dumps(self._data, ensure_ascii=False))
            os.replace(tmp, self.path)
            logger.info(f"barcode cache: saved {len(self._data)} entries")
        except Exception as e:
            logger.warning(f"cache save failed: {e}")


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class FoodstuffsScraper:
    def __init__(
        self,
        chain_key: str,
        branch_name: Optional[str] = None,
        branch_id: Optional[str] = None,
        category_slugs: Optional[list[str]] = None,
        headless: bool = True,
        dry_run: bool = False,
        cache: Optional[BarcodeCache] = None,
        proxy_url: Optional[str] = None,
        on_block: Optional[callable] = None,  # async callback called immediately on each block
        cf_state: Optional[CfState] = None,
        shared_browser: Optional[Browser] = None,
        rate_limiter: Optional[TokenBucketLimiter] = None,
        template_state: Optional[TemplateState] = None,
    ) -> None:
        if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
            raise RuntimeError(f"Missing Supabase env in {ENV_PATH}")
        if chain_key not in CHAINS:
            raise ValueError(f"Unknown chain: {chain_key}")

        self.chain_key = chain_key
        self.cfg = CHAINS[chain_key]
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        self.branch_name = branch_name or self.cfg["default_branch"]
        self.branch_id = branch_id
        self.api_store_id: Optional[str] = None
        self.chain_id: Optional[str] = None
        self._promo_date_cache = _PROMO_DATE_CACHE  # shared across branch workers

        slugs = category_slugs or self.cfg["categories"]
        self.category_urls = [f"{self.cfg['base_url']}/shop/category/{s}" for s in slugs]

        self.headless = headless
        self.dry_run = dry_run
        self.cache = cache if cache is not None else BarcodeCache()
        self.proxy_url = proxy_url
        self.on_block = on_block  # called as await on_block() on every block detection
        self._cf_state = cf_state
        self._shared_browser = shared_browser
        self._rate_limiter = rate_limiter
        self._template_state = template_state

        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self.blocks = 0  # Cloudflare challenge/block count for this branch
        self._api_headers: dict[str, str] = {}
        self._api_url: str = ""
        self._api_post_data: Optional[dict] = None
        self._cf_cookies: list[dict] = []  # cf_clearance cookies from nodriver pre-step
        self._cf_user_agent: Optional[str] = None  # UA nodriver used to solve Turnstile
        # Template saved after first successful browser category — reused for direct POSTs
        self._direct_template: Optional[dict] = None    # legacy single body (back-compat)
        self._direct_bodies: dict[str, dict] = {}       # per-category slug -> body (verbatim replay)
        self._direct_headers: dict[str, str] = {}
        self._direct_url: str = ""
        self._direct_template_store_id: str = ""
        self._token_request: Optional[dict] = None      # captured token-mint request
        self._token_seen_version: int = 0               # last refresh version this worker adopted
        self._captured_token_request: Optional[dict] = None  # discovered during the browser pass

    @property
    def _has_direct(self) -> bool:
        """True when this worker can serve categories without the browser."""
        return bool(self._direct_bodies or self._direct_template)

    def _load_direct_from_state(self) -> None:
        """Copy the shared template (per-category bodies + token-mint request) into this worker."""
        ts = self._template_state
        if not ts:
            return
        self._direct_bodies = {k: copy.deepcopy(v) for k, v in ts.bodies.items()}
        self._direct_template = copy.deepcopy(ts.body) if ts.body else None
        self._direct_url = ts.url
        self._direct_headers = dict(ts.headers)
        self._direct_template_store_id = ts.store_id
        self._token_request = ts.token_request
        self._token_seen_version = ts.token_version

    # ---- Browser / context ----------------------------------------------

    async def _start_browser(self) -> None:
        self._playwright = await async_playwright().start()
        launch_kwargs: dict = {
            "headless": self.headless,
            "args": [
                "--disable-http2",
                # "--disable-blink-features=AutomationControlled",  # not needed with patchright
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        }
        if self.proxy_url:
            launch_kwargs["proxy"] = _parse_proxy(self.proxy_url)
            logger.info(f"using proxy: {launch_kwargs['proxy'].get('server')}")
        self._browser = await self._playwright.chromium.launch(**launch_kwargs)

    async def _new_context(self) -> None:
        assert self._browser
        self._context = await self._browser.new_context(
            user_agent=self._cf_user_agent or USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="en-NZ",
            timezone_id="Pacific/Auckland",
            java_script_enabled=True,
            accept_downloads=False,
            extra_http_headers=EXTRA_HEADERS,
        )
        # await self._context.add_init_script(STEALTH_SCRIPT)  # triggers Runtime.enable — not needed with patchright
        if self._cf_cookies:
            await self._context.add_cookies(self._cf_cookies)
            logger.info(f"[cf] injected {len(self._cf_cookies)} CF cookies into browser context")
        self._page = await self._context.new_page()
        self._page.set_default_timeout(REQUEST_TIMEOUT_MS)
        # Bandwidth: block heavy assets we don't need (~70% smaller pages). Also piggy-back here
        # to capture the token-mint request (get-current-user) the first time we see it, so a
        # mid-run 401 can re-mint the Bearer. Same safe route path — no extra listeners.
        async def _asset_route(route, req):
            url = req.url or ""
            if (self._captured_token_request is None
                    and self._template_state is not None and not self._template_state.token_request
                    and "get-current-user" in url):
                try:
                    pb = req.post_data_json
                except Exception:
                    pb = None
                self._captured_token_request = {
                    "url": url, "method": req.method, "headers": dict(req.headers), "body": pb,
                }
                logger.info(f"[token] discovered mint endpoint: {req.method} {url}")
            if ASSET_BLOCK_RE.search(url):
                await route.abort()
            else:
                await route.continue_()
        await self._page.route("**/*", _asset_route)

    async def _refresh_context(self) -> None:
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
        await self._new_context()

    async def _close_browser(self) -> None:
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def _fetch_cf_clearance(self) -> None:
        self._cf_cookies, self._cf_user_agent = await _solve_cf_clearance(self.cfg)

    # ---- Branch resolution ----------------------------------------------

    def _resolve_branch(self) -> None:
        # READ-ONLY resolution: the scraper looks up chain/branch + api_store_id to
        # know which store to scrape, but must not WRITE. Chain/branch creation is
        # the importer's job. Create-if-missing upserts are commented out below.
        chain = (
            self.supabase.table("store_chains")
            .select("id").eq("slug", self.cfg["slug"]).execute().data
        )
        if not chain:
            # DATABASE WRITE DISABLED — do not create the chain row from the scraper.
            # chain = (
            #     self.supabase.table("store_chains")
            #     .upsert({"slug": self.cfg["slug"], "name": self.cfg["name"]}, on_conflict="slug")
            #     .execute().data
            # )
            logger.error(
                f"store_chains row missing for slug={self.cfg['slug']} — "
                f"cannot resolve chain (scraper no longer creates it). Aborting branch."
            )
            return
        self.chain_id = chain[0]["id"]

        if self.branch_id:
            r = (self.supabase.table("store_branches")
                 .select("id,name,api_store_id").eq("id", self.branch_id).execute().data)
            if r:
                self.branch_name = r[0]["name"]
                self.api_store_id = r[0].get("api_store_id")
            return

        r = (self.supabase.table("store_branches")
             .select("id,name,api_store_id")
             .eq("chain_id", self.chain_id).eq("name", self.branch_name)
             .execute().data)
        if r:
            self.branch_id = r[0]["id"]
            self.api_store_id = r[0].get("api_store_id")
            return

        # DATABASE WRITE DISABLED — do not create the branch row from the scraper.
        # r = (self.supabase.table("store_branches")
        #      .upsert({"chain_id": self.chain_id, "name": self.branch_name},
        #              on_conflict="chain_id,name").execute().data)
        # self.branch_id = r[0]["id"]
        # self.api_store_id = r[0].get("api_store_id")
        logger.error(
            f"store_branches row missing for {self.branch_name!r} (chain {self.chain_id}) — "
            f"scraper no longer creates it; branch unresolved, will be skipped."
        )

    # ---- Random delay ----------------------------------------------------

    async def _random_delay(self) -> None:
        await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    async def _scroll_to_bottom(self) -> None:
        assert self._page
        prev = 0
        for _ in range(8):  # cap at 8 — page fires API request well before full scroll
            h = await self._page.evaluate("document.body.scrollHeight")
            if h == prev:
                break
            await self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await self._page.wait_for_timeout(400)
            prev = h

    # ---- Scrape one category --------------------------------------------

    async def scrape_one_category(self, url: str) -> tuple[list[ScrapedProduct], bool]:
        """Returns (products, did_paginate)."""
        assert self._page
        captured: list[dict] = []
        self._api_headers = {}
        self._api_url = ""
        self._api_post_data = None
        did_paginate = False
        # Initialise upfront so early-return paths (block detection) can reference it.
        products: list[ScrapedProduct] = []

        async def intercept(route, request):
            if "paginated/products" in request.url:
                if not self._api_headers:
                    self._api_headers = dict(request.headers)
                if not self._api_url:
                    self._api_url = request.url
                    try:
                        self._api_post_data = request.post_data_json
                    except Exception:
                        self._api_post_data = None
            response = await route.fetch()
            try:
                body = await response.json()
                captured.append({"url": request.url, "data": body})
            except Exception:
                pass
            await route.fulfill(response=response)

        await self._page.route("**/paginated/products**", intercept)

        category = category_from_url(url)
        logger.info(f"  → {url}  [{category}]")

        try:
            await self._random_delay()
            if self._rate_limiter:
                await self._rate_limiter.acquire()
            nav_resp = await self._page.goto(url, wait_until="load", timeout=60_000)
            # 425 Too Early — TLS 0-RTT early data rejected (RFC 8470). Re-navigate over the
            # now-warm connection (no early data on the retry). Quick, before block handling.
            for _attempt_425 in range(3):
                if not (nav_resp and nav_resp.status == 425):
                    break
                logger.warning(f"  [425] Too Early on {url} — re-navigating ({_attempt_425 + 1}/3)")
                await asyncio.sleep(0.5 * (_attempt_425 + 1))
                nav_resp = await self._page.goto(url, wait_until="load", timeout=60_000)
            # Cloudflare block detection
            if nav_resp and nav_resp.status in (403, 429, 503):
                self.blocks += 1
                logger.warning(f"  [block] HTTP {nav_resp.status} on {url} — Cloudflare block")
                if nav_resp.status == 429:
                    headers = dict(nav_resp.headers)
                    retry_after = headers.get("retry-after") or headers.get("x-ratelimit-reset")
                    rate_limit = headers.get("x-ratelimit-limit")
                    remaining = headers.get("x-ratelimit-remaining")
                    logger.warning(
                        f"  [429] retry-after={retry_after}  limit={rate_limit}  remaining={remaining}  "
                        f"all-headers={list(headers.keys())}"
                    )
                    if retry_after and retry_after.isdigit():
                        ra = int(retry_after)
                        if ra > 60:
                            # Long-term IP ban — signal to skip all retries for this category
                            self._long_ban_active = True
                            pause_secs = 30  # brief global pause to let other workers notice
                        else:
                            self._long_ban_active = False
                            pause_secs = ra
                    else:
                        self._long_ban_active = False
                        pause_secs = 5
                    if self._rate_limiter:
                        asyncio.create_task(self._rate_limiter.pause(pause_secs))
                if self.on_block:
                    try:
                        await self.on_block()
                    except Exception:
                        pass
                return products, did_paginate
            try:
                page_title = (await self._page.title()).lower()
                if any(kw in page_title for kw in _CF_BLOCK_TITLES):
                    self.blocks += 1
                    logger.warning(f"  [block] challenge page '{page_title}' on {url}")
                    if self.on_block:
                        try:
                            await self.on_block()
                        except Exception:
                            pass
                    return products, did_paginate
            except Exception:
                pass
            await self._page.wait_for_timeout(2000)
            await self._scroll_to_bottom()
            await self._page.wait_for_timeout(500)

            # Step 1: pin to our branch by overriding storeId in the POST body.
            # (No fetch here — Step 2 does all paging authoritatively against the pinned body.)
            if self.api_store_id and self._api_post_data:
                store_field = next(
                    (f for f in ("storeId", "store_id") if f in self._api_post_data), None
                )
                old_id = self._api_post_data.get(store_field) if store_field else None
                if store_field and old_id != self.api_store_id:
                    self._api_post_data[store_field] = self.api_store_id
                    alg = self._api_post_data.get("algoliaQuery") or {}
                    if "filters" in alg and old_id and old_id in alg["filters"]:
                        alg["filters"] = alg["filters"].replace(old_id, self.api_store_id)

            # Step 2: fetch ALL pages authoritatively. The API is 0-indexed — page 0 is the
            # first page and `totalPages` is the page COUNT, so valid pages are 0..totalPages-1.
            # We discard whatever the browser happened to capture (it may be the pre-substitution
            # store, or a partial scroll) and re-fetch every page via the pinned body, so the
            # result matches the website exactly with no missing first page and no duplicates.
            if self._api_url and self._api_headers and self._api_post_data:
                captured[:] = [e for e in captured if "paginated/products" not in e.get("url", "")]
                headers = {k: v for k, v in self._api_headers.items()
                           if k.lower() not in ("host", "content-length")}

                # Page 0 first → gives the true page count for this store+category.
                total_pages = 1
                try:
                    body = copy.deepcopy(self._api_post_data); body["page"] = 0
                    if self._rate_limiter:
                        await self._rate_limiter.acquire()
                    resp = await self._post_with_425_retry(self._api_url, headers, body)
                    if not resp.ok and resp.status == 500:
                        await asyncio.sleep(3)
                        resp = await self._post_with_425_retry(self._api_url, headers, body)
                    if resp.ok:
                        d0 = await resp.json()
                        captured.append({"url": self._api_url, "data": d0})
                        total_pages = d0.get("totalPages") or 1
                    else:
                        logger.warning(f"    page 0: HTTP {resp.status} — stopping")
                except Exception as e:
                    logger.warning(f"    page 0: {e} — stopping")

                # Remaining pages 1..totalPages-1
                if total_pages > 1:
                    did_paginate = True
                    logger.info(f"    pagination: {total_pages} pages")
                    for page_num in range(1, total_pages):
                        try:
                            body = copy.deepcopy(self._api_post_data); body["page"] = page_num
                            if self._rate_limiter:
                                await self._rate_limiter.acquire()
                            resp = await self._post_with_425_retry(self._api_url, headers, body)
                            if resp.ok:
                                captured.append({"url": self._api_url, "data": await resp.json()})
                            elif resp.status == 500:
                                await asyncio.sleep(3)
                                resp2 = await self._post_with_425_retry(self._api_url, headers, body)
                                if resp2.ok:
                                    captured.append({"url": self._api_url, "data": await resp2.json()})
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

        # Step 3: parse (products list initialised at top of function)
        for entry in captured:
            data = entry.get("data") or {}
            items = (
                data.get("products") or data.get("items")
                or (data.get("pageProps") or {}).get("products") or []
            )
            for item in items:
                p = self._parse_item(item, category)
                if p:
                    products.append(p)
        logger.info(f"    parsed {len(products)} products from {len(captured)} responses")

        # Save the working request body for THIS category so other branches replay it verbatim
        # (no category0SI/field parsing → robust to the per-region query encoding). One body per
        # category slug.
        if products and self._api_post_data and self._api_url and self._api_headers:
            slug = url.rstrip("/").split("/")[-1]
            hdrs = {k: v for k, v in self._api_headers.items()
                    if k.lower() not in ("host", "content-length")}
            self._direct_bodies[slug] = copy.deepcopy(self._api_post_data)
            self._direct_url = self._api_url
            self._direct_headers = hdrs
            self._direct_template_store_id = self.api_store_id or ""
            if self._template_state:
                self._template_state.save_category(
                    slug, self._direct_bodies[slug], self._direct_url, hdrs,
                    self._direct_template_store_id,
                )
        # Persist the captured token-mint request (once) for mid-run Bearer refresh.
        if self._captured_token_request and self._template_state and not self._template_state.token_request:
            self._template_state.save_token_request(self._captured_token_request)

        return products, did_paginate

    async def _post_with_425_retry(self, url: str, headers: dict, data: dict, attempts: int = 4):
        """POST that retries HTTP 425 'Too Early'.

        Cloudflare rejects TLS 1.3 0-RTT early data with 425 (RFC 8470). Early data only
        rides the FIRST request on a resumed TLS connection; resending over the now-warm
        1-RTT connection succeeds. Surfaces most from a same-region (e.g. NZ) IP, where Chrome
        is more likely to have a session ticket for the local Cloudflare edge.
        """
        resp = None
        for i in range(attempts):
            resp = await self._page.request.post(url, headers=headers, data=data)
            if resp.status != 425:
                return resp
            logger.warning(f"  [425] Too Early on {url} — retry {i + 1}/{attempts} over warm connection")
            await asyncio.sleep(0.4 * (i + 1))
        return resp

    async def _refresh_token(self) -> bool:
        """Re-mint the anonymous Bearer by replaying the captured token-mint request. That endpoint
        is a www BFF route needing a live cf_clearance, so we refresh CF clearance first and let the
        context's live cookie ride the call (the captured cookie is stale). One lightweight www /api
        call — NOT the per-category shop-page nav that caused bans. Shared across workers via the
        lock + version so concurrent 401s trigger at most one refresh. Never deletes the bodies."""
        ts = self._template_state
        if ts is None or not ts.token_request:
            return False
        async with ts._lock:
            # Another worker may have refreshed while we waited on the lock — adopt theirs.
            if ts.token_version != self._token_seen_version and ts.headers.get("authorization"):
                self._direct_headers = {**self._direct_headers, "authorization": ts.headers["authorization"]}
                self._token_seen_version = ts.token_version
                return True
            # Ensure a live cf_clearance and inject it — direct workers skipped the CF solve.
            try:
                if self._cf_state:
                    await self._cf_state.ensure_fresh(self.cfg)
                    self._cf_cookies = list(self._cf_state.cookies)
                if self._cf_cookies and self._context:
                    await self._context.add_cookies(self._cf_cookies)
            except Exception as e:
                logger.warning(f"[token] could not refresh CF clearance for mint call: {e}")
            tr = ts.token_request
            # Drop volatile/stale headers so the context's LIVE cf_clearance & tracing are used.
            _volatile = {"cookie", "traceparent", "tracestate", "newrelic", "content-length", "host"}
            hdrs = {k: v for k, v in (tr.get("headers") or {}).items() if k.lower() not in _volatile}
            try:
                kwargs = {"method": tr.get("method", "POST"), "headers": hdrs}
                if tr.get("body") is not None:
                    kwargs["data"] = tr["body"]
                resp = await self._page.request.fetch(tr["url"], **kwargs)
            except Exception as e:
                logger.warning(f"[token] refresh request failed: {e}")
                return False
            if not resp.ok:
                logger.warning(f"[token] refresh got HTTP {resp.status} — cannot renew Bearer")
                return False
            try:
                new_token = _find_jwt(await resp.json())
            except Exception:
                new_token = None
            if not new_token:
                logger.warning("[token] refresh response had no JWT — cannot renew")
                return False
            ts.set_token(new_token)
            self._token_seen_version = ts.token_version
            self._direct_headers = {**self._direct_headers, "authorization": ts.headers["authorization"]}
            logger.info("[token] Bearer refreshed (single www /api call) — retrying direct POST")
            return True

    async def _scrape_category_direct(self, url: str) -> Optional[list[ScrapedProduct]]:
        """Skip browser navigation — POST directly using the saved template.

        Returns a product list on success, or None if a fallback to browser is needed.
        """
        if not self._has_direct or not self._direct_url or not self._direct_headers:
            return None

        slug = url.rstrip("/").split("/")[-1]

        if slug in self._direct_bodies:
            # Preferred path: replay the exact captured body for this category, swap store only.
            # No field parsing → works whatever field the (region-specific) bundle used.
            body = copy.deepcopy(self._direct_bodies[slug])
            _swap_store_in_body(body, self._direct_template_store_id, self.api_store_id)
        elif self._direct_template:
            # Legacy single-body template (old on-disk format): mutate category0SI in filters.
            display_name = SLUG_TO_DISPLAY_NAME.get(slug)
            if not display_name:
                return None
            body = copy.deepcopy(self._direct_template)
            _swap_store_in_body(body, self._direct_template_store_id, self.api_store_id)
            alg = body.get("algoliaQuery")
            if not alg or "filters" not in alg:
                logger.warning(f"  [direct] {url} → algoliaQuery/filters missing in template — browser fallback")
                return None
            if 'category0SI:"' not in alg["filters"]:
                logger.warning(f"  [direct] {url} → category0SI not found in filters — browser fallback")
                return None
            alg["filters"] = re.sub(r'category0SI:"[^"]*"', f'category0SI:"{display_name}"', alg["filters"])
        else:
            return None  # no captured body for this category yet — browser fallback

        body["page"] = 0  # API is 0-indexed — page 0 is the first page

        category = category_from_url(url)
        try:
            if self._rate_limiter:
                await self._rate_limiter.acquire()
            resp = await self._post_with_425_retry(
                self._direct_url, self._direct_headers, body
            )
            if resp.status in (401, 403):
                # Token expired. Re-mint the Bearer via the captured token request and retry ONCE.
                # The per-category bodies are NOT deleted.
                if await self._refresh_token():
                    resp = await self._post_with_425_retry(
                        self._direct_url, self._direct_headers, body
                    )
            if not resp.ok:
                if resp.status == 429 or resp.status >= 500:
                    raise Exception(f"HTTP {resp.status} on direct POST")
                if resp.status in (401, 403):
                    logger.warning(f"  [direct] {url} → HTTP {resp.status} — token refresh unavailable/failed, browser fallback")
                    return None
                logger.warning(f"  [direct] {url} → HTTP {resp.status}")
                return None
            rj = await resp.json()
        except Exception as e:
            logger.warning(f"  [direct] {url} → {e}")
            raise  # re-raise so retry loop in _scrape_branch handles it

        total_pages = rj.get("totalPages") or 1  # page COUNT; valid pages are 0..total_pages-1
        all_responses = [rj]

        for page_num in range(1, total_pages):
            pb = copy.deepcopy(body)
            pb["page"] = page_num
            try:
                if self._rate_limiter:
                    await self._rate_limiter.acquire()
                r2 = await self._post_with_425_retry(
                    self._direct_url, self._direct_headers, pb
                )
                if r2.ok:
                    all_responses.append(await r2.json())
                else:
                    logger.warning(f"  [direct] page {page_num}: HTTP {r2.status} — stopping")
                    break
            except Exception as e:
                logger.warning(f"  [direct] page {page_num}: {e} — stopping")
                break
            await asyncio.sleep(0.4)

        products: list[ScrapedProduct] = []
        seen_ids: set[str] = set()
        for data in all_responses:
            items = (
                data.get("products") or data.get("items")
                or (data.get("pageProps") or {}).get("products") or []
            )
            for item in items:
                p = self._parse_item(item, category)
                if p:
                    products.append(p)
                    if p.product_id:
                        seen_ids.add(p.product_id)

        if not products:
            logger.warning(f"  [direct] {url} → 0 products — browser fallback")
            return None

        recovered_label = ""
        if total_pages >= ALGOLIA_PAGE_CAP:
            recovered = await self._recover_capped_category(url, body, rj, category, seen_ids)
            if recovered:
                products.extend(recovered)
                recovered_label = f" (+{len(recovered)} beyond 1000-hit cap)"

        pages_label = f"{total_pages}p" if total_pages > 1 else ""
        logger.info(f"  [direct] {url}  {len(products)} products {pages_label}{recovered_label}".rstrip())
        return products

    async def _recover_capped_category(
        self, url: str, base_body: dict, page0_response: dict, category: str, seen_ids: set[str],
    ) -> list[ScrapedProduct]:
        """A category query that lands on exactly ALGOLIA_PAGE_CAP pages means Algolia
        stopped paginating at its 1000-hit ceiling — the same limit the live website
        hits. If the category0SI facet counts sum to more than 1000, the remainder is
        invisible to plain pagination but reachable by adding a category1SI filter
        (Algolia scopes each facet count independently of the pagination cap)."""
        facets = (page0_response.get("facets") or {}).get("category1SI") or {}
        asr_facets = ((page0_response.get("algoliaSearchResult") or {}).get("facets") or {}).get("category1SI") or {}
        if not facets and asr_facets:
            facets = asr_facets
        if not facets:
            import json as _json
            dbg_path = Path("/tmp/claude-1000/-home-boiledpotato-Downloads-scrapers/fba76523-0f9e-44bf-ba9f-4edf5db328bd/scratchpad/page0_debug.json")
            asr = page0_response.get("algoliaSearchResult") or {}
            summary = {
                "totalHits": page0_response.get("totalHits"),
                "totalPages": page0_response.get("totalPages"),
                "hitsPerPage": page0_response.get("hitsPerPage"),
                "top_keys": list(page0_response.keys()),
                "algoliaSearchResult_keys": list(asr.keys()) if isinstance(asr, dict) else str(type(asr)),
                "algoliaSearchResult_no_hits": {k: v for k, v in asr.items() if k != "hits"} if isinstance(asr, dict) else None,
            }
            dbg_path.write_text(_json.dumps(summary, indent=2, default=str))
            logger.warning(
                f"  [direct] {url} → capped at {ALGOLIA_PAGE_CAP * 50} hits, "
                f"no category1SI facet to split by — some products may be missing "
                f"(debug dump: {dbg_path}, totalHits={page0_response.get('totalHits')})"
            )
            return []

        base_filters = base_body["algoliaQuery"]["filters"]
        recovered: list[ScrapedProduct] = []
        for subcat, count in facets.items():
            if not count:
                continue
            sub_products = await self._fetch_filtered_category(
                url, base_body, base_filters, "category1SI", subcat, category
            )
            if len(sub_products) < count:
                # category1SI is multi-valued (a product can sit in >1 bucket), so this
                # is NOT double-counted against `count` — a real shortfall here means a
                # page in this bucket failed mid-pagination (see the warnings below).
                logger.warning(f"  [direct] {url} bucket '{subcat}': got {len(sub_products)}/{count} — possible shortfall of {count - len(sub_products)}")
            for p in sub_products:
                if p.product_id and p.product_id not in seen_ids:
                    seen_ids.add(p.product_id)
                    recovered.append(p)
        if recovered:
            logger.info(
                f"  [direct] {url} → recovered {len(recovered)} additional products "
                f"beyond the 1000-hit cap via category1SI split"
            )
        return recovered

    async def _fetch_filtered_category(
        self, url: str, base_body: dict, base_filters: str,
        facet_field: str, facet_value: str, category: str, depth: int = 0,
    ) -> list[ScrapedProduct]:
        """Re-run the category query with an extra `facet_field:"facet_value"` filter
        appended, paginating fully. If this narrower slice is STILL capped at 1000
        (a very large subcategory), recurse one more level by `brand` facet — depth
        is capped at 1 to bound the fan-out."""
        body = copy.deepcopy(base_body)
        body["algoliaQuery"]["filters"] = f'{base_filters} AND {facet_field}:"{facet_value}"'
        body["page"] = 0
        try:
            if self._rate_limiter:
                await self._rate_limiter.acquire()
            resp = await self._post_with_425_retry(self._direct_url, self._direct_headers, body)
            if not resp.ok:
                return []
            rj = await resp.json()
        except Exception as e:
            logger.warning(f"  [direct] {url} [{facet_field}={facet_value}]: {e} — skipping")
            return []

        total_pages = rj.get("totalPages") or 1
        all_responses = [rj]
        for page_num in range(1, total_pages):
            pb = copy.deepcopy(body)
            pb["page"] = page_num
            try:
                if self._rate_limiter:
                    await self._rate_limiter.acquire()
                r2 = await self._post_with_425_retry(self._direct_url, self._direct_headers, pb)
                if r2.ok:
                    all_responses.append(await r2.json())
                else:
                    logger.warning(f"  [direct] {facet_field}={facet_value} page {page_num}/{total_pages}: HTTP {r2.status} — truncating bucket here")
                    break
            except Exception as e:
                logger.warning(f"  [direct] {facet_field}={facet_value} page {page_num}/{total_pages}: {e} — truncating bucket here")
                break
            await asyncio.sleep(0.4)

        products: list[ScrapedProduct] = []
        seen_here: set[str] = set()
        for data in all_responses:
            items = (
                data.get("products") or data.get("items")
                or (data.get("pageProps") or {}).get("products") or []
            )
            for item in items:
                p = self._parse_item(item, category)
                if p:
                    products.append(p)
                    if p.product_id:
                        seen_here.add(p.product_id)

        if total_pages >= ALGOLIA_PAGE_CAP and depth == 0:
            brand_facets = (rj.get("facets") or {}).get("brand") or {}
            sub_filters = body["algoliaQuery"]["filters"]
            for brand, count in brand_facets.items():
                if not count:
                    continue
                brand_products = await self._fetch_filtered_category(
                    url, base_body, sub_filters, "brand", brand, category, depth=1
                )
                for p in brand_products:
                    if p.product_id and p.product_id not in seen_here:
                        seen_here.add(p.product_id)
                        products.append(p)

        return products

    def _parse_item(self, item: dict, category: str) -> Optional[ScrapedProduct]:
        clean_name = item.get("name") or None
        display_name = item.get("displayName") or None  # size/weight string e.g. "200g"
        brand = item.get("brand") or None

        parts = [p for p in [brand, clean_name, display_name] if p]
        raw_name = " ".join(parts) if parts else (item.get("title") or "")
        if not raw_name:
            return None

        sp = item.get("singlePrice") or {}
        # Foodstuffs price values are in cents
        try:
            price_cents = sp.get("price") or sp.get("originalPrice") or 0
            price = float(price_cents) / 100
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        # Specials come from the listing 'promotions' array (per-store, fresh) —
        # NOT from the detail page. singlePrice.price is the regular price; the
        # promotion's rewardValue (cents) is the deal price.
        special = comparison = promo_type = promo_text = loyalty_code = None
        card_required = False
        source_promotion_id = multibuy_quantity = multibuy_price_cents = None
        promo_starts_at = promo_ends_at = None
        promo_metadata: dict = {}
        promos = item.get("promotions") or []
        if promos:
            promo = next((p for p in promos if p.get("bestPromotion")), promos[0])
            reward_type = promo.get("rewardType")
            reward_value = promo.get("rewardValue")
            threshold = promo.get("threshold") or 1
            card_required = bool(promo.get("cardDependencyFlag"))
            if promo.get("promoId") is not None:
                source_promotion_id = str(promo.get("promoId"))
            # Badge / campaign metadata — kept for classification + audit, not price maths.
            for k in ("decal", "sapType", "rewardType", "limit", "description"):
                v = promo.get(k)
                if v is not None and v != "":
                    promo_metadata[k] = v
            # Full promo object kept for audit — the fixed whitelist above would
            # silently drop any field FS adds later.
            promo_metadata["raw_promotion"] = promo
            # NOTE: the listing promo object carries NO dates (confirmed against a
            # live response 2026-07-16). promo_starts_at/promo_ends_at are filled
            # later from the DETAIL response's promotionList[], looked up by
            # promoId — see _apply_promo_dates / enrich_barcodes.
            try:
                rv = float(reward_value) / 100 if reward_value else None
            except (TypeError, ValueError):
                rv = None
            if threshold > 1 and rv:
                # multibuy: rewardValue is the total price for `threshold` units.
                # Current price stays the regular single-buy price; the deal is
                # carried as structured multibuy fields.
                promo_type = "multibuy_fixed_price"
                promo_text = f"{threshold} for ${rv:.2f}"
                multibuy_quantity = int(threshold)
                multibuy_price_cents = int(round(rv * 100))
            elif reward_type == "NEW_PRICE" and rv is not None and rv < price:
                special = rv
                comparison = price                 # singlePrice.price = regular
                promo_type = "member_price" if card_required else "special"
                promo_text = f"Club Deal ${rv:.2f}" if card_required else f"Special ${rv:.2f}"
            elif rv is not None and rv < price:
                special = rv
                comparison = price
                promo_type = "special"
                promo_text = f"Special ${rv:.2f}"
            if card_required and promo_type:
                loyalty_code = "club-plus"
        if promo_type is None:
            # No classified promotion — don't attach stray promo id/badges/multibuy.
            source_promotion_id = multibuy_quantity = multibuy_price_cents = None
            promo_starts_at = promo_ends_at = None
            promo_metadata = {}

        # Unit price + label (free from the listing's comparativePrice)
        comp = sp.get("comparativePrice") or {}
        unit_price = None
        try:
            ppu = comp.get("pricePerUnit")
            if ppu:
                unit_price = float(ppu) / 100
        except (TypeError, ValueError):
            unit_price = None
        unit_label = comp.get("measureDescription") or comp.get("unitQuantityUom") or None

        # Full category hierarchy (listing carries categoryTrees too)
        category_path = category_path_from_trees(item.get("categoryTrees"))

        # Stock
        avail = (item.get("availability") or item.get("stockLevel") or "").lower() if isinstance(
            item.get("availability") or item.get("stockLevel"), str
        ) else ""
        in_stock = "out" not in avail
        sl = item.get("stockLevel")
        if isinstance(sl, (int, float)) and sl == 0:
            in_stock = False

        return ScrapedProduct(
            raw_name=raw_name,
            clean_name=clean_name or raw_name,
            price=price,
            category=category,
            special_price=special,
            image_url=(
                item.get("images", {}).get("main")
                if isinstance(item.get("images"), dict)
                else item.get("imageUrl")
            ),
            brand=brand,
            weight=display_name,
            in_stock=in_stock,
            product_id=str(item.get("productId") or item.get("id") or "") or None,
            unit_price=unit_price,
            unit_label=unit_label,
            category_path=category_path,
            comparison_price=comparison,
            promo_type=promo_type,
            promo_text=promo_text,
            card_required=card_required,
            loyalty_program_code=loyalty_code,
            source_promotion_id=source_promotion_id,
            multibuy_quantity=multibuy_quantity,
            multibuy_price_cents=multibuy_price_cents,
            promo_starts_at=promo_starts_at,
            promo_ends_at=promo_ends_at,
            promo_metadata=promo_metadata,
        )

    # ---- Parallel barcode enrichment with adaptive concurrency ----------

    async def enrich_barcodes(self, products: list[ScrapedProduct]) -> dict:
        """Looks up barcodes for unique product_ids in parallel.
        Adaptive: starts at ENRICH_CONCURRENCY_INITIAL, downscales on 429s.
        Uses persistent cache."""
        stats = {"requested": 0, "from_cache": 0, "fetched": 0, "missing": 0, "rate_limited": 0}
        if not self.api_store_id:
            logger.warning("  [barcode] api_store_id missing — skipping enrichment")
            return stats

        unique_pids: set[str] = {p.product_id for p in products if p.product_id}
        stats["requested"] = len(unique_pids)

        # Detail provides STATIC fields only: barcode, image, and the product
        # card (country/nutrition/category). Specials come from the listing's
        # 'promotions' (parsed in _parse_item), so detail is fetched cache-miss
        # only — fetch once per product, then served from cache.
        to_fetch: list[str] = [pid for pid in unique_pids if self.cache.needs_detail(pid)]

        # Promo dates come ONLY from the detail response, but detail is otherwise
        # fetched cache-miss only (1.7M cached / 501 fetched on the last run), so
        # promo products would essentially never re-fetch and dates would stay
        # null. Force a fetch for promos whose dates we don't have yet — but only
        # ONE product per promoId, not one per product: a single FS promoId spans
        # many products (promoId 130294910 covers 13 Bluebird SKUs) and
        # promotionClass is MASS, i.e. national. That turns ~300k fetches into one
        # per distinct deal. Already-queued pids are reused rather than refetched.
        queued = set(to_fetch)
        want_dates: dict[str, str] = {}  # promoId -> representative product_id
        for p in products:
            pid, promo_id = p.product_id, p.source_promotion_id
            if not pid or not promo_id or promo_id in self._promo_date_cache:
                continue
            if pid in queued:
                want_dates.setdefault(promo_id, pid)   # already fetching it anyway
            else:
                want_dates.setdefault(promo_id, pid)
        extra = [pid for pid in dict.fromkeys(want_dates.values()) if pid not in queued]
        to_fetch.extend(extra)
        logger.info(
            f"  [detail] {len(unique_pids) - len(to_fetch)}/{len(unique_pids)} cached, "
            f"{len(to_fetch)} need API fetch (barcode/image/card"
            f"{f'; +{len(extra)} for {len(want_dates)} promo dates' if extra else ''})"
        )

        # Adaptive parallel fetch. detail_map[pid] = parsed fresh detail this run.
        detail_map: dict[str, dict] = {}
        if to_fetch:
            current_concurrency = ENRICH_CONCURRENCY_INITIAL
            consecutive_429 = 0
            i = 0
            assert self._page
            headers = {
                k: v for k, v in (self._api_headers or self._direct_headers).items()
                if k.lower() not in ("host", "content-length")
            }
            t0 = time.time()
            while i < len(to_fetch):
                batch = to_fetch[i : i + current_concurrency]
                i += len(batch)

                async def fetch_one(pid: str) -> tuple[str, Optional[dict], int]:
                    url = BARCODE_API.format(store_id=self.api_store_id, pid=pid)
                    try:
                        assert self._page
                        resp = await self._page.request.get(url, headers=headers)
                        if resp.ok:
                            data = await resp.json()
                            return pid, self._parse_detail(data), resp.status
                        return pid, None, resp.status
                    except Exception:
                        return pid, None, 0

                results = await asyncio.gather(*[fetch_one(pid) for pid in batch])
                batch_429 = 0
                for pid, det, status in results:
                    if det is not None:
                        detail_map[pid] = det
                        self.cache.put_detail(pid, det.get("barcode"), det.get("image"),
                                              static=det.get("static_cache"))
                        stats["fetched"] += 1
                    elif status == 429:
                        batch_429 += 1
                        stats["rate_limited"] += 1
                    else:
                        stats["missing"] += 1
                if batch_429 >= ENRICH_429_THRESHOLD:
                    consecutive_429 += 1
                    new_c = max(ENRICH_CONCURRENCY_MIN, current_concurrency // 2)
                    if new_c != current_concurrency:
                        logger.warning(
                            f"  [detail] {batch_429} 429s in batch → concurrency "
                            f"{current_concurrency} → {new_c}"
                        )
                        current_concurrency = new_c
                    await asyncio.sleep(2 + consecutive_429)
                else:
                    consecutive_429 = 0
            dt = time.time() - t0
            logger.info(
                f"  [detail] fetched {stats['fetched']}/{len(to_fetch)} via API "
                f"in {dt:.1f}s (concurrency ended at {current_concurrency})"
            )

        # Harvest promo dates from everything fetched this run (any detail may
        # carry promotionList, not just the ones fetched for dates) into the
        # run-level, promoId-keyed cache shared across every branch.
        for det in detail_map.values():
            for promo_id, dates in (det.get("promo_dates") or {}).items():
                self._promo_date_cache.setdefault(promo_id, dates)

        # Apply STATIC fields to products: fresh detail this run, else cache
        # fallback. Specials are already set from the listing in _parse_item.
        applied = imaged = 0
        for p in products:
            if not p.product_id:
                continue
            det = detail_map.get(p.product_id)
            if det:
                if det.get("barcode"):
                    p.barcode = det["barcode"]; applied += 1
                if det.get("image"):
                    p.image_url = det["image"]; imaged += 1
                self._apply_detail(p, det)
            else:
                bc = self.cache.get(p.product_id)
                if bc:
                    p.barcode = bc; applied += 1
                img = self.cache.get_image(p.product_id)
                if img:
                    p.image_url = img; imaged += 1
                self._apply_static(p, self.cache.get_detail(p.product_id))

        # Stamp promo dates by promoId. A product only gets dates if it actually
        # carries that promo, so non-promo products stay clean.
        dated = 0
        for p in products:
            if p.source_promotion_id and not p.promo_starts_at:
                d = self._promo_date_cache.get(p.source_promotion_id)
                if d:
                    p.promo_starts_at, p.promo_ends_at = d.get("start"), d.get("end")
                    dated += 1
        if dated or self._promo_date_cache:
            logger.info(f"  [promo] dated {dated} product(s) from "
                        f"{len(self._promo_date_cache)} known promo(s)")
        stats["from_cache"] = stats["requested"] - stats["fetched"]
        specialed = sum(1 for p in products if p.special_price)
        carded = sum(1 for p in products if p.card_required)
        logger.info(
            f"  [detail] applied: {applied} barcode, {imaged} image, "
            f"{specialed} special (listing), {carded} member-card"
        )
        return stats

    @staticmethod
    def _parse_detail(data: dict) -> dict:
        """Parse a Foodstuffs detail response into the fields the JSONL needs.
        Separates volatile (member/non-member price) from static-cacheable."""
        sku = data.get("sku")
        # comparativePricePerUnit is cents; measure description is the unit label
        upu = data.get("comparativePricePerUnit")
        unit_price = (float(upu) / 100) if isinstance(upu, (int, float)) else None
        unit_label = data.get("comparativeUnitMeasureDescription") or None
        category_path = category_path_from_trees(data.get("categoryTrees"))
        rich = extract_fs_detail(data)
        static_cache = {
            "rich": rich,
            # We parsed a real 200 response. Some products are shipped bare by
            # Foodstuffs (no nutrition/origin/description at all) — record that we
            # looked, so an empty 'rich' isn't mistaken for "never fetched".
            "rich_checked": True,
            "unit_price": unit_price,
            "unit_label": unit_label,
            "category_path": category_path,
        }
        # Promo dates live ONLY on the detail response, under promotionList[] —
        # the listing's `promotions` object has no date field at all, which is why
        # every FS promotion imported with starts_at/ends_at NULL. Keyed by
        # promoId (the STRING form: Pak'nSave sends promotionId=404901247 as an
        # int but promoId="0404901247" WITH a leading zero, and the listing gives
        # us the string form — matching on the int would miss every PnS promo).
        # Deliberately NOT in static_cache: that cache is keyed per product and
        # effectively permanent (barcodes/images never change), but promo dates
        # roll over weekly, so caching them there would serve dead dates forever.
        promo_dates: dict[str, dict] = {}
        for promo in (data.get("promotionList") or []):
            if not isinstance(promo, dict):
                continue
            pid = promo.get("promoId")
            start, end = promo.get("startDate"), promo.get("endDate")
            if pid and (start or end):
                promo_dates[str(pid)] = {"start": start, "end": end,
                                         "suspended": bool(promo.get("suspended"))}
        return {
            "barcode": str(sku) if sku else None,
            "image": extract_fs_image(data),
            "member_price": data.get("price"),               # cents (loyalty/club)
            "nonmember_price": data.get("nonLoyaltyCardPrice"),  # cents (non-member)
            "unit_price": unit_price,
            "unit_label": unit_label,
            "category_path": category_path,
            "rich": static_cache["rich"],
            "promo_dates": promo_dates,
            "static_cache": static_cache,
        }

    def _apply_detail(self, p: "ScrapedProduct", det: dict) -> None:
        """Apply a freshly fetched detail dict — STATIC fields only. Specials are
        parsed from the listing 'promotions', not here."""
        self._apply_static(p, {
            "unit_price": det.get("unit_price"), "unit_label": det.get("unit_label"),
            "category_path": det.get("category_path"), "rich": det.get("rich"),
        })

    @staticmethod
    def _apply_static(p: "ScrapedProduct", st: dict) -> None:
        """Apply static fields (from fresh detail or cache fallback)."""
        if not st:
            return
        if st.get("unit_price") is not None:
            p.unit_price = st["unit_price"]
        if st.get("unit_label"):
            p.unit_label = st["unit_label"]
        if st.get("category_path"):
            p.category_path = st["category_path"]
        if st.get("rich"):
            p.detail = st["rich"]

    # ---- JSONL export (Scraper Data Contract — replaces DB writes) --------

    def _build_observation(self, p: "ScrapedProduct") -> Optional[dict]:
        """Map one ScrapedProduct to a contract observation record."""
        if not p.raw_name or p.price is None or p.price <= 0:
            return None
        on_special = bool(p.special_price and p.special_price < p.price)
        current = p.special_price if on_special else p.price
        comparison = p.comparison_price if on_special else None
        product_url = (
            f"{self.cfg['base_url']}/shop/product/{p.product_id}" if p.product_id else None
        )
        rec = {
            "source_product_id": p.product_id,
            "retailer_sku": p.product_id,         # FS has no SKU distinct from productId
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
            "multibuy_quantity": p.multibuy_quantity,
            "multibuy_price_cents": p.multibuy_price_cents,
            "promo_starts_at": p.promo_starts_at,
            "promo_ends_at": p.promo_ends_at,
            "promo_metadata": p.promo_metadata or None,
            "product_url": product_url,
            "image_url": p.image_url,
            "observed_at": p.scraped_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if p.detail:
            rec["raw_row"] = p.detail
        return clean_record(rec)

    def _export_jsonl(self, products: list["ScrapedProduct"]) -> None:
        """Stage 1 of the contract: write one JSONL file for this branch+run."""
        records = [r for r in (self._build_observation(p) for p in products) if r]
        write_jsonl(self.chain_key, self.branch_name, records)

    # ---- Run --------------------------------------------------------------

    async def run(self) -> dict:
        loop = asyncio.get_event_loop()
        await asyncio.sleep(random.uniform(0, 15))  # stagger workers across full branch cycle
        await loop.run_in_executor(None, self._resolve_branch)
        logger.info(
            f"chain={self.cfg['name']}  branch={self.branch_name}  "
            f"branch_id={self.branch_id}  api_store_id={self.api_store_id}"
        )

        run_id = self._start_run()
        _branch_t0 = time.time()
        stats = {"records_updated": 0, "records_failed": 0, "new_products": 0,
                 "price_changes": 0, "barcodes_from_cache": 0, "barcodes_fetched": 0,
                 "blocks": 0, "categories_failed": 0, "category_results": []}

        # Pre-load template from shared state (set by a previous run or earlier worker)
        if self._template_state and self._template_state.ready and not self._has_direct:
            self._load_direct_from_state()
            self._fast_categories = True  # all categories go direct — no browser nav needed
            logger.info("[template] pre-loaded — all categories will use direct POST, browser nav skipped")

        # CF solve: use shared state if available (solves once for all workers)
        # Skipped when template is pre-loaded — API calls go to api-prod (no CF)
        if self._has_direct:
            logger.info("[cf] template ready — skipping CF solve for this worker")
        elif self._cf_state:
            await self._cf_state.ensure_fresh(self.cfg)
            self._cf_cookies = list(self._cf_state.cookies)
            self._cf_user_agent = self._cf_state.user_agent
        else:
            await self._fetch_cf_clearance()

        # Browser: use shared instance if available (workers only create contexts)
        if self._shared_browser:
            self._browser = self._shared_browser
        else:
            await self._start_browser()

        await self._new_context()
        all_products: list[ScrapedProduct] = []
        fast_categories = getattr(self, "_fast_categories", False)
        # Warm-up capturer: a worker that begins with no usable template must visit
        # EVERY category through the browser so each body is captured. After the first
        # category is captured _has_direct flips True; without this latch the remaining
        # categories would take the direct-only path with no captured body and be
        # silently skipped — capturing exactly one category per warm-up branch.
        warmup_capture = not self._has_direct
        try:
            for cat_idx, url in enumerate(random.sample(self.category_urls, len(self.category_urls)), 1):
                did_paginate = False
                products: list[ScrapedProduct] = []
                blocks_before = self.blocks
                exc: Optional[Exception] = None

                # Pick up template if another worker captured it since this worker started
                if not self._has_direct and self._template_state and self._template_state.ready:
                    self._load_direct_from_state()
                    fast_categories = True
                    self._fast_categories = True
                    warmup_capture = False  # full template now in hand — go direct (ban-safe)
                    logger.info("[template] picked up from shared state mid-run — switching to direct POST")

                # Fast path: direct POST (skip browser) if template is ready and flag is set.
                # When fast_categories=True and template exists, no browser fallback — direct only.
                # Browser is only used when the template hasn't been captured yet (first category).
                self._long_ban_active = False
                used_direct = False
                if fast_categories and self._has_direct and not warmup_capture:
                    try:
                        direct_products = await self._scrape_category_direct(url)
                        if direct_products is not None:
                            products = direct_products
                            used_direct = True
                    except Exception as e:
                        exc = e  # triggers retry loop below
                        logger.warning(f"  [direct] {url} error: {e}")
                    # No browser fallback in fast mode — template exists, direct only

                if not used_direct and not (fast_categories and self._has_direct and not warmup_capture):
                    # Browser: either fast_categories=False, or template not yet captured
                    try:
                        products, did_paginate = await self.scrape_one_category(url)
                    except Exception as e:
                        exc = e
                        logger.warning(f"category failed: {url}: {e}")

                # Auto-recover: up to 2 retries with proper back-off
                blocked = self.blocks > blocks_before
                for attempt in range(1, 3):
                    if not ((blocked or exc) and not products):
                        break
                    # Long-term IP ban — don't retry, it won't help
                    if getattr(self, '_long_ban_active', False):
                        logger.warning(f"  [skip-retry] {url} — long-term rate ban, skipping retries")
                        break
                    reason = "block" if blocked else f"error: {exc}"
                    wait_secs = random.uniform(15 * attempt, 30 * attempt)
                    logger.warning(
                        f"  [retry {attempt}] category {url} ({reason}) — "
                        f"waiting {wait_secs:.0f}s then re-fetching CF clearance"
                    )
                    await asyncio.sleep(wait_secs)
                    if self._cf_state:
                        await self._cf_state.ensure_fresh(self.cfg)
                        self._cf_cookies = list(self._cf_state.cookies)
                        self._cf_user_agent = self._cf_state.user_agent
                    else:
                        await self._fetch_cf_clearance()
                    try:
                        await self._refresh_context()
                    except Exception as e:
                        logger.warning(f"  [retry {attempt}] context refresh failed: {e}")
                    exc = None
                    blocks_before = self.blocks
                    self._long_ban_active = False
                    try:
                        # In fast mode with template: retry direct POST (no browser)
                        if fast_categories and self._has_direct and not warmup_capture:
                            direct_products = await self._scrape_category_direct(url)
                            if direct_products is not None:
                                products = direct_products
                                logger.info(f"  [retry {attempt}] direct succeeded: {len(products)} products")
                            else:
                                logger.warning(f"  [retry {attempt}] direct STILL empty for {url}")
                                blocked = False
                        else:
                            products, did_paginate = await self.scrape_one_category(url)
                            if products:
                                logger.info(f"  [retry {attempt}] succeeded: {len(products)} products")
                            else:
                                logger.warning(f"  [retry {attempt}] STILL empty for {url}")
                                blocked = self.blocks > blocks_before
                    except Exception as e:
                        exc = e
                        logger.warning(f"  [retry {attempt}] failed: {e}")

                _cat_name = url.split("/category/")[-1]
                if not products and (exc is not None or blocked):
                    stats["categories_failed"] += 1
                    _reason = (
                        "CF block" if blocked
                        else f"{type(exc).__name__}: {exc}" if exc
                        else "empty result after retries"
                    )
                    stats["category_results"].append(
                        category_record(_cat_name, "failed", products, reason=_reason)
                    )
                elif not products:
                    stats["category_results"].append(
                        category_record(_cat_name, "empty", products)
                    )
                else:
                    stats["category_results"].append(
                        category_record(_cat_name, "success", products)
                    )
                all_products.extend(products)
                if did_paginate:
                    await self._refresh_context()
                if not used_direct:
                    await self._random_delay()

            stats["blocks"] = self.blocks
            logger.info(f"TOTAL scraped: {len(all_products)} products  blocks={self.blocks}")
            self._update_run(run_id, total_scraped=len(all_products))

            # Barcode enrichment (parallel + cache)
            if all_products:
                bstats = await self.enrich_barcodes(all_products)
                stats["barcodes_from_cache"] = bstats["from_cache"]
                stats["barcodes_fetched"] = bstats["fetched"]
                self.cache.save()  # persist after each branch run

            # --- DATABASE WRITES DISABLED (two-stage contract) ---
            # The scraper no longer writes to Supabase. It emits one JSONL file per
            # branch+run; pico-prod/import_products.py owns all DB writes. The old
            # direct write is kept (commented) for reference, do not re-enable.
            # if not self.dry_run:
            #     await loop.run_in_executor(None, self._save_to_supabase, all_products, stats)
            # else:
            #     logger.info("DRY RUN — skipping Supabase writes")
            await loop.run_in_executor(None, self._export_jsonl, all_products)

            status = (
                "failed" if (stats["records_failed"] and not stats["records_updated"])
                else "partial" if stats["records_failed"]
                else "success"
            )
            await loop.run_in_executor(None, lambda: self._end_run(run_id, status, stats))
            stats["status"] = status
            stats["branch_name"] = self.branch_name
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

            # disabled for server deploy (no monitor UI):
            # await loop.run_in_executor(None, lambda: post_branch_report(
            #     chain=self.cfg["name"],
            #     branch_name=self.branch_name,
            #     branch_id=str(self.branch_id) if self.branch_id else None,
            #     store_id=str(self.api_store_id) if self.api_store_id else None,
            #     status=status,
            #     total_products=len(all_products),
            #     categories=stats["category_results"],
            #     price_changes=stats["price_changes"],
            #     specials=specials,
            #     out_of_stock=out_of_stock,
            # ))
        except Exception as e:
            logger.exception("run failed")
            stats["status"] = "failed"
            stats["duration"] = time.time() - _branch_t0
            await loop.run_in_executor(None, lambda: self._end_run(run_id, "failed", stats, error=str(e)))
            raise
        finally:
            if self._shared_browser:
                # Shared browser: only close this worker's context, not the browser
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
            else:
                await self._close_browser()
        stats["duration"] = time.time() - _branch_t0
        return stats

    # ---- Supabase writes (mirrors woolworths_claude.py) -----------------

    def _start_run(self) -> Optional[str]:
        # DATABASE WRITE DISABLED — run tracking moves to ingest.import_runs on the
        # import side. Returning None makes _update_run/_end_run no-op safely.
        return None
        # try:
        #     r = self.supabase.table("scraper_runs").insert({
        #         "chain_id": self.chain_id,
        #         "branch_id": self.branch_id,
        #         "status": "running",
        #         "started_at": datetime.now(timezone.utc).isoformat(),
        #     }).execute()
        #     return r.data[0]["id"]
        # except Exception as e:
        #     logger.warning(f"could not insert scraper_runs row: {e}")
        #     return None

    def _update_run(self, run_id: Optional[str], **fields) -> None:
        if not run_id:
            return
        try:
            self.supabase.table("scraper_runs").update(fields).eq("id", run_id).execute()
        except Exception as e:
            logger.warning(f"scraper_runs update failed: {e}")

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
            logger.warning(f"scraper_runs finalise failed: {e}")
        if status in ("success", "partial") and self.branch_id:
            try:
                self.supabase.table("store_branches").update(
                    {"last_scraped_at": datetime.now(timezone.utc).isoformat()}
                ).eq("id", self.branch_id).execute()
            except Exception:
                pass

    # Transient Postgres errors worth retrying under concurrent writes:
    # 40P01 deadlock detected, 57014 statement timeout, 40001 serialization failure.
    _RETRY_PG_CODES = ("40P01", "57014", "40001")

    def _execute_with_retry(self, build, what: str, attempts: int = 4):
        """Execute a Supabase query (build() returns a fresh query builder) with
        backoff retries on transient DB errors from concurrent branch writes.
        Runs in the per-branch executor thread, so time.sleep is safe here."""
        for attempt in range(1, attempts + 1):
            try:
                return build().execute()
            except Exception as e:
                err = str(e)
                if not any(code in err for code in self._RETRY_PG_CODES) or attempt == attempts:
                    raise
                delay = min(2.0, 0.3 * (2 ** (attempt - 1))) + random.uniform(0, 0.3)
                logger.warning(
                    f"{what}: transient DB error (attempt {attempt}/{attempts}), "
                    f"retrying in {delay:.1f}s — {err[:120]}"
                )
                time.sleep(delay)

    def _save_to_supabase(self, products: list[ScrapedProduct], stats: dict) -> None:
        if not products:
            return
        CHUNK = 500

        # Dedupe within run (same item can appear in multiple categories)
        seen: set[str] = set()
        deduped: list[ScrapedProduct] = []
        for p in products:
            key = p.barcode or f"name:{p.raw_name}"
            if key in seen:
                continue
            seen.add(key)
            deduped.append(p)

        with_bc = [p for p in deduped if p.barcode]
        without_bc = [p for p in deduped if not p.barcode]
        logger.info(
            f"saving: {len(deduped)} unique  "
            f"({len(with_bc)} barcoded, {len(without_bc)} need name match)"
        )

        # Snapshot existing barcodes
        known_barcodes: set[str] = set()
        if with_bc:
            codes = [p.barcode for p in with_bc]
            for i in range(0, len(codes), 500):
                chunk = codes[i : i + 500]
                try:
                    r = self.supabase.table("products").select("barcode").in_("barcode", chunk).execute()
                    for row in r.data:
                        if row.get("barcode"):
                            known_barcodes.add(row["barcode"])
                except Exception as e:
                    logger.warning(f"barcode snapshot chunk failed: {e}")

        barcode_to_product: dict[str, dict] = {}
        if with_bc:
            rows: dict[str, dict] = {}
            for p in with_bc:
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

            # Sort by conflict key so all concurrent branches lock rows in the same
            # order — breaks the cyclic wait that causes 40P01 deadlocks.
            payload = sorted(rows.values(), key=lambda r: r["barcode"])
            for i in range(0, len(payload), CHUNK):
                chunk = payload[i : i + CHUNK]
                try:
                    # ignore_duplicates=False (default) so image_url/brand refresh on each run
                    self._execute_with_retry(
                        lambda c=chunk: self.supabase.table("products").upsert(
                            c, on_conflict="barcode"
                        ),
                        f"products upsert chunk {i // CHUNK + 1}",
                    )
                except Exception as e:
                    err = str(e)
                    if "products_name_key" in err or "23505" in err:
                        self._resolve_chunk_by_name(chunk, barcode_to_product)
                    else:
                        logger.error(f"products upsert chunk {i // CHUNK + 1}: {err[:200]}")

            all_codes = list(rows.keys())
            for i in range(0, len(all_codes), 500):
                chunk = all_codes[i : i + 500]
                try:
                    r = (self.supabase.table("products")
                         .select("id,name,brand,barcode").in_("barcode", chunk).execute())
                    for row in r.data:
                        barcode_to_product[row["barcode"]] = row
                except Exception as e:
                    logger.warning(f"products fetch by barcode chunk {i // 500 + 1}: {e}")

        # Resolve product_ids
        matched: list[tuple[str, ScrapedProduct]] = []
        new_products = 0
        for p in deduped:
            if p.barcode and p.barcode in barcode_to_product:
                if p.barcode not in known_barcodes:
                    new_products += 1
                matched.append((barcode_to_product[p.barcode]["id"], p))
            else:
                pid, was_new = self._resolve_by_name(p)
                if pid:
                    if was_new:
                        new_products += 1
                    matched.append((pid, p))
                else:
                    stats["records_failed"] += 1
        stats["new_products"] = new_products
        logger.info(f"matched {len(matched)} products  (new: {new_products})")

        sp_rows: list[dict] = []
        new_price_map: dict[str, float] = {}
        for product_id, p in matched:
            effective = p.special_price if p.special_price else p.price
            new_price_map[product_id] = effective
            retailer_url = (
                f"{self.cfg['base_url']}/shop/product/{p.product_id}"
                if p.product_id else None
            )
            sp_rows.append({
                "product_id": product_id, "store_id": self.branch_id,
                "sku": p.barcode, "current_price": effective,
                "unit_label": p.weight,          # size/pack label e.g. "200g"
                "retailer_url": retailer_url,
                "in_stock": p.in_stock,
                "scraped_at": p.scraped_at.isoformat(),
            })

        # Dedupe by product_id, then sort by it so concurrent branches lock
        # store_products / parent products rows in a consistent order (anti-deadlock).
        sp_rows = sorted({r["product_id"]: r for r in sp_rows}.values(), key=lambda r: r["product_id"])

        # Snapshot existing prices BEFORE the upsert.
        # Supabase upsert returns post-update values, so reading current_price from the
        # upsert response always equals the new price — diff is always 0. Pre-fetching
        # gives us the genuine old price to compare against.
        existing_map: dict[str, dict] = {}
        if self.branch_id:
            page_size = 1000
            offset = 0
            try:
                while True:
                    r = (self.supabase.table("store_products")
                         .select("id,product_id,current_price,unit_price")
                         .eq("store_id", self.branch_id)
                         .range(offset, offset + page_size - 1)
                         .execute())
                    for row in r.data:
                        existing_map[row["product_id"]] = row
                    if len(r.data) < page_size:
                        break
                    offset += page_size
            except Exception as e:
                logger.warning(f"could not fetch store_products snapshot: {e}")

        ph_rows: list[dict] = []
        for product_id, effective in new_price_map.items():
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

        ph_rows = list({r["store_product_id"]: r for r in ph_rows}.values())

        # product_id -> store_product_id (seed from existing rows, then upsert responses)
        sp_id_map: dict[str, str] = {
            pid: row["id"] for pid, row in existing_map.items() if row.get("id")
        }
        total = 0
        for i in range(0, len(sp_rows), CHUNK):
            chunk = sp_rows[i : i + CHUNK]
            try:
                r = self._execute_with_retry(
                    lambda c=chunk: self.supabase.table("store_products").upsert(
                        c, on_conflict="product_id,store_id"
                    ),
                    f"store_products upsert chunk {i // CHUNK + 1}",
                )
                total += len(r.data)
                for rr in (r.data or []):
                    if rr.get("product_id") and rr.get("id"):
                        sp_id_map[rr["product_id"]] = rr["id"]
            except Exception as e:
                logger.error(f"store_products upsert chunk {i // CHUNK + 1}: {e}")
                stats["records_failed"] += len(chunk)
        stats["records_updated"] = total
        logger.info(f"upserted {total} store_products rows")

        # ---- specials: write current specials, deactivate ended ones ----
        self._save_specials(matched, sp_id_map)

        if ph_rows:
            for i in range(0, len(ph_rows), CHUNK):
                chunk = ph_rows[i : i + CHUNK]
                try:
                    self._execute_with_retry(
                        lambda c=chunk: self.supabase.table("price_history").insert(c),
                        f"price_history insert chunk {i // CHUNK + 1}",
                    )
                except Exception as e:
                    logger.warning(f"price_history insert chunk {i // CHUNK + 1}: {e}")
            logger.info(f"recorded {len(ph_rows)} price changes")

        # OOS sweep: products in DB but not seen this run → mark in_stock=False.
        # Only runs when all categories succeeded — if any failed, we'd falsely
        # mark products from those categories as OOS.
        if stats.get("categories_failed", 0) == 0 and self.branch_id and existing_map:
            scraped_pids = set(new_price_map.keys())
            oos_pids = [pid for pid in existing_map if pid not in scraped_pids]
            if oos_pids:
                now_iso = datetime.now(timezone.utc).isoformat()
                oos_count = 0
                for i in range(0, len(oos_pids), CHUNK):
                    chunk_ids = oos_pids[i : i + CHUNK]
                    try:
                        self._execute_with_retry(
                            lambda ids=chunk_ids: self.supabase.table("store_products").update({
                                "in_stock": False,
                                "scraped_at": now_iso,
                            }).eq("store_id", self.branch_id).in_("product_id", ids),
                            f"[oos-sweep] chunk {i // CHUNK + 1}",
                        )
                        oos_count += len(chunk_ids)
                    except Exception as e:
                        logger.warning(f"[oos-sweep] chunk failed: {e}")
                logger.info(f"[oos-sweep] marked {oos_count} products as OOS (not seen this run)")
            else:
                logger.info("[oos-sweep] all existing products seen — no OOS to mark")

    def _save_specials(self, matched: list[tuple[str, ScrapedProduct]], sp_id_map: dict[str, str]) -> None:
        """Write specials for products currently on special; deactivate ended ones.
        Simple is_active model — one active special per store_product."""
        CHUNK = 500
        # store_product_id -> (special_price, original_price) for products on special now
        current: dict[str, tuple[float, float]] = {}
        for product_id, p in matched:
            if p.special_price and p.special_price < p.price:
                spid = sp_id_map.get(product_id)
                if spid:
                    current[spid] = (p.special_price, p.price)

        # Existing active specials for this branch's store_products (chunked .in_)
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

        # Deactivate specials no longer on special
        to_deactivate = [active[spid] for spid in active if spid not in current]
        for i in range(0, len(to_deactivate), 200):
            chunk = to_deactivate[i : i + 200]
            try:
                self._execute_with_retry(
                    lambda c=chunk: self.supabase.table("specials").update(
                        {"is_active": False}).in_("id", c),
                    f"specials deactivate chunk {i // 200 + 1}",
                )
            except Exception as e:
                logger.warning(f"specials deactivate chunk {i // 200 + 1}: {e}")

        # Insert specials for products newly on special (no active row yet)
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
                self._execute_with_retry(
                    lambda c=chunk: self.supabase.table("specials").insert(c),
                    f"specials insert chunk {i // CHUNK + 1}",
                )
            except Exception as e:
                logger.warning(f"specials insert chunk {i // CHUNK + 1}: {e}")
        logger.info(
            f"specials: {len(to_insert)} new, {len(to_deactivate)} deactivated, "
            f"{len(current)} on special now"
        )

    def _resolve_chunk_by_name(self, chunk: list[dict], barcode_to_product: dict[str, dict]) -> None:
        names = [row["name"] for row in chunk]
        try:
            r = (self.supabase.table("products")
                 .select("id,name,brand,barcode").in_("name", names).execute())
            name_map = {row["name"]: row for row in r.data}
        except Exception:
            name_map = {}
        new_rows: list[dict] = []
        for row in chunk:
            existing = name_map.get(row["name"])
            if existing:
                barcode_to_product[row["barcode"]] = existing
                if not existing.get("barcode") and row.get("barcode"):
                    try:
                        self.supabase.table("products").update(
                            {"barcode": row["barcode"]}
                        ).eq("id", existing["id"]).is_("barcode", "null").execute()
                        existing["barcode"] = row["barcode"]
                    except Exception:
                        pass
            else:
                new_rows.append(row)
        if new_rows:
            try:
                ins = self.supabase.table("products").upsert(
                    new_rows, on_conflict="barcode", ignore_duplicates=True
                ).execute()
                for r in (ins.data or []):
                    if r.get("barcode"):
                        barcode_to_product[r["barcode"]] = r
            except Exception as e:
                if "23505" in str(e):
                    try:
                        names = [r["name"] for r in new_rows if r.get("name")]
                        existing = self.supabase.table("products").select("name").in_("name", names).execute()
                        existing_names = {r["name"] for r in (existing.data or [])}
                        filtered = [r for r in new_rows if r.get("name") not in existing_names]
                        if filtered:
                            ins2 = self.supabase.table("products").upsert(
                                filtered, on_conflict="barcode", ignore_duplicates=True
                            ).execute()
                            for r in (ins2.data or []):
                                if r.get("barcode"):
                                    barcode_to_product[r["barcode"]] = r
                    except Exception:
                        pass
                else:
                    logger.warning(f"insert new chunk rows failed: {e}")

    def _resolve_by_name(self, p: ScrapedProduct) -> tuple[Optional[str], bool]:
        try:
            r = (self.supabase.table("products")
                 .select("id,barcode").eq("name", p.raw_name).limit(1).execute())
            if r.data:
                row = r.data[0]
                if p.barcode and not row.get("barcode"):
                    try:
                        self.supabase.table("products").update(
                            {"barcode": p.barcode}
                        ).eq("id", row["id"]).is_("barcode", "null").execute()
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

def parse_args(argv: Optional[list[str]] = None, default_chain: Optional[str] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Foodstuffs NZ scraper (Claude build) — covers New World + Pak'nSave")
    ap.add_argument("--chain", choices=list(CHAINS.keys()), default=default_chain,
                    required=(default_chain is None),
                    help="Which Foodstuffs chain to scrape")
    ap.add_argument("--branch", default=None, help="Branch name (default: chain default)")
    ap.add_argument("--branch-id", default=None, help="Branch UUID overrides --branch")
    ap.add_argument("--all-branches", action="store_true",
                    help="Loop every branch (uses api_store_id from store_branches)")
    ap.add_argument("--categories", default=None,
                    help="Comma-separated category slugs (e.g. fruit-and-vegetables,bakery)")
    ap.add_argument("--test", action="store_true",
                    help="Run only 3 categories: fruit-and-vegetables, bakery, hot-and-cold-drinks")
    ap.add_argument("--no-headless", action="store_true", help="Show browser window")
    ap.add_argument("--dry-run", action="store_true", help="Scrape but do not write to Supabase")
    ap.add_argument("--proxy", default=None,
                    help="Proxy URL (e.g. http://USER:PASS@host:port). Browser + API calls route through it.")
    ap.add_argument("--concurrency", type=int, default=1,
                    help="Number of branches to scrape in parallel (default 1)")
    ap.add_argument("--rate", type=float, default=DEFAULT_RATE_LIMIT,
                    help=f"Global request rate ceiling in req/sec shared across all "
                         f"workers (default {DEFAULT_RATE_LIMIT}). Both NW and PnS "
                         f"scraped with blocks=0 at 12, so there is headroom — raise "
                         f"in steps and watch the DONE line's blocks= counter.")
    ap.add_argument("--no-adaptive-drop", action="store_true", default=False,
                        help="Disable adaptive concurrency drop on 429 blocks (keep concurrency fixed)")
    ap.add_argument("--fast-categories", action="store_true", default=True,
                        help="Skip browser for categories 2-N per branch — direct POST reusing captured headers (default on)")
    ap.add_argument("--no-fast-categories", dest="fast_categories", action="store_false",
                        help="Force browser for every category (disables direct-POST fast path)")
    ap.add_argument("--resume", action="store_true", default=False,
                    help="Skip branches already completed in a previous run (uses checkpoint file)")
    ap.add_argument("--capsolver", action="store_true", default=False,
                    help="Solve Cloudflare via CapSolver (needs CAPSOLVER_API_KEY + CAPSOLVER_PROXY in .env). "
                         "Default: free UA-spoofed headless solve.")
    return ap.parse_args(argv)


def categories_for(args) -> Optional[list[str]]:
    if args.test:
        return ["fruit-and-vegetables", "bakery", "hot-and-cold-drinks"]
    if args.categories:
        return [s.strip() for s in args.categories.split(",") if s.strip()]
    return None


async def main_async(argv: Optional[list[str]] = None, default_chain: Optional[str] = None) -> int:
    args = parse_args(argv, default_chain=default_chain)

    # --capsolver toggles the CapSolver CF-solve path; default is the free headless UA-spoof solve.
    global CAPSOLVER_ENABLED
    CAPSOLVER_ENABLED = args.capsolver
    if args.capsolver and not (CAPSOLVER_API_KEY and CAPSOLVER_PROXY):
        logger.warning("[cf] --capsolver set but CAPSOLVER_API_KEY/CAPSOLVER_PROXY missing in .env — "
                       "falling back to headless UA-spoof solve")
    elif args.capsolver:
        logger.info("[cf] --capsolver enabled — Cloudflare will be solved via CapSolver AntiCloudflareTask")

    headless = not args.no_headless
    cats = categories_for(args)
    cache = BarcodeCache()

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    cfg = CHAINS[args.chain]
    _setup_file_logging(args.chain)

    # Only the CapSolver solve needs the tunnel (it's remote and must borrow your home IP).
    # The scrape runs locally — same home IP as the tunnel exit — so it goes direct, avoiding
    # the slow/flaky shared relay. cf_clearance stays valid because the IP is identical.
    scrape_proxy = args.proxy

    checkpoint_file = Path(__file__).parent / f".{args.chain}_checkpoint.json"

    branches: list[dict] = []
    if args.all_branches:
        chain = sb.table("store_chains").select("id").eq("slug", cfg["slug"]).execute().data
        if not chain:
            logger.error(f"no {cfg['name']} chain row"); return 1
        all_b = (sb.table("store_branches")
                 .select("id,name,api_store_id").eq("chain_id", chain[0]["id"]).execute().data)
        branches = [b for b in all_b if b.get("api_store_id")]
        logger.info(f"--all-branches: {len(branches)} branches with api_store_id")
    else:
        branches = [{"id": args.branch_id, "name": args.branch, "api_store_id": None}]

    # Checkpoint: skip already-completed branches on --resume
    completed_ids: set[str] = set()
    if args.resume and checkpoint_file.exists():
        try:
            completed_ids = set(json.loads(checkpoint_file.read_text()))
            skipped = len([b for b in branches if b.get("id") in completed_ids])
            branches = [b for b in branches if b.get("id") not in completed_ids]
            logger.info(f"[resume] skipping {skipped} already-completed branches, {len(branches)} remaining")
        except Exception as e:
            logger.warning(f"[resume] checkpoint load failed: {e} — starting fresh")
    elif not args.resume and args.all_branches and checkpoint_file.exists():
        # Only wipe checkpoint when doing a fresh full run, not single-branch tests
        checkpoint_file.unlink()

    # Template persistence — load from disk if available
    template_state = TemplateState(args.chain)
    if template_state.ready:
        logger.info("[template] API template loaded from disk — CF solve and browser nav will be skipped")

    # Solve CF once — shared across all workers (skipped if template already on disk)
    cf_state = CfState()
    if not template_state.ready:
        await cf_state.solve(cfg)
        if not cf_state.cookies:
            logger.info("[cf] startup solve returned no cookies — retrying in 3s ...")
            await asyncio.sleep(3)
            await cf_state.solve(cfg)
    else:
        logger.info("[cf] skipping CF solve — template pre-loaded")

    # Single shared browser — workers create lightweight contexts only
    playwright_instance = await async_playwright().start()
    launch_kwargs: dict = {
        "headless": headless,
        "args": ["--disable-http2", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    }
    if scrape_proxy:
        launch_kwargs["proxy"] = _parse_proxy(scrape_proxy)
        logger.info(f"[proxy] scrape routed through {launch_kwargs['proxy'].get('server')}")
    shared_browser = await playwright_instance.chromium.launch(**launch_kwargs)

    # Global rate limiter: max req/sec across all workers. Tunable via --rate;
    # burst tracks the rate so a raised ceiling isn't throttled by a stale burst.
    rate_limiter = TokenBucketLimiter(rate=args.rate, burst=max(1, int(round(args.rate))))
    logger.info(f"[rate] global ceiling {args.rate} req/sec across "
                f"{args.concurrency} worker(s)")

    overall = {"branches": 0, "updated": 0, "new": 0, "failed": 0, "price_changes": 0,
               "cache_hits": 0, "fetched": 0, "blocks": 0}
    t0 = time.time()
    runlog = ScraperRunLog(
        args.chain,
        mode="all-branches" if args.all_branches else "single-branch",
        total_branches=len(branches),
    )

    adaptive = AdaptiveSemaphore(max(1, args.concurrency), min_level=1)
    # Scale block threshold proportionally with concurrency (base rate: 5 blocks per worker)
    effective_block_threshold = max(ADAPTIVE_BLOCK_THRESHOLD,
                                    ADAPTIVE_BLOCK_THRESHOLD * args.concurrency // 4)
    logger.info(f"[adaptive] starting at concurrency={adaptive.current}  "
                f"block_threshold={effective_block_threshold}")
    completed = 0
    total = len(branches)
    completed_lock = asyncio.Lock()
    block_counter = 0  # cumulative blocks across all branches
    block_counter_lock = asyncio.Lock()
    overall["blocks"] = 0  # ensure counter is set before any callback
    clean_branches = 0  # consecutive successful branches since last block/downgrade
    upgrade_threshold = 5  # clean branches needed to restore full concurrency

    async def on_block_fired() -> None:
        """Immediate-downgrade callback: react to blocks while branches are in flight."""
        nonlocal block_counter, clean_branches
        async with block_counter_lock:
            block_counter += 1
            clean_branches = 0  # reset recovery counter on any block
            overall["blocks"] += 1
            if not args.no_adaptive_drop and block_counter >= effective_block_threshold:
                block_counter = 0
                old, new = adaptive.downgrade()
                if old != new:
                    logger.warning(
                        f"[adaptive] {overall['blocks']} total blocks (live) — "
                        f"dropping concurrency {old} -> {new}"
                    )

    async def run_one(b: dict) -> None:
        nonlocal completed, clean_branches
        async with adaptive:
            scraper = FoodstuffsScraper(
                chain_key=args.chain,
                branch_name=b.get("name") or cfg["default_branch"],
                branch_id=b.get("id"),
                category_slugs=cats,
                headless=headless,
                dry_run=args.dry_run,
                cache=cache,
                proxy_url=scrape_proxy,
                on_block=on_block_fired,
                cf_state=cf_state,
                shared_browser=shared_browser,
                rate_limiter=rate_limiter,
                template_state=template_state,
            )
            scraper._fast_categories = getattr(args, "fast_categories", False)
            try:
                stats = await scraper.run()
                overall["branches"] += 1
                overall["updated"] += stats["records_updated"]
                overall["new"] += stats["new_products"]
                overall["failed"] += stats["records_failed"]
                overall["price_changes"] += stats["price_changes"]
                overall["cache_hits"] += stats.get("barcodes_from_cache", 0)
                overall["fetched"] += stats.get("barcodes_fetched", 0)
                # Recover concurrency after enough clean branches
                if not args.no_adaptive_drop:
                    async with block_counter_lock:
                        clean_branches += 1
                        if clean_branches >= upgrade_threshold:
                            clean_branches = 0
                            old, new = await adaptive.upgrade()
                            if old != new:
                                logger.info(
                                    f"[adaptive] {upgrade_threshold} clean branches — "
                                    f"recovering concurrency {old} -> {new}"
                                )
                # Only checkpoint if branch had no empty categories — records_failed includes
                # normal unresolvable products so don't use it as a disqualifier
                branch_clean = stats.get("categories_failed", -1) == 0
                runlog.add_branch(
                    branch_name=stats.get("branch_name") or scraper.branch_name,
                    branch_slug=_slug(stats.get("branch_name") or scraper.branch_name),
                    status=stats.get("status", "success"),
                    duration_seconds=stats.get("duration", 0.0),
                    categories=stats.get("category_results", []),
                )
                logger.info(f"[checkpoint] branch={b.get('name')} id={b.get('id')} categories_failed={stats.get('categories_failed', 'MISSING')} branch_clean={branch_clean}")
                if b.get("id") and branch_clean:
                    completed_ids.add(b["id"])
                    try:
                        checkpoint_file.write_text(json.dumps(list(completed_ids)))
                        logger.info(f"[checkpoint] written — {len(completed_ids)} branches saved")
                    except Exception as e:
                        logger.error(f"[checkpoint] write failed: {e}")
                # Block-triggered downgrades happen live via on_block_fired callback
            except Exception as e:
                logger.error(f"branch {b.get('name') or b.get('id')} failed: {e}")
                overall["failed"] += 1
                runlog.add_branch(
                    branch_name=scraper.branch_name,
                    branch_slug=_slug(scraper.branch_name),
                    status="failed",
                    duration_seconds=0.0,
                    categories=[],
                    error=str(e)[:500],
                )
            async with completed_lock:
                completed += 1
                logger.info(
                    f"=== progress: {completed}/{total} branches done "
                    f"({(completed/total*100):.1f}%)  conc={adaptive.current} ==="
                )

    async def _periodic_cf_refresh():
        while True:
            await asyncio.sleep(25 * 60)
            if template_state.ready:
                return  # template in use — CF solve not needed
            logger.info("[cf-refresh] proactive CF clearance re-solve (25-min interval) ...")
            await cf_state.solve(cfg)

    refresh_task = asyncio.create_task(_periodic_cf_refresh())
    try:
        # Warm-up: when no template is on disk yet, capture it with ONE branch first, then
        # launch the rest. Otherwise every worker runs the browser-path template capture in
        # parallel at startup (each paginating ~20 pages/category) — a request burst that trips
        # Cloudflare 429 rate-limiting before steady-state scraping begins. Best-effort: if the
        # capture fails, `ready` stays False and the remaining branches run exactly as before.
        run_list = branches
        if branches and not template_state.ready:
            logger.info("[warmup] no template on disk — capturing with 1 branch before launching the rest")
            # Try branches one at a time (SERIAL) until one captures the template. A single
            # transient failure on the warm-up branch (DNS blip, network hiccup) must NOT fall
            # through to the parallel launch below — N browser workers hitting www at once is a
            # request burst that trips Cloudflare 429 with retry-after=86400 (24h IP ban). Serial
            # retry keeps www load to a single worker until the template exists; afterwards every
            # branch goes direct to api-prod (no www, no CF). If no branch can capture after a few
            # serial attempts, abort rather than burst — a parallel www burst guarantees the ban.
            max_warmup = min(len(branches), 5)
            attempted = 0
            for b in branches[:max_warmup]:
                attempted += 1
                await run_one(b)
                if template_state.ready:
                    break
                logger.warning(
                    f"[warmup] branch '{b.get('name')}' captured no template "
                    f"(attempt {attempted}/{max_warmup}, likely a transient error) — "
                    "retrying serially with the next branch (NOT launching parallel workers)"
                )
            run_list = branches[attempted:]
            if template_state.ready:
                logger.info(f"[warmup] template captured after {attempted} branch(es) — "
                            "remaining branches start in direct-POST mode")
            else:
                logger.error(
                    f"[warmup] no template captured after {attempted} serial attempts — aborting "
                    "before launching parallel browser workers (a parallel www burst would trip "
                    "Cloudflare's 24h rate ban). Check network/DNS connectivity and re-run."
                )
                run_list = []
        await asyncio.gather(*[run_one(b) for b in run_list], return_exceptions=True)
    finally:
        refresh_task.cancel()
        try:
            await refresh_task
        except asyncio.CancelledError:
            pass
        await shared_browser.close()
        await playwright_instance.stop()

    cache.save()
    dt = time.time() - t0
    logger.info(
        f"DONE  chain={cfg['name']}  branches={overall['branches']}  "
        f"updated={overall['updated']}  new={overall['new']}  "
        f"changes={overall['price_changes']}  failed={overall['failed']}  "
        f"blocks={overall['blocks']}  final_concurrency={adaptive.current}  "
        f"barcodes(cache/fetched)={overall['cache_hits']}/{overall['fetched']}  "
        f"elapsed={dt:.1f}s"
    )
    log_path = runlog.finish(overall)
    if log_path:
        logger.info(f"[runlog] scraper run log → {log_path}")
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
