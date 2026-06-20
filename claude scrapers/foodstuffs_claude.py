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
from report_client import post_branch_report

# ---------------------------------------------------------------------------
# Paths & env
# ---------------------------------------------------------------------------

THIS_DIR = Path(__file__).resolve().parent
SCRAPERS_DIR = THIS_DIR.parent
ENV_PATH = SCRAPERS_DIR / ".env"
CACHE_PATH = THIS_DIR / ".foodstuffs_cache.json"


def _template_path(chain_key: str) -> Path:
    return THIS_DIR / f".{chain_key}_direct_template.json"


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


async def _solve_cf_clearance(cfg: dict) -> tuple[list[dict], Optional[str]]:
    """Get CF clearance for the one-time template capture, with automatic fallback.

    - Default: free UA-spoofed headless solve first; if it yields no cookies, AUTO-SHIFT to
      CapSolver when creds are configured (CAPSOLVER_API_KEY + CAPSOLVER_PROXY in .env).
    - With --capsolver: CapSolver first, headless as the backup.

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
    if cookies:
        return cookies, ua

    # Auto-shift: headless got nothing → try CapSolver if creds are configured.
    if _capsolver_configured():
        logger.warning("[cf] headless UA-spoof solve got no cookies — auto-shifting to CapSolver")
        cs_cookies, cs_ua = await _solve_cf_via_capsolver(cfg)
        if cs_cookies:
            return cs_cookies, cs_ua
        logger.warning("[cf] CapSolver auto-fallback also produced no cookies")
    else:
        logger.warning("[cf] headless solve got no cookies and CapSolver not configured — "
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
        self.body: Optional[dict] = None
        self.url: str = ""
        self.headers: dict[str, str] = {}
        self.store_id: str = ""  # storeId embedded in the saved body
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text())
            self.body = data["body_template"]
            self.url = data["url"]
            self.headers = data["headers"]
            self.store_id = data.get("template_store_id", "")
            logger.info(f"[template] loaded from {self._path.name}")
        except Exception as e:
            logger.warning(f"[template] load failed: {e}")

    def save(self, body: dict, url: str, headers: dict, store_id: str) -> None:
        self.body = body
        self.url = url
        self.headers = headers
        self.store_id = store_id
        try:
            self._path.write_text(json.dumps(
                {"url": url, "headers": headers, "body_template": body, "template_store_id": store_id},
                ensure_ascii=False, indent=2,
            ))
            logger.info(f"[template] saved to {self._path.name}")
        except Exception as e:
            logger.warning(f"[template] save failed: {e}")

    def invalidate(self) -> None:
        self.body = None
        self.url = ""
        self.headers = {}
        self.store_id = ""
        try:
            self._path.unlink(missing_ok=True)
            logger.info("[template] invalidated (deleted from disk)")
        except Exception:
            pass

    @property
    def ready(self) -> bool:
        return bool(self.body and self.url and self.headers)


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
    """Simple JSON-file cache: {productId: barcode}. Shared across NW + PS runs."""

    def __init__(self, path: Path = CACHE_PATH) -> None:
        self.path = path
        self._data: dict[str, str] = {}
        if path.exists():
            try:
                self._data = json.loads(path.read_text())
            except Exception as e:
                logger.warning(f"cache read failed ({e}) — starting empty")
                self._data = {}
        logger.info(f"barcode cache: {len(self._data)} entries loaded from {path.name}")

    def get(self, product_id: str) -> Optional[str]:
        return self._data.get(product_id)

    def put(self, product_id: str, barcode: str) -> None:
        self._data[product_id] = barcode

    def save(self) -> None:
        try:
            self.path.write_text(json.dumps(self._data, ensure_ascii=False))
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
        self._direct_template: Optional[dict] = None
        self._direct_headers: dict[str, str] = {}
        self._direct_url: str = ""
        self._direct_template_store_id: str = ""

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
        # Bandwidth: block heavy assets we don't need (~70% smaller pages)
        await self._page.route(
            "**/*",
            lambda route, req: (
                route.abort() if ASSET_BLOCK_RE.search(req.url or "") else route.continue_()
            ),
        )

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
        chain = (
            self.supabase.table("store_chains")
            .select("id").eq("slug", self.cfg["slug"]).execute().data
        )
        if not chain:
            chain = (
                self.supabase.table("store_chains")
                .upsert({"slug": self.cfg["slug"], "name": self.cfg["name"]}, on_conflict="slug")
                .execute().data
            )
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

        # Branch missing — create it (api_store_id will need to be backfilled manually).
        r = (self.supabase.table("store_branches")
             .upsert({"chain_id": self.chain_id, "name": self.branch_name},
                     on_conflict="chain_id,name").execute().data)
        self.branch_id = r[0]["id"]
        self.api_store_id = r[0].get("api_store_id")

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
                    resp = await self._page.request.post(self._api_url, headers=headers, data=body)
                    if not resp.ok and resp.status == 500:
                        await asyncio.sleep(3)
                        resp = await self._page.request.post(self._api_url, headers=headers, data=body)
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
                            resp = await self._page.request.post(self._api_url, headers=headers, data=body)
                            if resp.ok:
                                captured.append({"url": self._api_url, "data": await resp.json()})
                            elif resp.status == 500:
                                await asyncio.sleep(3)
                                resp2 = await self._page.request.post(self._api_url, headers=headers, data=body)
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

        # Save template for direct-POST fast path after any successful browser category.
        # Only accept the template when the filters field contains category0SI — otherwise the
        # regex replacement in _scrape_category_direct will always fail to match.
        if products and self._api_post_data and self._api_url and self._api_headers:
            if not self._direct_template:
                _alg = self._api_post_data.get("algoliaQuery") or {}
                _filters = _alg.get("filters", "")
                if 'category0SI:' in _filters:
                    self._direct_template = copy.deepcopy(self._api_post_data)
                    self._direct_url = self._api_url
                    self._direct_headers = {k: v for k, v in self._api_headers.items()
                                             if k.lower() not in ("host", "content-length")}
                    self._direct_template_store_id = self.api_store_id or ""
                    logger.info(f"[template] direct-POST API url: {self._direct_url}")
                    if self._template_state and not self._template_state.ready:
                        self._template_state.save(
                            self._direct_template,
                            self._direct_url,
                            self._direct_headers,
                            self._direct_template_store_id,
                        )

        return products, did_paginate

    async def _scrape_category_direct(self, url: str) -> Optional[list[ScrapedProduct]]:
        """Skip browser navigation — POST directly using the saved template.

        Returns a product list on success, or None if a fallback to browser is needed.
        """
        if not self._direct_template or not self._direct_url or not self._direct_headers:
            return None

        slug = url.rstrip("/").split("/")[-1]
        display_name = SLUG_TO_DISPLAY_NAME.get(slug)
        if not display_name:
            return None  # unknown slug — browser fallback

        body = copy.deepcopy(self._direct_template)

        # Substitute this branch's storeId — template may have been captured by a different branch
        if self.api_store_id and self._direct_template_store_id and \
                self._direct_template_store_id != self.api_store_id:
            tmpl_sid = self._direct_template_store_id
            for _f in ("storeId", "store_id"):
                if _f in body:
                    body[_f] = self.api_store_id
            _alg0 = body.get("algoliaQuery") or {}
            if "filters" in _alg0 and tmpl_sid in _alg0["filters"]:
                _alg0["filters"] = _alg0["filters"].replace(tmpl_sid, self.api_store_id)

        alg = body.get("algoliaQuery")
        if not alg or "filters" not in alg:
            logger.warning(f"  [direct] {url} → algoliaQuery/filters missing in template — browser fallback")
            return None
        original_filters = alg["filters"]
        alg["filters"] = re.sub(
            r'category0SI:"[^"]*"',
            f'category0SI:"{display_name}"',
            original_filters,
        )
        if alg["filters"] == original_filters:
            logger.warning(f"  [direct] {url} → category0SI not found in filters — browser fallback")
            return None
        body["page"] = 0  # API is 0-indexed — page 0 is the first page

        category = category_from_url(url)
        try:
            if self._rate_limiter:
                await self._rate_limiter.acquire()
            resp = await self._page.request.post(
                self._direct_url, headers=self._direct_headers, data=body
            )
            if not resp.ok:
                if resp.status == 429 or resp.status >= 500:
                    raise Exception(f"HTTP {resp.status} on direct POST")
                if resp.status in (401, 403):
                    logger.warning(f"  [direct] {url} → HTTP {resp.status} — API key stale, invalidating template")
                    if self._template_state:
                        self._template_state.invalidate()
                    self._direct_template = None
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
                r2 = await self._page.request.post(
                    self._direct_url, headers=self._direct_headers, data=pb
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
        for data in all_responses:
            items = (
                data.get("products") or data.get("items")
                or (data.get("pageProps") or {}).get("products") or []
            )
            for item in items:
                p = self._parse_item(item, category)
                if p:
                    products.append(p)

        if not products:
            logger.warning(f"  [direct] {url} → 0 products — browser fallback")
            return None

        pages_label = f"{total_pages}p" if total_pages > 1 else ""
        logger.info(f"  [direct] {url}  {len(products)} products {pages_label}".rstrip())
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

        special = None
        special_field = None
        for key in ("salePrice", "promotionPrice", "specialPrice"):
            v = sp.get(key)
            try:
                if v and float(v) < float(price_cents):
                    special = float(v) / 100
                    special_field = key
                    break
            except (TypeError, ValueError):
                continue

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

        # Apply cache first
        to_fetch: list[str] = []
        cached_map: dict[str, str] = {}
        for pid in unique_pids:
            bc = self.cache.get(pid)
            if bc:
                cached_map[pid] = bc
                stats["from_cache"] += 1
            else:
                to_fetch.append(pid)
        logger.info(
            f"  [barcode] {stats['from_cache']}/{stats['requested']} from cache, "
            f"{len(to_fetch)} need API fetch"
        )

        # Adaptive parallel fetch
        result_map: dict[str, str] = dict(cached_map)
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

                async def fetch_one(pid: str) -> tuple[str, Optional[str], int]:
                    url = BARCODE_API.format(store_id=self.api_store_id, pid=pid)
                    try:
                        assert self._page
                        # try this if hitting many blocks
                        # if self._rate_limiter:
                        #     await self._rate_limiter.acquire()
                        resp = await self._page.request.get(url, headers=headers)
                        if resp.ok:
                            data = await resp.json()
                            sku = data.get("sku")
                            return pid, str(sku) if sku else None, resp.status
                        return pid, None, resp.status
                    except Exception:
                        return pid, None, 0

                results = await asyncio.gather(*[fetch_one(pid) for pid in batch])
                batch_429 = 0
                for pid, bc, status in results:
                    if bc:
                        result_map[pid] = bc
                        self.cache.put(pid, bc)
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
                            f"  [barcode] {batch_429} 429s in batch → concurrency "
                            f"{current_concurrency} → {new_c}"
                        )
                        current_concurrency = new_c
                    await asyncio.sleep(2 + consecutive_429)
                else:
                    consecutive_429 = 0
            dt = time.time() - t0
            logger.info(
                f"  [barcode] enriched {stats['fetched']}/{len(to_fetch)} via API "
                f"in {dt:.1f}s (concurrency ended at {current_concurrency})"
            )

        # Apply to scraped products
        applied = 0
        for p in products:
            if p.product_id and p.product_id in result_map:
                p.barcode = result_map[p.product_id]
                applied += 1
        logger.info(
            f"  [barcode] applied to {applied}/{len(products)} products"
        )
        return stats

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
        stats = {"records_updated": 0, "records_failed": 0, "new_products": 0,
                 "price_changes": 0, "barcodes_from_cache": 0, "barcodes_fetched": 0,
                 "blocks": 0, "categories_failed": 0, "category_results": []}

        # Pre-load template from shared state (set by a previous run or earlier worker)
        if self._template_state and self._template_state.ready and not self._direct_template:
            self._direct_template = copy.deepcopy(self._template_state.body)
            self._direct_url = self._template_state.url
            self._direct_headers = dict(self._template_state.headers)
            self._direct_template_store_id = self._template_state.store_id
            self._fast_categories = True  # all categories go direct — no browser nav needed
            logger.info("[template] pre-loaded — all categories will use direct POST, browser nav skipped")

        # CF solve: use shared state if available (solves once for all workers)
        # Skipped when template is pre-loaded — API calls go to api-prod (no CF)
        if self._direct_template:
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
        try:
            for cat_idx, url in enumerate(random.sample(self.category_urls, len(self.category_urls)), 1):
                did_paginate = False
                products: list[ScrapedProduct] = []
                blocks_before = self.blocks
                exc: Optional[Exception] = None

                # Pick up template if another worker captured it since this worker started
                if not self._direct_template and self._template_state and self._template_state.ready:
                    self._direct_template = copy.deepcopy(self._template_state.body)
                    self._direct_url = self._template_state.url
                    self._direct_headers = dict(self._template_state.headers)
                    self._direct_template_store_id = self._template_state.store_id
                    fast_categories = True
                    self._fast_categories = True
                    logger.info("[template] picked up from shared state mid-run — switching to direct POST")

                # Fast path: direct POST (skip browser) if template is ready and flag is set.
                # When fast_categories=True and template exists, no browser fallback — direct only.
                # Browser is only used when the template hasn't been captured yet (first category).
                self._long_ban_active = False
                used_direct = False
                if fast_categories and self._direct_template:
                    try:
                        direct_products = await self._scrape_category_direct(url)
                        if direct_products is not None:
                            products = direct_products
                            used_direct = True
                    except Exception as e:
                        exc = e  # triggers retry loop below
                        logger.warning(f"  [direct] {url} error: {e}")
                    # No browser fallback in fast mode — template exists, direct only

                if not used_direct and not (fast_categories and self._direct_template):
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
                        if fast_categories and self._direct_template:
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

            if not self.dry_run:
                await loop.run_in_executor(None, self._save_to_supabase, all_products, stats)
            else:
                logger.info("DRY RUN — skipping Supabase writes")

            status = (
                "failed" if (stats["records_failed"] and not stats["records_updated"])
                else "partial" if stats["records_failed"]
                else "success"
            )
            await loop.run_in_executor(None, lambda: self._end_run(run_id, status, stats))
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

            await loop.run_in_executor(None, lambda: post_branch_report(
                chain=self.cfg["name"],
                branch_name=self.branch_name,
                branch_id=str(self.branch_id) if self.branch_id else None,
                store_id=str(self.api_store_id) if self.api_store_id else None,
                status=status,
                total_products=len(all_products),
                categories=stats["category_results"],
                price_changes=stats["price_changes"],
                specials=specials,
                out_of_stock=out_of_stock,
            ))
        except Exception as e:
            logger.exception("run failed")
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
        return stats

    # ---- Supabase writes (mirrors woolworths_claude.py) -----------------

    def _start_run(self) -> Optional[str]:
        if self.dry_run:
            return None
        try:
            r = self.supabase.table("scraper_runs").insert({
                "chain_id": self.chain_id,
                "branch_id": self.branch_id,
                "status": "running",
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
                if wv is not None:
                    row["weight_value"] = wv
                if wu is not None:
                    row["weight_unit"] = wu
                rows[p.barcode] = row

            payload = list(rows.values())
            for i in range(0, len(payload), CHUNK):
                chunk = payload[i : i + CHUNK]
                try:
                    # ignore_duplicates=False (default) so image_url/brand refresh on each run
                    self.supabase.table("products").upsert(
                        chunk, on_conflict="barcode"
                    ).execute()
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
            sp_rows.append({
                "product_id": product_id, "store_id": self.branch_id,
                "sku": p.barcode, "current_price": effective,
                "unit_price": None, "unit_label": None,
                "in_stock": p.in_stock,
                "scraped_at": p.scraped_at.isoformat(),
            })

        sp_rows = list({r["product_id"]: r for r in sp_rows}.values())

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

        total = 0
        for i in range(0, len(sp_rows), CHUNK):
            chunk = sp_rows[i : i + CHUNK]
            try:
                r = self.supabase.table("store_products").upsert(
                    chunk, on_conflict="product_id,store_id"
                ).execute()
                total += len(r.data)
            except Exception as e:
                logger.error(f"store_products upsert chunk {i // CHUNK + 1}: {e}")
                stats["records_failed"] += len(chunk)
        stats["records_updated"] = total
        logger.info(f"upserted {total} store_products rows")

        if ph_rows:
            for i in range(0, len(ph_rows), CHUNK):
                chunk = ph_rows[i : i + CHUNK]
                try:
                    self.supabase.table("price_history").insert(chunk).execute()
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
                        self.supabase.table("store_products").update({
                            "in_stock": False,
                            "scraped_at": now_iso,
                        }).eq("store_id", self.branch_id).in_("product_id", chunk_ids).execute()
                        oos_count += len(chunk_ids)
                    except Exception as e:
                        logger.warning(f"[oos-sweep] chunk failed: {e}")
                logger.info(f"[oos-sweep] marked {oos_count} products as OOS (not seen this run)")
            else:
                logger.info("[oos-sweep] all existing products seen — no OOS to mark")

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

    # Global rate limiter: 4 req/sec max across all workers
    # try this if hitting many blocks: TokenBucketLimiter(rate=6.0, burst=6)
    rate_limiter = TokenBucketLimiter(rate=4.0, burst=4)

    overall = {"branches": 0, "updated": 0, "new": 0, "failed": 0, "price_changes": 0,
               "cache_hits": 0, "fetched": 0, "blocks": 0}
    t0 = time.time()

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
        await asyncio.gather(*[run_one(b) for b in branches], return_exceptions=True)
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
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
