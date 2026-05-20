# woolworths_scraper.py
"""
Woolworths NZ scraper.

Uses the same category-page + network-interception approach as New World / Pak'nSave.
The browser loads each browse category; we capture the /api/v1/products JSON responses
the page fires automatically — no extra requests are made.

API endpoint: GET /api/v1/products?dasFilter=Department%3B%3B{slug}%3Bfalse&target=browse&...
Key fields: name (lowercase), brand, price.originalPrice, price.salePrice, size.volumeSize, images.big

NOTE: Category URL slugs were inferred from DevTools — verify in browser if any 404.
"""
from __future__ import annotations

import asyncio
import copy
import logging
import math
import re
from pathlib import Path
from typing import Optional

from base_scraper import (
    BaseScraper,
    ScrapedProduct,
    EXTRA_HEADERS,
    STEALTH_SCRIPT,
    USER_AGENT,
)
from config import CHAIN_CONFIG, REQUEST_TIMEOUT_MS

logger = logging.getLogger(__name__)

BASE_URL = "https://www.woolworths.co.nz"

DELAY_MIN = 2.0
DELAY_MAX = 5.0

CATEGORY_URLS = [
    "https://www.woolworths.co.nz/shop/browse/fruit-veg",
    "https://www.woolworths.co.nz/shop/browse/meat-poultry",
    "https://www.woolworths.co.nz/shop/browse/fish-seafood",
    "https://www.woolworths.co.nz/shop/browse/fridge-deli",
    "https://www.woolworths.co.nz/shop/browse/bakery",
    "https://www.woolworths.co.nz/shop/browse/frozen",
    "https://www.woolworths.co.nz/shop/browse/pantry",
    "https://www.woolworths.co.nz/shop/browse/drinks",
    "https://www.woolworths.co.nz/shop/browse/health-body",
    "https://www.woolworths.co.nz/shop/browse/household",
]

URL_CATEGORY: dict[str, str] = {
    "fruit-veg": "produce",
    "meat-poultry": "meat",
    "fish-seafood": "meat",
    "fridge-deli": "dairy",
    "bakery": "bakery",
    "frozen": "frozen",
    "pantry": "pantry",
    "drinks": "drinks",
    "health-body": "health",
    "household": "household",
}


def _category_from_url(url: str) -> str:
    for slug, cat in URL_CATEGORY.items():
        if slug in url:
            return cat
    return "other"


class WoolworthsScraper(BaseScraper):
    store_name = "Woolworths"
    chain_slug = CHAIN_CONFIG["woolworths"]["slug"]
    chain_name = CHAIN_CONFIG["woolworths"]["name"]
    branch_name = CHAIN_CONFIG["woolworths"]["branch_name"]

    async def _load_branch_session(self) -> None:
        """Replace the default browser context with one loaded from saved session
        state for the current branch, so requests are pinned to the right store.
        """
        assert self._browser
        if not self._branch_id:
            logger.warning(
                f"[Woolworths] No branch_id set — cannot load saved session"
            )
            return

        session_path = (
            Path(__file__).parent / "sessions" / "woolworths" / f"{self._branch_id}.json"
        )
        if not session_path.exists():
            logger.warning(
                f"[Woolworths] No saved session for branch {self._branch_id} "
                f"— prices may be incorrect. Run bootstrap_woolworths_sessions.py first."
            )
            return

        if self._page:
            await self._page.close()
        if self._context:
            await self._context.close()

        self._context = await self._browser.new_context(
            storage_state=str(session_path),
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
        logger.info(f"[Woolworths] Loaded saved session for branch {self._branch_id}")

        # Session freshness check
        try:
            check_url = (
                "https://www.woolworths.co.nz/api/v1/products"
                "?target=browse&dasFilter=Department%3B%3Bfruit-veg%3Bfalse"
                "&inStockProductsOnly=false&size=1&page=1"
            )
            resp = await self._page.request.get(check_url)
            if resp.ok:
                body = await resp.json()
                products_node = body.get("products") or {}
                total_items = products_node.get("totalItems") or 0
                if total_items > 0:
                    logger.info(
                        f"[Woolworths] Session verified: {total_items} products in fruit-veg"
                    )
                else:
                    logger.warning(f"[Woolworths] Session may be stale (totalItems=0)")
            else:
                logger.warning(
                    f"[Woolworths] Session may be stale (HTTP {resp.status})"
                )
        except Exception as e:
            logger.warning(f"[Woolworths] Session may be stale (check failed: {e})")

    async def scrape(self) -> list[ScrapedProduct]:
        await self._load_branch_session()
        products: list[ScrapedProduct] = []
        captured: list[dict] = []
        self._api_headers: dict[str, str] = {}
        self._api_paginated_url: str = ""
        self._api_method: str = "GET"
        self._api_post_data: dict | None = None

        assert self._page

        async def intercept(route, request):
            if not self._api_headers:
                self._api_headers = dict(request.headers)
            if not self._api_paginated_url:
                self._api_paginated_url = request.url
                self._api_method = request.method
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

        urls = (
            [f"https://www.woolworths.co.nz/shop/searchproducts?search={t}" for t in self._search_terms]
            if self._search_terms else CATEGORY_URLS
        )

        async def _register_routes() -> None:
            assert self._page
            await self._page.route("**/api/v1/products**", intercept)

        await _register_routes()

        for url in urls:
            captured.clear()
            self._api_paginated_url = ""
            self._api_method = "GET"
            self._api_post_data = None
            category = _category_from_url(url)
            logger.info(f"  Scraping: {url} [{category}]")
            try:
                await self._random_delay(DELAY_MIN, DELAY_MAX)
                await self._page.goto(url, wait_until="load", timeout=60_000)
                await self._page.wait_for_timeout(3000)
                await self._scroll_to_bottom()
                await self._page.wait_for_timeout(1000)

                # Determine total pages from first captured Woolworths products response
                total_pages = 1
                for entry in captured:
                    data = entry.get("data")
                    if not isinstance(data, dict):
                        continue
                    products_node = data.get("products")
                    if isinstance(products_node, dict):
                        total_items = products_node.get("totalItems") or 0
                        page_size = data.get("currentPageSize") or 48
                        if total_items:
                            total_pages = math.ceil(total_items / page_size)
                            logger.info(f"  [pagination] totalItems={total_items} pageSize={page_size} → {total_pages} pages method={self._api_method} url_captured={'yes' if self._api_paginated_url else 'NO'}")
                            break
                else:
                    logger.info(f"  [pagination] no products.totalItems found (url_captured={'yes' if self._api_paginated_url else 'NO'})")

                # Fetch pages 2..N directly using captured auth headers
                if total_pages > 1 and self._api_paginated_url and self._api_headers:
                    headers = {k: v for k, v in self._api_headers.items()
                               if k.lower() not in ("host", "content-length")}
                    for page_num in range(2, total_pages + 1):
                        try:
                            if self._api_method == "POST" and self._api_post_data is not None:
                                body = copy.deepcopy(self._api_post_data)
                                body["page"] = page_num
                                resp = await self._page.request.post(
                                    self._api_paginated_url, headers=headers, data=body
                                )
                            else:
                                page_url = re.sub(r'\bpage=\d+\b', f'page={page_num}', self._api_paginated_url)
                                if 'page=' not in self._api_paginated_url:
                                    sep = '&' if '?' in page_url else '?'
                                    page_url = f"{page_url}{sep}page={page_num}"
                                resp = await self._page.request.get(page_url, headers=headers)
                            if resp.ok:
                                resp_body = await resp.json()
                                captured.append({"url": self._api_paginated_url, "data": resp_body})
                                logger.info(f"  Fetched page {page_num}/{total_pages}")
                            elif resp.status == 500:
                                _retry_ok = False
                                for _attempt in range(1, 3):
                                    logger.warning(f"  Page {page_num} HTTP 500 — retrying ({_attempt}/2)...")
                                    await asyncio.sleep(3)
                                    try:
                                        if self._api_method == "POST" and self._api_post_data is not None:
                                            _rb = copy.deepcopy(self._api_post_data)
                                            _rb["page"] = page_num
                                            _rr = await self._page.request.post(
                                                self._api_paginated_url, headers=headers, data=_rb
                                            )
                                        else:
                                            _ru = re.sub(r'\bpage=\d+\b', f'page={page_num}', self._api_paginated_url)
                                            if 'page=' not in self._api_paginated_url:
                                                _sep = '&' if '?' in _ru else '?'
                                                _ru = f"{_ru}{_sep}page={page_num}"
                                            _rr = await self._page.request.get(_ru, headers=headers)
                                        if _rr.ok:
                                            captured.append({"url": self._api_paginated_url, "data": await _rr.json()})
                                            logger.info(f"  Fetched page {page_num}/{total_pages}")
                                            _retry_ok = True
                                            break
                                    except Exception as _re:
                                        logger.warning(f"  Page {page_num} retry {_attempt} exception: {_re}")
                                if not _retry_ok:
                                    logger.warning(f"  Page {page_num} HTTP 500 after 2 retries — stopping pagination")
                                    logger.warning(
                                        f"  [{self.store_name}] {url} truncated — "
                                        f"captured {(page_num - 1) * page_size}/{total_items} products"
                                    )
                                    break
                            else:
                                logger.warning(f"  Page {page_num} HTTP {resp.status} — stopping pagination")
                                logger.warning(
                                    f"  [{self.store_name}] {url} truncated — "
                                    f"captured {(page_num - 1) * page_size}/{total_items} products"
                                )
                                break
                        except Exception as e:
                            logger.warning(f"  Failed to fetch page {page_num}: {e}")
                        await asyncio.sleep(0.5)
                    # Fresh browser context so the next page.goto() doesn't hit Cloudflare
                    await self._fresh_context()
                    await _register_routes()
                    await self._random_delay(2.0, 4.0)

                logger.info(f"  Captured {len(captured)} responses")
                found = 0
                for entry in captured:
                    data = entry.get("data")
                    if not isinstance(data, dict):
                        continue
                    # Woolworths: {"products": {"items": [...]}}
                    products_node = data.get("products")
                    if isinstance(products_node, dict):
                        items = products_node.get("items") or []
                    elif isinstance(products_node, list):
                        items = products_node
                    else:
                        items = data.get("items") or []
                    for item in items:
                        p = self._parse_api_product(item, category)
                        if p:
                            products.append(p)
                            found += 1

                logger.info(f"  Extracted {found} products")
                if found == 0:
                    logger.warning(f"  No API products captured for {url} — check slug or API pattern")

            except Exception as e:
                logger.warning(f"  Failed to scrape {url}: {e}")

        return products

    def _parse_api_product(self, item: dict, category: str) -> Optional[ScrapedProduct]:
        if item.get("type") != "Product":
            return None

        raw_name = item.get("name")
        if not raw_name:
            return None

        # API returns lowercase names — title-case for display
        brand_raw = item.get("brand") or None
        brand = brand_raw.title() if brand_raw else None

        # Strip brand prefix from name if present, then title-case
        clean = raw_name
        if brand_raw and clean.lower().startswith(brand_raw.lower()):
            clean = clean[len(brand_raw):].strip()
        clean_name = clean.title() if clean else raw_name.title()

        price_data = item.get("price") or {}
        price = price_data.get("originalPrice")
        if not price or float(price) <= 0:
            return None

        sale = price_data.get("salePrice")
        try:
            special = float(sale) if sale and float(sale) < float(price) else None
        except (TypeError, ValueError):
            special = None

        size = item.get("size") or {}
        weight = size.get("volumeSize") or None

        # Include weight in raw_name so the matcher's weight-mismatch check can compare
        # e.g. "Beehive Ham Shaved Champagne 97% Fat Free" + "2 x 50g"
        # vs "Beehive Shaved Champagne Ham 100g" — correctly treated as different products
        titled = raw_name.title()
        raw_name_full = f"{titled} {weight}" if weight else titled

        barcode = item.get("barcode") or None

        return ScrapedProduct(
            store_name=self.store_name,
            raw_name=raw_name_full,
            price=float(price),
            category=category,
            special_price=special,
            image_url=(item.get("images") or {}).get("big"),
            brand=brand,
            weight=weight,
            clean_name=clean_name,
            barcode=barcode,
        )
