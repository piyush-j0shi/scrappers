"""
New World scraper.

New World (Foodstuffs NZ) uses a React SPA backed by a JSON API.
This scraper intercepts API responses as the page navigates through
categories, with a DOM fallback if no responses are captured.

Stealth and HTTP/1.1 settings are handled in BaseScraper._setup().
A random 2–5 s delay is added between category loads.

NOTE: Site structure changes frequently. If the interceptor stops
      capturing data, open Chrome DevTools → Network, filter by XHR,
      and look for the updated API path pattern.
"""
from __future__ import annotations

import asyncio
import copy
import logging
import re
from typing import Optional

from base_scraper import BaseScraper, ScrapedProduct
from config import CHAIN_CONFIG

logger = logging.getLogger(__name__)

CATEGORY_URLS = [
    "https://www.newworld.co.nz/shop/category/fridge-deli-and-eggs",
    "https://www.newworld.co.nz/shop/category/bakery",
    "https://www.newworld.co.nz/shop/category/fruit-and-vegetables",
    "https://www.newworld.co.nz/shop/category/meat-poultry-and-seafood",
    "https://www.newworld.co.nz/shop/category/pantry",
    "https://www.newworld.co.nz/shop/category/hot-and-cold-drinks",
    "https://www.newworld.co.nz/shop/category/frozen",
    "https://www.newworld.co.nz/shop/category/health-and-body",
    "https://www.newworld.co.nz/shop/category/household-and-cleaning",
]

URL_CATEGORY: dict[str, str] = {
    "fridge-deli-and-eggs": "dairy",
    "bakery": "bakery",
    "meat-poultry-and-seafood": "meat",
    "fruit-and-vegetables": "produce",
    "pantry": "pantry",
    "hot-and-cold-drinks": "drinks",
    "frozen": "frozen",
    "health-and-body": "health",
    "household-and-cleaning": "household",
}

DELAY_MIN = 2.0
DELAY_MAX = 5.0



def _category_from_url(url: str) -> str:
    for slug, cat in URL_CATEGORY.items():
        if slug in url:
            return cat
    return "other"


class NewWorldScraper(BaseScraper):
    store_name = "New World"
    chain_slug = CHAIN_CONFIG["new_world"]["slug"]
    chain_name = CHAIN_CONFIG["new_world"]["name"]
    branch_name = CHAIN_CONFIG["new_world"]["branch_name"]

    async def scrape(self) -> list[ScrapedProduct]:
        products: list[ScrapedProduct] = []
        captured: list[dict] = []
        self._api_headers: dict[str, str] = {}
        self._api_paginated_url: str = ""
        self._api_method: str = "GET"
        self._api_post_data: dict | None = None

        assert self._page

        async def intercept(route, request):
            # Capture headers + method + body from paginated/products for reuse in barcode/pagination fetching
            if "paginated/products" in request.url:
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

        if self._search_terms:
            urls = [f"https://www.newworld.co.nz/shop/search?q={t}" for t in self._search_terms]
        elif self._categories:
            urls = [u for u in CATEGORY_URLS if any(c in u for c in self._categories)]
        else:
            urls = CATEGORY_URLS

        async def _register_routes() -> None:
            assert self._page
            await self._page.route("**/_next/data/**/shop/category/**", intercept)
            await self._page.route("**/paginated/products**", intercept)

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

                # Override store to New World New Lynn — Playwright has no session so
                # Foodstuffs defaults to a Metro store. Re-fetch page 1 with the right store.
                store_api_id = self._api_store_id or ""
                if store_api_id and self._api_post_data and self._api_paginated_url and self._api_headers:
                    store_field = next(
                        (f for f in ("storeId", "store_id") if f in self._api_post_data), None
                    )
                    if store_field and self._api_post_data.get(store_field) != store_api_id:
                        old_id = self._api_post_data[store_field]
                        self._api_post_data[store_field] = store_api_id
                        # Also patch the store ID embedded in algoliaQuery.filters
                        alg = self._api_post_data.get("algoliaQuery") or {}
                        if "filters" in alg and old_id in alg["filters"]:
                            alg["filters"] = alg["filters"].replace(old_id, store_api_id)
                        logger.info(f"  [store] storeId override: {old_id} → {store_api_id}")
                        # Drop page-1 data captured with wrong store and re-fetch
                        captured[:] = [e for e in captured if "paginated/products" not in e.get("url", "")]
                        _hdrs = {k: v for k, v in self._api_headers.items()
                                 if k.lower() not in ("host", "content-length")}
                        try:
                            p1_body = copy.deepcopy(self._api_post_data)
                            p1_body["page"] = 0
                            resp = await self._page.request.post(
                                self._api_paginated_url, headers=_hdrs, data=p1_body
                            )
                            if resp.ok:
                                p1_data = await resp.json()
                                captured.append({"url": self._api_paginated_url, "data": p1_data})
                                logger.info(f"  [store] Re-fetched page 0: totalHits={p1_data.get('totalHits')} totalPages={p1_data.get('totalPages')}")
                            else:
                                logger.warning(f"  [store] Re-fetch HTTP {resp.status}")
                        except Exception as e:
                            logger.warning(f"  [store] Re-fetch failed: {e}")
                    elif not store_field:
                        logger.info(f"  [store] storeId field not in POST body — keys: {list(self._api_post_data.keys())[:8]}")

                # Determine total pages from first captured paginated/products response
                total_pages = 1
                for entry in captured:
                    if "paginated/products" in entry.get("url", ""):
                        d = entry.get("data") or {}
                        total_pages = max(total_pages, d.get("totalPages") or 1)
                        post_summary = {k: v for k, v in (self._api_post_data or {}).items()
                                        if k not in ("algoliaUserToken", "userLocation")}
                        logger.info(f"  [pagination] totalPages={d.get('totalPages')} totalHits={d.get('totalHits')} post_body={post_summary}")
                        break
                else:
                    logger.info(f"  [pagination] no paginated/products entry in captured (url_captured={'yes' if self._api_paginated_url else 'NO'})")

                # Fetch pages 2..N directly using captured auth headers
                if total_pages > 1 and self._api_paginated_url and self._api_headers:
                    logger.info(f"  [pagination] method={self._api_method} post_data={'yes' if self._api_post_data else 'no'} pages 1..{total_pages - 1}")
                    headers = {k: v for k, v in self._api_headers.items()
                               if k.lower() not in ("host", "content-length")}
                    for page_num in range(1, total_pages):
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
                                    _total_hits = next(
                                        ((e.get("data") or {}).get("totalHits") or 0
                                         for e in captured if "paginated/products" in e.get("url", "")),
                                        0
                                    )
                                    logger.warning(f"  Page {page_num} HTTP 500 after 2 retries — stopping pagination")
                                    logger.warning(
                                        f"  [{self.store_name}] {url} truncated — captured "
                                        f"{(page_num - 1) * (_total_hits // total_pages if total_pages > 1 else 48)}"
                                        f"/{_total_hits} products"
                                    )
                                    break
                            else:
                                _total_hits = next(
                                    ((e.get("data") or {}).get("totalHits") or 0
                                     for e in captured if "paginated/products" in e.get("url", "")),
                                    0
                                )
                                logger.warning(f"  Page {page_num} HTTP {resp.status} — stopping pagination")
                                logger.warning(
                                    f"  [{self.store_name}] {url} truncated — captured "
                                    f"{(page_num - 1) * (_total_hits // total_pages if total_pages > 1 else 48)}"
                                    f"/{_total_hits} products"
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
                    data = entry.get("data") or {}
                    logger.info(f"  {entry['url'].split('?')[0][-60:]} → keys={list(data.keys())[:6]}")
                    items = (
                        data.get("products")
                        or data.get("items")
                        or (data.get("pageProps") or {}).get("products")
                        or []
                    )
                    for item in items:
                        p = self._parse_api_product(item, category)
                        if p:
                            products.append(p)
                            found += 1

                logger.info(f"  Extracted {found} products")
                if found == 0:
                    products.extend(await self._parse_dom(category))

            except Exception as e:
                logger.warning(f"  Failed to scrape {url}: {e}")

        await self._enrich_with_barcodes(products)
        return products

    def _parse_api_product(self, item: dict, category: str) -> Optional[ScrapedProduct]:
        # Foodstuffs API exposes brand, displayName (size/weight), and name (product name)
        # as distinct fields — use them directly instead of parsing from raw_name.
        clean_name = item.get("name") or None
        display_name = item.get("displayName") or None
        brand = item.get("brand") or None

        # raw_name: prefer "Brand + name + displayName" for matching/display fallback
        parts = [p for p in [brand, clean_name, display_name] if p]
        raw_name = " ".join(parts) if parts else (item.get("title") or "")
        if not raw_name:
            return None

        # Foodstuffs API returns prices in cents under singlePrice
        single_price = item.get("singlePrice") or {}
        price_cents = single_price.get("price") or single_price.get("originalPrice") or 0

        special_cents = (
            single_price.get("salePrice")
            or single_price.get("promotionPrice")
            or single_price.get("specialPrice")
        )

        # Some items have price=0 but a valid sale/promo price — use that as the price
        if not price_cents and special_cents:
            price_cents = special_cents
            special_cents = None

        price = float(price_cents) / 100

        try:
            special = float(special_cents) / 100 if special_cents and float(special_cents) < price_cents else None
        except (TypeError, ValueError):
            special = None

        if price <= 0:
            logger.debug(f"  [skip] {raw_name!r} — price=0, singlePrice={single_price}")
            return None

        return ScrapedProduct(
            store_name=self.store_name,
            raw_name=raw_name,
            price=price,
            category=category,
            special_price=special,
            image_url=(
                item.get("images", {}).get("main")
                if isinstance(item.get("images"), dict)
                else item.get("imageUrl")
            ),
            brand=brand or None,
            weight=display_name or None,
            clean_name=clean_name or None,
            product_id=item.get("productId") or None,
        )

    async def _enrich_with_barcodes(self, products: list[ScrapedProduct]) -> None:
        """Fetch product detail API for each unique productId to get the EAN barcode (sku)."""
        store_api_id = self._api_store_id or ""
        if not store_api_id:
            logger.warning("  [barcode] api_store_id not set for this branch — skipping barcode enrichment")
            return

        by_pid: dict[str, list[ScrapedProduct]] = {}
        for p in products:
            if p.product_id:
                by_pid.setdefault(p.product_id, []).append(p)

        logger.info(f"  [barcode] Fetching barcodes for {len(by_pid)} unique products...")
        hits = 0
        logged_first = False
        assert self._page
        headers = {k: v for k, v in self._api_headers.items()
                   if k.lower() not in ("host", "content-length")}
        for pid, items in by_pid.items():
            url = f"https://api-prod.newworld.co.nz/v1/edge/store/{store_api_id}/product/{pid}"
            try:
                resp = await self._page.request.get(url, headers=headers)
                if not logged_first:
                    logger.info(f"  [barcode] First response HTTP {resp.status}")
                    logged_first = True
                if resp.ok:
                    data = await resp.json()
                    barcode = data.get("sku")
                    if barcode:
                        for item in items:
                            item.barcode = str(barcode)
                        hits += 1
                    else:
                        names = [i.raw_name for i in items]
                        logger.info(f"  [barcode] No 'sku' for {pid} ({names}) — keys: {list(data.keys())[:10]}")
            except Exception as e:
                logger.warning(f"  [barcode] Failed for {pid}: {e}")
            await asyncio.sleep(0.1)

        logger.info(f"  [barcode] Got barcodes for {hits}/{len(by_pid)} products")

    async def _parse_dom(self, category: str) -> list[ScrapedProduct]:
        """DOM fallback — tries multiple selector patterns."""
        assert self._page
        products = []

        selectors = [
            ('[data-testid="product-card"]',  '[data-testid="product-title"]',  '[data-testid="product-price"]'),
            ('[class*="ProductCard"]',         '[class*="ProductName"]',         '[class*="ProductPrice"]'),
        ]

        for card_sel, name_sel, price_sel in selectors:
            cards = await self._page.query_selector_all(card_sel)
            if not cards:
                continue
            for card in cards:
                try:
                    name_el  = await card.query_selector(name_sel)
                    price_el = await card.query_selector(price_sel)
                    if not name_el or not price_el:
                        continue
                    name  = (await name_el.inner_text()).strip()
                    price = self.parse_price(await price_el.inner_text())
                    if name and price:
                        products.append(ScrapedProduct(
                            store_name=self.store_name,
                            raw_name=name,
                            price=price,
                            category=category,
                        ))
                except Exception:
                    pass
            if products:
                break

        return products
