"""
Woolworths NZ per-store pricing investigation — throwaway script.

Purpose
-------
Determine exactly how the Woolworths NZ website/API selects which store's
prices are returned, so the scraper can target every branch from a single
browser session rather than re-launching Chromium per store.

How to run
----------
    cd ~/projects/grocery_app/scrapers
    python3 investigate_woolworths.py 2>&1 | tee woolworths_investigation.log

Findings (after running this + investigate_woolworths_v2.py through v9)

    CORRECTION to the v1–v3 write-up below: the /shop/changestore URL is
    actually a 404 ("Oops! That didn't go to plan") — v1 drove the global
    navigation search box on the 404 page. The real fulfilment picker is
    at /bookatimeslot. Crucially, v8–v9 discovered the picker's
    underlying API call, which means THE UI IS NOT NEEDED AT ALL:

        PUT /api/v1/fulfilment/my/methods/pickup
            headers:  x-xsrf-token: <XSRF-TOKEN cookie value>
                      content-type: application/json
            body:     {"addressId": <api_store_id>}

    This binds the pickup store to the session. `addressId` here is the
    same integer as store_branches.api_store_id in our Supabase schema
    (confirmed: Woolworths Ponsonby = 1996677). All subsequent calls to
    /api/v1/products in that context return THAT store's catalog + prices.

    Caveats found while building the bootstrapper:
      * The PUT requires `x-xsrf-token` matching the `XSRF-TOKEN` cookie,
        otherwise the server returns 400.
      * `GET /api/v1/addresses/pickup-addresses` returns 400 unless the
        session has already been flipped to pickup — so you must PUT once
        with an empty body `{}` first to flip the method, THEN the GET
        works and enumerates all 362 pickup stores.
      * The PUT 200 response includes `addressCanNotBeServiced: false` —
        flip to true if the addressId is wrong, so we check that flag.

    See scrapers/bootstrap_woolworths_sessions.py for the production flow.
-------------------------------------------------------------------------
Original v1–v3 findings (left intact for reference — note the "must drive
UI" conclusion is superseded by the API-only path described above):
-------------------------------------------------------------------------
STORE SELECTION MECHANISM — server-side, NOT a URL parameter
    * `/api/v1/products` carries NO storeId query param and no store-identifying
      header in any observed call. The URL is the same across sessions:
          /api/v1/products?dasFilter=Department%3B%3B<slug>%3Bfalse
              &target=browse&inStockProductsOnly=false&size=48
    * Tested every obvious override; NONE work:
        - `?storeId=999` appended  → totalItems=0, server just returns
                                     promo tiles (param is ignored/rejected)
        - `x-store-id: 999` header → identical response to the baseline
                                     (header ignored)
        - Replaying the identical URL with post-changestore cookies →
          identical response (store wasn't actually switched by the visit)
    * The server resolves the active store from the session, keyed off the
      `ASP.NET_SessionId` + `browserSessionId` cookies. The same request
      returns whatever the session's stored "preferred store" is.
    * The /api/v2/dynamic-content response contains a component literally
      named "Regional Pricing alert" — confirming prices DO vary by store,
      and the site surfaces that difference in the UI.

HOW TO FORCE A SPECIFIC STORE FOR AN API CALL
    No direct API override exists. The store must be changed via the UI
    (/shop/changestore), which associates the chosen store with the
    current session cookies. After that, every /api/v1/products call
    made inside that session implicitly uses that store's prices.

    No flat /api/v1/stores, /api/v1/fulfilment/* or /api/v1/addresses/*
    endpoint responds — all probed variants return 404 (see v3 script).
    So a "headless POST the store ID" shortcut is not available; the
    scraper has to go through the real store-picker flow.

    Practical consequence for our scraper:
      * The browser context IS the store selector. Per-store scraping =
        one context per store, drive the store-picker once at startup,
        then the existing /api/v1/products pagination logic Just Works
        and will return that store's prices.
      * Store selection can be persisted by exporting Playwright's
        `context.storage_state()` AFTER the store-picker runs; subsequent
        runs can re-import it and skip the picker (until the session
        cookie expires — ASP.NET_SessionId is per-session).

    The store-picker flow (confirmed in v2) needs ONE of:
      (a) geolocation: grant location perms + set_geolocation(lat, lng),
          navigate to the landing page — site auto-selects nearest store;
      (b) address entry: type a suburb/postcode into the changestore
          Angular component's search input (this input renders late — wait
          for the SPA to hydrate, then drive the type-ahead and click the
          desired store's "Shop here" button).

CLOUDFLARE / AKAMAI CONSTRAINTS
    * Akamai Bot Manager is active. Observed cookies: `_abck`, `bm_sz`,
      `bm_sv`, `ak_bmsc`, `AKA_A2`, `akavpau_vpwww`. These are set on
      the initial HTML load and get updated by every /api/* response.
    * No Turnstile / challenge page was triggered during the investigation
      run (headless=True, stealth script applied). The existing scraper's
      `_fresh_context()` pattern remains a good idea when running many
      back-to-back API calls.
    * The Dynatrace `x-dtpc` header is auto-added per-request by RUM JS;
      it is NOT required for the API to respond (our page.request calls
      without it still returned 200).

-------------------------------------------------------------------------
The script runs four phases:
  1. Load the landing page and capture the first /api/v1/products request.
     Log its URL, every request header, and the full cookie jar.
  2. Navigate to /shop/changestore and switch to a store that is known to
     have different pricing from the default (e.g. Invercargill). Diff the
     cookie jar and any response headers against phase 1.
  3. Pick a single product SKU and hit /api/v1/products three ways:
       a) Using the captured headers/cookies from phase 1 (default store).
       b) Using the captured headers/cookies from phase 2 (changed store).
       c) Using phase 1 cookies but appending `&storeId=<other>` to the URL.
       d) Using phase 1 cookies but adding a plausible `x-store-id` header.
     Compare the price for that product across all four calls.
  4. Print a summary.

This file deliberately reuses the stealth/UA setup from base_scraper so
Cloudflare behaves the same as in the real scraper.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
from typing import Any, Optional

from playwright.async_api import async_playwright, BrowserContext, Page, Route, Request

# Mirror base_scraper's stealth/UA exactly so we see the same Cloudflare behaviour.
from base_scraper import USER_AGENT, EXTRA_HEADERS, STEALTH_SCRIPT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("investigate_woolworths")

BASE_URL = "https://www.woolworths.co.nz"
LANDING_URL = f"{BASE_URL}/shop/browse/fruit-veg"
CHANGESTORE_URL = f"{BASE_URL}/shop/changestore"

# Woolworths NZ exposes a store-picker API at /api/v1/addresses/pickupaddresses
# which returns every physical store. We'll fetch that first to pick two
# geographically distant stores to compare.
STORES_URL = f"{BASE_URL}/api/v1/addresses/pickupaddresses"


def _redact(s: Optional[str], keep: int = 6) -> str:
    if not s:
        return "<empty>"
    return f"{s[:keep]}…({len(s)} chars)"


def _summarise_cookies(cookies: list[dict[str, Any]]) -> dict[str, str]:
    return {c["name"]: c["value"] for c in cookies}


async def _capture_first_products_request(page: Page) -> dict[str, Any]:
    """Attach a route handler that records the first /api/v1/products request.
    Returns a dict once the request is seen, or times out."""
    state: dict[str, Any] = {"captured": None, "all_urls": []}

    async def handler(route: Route, request: Request) -> None:
        url = request.url
        state["all_urls"].append(url)
        if state["captured"] is None and "/api/v1/products" in url:
            state["captured"] = {
                "url": url,
                "method": request.method,
                "headers": dict(request.headers),
                "post_data": request.post_data,
            }
            logger.info(f"[capture] intercepted /api/v1/products: {url}")
        await route.continue_()

    await page.route("**/api/v1/**", handler)
    return state


async def _dump_context_state(ctx: BrowserContext, label: str) -> dict[str, Any]:
    cookies = await ctx.cookies()
    storage = await ctx.storage_state()
    cookie_map = _summarise_cookies(cookies)
    logger.info(f"[{label}] {len(cookies)} cookies — names: {sorted(cookie_map.keys())}")
    interesting = {
        k: v for k, v in cookie_map.items()
        if any(tok in k.lower() for tok in ("store", "fs", "address", "region", "cart", "auth", "session"))
    }
    if interesting:
        logger.info(f"[{label}] interesting cookies: {json.dumps(interesting, indent=2)}")
    # Compact origin-level storage summary
    origins = [o.get("origin") for o in storage.get("origins", [])]
    logger.info(f"[{label}] storage origins: {origins}")
    return {"cookies": cookies, "cookie_map": cookie_map, "storage": storage}


async def _try_api_with_headers(
    page: Page,
    label: str,
    url: str,
    headers: dict[str, str],
) -> dict[str, Any]:
    """Hit /api/v1/products using page.request (shares context cookies).
    Returns {status, total_items, first_products: [(name, price)]}."""
    # Strip hop-by-hop headers that Playwright may reject / double-set.
    clean = {k: v for k, v in headers.items() if k.lower() not in {
        "host", "content-length", "cookie", "accept-encoding", ":authority", ":method", ":path", ":scheme",
    }}
    try:
        resp = await page.request.get(url, headers=clean)
        status = resp.status
        body_text = ""
        data: Any = None
        try:
            data = await resp.json()
        except Exception:
            body_text = (await resp.text())[:400]
        logger.info(f"[{label}] GET {url[:120]}... -> {status}")
        summary: dict[str, Any] = {"status": status, "url": url}
        if isinstance(data, dict):
            products_node = data.get("products") or {}
            items = products_node.get("items") if isinstance(products_node, dict) else []
            total_items = products_node.get("totalItems") if isinstance(products_node, dict) else None
            summary["total_items"] = total_items
            summary["first_products"] = [
                {
                    "name": it.get("name"),
                    "sku": it.get("sku") or it.get("barcode") or it.get("productId"),
                    "price": (it.get("price") or {}).get("originalPrice"),
                    "sale": (it.get("price") or {}).get("salePrice"),
                }
                for it in (items or [])[:5]
            ]
            logger.info(f"[{label}] totalItems={total_items} first 3 products:")
            for p in summary["first_products"][:3]:
                logger.info(f"  - {p['name']!r}: ${p['price']} (sale ${p['sale']}) sku={p['sku']}")
        else:
            summary["body_preview"] = body_text
            logger.warning(f"[{label}] non-JSON response preview: {body_text}")
        return summary
    except Exception as e:
        logger.exception(f"[{label}] request failed: {e}")
        return {"status": None, "error": str(e)}


async def main() -> None:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--disable-http2",
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="en-NZ",
            timezone_id="Pacific/Auckland",
            extra_http_headers=EXTRA_HEADERS,
        )
        await ctx.add_init_script(STEALTH_SCRIPT)
        page = await ctx.new_page()
        page.set_default_timeout(60_000)

        # =====================================================================
        # PHASE 1 — baseline load, capture first /api/v1/products request
        # =====================================================================
        logger.info("=" * 78)
        logger.info("PHASE 1 — baseline load of %s", LANDING_URL)
        logger.info("=" * 78)

        cap_state = await _capture_first_products_request(page)
        try:
            await page.goto(LANDING_URL, wait_until="load", timeout=60_000)
        except Exception as e:
            logger.warning(f"[phase1] goto raised {e!r} — continuing")
        # Give the page time to fire its initial product calls.
        for _ in range(20):
            if cap_state["captured"]:
                break
            await page.wait_for_timeout(500)

        baseline_req = cap_state["captured"]
        if not baseline_req:
            logger.error("[phase1] NEVER captured /api/v1/products — dumping last 10 URLs seen:")
            for u in cap_state["all_urls"][-10:]:
                logger.error(f"  {u}")
        else:
            logger.info("[phase1] BASELINE REQUEST")
            logger.info("  url=%s", baseline_req["url"])
            logger.info("  method=%s", baseline_req["method"])
            logger.info("  headers (full):")
            for k, v in baseline_req["headers"].items():
                # cookies are usually a long single header — redact length but keep value short for analysis
                if k.lower() == "cookie":
                    logger.info(f"    {k}: <{len(v)} chars>  ({v[:200]}...)")
                else:
                    logger.info(f"    {k}: {v}")

        phase1_state = await _dump_context_state(ctx, "phase1")

        # =====================================================================
        # PHASE 2 — switch store via /shop/changestore
        # =====================================================================
        logger.info("=" * 78)
        logger.info("PHASE 2 — switch store via %s", CHANGESTORE_URL)
        logger.info("=" * 78)

        # First: enumerate available stores via the pickup-addresses API so we
        # can pick one that is definitely different from the default.
        stores_info: Any = None
        try:
            r = await page.request.get(STORES_URL)
            logger.info(f"[phase2] {STORES_URL} -> {r.status}")
            if r.ok:
                stores_info = await r.json()
                # Log just a name+id sample so the log isn't gigantic.
                if isinstance(stores_info, dict):
                    # Response shape is often {"storeAreas": [{...branches...}]}
                    logger.info(f"[phase2] store-list top-level keys: {list(stores_info.keys())}")
                    logger.info(f"[phase2] sample payload: {json.dumps(stores_info)[:1200]}")
        except Exception as e:
            logger.warning(f"[phase2] could not fetch store list: {e}")

        # Watch response headers on the changestore navigation and any XHRs it fires.
        xhr_log: list[dict[str, Any]] = []

        async def on_response(resp):
            try:
                if "/api/" in resp.url or "changestore" in resp.url.lower():
                    xhr_log.append({
                        "url": resp.url,
                        "status": resp.status,
                        "set_cookie": resp.headers.get("set-cookie"),
                        "content_type": resp.headers.get("content-type"),
                    })
            except Exception:
                pass

        page.on("response", on_response)

        try:
            await page.goto(CHANGESTORE_URL, wait_until="load", timeout=60_000)
            await page.wait_for_timeout(4000)
            # Heuristic: the page has a <Select a store> list. Try to click a
            # store that isn't the default. We'll search for text "Invercargill"
            # (far from Auckland — prices often differ).
            # If that fails, fall back to the second store tile on the page.
            try:
                target = page.locator("text=Invercargill").first
                if await target.count() > 0:
                    logger.info("[phase2] clicking store tile: Invercargill")
                    await target.click(timeout=5000)
                else:
                    logger.info("[phase2] 'Invercargill' not visible; clicking second store tile")
                    tiles = page.locator("[data-testid*='store'], button:has-text('Select')")
                    count = await tiles.count()
                    logger.info(f"[phase2] store-tile candidates: {count}")
                    if count >= 2:
                        await tiles.nth(1).click(timeout=5000)
                await page.wait_for_timeout(4000)
            except Exception as e:
                logger.warning(f"[phase2] store-click failed: {e}. Will proceed anyway.")
        except Exception as e:
            logger.warning(f"[phase2] goto changestore raised: {e!r}")

        page.remove_listener("response", on_response)

        phase2_state = await _dump_context_state(ctx, "phase2")

        # Diff cookie maps
        before = phase1_state["cookie_map"]
        after = phase2_state["cookie_map"]
        added = {k: after[k] for k in after if k not in before}
        removed = {k: before[k] for k in before if k not in after}
        changed = {k: (before[k], after[k]) for k in after if k in before and before[k] != after[k]}
        logger.info("[diff] cookies ADDED: %s", json.dumps(added, indent=2) if added else "none")
        logger.info("[diff] cookies REMOVED: %s", list(removed.keys()) if removed else "none")
        logger.info("[diff] cookies CHANGED: %s", json.dumps({k: {"was": v[0], "now": v[1]} for k, v in changed.items()}, indent=2) if changed else "none")

        # Dump only XHR responses with a set-cookie header — those usually carry the
        # store-selection side-effect.
        sc_entries = [e for e in xhr_log if e["set_cookie"]]
        logger.info("[phase2] %d XHRs carried Set-Cookie:", len(sc_entries))
        for e in sc_entries[:20]:
            logger.info(f"  {e['status']} {e['url']}")
            logger.info(f"    set-cookie: {e['set_cookie'][:400]}")

        # Capture the post-change /api/v1/products URL
        cap_state2 = await _capture_first_products_request(page)
        try:
            await page.goto(LANDING_URL, wait_until="load", timeout=60_000)
        except Exception as e:
            logger.warning(f"[phase2] re-goto landing raised {e!r}")
        for _ in range(20):
            if cap_state2["captured"]:
                break
            await page.wait_for_timeout(500)
        changed_req = cap_state2["captured"]
        if changed_req:
            logger.info("[phase2] POST-CHANGE REQUEST")
            logger.info("  url=%s", changed_req["url"])
            logger.info("  method=%s", changed_req["method"])
            for k, v in changed_req["headers"].items():
                if k.lower() == "cookie":
                    logger.info(f"    {k}: <{len(v)} chars>  ({v[:200]}...)")
                else:
                    logger.info(f"    {k}: {v}")

        # =====================================================================
        # PHASE 3 — try to force a specific store via query param / header
        # =====================================================================
        logger.info("=" * 78)
        logger.info("PHASE 3 — force-store experiments")
        logger.info("=" * 78)

        if baseline_req:
            baseline_url = baseline_req["url"]
            baseline_headers = baseline_req["headers"]

            # 3a — replay baseline exactly
            a = await _try_api_with_headers(page, "3a-baseline-replay", baseline_url, baseline_headers)

            # 3b — change-store cookies already in context; just replay same URL.
            # page.request uses the context cookie jar, so this naturally uses the
            # post-change cookie set.
            b = await _try_api_with_headers(page, "3b-post-change-same-url", baseline_url, baseline_headers)

            # 3c — append a storeId= query param to the baseline URL (with phase-1 cookies still in jar)
            sep = "&" if "?" in baseline_url else "?"
            c_url = f"{baseline_url}{sep}storeId=999"
            c = await _try_api_with_headers(page, "3c-query-param-storeId", c_url, baseline_headers)

            # 3d — add x-store-id header
            hdrs = dict(baseline_headers)
            hdrs["x-store-id"] = "999"
            d = await _try_api_with_headers(page, "3d-header-x-store-id", baseline_url, hdrs)

            logger.info("=" * 78)
            logger.info("SUMMARY of phase-3 price comparisons")
            logger.info("=" * 78)
            for label, r in (("3a-baseline-replay", a), ("3b-post-change-same-url", b),
                             ("3c-query-param-storeId", c), ("3d-header-x-store-id", d)):
                top = (r.get("first_products") or [{}])[0]
                logger.info(f"{label}: status={r.get('status')} totalItems={r.get('total_items')} "
                            f"first={top.get('name')!r} ${top.get('price')}")

        logger.info("Investigation complete. Check woolworths_investigation.log for the full record.")

        await ctx.close()
        await browser.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
