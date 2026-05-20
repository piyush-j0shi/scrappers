"""
Woolworths NZ per-store pricing investigation — phase 2 (deeper dig).

Phase 1 (investigate_woolworths.py) proved:
  * /api/v1/products URL has NO storeId parameter at all
  * Adding ?storeId=999 or x-store-id header has no effect (or returns 0 items)
  * The cookie jar has no obvious store-selection cookie
  * /shop/changestore rendered no store tiles — we never actually switched store

Conclusion so far: store selection is SERVER-SIDE, keyed off the ASP.NET session
(ASP.NET_SessionId / browserSessionId), and the API call is implicitly scoped to
whatever store the session is associated with.

This follow-up script tries to actually change the store so we can observe the
store-mutation request and its response. It:

  1. Navigates to /shop/changestore
  2. Dumps the rendered HTML + screenshot so we can see the UI
  3. Records every XHR the page fires (URL, method, post-body, response, set-cookie)
  4. Types a suburb ("Invercargill") into the store-finder and clicks the first
     result. Records the mutation XHRs that fire.
  5. Re-visits the landing page and diffs the /api/v1/products response for the
     banana product against the phase-1 price to confirm store actually changed.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path

from playwright.async_api import async_playwright

from base_scraper import USER_AGENT, EXTRA_HEADERS, STEALTH_SCRIPT

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("investigate_v2")

BASE_URL = "https://www.woolworths.co.nz"
LANDING_URL = f"{BASE_URL}/shop/browse/fruit-veg"
CHANGESTORE_URL = f"{BASE_URL}/shop/changestore"

OUT = Path("/tmp/ww-investigate")
OUT.mkdir(exist_ok=True, parents=True)


async def main() -> None:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--disable-http2", "--disable-blink-features=AutomationControlled",
                  "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
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

        # Record every request + response for later inspection.
        xhr_log: list[dict] = []

        def on_request(req):
            if "woolworths.co.nz/api" in req.url or "changestore" in req.url.lower():
                xhr_log.append({
                    "dir": "req",
                    "url": req.url,
                    "method": req.method,
                    "post_data": req.post_data,
                    "headers": dict(req.headers),
                })

        async def on_response(resp):
            try:
                if "woolworths.co.nz/api" in resp.url:
                    body_preview = None
                    try:
                        text = await resp.text()
                        body_preview = text[:600]
                    except Exception:
                        pass
                    xhr_log.append({
                        "dir": "resp",
                        "url": resp.url,
                        "status": resp.status,
                        "set_cookie": resp.headers.get("set-cookie"),
                        "body_preview": body_preview,
                    })
            except Exception:
                pass

        page.on("request", on_request)
        page.on("response", on_response)

        # Step 1: land on fruit-veg to establish a session + capture default-store price
        logger.info("Step 1: landing page to establish session")
        await page.goto(LANDING_URL, wait_until="load", timeout=60_000)
        await page.wait_for_timeout(3000)

        default_banana = None
        for e in xhr_log:
            if e["dir"] == "resp" and "/api/v1/products" in e["url"] and e.get("body_preview"):
                try:
                    data = json.loads(e["body_preview"])  # may be truncated
                except Exception:
                    continue
        # Fetch fresh from request context for an accurate default price
        r = await page.request.get(
            f"{BASE_URL}/api/v1/products?dasFilter=Department%3B%3Bfruit-veg%3Bfalse&target=browse&inStockProductsOnly=false&size=48"
        )
        data = await r.json()
        items = (data.get("products") or {}).get("items") or []
        # Grab the loose bananas — sku 133211 from phase 1
        default_banana = next((it for it in items if it.get("sku") == 133211), None)
        logger.info(f"[default-store] bananas loose: ${(default_banana or {}).get('price', {}).get('originalPrice') if default_banana else 'MISSING'}")

        # Step 2: go to /shop/changestore, dump HTML + screenshot
        logger.info("Step 2: /shop/changestore — dump HTML + screenshot")
        await page.goto(CHANGESTORE_URL, wait_until="load", timeout=60_000)
        await page.wait_for_timeout(4000)
        html = await page.content()
        (OUT / "changestore.html").write_text(html)
        await page.screenshot(path=str(OUT / "changestore.png"), full_page=True)
        logger.info(f"  html length={len(html)} saved to {OUT / 'changestore.html'}")

        # Step 3: look for search/input elements & known store-picker IDs
        logger.info("Step 3: inspect DOM for store-picker")
        probes = [
            "input[type=search]",
            "input[type=text]",
            "input[placeholder*='store' i]",
            "input[placeholder*='suburb' i]",
            "input[placeholder*='postcode' i]",
            "input[placeholder*='address' i]",
            "button:has-text('Find')",
            "button:has-text('Search')",
            "[data-testid*='store' i]",
            "[data-testid*='search' i]",
        ]
        for sel in probes:
            try:
                c = await page.locator(sel).count()
                if c:
                    logger.info(f"  {c:3d}× {sel}")
            except Exception:
                pass

        # Step 4: type into a search input if one exists
        logger.info("Step 4: try typing 'Invercargill' into a search input")
        try:
            sb = page.locator("input[type=text], input[type=search]").first
            if await sb.count() > 0:
                await sb.click()
                await sb.type("Invercargill", delay=80)
                await page.wait_for_timeout(2500)
                await page.screenshot(path=str(OUT / "after_type.png"), full_page=True)
                # Click first visible suggestion
                sug = page.locator("li, [role='option'], button").filter(has_text="Invercargill").first
                if await sug.count() > 0:
                    logger.info("  clicking suggestion containing 'Invercargill'")
                    await sug.click()
                    await page.wait_for_timeout(3000)
                else:
                    logger.info("  no suggestion matched — clicking search/Enter")
                    await sb.press("Enter")
                    await page.wait_for_timeout(3000)
                await page.screenshot(path=str(OUT / "after_search.png"), full_page=True)
                # Click first visible store-select button
                pick = page.locator("button:has-text('Shop'), button:has-text('Choose'), button:has-text('Select'), button:has-text('Change')").first
                if await pick.count() > 0:
                    logger.info("  clicking first Shop/Select button")
                    await pick.click()
                    await page.wait_for_timeout(4000)
            else:
                logger.warning("  NO text/search input found on changestore")
        except Exception as e:
            logger.warning(f"  search flow raised: {e}")

        await page.screenshot(path=str(OUT / "after_click.png"), full_page=True)

        # Step 5: re-fetch fruit-veg to see if the banana price changed
        logger.info("Step 5: re-fetch fruit-veg — compare banana price")
        r = await page.request.get(
            f"{BASE_URL}/api/v1/products?dasFilter=Department%3B%3Bfruit-veg%3Bfalse&target=browse&inStockProductsOnly=false&size=48"
        )
        data = await r.json()
        items = (data.get("products") or {}).get("items") or []
        new_banana = next((it for it in items if it.get("sku") == 133211), None)
        logger.info(f"[post-change-attempt] bananas loose: ${(new_banana or {}).get('price', {}).get('originalPrice') if new_banana else 'MISSING'}")

        # Step 6: write the full XHR log
        (OUT / "xhr_log.json").write_text(json.dumps(xhr_log, indent=2, default=str))
        logger.info(f"XHR log ({len(xhr_log)} entries) -> {OUT / 'xhr_log.json'}")

        # Dump a narrow report: only store-related XHRs and any Set-Cookie responses
        store_related = [
            e for e in xhr_log
            if any(tok in e.get("url", "").lower() for tok in (
                "changestore", "store", "fulfilment", "customerlocation",
                "addresses", "pickup", "delivery", "preferences", "session"
            ))
        ]
        logger.info("Store-related XHRs:")
        for e in store_related[:50]:
            if e["dir"] == "req":
                logger.info(f"  REQ  {e['method']} {e['url']}  body={e.get('post_data')!r:.200}")
            else:
                sc = e.get("set_cookie") or ""
                preview = (e.get("body_preview") or "")[:200]
                logger.info(f"  RESP {e['status']} {e['url']}  set-cookie={sc[:150]!r}  body={preview!r}")

        sc_entries = [e for e in xhr_log if e["dir"] == "resp" and e.get("set_cookie")]
        logger.info(f"All responses that issued Set-Cookie ({len(sc_entries)}):")
        for e in sc_entries[:30]:
            logger.info(f"  {e['status']} {e['url']}  set-cookie={e['set_cookie'][:200]!r}")

        await ctx.close()
        await browser.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
