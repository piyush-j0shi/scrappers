"""
Woolworths NZ per-store investigation — phase 3 (endpoint probe).

Goal: find the exact endpoint that (a) lists stores and (b) mutates the
session's selected store. Phase 2 rendered the changestore page but the
Angular store-finder component needed JS interaction we couldn't reliably
drive headlessly. So instead we brute-probe a small set of plausible
endpoints, read localStorage/sessionStorage for hints, and try to extract
a real store ID from window state.
"""
from __future__ import annotations

import asyncio
import json
import logging
from playwright.async_api import async_playwright
from base_scraper import USER_AGENT, EXTRA_HEADERS, STEALTH_SCRIPT

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("v3")

BASE = "https://www.woolworths.co.nz"

CANDIDATE_ENDPOINTS = [
    # Read-type endpoints
    "/api/v1/stores",
    "/api/v1/stores?size=200",
    "/api/v1/store",
    "/api/v1/addresses/stores",
    "/api/v1/addresses",
    "/api/v1/fulfilment/stores",
    "/api/v1/fulfilment/shop-stores",
    "/api/v1/fulfilment/pickup-stores",
    "/api/v1/shoppingLists/stores",
    "/api/v1/shell/stores",
    "/api/v1/locations",
    "/api/v1/customer/stores",
    "/api/v1/addresses/pickupaddresses",
    "/api/v1/addresses/pickupareas",
    "/api/v1/addresses/deliveryareas",
    # Fulfilment-pref endpoints (often drive store selection server-side)
    "/api/v1/fulfilment",
    "/api/v1/fulfilment/info",
    "/api/v1/fulfilment/currentmethod",
    "/api/v1/fulfilment/pickupstore",
    "/api/v1/shoppers/fulfilment",
    "/api/v1/changestore",
    "/api/v1/ChangeStore",
    "/api/v1/ChangeStore/1",
]

WINDOW_PROBES = [
    "Object.keys(localStorage)",
    "Object.fromEntries(Object.keys(localStorage).map(k => [k, localStorage.getItem(k)]))",
    "Object.fromEntries(Object.keys(sessionStorage).map(k => [k, sessionStorage.getItem(k)]))",
    # Angular sets some config on window
    "window.appSettings && JSON.stringify(window.appSettings).slice(0, 4000)",
    "window.__INITIAL_STATE__ && JSON.stringify(window.__INITIAL_STATE__).slice(0, 4000)",
    "document.cookie",
]


async def probe(page, path: str) -> dict:
    try:
        r = await page.request.get(f"{BASE}{path}")
        status = r.status
        body = (await r.text())[:400]
        return {"path": path, "status": status, "body": body}
    except Exception as e:
        return {"path": path, "status": None, "error": str(e)}


async def main() -> None:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--disable-http2", "--disable-blink-features=AutomationControlled",
                  "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        ctx = await browser.new_context(
            user_agent=USER_AGENT, viewport={"width": 1366, "height": 768},
            locale="en-NZ", timezone_id="Pacific/Auckland",
            extra_http_headers=EXTRA_HEADERS,
        )
        await ctx.add_init_script(STEALTH_SCRIPT)
        page = await ctx.new_page()
        page.set_default_timeout(60_000)

        log.info("Establishing session...")
        await page.goto(f"{BASE}/shop/browse/fruit-veg", wait_until="load", timeout=60_000)
        await page.wait_for_timeout(3000)

        log.info("=== Probe endpoints ===")
        for path in CANDIDATE_ENDPOINTS:
            r = await probe(page, path)
            mark = "OK " if r["status"] and r["status"] < 400 else "XX "
            log.info(f"{mark} {r['status']:<4} {path}")
            if r.get("status") and r["status"] < 400:
                log.info(f"     body: {r['body'][:300]}")

        log.info("=== Window state probes (on /shop/changestore) ===")
        await page.goto(f"{BASE}/shop/changestore", wait_until="load", timeout=60_000)
        await page.wait_for_timeout(5000)
        for expr in WINDOW_PROBES:
            try:
                val = await page.evaluate(expr)
                log.info(f"{expr}")
                log.info(f"   -> {str(val)[:600]}")
            except Exception as e:
                log.info(f"{expr} -> err {e}")

        # Try to read the Angular component state for store info
        log.info("=== Look for store-ID-like strings in changestore HTML (post-SPA render) ===")
        content = await page.content()
        import re
        for pat in [
            r"storeId[\"']?\s*[:=]\s*[\"']?(\d+)",
            r'"storeNumber"\s*:\s*"?(\d+)"?',
            r'"storeId"\s*:\s*"?(\d+)"?',
            r"currentStore[\"']?\s*[:=]",
            r'"primaryAddress"[^}]{0,500}',
            r'"pickupAddress"[^}]{0,500}',
        ]:
            matches = re.findall(pat, content)[:5]
            if matches:
                log.info(f"  pattern {pat!r} matched {len(matches)} → {matches}")

        # Try the typeahead / search inputs again — but this time target the
        # changestore page's own input (there are 2 inputs on the page now).
        log.info("=== Inputs on the /shop/changestore page ===")
        inputs = await page.locator("input").all()
        for i, inp in enumerate(inputs):
            ph = await inp.get_attribute("placeholder")
            nm = await inp.get_attribute("name")
            tp = await inp.get_attribute("type")
            log.info(f"  [{i}] type={tp} name={nm!r} placeholder={ph!r}")

        await ctx.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
