"""
Phase 6: /bookatimeslot has Delivery/Pickup TILES (not buttons).
Click the Pick up tile, then search a suburb, pick a store, confirm.
"""
from __future__ import annotations
import asyncio, json, logging
from pathlib import Path
from playwright.async_api import async_playwright
from base_scraper import USER_AGENT, EXTRA_HEADERS, STEALTH_SCRIPT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("v6")
OUT = Path("/tmp/ww-investigate")


async def main() -> None:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--disable-http2","--disable-blink-features=AutomationControlled","--no-sandbox","--disable-dev-shm-usage","--disable-gpu"],
        )
        ctx = await browser.new_context(
            user_agent=USER_AGENT, viewport={"width":1366,"height":768},
            locale="en-NZ", timezone_id="Pacific/Auckland", extra_http_headers=EXTRA_HEADERS,
        )
        await ctx.add_init_script(STEALTH_SCRIPT)
        page = await ctx.new_page()
        page.set_default_timeout(60_000)

        xhrs = []
        async def on_resp(r):
            try:
                if "/api/" in r.url and "woolworths.co.nz" in r.url:
                    txt = ""
                    try: txt = (await r.text())[:2500]
                    except: pass
                    xhrs.append({"url": r.url, "status": r.status, "method": r.request.method,
                                 "post_data": r.request.post_data,
                                 "set_cookie": r.headers.get("set-cookie"), "body": txt})
            except: pass
        page.on("response", on_resp)

        log.info("Opening /bookatimeslot")
        await page.goto("https://www.woolworths.co.nz/bookatimeslot", wait_until="load", timeout=60_000)
        await page.wait_for_timeout(5000)

        log.info("Clicking 'Pick up' TILE")
        # The label/tile is clickable — try multiple selectors
        for sel in ["label:has-text('Pick up')", "div:has-text('Pick up')",
                    "[role='radio']:has-text('Pick up')",
                    "label:has(input[value='pickup'])", "input[value='pickup']"]:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    log.info(f"  trying {sel}")
                    await loc.click(timeout=4000)
                    await page.wait_for_timeout(2000)
                    break
            except Exception as e:
                log.info(f"  click {sel} err: {e}")

        await page.wait_for_timeout(3000)
        await page.screenshot(path=str(OUT / "v6_pickup_clicked.png"), full_page=True)
        (OUT / "v6_pickup.html").write_text(await page.content())

        # See what inputs appeared after clicking pickup
        log.info("Inputs after Pickup click:")
        for i, inp in enumerate(await page.locator("input").all()):
            if not await inp.is_visible(): continue
            ph = await inp.get_attribute("placeholder")
            tp = await inp.get_attribute("type")
            log.info(f"  [{i}] type={tp} placeholder={ph!r}")

        # Try to type in a non-global-nav search
        log.info("Locating pickup-store search...")
        target_input = None
        for inp in await page.locator("input[type=text], input[type=search]").all():
            if not await inp.is_visible(): continue
            ph = await inp.get_attribute("placeholder") or ""
            if "Search" == ph.strip():
                # skip global nav
                continue
            if any(k in ph.lower() for k in ("suburb","store","location","postcode","address","pickup")):
                target_input = inp
                log.info(f"  FOUND: placeholder={ph!r}")
                break
        if not target_input:
            # Pickup tile shows a store-search below it. Inspect any text input now.
            for inp in await page.locator("input").all():
                if not await inp.is_visible(): continue
                tp = await inp.get_attribute("type")
                if tp in ("text", "search"):
                    ph = await inp.get_attribute("placeholder") or ""
                    # The global nav search has placeholder 'Search' exactly; skip it.
                    if ph.strip() == "Search": continue
                    target_input = inp
                    log.info(f"  fallback input placeholder={ph!r}")
                    break

        if target_input:
            await target_input.click()
            await target_input.type("Ponsonby", delay=80)
            await page.wait_for_timeout(3500)
            await page.screenshot(path=str(OUT / "v6_typed.png"), full_page=True)
            # Click first matching suggestion
            try:
                # look for the store tile with "Ponsonby"
                sug = page.locator("text=/Woolworths Ponsonby/i").first
                if await sug.count() > 0:
                    log.info("  clicking 'Woolworths Ponsonby' option")
                    await sug.click(timeout=5000)
                else:
                    sug = page.locator("text=/Ponsonby/i").first
                    log.info("  fallback: clicking first Ponsonby text")
                    await sug.click(timeout=5000)
                await page.wait_for_timeout(4000)
                await page.screenshot(path=str(OUT / "v6_ponsonby_clicked.png"), full_page=True)
            except Exception as e:
                log.info(f"  sug click err: {e}")
        else:
            log.info("  NO suitable input found. Dumping page HTML.")
            (OUT / "v6_no_input.html").write_text(await page.content())

        # Try "Keep shopping" or similar confirm buttons
        for sel in ["button:has-text('Shop here')","button:has-text('Keep shopping')",
                    "button:has-text('Confirm')","button:has-text('Continue')",
                    "button:has-text('Start shopping')"]:
            c = await page.locator(sel).count()
            if c:
                log.info(f"  {c}× {sel} — clicking")
                try:
                    await page.locator(sel).first.click(timeout=5000)
                    await page.wait_for_timeout(3000)
                    break
                except Exception as e:
                    log.info(f"  err: {e}")

        await page.screenshot(path=str(OUT / "v6_final.png"), full_page=True)

        # Verify store by fetching products
        r = await page.request.get("https://www.woolworths.co.nz/api/v1/products?dasFilter=Department%3B%3Bfruit-veg%3Bfalse&target=browse&inStockProductsOnly=false&size=48")
        log.info(f"products status={r.status}")
        try:
            data = await r.json()
            total = (data.get("products") or {}).get("totalItems")
            items = (data.get("products") or {}).get("items") or []
            log.info(f"  totalItems={total}")
            for it in items[:5]:
                if it.get("type") == "Product":
                    log.info(f"  {it.get('name')!r} sku={it.get('sku')} ${(it.get('price') or {}).get('originalPrice')}")
        except Exception as e:
            log.info(f"  json err: {e}")

        # Print all relevant fulfilment XHRs
        log.info("\n-- fulfilment/store/address XHRs (unique) --")
        seen_u = set()
        for x in xhrs:
            if x["url"] in seen_u: continue
            seen_u.add(x["url"])
            if any(k in x["url"].lower() for k in ("fulfilment","store","pickup","delivery","address","location","bootstrap","context")):
                log.info(f"  {x['method']} {x['status']} {x['url']}")
                if x.get("post_data"): log.info(f"    POST: {x['post_data'][:250]}")
                if x.get("set_cookie"): log.info(f"    SET-COOKIE: {x['set_cookie'][:200]}")
                log.info(f"    BODY: {x['body'][:300]}")

        # Check localStorage for store info
        ls = await page.evaluate("Object.fromEntries(Object.keys(localStorage).map(k => [k, localStorage.getItem(k)]))")
        for k, v in ls.items():
            if any(t in k.lower() for t in ("store","fulfilment","address","pickup","branch")):
                log.info(f"  LS {k} = {str(v)[:300]}")

        await ctx.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
