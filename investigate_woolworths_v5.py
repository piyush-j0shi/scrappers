"""
Phase 5: /bookatimeslot is the fulfilment/store picker. Drive it.
"""
from __future__ import annotations
import asyncio, json, logging, re
from pathlib import Path
from playwright.async_api import async_playwright
from base_scraper import USER_AGENT, EXTRA_HEADERS, STEALTH_SCRIPT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("v5")
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
                    try: txt = (await r.text())[:2000]
                    except: pass
                    xhrs.append({"url": r.url, "status": r.status, "method": r.request.method, "set_cookie": r.headers.get("set-cookie"), "body": txt})
            except: pass
        page.on("response", on_resp)

        async def on_req(rq):
            try:
                if "/api/" in rq.url and "woolworths.co.nz" in rq.url:
                    pd = rq.post_data
                    if pd:
                        log.info(f"  REQ-POST {rq.method} {rq.url}  body={pd[:200]}")
            except: pass
        page.on("request", on_req)

        log.info("--- /bookatimeslot ---")
        await page.goto("https://www.woolworths.co.nz/bookatimeslot", wait_until="load", timeout=60_000)
        await page.wait_for_timeout(6000)
        await page.screenshot(path=str(OUT / "bookatimeslot.png"), full_page=True)
        (OUT / "bookatimeslot.html").write_text(await page.content())

        # List all inputs and buttons visible
        log.info("Inputs on /bookatimeslot:")
        for i, inp in enumerate(await page.locator("input").all()):
            ph = await inp.get_attribute("placeholder")
            nm = await inp.get_attribute("name")
            tp = await inp.get_attribute("type")
            vis = await inp.is_visible()
            log.info(f"  [{i}] type={tp} name={nm!r} placeholder={ph!r} visible={vis}")

        log.info("Buttons on /bookatimeslot:")
        for i, b in enumerate(await page.locator("button").all()):
            try:
                txt = (await b.inner_text())[:60]
                vis = await b.is_visible()
                log.info(f"  [{i}] vis={vis}  {txt!r}")
            except: pass

        # Look for tabs: Pick up / Delivery
        log.info("Looking for tabs...")
        for sel in ["button:has-text('Pick up')", "button:has-text('Delivery')",
                    "[role='tab']:has-text('Pick')", "[role='tab']:has-text('Delivery')"]:
            c = await page.locator(sel).count()
            if c:
                log.info(f"  {c}× {sel}")

        # Click Pick up tab
        try:
            pu = page.locator("button:has-text('Pick up'), [role='tab']:has-text('Pick')").first
            if await pu.count() > 0:
                log.info("Clicking 'Pick up'")
                await pu.click()
                await page.wait_for_timeout(3000)
                await page.screenshot(path=str(OUT / "pickup_tab.png"), full_page=True)
        except Exception as e:
            log.info(f"pickup click err: {e}")

        # Now look again for inputs
        log.info("After Pickup click — inputs:")
        for i, inp in enumerate(await page.locator("input").all()):
            ph = await inp.get_attribute("placeholder")
            tp = await inp.get_attribute("type")
            vis = await inp.is_visible()
            log.info(f"  [{i}] type={tp} placeholder={ph!r} visible={vis}")

        # Type into any visible store search
        try:
            searchbox = None
            for inp in await page.locator("input[type=text], input[type=search]").all():
                if await inp.is_visible():
                    ph = await inp.get_attribute("placeholder") or ""
                    if any(k in ph.lower() for k in ("suburb","store","address","postcode","location","find")):
                        searchbox = inp
                        log.info(f"  Using input placeholder={ph!r}")
                        break
            if searchbox:
                await searchbox.click()
                await searchbox.type("Ponsonby", delay=80)
                await page.wait_for_timeout(3000)
                await page.screenshot(path=str(OUT / "typed_ponsonby.png"), full_page=True)
                # Dump suggestion list
                suggestions = await page.locator("[role='option'], li, .typeahead-option").all()
                for s in suggestions[:10]:
                    try:
                        t = (await s.inner_text())[:100]
                        if t.strip(): log.info(f"  SUGG: {t!r}")
                    except: pass
                # Click first Ponsonby option
                try:
                    opt = page.locator("text=/Ponsonby/i").first
                    if await opt.count() > 0:
                        await opt.click(timeout=5000)
                        await page.wait_for_timeout(3000)
                        await page.screenshot(path=str(OUT / "ponsonby_clicked.png"), full_page=True)
                except Exception as e:
                    log.info(f"  opt click err: {e}")
                # Look for "Shop here" button
                for sel in ["button:has-text('Shop here')","button:has-text('Select')",
                            "button:has-text('Confirm')","button:has-text('Choose')","button:has-text('Start')"]:
                    c = await page.locator(sel).count()
                    if c:
                        log.info(f"  {c}× {sel}")
                        try:
                            await page.locator(sel).first.click(timeout=5000)
                            await page.wait_for_timeout(3000)
                            log.info(f"  clicked {sel}")
                            break
                        except Exception as e:
                            log.info(f"  click err {sel}: {e}")
            else:
                log.info("No store-finder-like input visible")
        except Exception as e:
            log.info(f"search box err: {e}")

        await page.screenshot(path=str(OUT / "after_picker_final.png"), full_page=True)

        # Confirm: fetch products & see if price differs from default
        r = await page.request.get("https://www.woolworths.co.nz/api/v1/products?dasFilter=Department%3B%3Bfruit-veg%3Bfalse&target=browse&inStockProductsOnly=false&size=48")
        log.info(f"products status={r.status}")
        try:
            data = await r.json()
            items = (data.get("products") or {}).get("items") or []
            total = (data.get("products") or {}).get("totalItems")
            log.info(f"  totalItems={total}")
            for it in items[:3]:
                if it.get("type") == "Product":
                    log.info(f"  {it.get('name')!r} sku={it.get('sku')} ${(it.get('price') or {}).get('originalPrice')} sale=${(it.get('price') or {}).get('salePrice')}")
        except Exception as e:
            log.info(f"  json err: {e}")

        # Dump unique relevant XHRs
        log.info("\nUnique store/fulfilment XHRs (deduped by URL):")
        seen_u = set()
        for x in xhrs:
            if x["url"] in seen_u: continue
            seen_u.add(x["url"])
            if any(k in x["url"].lower() for k in ("fulfilment","store","pickup","delivery","address","location","customer","bootstrap","context")):
                log.info(f"  {x['status']} {x['url']}")
                if x.get("set_cookie"): log.info(f"    SET-COOKIE: {x['set_cookie'][:200]}")
                log.info(f"    body: {x['body'][:500]}")

        # Dump current store info from cookies & local storage
        cookies = await ctx.cookies()
        log.info(f"\nPost-pick cookies ({len(cookies)}):")
        for c in cookies:
            log.info(f"  {c['name']} = {c['value'][:120]}")

        await ctx.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
