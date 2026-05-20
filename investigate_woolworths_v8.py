"""Phase 8: capture the PUT /api/v1/fulfilment/my/methods/pickup body,
and find the endpoint that sets a specific pickup store."""
from __future__ import annotations
import asyncio, logging, json
from pathlib import Path
from playwright.async_api import async_playwright
from base_scraper import USER_AGENT, EXTRA_HEADERS, STEALTH_SCRIPT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("v8")
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

        # log BOTH request bodies AND response bodies for fulfilment calls
        requests_seen = []
        def on_req(req):
            if "/api/v1/fulfilment" in req.url or "/api/v1/address" in req.url:
                requests_seen.append({
                    "ts": "req", "method": req.method, "url": req.url,
                    "post_data": req.post_data, "headers": dict(req.headers),
                })
        page.on("request", on_req)

        responses_seen = []
        async def on_resp(r):
            if "/api/v1/fulfilment" in r.url or "/api/v1/address" in r.url:
                txt = ""
                try: txt = (await r.text())[:2000]
                except: pass
                responses_seen.append({
                    "ts": "resp", "method": r.request.method, "url": r.url,
                    "status": r.status, "body": txt,
                })
        page.on("response", on_resp)

        log.info("Open /bookatimeslot")
        await page.goto("https://www.woolworths.co.nz/bookatimeslot", wait_until="load", timeout=60_000)
        await page.wait_for_timeout(4000)

        log.info("JS-click #method-pickup")
        await page.evaluate("document.querySelector('#method-pickup').click()")
        await page.wait_for_timeout(4000)

        log.info("Click 'Change store' button")
        try:
            await page.evaluate("""() => {
                const btns = Array.from(document.querySelectorAll('button'));
                const b = btns.find(x => x.textContent.trim() === 'Change store');
                if (b) b.click();
            }""")
            await page.wait_for_timeout(4000)
            await page.screenshot(path=str(OUT / "v8_change_store.png"), full_page=True)
            (OUT / "v8_change_store.html").write_text(await page.content())
        except Exception as e:
            log.info(f"change-store click err: {e}")

        # What inputs & buttons are visible now?
        log.info("Visible inputs after 'Change store':")
        for i, inp in enumerate(await page.locator("input").all()):
            if not await inp.is_visible(): continue
            ph = await inp.get_attribute("placeholder")
            tp = await inp.get_attribute("type")
            id_ = await inp.get_attribute("id")
            log.info(f"  [{i}] type={tp} id={id_!r} placeholder={ph!r}")

        # Try typing Ponsonby into any suitable new input
        typed = False
        for inp in await page.locator("input").all():
            if not await inp.is_visible(): continue
            tp = await inp.get_attribute("type")
            ph = (await inp.get_attribute("placeholder") or "").lower()
            id_ = (await inp.get_attribute("id") or "")
            if tp in ("text","search") and id_ != "search" and ph != "search":
                log.info(f"  typing into id={id_!r} placeholder={ph!r}")
                await inp.click()
                await inp.type("Ponsonby", delay=80)
                await page.wait_for_timeout(3000)
                await page.screenshot(path=str(OUT / "v8_typed.png"), full_page=True)
                typed = True
                break
        if not typed:
            log.info("No new store-search input visible; using postMessage/evaluate fallback impossible without knowing component")

        # Click any option containing Ponsonby
        for sel in ["text=/Woolworths Ponsonby/", "button:has-text('Ponsonby')",
                    "[role='option']:has-text('Ponsonby')"]:
            c = await page.locator(sel).count()
            if c:
                log.info(f"  clicking {sel} ({c} match)")
                try:
                    await page.locator(sel).first.click(timeout=5000)
                    await page.wait_for_timeout(3000)
                    break
                except Exception as e:
                    log.info(f"  err: {e}")

        await page.screenshot(path=str(OUT / "v8_after_click_ponsonby.png"), full_page=True)

        # Click any "Shop here" / "Select" / confirm button
        for sel in ["button:has-text('Shop here')","button:has-text('Select')",
                    "button:has-text('Choose store')","button:has-text('Confirm')"]:
            c = await page.locator(sel).count()
            if c:
                log.info(f"  clicking {sel}")
                try:
                    await page.locator(sel).first.click(timeout=5000)
                    await page.wait_for_timeout(4000)
                except Exception as e:
                    log.info(f"  err {sel}: {e}")

        # Dump everything we captured
        log.info("\n=== CAPTURED REQUESTS ===")
        for e in requests_seen:
            log.info(f"  REQ {e['method']} {e['url']}")
            if e.get("post_data"): log.info(f"    body: {e['post_data']}")
        log.info("\n=== CAPTURED RESPONSES ===")
        for e in responses_seen:
            log.info(f"  RESP {e['method']} {e['status']} {e['url']}")
            log.info(f"    body: {e['body'][:500]}")

        # Finally: verify the session now has the picked store by fetching products
        r = await page.request.get("https://www.woolworths.co.nz/api/v1/products?dasFilter=Department%3B%3Bfruit-veg%3Bfalse&target=browse&inStockProductsOnly=false&size=48")
        log.info(f"\nproducts status={r.status}")
        try:
            data = await r.json()
            total = (data.get("products") or {}).get("totalItems")
            items = (data.get("products") or {}).get("items") or []
            log.info(f"  totalItems={total}")
            for it in items[:5]:
                if it.get("type") == "Product":
                    log.info(f"  {it.get('name')!r} sku={it.get('sku')} ${(it.get('price') or {}).get('originalPrice')} sale={(it.get('price') or {}).get('salePrice')}")
        except Exception as e:
            log.info(f"  json err: {e}")

        await ctx.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
