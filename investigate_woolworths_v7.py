"""Phase 7: tile click — use JS click on #method-pickup to bypass overlay."""
from __future__ import annotations
import asyncio, logging
from pathlib import Path
from playwright.async_api import async_playwright
from base_scraper import USER_AGENT, EXTRA_HEADERS, STEALTH_SCRIPT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("v7")
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

        xhr_urls = []
        async def on_resp(r):
            try:
                if "/api/" in r.url and "woolworths.co.nz" in r.url:
                    xhr_urls.append((r.request.method, r.status, r.url, (await r.text())[:400] if r.status < 400 else ""))
            except: pass
        page.on("response", on_resp)

        await page.goto("https://www.woolworths.co.nz/bookatimeslot", wait_until="load", timeout=60_000)
        await page.wait_for_timeout(4000)

        # 1. JS-click the pickup radio
        log.info("JS-click #method-pickup")
        await page.evaluate("document.querySelector('#method-pickup').click()")
        await page.wait_for_timeout(3000)
        await page.screenshot(path=str(OUT / "v7_pickup.png"), full_page=True)
        (OUT / "v7_pickup.html").write_text(await page.content())

        # 2. Dump visible inputs and their surrounding context
        log.info("All VISIBLE inputs:")
        for i, inp in enumerate(await page.locator("input").all()):
            if not await inp.is_visible(): continue
            ph = await inp.get_attribute("placeholder")
            tp = await inp.get_attribute("type")
            id_ = await inp.get_attribute("id")
            log.info(f"  [{i}] type={tp} id={id_!r} placeholder={ph!r}")

        # 3. Dump visible buttons to see store-picker UI
        log.info("Visible buttons:")
        for i, b in enumerate(await page.locator("button").all()):
            try:
                if not await b.is_visible(): continue
                txt = (await b.inner_text())[:80].replace("\n"," ")
                log.info(f"  [{i}] {txt!r}")
            except: pass

        # 4. Dump any form of search/typeahead visible
        for sel in ["[placeholder*='suburb' i]", "[placeholder*='search' i]",
                    "[placeholder*='postcode' i]", "[placeholder*='address' i]",
                    "[placeholder*='store' i]", "[placeholder*='find' i]",
                    "[aria-label*='store' i]", "[aria-label*='suburb' i]",
                    "store-search", "pickup-search", "app-store-search"]:
            c = await page.locator(sel).count()
            if c:
                log.info(f"  {c}× {sel}")

        # 5. Recent XHRs
        log.info("Latest fulfilment-related XHRs after pickup click:")
        for m, st, u, b in xhr_urls[-20:]:
            log.info(f"  {m} {st} {u}")

        await ctx.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
