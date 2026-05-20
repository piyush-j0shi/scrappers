"""
Phase 4: /shop/changestore is a 404. Find the real store-picker entrypoint.
"""
from __future__ import annotations
import asyncio, json, logging, re
from pathlib import Path
from playwright.async_api import async_playwright
from base_scraper import USER_AGENT, EXTRA_HEADERS, STEALTH_SCRIPT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("v4")
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
                    try: txt = (await r.text())[:1200]
                    except: pass
                    xhrs.append({"url": r.url, "status": r.status, "set_cookie": r.headers.get("set-cookie"), "body": txt})
            except: pass
        page.on("response", on_resp)

        log.info("--- Homepage: look for 'Change location' / 'Change store' links ---")
        await page.goto("https://www.woolworths.co.nz/", wait_until="load", timeout=60_000)
        await page.wait_for_timeout(4000)

        html = await page.content()
        # Find links containing keywords
        links = re.findall(r'href="([^"]+)"[^>]*>([^<]{0,80})', html)
        interesting = [(h, t) for h, t in links if any(k in (t + h).lower() for k in ("change", "store", "pickup", "delivery", "fulfilment", "location"))]
        log.info(f"{len(interesting)} potentially-relevant links")
        seen = set()
        for h, t in interesting[:60]:
            key = (h, t.strip())
            if key in seen: continue
            seen.add(key)
            log.info(f"  {h}  →  {t.strip()!r}")

        log.info("\n--- Click 'Change location' if present ---")
        for sel in [
            "a:has-text('Change location')",
            "a:has-text('Change store')",
            "button:has-text('Change location')",
            "button:has-text('Change')",
            "a:has-text('Pick up')",
            "a:has-text('Delivery')",
        ]:
            c = await page.locator(sel).count()
            log.info(f"  {c}× {sel}")

        try:
            await page.locator("a:has-text('Change location')").first.click(timeout=3000)
            await page.wait_for_timeout(4000)
            log.info(f"  after click, url = {page.url}")
            await page.screenshot(path=str(OUT / "after_change_location.png"), full_page=True)
            (OUT / "after_change_location.html").write_text(await page.content())
        except Exception as e:
            log.info(f"  no 'Change location' link clickable: {e}")

        log.info("\n--- Probe candidate URLs ---")
        for path in [
            "/shop/pickupordelivery",
            "/shop/fulfilment",
            "/shop/fulfilment/pickup",
            "/shop/fulfilment/delivery",
            "/shop/locate",
            "/shop/selectstore",
            "/shop/select-store",
            "/shop/findstore",
            "/shop/find-store",
            "/shop/storelocator",
            "/shop/stores",
        ]:
            try:
                r = await page.goto(f"https://www.woolworths.co.nz{path}", wait_until="load", timeout=20_000)
                status = r.status if r else None
                title = await page.title()
                log.info(f"  {status} {path} — title={title!r}")
                if status and status < 400:
                    await page.screenshot(path=str(OUT / f"probe_{path.replace('/','_')}.png"), full_page=True)
            except Exception as e:
                log.info(f"  ERR {path}: {e}")
            await page.wait_for_timeout(500)

        log.info("\n--- Relevant XHRs seen ---")
        for x in xhrs:
            u = x["url"]
            if any(k in u.lower() for k in ("fulfilment","store","pickup","delivery","address","location","customer")):
                log.info(f"  {x['status']} {u}")
                log.info(f"    body: {x['body'][:400]}")

        await ctx.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
