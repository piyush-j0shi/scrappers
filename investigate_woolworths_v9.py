"""Phase 9: try to set store directly via PUT /api/v1/fulfilment/my/methods/pickup."""
from __future__ import annotations
import asyncio, logging, json
from playwright.async_api import async_playwright
from base_scraper import USER_AGENT, EXTRA_HEADERS, STEALTH_SCRIPT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("v9")


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

        log.info("Establish session via homepage")
        await page.goto("https://www.woolworths.co.nz/", wait_until="load", timeout=60_000)
        await page.wait_for_timeout(3000)

        # Grab XSRF-TOKEN cookie for CSRF header
        cookies = await ctx.cookies()
        xsrf = next((c["value"] for c in cookies if c["name"] == "XSRF-TOKEN"), None)
        log.info(f"XSRF-TOKEN: {xsrf[:60] if xsrf else None}...")
        common_headers = {
            "content-type": "application/json",
            "accept": "application/json, text/plain, */*",
            "x-requested-with": "OnlineShopping.WebApp",
            "x-ui-ver": "7.73.30",
        }
        if xsrf:
            common_headers["x-xsrf-token"] = xsrf

        # Switch to pickup method first (empty body) so pickup-addresses returns data
        r0 = await page.request.put(
            "https://www.woolworths.co.nz/api/v1/fulfilment/my/methods/pickup",
            headers=common_headers, data="{}",
        )
        log.info(f"PUT pickup method status={r0.status}")

        await page.wait_for_timeout(1000)
        # Fetch pickup addresses so we have the catalog of store IDs
        r = await page.request.get(
            "https://www.woolworths.co.nz/api/v1/addresses/pickup-addresses",
            headers=common_headers,
        )
        if r.status != 200:
            log.info(f"  get text: {(await r.text())[:300]}")
        log.info(f"pickup-addresses status={r.status}")
        data = await r.json()
        # Find Ponsonby
        all_stores = []
        for area in data.get("storeAreas") or []:
            for s in area.get("storeAddresses") or []:
                all_stores.append(s)
        log.info(f"  total stores: {len(all_stores)}")
        ponsonby = next((s for s in all_stores if "Ponsonby" in (s.get("name") or "")), None)
        log.info(f"  Ponsonby match: {ponsonby}")
        if not ponsonby:
            return
        store_id = ponsonby["id"]  # 1996677 presumably

        # Set method to pickup first
        log.info("PUT pickup method empty")
        r = await page.request.put(
            "https://www.woolworths.co.nz/api/v1/fulfilment/my/methods/pickup",
            data={},
        )
        log.info(f"  status={r.status}")

        # Try SET store with body containing addressId
        async def try_payload(url, body, method="PUT"):
            try:
                r = await page.request.fetch(url, method=method,
                    headers=common_headers,
                    data=json.dumps(body))
                txt = (await r.text())[:400]
                log.info(f"  {method} {url} body={body} -> {r.status}  {txt[:200]}")
                return r.status
            except Exception as e:
                log.info(f"  ERR {e}")
                return None

        log.info(f"Trying variants with store_id={store_id}")
        for body in [
            {"addressId": store_id},
            {"storeId": store_id},
            {"id": store_id},
            {"addressId": store_id, "isPrimary": True},
            {"pickupAddressId": store_id},
        ]:
            await try_payload("https://www.woolworths.co.nz/api/v1/fulfilment/my/methods/pickup", body)

        for body in [
            {"addressId": store_id},
            {"storeId": store_id},
            {"id": store_id},
        ]:
            await try_payload(f"https://www.woolworths.co.nz/api/v1/fulfilment/my/pickup/address", body)
            await try_payload(f"https://www.woolworths.co.nz/api/v1/addresses/pickup-addresses/{store_id}", body)

        # Try POST setting pickup address
        for url in [
            f"https://www.woolworths.co.nz/api/v1/addresses/pickup-addresses/{store_id}/set-primary",
            f"https://www.woolworths.co.nz/api/v1/fulfilment/my/pickup/address/{store_id}",
        ]:
            try:
                r = await page.request.put(url, data={})
                log.info(f"  PUT {url} -> {r.status}")
            except Exception as e:
                log.info(f"  ERR {e}")
            try:
                r = await page.request.post(url, data={})
                log.info(f"  POST {url} -> {r.status}")
            except Exception as e:
                log.info(f"  ERR {e}")

        # Verify: fetch fruit-veg, look at banana price
        log.info("\nVerify via /api/v1/products:")
        r = await page.request.get("https://www.woolworths.co.nz/api/v1/products?dasFilter=Department%3B%3Bfruit-veg%3Bfalse&target=browse&inStockProductsOnly=false&size=48")
        log.info(f"status={r.status}")
        data = await r.json()
        total = (data.get("products") or {}).get("totalItems")
        items = (data.get("products") or {}).get("items") or []
        log.info(f"totalItems={total}")
        for it in items[:5]:
            if it.get("type") == "Product":
                log.info(f"  {it.get('name')!r} sku={it.get('sku')} ${(it.get('price') or {}).get('originalPrice')}")

        await ctx.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
