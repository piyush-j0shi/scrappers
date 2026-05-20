"""
Diagnostic: load Foodstuffs (New World + Pak'nSave) through a proxy and
report exactly WHAT comes back — status code, page title, response headers,
body excerpt — so we can tell Cloudflare 403 vs age-gate vs anything else.
"""
from __future__ import annotations
import asyncio, sys
from playwright.async_api import async_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

PROXY = "http://23.236.236.35:8800"
TARGETS = [
    "https://www.newworld.co.nz/shop/category/bakery",
    "https://www.paknsave.co.nz/shop/category/bakery",
]

async def diag(url: str):
    print(f"\n{'='*60}\n  {url}\n{'='*60}")
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            proxy={"server": PROXY},
            args=["--disable-http2", "--disable-blink-features=AutomationControlled",
                  "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        ctx = await browser.new_context(user_agent=UA, locale="en-NZ", viewport={"width":1366,"height":900})
        page = await ctx.new_page()

        # Capture ALL responses to see what proxied traffic actually returns
        responses = []
        page.on("response", lambda r: responses.append({
            "url": r.url, "status": r.status,
            "server": r.headers.get("server"),
            "cf_ray": r.headers.get("cf-ray"),
            "cf_cache_status": r.headers.get("cf-cache-status"),
        }))

        try:
            resp = await page.goto(url, wait_until="load", timeout=45_000)
            nav_status = resp.status if resp else None
            nav_headers = dict(resp.headers) if resp else {}
            await page.wait_for_timeout(2500)

            title = await page.title()
            body_text = await page.evaluate(
                "document.body ? document.body.innerText.substring(0, 400) : 'NO BODY'"
            )

            print(f"navigation status: {nav_status}")
            print(f"  server header:   {nav_headers.get('server')}")
            print(f"  cf-ray:          {nav_headers.get('cf-ray')}")
            print(f"  page title:      {title!r}")
            print(f"  body text excerpt:")
            for line in body_text.split('\n')[:8]:
                print(f"    | {line.strip()[:100]}")

            # Look at all responses — were any /paginated/products called?
            api_calls = [r for r in responses if "paginated/products" in r["url"]]
            print(f"\n  total responses: {len(responses)}")
            print(f"  /paginated/products calls: {len(api_calls)}")
            if api_calls:
                for c in api_calls[:3]:
                    print(f"    - status={c['status']} {c['url'][:80]}")

            # Distribution of response statuses
            from collections import Counter
            status_dist = Counter(r["status"] for r in responses)
            print(f"  status code distribution: {dict(status_dist)}")
        except Exception as e:
            print(f"navigation FAILED: {e}")
        finally:
            await browser.close()

async def main():
    print(f"Using proxy: {PROXY}")
    for url in TARGETS:
        await diag(url)

asyncio.run(main())
