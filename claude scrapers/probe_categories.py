"""Probe live category navigation on Woolworths, New World, Pak'nSave.

Loads each retailer's homepage in Playwright, extracts every link that points
to /shop/browse/<slug> or /shop/category/<slug>, and prints them sorted.
Use this to confirm the scraper's hardcoded category list still matches reality.
"""
from __future__ import annotations
import asyncio, re
from playwright.async_api import async_playwright

SITES = [
    # Woolworths: homepage doesn't render shop links until JS runs.
    # Use a known browse page so the side-nav renders fully.
    ("Woolworths", "https://www.woolworths.co.nz/shop/browse/fruit-veg", r"/shop/browse/([a-z0-9-]+)"),
    ("New World",  "https://www.newworld.co.nz/shop/category/fruit-and-vegetables", r"/shop/category/([a-z0-9-]+)"),
    ("Pak'nSave",  "https://www.paknsave.co.nz/shop/category/fruit-and-vegetables", r"/shop/category/([a-z0-9-]+)"),
]

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

async def probe(label: str, url: str, slug_re: str) -> set[str]:
    pat = re.compile(slug_re)
    found: set[str] = set()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-http2"])
        ctx = await browser.new_context(user_agent=UA, locale="en-NZ", viewport={"width":1366,"height":900})
        page = await ctx.new_page()
        try:
            await page.goto(url, wait_until="load", timeout=60_000)
            await page.wait_for_timeout(3500)
            # Try to open the main "Shop" / "Browse" mega-menu so dropdown links render
            for sel in ["button:has-text('Shop')", "a:has-text('Shop')", "button:has-text('Browse')"]:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.hover()
                        await page.wait_for_timeout(800)
                        break
                except Exception:
                    pass
            html = await page.content()
            for m in pat.finditer(html):
                slug = m.group(1)
                # Filter out clearly sub-paths (e.g. "fruit-veg/apples-pears")
                if "/" in slug or len(slug) > 60:
                    continue
                found.add(slug)
        except Exception as e:
            print(f"  {label} probe failed: {e}")
        finally:
            await browser.close()
    return found

async def main():
    for label, url, regex in SITES:
        print(f"\n=== {label} ({url}) ===")
        slugs = await probe(label, url, regex)
        for s in sorted(slugs):
            print(f"  /{s}")
        print(f"  total: {len(slugs)}")

asyncio.run(main())
