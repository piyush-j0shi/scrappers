"""Dump structure of Woolworths page DOM to understand how nav links are exposed."""
from __future__ import annotations
import asyncio, re
from pathlib import Path
from playwright.async_api import async_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
SESSIONS_DIR = Path(__file__).resolve().parent.parent / "sessions" / "woolworths"

async def main():
    sess = next(SESSIONS_DIR.glob("*.json"), None)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-http2"])
        ctx = await browser.new_context(
            storage_state=str(sess) if sess else None,
            user_agent=UA, locale="en-NZ", viewport={"width":1366,"height":900}
        )
        page = await ctx.new_page()
        await page.goto("https://www.woolworths.co.nz/shop/browse/fruit-veg", wait_until="load", timeout=60_000)
        await page.wait_for_timeout(5000)

        # All hrefs on the page
        hrefs = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.getAttribute('href'))")
        # Filter to anything that looks shop-related
        shop_links = [h for h in hrefs if h and ('shop' in h.lower() or 'browse' in h.lower() or 'category' in h.lower())]
        print(f"Total anchors: {len(hrefs)}")
        print(f"Shop-related: {len(shop_links)}")
        unique_pat = set()
        for h in shop_links:
            # Strip query strings, normalize
            h_clean = h.split('?')[0]
            unique_pat.add(h_clean)
        print("\nUnique shop-link paths:")
        for u in sorted(unique_pat)[:60]:
            print(f"  {u}")

        # Also try the drawer/menu opener
        for sel in ["button[aria-label*='Browse' i]", "button[aria-label*='Menu' i]", "button:has-text('Browse')"]:
            btn = await page.query_selector(sel)
            if btn:
                try:
                    await btn.click()
                    await page.wait_for_timeout(1500)
                    print(f"\nClicked {sel} — re-checking links...")
                    hrefs2 = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.getAttribute('href'))")
                    new_shop = [h for h in hrefs2 if h and 'browse' in h.lower()]
                    new_unique = set(h.split('?')[0] for h in new_shop)
                    diff = new_unique - unique_pat
                    print(f"  added {len(diff)} new shop-related links:")
                    for u in sorted(diff)[:40]:
                        print(f"    {u}")
                    break
                except Exception as e:
                    print(f"  click failed: {e}")
        await browser.close()

asyncio.run(main())
