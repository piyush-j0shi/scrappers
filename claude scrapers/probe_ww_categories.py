"""Probe Woolworths departments by walking the side-nav on every browse page.
Uses a saved session to bypass Akamai. Outputs a clean sorted list of unique
top-level slugs from /shop/browse/<slug>.
"""
from __future__ import annotations
import asyncio, json, re
from pathlib import Path
from playwright.async_api import async_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
SESSIONS_DIR = Path(__file__).resolve().parent.parent / "sessions" / "woolworths"

SLUG_RE = re.compile(r'/shop/browse/([a-z0-9-]+)(?:[/"?#]|$)')
SUB_RE  = re.compile(r'/shop/browse/([a-z0-9-]+/[a-z0-9-]+)')

START_PAGES = [
    "https://www.woolworths.co.nz/shop/browse/fruit-veg",
    "https://www.woolworths.co.nz/shop/browse/pantry",
    "https://www.woolworths.co.nz/shop/browse/drinks",
]

async def main():
    sess = next(SESSIONS_DIR.glob("*.json"), None)
    print(f"using session: {sess.name if sess else 'none'}")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-http2"])
        ctx = await browser.new_context(
            storage_state=str(sess) if sess else None,
            user_agent=UA, locale="en-NZ", viewport={"width":1366,"height":900}
        )
        page = await ctx.new_page()
        all_slugs: set[str] = set()
        for url in START_PAGES:
            try:
                await page.goto(url, wait_until="load", timeout=60_000)
                await page.wait_for_timeout(3000)
                html = await page.content()
                for m in SLUG_RE.finditer(html):
                    all_slugs.add(m.group(1))
            except Exception as e:
                print(f"  failed {url}: {e}")

        print(f"\nTotal unique top-level slugs: {len(all_slugs)}")
        for s in sorted(all_slugs):
            print(f"  /{s}")

        await browser.close()

asyncio.run(main())
