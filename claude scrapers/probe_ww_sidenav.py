"""Read Woolworths side-nav from a real page render to enumerate every
top-level department. Uses a saved session so the page renders fully."""
from __future__ import annotations
import asyncio, json, re
from pathlib import Path
from playwright.async_api import async_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
SESSIONS_DIR = Path(__file__).resolve().parent.parent / "sessions" / "woolworths"

START = "https://www.woolworths.co.nz/shop/browse/fruit-veg"

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
        await page.goto(START, wait_until="load", timeout=60_000)
        await page.wait_for_timeout(4500)

        # Pull every anchor href that looks like a top-level browse link
        hrefs = await page.eval_on_selector_all(
            "a[href*='/shop/browse/']",
            "els => Array.from(new Set(els.map(e => e.getAttribute('href'))))"
        )
        slugs = set()
        for h in hrefs:
            m = re.match(r"^(?:/|https?://[^/]+)?/shop/browse/([a-z0-9-]+)(?:[/?#]|$)", h or "")
            if m:
                slugs.add(m.group(1))
        # Also dump the menu-item text alongside the slugs we found
        nav_items = await page.eval_on_selector_all(
            "nav a, [class*='SideNav'] a, [class*='sidebar'] a, [class*='Department'] a",
            "els => els.map(e => ({text: e.innerText.trim(), href: e.getAttribute('href')}))"
        )
        nav_top = [
            n for n in nav_items
            if (n.get('href') or '').count('/shop/browse/') == 1
            and len((n.get('href') or '').split('/shop/browse/')[1].split('/')[0]) > 0
            and '/shop/browse/' in (n.get('href') or '')
            and len((n.get('href') or '').split('/shop/browse/')[1].split('/')) <= 2
        ]
        # De-dupe by slug
        seen = set()
        unique = []
        for n in nav_top:
            slug = (n.get('href') or '').split('/shop/browse/')[-1].split('/')[0].split('?')[0]
            if slug in seen or not slug:
                continue
            seen.add(slug)
            unique.append((slug, n.get('text','')[:60]))
        print(f"\nUnique top-level slugs found: {len(slugs)}")
        for s in sorted(slugs):
            label = next((u[1] for u in unique if u[0] == s), '')
            print(f"  /{s}  ←  '{label}'")
        await browser.close()

asyncio.run(main())
