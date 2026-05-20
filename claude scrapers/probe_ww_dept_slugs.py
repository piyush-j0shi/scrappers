"""Test which Woolworths department slugs are real by hitting the products API
for each candidate. Reports total items per slug — non-zero means real, 0 or 4xx means dead.
Uses a saved session to bypass Akamai.
"""
from __future__ import annotations
import asyncio, json
from pathlib import Path
from urllib.parse import quote
from playwright.async_api import async_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
SESSIONS_DIR = Path(__file__).resolve().parent.parent / "sessions" / "woolworths"

# Current known + plausible additions from naming patterns and competitor sites
CANDIDATES = [
    # Currently in scraper
    "fruit-veg", "meat-poultry", "fish-seafood", "fridge-deli", "bakery",
    "frozen", "pantry", "drinks", "health-body", "household",
    "baby-child", "pet",
    # Possible missing
    "beer-wine-spirits", "beer-wine", "liquor", "alcohol",
    "deli", "cheese-deli", "fresh-foods",
    "snacks", "snacks-and-treats", "sweets-snacks",
    "specials", "on-special", "promotions",
    "world-foods", "international",
    "ready-meals", "meal-solutions",
    "dietary-and-vegan",
    "easter", "christmas", "halloween",
    "garden-outdoor",
    "clothing-and-toys",
    "bbq-and-outdoor",
    "office-and-stationary",
]

async def main():
    sess = next(SESSIONS_DIR.glob("*.json"), None)
    if not sess:
        print("no saved session, aborting"); return
    print(f"using session: {sess.name}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-http2"])
        ctx = await browser.new_context(
            storage_state=str(sess), user_agent=UA, locale="en-NZ",
            viewport={"width":1366,"height":900}
        )
        page = await ctx.new_page()

        live: list[tuple[str, int]] = []
        dead: list[str] = []
        for slug in CANDIDATES:
            url = (f"https://www.woolworths.co.nz/api/v1/products"
                   f"?dasFilter=Department%3B%3B{quote(slug)}%3Bfalse"
                   f"&target=browse&inStockProductsOnly=false&size=1&page=1")
            try:
                resp = await page.request.get(url)
                if resp.ok:
                    data = await resp.json()
                    pn = data.get("products") or {}
                    total = pn.get("totalItems") or 0
                    if total > 0:
                        live.append((slug, total))
                    else:
                        dead.append(slug)
                else:
                    dead.append(f"{slug}(http {resp.status})")
            except Exception as e:
                dead.append(f"{slug}(err)")
            await asyncio.sleep(0.2)

        print(f"\n--- LIVE departments ({len(live)}) ---")
        for slug, total in sorted(live, key=lambda x: -x[1]):
            print(f"  {slug:30}  totalItems={total}")
        print(f"\n--- DEAD or empty ({len(dead)}) ---")
        for s in dead:
            print(f"  {s}")

        await browser.close()

asyncio.run(main())
