"""Verify a bootstrapped session: load storage_state, fetch /api/v1/products,
print prices — and compare against a fresh no-session fetch so we can see
per-store price differences.

Usage:
    python3 verify_bootstrapped_session.py <branch_id>
"""
from __future__ import annotations
import asyncio, json, sys
from pathlib import Path
from playwright.async_api import async_playwright
from base_scraper import USER_AGENT, EXTRA_HEADERS, STEALTH_SCRIPT

SESSIONS_DIR = Path(__file__).parent / "sessions" / "woolworths"
URL = ("https://www.woolworths.co.nz/api/v1/products"
       "?dasFilter=Department%3B%3Bfruit-veg%3Bfalse&target=browse&inStockProductsOnly=false&size=48")
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "x-requested-with": "OnlineShopping.WebApp",
    "x-ui-ver": "7.73.30",
    "referer": "https://www.woolworths.co.nz/shop/browse/fruit-veg",
}


async def fetch_with_state(pw, state_path=None):
    browser = await pw.chromium.launch(
        headless=True,
        args=["--disable-http2","--disable-blink-features=AutomationControlled",
              "--no-sandbox","--disable-dev-shm-usage","--disable-gpu"],
    )
    kwargs = dict(
        user_agent=USER_AGENT, viewport={"width":1366,"height":768},
        locale="en-NZ", timezone_id="Pacific/Auckland",
        extra_http_headers=EXTRA_HEADERS,
    )
    if state_path:
        # Strip _pico_meta before handing the state to Playwright.
        raw = json.loads(Path(state_path).read_text())
        raw.pop("_pico_meta", None)
        tmp = Path("/tmp") / f"ww_state_{Path(state_path).stem}.json"
        tmp.write_text(json.dumps(raw))
        kwargs["storage_state"] = str(tmp)

    ctx = await browser.new_context(**kwargs)
    await ctx.add_init_script(STEALTH_SCRIPT)
    page = await ctx.new_page()
    if not state_path:
        # Cold context: must seed cookies first, otherwise Akamai may 403.
        await page.goto("https://www.woolworths.co.nz/", wait_until="load", timeout=60_000)
        await page.wait_for_timeout(2000)
    r = await page.request.get(URL, headers=HEADERS)
    data = await r.json() if r.status == 200 else {"error": r.status}
    await browser.close()
    return data


def summarize(label, data):
    print(f"\n--- {label} ---")
    if "error" in data:
        print(f"  HTTP {data['error']}")
        return
    node = data.get("products") or {}
    items = [it for it in (node.get("items") or []) if it.get("type") == "Product"]
    print(f"  totalItems={node.get('totalItems')}  returned={len(items)}")
    for it in items[:10]:
        print(f"    {it.get('sku')}  {it.get('name')!r:<55}  ${(it.get('price') or {}).get('originalPrice')}  sale=${(it.get('price') or {}).get('salePrice')}")
    return {it.get("sku"): (it.get("price") or {}).get("originalPrice") for it in items}


async def main(branch_id: str) -> None:
    state_path = SESSIONS_DIR / f"{branch_id}.json"
    if not state_path.exists():
        print(f"Missing session file: {state_path}")
        sys.exit(1)
    meta = json.loads(state_path.read_text()).get("_pico_meta", {})
    print(f"Session meta: {json.dumps(meta, indent=2)}")

    async with async_playwright() as pw:
        print("\nFetching with bootstrapped session (pinned store)...")
        data_pinned = await fetch_with_state(pw, state_path=state_path)
        print("Fetching with a COLD context (default/unbound store)...")
        data_cold = await fetch_with_state(pw, state_path=None)

    pinned_prices = summarize(f"PINNED (branch {branch_id})", data_pinned)
    cold_prices = summarize("COLD (default store)", data_cold)

    if isinstance(pinned_prices, dict) and isinstance(cold_prices, dict):
        common = set(pinned_prices) & set(cold_prices)
        diff = [sku for sku in common if pinned_prices[sku] != cold_prices[sku]]
        print(f"\nShared SKUs: {len(common)}   Price differences: {len(diff)}")
        for sku in diff[:15]:
            print(f"  sku {sku}: pinned=${pinned_prices[sku]}   cold=${cold_prices[sku]}")
        only_pinned = set(pinned_prices) - set(cold_prices)
        only_cold = set(cold_prices) - set(pinned_prices)
        print(f"Only in pinned: {len(only_pinned)}   Only in cold: {len(only_cold)}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
