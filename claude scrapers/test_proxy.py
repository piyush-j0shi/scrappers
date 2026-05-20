"""One-shot proxy verification.

Tests a proxy URL against all 3 NZ retailers (Woolworths, New World, Pak'nSave)
using minimal queries. Reports whether each retailer returned valid product
data — that's the only signal that matters.

Usage:
  # Test against all 3 retailers (default)
  python3 test_proxy.py --proxy http://USER:PASS@host:port

  # Test only one retailer
  python3 test_proxy.py --proxy http://USER:PASS@host:port --chain woolworths
  python3 test_proxy.py --proxy http://USER:PASS@host:port --chain newworld
  python3 test_proxy.py --proxy http://USER:PASS@host:port --chain paknsave

  # Show the browser (useful when debugging captcha/age-verify)
  python3 test_proxy.py --proxy ... --no-headless

Exit codes:
  0  — proxy works against all tested retailers
  1  — at least one retailer failed
  2  — bad arguments

Costs nothing on Supabase — never writes anything to the DB.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# Reuse the existing scraper code paths
sys.path.insert(0, str(Path(__file__).resolve().parent))
from woolworths_claude import WoolworthsClaudeScraper, CATEGORY_URLS as WW_CATEGORIES
from foodstuffs_claude import FoodstuffsScraper, BarcodeCache, CHAINS as FS_CHAINS


SMOKE_TARGETS = {
    "woolworths": {
        "label": "Woolworths",
        # Single small category — keeps the test under a minute per chain
        "category_url": "https://www.woolworths.co.nz/shop/browse/bakery",
    },
    "newworld": {
        "label": "New World",
        "category_slug": "bakery",
    },
    "paknsave": {
        "label": "Pak'nSave",
        "category_slug": "bakery",
    },
}


async def test_woolworths(proxy_url: str, headless: bool) -> dict:
    """Fire one Woolworths bakery scrape via proxy. Don't touch Supabase."""
    s = WoolworthsClaudeScraper(
        category_urls=[SMOKE_TARGETS["woolworths"]["category_url"]],
        headless=headless,
        dry_run=True,
        proxy_url=proxy_url,
    )
    s._resolve_branch()
    await s._start_browser()
    sp = s._session_path_for_branch()
    await s._new_context(storage_state=str(sp) if sp else None)
    try:
        result = await s.scrape_one_category(SMOKE_TARGETS["woolworths"]["category_url"])
        products = result[0]  # 2-tuple or 3-tuple
    finally:
        await s._close_browser()
    have_barcode = sum(1 for p in products if p.barcode)
    return {
        "products": len(products),
        "barcoded": have_barcode,
        "ok": len(products) > 0 and have_barcode > 0,
    }


async def test_foodstuffs(chain_key: str, proxy_url: str, headless: bool) -> dict:
    """Fire one Foodstuffs bakery scrape via proxy. Don't touch Supabase."""
    cache = BarcodeCache()
    s = FoodstuffsScraper(
        chain_key=chain_key,
        category_slugs=["bakery"],
        headless=headless,
        dry_run=True,
        cache=cache,
        proxy_url=proxy_url,
    )
    s._resolve_branch()
    await s._start_browser()
    await s._new_context()
    try:
        url = f"{FS_CHAINS[chain_key]['base_url']}/shop/category/bakery"
        products, _ = await s.scrape_one_category(url)
        if products:
            await s.enrich_barcodes(products)
    finally:
        await s._close_browser()
    have_barcode = sum(1 for p in products if p.barcode)
    return {
        "products": len(products),
        "barcoded": have_barcode,
        "ok": len(products) > 0,
    }


async def main_async() -> int:
    ap = argparse.ArgumentParser(description="Test a proxy URL against NZ supermarket sites")
    ap.add_argument("--proxy", required=True,
                    help="Proxy URL (e.g. http://USER:PASS@gateway.iproyal.com:12321)")
    ap.add_argument("--chain", choices=list(SMOKE_TARGETS) + ["all"], default="all",
                    help="Which retailer(s) to test")
    ap.add_argument("--no-headless", action="store_true", help="Show the browser window")
    args = ap.parse_args()

    chains = [args.chain] if args.chain != "all" else list(SMOKE_TARGETS.keys())
    headless = not args.no_headless

    print(f"\n=== Proxy test ===")
    print(f"proxy:      {args.proxy.split('@')[-1] if '@' in args.proxy else args.proxy}")
    print(f"chains:     {', '.join(chains)}")
    print()

    results: dict[str, dict] = {}
    overall_ok = True
    for chain in chains:
        label = SMOKE_TARGETS[chain]["label"]
        print(f"  [{label}] running …", flush=True)
        try:
            if chain == "woolworths":
                r = await test_woolworths(args.proxy, headless)
            else:
                r = await test_foodstuffs(chain, args.proxy, headless)
            results[chain] = r
            status = "✅ OK" if r["ok"] else "❌ FAIL"
            print(f"  [{label}] {status}  products={r['products']}  barcoded={r['barcoded']}")
            overall_ok &= r["ok"]
        except Exception as e:
            results[chain] = {"products": 0, "barcoded": 0, "ok": False, "error": str(e)[:200]}
            print(f"  [{label}] ❌ ERROR  {str(e)[:200]}")
            overall_ok = False
        print()

    print("=" * 50)
    print("Verdict:", "✅ proxy works" if overall_ok else "❌ at least one chain failed")
    print()
    print("Per-chain summary:")
    for chain, r in results.items():
        print(f"  {SMOKE_TARGETS[chain]['label']:12}  ok={r['ok']:1}  products={r.get('products', 0):>4}  barcoded={r.get('barcoded', 0):>4}")

    return 0 if overall_ok else 1


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
