"""Quick sanity script: scrape one category, dump 3 sample products. Doesn't write to Supabase."""
import asyncio, sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from woolworths_claude import WoolworthsClaudeScraper

async def main():
    s = WoolworthsClaudeScraper(
        category_urls=["https://www.woolworths.co.nz/shop/browse/bakery"],
        headless=True, dry_run=True,
    )
    s._resolve_branch()
    await s._start_browser()
    sp = s._session_path_for_branch()
    await s._new_context(storage_state=str(sp) if sp else None)
    result = await s.scrape_one_category("https://www.woolworths.co.nz/shop/browse/bakery")
    products = result[0]
    print(f"\n=== {len(products)} products ===")
    have_barcode = sum(1 for p in products if p.barcode)
    in_stock = sum(1 for p in products if p.in_stock)
    have_special = sum(1 for p in products if p.special_price)
    have_weight = sum(1 for p in products if p.weight)
    have_brand = sum(1 for p in products if p.brand)
    print(f"barcode: {have_barcode}/{len(products)}  brand: {have_brand}  weight: {have_weight}  in_stock: {in_stock}  on special: {have_special}")
    print("\nSamples:")
    for p in products[:3]:
        print(f"  raw_name={p.raw_name!r}")
        print(f"    clean={p.clean_name!r} brand={p.brand!r} barcode={p.barcode!r}")
        print(f"    price={p.price} special={p.special_price} weight={p.weight!r} in_stock={p.in_stock}")
    await s._close_browser()

asyncio.run(main())
