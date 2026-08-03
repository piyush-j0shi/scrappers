"""Super Liquor (NZ) single-branch price scraper — Stage 1 of the pipeline.

nopCommerce, server-rendered. Every Super Liquor store is its OWN SUBDOMAIN
(e.g. Super Liquor Hobsonville -> https://hobsonville.superliquor.co.nz), and
that subdomain renders THAT branch's prices inline — no store cookie needed.

Emits one JSONL file per branch+run via the shared Scraper Data Contract
(jsonl_export.write_jsonl); the importer (import_products.py) owns all DB writes.

Usage:
    python superliquor_claude.py --store hobsonville
    python superliquor_claude.py --store hobsonville --categories beer,wine
"""
from __future__ import annotations

import argparse
import html as htmllib
import re
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Optional

from jsonl_export import write_jsonl, to_cents, clean_record

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# Top-level category slugs (discovered from the store nav). Sub-categories are
# crawled too but products are de-duped by source_product_id, so overlap is safe.
DEFAULT_CATEGORIES = ["beer", "wine", "spirits", "rtds", "cider", "premix"]


def _get(url: str, tries: int = 3) -> str:
    last = None
    for attempt in range(1, tries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA,
                                                        "Accept": "text/html"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode("utf-8", "replace")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
            last = exc
            time.sleep(0.5 * attempt)
    raise RuntimeError(f"GET failed after {tries} tries: {url} ({last})")


_PRODUCT_ITEM = "product-item"
_PACK_RE = re.compile(r"(\d+\s*[xX]\s*\d+\s*m?l|\d+\s*(?:ml|mL|L|litre|pack|pk|cans?|bottles?)\b)", re.I)


def _parse_tile(block: str) -> Optional[dict]:
    pid = re.search(r'data-productid="(\d+)"', block)
    name = re.search(r'<h2 class="product-title[^"]*">\s*<a[^>]*>([^<]+)</a>', block)
    href = re.search(r'<h2 class="product-title[^"]*">\s*<a href="([^"]+)"', block)
    if not (pid and name):
        return None
    price = re.search(r'class="[^"]*actual-price[^"]*">\s*\$?\s*([0-9]+\.[0-9]{2})', block)
    old = re.search(r'class="[^"]*old-price[^"]*">\s*\$?\s*([0-9]+\.[0-9]{2})', block)
    img = re.search(r'data-src="([^"]+)"', block) or re.search(r'<img[^>]+src="([^"]+)"', block)
    nm = htmllib.unescape(name.group(1).strip())
    pack = _PACK_RE.search(nm)
    on_special = bool(old)
    return {
        "source_product_id": pid.group(1),
        "retailer_sku": pid.group(1),
        "raw_name": nm,
        "size": pack.group(1) if pack else None,
        "current_price_cents": to_cents(float(price.group(1))) if price else None,
        # On special the struck-through old price is the everyday/comparison price.
        "comparison_price_cents": to_cents(float(old.group(1))) if old else None,
        "promo_type": "special" if on_special else None,
        "stock_status": "in_stock",
        "product_url": href.group(1) if href else None,
        "image_url": img.group(1) if img else None,
    }


class SuperLiquorScraper:
    def __init__(self, store: str, categories: list[str]):
        self.store = store
        self.base = f"https://{store}.superliquor.co.nz"
        self.branch_name = f"Super Liquor {store.replace('-', ' ').title()}"
        self.categories = categories

    def _abs(self, url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        return url if url.startswith("http") else self.base + url

    def _crawl_category(self, cat: str) -> list[dict]:
        out, page, seen_first = [], 1, None
        while True:
            url = f"{self.base}/{cat}?pagenumber={page}"
            try:
                h = _get(url)
            except RuntimeError as exc:
                print(f"  [{cat}] page {page} failed: {exc}", file=sys.stderr)
                break
            blocks = h.split(f'<div class="{_PRODUCT_ITEM}"')[1:]
            recs = [r for r in (_parse_tile(b) for b in blocks) if r]
            if not recs:
                break
            # nopCommerce clamps out-of-range pagenumber to the last page and
            # re-serves it — stop if this page's first product repeats page 1's.
            if page > 1 and recs[0]["source_product_id"] == seen_first:
                break
            if page == 1:
                seen_first = recs[0]["source_product_id"]
            out.extend(recs)
            print(f"  [{cat}] page {page}: {len(recs)} products")
            page += 1
            time.sleep(0.3)
        return out

    def run(self) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        by_id: dict[str, dict] = {}
        for cat in self.categories:
            for r in self._crawl_category(cat):
                r["category_path"] = cat
                r["observed_at"] = now
                r["product_url"] = self._abs(r["product_url"])
                by_id.setdefault(r["source_product_id"], r)
        records = [clean_record(r) for r in by_id.values()]
        specials = sum(1 for r in records if r.get("promo_type"))
        path = write_jsonl("superliquor", self.branch_name, records)
        print(f"\n{self.branch_name}: {len(records)} products "
              f"({specials} on special) -> {path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Super Liquor single-branch scraper")
    p.add_argument("--store", default="hobsonville",
                   help="store subdomain, e.g. hobsonville (default) -> "
                        "https://hobsonville.superliquor.co.nz")
    p.add_argument("--categories", default=",".join(DEFAULT_CATEGORIES),
                   help="comma-separated category slugs to crawl")
    args = p.parse_args(argv)
    cats = [c.strip() for c in args.categories.split(",") if c.strip()]
    SuperLiquorScraper(args.store, cats).run()


if __name__ == "__main__":
    main()
