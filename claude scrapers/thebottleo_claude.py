"""The Bottle-O (NZ) single-branch price scraper — Stage 1 of the pipeline.

Myfoodlink "shopfront". Category pages server-render products with an embedded
JSON object per line ({id(=barcode), name, price, brand, category}) alongside the
"talker" card HTML (product url, image, multibuy badge). Plain HTTP, no browser.

Store note: the multi-store chooser at shop.thebottleo.co.nz routes to each
store's shop via a client-side JS/geo flow that can't be driven by pure HTTP, so
this scrapes the shop served at `--host` (default thebottleo.co.nz). The Bottle-O's
online pricing is national (the default host shows the same specials as the
chooser), so the numbers match; `--branch-name` sets the label written to the file.

Category list comes from /sitemap.xml (/category/<slug>); products are de-duped by
barcode across categories.

Emits one JSONL file per branch+run via jsonl_export.write_jsonl.

Usage:
    python thebottleo_claude.py --branch-name "The Bottle-O Glenfield"
    python thebottleo_claude.py --categories beer,wine,spirits
"""
from __future__ import annotations

import argparse
import html as htmllib
import json
import re
import time
import urllib.request
from datetime import datetime, timezone
from typing import Optional

from jsonl_export import write_jsonl, to_cents, clean_record

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# One analytics (dataLayer) JSON object per product line — carries the barcode
# (id), brand and category. It's a SEPARATE list from the visual cards and its
# order can drift, so it's matched to a card by product name, never by index.
_PROD_JSON = re.compile(r'\{"id":"\d+","name":"(?:[^"\\]|\\.)*","price":"[0-9.]*",'
                        r'"brand":"(?:[^"\\]|\\.)*","category":"(?:[^"\\]|\\.)*"\}')
_MULTIBUY = re.compile(r'(\d+)\s*for\s*\$?\s*([0-9]+(?:\.[0-9]{2})?)', re.I)
# Deal sticker: href carries the promotion id; label carries the human text
# ("2 for $30", "Save $5", ...). Spans/<br> are stripped before matching.
_STICKER = re.compile(
    r'talker__sticker[^"]*"[^>]*href="/deals/([a-f0-9]+)"[^>]*>(.*?)</a>', re.S | re.I)
_STICKER_LABEL = re.compile(r'talker__sticker__label[^>]*>(.*?)</span>\s*</a>', re.S | re.I)
_PACK = re.compile(r"(\d+\s*[xX]\s*\d+\s*m?[lL]|\d+\s*(?:ml|mL|[lL]|pack|pk|cans?|bottles?)\b)")


def _norm(name: str) -> str:
    return re.sub(r"\s+", " ", htmllib.unescape(name or "")).strip().lower()


class TheBottleOScraper:
    def __init__(self, host: str, branch_name: str, categories: Optional[list[str]]):
        self.host = host.rstrip("/")
        self.base = f"https://{self.host}"
        self.branch_name = branch_name
        self.categories = categories  # None -> discover from sitemap

    def _get(self, url: str, tries: int = 3) -> str:
        last = None
        for attempt in range(1, tries + 1):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": UA,
                                                           "Accept": "text/html"})
                with urllib.request.urlopen(req, timeout=30) as r:
                    return r.read().decode("utf-8", "replace")
            except Exception as exc:
                last = exc
                time.sleep(0.5 * attempt)
        raise RuntimeError(f"GET failed after {tries} tries: {url} ({last})")

    def discover_categories(self) -> list[str]:
        xml = self._get(f"{self.base}/sitemap.xml")
        cats = sorted(set(re.findall(r"/category/([a-z0-9-]+)", xml)))
        print(f"discovered {len(cats)} categories from sitemap")
        return cats

    @staticmethod
    def _impressions(html: str) -> list[dict]:
        """The GA dataLayer `impressions` array — one entry per product, in the
        SAME order as the visual cards, carrying id(=barcode)/name/price/brand/
        category. Parsed as a whole (never per-object, which can silently drop an
        entry and shift the index)."""
        i = html.find('"impressions":[')
        if i == -1:
            return []
        start = html.find("[", i)
        depth = 0
        for j in range(start, len(html)):
            if html[j] == "[":
                depth += 1
            elif html[j] == "]":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(html[start:j + 1])
                    except Exception:
                        return []
        return []

    def _parse_page(self, html: str) -> list[dict]:
        impressions = self._impressions(html)
        cards = html.split('class="TalkerGrid__Item"')[1:]
        # Cards and impressions are 1:1 in display order. Card carries url/image/
        # price/multibuy; impression carries barcode/brand/category/name(+size).
        aligned = len(cards) == len(impressions)
        by_name = {_norm(d.get("name")): d for d in impressions}
        out = []
        for idx, c in enumerate(cards):
            name_m = re.search(r'talker__product-name[^>]*>\s*(?:<[^>]+>\s*)*([^<]+)', c)
            card_name = htmllib.unescape(name_m.group(1).strip()) if name_m else None
            href = re.search(r'href="(/lines/[^"]+)"', c)
            img = re.search(r'<img[^>]+src="([^"]+)"', c)
            price = (re.search(r'talker__prices__sell[^>]*>\s*\$?\s*([0-9]+\.[0-9]{2})', c)
                     or re.search(r'price__sell[^>]*>\s*\$?\s*([0-9]+\.[0-9]{2})', c))
            # Deal sticker over the WHOLE card (image URLs are long, so a prefix
            # slice can miss it). deal_id -> source_promotion_id; label -> multibuy.
            deal_id, deal_label, mb = None, None, None
            sm = _STICKER.search(c)
            if sm:
                deal_id = sm.group(1)
                lm = _STICKER_LABEL.search(sm.group(2))
                deal_label = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ",
                                    (lm.group(1) if lm else sm.group(2)))).strip()
                m = _MULTIBUY.search(deal_label)
                if m:
                    mb = (int(m.group(1)), m.group(2))
            # Prefer positional alignment; fall back to name match if counts differ
            # (card name omits size, so match by card-name being a prefix).
            meta = impressions[idx] if aligned else (
                by_name.get(_norm(card_name))
                or next((d for d in impressions
                         if card_name and _norm(d.get("name")).startswith(_norm(card_name))), {}))
            if not (card_name or meta):
                continue
            out.append({
                "name": (meta or {}).get("name") or card_name,
                "url": self.base + href.group(1) if href else None,
                "image": img.group(1) if img else None,
                "price": price.group(1) if price else None,
                "multibuy": mb,
                "deal_id": deal_id,
                "deal_label": deal_label,
                "meta": meta or {},
            })
        return out

    def _crawl_category(self, cat: str, now: str) -> list[dict]:
        out, page, prev_first = [], 1, None
        while True:
            try:
                html = self._get(f"{self.base}/category/{cat}?page={page}")
            except RuntimeError:
                break
            items = self._parse_page(html)
            if not items:
                break
            first = items[0]["url"] or items[0]["name"]
            if page > 1 and first == prev_first:
                break  # pagination clamped back to page 1
            prev_first = first if page == 1 else prev_first
            for it in items:
                out.append(self._record(it, cat, now))
            page += 1
            time.sleep(0.3)
        return out

    def _record(self, it: dict, cat: str, now: str) -> dict:
        meta = it.get("meta") or {}
        name = it["name"]
        # Card price is authoritative; dataLayer price is the fallback.
        price = it.get("price") or meta.get("price")
        barcode = meta.get("id")
        pack = _PACK.search(name)
        rec = {
            "source_product_id": barcode or (it["url"] or "").rsplit("/", 1)[-1] or name,
            "barcode": barcode,
            "raw_name": name,
            "brand": htmllib.unescape(meta["brand"]) if meta.get("brand") else None,
            "category_path": htmllib.unescape(meta["category"]) if meta.get("category") else cat,
            "size": pack.group(1) if pack else None,
            "current_price_cents": to_cents(float(price)) if price else None,
            "stock_status": "in_stock",
            "product_url": it.get("url"),
            "image_url": it.get("image"),
            "observed_at": now,
        }
        mb = it.get("multibuy")
        if it.get("deal_id") or mb:
            rec["source_promotion_id"] = it.get("deal_id")
            rec["promo_text"] = it.get("deal_label")
            if mb:
                qty, total = mb
                rec["promo_type"] = "multibuy"
                rec["multibuy_quantity"] = qty
                rec["multibuy_price_cents"] = to_cents(float(total))
            else:
                rec["promo_type"] = "special"
        return rec

    def run(self) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        cats = self.categories or self.discover_categories()
        by_bc: dict[str, dict] = {}
        for i, cat in enumerate(cats, 1):
            recs = self._crawl_category(cat, now)
            new = 0
            for r in recs:
                key = r["source_product_id"] or r["product_url"]
                if key and key not in by_bc:
                    by_bc[key] = r
                    new += 1
            print(f"  [{i}/{len(cats)}] {cat}: {len(recs)} rows (+{new} new, total {len(by_bc)})")
        records = [clean_record(r) for r in by_bc.values()]
        promos = sum(1 for r in records if r.get("promo_type"))
        path = write_jsonl("thebottleo", self.branch_name, records)
        print(f"\n{self.branch_name}: {len(records)} products "
              f"({promos} multibuy) -> {path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="The Bottle-O single-branch scraper")
    p.add_argument("--host", default="thebottleo.co.nz", help="shop host to scrape")
    p.add_argument("--branch-name", default="The Bottle-O",
                   help="branch label written to the export filename")
    p.add_argument("--categories", default=None,
                   help="comma-separated category slugs (default: all from sitemap)")
    args = p.parse_args(argv)
    cats = ([c.strip() for c in args.categories.split(",") if c.strip()]
            if args.categories else None)
    TheBottleOScraper(args.host, args.branch_name, cats).run()


if __name__ == "__main__":
    main()
