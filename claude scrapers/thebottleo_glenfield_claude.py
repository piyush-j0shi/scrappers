"""The Bottle-O GLENFIELD branch scraper (rich) — Stage 1 of the pipeline.

Unlike thebottleo_claude.py (which scraped the national thebottleo.co.nz shop,
only 399 products), this targets the BRANCH webshop
`https://glenfield.shop.thebottleo.co.nz` (~2,600 products, real branch pricing).

Two-pass, because the branch listing pages carry only name/price/image/url and
there is NO bulk API/feed (verified): everything else lives on each product's
detail page.

  Pass 1 (cheap, ~55 + ~9 requests):
    - GET /search?page=N          -> enumerate ALL products (slug, name, price, image)
    - GET /search?q[]=special:1   -> the promo set + sticker label (Special / "N for $X")
  Pass 2 (per product, concurrent):
    - GET /lines/<slug>           -> brand, category+sub-category (breadcrumb),
                                     availability, hi-res image, internal SKU, and the
                                     Liquor spec table (Style, ABV, Standard Drinks,
                                     Closure, Region, Bottled In, Tasting Notes).

Derived columns: Quantity + Size split from the name/slug; Type (Can/Bottle/Keg/
Cask/Other) from Closure with a name fallback; Special/Discount booleans from the
promo set. No Description (per request). Barcodes are not published on the branch
shop, so the identifier is the URL slug (+ the Myfoodlink internal line id).

Writes a JSONL of rich records via jsonl_export.write_jsonl; build the Excel with
build_bottleo_glenfield_xlsx.py.

Usage:
    python thebottleo_glenfield_claude.py                 # full run
    python thebottleo_glenfield_claude.py --limit 40      # quick test
    python thebottleo_glenfield_claude.py --workers 8
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import html as htmllib
import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from typing import Optional

from jsonl_export import write_jsonl, to_cents, clean_record
from liquor_common import parse_qty_size, classify_type

HOST = "glenfield.shop.thebottleo.co.nz"
BASE = f"https://{HOST}"
BRANCH_NAME = "The Bottle-O Glenfield"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

_TALKER = 'class="TalkerGrid__Item"'
_MULTIBUY = re.compile(r'(\d+)\s*for\s*\$?\s*([0-9]+(?:\.[0-9]{2})?)', re.I)


def _get(url: str, tries: int = 3) -> Optional[str]:
    last = None
    for attempt in range(1, tries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/html"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode("utf-8", "replace")
        except Exception as exc:  # noqa: BLE001
            last = exc
            if attempt < tries:
                time.sleep(0.4 * attempt)
    print(f"  GET failed: {url} ({last})", file=sys.stderr)
    return None


# ---- small parsers ----------------------------------------------------------

def _norm(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", htmllib.unescape(s or "")).strip()


def _sticker_label(card: str) -> Optional[str]:
    """Promo sticker label from a listing card: 'Special' or 'N for $X'. The label
    span can nest an inner span (e.g. <span class=weak>2 for</span><br>$99), so read
    to the outer </span> that precedes the sticker's </a> or </span> close."""
    lab = re.search(r'talker__sticker__label[^>]*>(.*?)</span>\s*(?:</a>|</span>)',
                    card, re.S | re.I)
    mod = re.search(r'talker__sticker--(\w+)', card)
    if not (lab or mod):
        return None
    if lab:
        text = _norm(re.sub(r"<[^>]+>", " ", lab.group(1)))
        if text:
            return text
    return mod.group(1) if mod else None


# ---- pass 1: enumerate ------------------------------------------------------

def _cards(html: str) -> list[str]:
    return html.split(_TALKER)[1:]


def _card_basic(card: str) -> Optional[dict]:
    href = re.search(r'href="(/lines/[^"]+)"', card)
    if not href:
        return None
    name = re.search(r'talker__product-name[^>]*>\s*(?:<[^>]+>\s*)*([^<]+)', card)
    price = re.search(r'price__sell[^>]*>\s*\$?\s*([0-9]+\.[0-9]{2})', card)
    img = re.search(r'<img[^>]+src="([^"]+)"', card)
    slug = href.group(1).rsplit("/", 1)[-1]
    return {
        "slug": slug,
        "product_url": BASE + href.group(1),
        "raw_name": _norm(name.group(1)) if name else slug.replace("-", " "),
        "list_price": price.group(1) if price else None,
        "list_image": img.group(1) if img else None,
    }


def enumerate_products(max_pages: Optional[int] = None) -> dict[str, dict]:
    out: dict[str, dict] = {}
    page, prev_first = 1, None
    while True:
        html = _get(f"{BASE}/search?page={page}")
        if not html:
            break
        cards = _cards(html)
        if not cards:
            break
        first = None
        for c in cards:
            b = _card_basic(c)
            if not b:
                continue
            if first is None:
                first = b["slug"]
            out.setdefault(b["slug"], b)
        if page > 1 and first == prev_first:  # clamp safety: page repeated
            break
        prev_first = first
        print(f"  [enumerate] page {page}: {len(cards)} cards (total {len(out)})", flush=True)
        if max_pages and page >= max_pages:
            break
        page += 1
        time.sleep(0.15)
    return out


def enumerate_sitemap() -> set[str]:
    """All /lines product slugs from the sitemap. /search only serves ~2,400 of
    the ~2,770 published products (it silently caps), so the sitemap is the
    completeness backstop; dead entries are dropped later when their detail 404s."""
    xml = _get(f"{BASE}/sitemap.xml")
    return set(re.findall(r"/lines/([a-z0-9\-]+)", xml)) if xml else set()


def enumerate_promos(max_pages: Optional[int] = None) -> dict[str, dict]:
    """slug -> {'label':..., 'multibuy':(qty,total)|None} for the specials set."""
    promos: dict[str, dict] = {}
    page, prev_len = 1, -1
    while True:
        html = _get(f"{BASE}/search?q%5B%5D=special%3A1&page={page}")
        if not html:
            break
        cards = _cards(html)
        if not cards:
            break
        for c in cards:
            href = re.search(r'href="(/lines/[^"]+)"', c)
            if not href:
                continue
            slug = href.group(1).rsplit("/", 1)[-1]
            label = _sticker_label(c)
            mb = None
            if label:
                m = _MULTIBUY.search(label)
                if m:
                    mb = (int(m.group(1)), m.group(2))
                    label = f"{m.group(1)} for ${m.group(2)}"
                elif "special" in label.lower():
                    label = "Special"
            promos[slug] = {"label": label, "multibuy": mb}
        if len(promos) == prev_len:  # clamp safety: no new promos added
            break
        prev_len = len(promos)
        print(f"  [promos] page {page}: total {len(promos)}", flush=True)
        if max_pages and page >= max_pages:
            break
        page += 1
        time.sleep(0.15)
    return promos


# ---- pass 2: detail ---------------------------------------------------------

_LD = re.compile(r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>', re.S)
_SPEC_ROW = re.compile(r'<tr>\s*<th>([^<]+)</th>\s*<td>(.*?)</td>', re.S)
_NOTE_ID = re.compile(r'note="\{&quot;id&quot;:&quot;([a-f0-9]{24})&quot;')


def _ld_blocks(html: str) -> tuple[dict, list]:
    product, crumbs = {}, []
    for b in _LD.findall(html):
        try:
            d = json.loads(b)
        except Exception:  # noqa: BLE001
            continue
        if d.get("@type") == "Product":
            product = d
        elif d.get("@type") == "BreadcrumbList":
            for e in d.get("itemListElement", []):
                item = e.get("item")
                crumbs.append(item.get("name") if isinstance(item, dict) else e.get("name"))
    return product, crumbs


def fetch_detail(slug: str) -> dict:
    html = _get(f"{BASE}/lines/{slug}")
    if not html:
        return {"_slug": slug, "_ok": False}
    product, crumbs = _ld_blocks(html)
    offers = product.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    brand = product.get("brand")
    if isinstance(brand, dict):
        brand = brand.get("name")

    specs = {}
    for th, td in _SPEC_ROW.findall(html):
        specs[_norm(th).lower()] = _norm(re.sub(r"<[^>]+>", " ", td))

    tn = re.search(r'MoreInfo__Section--TastingNotes(.*?)(?:MoreInfo__Section--|</section>)',
                   html, re.S)
    tasting = _norm(re.sub(r"<[^>]+>", " ", tn.group(1))) if tn else None
    if tasting:
        tasting = re.sub(r"^\s*Tasting Notes\s*", "", tasting, flags=re.I).strip() or None

    # breadcrumb: [Home, All Products, Category, Sub-Category, ...]
    trail = [c for c in crumbs if c and c.lower() not in ("home", "all products")
             and not c.lower().startswith("home")]
    category = trail[0] if trail else None
    subcategory = trail[1] if len(trail) > 1 else None

    note = _NOTE_ID.search(html)
    avail = (offers.get("availability") or "").split("/")[-1]

    def _num(v):
        if not v:
            return None
        m = re.search(r'[0-9]+(?:\.[0-9]+)?', v)
        return m.group(0) if m else None

    closure = specs.get("closure")
    return {
        "_slug": slug,
        "_ok": True,
        "name": _norm(product.get("name")),
        "brand": _norm(brand) if brand else None,
        "detail_price": offers.get("price"),
        "availability": "in_stock" if avail.lower() == "instock" else
                        ("out_of_stock" if avail else None),
        "image_hi": product.get("image"),
        "description": _norm(product.get("description")) or None,
        "other_details": json.dumps(specs, ensure_ascii=False) if specs else None,
        "category": category,
        "subcategory": subcategory,
        "internal_sku": note.group(1) if note else None,
        "liquor_style": specs.get("liquor style"),
        "abv": _num(specs.get("alcohol by volume")),
        "standard_drinks": _num(specs.get("standard drinks")),
        "closure": closure,
        "region": specs.get("region"),
        "bottled_in": specs.get("bottled in"),
        "varietal": specs.get("varietal") or specs.get("grape variety"),
        "country": specs.get("country") or specs.get("country of origin"),
        "vintage": specs.get("vintage"),
        "tasting_notes": tasting,
    }


# ---- assemble ---------------------------------------------------------------

def build_record(basic: dict, detail: dict, promo: Optional[dict], now: str) -> dict:
    name = detail.get("name") or basic["raw_name"]
    qty, size = parse_qty_size(name, basic["slug"].replace("-", " "))
    ptype = classify_type(detail.get("closure"), name, size)
    price = detail.get("detail_price") or basic.get("list_price")

    special = bool(promo and promo.get("label") == "Special")
    multibuy = promo.get("multibuy") if promo else None
    discount = bool(promo)  # any promo = a price saving on this shop

    rec = {
        "source_product_id": basic["slug"],
        "internal_sku": detail.get("internal_sku"),
        "raw_name": name,
        "brand": detail.get("brand"),
        "type": ptype,
        "quantity": qty,
        "size": size,
        "current_price_cents": to_cents(float(price)) if price else None,
        "special": special,
        "discount": discount,
        "promo_text": (promo or {}).get("label"),
        "stock_status": detail.get("availability") or "in_stock",
        "category_path": detail.get("category"),
        "sub_category": detail.get("subcategory"),
        "liquor_style": detail.get("liquor_style"),
        "abv": detail.get("abv"),
        "standard_drinks": detail.get("standard_drinks"),
        "closure": detail.get("closure"),
        "region": detail.get("region"),
        "bottled_in": detail.get("bottled_in"),
        "varietal": detail.get("varietal"),
        "country": detail.get("country"),
        "vintage": detail.get("vintage"),
        "tasting_notes": detail.get("tasting_notes"),
        "description": detail.get("description"),
        "other_details": detail.get("other_details"),
        "image_lowres": basic.get("list_image"),
        "image_url": detail.get("image_hi") or basic.get("list_image"),
        "product_url": basic["product_url"],
        "observed_at": now,
    }
    if multibuy:
        qty_m, total = multibuy
        rec["promo_type"] = "multibuy"
        rec["multibuy_quantity"] = qty_m
        rec["multibuy_price_cents"] = to_cents(float(total))
    elif special:
        rec["promo_type"] = "special"
    return rec


def run(limit: Optional[int], workers: int, enum_pages: Optional[int] = None) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print("Pass 1a: enumerating all products via /search ...", flush=True)
    products = enumerate_products(enum_pages)
    print(f"  -> {len(products)} via /search", flush=True)
    if not enum_pages:  # full run: backfill from sitemap (/search caps at ~2,400)
        added = 0
        for slug in enumerate_sitemap():
            if slug not in products:
                products[slug] = {"slug": slug, "product_url": f"{BASE}/lines/{slug}",
                                  "raw_name": slug.replace("-", " "),
                                  "list_price": None, "list_image": None}
                added += 1
        print(f"  + {added} extra slugs from sitemap (total {len(products)})", flush=True)
    print("Pass 1b: enumerating promo set via /search?special:1 ...", flush=True)
    promos = enumerate_promos(enum_pages)
    print(f"  -> {len(promos)} on promo", flush=True)

    slugs = list(products)
    if limit:
        slugs = slugs[:limit]
        print(f"(limited to {len(slugs)} for this run)")

    print(f"Pass 2: fetching {len(slugs)} detail pages with {workers} workers ...")
    details: dict[str, dict] = {}
    done = 0
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_detail, s): s for s in slugs}
        for fut in cf.as_completed(futs):
            d = fut.result()
            details[d["_slug"]] = d
            done += 1
            if done % 200 == 0 or done == len(slugs):
                print(f"  detail {done}/{len(slugs)}", flush=True)

    records = []
    dropped = 0
    for slug in slugs:
        d = details.get(slug, {"_slug": slug})
        rec = build_record(products[slug], d, promos.get(slug), now)
        # drop dead sitemap entries (detail 404 and no listing price)
        if rec.get("current_price_cents") is None and not d.get("_ok"):
            dropped += 1
            continue
        records.append(clean_record(rec))
    if dropped:
        print(f"  (dropped {dropped} dead/404 entries)", flush=True)

    ok = sum(1 for d in details.values() if d.get("_ok"))
    priced = sum(1 for r in records if r.get("current_price_cents"))
    specials = sum(1 for r in records if r.get("special"))
    disc = sum(1 for r in records if r.get("discount"))
    withabv = sum(1 for r in records if r.get("abv"))
    path = write_jsonl("thebottleo_glenfield", BRANCH_NAME, records)
    print(f"\n{BRANCH_NAME}: {len(records)} products "
          f"(details ok {ok}, priced {priced}, special {specials}, discount {disc}, "
          f"abv {withabv}) -> {path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="The Bottle-O Glenfield branch scraper")
    p.add_argument("--limit", type=int, default=None, help="cap #products (testing)")
    p.add_argument("--workers", type=int, default=8, help="concurrent detail fetches")
    p.add_argument("--enum-pages", type=int, default=None, help="cap enumeration pages (testing)")
    args = p.parse_args(argv)
    run(args.limit, args.workers, args.enum_pages)


if __name__ == "__main__":
    main()
