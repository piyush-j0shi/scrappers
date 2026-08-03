"""Liquorland (NZ) single-branch price scraper — Stage 1 of the pipeline.

Vue SPA, but every category page server-renders its data, so this is plain HTTP
(no browser). Liquorland stores are locally owned and PRICES VARY BY STORE, so we
bind a preferred store first, then crawl.

Per-store binding:  POST /api/stores/preferred  body storeid=<id>  -> sets cookies
                    (preferredstoreid, ...). Store list + ids: window.stores on
                    /store-locations; search API GET /api/stores/<query>.
Pagination:         GET /{category}?p=<n>  (param is `p`, NOT `page`); 24/page.
                    Page count = ceil(totalItems / 24) from the embedded response.
Data per page:      schema.org JSON-LD Product blocks (name, price, availability,
                    image, url) MERGED by url slug with the internal product array
                    (barcode, pack size, on-special flag, everyday price).

Emits one JSONL file per branch+run via jsonl_export.write_jsonl.

Usage:
    python liquorland_claude.py --storeid 48                 # Liquorland Hobsonville
    python liquorland_claude.py --store-query hobsonville    # resolve id by name
"""
from __future__ import annotations

import argparse
import http.cookiejar
import json
import math
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Optional

from jsonl_export import write_jsonl, to_cents, clean_record

BASE = "https://www.liquorland.co.nz"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
PER_PAGE = 24
DEFAULT_CATEGORIES = ["beer", "wine", "spirits", "rtd", "cider"]
# Liquorland's placeholder unitprice for items a store doesn't price/stock is
# ~99999.99 (with a few $100k+ variants); everything real is far below this.
PRICE_SENTINEL = 99999.0


def _is_real_price(v: str) -> bool:
    try:
        return float(v) < PRICE_SENTINEL
    except (TypeError, ValueError):
        return False


class LiquorlandScraper:
    def __init__(self, categories: list[str]):
        self.categories = categories
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.jar))
        self.opener.addheaders = [("User-Agent", UA), ("Accept", "text/html")]
        self.store_label = "Liquorland"

    # ---- HTTP -----------------------------------------------------------------
    def _get(self, url: str, accept: str = "text/html", tries: int = 3) -> str:
        last = None
        for attempt in range(1, tries + 1):
            try:
                req = urllib.request.Request(url, headers={"Accept": accept})
                with self.opener.open(req, timeout=30) as r:
                    return r.read().decode("utf-8", "replace")
            except Exception as exc:  # urllib raises a zoo of errors
                last = exc
                time.sleep(0.5 * attempt)
        raise RuntimeError(f"GET failed after {tries} tries: {url} ({last})")

    def _post(self, path: str, data: dict) -> str:
        body = urllib.parse.urlencode(data).encode()
        req = urllib.request.Request(BASE + path, data=body, headers={
            "User-Agent": UA, "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded"})
        with self.opener.open(req, timeout=30) as r:
            return r.read().decode("utf-8", "replace")

    # ---- store binding --------------------------------------------------------
    def resolve_store(self, query: str) -> int:
        d = json.loads(self._get(f"{BASE}/api/stores/{urllib.parse.quote(query)}",
                                  accept="application/json"))
        stores = (d.get("data") or {}).get("stores") or []
        if not stores:
            raise RuntimeError(f"no Liquorland store matched {query!r}")
        s = stores[0]
        print(f"resolved store: {s.get('label')} (storeid={s.get('storeid')})")
        return int(s["storeid"])

    def set_store(self, storeid: int) -> None:
        resp = json.loads(self._post("/api/stores/preferred", {"storeid": storeid}))
        store = (resp.get("data") or {}).get("store") or {}
        self.store_label = store.get("label") or f"Liquorland {storeid}"
        if not any(c.name == "preferredstoreid" for c in self.jar):
            raise RuntimeError("preferred-store cookie was not set")
        print(f"bound store: {self.store_label}")

    # ---- parsing --------------------------------------------------------------
    _LD = re.compile(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.S)
    # internal product array entry: url, on-special flag, barcode, size, prices
    _INT = re.compile(
        r'"url":"(\\?/[^"]+)","categoryid":\d+,"categoryids":\[[^\]]*\],'
        r'"category":[^,]*,"saleprice":(true|false),"variant":\{'
        r'"barcode":"([^"]*)","size":"([^"]*)"[^}]*?'
        r'"baseunitprice":"([^"]*)","unitprice":"([^"]*)"')

    def _ld_products(self, html: str) -> list[dict]:
        out: list[dict] = []

        def walk(o):
            if isinstance(o, dict):
                if o.get("@type") == "Product":
                    off = o.get("offers") or {}
                    if isinstance(off, list):
                        off = off[0] if off else {}
                    out.append({
                        "name": o.get("name"),
                        "url": o.get("url"),
                        "image": o.get("image"),
                        "price": off.get("price"),
                        "availability": (off.get("availability") or "").split("/")[-1],
                    })
                for v in o.values():
                    walk(v)
            elif isinstance(o, list):
                for v in o:
                    walk(v)

        for block in self._LD.findall(html):
            try:
                walk(json.loads(block))
            except Exception:
                pass
        return out

    def _internal(self, html: str) -> dict[str, dict]:
        by_slug: dict[str, dict] = {}
        for url, sale, bc, size, base, unit in self._INT.findall(html):
            slug = url.replace("\\/", "/")
            by_slug[slug] = {"barcode": bc or None, "size": size or None,
                             "on_special": sale == "true",
                             "base_price": base, "unit_price": unit}
        return by_slug

    @staticmethod
    def _slug_of(url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        return "/" + url.split("liquorland.co.nz/")[-1].lstrip("/")

    def _total_items(self, html: str) -> int:
        m = re.search(r'"itemsPerPage":\d+,"totalItems":(\d+)', html)
        return int(m.group(1)) if m else 0

    # ---- crawl ----------------------------------------------------------------
    def _crawl_category(self, cat: str, now: str) -> list[dict]:
        first = self._get(f"{BASE}/{cat}?p=1")
        total = self._total_items(first)
        pages = max(1, math.ceil(total / PER_PAGE)) if total else 1
        print(f"  [{cat}] {total} products -> {pages} page(s)")
        out: list[dict] = []
        for p in range(1, pages + 1):
            html = first if p == 1 else self._get(f"{BASE}/{cat}?p={p}")
            ld = self._ld_products(html)
            internal = self._internal(html)
            for prod in ld:
                slug = self._slug_of(prod["url"])
                extra = internal.get(slug, {})
                on_special = extra.get("on_special", False)
                base = extra.get("base_price")
                # Price comes from the internal array (`unitprice`, present for every
                # product); the JSON-LD `offers.price` is only rendered for some and
                # is used as a fallback.
                unit = extra.get("unit_price") or prod.get("price")
                # Liquorland lists the whole national catalogue and uses a ~99999.99
                # sentinel unitprice for items this store does not price/stock (all
                # such rows are out_of_stock). Store no price rather than the fake one;
                # real bottles top out well under $1000.
                unit_cents = to_cents(float(unit)) if unit and _is_real_price(unit) else None
                base_cents = (to_cents(float(base))
                              if on_special and base and base != unit
                              and _is_real_price(base) else None)
                out.append({
                    "source_product_id": (extra.get("barcode")
                                          or (slug or "").rsplit("-", 1)[-1] or slug),
                    "barcode": extra.get("barcode"),
                    "raw_name": prod["name"],
                    "size": extra.get("size"),
                    "category_path": cat,
                    "current_price_cents": unit_cents,
                    # everyday price when it differs from the on-special price
                    "comparison_price_cents": base_cents,
                    "promo_type": "special" if on_special else None,
                    "stock_status": ("in_stock" if prod.get("availability") == "InStock"
                                     else "out_of_stock"),
                    "product_url": prod["url"],
                    "image_url": prod.get("image"),
                    "observed_at": now,
                })
            if p < pages:
                time.sleep(0.3)
        return out

    def run(self) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        by_id: dict[str, dict] = {}
        for cat in self.categories:
            for r in self._crawl_category(cat, now):
                key = r["source_product_id"] or r["product_url"]
                by_id.setdefault(key, r)
        records = [clean_record(r) for r in by_id.values()]
        specials = sum(1 for r in records if r.get("promo_type"))
        path = write_jsonl("liquorland", self.store_label, records)
        print(f"\n{self.store_label}: {len(records)} products "
              f"({specials} on special) -> {path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Liquorland single-branch scraper")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--storeid", type=int, help="preferred store id (e.g. 48 = Hobsonville)")
    g.add_argument("--store-query", help="resolve store id by name, e.g. hobsonville")
    p.add_argument("--categories", default=",".join(DEFAULT_CATEGORIES),
                   help="comma-separated category slugs")
    args = p.parse_args(argv)
    cats = [c.strip() for c in args.categories.split(",") if c.strip()]
    s = LiquorlandScraper(cats)
    storeid = args.storeid
    if storeid is None and args.store_query:
        storeid = s.resolve_store(args.store_query)
    if storeid is None:
        print("error: pass --storeid or --store-query", file=sys.stderr)
        return 2
    s.set_store(storeid)
    s.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
