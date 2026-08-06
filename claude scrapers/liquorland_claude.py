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
from liquor_common import parse_qty_size, classify_type

BASE = "https://www.liquorland.co.nz"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
PER_PAGE = 24
DEFAULT_CATEGORIES = ["beer", "wine", "spirits", "rtd", "cider"]
# Liquorland's placeholder unitprice for items a store doesn't price/stock is
# ~99999.99 (with a few $100k+ variants); everything real is far below this.
PRICE_SENTINEL = 99999.0

# Liquorland's fortnightly specials live in the "digital mailer", a third-party
# Salefinder catalogue (embed.salefinder.co.nz, retailer 73). It is a single
# NATIONAL catalogue — there is no per-branch version — so we fetch it once and
# flag only the products THIS branch actually stocks (matched by URL/barcode).
# The catalogue's saleId rotates each fortnight, so we discover it dynamically.
SALEFINDER_HOST = "https://embed.salefinder.co.nz"
SALEFINDER_RETAILER = 73
SALEFINDER_LOCATION = 13  # embed default; catalogue content is location-agnostic

_LL_HOST_RE = re.compile(r"^https?://(?:www\.)?liquorland\.co\.nz/", re.I)
_BARCODE_TAIL_RE = re.compile(r"-(\d{6,14})$")


def _is_real_price(v: str) -> bool:
    try:
        return float(v) < PRICE_SENTINEL
    except (TypeError, ValueError):
        return False


def _ll_slug(url: Optional[str]) -> str:
    """Normalise a Liquorland product URL to a bare slug for special-matching."""
    return _LL_HOST_RE.sub("", (url or "")).strip("/").lower()


def _to_float(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


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

    # ---- specials (digital mailer / Salefinder) -------------------------------
    def _get_jsonp(self, url: str) -> dict:
        """GET a Salefinder JSONP endpoint (Referer must be liquorland) and strip
        the `(...)` / `cb(...)` wrapper, returning the decoded JSON object."""
        req = urllib.request.Request(url, headers={
            "User-Agent": UA, "Accept": "application/json, text/javascript, */*",
            "Referer": BASE + "/", "X-Requested-With": "XMLHttpRequest"})
        with self.opener.open(req, timeout=30) as r:
            body = r.read().decode("utf-8", "replace").strip()
        m = re.match(r"^[A-Za-z0-9_.$]*\((.*)\)\s*;?\s*$", body, re.S)
        return json.loads(m.group(1) if m else body)

    def _current_sale_id(self) -> Optional[str]:
        """Discover the live catalogue's saleId (it rotates each fortnight)."""
        try:
            d = self._get_jsonp(f"{SALEFINDER_HOST}/catalogues/view/"
                                f"{SALEFINDER_RETAILER}/?format=json&saleGroup=0")
        except Exception as exc:
            print(f"  [specials] catalogue list failed: {exc}", file=sys.stderr)
            return None
        m = re.search(r"saleId=(\d+)", d.get("content", ""))
        return m.group(1) if m else None

    def fetch_specials(self) -> dict:
        """Return {slug/barcode -> promo dict} for the current mailer catalogue.

        Each promo dict carries the advertised (special) price, catalogue page,
        and the sale's name + start/end dates. Keyed by BOTH the normalised
        product slug and the barcode in the URL tail so callers can match either.
        Returns {} on any failure so the scrape still completes without specials.
        """
        sale_id = self._current_sale_id()
        if not sale_id:
            return {}
        url = (f"{SALEFINDER_HOST}/catalogue/svgData/{sale_id}/?format=json"
               f"&pagetype=catalogue2&retailerId={SALEFINDER_RETAILER}"
               f"&saleGroup=0&locationId={SALEFINDER_LOCATION}")
        try:
            data = self._get_jsonp(url)
        except Exception as exc:
            print(f"  [specials] svgData failed: {exc}", file=sys.stderr)
            return {}
        sale_name = data.get("saleName") or "Specials"
        starts = (data.get("startDate") or "")[:10] or None
        ends = (data.get("endDate") or "")[:10] or None
        out: dict[str, dict] = {}
        tiles = 0
        for page in data.get("catalogue") or []:
            if not isinstance(page, dict):
                continue
            page_no = page.get("pageNo")
            for v in page.values():
                if not (isinstance(v, dict) and v.get("itemId") and v.get("itemName")):
                    continue
                tiles += 1
                promo = {
                    "special_price_cents": to_cents(_to_float(v.get("lowestPrice"))),
                    "promo_text": sale_name,
                    "promo_starts_at": starts,
                    "promo_ends_at": ends,
                    "source_promotion_id": sale_id,
                    "catalogue_page": page_no,
                }
                slug = _ll_slug(v.get("href"))
                if slug:
                    out.setdefault(slug, promo)
                bc = _BARCODE_TAIL_RE.search(slug or "")
                if bc:
                    out.setdefault(bc.group(1), promo)
        print(f"  [specials] '{sale_name}' {starts}..{ends}: {tiles} tiles "
              f"(saleId {sale_id})")
        return out

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
                # Split quantity + size from the name (variant.size is a pack
                # descriptor like "15Pk"; the ml is only in the product name).
                qty, size = parse_qty_size(prod["name"], extra.get("size") or "")
                out.append({
                    "source_product_id": (extra.get("barcode")
                                          or (slug or "").rsplit("-", 1)[-1] or slug),
                    "barcode": extra.get("barcode"),
                    "raw_name": prod["name"],
                    "type": classify_type(None, prod["name"], size),
                    "quantity": qty,
                    "size": size,
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

    def _apply_specials(self, rows: list[dict]) -> int:
        """Flag rows that appear in the current mailer catalogue as on-special.
        Only STOCKED rows (with a price) are flagged, so the national catalogue is
        narrowed to this branch's actual specials. Returns the count flagged."""
        promos = self.fetch_specials()
        if not promos:
            return 0
        flagged = 0
        for r in rows:
            if not r.get("current_price_cents"):
                continue  # not stocked/priced at this branch — skip
            promo = promos.get(_ll_slug(r.get("product_url")))
            if not promo and r.get("barcode"):
                promo = promos.get(r["barcode"])
            if not promo:
                continue
            r["promo_type"] = "special"
            r["special"] = True
            r["discount"] = True
            r["promo_text"] = promo["promo_text"]
            r["promo_starts_at"] = promo["promo_starts_at"]
            r["promo_ends_at"] = promo["promo_ends_at"]
            r["source_promotion_id"] = promo["source_promotion_id"]
            r["catalogue_page"] = promo["catalogue_page"]
            flagged += 1
        return flagged

    def run(self) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        by_id: dict[str, dict] = {}
        for cat in self.categories:
            for r in self._crawl_category(cat, now):
                key = r["source_product_id"] or r["product_url"]
                by_id.setdefault(key, r)
        rows = list(by_id.values())
        flagged = self._apply_specials(rows)
        print(f"  [specials] flagged {flagged} branch-stocked products on special")
        records = [clean_record(r) for r in rows]
        specials = sum(1 for r in records if r.get("special"))
        path = write_jsonl("liquorland", self.store_label, records)
        print(f"\n{self.store_label}: {len(records)} products "
              f"({specials} on special) -> {path}")


# Branch registry: short key -> (Liquorland storeid, canonical label). --branch
# selects one; omitting --branch scrapes them ALL. Store ids verified against
# /api/stores/<query> (2026-08-05). Specials are ONE national catalogue, so each
# branch flags only the products it actually stocks (see _apply_specials).
BRANCHES: dict[str, tuple[int, str]] = {
    "hobsonville":  (48, "Liquorland Hobsonville"),
    "albany":       (28, "Liquorland Albany"),
    "west-harbour": (41, "Liquorland West Harbour"),
    "glenfield":    (47, "Liquorland Glenfield"),
}


def _norm_branch(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (key or "").lower()).strip("-")


def main(argv=None):
    p = argparse.ArgumentParser(description="Liquorland branch scraper")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--branch", help="branch key: " + ", ".join(BRANCHES)
                   + " (omit to scrape ALL of them)")
    g.add_argument("--storeid", type=int, help="ad-hoc preferred store id (e.g. 48)")
    g.add_argument("--store-query", help="ad-hoc: resolve store id by name, e.g. hobsonville")
    p.add_argument("--categories", default=",".join(DEFAULT_CATEGORIES),
                   help="comma-separated category slugs")
    args = p.parse_args(argv)
    cats = [c.strip() for c in args.categories.split(",") if c.strip()]

    # Ad-hoc single store by id / name (unchanged behaviour).
    if args.storeid is not None or args.store_query:
        s = LiquorlandScraper(cats)
        storeid = args.storeid if args.storeid is not None else s.resolve_store(args.store_query)
        s.set_store(storeid)
        s.run()
        return 0

    # Registry-driven: one branch (--branch) or all of them (default).
    if args.branch:
        key = _norm_branch(args.branch)
        if key not in BRANCHES:
            print(f"error: unknown branch {args.branch!r}; choose from: "
                  f"{', '.join(BRANCHES)}", file=sys.stderr)
            return 2
        targets = [key]
    else:
        targets = list(BRANCHES)
        print(f"no --branch given -> scraping all {len(targets)} Liquorland branches: "
              f"{', '.join(targets)}")

    for key in targets:
        storeid, label = BRANCHES[key]
        print(f"\n=== {label} (storeid {storeid}) ===")
        s = LiquorlandScraper(cats)
        s.set_store(storeid)
        s.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
