#!/usr/bin/env python3
"""
import_products.py — Pico branch importer.

Loads ONE retailer-branch scrape export (JSONL / JSON array / CSV produced by the
scrapers) and writes it into the live pico-prod schema, exactly as described in the
"Pico Scraper Data Contract and Import Guide". The scrapers never touch the
database; this importer owns all pico-prod writes, matching, current price/stock,
change-only history, promotions, and the admin review queue.

Three modes (Guide section 9):

  --dry-run        Parse-only validation. NO database connection. Validates every
                   row, deduplicates by stable id, prints a report. Use this to
                   check an export before any DB access (no secrets needed).

  --rollback-test  Run the FULL import inside one transaction, then ROLLBACK.
                   Exercises the whole database path and leaves nothing behind.

  (real import)    Run the full import and COMMIT. Add --full-branch to also mark
                   products not seen in this run as out_of_stock and deactivate
                   their stale specials (only safe after a complete scrape).

Connection: reads DATABASE_URL (Supabase Postgres pooler) from the environment /
.env. The URL is a secret and is never printed or written to output.

Examples:
  python import_products.py --input exports/newworld_x.jsonl \\
      --retailer new-world --external-store-id 1A2B --source-system newworld_scraper --dry-run

  python import_products.py --input exports/newworld_x.jsonl \\
      --retailer new-world --external-store-id 1A2B --source-system newworld_scraper --rollback-test

  python import_products.py --input exports/newworld_x.jsonl \\
      --retailer new-world --external-store-id 1A2B --source-system newworld_scraper --full-branch
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
    load_dotenv(Path(__file__).resolve().parent / ".env")
except Exception:  # pragma: no cover - dotenv is optional at runtime
    pass

log = logging.getLogger("import_products")


# --------------------------------------------------------------------------- #
# Contract constants
# --------------------------------------------------------------------------- #
ALLOWED_PROMO_TYPES = {
    "special", "percent_off", "half_price", "better_than_half_price",
    "multibuy_fixed_price", "multibuy_quantity_discount", "member_price",
    "fresh_deal", "clearance", "coupon", "loyalty_points", "bundle",
    "everyday_low_price", "competition_badge", "other",
}
ALLOWED_STOCK = {"in_stock", "out_of_stock", "low_stock", "unknown"}

# Identifier types treated as a barcode when matching (catalog.product_identifiers).
BARCODE_IDENTIFIER_TYPES = ("barcode", "gtin", "ean", "upc")

# Full-branch import aborts if more than this fraction of rows fail validation
# (Guide section 13: recommended production threshold 0.5%).
MAX_FAILED_FRACTION = 0.005

# Default mapping from the --retailer slug to the loyalty programme code used when
# a row sets card_required but omits required_loyalty_program_code.
DEFAULT_LOYALTY_BY_RETAILER = {
    "new-world": "club-plus",
    "paknsave": "club-plus",
    "pak-n-save": "club-plus",
    "woolworths": "everyday-rewards",
}


# --------------------------------------------------------------------------- #
# File reading
# --------------------------------------------------------------------------- #
def read_records(path: Path) -> list[dict]:
    """Read JSONL (preferred), a JSON array, or CSV into a list of dicts."""
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        return list(_read_jsonl(path))
    if suffix == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):  # tolerate {"products": [...]} wrappers
            for key in ("products", "items", "records", "observations"):
                if isinstance(data.get(key), list):
                    return list(data[key])
            raise ValueError(f"{path.name}: JSON object has no product array")
        if not isinstance(data, list):
            raise ValueError(f"{path.name}: expected a JSON array")
        return data
    if suffix == ".csv":
        with path.open(newline="", encoding="utf-8") as fh:
            return list(csv.DictReader(fh))
    raise ValueError(f"unsupported input format: {path.suffix!r} (use .jsonl/.json/.csv)")


def _read_jsonl(path: Path) -> Iterable[dict]:
    with path.open(encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path.name}:{lineno}: invalid JSON ({exc})") from exc


# --------------------------------------------------------------------------- #
# Money / value helpers
# --------------------------------------------------------------------------- #
def to_cents(value: Any) -> Optional[int]:
    """Best-effort dollars/cents -> integer cents. Returns None when not parseable."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value * 100)
    s = str(value).strip().replace("$", "").replace(",", "")
    if not s:
        return None
    try:
        return round(float(s) * 100) if "." in s else int(s)
    except ValueError:
        return None


def _first_cents(rec: dict, cents_keys: tuple, dollar_keys: tuple) -> Optional[int]:
    """Prefer explicit *_cents fields, then fall back to dollar fields (Guide §3)."""
    for k in cents_keys:
        if rec.get(k) not in (None, ""):
            v = rec[k]
            return v if isinstance(v, int) else to_cents(v)
    for k in dollar_keys:
        if rec.get(k) not in (None, ""):
            return to_cents(rec[k])
    return None


def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


# --------------------------------------------------------------------------- #
# Product-matching helpers (Pico Scraper Product Matching Flow)
# --------------------------------------------------------------------------- #
def db_normalized_name(name: str) -> str:
    """Mirror catalog.canonical_products/brands' GENERATED normalized_name column
    exactly: lower(regexp_replace(name, '\\s+', ' ', 'g')). Whitespace-collapse
    only — no punctuation stripping. Lookups MUST use this or they silently miss."""
    return re.sub(r"\s+", " ", name).lower()


def canonical_name(rec: dict) -> str:
    """Base product name with size stripped, e.g. 'Haribo Goldbears' (not '...150g').
    Prefer brand + clean_name (scrapers already split these); clean_name from
    Foodstuffs already excludes the brand and size, so this reconstructs the form
    the doc's example expects without any fragile regex on raw_name."""
    brand = (rec.get("brand") or "").strip()
    clean = (rec.get("clean_name") or "").strip()
    if brand and clean:
        if clean.lower().startswith(brand.lower()):
            return clean
        return f"{brand} {clean}".strip()
    if clean:
        return clean
    name = rec["raw_name"]
    size = rec.get("size")
    if size and name.lower().endswith(str(size).lower()):
        name = name[: -len(size)].strip()
    return name


def _fmt_dec(d: Decimal) -> str:
    if d == d.to_integral_value():
        return str(int(d))
    return format(d, "f").rstrip("0").rstrip(".")


_SIZE_MULTI_RE = re.compile(r"^\s*(\d+)\s*x\s*(\d+(?:\.\d+)?)\s*(ml|l|g|kg|mg)\s*$", re.I)
_SIZE_SIMPLE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ml|l|g|kg|mg)\s*$", re.I)
_SIZE_PACK_RE = re.compile(r"^\s*(\d+)\s*(?:pk|pack)\s*$", re.I)
_SIZE_EACH_RE = re.compile(r"^\s*(each|ea)\s*$", re.I)


def parse_size(size_str: Optional[str]):
    """Parse the scraper's clean `size` field (e.g. '750ml', '6 x 330ml', '4pk')
    into (size_value, size_unit, package_quantity, total_size_value, total_size_unit,
    normalized_size). Falls back to a whitespace-stripped literal when the pattern
    isn't recognized, so exact-match lookups still work even if the numeric fields
    stay empty."""
    if not size_str:
        return None, None, Decimal("1"), None, None, None
    s = size_str.strip()

    m = _SIZE_MULTI_RE.match(s)
    if m:
        qty_s, val_s, unit = m.groups()
        qty, val, unit = Decimal(qty_s), Decimal(val_s), unit.lower()
        return val, unit, qty, qty * val, unit, f"{_fmt_dec(qty)}x{_fmt_dec(val)}{unit}"

    m = _SIZE_SIMPLE_RE.match(s)
    if m:
        val_s, unit = m.groups()
        val, unit = Decimal(val_s), unit.lower()
        return val, unit, Decimal("1"), val, unit, f"{_fmt_dec(val)}{unit}"

    m = _SIZE_PACK_RE.match(s)
    if m:
        qty = Decimal(m.group(1))
        return None, None, qty, None, None, f"{_fmt_dec(qty)}pk"

    if _SIZE_EACH_RE.match(s):
        return None, "each", Decimal("1"), None, "each", "each"

    norm = re.sub(r"\s+", "", s.lower())
    return None, None, Decimal("1"), None, None, (norm or None)


# --------------------------------------------------------------------------- #
# Validation / normalization
# --------------------------------------------------------------------------- #
class ValidationError(Exception):
    pass


def normalize(rec: dict) -> dict:
    """Validate one source row against the contract and return a normalized record.

    Raises ValidationError with a human reason when the row must be rejected
    (Guide section 13: missing name, missing/negative price, no stable id).
    """
    raw_name = _clean_str(rec.get("raw_name") or rec.get("name"))
    if not raw_name:
        raise ValidationError("missing product name")

    source_product_id = _clean_str(rec.get("source_product_id"))
    retailer_sku = _clean_str(rec.get("retailer_sku"))
    barcode = _clean_str(rec.get("barcode"))
    if not (source_product_id or retailer_sku or barcode):
        raise ValidationError("no stable id, sku, or barcode")

    current = _first_cents(
        rec, ("current_price_cents",), ("current_price", "price", "special_price"))
    if current is None:
        raise ValidationError("missing current price")
    if current < 0:
        raise ValidationError("negative current price")

    comparison = _first_cents(
        rec, ("comparison_price_cents",), ("comparison_price", "was_price", "original_price"))
    unit_price = _first_cents(rec, ("unit_price_cents",), ("unit_price",))

    stock_status = (_clean_str(rec.get("stock_status")) or "unknown").lower()
    if stock_status not in ALLOWED_STOCK:
        stock_status = "unknown"

    stock_quantity = rec.get("stock_quantity")
    if stock_quantity in ("", None):
        stock_quantity = None
    elif not isinstance(stock_quantity, int):
        try:
            stock_quantity = int(stock_quantity)
        except (TypeError, ValueError):
            stock_quantity = None

    promo_type = _clean_str(rec.get("promo_type"))
    if promo_type and promo_type not in ALLOWED_PROMO_TYPES:
        promo_type = "other"
    promo_text = _clean_str(rec.get("promo_text"))
    card_required = bool(rec.get("card_required"))
    loyalty_code = _clean_str(rec.get("required_loyalty_program_code"))

    observed_at = _clean_str(rec.get("observed_at")) or \
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    raw_row = rec.get("raw_row")
    if not isinstance(raw_row, dict):
        raw_row = {}

    return {
        "source_product_id": source_product_id,
        "retailer_sku": retailer_sku,
        "barcode": barcode,
        "raw_name": raw_name,
        "clean_name": _clean_str(rec.get("clean_name")),
        "brand": _clean_str(rec.get("brand")),
        "category_path": _clean_str(rec.get("category_path")),
        "size": _clean_str(rec.get("size")),
        "current_price_cents": current,
        "comparison_price_cents": comparison,
        "unit_price_cents": unit_price,
        "unit_label": _clean_str(rec.get("unit_label")),
        "stock_status": stock_status,
        "stock_quantity": stock_quantity,
        "promo_text": promo_text,
        "promo_type": promo_type,
        "card_required": card_required,
        "required_loyalty_program_code": loyalty_code,
        "product_url": _clean_str(rec.get("product_url")),
        "image_url": _clean_str(rec.get("image_url")),
        "observed_at": observed_at,
        "raw_row": raw_row,
    }


def dedupe(records: list[dict]) -> tuple[list[dict], int]:
    """Deduplicate by stable id (source_product_id, then retailer_sku, then barcode),
    keeping the most complete record. Returns (deduped, duplicates_removed)."""
    def key(r: dict):
        return r.get("source_product_id") or r.get("retailer_sku") or r.get("barcode")

    def completeness(r: dict) -> int:
        return sum(1 for v in r.values() if v not in (None, "", {}, []))

    best: dict[Any, dict] = {}
    dupes = 0
    for r in records:
        k = key(r)
        if k in best:
            dupes += 1
            log.info("DUPLICATE skipped: id=%s — %s", k, r.get("raw_name") or r.get("name"))
            if completeness(r) > completeness(best[k]):
                best[k] = r
        else:
            best[k] = r
    return list(best.values()), dupes


# --------------------------------------------------------------------------- #
# Database importer
# --------------------------------------------------------------------------- #
class Importer:
    """Owns the single import transaction and all pico-prod writes."""

    def __init__(self, conn, retailer_slug: str, source_system: str):
        self.conn = conn
        self.cur = conn.cursor()
        self.retailer_slug = retailer_slug
        self.source_system = source_system
        self.retailer_id: Optional[str] = None
        self.branch_id: Optional[str] = None
        self.source_system_id: Optional[str] = None
        self.run_id: Optional[str] = None
        self._loyalty_cache: dict[str, Optional[str]] = {}
        # counters
        self.inserted = 0
        self.updated = 0
        self.failed = 0
        self.price_changes = 0
        self.new_products = 0
        self.reviews_created = 0
        self.reviews_resolved = 0
        self.matched = 0
        self.canonicals_created = 0
        self.variants_created = 0
        self.identifiers_created = 0
        self._seen_retailer_product_ids: set[str] = set()

    def _one(self, sql: str, params: tuple = ()):  # first row or None
        self.cur.execute(sql, params)
        return self.cur.fetchone()

    # ---- resolution -------------------------------------------------------- #
    def resolve(self, *, external_store_id, branch_slug, branch_code, branch_name):
        row = self._one(
            "SELECT id FROM catalog.retailers WHERE slug = %s", (self.retailer_slug,))
        if not row:
            raise RuntimeError(f"retailer slug not found: {self.retailer_slug!r}")
        self.retailer_id = row[0]

        self.branch_id = self._resolve_branch(
            external_store_id, branch_slug, branch_code, branch_name)
        if not self.branch_id:
            raise RuntimeError(
                "could not resolve branch — refusing to import national/default "
                "prices as branch prices (Guide section 7)")

        self.source_system_id = self._resolve_source_system()

    def _resolve_branch(self, external_store_id, branch_slug, branch_code, branch_name):
        if external_store_id:
            row = self._one(
                "SELECT e.branch_id FROM catalog.external_store_ids e "
                "JOIN catalog.branches b ON b.id = e.branch_id "
                "WHERE e.external_id = %s AND b.retailer_id = %s",
                (external_store_id, self.retailer_id))
            if row:
                return row[0]
        if branch_slug:
            row = self._one(
                "SELECT id FROM catalog.branches WHERE slug = %s AND retailer_id = %s",
                (branch_slug, self.retailer_id))
            if row:
                return row[0]
        if branch_code:
            row = self._one(
                "SELECT id FROM catalog.branches WHERE branch_code = %s AND retailer_id = %s",
                (branch_code, self.retailer_id))
            if row:
                return row[0]
        if branch_name:
            row = self._one(
                "SELECT id FROM catalog.branches WHERE name = %s AND retailer_id = %s",
                (branch_name, self.retailer_id))
            if row:
                return row[0]
        return None

    def _resolve_source_system(self):
        row = self._one(
            "SELECT id FROM ingest.source_systems WHERE name = %s", (self.source_system,))
        if row:
            return row[0]
        row = self._one(
            "INSERT INTO ingest.source_systems (source_kind, name, retailer_id, is_active) "
            "VALUES ('scraper', %s, %s, true) RETURNING id",
            (self.source_system, self.retailer_id))
        log.info("registered new source_system %r", self.source_system)
        return row[0]

    def _loyalty_id(self, code: Optional[str]) -> Optional[str]:
        if not code:
            return None
        if code in self._loyalty_cache:
            return self._loyalty_cache[code]
        row = self._one("SELECT id FROM loyalty.programs WHERE code = %s", (code,))
        pid = row[0] if row else None
        if not row:
            log.warning("unknown loyalty program code %r (card flag kept, id left null)", code)
        self._loyalty_cache[code] = pid
        return pid

    # ---- run lifecycle ----------------------------------------------------- #
    def start_run(self, run_type: str, total: int):
        row = self._one(
            "INSERT INTO ingest.import_runs "
            "(source_system_id, retailer_id, branch_id, run_type, status, "
            " started_at, total_observations) "
            "VALUES (%s, %s, %s, %s, 'running', now(), %s) RETURNING id",
            (self.source_system_id, self.retailer_id, self.branch_id, run_type, total))
        self.run_id = row[0]

    def finish_run(self, status: str, error_log: Optional[str] = None):
        self.cur.execute(
            "UPDATE ingest.import_runs SET status = %s, finished_at = now(), "
            "records_inserted = %s, records_updated = %s, records_failed = %s, "
            "price_changes = %s, new_products = %s, error_log = %s WHERE id = %s",
            (status, self.inserted, self.updated, self.failed, self.price_changes,
             self.new_products, error_log, self.run_id))

    # ---- per-observation --------------------------------------------------- #
    def process(self, rec: dict):
        from psycopg.types.json import Json
        loyalty_id = self._loyalty_id(
            rec["required_loyalty_program_code"]
            or (DEFAULT_LOYALTY_BY_RETAILER.get(self.retailer_slug)
                if rec["card_required"] else None))
        normalized_promo = rec["promo_type"] if rec["promo_type"] in ALLOWED_PROMO_TYPES else None

        # 1. ingest.scraped_observations — always one row per product per run.
        self.cur.execute(
            "INSERT INTO ingest.scraped_observations "
            "(import_run_id, retailer_id, branch_id, source_product_id, retailer_sku, "
            " barcode, raw_name, raw_brand, raw_category_path, product_url, image_url, "
            " price_cents, comparison_price_cents, unit_price_cents, unit_label, "
            " stock_status, stock_quantity, raw_promo_text, normalized_promo_type, "
            " observed_at, raw_row, card_required, required_loyalty_program_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
            "%s::timestamptz,%s,%s,%s)",
            (self.run_id, self.retailer_id, self.branch_id, rec["source_product_id"],
             rec["retailer_sku"], rec["barcode"], rec["raw_name"], rec["brand"],
             rec["category_path"], rec["product_url"], rec["image_url"],
             rec["current_price_cents"], rec["comparison_price_cents"],
             rec["unit_price_cents"], rec["unit_label"], rec["stock_status"],
             rec["stock_quantity"], rec["promo_text"], normalized_promo,
             rec["observed_at"], Json(rec["raw_row"]), rec["card_required"], loyalty_id))

        # 2. catalog.retailer_products — durable retailer listing.
        rp_id, canonical_id, variant_id, is_new = self._upsert_retailer_product(rec)
        self._seen_retailer_product_ids.add(rp_id)
        if is_new:
            self.new_products += 1

        # 3. matching — barcode-first; new/unmatched -> admin review queue.
        canonical_id, variant_id = self._match(rec, rp_id, canonical_id, variant_id)

        # 4. promotions (offer rule) before current state, so we can link it.
        promo_id = self._upsert_promotion(rec, rp_id, loyalty_id) if rec["promo_type"] else None

        # 5. catalog.branch_product_current (+ change-only price_history).
        self._upsert_current(rec, rp_id, canonical_id, variant_id, promo_id)

    def _upsert_retailer_product(self, rec: dict):
        from psycopg.types.json import Json
        row = self._one(
            "SELECT id, canonical_product_id, product_variant_id FROM catalog.retailer_products "
            "WHERE retailer_id = %s AND source_product_id IS NOT DISTINCT FROM %s "
            "AND (%s::text IS NOT NULL OR retailer_sku IS NOT DISTINCT FROM %s) LIMIT 1",
            (self.retailer_id, rec["source_product_id"],
             rec["source_product_id"], rec["retailer_sku"]))
        if row:
            rp_id, canonical_id, variant_id = row
            # raw_latest is per-retailer, so every branch export writes the same row.
            # An export with an empty card (a stale file, or a failed detail fetch) must
            # never wipe a populated one — keep the existing card unless this row has
            # something to say. scraped_observations still records the raw truth per run.
            self.cur.execute(
                "UPDATE catalog.retailer_products SET "
                "retailer_sku = COALESCE(%s, retailer_sku), raw_name = %s, "
                "clean_name = COALESCE(%s, clean_name), brand_text = COALESCE(%s, brand_text), "
                "raw_category_path = COALESCE(%s, raw_category_path), "
                "product_url = COALESCE(%s, product_url), image_url = COALESCE(%s, image_url), "
                "barcode = COALESCE(%s, barcode), "
                "raw_latest = COALESCE(NULLIF(%s::jsonb, '{}'::jsonb), raw_latest), "
                "last_seen_at = now(), "
                "is_active = true, updated_at = now() WHERE id = %s",
                (rec["retailer_sku"], rec["raw_name"], rec["clean_name"], rec["brand"],
                 rec["category_path"], rec["product_url"], rec["image_url"], rec["barcode"],
                 Json(rec["raw_row"]), rp_id))
            return rp_id, canonical_id, variant_id, False

        row = self._one(
            "INSERT INTO catalog.retailer_products "
            "(retailer_id, retailer_sku, raw_name, clean_name, brand_text, raw_category_path, "
            " product_url, image_url, barcode, source_product_id, raw_latest, match_status, "
            " is_active, first_seen_at, last_seen_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'unmatched',true,now(),now()) RETURNING id",
            (self.retailer_id, rec["retailer_sku"], rec["raw_name"], rec["clean_name"],
             rec["brand"], rec["category_path"], rec["product_url"], rec["image_url"],
             rec["barcode"], rec["source_product_id"], Json(rec["raw_row"])))
        return row[0], None, None, True

    def _match(self, rec, rp_id, canonical_id, variant_id):
        """Pico Scraper Product Matching Flow: barcode first, then exact
        brand+name+size, then create, review only on genuine conflict.

        Evidence-based design note: measured name-similarity across ~14,800 real
        cross-chain same-barcode pairs (NW/PNS/Woolworths exports) — genuinely
        identical products score as low as 0.25 because retailers word their copy
        very differently ("E2 Mango Liquid Energy Fruit Drink" vs "E2 Sports Drink
        Mango"). Product-name text is NOT a reliable conflict signal. `size`
        (structured, retailer-supplied) is: use that instead. See
        [[product-matching-flow-deferred]] memory for the full measurement.
        """
        if canonical_id and variant_id:
            return canonical_id, variant_id  # already matched on a previous run

        barcode = rec["barcode"]
        _, _, _, _, _, rec_size = parse_size(rec.get("size"))
        cname = canonical_name(rec)

        if barcode:
            row = self._one(
                "SELECT canonical_product_id, product_variant_id FROM catalog.product_identifiers "
                "WHERE identifier_type = 'barcode' AND identifier_value = %s "
                "AND retailer_id IS NULL", (barcode,))
            if row and row[0]:
                cid, vid = row
                if self._size_conflicts(vid, rec_size):
                    return self._queue_review(rec, rp_id, "barcode_conflict", cid, vid, Decimal("0.35"))
                return self._link(rp_id, cid, vid, Decimal("0.98"))

            row = self._one(
                "SELECT canonical_product_id, product_variant_id FROM catalog.retailer_products "
                "WHERE barcode = %s AND canonical_product_id IS NOT NULL AND id <> %s LIMIT 1",
                (barcode, rp_id))
            if row:
                cid, vid = row
                if self._size_conflicts(vid, rec_size):
                    return self._queue_review(rec, rp_id, "barcode_conflict", cid, vid, Decimal("0.35"))
                self._upsert_global_identifier(barcode, cid, vid)
                return self._link(rp_id, cid, vid, Decimal("0.95"))

            # brand-new barcode, no existing link anywhere — safe to create.
            cid, vid = self._create_canonical_and_variant(rec, cname, rec_size)
            self._upsert_global_identifier(barcode, cid, vid)
            return self._link(rp_id, cid, vid, Decimal("0.90"))

        # No barcode at all: only auto-match/create on an exact brand+name+size hit,
        # otherwise this is genuinely low-confidence (Guide: "size or pack cannot be
        # parsed confidently" / no strong identifier at all) -> review.
        cname_norm = db_normalized_name(cname)
        if rec_size:
            row = self._one(
                "SELECT cp.id, pv.id FROM catalog.product_variants pv "
                "JOIN catalog.canonical_products cp ON cp.id = pv.canonical_product_id "
                "WHERE cp.normalized_name = %s AND pv.normalized_size = %s LIMIT 1",
                (cname_norm, rec_size))
            if row:
                return self._link(rp_id, row[0], row[1], Decimal("0.75"))

            cid, vid = self._create_canonical_and_variant(rec, cname, rec_size)
            return self._link(rp_id, cid, vid, Decimal("0.70"))

        return self._queue_review(rec, rp_id, "no_barcode_no_size", None, None, Decimal("0.15"))

    def _size_conflicts(self, variant_id, rec_size: Optional[str]) -> bool:
        """The only conflict signal we trust (see _match docstring): a structured
        size disagreement. Missing size on either side is not a conflict."""
        if not rec_size:
            return False
        existing = self._variant_size(variant_id)
        return bool(existing and existing != rec_size)

    def _variant_size(self, variant_id) -> Optional[str]:
        row = self._one(
            "SELECT normalized_size FROM catalog.product_variants WHERE id = %s", (variant_id,))
        return row[0] if row else None

    def _link(self, rp_id, canonical_id, variant_id, confidence: Decimal):
        self.cur.execute(
            "UPDATE catalog.retailer_products SET canonical_product_id = %s, "
            "product_variant_id = %s, match_status = 'matched', match_confidence = %s, "
            "updated_at = now() WHERE id = %s",
            (canonical_id, variant_id, confidence, rp_id))
        # Doc: "Resolve or avoid the matching review rows because this is a confident
        # match" — clear any stale review now that we have a real link. Includes the
        # legacy 'pending' status: 4,139 rows were queued under that name before this
        # matcher existed (the DB's real convention/partial-unique-index is 'open').
        self.cur.execute(
            "UPDATE catalog.product_match_reviews SET status = 'resolved', "
            "resolved_at = now(), resolution_notes = 'auto-resolved: matcher found a confident link' "
            "WHERE retailer_product_id = %s AND status IN ('open', 'pending')", (rp_id,))
        if self.cur.rowcount:
            self.reviews_resolved += 1
        self.matched += 1
        return canonical_id, variant_id

    def _queue_review(self, rec, rp_id, reason, suggested_cid, suggested_vid, confidence: Decimal):
        from psycopg.types.json import Json
        self.cur.execute(
            "UPDATE catalog.retailer_products SET match_status = 'needs_review', "
            "match_confidence = %s, updated_at = now() WHERE id = %s", (confidence, rp_id))
        self.cur.execute(
            "INSERT INTO catalog.product_match_reviews "
            "(retailer_product_id, suggested_canonical_product_id, suggested_product_variant_id, "
            " review_reason, status, score, raw_evidence) "
            "VALUES (%s,%s,%s,%s,'open',%s,%s) "
            "ON CONFLICT (retailer_product_id, review_reason) WHERE status = 'open' DO UPDATE SET "
            "suggested_canonical_product_id = EXCLUDED.suggested_canonical_product_id, "
            "suggested_product_variant_id = EXCLUDED.suggested_product_variant_id, "
            "score = EXCLUDED.score, raw_evidence = EXCLUDED.raw_evidence",
            (rp_id, suggested_cid, suggested_vid, reason, confidence, Json({
                "barcode": rec["barcode"], "raw_name": rec["raw_name"],
                "brand": rec["brand"], "size": rec["size"]})))
        self.reviews_created += 1
        return None, None

    def _upsert_brand(self, brand_text: Optional[str]) -> Optional[str]:
        brand = (brand_text or "").strip()
        if not brand:
            return None
        row = self._one(
            "INSERT INTO catalog.brands (name) VALUES (%s) "
            "ON CONFLICT (normalized_name) DO UPDATE SET name = catalog.brands.name "
            "RETURNING id", (brand,))
        return row[0]

    def _create_canonical_and_variant(self, rec, cname: str, rec_size: Optional[str]):
        brand_id = self._upsert_brand(rec["brand"])
        row = self._one(
            "INSERT INTO catalog.canonical_products "
            "(brand_id, name, image_url, source, data_quality_score, is_active) "
            "VALUES (%s,%s,%s,%s,0.70,true) RETURNING id",
            (brand_id, cname, rec["image_url"], self.source_system))
        canonical_id = row[0]
        self.canonicals_created += 1

        size_value, size_unit, package_quantity, total_value, total_unit, _ = \
            parse_size(rec.get("size"))
        row = self._one(
            "INSERT INTO catalog.product_variants "
            "(canonical_product_id, variant_name, package_description, size_value, size_unit, "
            " package_quantity, total_size_value, total_size_unit, normalized_size, image_url, "
            " is_active) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true) RETURNING id",
            (canonical_id, rec["raw_name"], rec.get("size"), size_value, size_unit,
             package_quantity, total_value, total_unit, rec_size, rec["image_url"]))
        variant_id = row[0]
        self.variants_created += 1
        return canonical_id, variant_id

    def _upsert_global_identifier(self, barcode: str, canonical_id, variant_id):
        existing = self._one(
            "SELECT 1 FROM catalog.product_identifiers WHERE identifier_type = 'barcode' "
            "AND identifier_value = %s AND retailer_id IS NULL", (barcode,))
        self.cur.execute(
            "INSERT INTO catalog.product_identifiers "
            "(product_variant_id, canonical_product_id, retailer_id, identifier_type, "
            " identifier_value, is_primary, source, first_seen_at, last_seen_at) "
            "VALUES (%s,%s,NULL,'barcode',%s,true,%s,now(),now()) "
            "ON CONFLICT (identifier_type, identifier_value) WHERE retailer_id IS NULL "
            "AND identifier_type = ANY(ARRAY['barcode','gtin','ean','upc']::catalog.product_identifier_type[]) "
            "DO UPDATE SET product_variant_id = EXCLUDED.product_variant_id, "
            "canonical_product_id = EXCLUDED.canonical_product_id, last_seen_at = now()",
            (variant_id, canonical_id, barcode, self.source_system))
        if not existing:
            self.identifiers_created += 1

    def _upsert_current(self, rec, rp_id, canonical_id, variant_id, promo_id):
        existing = self._one(
            "SELECT id, current_price_cents, unit_price_cents, stock_status "
            "FROM catalog.branch_product_current "
            "WHERE branch_id = %s AND retailer_product_id = %s",
            (self.branch_id, rp_id))
        is_special = promo_id is not None
        if existing:
            bpc_id, old_price, old_unit, old_stock = existing
            price_changed = old_price != rec["current_price_cents"]
            unit_changed = old_unit != rec["unit_price_cents"]
            stock_changed = old_stock != rec["stock_status"]
            self.cur.execute(
                "UPDATE catalog.branch_product_current SET "
                "canonical_product_id = COALESCE(%s, canonical_product_id), "
                "product_variant_id = COALESCE(%s, product_variant_id), "
                "current_price_cents = %s, comparison_price_cents = %s, "
                "unit_price_cents = %s, unit_label = %s, stock_status = %s, "
                "stock_quantity = %s, is_on_special = %s, active_promotion_id = %s, "
                "scraped_at = %s::timestamptz, "
                "price_updated_at = CASE WHEN %s THEN %s::timestamptz ELSE price_updated_at END, "
                "stock_updated_at = CASE WHEN %s THEN %s::timestamptz ELSE stock_updated_at END, "
                "last_seen_at = now(), updated_at = now() WHERE id = %s",
                (canonical_id, variant_id, rec["current_price_cents"],
                 rec["comparison_price_cents"], rec["unit_price_cents"], rec["unit_label"],
                 rec["stock_status"], rec["stock_quantity"], is_special, promo_id,
                 rec["observed_at"], price_changed, rec["observed_at"],
                 stock_changed, rec["observed_at"], bpc_id))
            self.updated += 1
            if price_changed or unit_changed or stock_changed:
                self._record_history(
                    bpc_id, rp_id, canonical_id, variant_id, promo_id, rec,
                    old_price, old_unit, old_stock)
                if price_changed:
                    self.price_changes += 1
        else:
            row = self._one(
                "INSERT INTO catalog.branch_product_current "
                "(branch_id, retailer_product_id, canonical_product_id, product_variant_id, "
                " current_price_cents, comparison_price_cents, unit_price_cents, unit_label, "
                " currency_code, stock_status, stock_quantity, is_on_special, active_promotion_id, "
                " scraped_at, price_updated_at, stock_updated_at, first_seen_at, last_seen_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'NZD',%s,%s,%s,%s,"
                "%s::timestamptz,%s::timestamptz,%s::timestamptz,now(),now()) RETURNING id",
                (self.branch_id, rp_id, canonical_id, variant_id, rec["current_price_cents"],
                 rec["comparison_price_cents"], rec["unit_price_cents"], rec["unit_label"],
                 rec["stock_status"], rec["stock_quantity"], is_special, promo_id,
                 rec["observed_at"], rec["observed_at"], rec["observed_at"]))
            bpc_id = row[0]
            self.inserted += 1
            # Baseline history row on first sighting (Guide: "baseline plus changes").
            self._record_history(
                bpc_id, rp_id, canonical_id, variant_id, promo_id, rec,
                None, None, None)

        if promo_id:
            self._upsert_promotion_item(promo_id, rp_id, bpc_id, rec)

    def _record_history(self, bpc_id, rp_id, canonical_id, variant_id, promo_id, rec,
                        old_price, old_unit, old_stock):
        self.cur.execute(
            "INSERT INTO catalog.price_history "
            "(branch_product_id, branch_id, retailer_product_id, canonical_product_id, "
            " product_variant_id, old_price_cents, new_price_cents, old_unit_price_cents, "
            " new_unit_price_cents, old_stock_status, new_stock_status, promotion_id, "
            " source_run_id, changed_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::timestamptz)",
            (bpc_id, self.branch_id, rp_id, canonical_id, variant_id, old_price,
             rec["current_price_cents"], old_unit, rec["unit_price_cents"], old_stock,
             rec["stock_status"], promo_id, self.run_id, rec["observed_at"]))

    def _upsert_promotion(self, rec, rp_id, loyalty_id):
        existing = self._one(
            "SELECT id FROM catalog.promotions WHERE retailer_id = %s AND branch_id = %s "
            "AND promo_type = %s AND raw_promo_text IS NOT DISTINCT FROM %s "
            "AND is_active = true LIMIT 1",
            (self.retailer_id, self.branch_id, rec["promo_type"], rec["promo_text"]))
        if existing:
            promo_id = existing[0]
            self.cur.execute(
                "UPDATE catalog.promotions SET card_required = %s, "
                "required_loyalty_program_id = COALESCE(%s, required_loyalty_program_id), "
                "is_active = true, updated_at = now() WHERE id = %s",
                (rec["card_required"], loyalty_id, promo_id))
            return promo_id
        row = self._one(
            "INSERT INTO catalog.promotions "
            "(retailer_id, branch_id, promo_type, title, raw_promo_text, card_required, "
            " source, required_loyalty_program_id, is_active, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,true,now(),now()) RETURNING id",
            (self.retailer_id, self.branch_id, rec["promo_type"], rec["promo_text"],
             rec["promo_text"], rec["card_required"], self.source_system, loyalty_id))
        return row[0]

    def _upsert_promotion_item(self, promo_id, rp_id, bpc_id, rec):
        original = rec["comparison_price_cents"]
        special = rec["current_price_cents"]
        discount = None
        if original and original > 0 and special is not None and special <= original:
            discount = round((original - special) / original * 100)
        # is_best_deal is a GENERATED column in pico-prod (discount_percent >= 60);
        # the database computes it, so we must not write it.
        mb_qty = rec["raw_row"].get("multibuy_quantity")
        mb_total = to_cents(rec["raw_row"].get("multibuy_total_cents")
                            or rec["raw_row"].get("multibuy_price_cents"))
        existing = self._one(
            "SELECT id FROM catalog.promotion_items "
            "WHERE promotion_id = %s AND retailer_product_id = %s", (promo_id, rp_id))
        if existing:
            self.cur.execute(
                "UPDATE catalog.promotion_items SET branch_product_id = %s, "
                "original_price_cents = %s, special_price_cents = %s, discount_percent = %s, "
                "multibuy_quantity = %s, multibuy_price_cents = %s, unit_price_cents = %s "
                "WHERE id = %s",
                (bpc_id, original, special, discount, mb_qty, mb_total,
                 rec["unit_price_cents"], existing[0]))
        else:
            self.cur.execute(
                "INSERT INTO catalog.promotion_items "
                "(promotion_id, retailer_product_id, branch_product_id, original_price_cents, "
                " special_price_cents, discount_percent, multibuy_quantity, multibuy_price_cents, "
                " unit_price_cents) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (promo_id, rp_id, bpc_id, original, special, discount, mb_qty, mb_total,
                 rec["unit_price_cents"]))

    # ---- full-branch sweep ------------------------------------------------- #
    def sweep_unseen(self):
        """Mark branch products not seen in this run as out_of_stock and deactivate
        their stale specials. Only call after a verified complete scrape."""
        seen = list(self._seen_retailer_product_ids)
        self.cur.execute(
            "UPDATE catalog.branch_product_current SET stock_status = 'out_of_stock', "
            "is_on_special = false, active_promotion_id = NULL, stock_updated_at = now(), "
            "updated_at = now() "
            "WHERE branch_id = %s AND stock_status <> 'out_of_stock' "
            "AND NOT (retailer_product_id = ANY(%s))",
            (self.branch_id, seen))
        swept = self.cur.rowcount
        self.cur.execute(
            "UPDATE catalog.promotions SET is_active = false, updated_at = now() "
            "WHERE branch_id = %s AND is_active = true AND id NOT IN ("
            "  SELECT active_promotion_id FROM catalog.branch_product_current "
            "  WHERE branch_id = %s AND active_promotion_id IS NOT NULL)",
            (self.branch_id, self.branch_id))
        log.info("full-branch sweep: %d products marked out_of_stock", swept)


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def validate_all(records: list[dict]):
    """Normalize + validate every row. Returns (good, failures)."""
    good, failures = [], []
    for i, rec in enumerate(records):
        try:
            good.append(normalize(rec))
        except ValidationError as exc:
            failures.append((i, str(exc), rec.get("raw_name") or rec.get("name")))
    return good, failures


def run_dry(records: list[dict]) -> int:
    good, failures = validate_all(records)
    deduped, dupes = dedupe(good)
    specials = sum(1 for r in deduped if r["promo_type"])
    members = sum(1 for r in deduped if r["card_required"])
    with_barcode = sum(1 for r in deduped if r["barcode"])

    print("\n=== DRY RUN (parse-only, no database) ===")
    print(f"  rows read          : {len(records)}")
    print(f"  valid              : {len(good)}")
    print(f"  failed validation  : {len(failures)}")
    print(f"  duplicates removed : {dupes}")
    print(f"  unique products    : {len(deduped)}")
    print(f"  with barcode       : {with_barcode} "
          f"({_pct(with_barcode, len(deduped))})")
    print(f"  on special         : {specials}")
    print(f"  member-price rows  : {members}")
    if failures:
        print("\n  first failures:")
        for idx, reason, name in failures[:10]:
            print(f"    row {idx}: {reason} — {name!r}")
    frac = len(failures) / len(records) if records else 0
    print(f"\n  failure rate       : {_pct(len(failures), len(records))} "
          f"(full-branch threshold {MAX_FAILED_FRACTION:.1%})")
    if frac > MAX_FAILED_FRACTION:
        print("  WARNING: failure rate exceeds the full-branch threshold.")
    print("=========================================\n")
    return 0


def run_db(args, records: list[dict], rollback: bool) -> int:
    import psycopg
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log.error("DATABASE_URL is not set (Supabase Postgres pooler). "
                  "Add it to your private .env; it is never printed.")
        return 2

    good, failures = validate_all(records)
    for idx, reason, name in failures:
        log.warning("FAILED row %d: %s — %s", idx, reason, name)
    deduped, dupes = dedupe(good)
    run_type = "full_branch" if args.full_branch else ("rollback_test" if rollback else "incremental")
    log.info("%d valid, %d failed, %d duplicates removed -> %d unique products",
             len(good), len(failures), dupes, len(deduped))

    if args.limit is not None and args.limit < len(deduped):
        deduped = deduped[:args.limit]
        log.info("LIMIT active: importing only the first %d product(s) for testing. "
                 "Out-of-stock sweep is disabled while limited.", len(deduped))

    frac = len(failures) / len(records) if records else 0
    if args.full_branch and frac > MAX_FAILED_FRACTION:
        log.error("ABORT full-branch: %.2f%% rows failed validation (> %.1f%% threshold)",
                  frac * 100, MAX_FAILED_FRACTION * 100)
        return 3

    conn = psycopg.connect(dsn)
    conn.autocommit = False
    # Supabase's transaction pooler (port 6543, pgbouncer transaction mode) does
    # not support server-side prepared statements; disable them there. On the
    # session pooler (5432) / a direct connection the whole import shares one
    # backend, so keeping prepared statements lets the repeated INSERT/UPDATE SQL
    # be parsed once and reused across all rows — a large speedup.
    if ":6543" in dsn:
        conn.prepare_threshold = None
    imp = Importer(conn, args.retailer, args.source_system)
    try:
        imp.resolve(external_store_id=args.external_store_id, branch_slug=args.branch_slug,
                    branch_code=args.branch_code, branch_name=args.branch_name)
        imp.failed = len(failures)
        imp.start_run(run_type, len(records))
        total = len(deduped)
        for i, rec in enumerate(deduped, 1):
            before = (imp.inserted, imp.updated, imp.new_products,
                      imp.reviews_created, imp.price_changes)
            imp.process(rec)
            action = ("INSERT" if imp.inserted > before[0]
                      else "UPDATE" if imp.updated > before[1] else "SEEN")
            tags = []
            if imp.new_products > before[2]:    tags.append("new-listing")
            if imp.reviews_created > before[3]: tags.append("review-queued")
            if imp.price_changes > before[4]:   tags.append("price-changed")
            price = rec["current_price_cents"]
            log.info("[%d/%d] %-6s id=%s $%s %s %s| %s",
                     i, total, action,
                     rec["source_product_id"] or rec["retailer_sku"] or rec["barcode"],
                     f"{price/100:.2f}" if price is not None else "-",
                     rec["stock_status"],
                     ("[" + " ".join(tags) + "] ") if tags else "",
                     rec["raw_name"])
        if args.full_branch and args.limit is None:
            imp.sweep_unseen()
        imp.finish_run("success")

        if rollback:
            conn.rollback()
            log.info("ROLLBACK TEST complete — database path verified, nothing retained.")
        else:
            conn.commit()
            log.info("COMMIT — import successful.")
    except Exception as exc:
        conn.rollback()
        log.exception("import failed; transaction rolled back")
        try:  # best-effort failure record in its own short transaction
            if imp.run_id:
                imp.finish_run("failed", str(exc)[:2000])
                conn.commit()
        except Exception:
            conn.rollback()
        return 1
    finally:
        conn.close()

    print("\n=== IMPORT SUMMARY ===")
    print(f"  mode              : {run_type}{' (rolled back)' if rollback else ''}")
    print(f"  branch products   : inserted {imp.inserted}, updated {imp.updated}")
    print(f"  new products      : {imp.new_products}")
    print(f"  price changes     : {imp.price_changes}")
    print(f"  matched           : {imp.matched}")
    print(f"  canonicals created: {imp.canonicals_created}")
    print(f"  variants created  : {imp.variants_created}")
    print(f"  barcodes linked   : {imp.identifiers_created}")
    print(f"  reviews resolved  : {imp.reviews_resolved}")
    print(f"  review queued     : {imp.reviews_created}")
    print(f"  failed rows       : {imp.failed}")
    print("======================\n")
    return 0


def _pct(n: int, total: int) -> str:
    return f"{(100 * n / total):.1f}%" if total else "0.0%"


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Import a scraped branch export into pico-prod.")
    p.add_argument("--input", required=True, type=Path, help="JSONL/JSON/CSV export file")
    p.add_argument("--retailer", required=True, help="retailer slug, e.g. new-world")
    p.add_argument("--source-system", default="scraper", help="ingest.source_systems.name")
    # branch resolution (preferred order: external store id, slug, code, name)
    p.add_argument("--external-store-id")
    p.add_argument("--branch-slug")
    p.add_argument("--branch-code")
    p.add_argument("--branch-name")
    # modes
    p.add_argument("--dry-run", action="store_true", help="parse-only validation, no DB")
    p.add_argument("--rollback-test", action="store_true", help="full DB path then ROLLBACK")
    p.add_argument("--full-branch", action="store_true",
                   help="real import + mark unseen products out_of_stock")
    p.add_argument("--limit", type=int, default=None,
                   help="import only the first N unique products (testing). "
                        "Disables the out-of-stock sweep so it won't touch other rows.")
    return p.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%H:%M:%S")
    args = parse_args(argv)
    if not args.input.exists():
        log.error("input file not found: %s", args.input)
        return 2
    if not (args.external_store_id or args.branch_slug or args.branch_code or args.branch_name):
        if not args.dry_run:
            log.error("a branch selector is required "
                      "(--external-store-id / --branch-slug / --branch-code / --branch-name)")
            return 2

    try:
        records = read_records(args.input)
    except ValueError as exc:
        log.error("%s", exc)
        return 2
    log.info("read %d rows from %s", len(records), args.input.name)

    if args.dry_run:
        return run_dry(records)
    return run_db(args, records, rollback=args.rollback_test)


if __name__ == "__main__":
    sys.exit(main())
