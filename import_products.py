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

  --rollback-test  Run the FULL import inside transactions, then ROLLBACK every
                   batch. Exercises the whole database path and leaves nothing
                   behind.

  (real import)    Run the full import, committing after every batch (see
                   --batch-size). Add --full-branch to also mark products not
                   seen in this run as out_of_stock and deactivate their stale
                   specials (only safe after a complete scrape).

Connection: reads DATABASE_URL (Supabase Postgres pooler) from the environment /
.env. The URL is a secret and is never printed or written to output.

Batching: measured round-trip latency to pico-prod is ~310ms per SQL statement
(pure network RTT, not query time). A naive one-row-at-a-time importer needs
~15-19 round trips per product (read-then-decide-then-write for retailer_products,
matching, promotions, branch_product_current, price_history) -> ~5s/product ->
hours for a full branch. This importer processes records in batches
(--batch-size, default 2000 — measured best tradeoff: 500->1500 is ~25% faster,
1500->2800 only ~5% more, and Postgres/psycopg's 65,535-bind-param-per-statement
limit caps this at ~2849 given the widest bulk statement here has 23 columns):
a handful of *bulk* multi-row SQL statements per batch (each still ~310ms, but
covering thousands of products at once) replace
thousands of tiny round trips. Each batch commits independently, so a crash
loses at most one batch, not the whole run.

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
import random
import re
import sys
import time
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

# File logging — one file PER RUN (not shared across branches/retailers), named
# logs/import_<input export filename stem>_<run timestamp>.log, set up in main()
# once args are parsed so the file name can include the branch being imported.
_log_dir = Path(__file__).parent / "logs"
_log_dir.mkdir(exist_ok=True)


def _setup_file_logging(label: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_file = _log_dir / f"import_{label}_{stamp}.log"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    logging.getLogger().addHandler(fh)
    log.info(f"logging to {log_file}")


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

DEFAULT_BATCH_SIZE = 2000

# Concurrency safety (only engaged when >1 branch imports run at once):
#  - CANON_CREATE_LOCK: a fixed pg_advisory_xact_lock key taken at the start of
#    the matching stage ONLY when a batch has products still needing a match
#    (i.e. new/first-time products). It serializes canonical-product creation
#    across concurrent importers so two of them can't both "not find, then
#    create" the same canonical -> duplicate. Held to commit, so a waiter always
#    sees the winner's committed canonical and reuses it. Re-imports (nothing to
#    match) skip it entirely -> zero contention on the common path.
#  - MAX_DEADLOCK_RETRIES: concurrent importers of the SAME retailer write the
#    shared catalog.retailer_products rows and can deadlock; a deadlock aborts
#    the batch transaction atomically, so we just retry the whole batch.
CANON_CREATE_LOCK = 0x1CE5_CA7A_10B  # arbitrary constant, fits in bigint
MAX_BATCH_RETRIES = 10
# Bound the wait for the creation advisory lock so a contended waiter fails fast
# with a clean, retryable LockNotAvailable instead of blocking until Supabase's
# 2-minute statement_timeout fires (which cancels the statement AND drops the
# connection — the cause of the concurrent-run branch failures).
CREATE_LOCK_TIMEOUT = "8s"

# --retailer slug (DB, catalog.retailers.slug) -> export filename prefix
# ({scraper_name} passed to jsonl_export.write_jsonl — confirmed straight from
# the scraper source, not guessed: foodstuffs_claude.py uses args.chain
# ("newworld"/"paknsave"), woolworths_claude.py hardcodes "woolworths"). Only
# new-world differs from its retailer slug (the hyphen).
RETAILER_FILE_PREFIX = {
    "new-world": "newworld",
    "paknsave": "paknsave",
    "pak-n-save": "paknsave",
    "woolworths": "woolworths",
}

# Matches jsonl_export.export_filename's `{prefix}_{branch}_{UTC timestamp}.jsonl`.
_EXPORT_FILENAME_RE = re.compile(r"^(.+)_(\d{8}T\d{6}Z)\.jsonl$")


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


def _coerce_int(value: Any) -> Optional[int]:
    """None-safe int coercion (empty string / bad value -> None)."""
    if value in ("", None):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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

    # Structured promo fields emitted by the scrapers (2026-07). multibuy_* and
    # *_price_cents already arrive in cents; promo_metadata is a badge/audit dict.
    source_promotion_id = _clean_str(rec.get("source_promotion_id"))
    promo_starts_at = _clean_str(rec.get("promo_starts_at"))
    promo_ends_at = _clean_str(rec.get("promo_ends_at"))
    multibuy_quantity = _coerce_int(rec.get("multibuy_quantity"))
    multibuy_price_cents = _coerce_int(rec.get("multibuy_price_cents"))
    promo_metadata = rec.get("promo_metadata")
    if not isinstance(promo_metadata, dict):
        promo_metadata = {}

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
        "source_promotion_id": source_promotion_id,
        "promo_starts_at": promo_starts_at,
        "promo_ends_at": promo_ends_at,
        "multibuy_quantity": multibuy_quantity,
        "multibuy_price_cents": multibuy_price_cents,
        "promo_metadata": promo_metadata,
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
            log.debug("DUPLICATE skipped: id=%s — %s", k, r.get("raw_name") or r.get("name"))
            if completeness(r) > completeness(best[k]):
                best[k] = r
        else:
            best[k] = r
    return list(best.values()), dupes


def chunked(seq: list, size: int) -> Iterable[list]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# --------------------------------------------------------------------------- #
# Database importer
# --------------------------------------------------------------------------- #
class Importer:
    """Owns the import transaction(s) and all pico-prod writes.

    Processes records in batches. Measured round-trip latency to pico-prod is
    ~310ms per SQL statement — pure network RTT, not query execution time (a
    trivial `SELECT 1` on an already-open connection costs the same). That means
    the only lever that matters is the *number* of round trips, not query
    tuning. Each per-record method in the original design (SELECT-then-decide,
    one row at a time) cost ~15-19 round trips per product. Here, every method
    takes the whole batch and does a small, fixed number of *bulk* multi-row
    SQL statements — each one still ~310ms, but covering hundreds of products
    at once. See OPERATIONS.md / conversation history for the measurement.
    """

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
        # Run-level caches: these keys repeat heavily across a whole branch
        # (the same brand / promo text appears on hundreds of products), so
        # they're resolved once and reused for the rest of the run instead of
        # being re-looked-up every batch.
        self._brand_cache: dict[str, Optional[str]] = {}   # normalized_name -> brand_id
        self._promo_cache: dict[tuple, str] = {}            # (promo_type, text) -> promotion_id
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

    def _all(self, sql: str, params: tuple = ()) -> list[tuple]:
        self.cur.execute(sql, params)
        return self.cur.fetchall()

    # Stat counters mutated during a batch — snapshotted so a rolled-back /
    # retried batch doesn't double-count. (`failed` is set once before the loop
    # and never touched inside a batch, so it's intentionally excluded.)
    _BATCH_COUNTERS = ("inserted", "updated", "price_changes", "new_products",
                       "reviews_created", "reviews_resolved", "matched",
                       "canonicals_created", "variants_created", "identifiers_created")

    def _counter_snapshot(self) -> dict:
        return {k: getattr(self, k) for k in self._BATCH_COUNTERS}

    def _restore_counters(self, snap: dict) -> None:
        for k, v in snap.items():
            setattr(self, k, v)

    def reconnect(self, dsn: str) -> None:
        """Rebuild a dead connection (e.g. the pooler dropped us after a timeout).
        Run-level caches hold only committed ids, so they stay valid across a
        reconnect — only the live conn/cursor need replacing."""
        import psycopg
        try:
            self.conn.close()
        except Exception:
            pass
        conn = psycopg.connect(dsn)
        conn.autocommit = False
        if ":6543" in dsn:
            conn.prepare_threshold = None
        self.conn = conn
        self.cur = conn.cursor()

    # ---- bulk SQL helpers --------------------------------------------------- #
    @staticmethod
    def _values_sql(rows: list[tuple], types: tuple[str, ...]) -> tuple[str, list]:
        """Build a `(VALUES ...)` fragment for a multi-row statement. Explicit
        `::type` casts go on row 0 only — Postgres infers every other row's
        column types from that, which avoids "could not determine data type"
        errors when a column happens to be NULL in every row of a batch."""
        placeholders = []
        for i, row in enumerate(rows):
            ph = [f"%s::{t}" for t in types] if i == 0 else ["%s"] * len(types)
            placeholders.append("(" + ",".join(ph) + ")")
        params = [v for row in rows for v in row]
        return ",".join(placeholders), params

    def _bulk_insert(self, table: str, cols: tuple, types: tuple, rows: list[tuple],
                      returning: Optional[str] = None):
        if not rows:
            return [] if returning else None
        values_sql, params = self._values_sql(rows, types)
        sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES {values_sql}"
        if returning:
            sql += f" RETURNING {returning}"
        self.cur.execute(sql, params)
        return [r[0] for r in self.cur.fetchall()] if returning else None

    def _bulk_update(self, table: str, key_col: str, key_type: str,
                      set_cols: tuple, set_types: tuple, rows: list[tuple],
                      coalesce: bool = False, extra_set: str = "") -> int:
        """rows: (key_value, *set_values) tuples aligned with set_cols."""
        if not rows:
            return 0
        all_cols = (key_col,) + set_cols
        all_types = (key_type,) + set_types
        values_sql, params = self._values_sql(rows, all_types)
        if coalesce:
            set_clause = ", ".join(f"{c} = COALESCE(v.{c}, t.{c})" for c in set_cols)
        else:
            set_clause = ", ".join(f"{c} = v.{c}" for c in set_cols)
        if extra_set:
            set_clause = f"{set_clause}, {extra_set}" if set_clause else extra_set
        sql = (f"UPDATE {table} AS t SET {set_clause} "
               f"FROM (VALUES {values_sql}) AS v({','.join(all_cols)}) "
               f"WHERE t.{key_col} = v.{key_col}")
        self.cur.execute(sql, params)
        return self.cur.rowcount

    # ---- COPY-based bulk helpers (fast path for big INSERT/UPDATE) --------- #
    # Profiling a 14.5k-product branch showed ~64% of import time was the server
    # parsing giant multi-row `VALUES (...),(...)` literals for the observations
    # INSERT and the retailer_products / branch_product_current UPDATEs. COPY
    # streams the rows instead (no VALUES literal to parse) — same tables, same
    # rows, just a faster door in. Text-format COPY: each Python value is rendered
    # to text by its normal dumper (str/int/Decimal/bool as text, None -> NULL,
    # Jsonb -> json text) and Postgres applies the destination column's input
    # function — so enum columns take their label string, timestamptz/uuid take
    # their text form, etc. No `set_types` needed. `write_row` does COPY escaping.
    def _copy_insert(self, table: str, cols: tuple, rows: list[tuple]) -> None:
        """Stream rows straight into `table` via COPY (append-only; no RETURNING)."""
        if not rows:
            return
        with self.cur.copy(f"COPY {table} ({','.join(cols)}) FROM STDIN") as cp:
            for row in rows:
                cp.write_row(row)

    def _copy_update(self, target: str, temp: str, temp_ddl: str,
                     cols: tuple, rows: list[tuple], set_sql: str,
                     key: str = "id") -> int:
        """COPY `rows` into an ephemeral TEMP staging table, then
        `UPDATE target FROM staging`. The TEMP table is ON COMMIT DROP — pure
        scratch tied to this transaction; it never touches real catalog data and
        is auto-discarded at commit/rollback (no DELETE/DROP issued here).
        `set_sql` references staging as `v` and the target as `t`."""
        if not rows:
            return 0
        self.cur.execute(f"CREATE TEMP TABLE {temp} ({temp_ddl}) ON COMMIT DROP")
        with self.cur.copy(f"COPY {temp} ({','.join(cols)}) FROM STDIN") as cp:
            for row in rows:
                cp.write_row(row)
        self.cur.execute(
            f"UPDATE {target} AS t SET {set_sql} FROM {temp} AS v WHERE t.{key} = v.{key}")
        return self.cur.rowcount

    # ---- resolution (small, one-off — not batch-sensitive) ----------------- #
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

    # ---- run lifecycle ------------------------------------------------------ #
    def start_run(self, run_type: str, total: int):
        row = self._one(
            "INSERT INTO ingest.import_runs "
            "(source_system_id, retailer_id, branch_id, run_type, status, "
            " started_at, total_observations) "
            "VALUES (%s, %s, %s, %s, 'running', now(), %s) RETURNING id",
            (self.source_system_id, self.retailer_id, self.branch_id, run_type, total))
        self.run_id = row[0]
        self.conn.commit()

    def finish_run(self, status: str, error_log: Optional[str] = None):
        self.cur.execute(
            "UPDATE ingest.import_runs SET status = %s, finished_at = now(), "
            "records_inserted = %s, records_updated = %s, records_failed = %s, "
            "price_changes = %s, new_products = %s, error_log = %s WHERE id = %s",
            (status, self.inserted, self.updated, self.failed, self.price_changes,
             self.new_products, error_log, self.run_id))

    # ===================================================================== #
    # BATCH PROCESSING — replaces the old one-record-at-a-time process()
    # ===================================================================== #
    def process_batch(self, batch: list[dict], commit: bool = True) -> None:
        """Process one batch with a small, fixed number of bulk round trips.
        Commits by default; pass commit=False (rollback-test) to leave the
        transaction open so the caller can roll it back instead — this method
        must NOT commit unconditionally, or --rollback-test silently persists
        real data (caught by testing: see git history / conversation log).
        `batch` records are mutated in place with a few derived `_*` scratch
        fields used only within this method."""
        from psycopg.types.json import Jsonb
        # Snapshot the run-level caches AND stat counters up front. Any path that
        # rolls this transaction back (rollback-test OR a mid-batch exception such
        # as a concurrency DeadlockDetected that the caller will retry) must undo
        # them too: the INSERT...RETURNING ids they hold would otherwise dangle
        # (point at rolled-back rows) and corrupt matching / inflate counts on the
        # next attempt. (Concurrency-safe retry relies on this — see run_db.)
        brand_cache_snapshot = dict(self._brand_cache)
        promo_cache_snapshot = dict(self._promo_cache)
        counter_snapshot = self._counter_snapshot()
        try:
            for rec in batch:
                _, _, _, _, _, rec["_norm_size"] = parse_size(rec.get("size"))
                rec["_cname"] = canonical_name(rec)
                # A loyalty program id must never be set without card_required=True
                # (DB CHECK constraint promotions_loyalty_program_requires_card_check).
                # Gate on card_required FIRST — a source row that sets
                # required_loyalty_program_code without also setting card_required
                # (a data inconsistency upstream) must not leak a loyalty id through.
                rec["_loyalty_id"] = self._loyalty_id(
                    rec["required_loyalty_program_code"] or DEFAULT_LOYALTY_BY_RETAILER.get(self.retailer_slug)
                ) if rec["card_required"] else None
                rec["_normalized_promo"] = rec["promo_type"] if rec["promo_type"] in ALLOWED_PROMO_TYPES else None

            self._bulk_insert_observations(batch, Jsonb)

            rp_ids, canonical_ids, variant_ids, is_new = self._bulk_upsert_retailer_products(batch, Jsonb)

            self._bulk_match(batch, rp_ids, canonical_ids, variant_ids)

            for is_n in is_new:
                if is_n:
                    self.new_products += 1

            promo_ids = self._bulk_upsert_promotions(batch)

            self._bulk_upsert_current(batch, rp_ids, canonical_ids, variant_ids, promo_ids)
        except Exception:
            # Abort atomically and reset every side effect so the batch can be
            # retried from a clean slate (or so an aborted rollback-test batch
            # doesn't corrupt the next one). The rollback is best-effort: if the
            # connection itself died (pooler drop), the server already aborted the
            # tx — swallow that so the ORIGINAL error (deadlock/timeout/conn-lost)
            # is what propagates to the retry loop, not a secondary rollback error.
            try:
                self.conn.rollback()
            except Exception:
                pass
            self._brand_cache = brand_cache_snapshot
            self._promo_cache = promo_cache_snapshot
            self._restore_counters(counter_snapshot)
            raise

        if commit:
            self.conn.commit()
        else:
            # rollback-test: undo the DB writes and the cache additions, but KEEP
            # the stat counters so the summary still reports what WOULD have been
            # written (that's the point of the test).
            self.conn.rollback()
            self._brand_cache = brand_cache_snapshot
            self._promo_cache = promo_cache_snapshot
        for rp_id in rp_ids:
            self._seen_retailer_product_ids.add(rp_id)

    # ---- stage: scraped_observations (always insert) ----------------------- #
    def _bulk_insert_observations(self, batch: list[dict], Jsonb) -> None:
        cols = ("import_run_id", "retailer_id", "branch_id", "source_product_id",
                "retailer_sku", "barcode", "raw_name", "raw_brand", "raw_category_path",
                "product_url", "image_url", "price_cents", "comparison_price_cents",
                "unit_price_cents", "unit_label", "stock_status", "stock_quantity",
                "raw_promo_text", "normalized_promo_type", "observed_at", "raw_row",
                "card_required", "required_loyalty_program_id")
        types = ("uuid", "uuid", "uuid", "text", "text", "text", "text", "text", "text",
                 "text", "text", "integer", "integer", "integer", "text", "catalog.stock_status",
                 "integer", "text", "catalog.promo_type", "timestamptz", "jsonb", "boolean", "uuid")
        rows = [(
            self.run_id, self.retailer_id, self.branch_id, rec["source_product_id"],
            rec["retailer_sku"], rec["barcode"], rec["raw_name"], rec["brand"],
            rec["category_path"], rec["product_url"], rec["image_url"],
            rec["current_price_cents"], rec["comparison_price_cents"],
            rec["unit_price_cents"], rec["unit_label"], rec["stock_status"],
            rec["stock_quantity"], rec["promo_text"], rec["_normalized_promo"],
            rec["observed_at"], Jsonb(rec["raw_row"]), rec["card_required"], rec["_loyalty_id"],
        ) for rec in batch]
        # COPY fast path: this table is append-only (no ON CONFLICT / RETURNING),
        # so it's the safest and biggest single win. `types` above is kept as an
        # inline reference to the column types (COPY infers them from the table).
        self._copy_insert("ingest.scraped_observations", cols, rows)

    # ---- stage: retailer_products (bulk resolve existing + bulk write) ----- #
    def _bulk_upsert_retailer_products(self, batch: list[dict], Jsonb):
        n = len(batch)
        spids = [rec["source_product_id"] for rec in batch if rec["source_product_id"]]
        skus_no_spid = [rec["retailer_sku"] for rec in batch
                         if not rec["source_product_id"] and rec["retailer_sku"]]

        existing: dict[tuple, tuple] = {}  # (kind, key) -> (id, canonical_id, variant_id)
        if spids or skus_no_spid:
            rows = self._all(
                "SELECT id, canonical_product_id, product_variant_id, source_product_id, retailer_sku "
                "FROM catalog.retailer_products WHERE retailer_id = %s "
                "AND (source_product_id = ANY(%s) "
                "     OR (source_product_id IS NULL AND retailer_sku = ANY(%s)))",
                (self.retailer_id, spids or [], skus_no_spid or []))
            for rp_id, cid, vid, spid, sku in rows:
                key = ("spid", spid) if spid else ("sku", sku)
                existing[key] = (rp_id, cid, vid)

        def ident(rec):
            return ("spid", rec["source_product_id"]) if rec["source_product_id"] \
                else ("sku", rec["retailer_sku"])

        rp_ids: list[Optional[str]] = [None] * n
        canonical_ids: list[Optional[str]] = [None] * n
        variant_ids: list[Optional[str]] = [None] * n
        is_new: list[bool] = [False] * n

        update_rows = []
        new_idxs = []
        for i, rec in enumerate(batch):
            hit = existing.get(ident(rec))
            if hit:
                rp_ids[i], canonical_ids[i], variant_ids[i] = hit
                update_rows.append((
                    rp_ids[i], rec["retailer_sku"], rec["raw_name"], rec["clean_name"],
                    rec["brand"], rec["category_path"], rec["product_url"], rec["image_url"],
                    rec["barcode"], Jsonb(rec["raw_row"] or {}),
                ))
            else:
                new_idxs.append(i)

        if update_rows:
            # raw_latest: NULLIF('{}') keeps an existing populated card when this
            # export's card is empty (a stale/failed detail fetch must never wipe
            # a populated one) — mirrors the original per-row COALESCE(NULLIF(...)).
            # COPY-into-staging fast path (was a multi-row VALUES UPDATE, ~29% of
            # import time from parsing the giant literal).
            self._copy_update(
                "catalog.retailer_products", "_stg_rp",
                "id uuid, retailer_sku text, raw_name text, clean_name text, "
                "brand_text text, raw_category_path text, product_url text, "
                "image_url text, barcode text, raw_latest jsonb",
                ("id", "retailer_sku", "raw_name", "clean_name", "brand_text",
                 "raw_category_path", "product_url", "image_url", "barcode", "raw_latest"),
                update_rows,
                "retailer_sku = COALESCE(v.retailer_sku, t.retailer_sku), "
                "raw_name = v.raw_name, "
                "clean_name = COALESCE(v.clean_name, t.clean_name), "
                "brand_text = COALESCE(v.brand_text, t.brand_text), "
                "raw_category_path = COALESCE(v.raw_category_path, t.raw_category_path), "
                "product_url = COALESCE(v.product_url, t.product_url), "
                "image_url = COALESCE(v.image_url, t.image_url), "
                "barcode = COALESCE(v.barcode, t.barcode), "
                "raw_latest = COALESCE(NULLIF(v.raw_latest, '{}'::jsonb), t.raw_latest), "
                "last_seen_at = now(), is_active = true, updated_at = now()")

        if new_idxs:
            # match_status defaults to 'unreviewed' at the DB level, but this
            # importer's convention (matched by _bulk_match below) is 'unmatched'
            # for anything not yet resolved — must be set explicitly, NOT left
            # to the column default. is_active/first_seen_at/last_seen_at DO
            # have matching defaults (checked against the live schema), so they
            # don't need to be in the INSERT.
            new_rows = [(
                self.retailer_id, batch[i]["retailer_sku"], batch[i]["raw_name"],
                batch[i]["clean_name"], batch[i]["brand"], batch[i]["category_path"],
                batch[i]["product_url"], batch[i]["image_url"], batch[i]["barcode"],
                batch[i]["source_product_id"], Jsonb(batch[i]["raw_row"] or {}), "unmatched",
            ) for i in new_idxs]
            new_ids = self._bulk_insert(
                "catalog.retailer_products",
                ("retailer_id", "retailer_sku", "raw_name", "clean_name", "brand_text",
                 "raw_category_path", "product_url", "image_url", "barcode",
                 "source_product_id", "raw_latest", "match_status"),
                ("uuid", "text", "text", "text", "text", "text", "text", "text", "text",
                 "text", "jsonb", "text"),
                new_rows,
                returning="id")
            for pos, i in enumerate(new_idxs):
                rp_ids[i] = new_ids[pos]
                is_new[i] = True

        return rp_ids, canonical_ids, variant_ids, is_new

    # ---- stage: brand resolution (run-level cache) -------------------------- #
    def _resolve_brands(self, brand_texts: list[Optional[str]]) -> list[Optional[str]]:
        """Bulk-resolve brand text -> brand id, using + populating the run-level
        cache. Returns a list aligned with `brand_texts` (None passes through)."""
        out: list[Optional[str]] = [None] * len(brand_texts)
        to_resolve: dict[str, str] = {}  # normalized_name -> original text (first wins)
        for i, text in enumerate(brand_texts):
            b = (text or "").strip()
            if not b:
                continue
            norm = db_normalized_name(b)
            if norm in self._brand_cache:
                out[i] = self._brand_cache[norm]
            else:
                to_resolve.setdefault(norm, b)

        if to_resolve:
            # ON CONFLICT DO UPDATE (a no-op update, mirroring the original
            # per-record _upsert_brand) — NOT a plain INSERT: the run-level
            # cache only tracks brands seen so far *in this run*, so a brand
            # already in the DB from a prior import (e.g. "Pams" on any repeat
            # scrape of an already-imported branch) would otherwise 23505.
            names = list(to_resolve.values())
            values_sql, params = self._values_sql([(n,) for n in names], ("text",))
            self.cur.execute(
                f"INSERT INTO catalog.brands (name) VALUES {values_sql} "
                "ON CONFLICT (normalized_name) DO UPDATE SET name = catalog.brands.name "
                "RETURNING id",
                params)
            ids = [r[0] for r in self.cur.fetchall()]
            for norm, bid in zip(to_resolve.keys(), ids):
                self._brand_cache[norm] = bid

        for i, text in enumerate(brand_texts):
            b = (text or "").strip()
            if b:
                out[i] = self._brand_cache.get(db_normalized_name(b))
        return out

    # ---- stage: matching ----------------------------------------------------- #
    def _bulk_match(self, batch: list[dict], rp_ids: list, canonical_ids: list, variant_ids: list) -> None:
        """Mutates canonical_ids/variant_ids in place for every record that
        needs (re)matching. Faithfully replicates the original per-record
        `_match` priority order: barcode-global -> barcode-elsewhere ->
        brand-new-barcode-create -> exact name+size -> create -> review."""
        n = len(batch)
        needs_match = [i for i in range(n) if canonical_ids[i] is None or variant_ids[i] is None]
        if not needs_match:
            return

        # Concurrency guard: serialize the *creation* of canonical products across
        # any importers running in parallel. Taken here (before the lookups) and
        # held to commit, so a second importer blocks until the first commits, then
        # its own lookups below see that committed canonical and reuse it instead
        # of creating a duplicate. No-op cost for a single importer; skipped
        # entirely by re-imports (they never reach here). See CANON_CREATE_LOCK.
        # lock_timeout bounds the wait: a contended waiter fails with a clean,
        # retryable LockNotAvailable (connection intact) instead of blocking to the
        # 2-min statement_timeout that cancels the statement and drops the conn.
        self.cur.execute(f"SET LOCAL lock_timeout = '{CREATE_LOCK_TIMEOUT}'")
        self.cur.execute("SELECT pg_advisory_xact_lock(%s)", (CANON_CREATE_LOCK,))

        barcode_idxs = [i for i in needs_match if batch[i]["barcode"]]
        nobarcode_idxs = [i for i in needs_match if not batch[i]["barcode"]]

        link_rows = []      # (rp_id, canonical_id, variant_id, match_status, confidence)
        review_rp_updates = []  # (rp_id, confidence)
        review_insert_rows = []  # queue_review rows

        # ---------------- barcode path ---------------- #
        if barcode_idxs:
            barcodes = list({batch[i]["barcode"] for i in barcode_idxs})

            global_map = {}
            for bc, cid, vid, nsize in self._all(
                "SELECT pi.identifier_value, pi.canonical_product_id, pi.product_variant_id, "
                "       pv.normalized_size "
                "FROM catalog.product_identifiers pi "
                "JOIN catalog.product_variants pv ON pv.id = pi.product_variant_id "
                "WHERE pi.identifier_type = 'barcode' AND pi.identifier_value = ANY(%s) "
                "AND pi.retailer_id IS NULL", (barcodes,)
            ):
                global_map[bc] = (cid, vid, nsize)

            remaining = [i for i in barcode_idxs if batch[i]["barcode"] not in global_map]

            retailer_map = {}
            if remaining:
                rem_bcs = list({batch[i]["barcode"] for i in remaining})
                for bc, cid, vid, nsize in self._all(
                    "SELECT rp.barcode, rp.canonical_product_id, rp.product_variant_id, "
                    "       pv.normalized_size "
                    "FROM catalog.retailer_products rp "
                    "JOIN catalog.product_variants pv ON pv.id = rp.product_variant_id "
                    "WHERE rp.barcode = ANY(%s) AND rp.canonical_product_id IS NOT NULL",
                    (rem_bcs,)
                ):
                    retailer_map.setdefault(bc, (cid, vid, nsize))

            remaining2 = [i for i in remaining if batch[i]["barcode"] not in retailer_map]

            # Brand-new barcodes: dedupe within the batch — the first record (file
            # order) creates the canonical+variant, later same-barcode records in
            # this batch reuse it (mirrors what sequential processing would do).
            creator_of: dict[str, int] = {}
            for i in remaining2:
                bc = batch[i]["barcode"]
                creator_of.setdefault(bc, i)

            new_barcode_resolved: dict[str, tuple] = {}  # barcode -> (cid, vid, nsize)
            if creator_of:
                creator_idxs = list(creator_of.values())
                brand_ids = self._resolve_brands([batch[i]["brand"] for i in creator_idxs])

                canon_rows = [(
                    brand_ids[pos], batch[i]["_cname"], batch[i]["image_url"], self.source_system,
                ) for pos, i in enumerate(creator_idxs)]
                new_canon_ids = self._bulk_insert(
                    "catalog.canonical_products",
                    ("brand_id", "name", "image_url", "source", "data_quality_score", "is_active"),
                    ("uuid", "text", "text", "text", "numeric", "boolean"),
                    [(bid, name, img, src, Decimal("0.70"), True)
                     for (bid, name, img, src) in canon_rows],
                    returning="id")
                self.canonicals_created += len(new_canon_ids)

                var_rows = []
                for pos, i in enumerate(creator_idxs):
                    rec = batch[i]
                    sv, su, pq, tv, tu, _ = parse_size(rec.get("size"))
                    var_rows.append((
                        new_canon_ids[pos], rec["raw_name"], rec.get("size"), sv, su, pq,
                        tv, tu, rec["_norm_size"], rec["image_url"], True,
                    ))
                new_var_ids = self._bulk_insert(
                    "catalog.product_variants",
                    ("canonical_product_id", "variant_name", "package_description",
                     "size_value", "size_unit", "package_quantity", "total_size_value",
                     "total_size_unit", "normalized_size", "image_url", "is_active"),
                    ("uuid", "text", "text", "numeric", "text", "numeric", "numeric",
                     "text", "text", "text", "boolean"),
                    var_rows,
                    returning="id")
                self.variants_created += len(new_var_ids)

                id_rows = []
                for pos, (bc, i) in enumerate(creator_of.items()):
                    cid, vid = new_canon_ids[pos], new_var_ids[pos]
                    new_barcode_resolved[bc] = (cid, vid, batch[i]["_norm_size"])
                    id_rows.append((vid, cid, bc, self.source_system))

                # These barcodes are, by construction, absent from `global_map`
                # (that's why they reached this "creator" branch) — so bar a
                # concurrent writer racing us between the two queries, every
                # row here is a genuinely new global identifier.
                values_sql, params = self._values_sql(
                    id_rows, ("uuid", "uuid", "text", "text"))
                self.cur.execute(
                    "INSERT INTO catalog.product_identifiers "
                    "(product_variant_id, canonical_product_id, retailer_id, identifier_type, "
                    " identifier_value, is_primary, source, first_seen_at, last_seen_at) "
                    "SELECT v.product_variant_id, v.canonical_product_id, NULL, 'barcode', "
                    "       v.identifier_value, true, v.source, now(), now() "
                    f"FROM (VALUES {values_sql}) AS v(product_variant_id, canonical_product_id, "
                    "identifier_value, source) "
                    "ON CONFLICT (identifier_type, identifier_value) WHERE retailer_id IS NULL "
                    "AND identifier_type = ANY(ARRAY['barcode','gtin','ean','upc']"
                    "::catalog.product_identifier_type[]) "
                    "DO UPDATE SET product_variant_id = EXCLUDED.product_variant_id, "
                    "canonical_product_id = EXCLUDED.canonical_product_id, last_seen_at = now()",
                    params)
                self.identifiers_created += len(id_rows)

            # resolve every barcode-path record against whichever map it landed in
            for i in barcode_idxs:
                bc = batch[i]["barcode"]
                if bc in global_map:
                    cid, vid, nsize = global_map[bc]
                    conf = Decimal("0.98")
                elif bc in retailer_map:
                    cid, vid, nsize = retailer_map[bc]
                    conf = Decimal("0.95")
                else:
                    cid, vid, nsize = new_barcode_resolved[bc]
                    conf = Decimal("0.90")

                rec_size = batch[i]["_norm_size"]
                if rec_size and nsize and rec_size != nsize:
                    review_rp_updates.append((rp_ids[i], Decimal("0.35")))
                    review_insert_rows.append(self._review_row(batch[i], rp_ids[i], "barcode_conflict", cid, vid, Decimal("0.35")))
                    continue
                canonical_ids[i], variant_ids[i] = cid, vid
                link_rows.append((rp_ids[i], cid, vid, "matched", conf))
                self.matched += 1

        # ---------------- no-barcode path ---------------- #
        if nobarcode_idxs:
            sized = [i for i in nobarcode_idxs if batch[i]["_norm_size"]]
            unsized = [i for i in nobarcode_idxs if not batch[i]["_norm_size"]]

            exact_map = {}
            if sized:
                cnames = [db_normalized_name(batch[i]["_cname"]) for i in sized]
                sizes = [batch[i]["_norm_size"] for i in sized]
                for cname_norm, size, cid, vid in self._all(
                    "SELECT want.cname_norm, want.size, cp.id, pv.id "
                    "FROM catalog.product_variants pv "
                    "JOIN catalog.canonical_products cp ON cp.id = pv.canonical_product_id "
                    "JOIN unnest(%s::text[], %s::text[]) AS want(cname_norm, size) "
                    "  ON cp.normalized_name = want.cname_norm AND pv.normalized_size = want.size",
                    (cnames, sizes)
                ):
                    exact_map[(cname_norm, size)] = (cid, vid)

                create_idxs = []
                for i in sized:
                    key = (db_normalized_name(batch[i]["_cname"]), batch[i]["_norm_size"])
                    if key in exact_map:
                        cid, vid = exact_map[key]
                        canonical_ids[i], variant_ids[i] = cid, vid
                        link_rows.append((rp_ids[i], cid, vid, "matched", Decimal("0.75")))
                        self.matched += 1
                    else:
                        create_idxs.append(i)

                # dedupe brand-new (name,size) creations within this batch too
                creator_of2: dict[tuple, int] = {}
                for i in create_idxs:
                    key = (db_normalized_name(batch[i]["_cname"]), batch[i]["_norm_size"])
                    creator_of2.setdefault(key, i)
                if creator_of2:
                    c_idxs = list(creator_of2.values())
                    brand_ids = self._resolve_brands([batch[i]["brand"] for i in c_idxs])
                    canon_rows = [(brand_ids[pos], batch[i]["_cname"], batch[i]["image_url"], self.source_system)
                                  for pos, i in enumerate(c_idxs)]
                    new_cids = self._bulk_insert(
                        "catalog.canonical_products",
                        ("brand_id", "name", "image_url", "source", "data_quality_score", "is_active"),
                        ("uuid", "text", "text", "text", "numeric", "boolean"),
                        [(b, nm, im, sr, Decimal("0.70"), True) for (b, nm, im, sr) in canon_rows],
                        returning="id")
                    self.canonicals_created += len(new_cids)
                    var_rows = []
                    for pos, i in enumerate(c_idxs):
                        rec = batch[i]
                        sv, su, pq, tv, tu, _ = parse_size(rec.get("size"))
                        var_rows.append((new_cids[pos], rec["raw_name"], rec.get("size"), sv, su,
                                          pq, tv, tu, rec["_norm_size"], rec["image_url"], True))
                    new_vids = self._bulk_insert(
                        "catalog.product_variants",
                        ("canonical_product_id", "variant_name", "package_description",
                         "size_value", "size_unit", "package_quantity", "total_size_value",
                         "total_size_unit", "normalized_size", "image_url", "is_active"),
                        ("uuid", "text", "text", "numeric", "text", "numeric", "numeric",
                         "text", "text", "text", "boolean"),
                        var_rows, returning="id")
                    self.variants_created += len(new_vids)
                    resolved2 = {key: (new_cids[pos], new_vids[pos])
                                 for pos, key in enumerate(creator_of2.keys())}
                    for i in create_idxs:
                        key = (db_normalized_name(batch[i]["_cname"]), batch[i]["_norm_size"])
                        cid, vid = resolved2[key]
                        canonical_ids[i], variant_ids[i] = cid, vid
                        link_rows.append((rp_ids[i], cid, vid, "matched", Decimal("0.70")))
                        self.matched += 1

            for i in unsized:
                review_rp_updates.append((rp_ids[i], Decimal("0.15")))
                review_insert_rows.append(self._review_row(batch[i], rp_ids[i], "no_barcode_no_size", None, None, Decimal("0.15")))

        # ---------------- flush link / review writes ---------------- #
        if link_rows:
            self._bulk_update(
                "catalog.retailer_products", "id", "uuid",
                ("canonical_product_id", "product_variant_id", "match_status", "match_confidence"),
                ("uuid", "uuid", "text", "numeric"),
                [(rp, cid, vid, status, conf) for (rp, cid, vid, status, conf) in link_rows],
                extra_set="updated_at = now()")
            linked_rp_ids = [row[0] for row in link_rows]
            resolved = self._bulk_update(
                "catalog.product_match_reviews", "retailer_product_id", "uuid",
                ("status", "resolved_at", "resolution_notes"),
                ("text", "timestamptz", "text"),
                [(rp, "resolved", datetime.now(timezone.utc).isoformat(),
                  "auto-resolved: matcher found a confident link") for rp in linked_rp_ids])
            self.reviews_resolved += resolved

        if review_rp_updates:
            self._bulk_update(
                "catalog.retailer_products", "id", "uuid",
                ("match_status", "match_confidence"), ("text", "numeric"),
                [(rp, "needs_review", conf) for (rp, conf) in review_rp_updates],
                extra_set="updated_at = now()")

        if review_insert_rows:
            values_sql, params = self._values_sql(
                review_insert_rows,
                ("uuid", "uuid", "uuid", "text", "text", "numeric", "jsonb"))
            self.cur.execute(
                "INSERT INTO catalog.product_match_reviews "
                "(retailer_product_id, suggested_canonical_product_id, suggested_product_variant_id, "
                " review_reason, status, score, raw_evidence) "
                f"SELECT v.rp_id, v.scid, v.svid, v.reason, v.status, v.score, v.evidence "
                f"FROM (VALUES {values_sql}) AS v(rp_id, scid, svid, reason, status, score, evidence) "
                "ON CONFLICT (retailer_product_id, review_reason) WHERE status = 'open' DO UPDATE SET "
                "suggested_canonical_product_id = EXCLUDED.suggested_canonical_product_id, "
                "suggested_product_variant_id = EXCLUDED.suggested_product_variant_id, "
                "score = EXCLUDED.score, raw_evidence = EXCLUDED.raw_evidence",
                params)
            self.reviews_created += len(review_insert_rows)

    def _review_row(self, rec, rp_id, reason, cid, vid, confidence):
        from psycopg.types.json import Jsonb
        return (rp_id, cid, vid, reason, "open", confidence, Jsonb({
            "barcode": rec["barcode"], "raw_name": rec["raw_name"],
            "brand": rec["brand"], "size": rec["size"]}))

    # ---- stage: promotions (run-level cache, branch-scoped & bounded) ------ #
    def _bulk_upsert_promotions(self, batch: list[dict]) -> list[Optional[str]]:
        from psycopg.types.json import Jsonb
        n = len(batch)
        promo_ids: list[Optional[str]] = [None] * n
        promo_idxs = [i for i in range(n) if batch[i]["promo_type"]]
        if not promo_idxs:
            return promo_ids

        # Natural key includes source_promotion_id: two products can share the same
        # promo text ("2 for $5.00") yet belong to DIFFERENT retailer promos (distinct
        # promoId) — keying on the id keeps them as separate promotion rows. When the
        # retailer exposes no id (Woolworths), it's NULL and the key falls back to
        # (promo_type, raw_promo_text), matching the previous behaviour.
        def _pkey(i):
            return (batch[i]["promo_type"], batch[i]["promo_text"], batch[i]["source_promotion_id"])
        keys_needed = {_pkey(i) for i in promo_idxs}
        uncached = [k for k in keys_needed if k not in self._promo_cache]
        if uncached:
            types_ = [k[0] for k in uncached]
            texts_ = [k[1] for k in uncached]
            sids_ = [k[2] for k in uncached]
            for pid, ptype, ptext, psid in self._all(
                "SELECT pr.id, want.ptype, want.ptext, want.psid FROM catalog.promotions pr "
                "JOIN unnest(%s::text[], %s::text[], %s::text[]) AS want(ptype, ptext, psid) "
                "  ON pr.promo_type::text = want.ptype "
                "  AND pr.raw_promo_text IS NOT DISTINCT FROM want.ptext "
                "  AND pr.source_promotion_id IS NOT DISTINCT FROM want.psid "
                "WHERE pr.retailer_id = %s AND pr.branch_id = %s AND pr.is_active = true",
                (types_, texts_, sids_, self.retailer_id, self.branch_id)
            ):
                self._promo_cache[(ptype, ptext, psid)] = pid

            still_missing = [k for k in uncached if k not in self._promo_cache]
            # dedupe (should already be unique via the set, but be explicit)
            creator_for: dict[tuple, int] = {}
            for i in promo_idxs:
                k = _pkey(i)
                if k in still_missing:
                    creator_for.setdefault(k, i)
            if creator_for:
                keys = list(creator_for.keys())
                rows = [(
                    self.retailer_id, self.branch_id, k[0], batch[creator_for[k]]["promo_text"],
                    batch[creator_for[k]]["promo_text"], batch[creator_for[k]]["card_required"],
                    self.source_system, batch[creator_for[k]]["_loyalty_id"],
                    k[2], batch[creator_for[k]]["promo_starts_at"],
                    batch[creator_for[k]]["promo_ends_at"],
                    Jsonb(batch[creator_for[k]]["promo_metadata"] or {}),
                ) for k in keys]
                new_ids = self._bulk_insert(
                    "catalog.promotions",
                    ("retailer_id", "branch_id", "promo_type", "title", "raw_promo_text",
                     "card_required", "source", "required_loyalty_program_id",
                     "source_promotion_id", "starts_at", "ends_at", "metadata"),
                    ("uuid", "uuid", "catalog.promo_type", "text", "text", "boolean", "text", "uuid",
                     "text", "timestamptz", "timestamptz", "jsonb"),
                    rows, returning="id")
                for k, pid in zip(keys, new_ids):
                    self._promo_cache[k] = pid

        existing_update_rows = []
        for i in promo_idxs:
            promo_ids[i] = self._promo_cache[_pkey(i)]
        # keep card_required/loyalty program + dates/metadata current on cached promos
        seen_keys = set()
        for i in promo_idxs:
            k = _pkey(i)
            if k in seen_keys:
                continue
            seen_keys.add(k)
            existing_update_rows.append((
                promo_ids[i], batch[i]["card_required"], batch[i]["_loyalty_id"],
                batch[i]["promo_starts_at"], batch[i]["promo_ends_at"],
                Jsonb(batch[i]["promo_metadata"] or {}),
            ))
        if existing_update_rows:
            # card_required is a plain overwrite; required_loyalty_program_id is
            # COALESCE'd (keep the existing value when this batch's is NULL) —
            # BUT ONLY when the incoming row still requires a card. Two products
            # can legitimately share the exact same promo text (e.g. "2 for
            # $4.00") while disagreeing on card_required; a plain COALESCE would
            # let an old loyalty id stick around after card_required flips to
            # false, violating promotions_loyalty_program_requires_card_check
            # (card_required OR required_loyalty_program_id IS NULL). Caught by
            # a real 12,000-row-in run failure — see conversation/commit history.
            values_sql, params = self._values_sql(
                existing_update_rows,
                ("uuid", "boolean", "uuid", "timestamptz", "timestamptz", "jsonb"))
            self.cur.execute(
                "UPDATE catalog.promotions AS t SET "
                "card_required = v.card_required, "
                "required_loyalty_program_id = CASE WHEN v.card_required THEN "
                "  COALESCE(v.required_loyalty_program_id, t.required_loyalty_program_id) "
                "ELSE NULL END, "
                # keep a real date once we have one; don't wipe it with a later NULL
                "starts_at = COALESCE(v.starts_at, t.starts_at), "
                "ends_at = COALESCE(v.ends_at, t.ends_at), "
                # refresh badges only when this run actually carried some
                "metadata = CASE WHEN v.metadata = '{}'::jsonb THEN t.metadata ELSE v.metadata END, "
                "is_active = true, updated_at = now() "
                f"FROM (VALUES {values_sql}) AS v(id, card_required, required_loyalty_program_id, "
                "starts_at, ends_at, metadata) "
                "WHERE t.id = v.id",
                params)
        return promo_ids

    # ---- stage: branch_product_current + price_history + promotion_items -- #
    def _bulk_upsert_current(self, batch: list[dict], rp_ids: list, canonical_ids: list,
                              variant_ids: list, promo_ids: list) -> None:
        n = len(batch)
        existing = {}
        rows = self._all(
            "SELECT retailer_product_id, id, current_price_cents, unit_price_cents, stock_status "
            "FROM catalog.branch_product_current "
            "WHERE branch_id = %s AND retailer_product_id = ANY(%s)",
            (self.branch_id, rp_ids))
        for rp_id, bpc_id, old_price, old_unit, old_stock in rows:
            existing[rp_id] = (bpc_id, old_price, old_unit, old_stock)

        update_rows = []
        insert_idxs = []
        bpc_id_by_idx: list[Optional[str]] = [None] * n
        history_rows = []  # (bpc_id, rp_id, cid, vid, promo_id, rec, old_price, old_unit, old_stock)

        for i, rec in enumerate(batch):
            is_special = promo_ids[i] is not None
            hit = existing.get(rp_ids[i])
            if hit:
                bpc_id, old_price, old_unit, old_stock = hit
                bpc_id_by_idx[i] = bpc_id
                price_changed = old_price != rec["current_price_cents"]
                unit_changed = old_unit != rec["unit_price_cents"]
                stock_changed = old_stock != rec["stock_status"]
                update_rows.append((
                    bpc_id, canonical_ids[i], variant_ids[i], rec["current_price_cents"],
                    rec["comparison_price_cents"], rec["unit_price_cents"], rec["unit_label"],
                    rec["stock_status"], rec["stock_quantity"], is_special, promo_ids[i],
                    rec["observed_at"], price_changed, rec["observed_at"] if price_changed else None,
                    stock_changed, rec["observed_at"] if stock_changed else None,
                ))
                self.updated += 1
                if price_changed or unit_changed or stock_changed:
                    history_rows.append((bpc_id, rp_ids[i], canonical_ids[i], variant_ids[i],
                                          promo_ids[i], rec, old_price, old_unit, old_stock))
                    if price_changed:
                        self.price_changes += 1
            else:
                insert_idxs.append(i)

        if update_rows:
            # COPY-into-staging fast path (was a multi-row VALUES UPDATE). The
            # staging table carries price_changed/stock_changed for row-shape
            # parity with the tuple, though the SET only reads the *_updated_at
            # columns (already computed in Python above).
            self._copy_update(
                "catalog.branch_product_current", "_stg_bpc",
                "id uuid, canonical_product_id uuid, product_variant_id uuid, "
                "current_price_cents integer, comparison_price_cents integer, "
                "unit_price_cents integer, unit_label text, "
                "stock_status catalog.stock_status, stock_quantity integer, "
                "is_on_special boolean, active_promotion_id uuid, scraped_at timestamptz, "
                "price_changed boolean, price_updated_at timestamptz, "
                "stock_changed boolean, stock_updated_at timestamptz",
                ("id", "canonical_product_id", "product_variant_id", "current_price_cents",
                 "comparison_price_cents", "unit_price_cents", "unit_label", "stock_status",
                 "stock_quantity", "is_on_special", "active_promotion_id", "scraped_at",
                 "price_changed", "price_updated_at", "stock_changed", "stock_updated_at"),
                update_rows,
                "canonical_product_id = COALESCE(v.canonical_product_id, t.canonical_product_id), "
                "product_variant_id = COALESCE(v.product_variant_id, t.product_variant_id), "
                "current_price_cents = v.current_price_cents, "
                "comparison_price_cents = v.comparison_price_cents, "
                "unit_price_cents = v.unit_price_cents, unit_label = v.unit_label, "
                "stock_status = v.stock_status, stock_quantity = v.stock_quantity, "
                "is_on_special = v.is_on_special, active_promotion_id = v.active_promotion_id, "
                "scraped_at = v.scraped_at, "
                "price_updated_at = COALESCE(v.price_updated_at, t.price_updated_at), "
                "stock_updated_at = COALESCE(v.stock_updated_at, t.stock_updated_at), "
                "last_seen_at = now(), updated_at = now()")

        if insert_idxs:
            ins_rows = [(
                self.branch_id, rp_ids[i], canonical_ids[i], variant_ids[i],
                batch[i]["current_price_cents"], batch[i]["comparison_price_cents"],
                batch[i]["unit_price_cents"], batch[i]["unit_label"], batch[i]["stock_status"],
                batch[i]["stock_quantity"], promo_ids[i] is not None, promo_ids[i],
                batch[i]["observed_at"], batch[i]["observed_at"], batch[i]["observed_at"],
            ) for i in insert_idxs]
            new_bpc_ids = self._bulk_insert(
                "catalog.branch_product_current",
                ("branch_id", "retailer_product_id", "canonical_product_id", "product_variant_id",
                 "current_price_cents", "comparison_price_cents", "unit_price_cents", "unit_label",
                 "stock_status", "stock_quantity", "is_on_special", "active_promotion_id",
                 "scraped_at", "price_updated_at", "stock_updated_at"),
                ("uuid", "uuid", "uuid", "uuid", "integer", "integer", "integer", "text",
                 "catalog.stock_status", "integer", "boolean", "uuid", "timestamptz",
                 "timestamptz", "timestamptz"),
                ins_rows, returning="id")
            # currency_code/first_seen_at/last_seen_at all have matching column
            # defaults (checked against the live schema) — no follow-up write needed.
            self.inserted += len(new_bpc_ids)
            for pos, i in enumerate(insert_idxs):
                bpc_id_by_idx[i] = new_bpc_ids[pos]
                # baseline history row on first sighting
                history_rows.append((new_bpc_ids[pos], rp_ids[i], canonical_ids[i], variant_ids[i],
                                      promo_ids[i], batch[i], None, None, None))

        if history_rows:
            hist_ins = [(
                bpc_id, self.branch_id, rp_id, cid, vid, old_price, rec["current_price_cents"],
                old_unit, rec["unit_price_cents"], old_stock, rec["stock_status"], promo_id,
                self.run_id, rec["observed_at"],
            ) for (bpc_id, rp_id, cid, vid, promo_id, rec, old_price, old_unit, old_stock) in history_rows]
            self._bulk_insert(
                "catalog.price_history",
                ("branch_product_id", "branch_id", "retailer_product_id", "canonical_product_id",
                 "product_variant_id", "old_price_cents", "new_price_cents", "old_unit_price_cents",
                 "new_unit_price_cents", "old_stock_status", "new_stock_status", "promotion_id",
                 "source_run_id", "changed_at"),
                ("uuid", "uuid", "uuid", "uuid", "uuid", "integer", "integer", "integer",
                 "integer", "catalog.stock_status", "catalog.stock_status", "uuid", "uuid",
                 "timestamptz"),
                hist_ins)

        self._bulk_upsert_promotion_items(batch, rp_ids, bpc_id_by_idx, promo_ids)

    def _bulk_upsert_promotion_items(self, batch, rp_ids, bpc_ids, promo_ids) -> None:
        from psycopg.types.json import Jsonb
        idxs = [i for i in range(len(batch)) if promo_ids[i] is not None]
        if not idxs:
            return
        pairs = [(promo_ids[i], rp_ids[i]) for i in idxs]
        existing = {}
        for pid, rpid, item_id in self._all(
            "SELECT promotion_id, retailer_product_id, id FROM catalog.promotion_items "
            "WHERE (promotion_id, retailer_product_id) IN ("
            "  SELECT * FROM unnest(%s::uuid[], %s::uuid[]))",
            ([p[0] for p in pairs], [p[1] for p in pairs])
        ):
            existing[(pid, rpid)] = item_id

        update_rows, insert_rows = [], []
        for i in idxs:
            rec = batch[i]
            original = rec["comparison_price_cents"]
            special = rec["current_price_cents"]
            discount = None
            if original and original > 0 and special is not None and special <= original:
                discount = round((original - special) / original * 100)
            # NB: promotion_items.is_best_deal is a GENERATED column
            # (discount_percent >= 60) — the DB computes it, so we only populate
            # discount_percent. This keeps Pico's best-deal rule SEPARATE from
            # promo_type so a deep clearance isn't reclassified as half_price.
            # multibuy_* are structured, already-in-cents fields on the record now
            # (they used to be dug out of raw_row, which the scrapers never populated).
            mb_qty = rec["multibuy_quantity"]
            mb_total = rec["multibuy_price_cents"]
            meta = Jsonb(rec["promo_metadata"] or {})
            key = (promo_ids[i], rp_ids[i])
            if key in existing:
                update_rows.append((existing[key], bpc_ids[i], original, special, discount,
                                     mb_qty, mb_total, rec["unit_price_cents"], meta))
            else:
                insert_rows.append((promo_ids[i], rp_ids[i], bpc_ids[i], original, special,
                                     discount, mb_qty, mb_total, rec["unit_price_cents"], meta))

        if update_rows:
            self._bulk_update(
                "catalog.promotion_items", "id", "uuid",
                ("branch_product_id", "original_price_cents", "special_price_cents",
                 "discount_percent", "multibuy_quantity", "multibuy_price_cents",
                 "unit_price_cents", "metadata"),
                ("uuid", "integer", "integer", "integer", "integer", "integer", "integer",
                 "jsonb"),
                update_rows)
        if insert_rows:
            self._bulk_insert(
                "catalog.promotion_items",
                ("promotion_id", "retailer_product_id", "branch_product_id", "original_price_cents",
                 "special_price_cents", "discount_percent", "multibuy_quantity",
                 "multibuy_price_cents", "unit_price_cents", "metadata"),
                ("uuid", "uuid", "uuid", "integer", "integer", "integer", "integer",
                 "integer", "integer", "jsonb"),
                insert_rows)

    # ---- full-branch sweep --------------------------------------------------- #
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
        self.conn.commit()


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

    summary_lines = [
        "=== DRY RUN (parse-only, no database) ===",
        f"  rows read          : {len(records)}",
        f"  valid              : {len(good)}",
        f"  failed validation  : {len(failures)}",
        f"  duplicates removed : {dupes}",
        f"  unique products    : {len(deduped)}",
        f"  with barcode       : {with_barcode} ({_pct(with_barcode, len(deduped))})",
        f"  on special         : {specials}",
        f"  member-price rows  : {members}",
    ]
    if failures:
        summary_lines.append("  first failures:")
        for idx, reason, name in failures[:10]:
            summary_lines.append(f"    row {idx}: {reason} — {name!r}")
    frac = len(failures) / len(records) if records else 0
    summary_lines.append(
        f"  failure rate       : {_pct(len(failures), len(records))} "
        f"(full-branch threshold {MAX_FAILED_FRACTION:.1%})"
    )
    if frac > MAX_FAILED_FRACTION:
        summary_lines.append("  WARNING: failure rate exceeds the full-branch threshold.")
    summary_lines.append("=========================================")
    for line in summary_lines:
        log.info(line)
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
    if ":6543" in dsn:
        conn.prepare_threshold = None
    imp = Importer(conn, args.retailer, args.source_system)
    batch_size = args.batch_size
    total = len(deduped)
    try:
        imp.resolve(external_store_id=args.external_store_id, branch_slug=args.branch_slug,
                    branch_code=args.branch_code, branch_name=args.branch_name)
        imp.failed = len(failures)
        imp.start_run(run_type, len(records))

        done = 0
        label = "rolled back" if rollback else "committed"
        # Transient errors under concurrency (connection stays alive, tx aborted):
        # deadlock on shared rows, lock_timeout on the creation lock, or a
        # statement_timeout cancel. process_batch aborts+resets atomically, so the
        # batch is always safe to re-run from a clean slate.
        TRANSIENT = (psycopg.errors.DeadlockDetected,
                     psycopg.errors.LockNotAvailable,
                     psycopg.errors.QueryCanceled)
        for batch in chunked(deduped, batch_size):
            before = (imp.inserted, imp.updated, imp.new_products, imp.matched)
            for attempt in range(1, MAX_BATCH_RETRIES + 1):
                try:
                    imp.process_batch(batch, commit=not rollback)
                    break
                except TRANSIENT as exc:
                    if attempt == MAX_BATCH_RETRIES:
                        raise
                    # a timeout can also drop the pooled connection — rebuild it
                    # if it didn't survive, so the retry runs on a live conn.
                    if imp.conn.closed:
                        imp.reconnect(dsn)
                    sleep_s = min(10.0, 0.4 * attempt) + random.uniform(0, 0.5)
                    log.warning("batch retryable %s (attempt %d/%d) — retrying in %.1fs",
                                type(exc).__name__, attempt, MAX_BATCH_RETRIES, sleep_s)
                    time.sleep(sleep_s)
                except psycopg.OperationalError as exc:
                    # connection lost outright (pooler dropped us). Rebuild and retry.
                    if attempt == MAX_BATCH_RETRIES:
                        raise
                    log.warning("batch connection lost (%s) (attempt %d/%d) — reconnecting",
                                type(exc).__name__, attempt, MAX_BATCH_RETRIES)
                    imp.reconnect(dsn)
                    time.sleep(min(10.0, 0.5 * attempt) + random.uniform(0, 0.5))
            done += len(batch)
            log.info("[%d/%d] batch %s: +%d inserted, +%d updated, +%d new-listing, +%d matched",
                      done, total, label, imp.inserted - before[0], imp.updated - before[1],
                      imp.new_products - before[2], imp.matched - before[3])

        if args.full_branch and args.limit is None and not rollback:
            imp.sweep_unseen()

        imp.finish_run("success")
        conn.commit()
        if rollback:
            log.info("ROLLBACK TEST complete — database path verified per batch, nothing retained.")
        else:
            log.info("all batches committed — import successful.")
    except Exception as exc:
        conn.rollback()
        log.exception("import failed (batch in progress rolled back; earlier committed "
                      "batches are NOT undone — safe to resume)")
        try:  # best-effort failure record in its own short transaction
            if imp.run_id:
                imp.finish_run("failed", str(exc)[:2000])
                conn.commit()
        except Exception:
            conn.rollback()
        return 1
    finally:
        conn.close()

    summary_lines = [
        "=== IMPORT SUMMARY ===",
        f"  mode              : {run_type}{' (rolled back)' if rollback else ''}",
        f"  branch products   : inserted {imp.inserted}, updated {imp.updated}",
        f"  new products      : {imp.new_products}",
        f"  price changes     : {imp.price_changes}",
        f"  matched           : {imp.matched}",
        f"  canonicals created: {imp.canonicals_created}",
        f"  variants created  : {imp.variants_created}",
        f"  barcodes linked   : {imp.identifiers_created}",
        f"  reviews resolved  : {imp.reviews_resolved}",
        f"  review queued     : {imp.reviews_created}",
        f"  failed rows       : {imp.failed}",
        "======================",
    ]
    for line in summary_lines:
        log.info(line)
    return 0


def _pct(n: int, total: int) -> str:
    return f"{(100 * n / total):.1f}%" if total else "0.0%"


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Import a scraped branch export into pico-prod.")
    p.add_argument("--input", type=Path, help="JSONL/JSON/CSV export file (single branch)")
    p.add_argument("--input-dir", type=Path,
                   help="directory of export files for EVERY branch of --retailer "
                        "(one command for the full chain, instead of --input per branch). "
                        "Files are matched by the scraper's own naming convention "
                        f"({{prefix}}_{{branch-slug}}_{{UTC timestamp}}.jsonl) and the "
                        "branch-slug is parsed straight out of the filename — mutually "
                        "exclusive with --input/--branch-slug/--external-store-id/--branch-code/"
                        "--branch-name, which only make sense for a single branch.")
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
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                   help=f"records per bulk-write batch / commit (default {DEFAULT_BATCH_SIZE}). "
                        "Measured: 500->1500 is ~25%% faster, 1500->2800 only ~5%% more — "
                        "diminishing returns past ~1500-2000. Keep well under ~2849: "
                        "Postgres/psycopg cap statements at 65,535 bind params, and the "
                        "widest bulk statement here (scraped_observations, 23 cols) hits "
                        "that ceiling at batch_size=2849.")
    p.add_argument("--jobs", type=int, default=1,
                   help="how many branches to import in PARALLEL (default 1 = "
                        "sequential; only applies to --input-dir). Each branch runs in "
                        "its own process (real parallelism for the JSON-serialization "
                        "bottleneck) and its own DB connection. The importer is "
                        "concurrency-safe: canonical-product creation is serialized so "
                        "no duplicates, and same-retailer row deadlocks are retried. "
                        "3-4 is a good, DB-friendly starting point.")
    return p.parse_args(argv)


def _run_one_file(args, path: Path, branch_slug: Optional[str]) -> int:
    """Shared by both single-file and --input-dir modes: read + validate-or-import
    one export file for one branch. Mutates args.input/branch_slug for this call."""
    args.input = path
    if branch_slug is not None:
        args.branch_slug = branch_slug
    try:
        records = read_records(path)
    except ValueError as exc:
        log.error("%s", exc)
        return 2
    log.info("read %d rows from %s", len(records), path.name)
    if args.dry_run:
        return run_dry(records)
    return run_db(args, records, rollback=args.rollback_test)


def _worker_import(args, slug: str, path: Path) -> tuple:
    """Process-pool entry point for one branch (must be module-level so it
    pickles). Each worker gets its own copy of `args` (isolated per process), so
    _run_one_file's in-place args mutation is process-local and safe."""
    try:
        rc = _run_one_file(args, path, slug)
        return slug, rc
    except Exception:
        log.exception("branch %s FAILED (unhandled exception)", slug)
        return slug, 1


def _main_input_dir(args) -> int:
    """One command for every branch of a chain: glob the scraper's own export
    files by naming convention, parse the branch-slug out of each filename, and
    import them one at a time — continuing past a single branch's failure
    rather than aborting the whole sweep (each branch is independently
    resumable/re-runnable, so there's no reason a bad branch should block the
    other 147)."""
    if args.dry_run is False and not args.full_branch:
        log.warning("--input-dir without --full-branch: unseen-product sweep will be "
                    "skipped for every branch (usually not what you want for a full chain import)")
    prefix = RETAILER_FILE_PREFIX.get(args.retailer)
    if not prefix:
        log.error("no known export filename prefix for --retailer %r "
                  "(known: %s) — add it to RETAILER_FILE_PREFIX", args.retailer,
                  ", ".join(sorted(set(RETAILER_FILE_PREFIX.values()))))
        return 2
    if not args.input_dir.is_dir():
        log.error("--input-dir not found: %s", args.input_dir)
        return 2

    branches: dict[str, Path] = {}  # branch-slug -> latest file for it
    for path in sorted(args.input_dir.glob(f"{prefix}_*.jsonl")):
        m = _EXPORT_FILENAME_RE.match(path.name)
        if not m:
            log.warning("skipping %s — doesn't match {prefix}_{branch}_{timestamp}.jsonl", path.name)
            continue
        rest = m.group(1)  # "{prefix}_{branch}" with the timestamp already stripped
        if not rest.startswith(prefix + "_"):
            log.warning("skipping %s — prefix mismatch", path.name)
            continue
        slug = rest[len(prefix) + 1:]
        ts = m.group(2)
        # sorted() + dict overwrite keeps the LATEST file per branch, matching
        # jsonl_export's one-file-per-branch-per-run convention.
        if slug not in branches or path.name > branches[slug].name:
            branches[slug] = path
    if not branches:
        log.error("no %s_*.jsonl files found in %s", prefix, args.input_dir)
        return 2

    jobs = max(1, args.jobs)
    log.info("=== full-chain import: %d branches found for retailer=%s (--jobs %d) ===",
             len(branches), args.retailer, jobs)
    ordered = sorted(branches.items())
    ok, failed = [], []

    if jobs == 1:
        for i, (slug, path) in enumerate(ordered, 1):
            log.info("--- branch %d/%d: %s (%s) ---", i, len(branches), slug, path.name)
            try:
                rc = _run_one_file(args, path, slug)
                (ok if rc == 0 else failed).append(slug)
                if rc != 0:
                    log.error("branch %s FAILED (exit %d) — continuing to next branch", slug, rc)
            except Exception:
                log.exception("branch %s FAILED (unhandled exception) — continuing to next branch", slug)
                failed.append(slug)
    else:
        # Parallel: one OS process per branch (true parallelism for the
        # CPU-bound JSON serialization + overlapping DB I/O), each with its own
        # connection. The importer's advisory-lock creation guard + deadlock
        # retry make concurrent same-retailer branches safe. Failures don't
        # abort the sweep — every branch is independently resumable.
        from concurrent.futures import ProcessPoolExecutor, as_completed
        with ProcessPoolExecutor(max_workers=jobs) as ex:
            futs = {ex.submit(_worker_import, args, slug, path): slug
                    for slug, path in ordered}
            for done_n, fut in enumerate(as_completed(futs), 1):
                slug, rc = fut.result()
                (ok if rc == 0 else failed).append(slug)
                log.info("--- branch %d/%d done: %s (exit %d) ---", done_n, len(branches), slug, rc)
                if rc != 0:
                    log.error("branch %s FAILED (exit %d)", slug, rc)

    log.info("=== full-chain import done: %d/%d branches OK ===", len(ok), len(branches))
    if failed:
        log.error("FAILED branches (%d): %s", len(failed), ", ".join(failed))
        log.error("re-run just those with --input <file> --branch-slug <slug> --full-branch")
    return 0 if not failed else 1


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%H:%M:%S")
    args = parse_args(argv)

    if bool(args.input) == bool(args.input_dir):
        log.error("specify exactly one of --input (single branch) or --input-dir (full chain)")
        return 2

    if args.input_dir:
        _setup_file_logging(f"{args.retailer}-all-branches")
        if args.branch_slug or args.external_store_id or args.branch_code or args.branch_name:
            log.error("--branch-slug/--external-store-id/--branch-code/--branch-name are "
                      "single-branch options — not valid with --input-dir")
            return 2
        return _main_input_dir(args)

    _setup_file_logging(args.input.stem)
    if not args.input.exists():
        log.error("input file not found: %s", args.input)
        return 2
    if not (args.external_store_id or args.branch_slug or args.branch_code or args.branch_name):
        if not args.dry_run:
            log.error("a branch selector is required "
                      "(--external-store-id / --branch-slug / --branch-code / --branch-name)")
            return 2
    return _run_one_file(args, args.input, None)


if __name__ == "__main__":
    sys.exit(main())
