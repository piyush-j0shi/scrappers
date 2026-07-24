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

# Shared run-log writer lives next to the scrapers (one logs/ dir + index for
# both scraper and importer runs). It has no heavy deps, so importing it here is
# cheap; if the layout ever changes, run-logging just no-ops rather than breaking
# the import.
_run_log = None
try:
    _scrapers_dir = Path(__file__).resolve().parent / "claude scrapers"
    if _scrapers_dir.is_dir() and str(_scrapers_dir) not in sys.path:
        sys.path.insert(0, str(_scrapers_dir))
    import run_log as _run_log  # noqa: E402
except Exception:  # pragma: no cover - run logging is best-effort
    _run_log = None

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

# Whole-batch retry for TRANSIENT database errors. Kept after parallelism was
# removed because these still happen to a lone importer: the Supabase pooler
# drops a connection, or a statement hits the 2-minute statement_timeout.
# process_batch aborts and resets its side effects atomically, so a batch is
# always safe to re-run from a clean slate.
MAX_BATCH_RETRIES = 10
# PARALLELISM REMOVED 2026-07-20. The importer is single-process by design now.
# What went: --jobs, the ProcessPoolExecutor sweep, _worker_import, and the
# CANON_CREATE_LOCK advisory lock that serialized canonical creation across
# workers. Measured reason: the lock was held from _bulk_match all the way to
# commit — i.e. across nearly the whole batch — so --jobs 4 produced a dead-even
# 13.2s commit cadence (one batch at a time, zero overlap) plus 34 wasted
# whole-batch retries from lock_timeout. Effective parallelism was ~1x with
# strictly negative overhead. One process doing uncontended work is faster.
# An import_run still 'running' after this long has lost its process (finish_run
# is best-effort and never runs on SIGKILL/OOM/connection loss). Comfortably
# longer than the slowest observed single-branch import (~15 min) so a live run
# is never reaped out from under itself.
STALE_RUN_AFTER = "2 hours"
# catalog.product_identifiers.last_seen_at is only bumped once a barcode has gone
# this long without a touch. Barcodes live in the GLOBAL identifier space
# (retailer_id IS NULL), so they are shared by every branch of every chain —
# without this gate a 200-branch sweep would rewrite the same ~15k rows 200 times
# per run (millions of pointless row versions, WAL, and vacuum debt) to record
# information that only needs to be accurate to within a scrape cycle.
IDENTIFIER_TOUCH_AFTER = "12 hours"
# Freshness semantics (these three must not be conflated):
#   last_seen_at      the product was SEEN in this scrape.
#   updated_at        the stored product data actually CHANGED.
#   price_updated_at  the price specifically changed.
# updated_at and price_updated_at are written ONLY on the real-change path
# (set_sql), i.e. only for rows that were going to be rewritten anyway — they
# cost nothing extra and are always correct.
#
# last_seen_at for UNCHANGED rows is a different story: stamping it means a heap
# rewrite of every observed row on every import. Measured on one branch that is
# 79 -> 11,838 writes on retailer_products, and retailer_products is
# retailer-wide, so a 148-branch chain rewrites the same row up to 148 times.
# It is OFF by default (set IMPORTER_TOUCH_UNCHANGED=1 to enable).
#
# Turning it off does NOT weaken out-of-stock detection: sweep_unseen() decides
# what disappeared using the in-memory seen-set from the current run
# (_seen_retailer_product_ids), never last_seen_at timestamps. Products that
# vanish are marked out_of_stock there, and their last_seen_at correctly stays
# old because they genuinely were not seen.
TOUCH_UNCHANGED = os.environ.get("IMPORTER_TOUCH_UNCHANGED", "0") != "0"

# Connection keepalive. WITHOUT these, a silently-dropped Supabase pooler
# connection (SSL closed by the far end with no FIN reaching us) leaves psycopg
# blocked on a socket read forever: an import once hung 17.5 HOURS on one branch
# waiting for a reply that was never coming, before TCP finally gave up. With
# them the kernel probes a quiet socket and surfaces the dead connection in
# ~connect_timeout + (idle + interval*count) seconds — here ~30s worst case —
# which the batch retry loop then reconnects and continues from.
#   connect_timeout   : cap the initial TCP+auth handshake
#   keepalives_idle   : start probing after 30s of silence
#   keepalives_interval: 10s between probes
#   keepalives_count  : 3 unanswered probes -> declare the socket dead (~30s)
CONNECT_KWARGS = dict(
    connect_timeout=10,
    keepalives=1,
    keepalives_idle=30,
    keepalives_interval=10,
    keepalives_count=3,
)


def db_connect(dsn: str):
    """Single place every importer connection is opened. Applies the keepalive
    settings and the pooler-specific prepared-statement disable, so no call site
    can forget either. Returns a NON-autocommit connection."""
    import psycopg
    conn = psycopg.connect(dsn, **CONNECT_KWARGS)
    conn.autocommit = False
    if ":6543" in dsn:  # Supabase transaction pooler can't do prepared statements
        conn.prepare_threshold = None
    return conn

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
        # Latest observed_at seen this run — stamped onto catalog.branches
        # .last_scraped_at at finish_run. Tracks the SCRAPE time from the export
        # records, not import wall-clock, so a late import of an old export
        # can't claim the branch was scraped just now.
        self.max_observed_at: Optional[str] = None
        self.canonicals_created = 0
        self.variants_created = 0
        self.identifiers_created = 0
        self.identifiers_touched = 0
        # promotions flipped is_active=false by sweep_unseen because no product in
        # the branch still points at them this run — i.e. the promo has ENDED.
        self.promos_deactivated = 0
        self.stage_times: dict[str, float] = {}
        # split of the re-import write path (see _copy_update)
        self.changed_rows = 0
        self.unchanged_rows = 0
        self.rows_written: dict[str, int] = {}   # table -> rows actually written
        self.rows_skipped: dict[str, int] = {}   # table -> rows sent but unchanged
        # retailer_sku values this run declined to write because another row already
        # owned them (see sku_for). Non-fatal, but a high count means the export has
        # a lot of sku reuse and is worth reporting rather than silently swallowing.
        self.sku_conflicts = 0
        # Same product, a barcode we hadn't seen before. Name matched, so the new
        # barcode is kept as an extra (non-primary) identifier rather than
        # overwriting the one already stored.
        self.barcodes_aliased = 0
        # Same sku/id but a DIFFERENT barcode AND a different name — never
        # overwritten, queued for a human instead.
        self.barcode_conflicts = 0
        self._seen_retailer_product_ids: set[str] = set()

    # ---- stage profiling ---------------------------------------------------- #
    # Where the wall-clock actually goes, per stage, accumulated across every
    # batch and printed in the summary. Optimising without this is guesswork.
    from contextlib import contextmanager

    @contextmanager
    def _stage(self, name: str):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            dt = time.perf_counter() - t0
            self.stage_times[name] = self.stage_times.get(name, 0.0) + dt

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
                       "canonicals_created", "variants_created", "identifiers_created",
                       "identifiers_touched", "sku_conflicts",
                       "barcodes_aliased", "barcode_conflicts",
                       "changed_rows", "unchanged_rows")

    # Per-table row-write tallies. Kept as dicts (not plain ints) so the log can
    # say WHICH table absorbed the writes — "342 rows written" is only
    # actionable if you know whether it was retailer_products or the price rows.
    _BATCH_COUNTER_DICTS = ("rows_written", "rows_skipped")

    def _counter_snapshot(self) -> dict:
        snap = {k: getattr(self, k) for k in self._BATCH_COUNTERS}
        # dicts must be COPIED — a reference would be mutated by the very batch
        # we are trying to be able to roll back.
        snap.update({k: dict(getattr(self, k)) for k in self._BATCH_COUNTER_DICTS})
        return snap

    def _restore_counters(self, snap: dict) -> None:
        for k, v in snap.items():
            setattr(self, k, dict(v) if isinstance(v, dict) else v)

    def _write_tally(self, table: str, written: int, offered: int) -> None:
        """Record that `offered` rows were sent for `table` and `written` of them
        actually needed writing. The gap is the work the change-detection saved."""
        short = table.split(".")[-1]
        self.rows_written[short] = self.rows_written.get(short, 0) + written
        self.rows_skipped[short] = self.rows_skipped.get(short, 0) + (offered - written)

    def reconnect(self, dsn: str) -> None:
        """Rebuild a dead connection (e.g. the pooler dropped us after a timeout).
        Run-level caches hold only committed ids, so they stay valid across a
        reconnect — only the live conn/cursor need replacing."""
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = db_connect(dsn)
        self.cur = self.conn.cursor()

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

    @staticmethod
    def _unnest_sql(rows: list[tuple], types: tuple[str, ...]) -> tuple[str, list]:
        """Build an `unnest(%s::t1[], %s::t2[], ...)` FROM-fragment plus one array
        param per column (columns transposed out of `rows`). Each column is a
        SINGLE bind param regardless of row count, so this sidesteps Postgres's
        65,535 bind-param ceiling that a multi-row `VALUES (...),(...)` hits at
        ~2.8k rows — batches can be 20k+. The `::type[]` array cast pins the
        column type (so an all-NULL column is unambiguous, same guarantee the
        row-0 cast gave `_values_sql`). psycopg adapts Python lists to Postgres
        arrays element-wise: None->NULL, Jsonb->jsonb, enum-label str->enum, etc.
        Row order is preserved (unnest yields array-index order) — the same
        positional RETURNING assumption the `VALUES` path already relied on.
        Multi-arg `unnest(a,b) AS v(x,y)` is used elsewhere here (see promo /
        exact-name lookups), so this is an established pattern, not a new trick."""
        frag = "unnest(" + ",".join(f"%s::{t}[]" for t in types) + ")"
        params = [list(col) for col in zip(*rows)]
        return frag, params

    def _bulk_insert(self, table: str, cols: tuple, types: tuple, rows: list[tuple],
                      returning: Optional[str] = None):
        if not rows:
            return [] if returning else None
        frag, params = self._unnest_sql(rows, types)
        sql = f"INSERT INTO {table} ({','.join(cols)}) SELECT * FROM {frag}"
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
        frag, params = self._unnest_sql(rows, all_types)
        if coalesce:
            set_clause = ", ".join(f"{c} = COALESCE(v.{c}, t.{c})" for c in set_cols)
        else:
            set_clause = ", ".join(f"{c} = v.{c}" for c in set_cols)
        if extra_set:
            set_clause = f"{set_clause}, {extra_set}" if set_clause else extra_set
        sql = (f"UPDATE {table} AS t SET {set_clause} "
               f"FROM {frag} AS v({','.join(all_cols)}) "
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
                     key: str = "id", changed_sql: Optional[str] = None,
                     touch_sql: Optional[str] = None) -> int:
        """COPY `rows` into an ephemeral TEMP staging table, then
        `UPDATE target FROM staging`. The TEMP table is ON COMMIT DROP — pure
        scratch tied to this transaction; it never touches real catalog data and
        is auto-discarded at commit/rollback (no DELETE/DROP issued here).
        `set_sql` references staging as `v` and the target as `t`.

        `changed_sql` + `touch_sql` split the write in two, which is the single
        biggest speed win in the importer. A re-import re-sends every row, but
        almost nothing has actually changed (measured: 17,642 rows "updated" for
        20 real price changes). Rewriting them all is not free — each one is a
        new heap tuple, new index entries for any indexed column it touches, plus
        TOAST churn for wide jsonb, plus the WAL for all of it.

        So: rows that genuinely differ take the full UPDATE; every other row gets
        only `touch_sql`, which sets nothing but freshness columns. Because those
        columns carry no index, Postgres can apply it as a HOT (heap-only tuple)
        update — no index maintenance at all — and there is no jsonb to re-TOAST.

        Order matters. The touch runs FIRST: it writes only columns that
        `changed_sql` does not read, so the second statement still evaluates its
        predicate against the original values. Run the other way round, the full
        UPDATE would make the rows match and the touch would then re-hit every
        row it had just skipped."""
        if not rows:
            return 0
        self.cur.execute(f"CREATE TEMP TABLE {temp} ({temp_ddl}) ON COMMIT DROP")
        with self.cur.copy(f"COPY {temp} ({','.join(cols)}) FROM STDIN") as cp:
            for row in rows:
                cp.write_row(row)
        where = f"t.{key} = v.{key}"
        if changed_sql and not TOUCH_UNCHANGED:
            # Default: unchanged rows are not written at all. Out-of-stock
            # detection does not depend on this (see the note by TOUCH_UNCHANGED).
            where += f" AND ({changed_sql})"
        elif changed_sql and touch_sql:
            self.cur.execute(
                f"UPDATE {target} AS t SET {touch_sql} FROM {temp} AS v "
                f"WHERE {where} AND NOT ({changed_sql})")
            self.unchanged_rows += self.cur.rowcount
            where += f" AND ({changed_sql})"
        self.cur.execute(
            f"UPDATE {target} AS t SET {set_sql} FROM {temp} AS v WHERE {where}")
        written = self.cur.rowcount
        self.changed_rows += written
        # `offered` is what we sent, `written` is what actually needed writing —
        # the difference is the point of the whole change-detection path, so it
        # is tallied per table rather than inferred from a batch-size figure.
        self._write_tally(target, written, len(rows))
        return written

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
        # get-or-create is racy under parallel workers: they all miss the SELECT
        # above, then collide on the (source_kind, name) unique constraint. ON
        # CONFLICT makes the INSERT a no-op for the losers, who re-SELECT the
        # winner's row below instead of dying with UniqueViolation.
        row = self._one(
            "INSERT INTO ingest.source_systems (source_kind, name, retailer_id, is_active) "
            "VALUES ('scraper', %s, %s, true) "
            "ON CONFLICT (source_kind, name) DO NOTHING RETURNING id",
            (self.source_system, self.retailer_id))
        if row:
            log.info("registered new source_system %r", self.source_system)
            return row[0]
        row = self._one(
            "SELECT id FROM ingest.source_systems WHERE name = %s", (self.source_system,))
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
    def reap_stale_runs(self, older_than: str = STALE_RUN_AFTER):
        """Close out this branch's abandoned 'running' rows before starting a new
        one. finish_run() is best-effort: a SIGKILL (pkill -9), an OOM, or a
        dropped connection leaves status='running' forever, so runs accumulate as
        permanent false positives (45 of them by 2026-07-16). Scoped to THIS
        branch so 300+ parallel workers don't all contend on the same rows, and
        age-gated so a legitimately in-flight import of the same branch is never
        stolen. Every branch self-heals on its next import."""
        if not self.branch_id:
            return 0
        self.cur.execute(
            "UPDATE ingest.import_runs SET status = 'stale', finished_at = now(), "
            "error_log = COALESCE(error_log, '') || "
            "  '[reaper] still ''running'' after ' || %s || '; process died without "
            "finishing (killed / OOM / lost connection)' "
            "WHERE branch_id = %s AND status = 'running' "
            "  AND started_at < now() - %s::interval",
            (older_than, self.branch_id, older_than))
        n = self.cur.rowcount
        self.conn.commit()
        if n:
            log.warning("reaped %d stale 'running' import_run(s) for this branch", n)
        return n

    def start_run(self, run_type: str, total: int):
        self.reap_stale_runs()
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
        # catalog.branches.last_scraped_at had NO writer at all: the scrapers
        # stamp it on store_branches in the app's OPERATIONAL Supabase, which is
        # a different database from pico-prod, so all 393 rows here sat NULL
        # forever. The importer is pico-prod's only writer, so it stamps it —
        # GREATEST() keeps the newest scrape when branches import out of order,
        # and only on success so a failed import can't advertise fresh data.
        if status == "success" and self.branch_id and self.max_observed_at:
            self.cur.execute(
                "UPDATE catalog.branches SET last_scraped_at = "
                "  GREATEST(COALESCE(last_scraped_at, '-infinity'::timestamptz), %s::timestamptz), "
                "  updated_at = now() "
                "WHERE id = %s",
                (self.max_observed_at, self.branch_id))

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
        # Only track the scrape high-water mark for batches that will actually be
        # committed: --rollback-test still calls finish_run('success'), so letting
        # it accumulate here would stamp branches.last_scraped_at for data that
        # was deliberately rolled back. ISO-8601 UTC ('...Z') sorts correctly as
        # a plain string — all scrapers emit exactly that format.
        if commit:
            seen = [rec["observed_at"] for rec in batch if rec.get("observed_at")]
            if seen:
                self.max_observed_at = max(seen + ([self.max_observed_at]
                                                   if self.max_observed_at else []))
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

            # DISABLED 2026-07-14 (per request): skip scraped_observations write.
            # self._bulk_insert_observations(batch, Jsonb)

            with self._stage("prep"):
                pass
            with self._stage("retailer_products"):
                rp_ids, canonical_ids, variant_ids, is_new = \
                    self._bulk_upsert_retailer_products(batch, Jsonb)

            with self._stage("match"):
                self._bulk_match(batch, rp_ids, canonical_ids, variant_ids)

            # After matching: any barcode the matcher had to create now exists, so
            # this sees the complete set.
            with self._stage("identifiers"):
                self._bulk_touch_identifiers(batch)

            for is_n in is_new:
                if is_n:
                    self.new_products += 1

            with self._stage("promotions"):
                promo_ids = self._bulk_upsert_promotions(batch)

            with self._stage("current+history+items"):
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
    # DISABLED 2026-07-14 (per request): scraped_observations preprocessing +
    # DB write are commented out. Restore by removing the `return` below and
    # uncommenting the body, plus the call site in process_batch (~line 745).
    def _bulk_insert_observations(self, batch: list[dict], Jsonb) -> None:
        return  # no-op while scraped_observations is disabled
        # cols = ("import_run_id", "retailer_id", "branch_id", "source_product_id",
        #         "retailer_sku", "barcode", "raw_name", "raw_brand", "raw_category_path",
        #         "product_url", "image_url", "price_cents", "comparison_price_cents",
        #         "unit_price_cents", "unit_label", "stock_status", "stock_quantity",
        #         "raw_promo_text", "normalized_promo_type", "observed_at", "raw_row",
        #         "card_required", "required_loyalty_program_id")
        # types = ("uuid", "uuid", "uuid", "text", "text", "text", "text", "text", "text",
        #          "text", "text", "integer", "integer", "integer", "text", "catalog.stock_status",
        #          "integer", "text", "catalog.promo_type", "timestamptz", "jsonb", "boolean", "uuid")
        # rows = [(
        #     self.run_id, self.retailer_id, self.branch_id, rec["source_product_id"],
        #     rec["retailer_sku"], rec["barcode"], rec["raw_name"], rec["brand"],
        #     rec["category_path"], rec["product_url"], rec["image_url"],
        #     rec["current_price_cents"], rec["comparison_price_cents"],
        #     rec["unit_price_cents"], rec["unit_label"], rec["stock_status"],
        #     rec["stock_quantity"], rec["promo_text"], rec["_normalized_promo"],
        #     rec["observed_at"], Jsonb(rec["raw_row"]), rec["card_required"], rec["_loyalty_id"],
        # ) for rec in batch]
        # # COPY fast path: this table is append-only (no ON CONFLICT / RETURNING),
        # # so it's the safest and biggest single win. `types` above is kept as an
        # # inline reference to the column types (COPY infers them from the table).
        # self._copy_insert("ingest.scraped_observations", cols, rows)

    # ---- stage: retailer_products (bulk resolve existing + bulk write) ----- #
    def _bulk_upsert_retailer_products(self, batch: list[dict], Jsonb):
        n = len(batch)
        spids = [rec["source_product_id"] for rec in batch if rec["source_product_id"]]
        all_skus = [rec["retailer_sku"] for rec in batch if rec["retailer_sku"]]

        existing: dict[tuple, tuple] = {}  # (kind, key) -> (id, canonical_id, variant_id)
        # sku -> (id, canonical_id, variant_id) of whichever row already OWNS it.
        # catalog.retailer_products has TWO unique constraints —
        # (retailer_id, source_product_id) AND (retailer_id, retailer_sku) — and the
        # ON CONFLICT below can only arbitrate on one of them, so a collision on
        # retailer_sku still 23505s on ..._retailer_sku_key. That is NOT a race:
        # two rows in the SAME export with different source_product_ids sharing one
        # retailer_sku fail identically on every one of the 10 batch retries and
        # kill the branch (the duplicate-key failures reported 2026-07-16). So every
        # sku in the batch is looked up here — not just the spid-less ones, as
        # before — and a sku already spoken for is dropped rather than fought over.
        sku_owner: dict[str, tuple] = {}
        # rp_id -> (stored barcode, stored clean_name). Needed to decide what to do
        # when a record resolves to a product we already have but carries a
        # different barcode: same name => extra barcode, different name => review.
        rp_info: dict[str, tuple] = {}
        if spids or all_skus:
            rows = self._all(
                "SELECT id, canonical_product_id, product_variant_id, source_product_id, "
                "retailer_sku, barcode, clean_name "
                "FROM catalog.retailer_products WHERE retailer_id = %s "
                "AND (source_product_id = ANY(%s) OR retailer_sku = ANY(%s))",
                (self.retailer_id, spids or [], all_skus or []))
            for rp_id, cid, vid, spid, sku, old_bc, old_name in rows:
                key = ("spid", spid) if spid else ("sku", sku)
                existing[key] = (rp_id, cid, vid)
                if sku:
                    sku_owner[sku] = (rp_id, cid, vid)
                rp_info[rp_id] = (old_bc, old_name)

        def ident(rec):
            return ("spid", rec["source_product_id"]) if rec["source_product_id"] \
                else ("sku", rec["retailer_sku"])

        # Which batch row gets to keep a contested sku. Records with NO
        # source_product_id are IDENTIFIED by their retailer_sku, so they claim
        # first — take it away and ident() keys on None and the row loses its
        # identity entirely. dedupe() guarantees at most one spid-less record per
        # sku, so these pre-claims never fight each other; the only possible
        # in-batch rival is a spid-bearing row, which can safely give the sku up.
        claimed: dict[str, int] = {}
        for i, rec in enumerate(batch):
            if not rec["source_product_id"] and rec["retailer_sku"]:
                claimed.setdefault(rec["retailer_sku"], i)

        def sku_for(i: int, rec: dict, rp_id: Optional[str]) -> Optional[str]:
            """The retailer_sku this row may safely write, or None. Dropping one is
            lossless in practice: NULL is exempt from the unique index, the UPDATE
            path COALESCEs (so an existing good value is kept), and a spid-bearing
            row is identified by its spid anyway. It only declines to STEAL a sku
            another row already holds."""
            sku = rec["retailer_sku"]
            if not sku:
                return None
            owner = sku_owner.get(sku)
            if owner and owner[0] != rp_id:
                self.sku_conflicts += 1
                return None          # a different retailer_product already holds it
            if claimed.setdefault(sku, i) != i:
                self.sku_conflicts += 1
                return None          # an earlier row in THIS batch already took it
            return sku

        rp_ids: list[Optional[str]] = [None] * n
        canonical_ids: list[Optional[str]] = [None] * n
        variant_ids: list[Optional[str]] = [None] * n
        is_new: list[bool] = [False] * n

        update_rows = []
        new_idxs = []
        alias_rows = []      # (cid, vid, barcode, source) — extra barcodes to keep
        conflict_rows = []   # (rp_id, cid, vid, rec, old_barcode, old_name)
        for i, rec in enumerate(batch):
            hit = existing.get(ident(rec))
            if hit is None and not rec["source_product_id"] and rec["retailer_sku"]:
                # No spid, so the sku IS this row's identity. If a row that DOES
                # have a spid already owns that sku it is the same product at the
                # same retailer — reuse it instead of inserting a second row that
                # would collide on (retailer_id, retailer_sku).
                hit = sku_owner.get(rec["retailer_sku"])
            if hit:
                rp_ids[i], canonical_ids[i], variant_ids[i] = hit
                old_bc, old_name = rp_info.get(rp_ids[i], (None, None))
                new_bc = rec["barcode"]
                if new_bc and old_bc and new_bc != old_bc:
                    # Same product identity (spid/sku) but a barcode we don't hold.
                    # The name decides: matching name = the retailer gave us another
                    # barcode for the same item, so keep BOTH. Different name means
                    # the identity is suspect and a human has to look.
                    if _same_product_name(old_name, rec["clean_name"] or rec["raw_name"]):
                        alias_rows.append((canonical_ids[i], variant_ids[i], new_bc,
                                           self.source_system))
                        self.barcodes_aliased += 1
                    else:
                        conflict_rows.append((rp_ids[i], canonical_ids[i], variant_ids[i],
                                              rec, old_bc, old_name))
                update_rows.append((
                    rp_ids[i], sku_for(i, rec, rp_ids[i]), rec["raw_name"], rec["clean_name"],
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
                # NEVER overwrite a barcode we already hold: t.barcode wins, and a
                # new value only fills an empty column. A differing barcode is
                # handled out-of-band (aliased onto the product, or queued for
                # review when the name doesn't match) — see _classify_barcode.
                "barcode = COALESCE(t.barcode, v.barcode), "
                "raw_latest = COALESCE(NULLIF(v.raw_latest, '{}'::jsonb), t.raw_latest), "
                "last_seen_at = now(), is_active = true, updated_at = now()",
                # Cheap scalars first — OR short-circuits left to right, so a row
                # that differs on a name/url never pays for the jsonb comparison.
                # raw_latest goes last precisely because it has to detoast.
                changed_sql=(
                    "t.raw_name IS DISTINCT FROM v.raw_name"
                    " OR t.is_active IS DISTINCT FROM true"
                    " OR t.retailer_sku IS DISTINCT FROM COALESCE(v.retailer_sku, t.retailer_sku)"
                    " OR t.clean_name IS DISTINCT FROM COALESCE(v.clean_name, t.clean_name)"
                    " OR t.brand_text IS DISTINCT FROM COALESCE(v.brand_text, t.brand_text)"
                    " OR t.raw_category_path IS DISTINCT FROM COALESCE(v.raw_category_path, t.raw_category_path)"
                    " OR t.product_url IS DISTINCT FROM COALESCE(v.product_url, t.product_url)"
                    " OR t.image_url IS DISTINCT FROM COALESCE(v.image_url, t.image_url)"
                    # only true when t.barcode IS NULL and v.barcode is not, i.e.
                    # we are filling a blank rather than replacing anything
                    " OR t.barcode IS DISTINCT FROM COALESCE(t.barcode, v.barcode)"
                    " OR t.raw_latest IS DISTINCT FROM COALESCE(NULLIF(v.raw_latest, '{}'::jsonb), t.raw_latest)"),
                # freshness only — updated_at means "the data changed", so it is
                # deliberately NOT written here (see the semantics note up top).
                touch_sql="last_seen_at = now()")

        # Both are no-ops when nothing conflicted, which is the overwhelming case.
        self._save_extra_barcodes(alias_rows)
        self._queue_barcode_conflicts(conflict_rows)

        if new_idxs:
            # match_status defaults to 'unreviewed' at the DB level, but this
            # importer's convention (matched by _bulk_match below) is 'unmatched'
            # for anything not yet resolved — must be set explicitly, NOT left
            # to the column default. is_active/first_seen_at/last_seen_at DO
            # have matching defaults (checked against the live schema), so they
            # don't need to be in the INSERT.
            new_rows = [(
                self.retailer_id, sku_for(i, batch[i], None), batch[i]["raw_name"],
                batch[i]["clean_name"], batch[i]["brand"], batch[i]["category_path"],
                batch[i]["product_url"], batch[i]["image_url"], batch[i]["barcode"],
                batch[i]["source_product_id"], Jsonb(batch[i]["raw_row"] or {}), "unmatched",
            ) for i in new_idxs]
            # ON CONFLICT DO NOTHING instead of a bare INSERT: under --jobs>1 two
            # branches can both SELECT-miss the SAME brand-new shared product and
            # race to INSERT it. A bare INSERT makes the loser 23505 and forces a
            # whole-batch retry (which livelocks big batches — see the jobs-4/8k
            # Pak'nSave run: 6 branches died on retailer_products_..._key). DO
            # NOTHING lets the loser's row be silently skipped; RETURNING then
            # yields ONLY the rows THIS worker actually inserted (that's the
            # is_new set, canonical still NULL -> matcher creates it). The raced
            # rows the winner already committed are fetched below by natural key
            # and REUSED — no duplicate canonical, no double-count. Results are
            # mapped to records BY KEY (spid/sku), never by RETURNING position,
            # so a shuffled result set can never mislink a product to the wrong
            # canonical. TARGETED at (retailer_id, source_product_id) — the exact
            # constraint that races — NOT a bare `ON CONFLICT DO NOTHING`: a
            # target-less form would also swallow a conflict on some OTHER unique
            # index (e.g. barcode), and that skipped row wouldn't be found by the
            # spid/sku re-SELECT below -> KeyError. Any non-spid conflict instead
            # falls through to 23505 and the existing transient-retry path. Rows
            # with NULL source_product_id never match this index, so they always
            # insert and appear in RETURNING.
            frag, params = self._unnest_sql(
                new_rows,
                ("uuid", "text", "text", "text", "text", "text", "text", "text", "text",
                 "text", "jsonb", "text"))
            self.cur.execute(
                "INSERT INTO catalog.retailer_products "
                "(retailer_id, retailer_sku, raw_name, clean_name, brand_text, "
                " raw_category_path, product_url, image_url, barcode, source_product_id, "
                " raw_latest, match_status) "
                f"SELECT * FROM {frag} "
                "ON CONFLICT (retailer_id, source_product_id) DO NOTHING "
                "RETURNING id, source_product_id, retailer_sku",
                params)
            inserted_by_key: dict[tuple, str] = {}
            for rid, spid, sku in self.cur.fetchall():
                inserted_by_key[("spid", spid) if spid else ("sku", sku)] = rid

            # Anything in new_idxs NOT returned above lost an insert race — the
            # winner's row is already committed; fetch its id + existing
            # canonical/variant so we reuse (never recreate) them.
            raced = [i for i in new_idxs if ident(batch[i]) not in inserted_by_key]
            raced_by_key: dict[tuple, tuple] = {}
            if raced:
                r_spids = [batch[i]["source_product_id"] for i in raced if batch[i]["source_product_id"]]
                r_skus = [batch[i]["retailer_sku"] for i in raced
                          if not batch[i]["source_product_id"] and batch[i]["retailer_sku"]]
                for rid, cid, vid, spid, sku in self._all(
                    "SELECT id, canonical_product_id, product_variant_id, source_product_id, retailer_sku "
                    "FROM catalog.retailer_products WHERE retailer_id = %s "
                    "AND (source_product_id = ANY(%s) "
                    "     OR (source_product_id IS NULL AND retailer_sku = ANY(%s)))",
                    (self.retailer_id, r_spids or [], r_skus or [])):
                    raced_by_key[("spid", spid) if spid else ("sku", sku)] = (rid, cid, vid)

            for i in new_idxs:
                key = ident(batch[i])
                if key in inserted_by_key:
                    rp_ids[i] = inserted_by_key[key]
                    is_new[i] = True  # genuinely inserted by us; matcher creates its canonical
                else:
                    # lost the race: reuse the winner's row + its canonical/variant
                    rp_ids[i], canonical_ids[i], variant_ids[i] = raced_by_key[key]

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
            # Sorted by normalized_name so every parallel worker takes the brand
            # row locks in the SAME order. Unsorted, two workers whose batches
            # share brands in different orders lock them head-on and deadlock
            # (observed as DeadlockDetected in relation "brands").
            items = sorted(to_resolve.items())  # [(normalized_name, original_text)]
            values_sql, params = self._values_sql([(n,) for _, n in items], ("text",))
            self.cur.execute(
                f"INSERT INTO catalog.brands (name) VALUES {values_sql} "
                "ON CONFLICT (normalized_name) DO UPDATE SET name = catalog.brands.name "
                "RETURNING id, normalized_name",
                params)
            # Mapped BY KEY off the returned normalized_name, never by RETURNING
            # position: RETURNING order is not guaranteed to follow VALUES order,
            # so zipping ids onto the input list can bind a brand to the WRONG id.
            returned = {norm: bid for bid, norm in self.cur.fetchall()}
            for py_norm, text in items:
                bid = returned.get(py_norm)
                if bid is None:
                    # python's db_normalized_name() diverged from the DB's
                    # GENERATED column (its \s matches unicode spaces like \xa0,
                    # Postgres' does not), so the returned key isn't the one we
                    # cache under. Ask the DB rather than drop the brand to NULL.
                    row = self._one(
                        "SELECT id FROM catalog.brands WHERE normalized_name = "
                        "lower(regexp_replace(%s, '\\s+', ' ', 'g'))", (text,))
                    bid = row[0] if row else None
                if bid is not None:
                    self._brand_cache[py_norm] = bid

        for i, text in enumerate(brand_texts):
            b = (text or "").strip()
            if b:
                out[i] = self._brand_cache.get(db_normalized_name(b))
        return out

    # ---- barcode preservation (never overwrite) ----------------------------- #
    def _save_extra_barcodes(self, alias_rows: list) -> None:
        """Keep an additional barcode for a product we already know.

        The product's stored `retailer_products.barcode` is left alone; the new
        value is recorded as a non-primary global identifier so nothing is lost.
        DO NOTHING on conflict is deliberate: if this barcode already exists
        globally it belongs to some product's match chain, and silently
        re-pointing it would corrupt matching for whatever owns it.
        """
        if not alias_rows:
            return
        frag, params = self._unnest_sql(alias_rows, ("uuid", "uuid", "text", "text"))
        self.cur.execute(
            "INSERT INTO catalog.product_identifiers "
            "(product_variant_id, canonical_product_id, retailer_id, identifier_type, "
            " identifier_value, is_primary, source, first_seen_at, last_seen_at) "
            "SELECT v.vid, v.cid, NULL, 'barcode', v.bc, false, v.source, now(), now() "
            f"FROM {frag} AS v(cid, vid, bc, source) "
            "ON CONFLICT (identifier_type, identifier_value) WHERE retailer_id IS NULL "
            "AND identifier_type = ANY(ARRAY['barcode','gtin','ean','upc']"
            "::catalog.product_identifier_type[]) DO NOTHING",
            params)

    def _queue_barcode_conflicts(self, conflict_rows: list) -> None:
        """Same sku/source id, different barcode AND a different name — refuse to
        guess. The stored barcode stays untouched and a human gets the evidence."""
        if not conflict_rows:
            return
        from psycopg.types.json import Jsonb
        rows = [(
            rp_id, cid, vid, "barcode_conflict", "open", None,
            Jsonb({
                "existing_barcode": old_bc,
                "incoming_barcode": rec["barcode"],
                "existing_name": old_name,
                "incoming_name": rec["clean_name"] or rec["raw_name"],
                "retailer_sku": rec["retailer_sku"],
                "source_product_id": rec["source_product_id"],
                "brand": rec["brand"],
                "size": rec["size"],
            }),
        ) for (rp_id, cid, vid, rec, old_bc, old_name) in conflict_rows]
        frag, params = self._unnest_sql(
            rows, ("uuid", "uuid", "uuid", "text", "text", "numeric", "jsonb"))
        self.cur.execute(
            "INSERT INTO catalog.product_match_reviews "
            "(retailer_product_id, suggested_canonical_product_id, suggested_product_variant_id, "
            " review_reason, status, score, raw_evidence) "
            f"SELECT v.rp_id, v.scid, v.svid, v.reason, v.status, v.score, v.evidence "
            f"FROM {frag} AS v(rp_id, scid, svid, reason, status, score, evidence) "
            "ON CONFLICT (retailer_product_id, review_reason) WHERE status = 'open' "
            "DO UPDATE SET raw_evidence = EXCLUDED.raw_evidence",
            params)
        self.reviews_created += len(rows)
        self.barcode_conflicts += len(rows)

    # ---- stage: keep global barcode identifiers fresh ----------------------- #
    def _bulk_touch_identifiers(self, batch: list[dict]) -> None:
        """Bump last_seen_at on the global barcode rows this batch just observed.

        Without this the column lies. The only writer was the ON CONFLICT DO
        UPDATE on the create path in _bulk_match, which is reached ONLY for a
        barcode we believed to be brand new — and _bulk_match returns early for
        anything already matched, which is every record of every re-import. So a
        barcode's last_seen_at recorded when we first inserted it and never moved
        again, however many times we saw the product afterwards.

        Runs from process_batch, NOT from _bulk_match, precisely because the
        matcher short-circuits on re-imports — the case this is for.

        Deadlock-safe by construction: these rows are global (retailer_id IS
        NULL), so under --jobs>1 every worker wants overlapping sets of them.
        FOR UPDATE ... SKIP LOCKED takes only the rows nobody else holds and
        walks them in a fixed ORDER BY id, so this can never sit head-on against
        another worker. A row skipped because a sibling had it is simply left for
        next time — the sibling is bumping it to the same now() anyway.
        """
        barcodes = sorted({rec["barcode"] for rec in batch if rec["barcode"]})
        if not barcodes:
            return
        self.cur.execute(
            "UPDATE catalog.product_identifiers SET last_seen_at = now() "
            "WHERE id IN ("
            "  SELECT id FROM catalog.product_identifiers "
            "   WHERE identifier_type = ANY(%s::catalog.product_identifier_type[]) "
            "     AND retailer_id IS NULL "
            "     AND identifier_value = ANY(%s) "
            "     AND last_seen_at < now() - %s::interval "  # NOT NULL in the schema
            "   ORDER BY id FOR UPDATE SKIP LOCKED)",
            (list(BARCODE_IDENTIFIER_TYPES), barcodes, IDENTIFIER_TOUCH_AFTER))
        self.identifiers_touched += self.cur.rowcount

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

        # (The advisory creation lock that used to be taken here is gone with
        # parallelism — a single process cannot race itself, and it cost two
        # round trips plus the lock hold on every batch that created anything.)
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
                frag, params = self._unnest_sql(
                    id_rows, ("uuid", "uuid", "text", "text"))
                self.cur.execute(
                    "INSERT INTO catalog.product_identifiers "
                    "(product_variant_id, canonical_product_id, retailer_id, identifier_type, "
                    " identifier_value, is_primary, source, first_seen_at, last_seen_at) "
                    "SELECT v.product_variant_id, v.canonical_product_id, NULL, 'barcode', "
                    "       v.identifier_value, true, v.source, now(), now() "
                    f"FROM {frag} AS v(product_variant_id, canonical_product_id, "
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
            frag, params = self._unnest_sql(
                review_insert_rows,
                ("uuid", "uuid", "uuid", "text", "text", "numeric", "jsonb"))
            self.cur.execute(
                "INSERT INTO catalog.product_match_reviews "
                "(retailer_product_id, suggested_canonical_product_id, suggested_product_variant_id, "
                " review_reason, status, score, raw_evidence) "
                f"SELECT v.rp_id, v.scid, v.svid, v.reason, v.status, v.score, v.evidence "
                f"FROM {frag} AS v(rp_id, scid, svid, reason, status, score, evidence) "
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
            frag, params = self._unnest_sql(
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
                f"FROM {frag} AS v(id, card_required, required_loyalty_program_id, "
                "starts_at, ends_at, metadata) "
                "WHERE t.id = v.id",
                params)
        return promo_ids

    # ---- stage: branch_product_current + price_history + promotion_items -- #
    def _bulk_upsert_current(self, batch: list[dict], rp_ids: list, canonical_ids: list,
                              variant_ids: list, promo_ids: list) -> None:
        n = len(batch)
        existing = {}
        with self._stage("  bpc:select"):
            rows = self._all(
                "SELECT retailer_product_id, id, current_price_cents, unit_price_cents, "
                "stock_status, active_promotion_id "
                "FROM catalog.branch_product_current "
                "WHERE branch_id = %s AND retailer_product_id = ANY(%s)",
                (self.branch_id, rp_ids))
        for rp_id, bpc_id, old_price, old_unit, old_stock, old_promo in rows:
            existing[rp_id] = (bpc_id, old_price, old_unit, old_stock, old_promo)

        update_rows = []
        insert_idxs = []
        bpc_id_by_idx: list[Optional[str]] = [None] * n
        # (bpc_id, rp_id, cid, vid, promo_id, rec, old_price, old_unit, old_stock, events)
        history_rows = []

        # Two batch records can resolve to the SAME retailer_product — e.g. a
        # spid-less record whose retailer_sku is owned by a spid-bearing row (see
        # sku_for in _bulk_upsert_retailer_products). branch_product_current is
        # UNIQUE (branch_id, retailer_product_id), so letting both through inserts
        # the same pair twice: a 23505 that is NOT transient — it reproduces
        # identically on all MAX_BATCH_RETRIES attempts and kills the branch.
        # Collapse them, LAST occurrence winning (the freshest observation, which
        # is what per-record sequential processing would have left behind), and
        # point the losers at the winner's bpc id so promotion_items still links.
        winner_for_rp: dict[str, int] = {}
        for i in range(n):
            if rp_ids[i] is not None:
                winner_for_rp[rp_ids[i]] = i
        dup_idxs = [i for i in range(n)
                    if rp_ids[i] is not None and winner_for_rp[rp_ids[i]] != i]
        if dup_idxs:
            log.debug("%d record(s) collapsed onto an already-claimed retailer_product",
                      len(dup_idxs))
        skip = set(dup_idxs)

        for i, rec in enumerate(batch):
            if i in skip:
                continue
            is_special = promo_ids[i] is not None
            hit = existing.get(rp_ids[i])
            if hit:
                bpc_id, old_price, old_unit, old_stock, old_promo = hit
                bpc_id_by_idx[i] = bpc_id
                price_changed = old_price != rec["current_price_cents"]
                unit_changed = old_unit != rec["unit_price_cents"]
                stock_changed = old_stock != rec["stock_status"]
                # str() both sides: old_promo comes back as a uuid object, the new
                # one is a string, and uuid != str is always True — which would
                # log a promo_change on every single import.
                promo_changed = (str(old_promo) if old_promo else None) != \
                                (str(promo_ids[i]) if promo_ids[i] else None)
                update_rows.append((
                    bpc_id, canonical_ids[i], variant_ids[i], rec["current_price_cents"],
                    rec["comparison_price_cents"], rec["unit_price_cents"], rec["unit_label"],
                    rec["stock_status"], rec["stock_quantity"], is_special, promo_ids[i],
                    rec["observed_at"], price_changed, rec["observed_at"] if price_changed else None,
                    stock_changed, rec["observed_at"] if stock_changed else None,
                ))
                self.updated += 1
                events = _history_events(price_changed, unit_changed, stock_changed, promo_changed)
                if events:
                    history_rows.append((bpc_id, rp_ids[i], canonical_ids[i], variant_ids[i],
                                          promo_ids[i], rec, old_price, old_unit, old_stock,
                                          events))
                    if price_changed:
                        self.price_changes += 1
            else:
                insert_idxs.append(i)

        if update_rows:
            # COPY-into-staging fast path (was a multi-row VALUES UPDATE). The
            # staging table carries price_changed/stock_changed for row-shape
            # parity with the tuple, though the SET only reads the *_updated_at
            # columns (already computed in Python above).
          with self._stage("  bpc:copy_update"):
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
                "last_seen_at = now(), updated_at = now()",
                # All cheap scalar comparisons — no jsonb on this table.
                changed_sql=(
                    "t.current_price_cents IS DISTINCT FROM v.current_price_cents"
                    " OR t.comparison_price_cents IS DISTINCT FROM v.comparison_price_cents"
                    " OR t.unit_price_cents IS DISTINCT FROM v.unit_price_cents"
                    " OR t.unit_label IS DISTINCT FROM v.unit_label"
                    " OR t.stock_status IS DISTINCT FROM v.stock_status"
                    " OR t.stock_quantity IS DISTINCT FROM v.stock_quantity"
                    " OR t.is_on_special IS DISTINCT FROM v.is_on_special"
                    " OR t.active_promotion_id IS DISTINCT FROM v.active_promotion_id"
                    " OR t.canonical_product_id IS DISTINCT FROM COALESCE(v.canonical_product_id, t.canonical_product_id)"
                    " OR t.product_variant_id IS DISTINCT FROM COALESCE(v.product_variant_id, t.product_variant_id)"),
                # scraped_at still moves for every row we saw — that is the
                # "we observed this product in this scrape" fact, and it is what
                # keeps the branch from looking stale.
                # freshness only — scraped_at/last_seen_at record "we observed
                # this product in this scrape"; updated_at stays put because
                # nothing about the row's data actually changed.
                touch_sql="scraped_at = v.scraped_at, last_seen_at = now()")

        if insert_idxs:
          with self._stage("  bpc:insert"):
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
                                      promo_ids[i], batch[i], None, None, None,
                                      ["initial_seen"]))

        if history_rows:
          with self._stage("  price_history"):
            from psycopg.types.json import Jsonb
            hist_ins = [(
                bpc_id, self.branch_id, rp_id, cid, vid, old_price, rec["current_price_cents"],
                old_unit, rec["unit_price_cents"], old_stock, rec["stock_status"], promo_id,
                self.run_id, rec["observed_at"],
                # event_type = the dominant reason this row exists (for filtering a
                # price chart); events = everything that changed at this instant, so
                # a simultaneous price+stock move isn't lost.
                Jsonb({"event_type": events[0], "events": events}),
            ) for (bpc_id, rp_id, cid, vid, promo_id, rec, old_price, old_unit, old_stock,
                   events) in history_rows]
            self._bulk_insert(
                "catalog.price_history",
                ("branch_product_id", "branch_id", "retailer_product_id", "canonical_product_id",
                 "product_variant_id", "old_price_cents", "new_price_cents", "old_unit_price_cents",
                 "new_unit_price_cents", "old_stock_status", "new_stock_status", "promotion_id",
                 "source_run_id", "changed_at", "metadata"),
                ("uuid", "uuid", "uuid", "uuid", "uuid", "integer", "integer", "integer",
                 "integer", "catalog.stock_status", "catalog.stock_status", "uuid", "uuid",
                 "timestamptz", "jsonb"),
                hist_ins)

        # Losers inherit the winner's bpc id: they are the same product in the same
        # branch, so anything keyed off branch_product_id still resolves.
        for i in dup_idxs:
            bpc_id_by_idx[i] = bpc_id_by_idx[winner_for_rp[rp_ids[i]]]

        self._bulk_upsert_promotion_items(batch, rp_ids, bpc_id_by_idx, promo_ids, skip)

    def _bulk_upsert_promotion_items(self, batch, rp_ids, bpc_ids, promo_ids,
                                      skip: Optional[set] = None) -> None:
        from psycopg.types.json import Jsonb
        # `skip` carries the collapsed duplicates from _bulk_upsert_current — they
        # would insert a second row for the same (promotion_id, retailer_product_id,
        # branch_product_id), which promotion_items_unique_idx rejects.
        skip = skip or set()
        idxs = [i for i in range(len(batch))
                if promo_ids[i] is not None and i not in skip]
        if not idxs:
            return
        pairs = [(promo_ids[i], rp_ids[i]) for i in idxs]
        existing = {}
        with self._stage("  items:select"):
          _existing_rows = self._all(
            "SELECT promotion_id, retailer_product_id, id FROM catalog.promotion_items "
            "WHERE (promotion_id, retailer_product_id) IN ("
            "  SELECT * FROM unnest(%s::uuid[], %s::uuid[]))",
            ([p[0] for p in pairs], [p[1] for p in pairs]))
        for pid, rpid, item_id in _existing_rows:
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
          with self._stage("  items:update"):
            self._bulk_update(
                "catalog.promotion_items", "id", "uuid",
                ("branch_product_id", "original_price_cents", "special_price_cents",
                 "discount_percent", "multibuy_quantity", "multibuy_price_cents",
                 "unit_price_cents", "metadata"),
                ("uuid", "integer", "integer", "integer", "integer", "integer", "integer",
                 "jsonb"),
                update_rows)
        if insert_rows:
          with self._stage("  items:insert"):
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
        # Promo-ended: a product's own promo fields (is_on_special/active_promotion_id)
        # are already cleared to NULL on re-import when the latest scrape carries no
        # promo for it. That orphans the promotion row (nothing points at it), so we
        # soft-deactivate it here. The subquery is filtered to NON-NULL ids, so the
        # NOT IN can't be poisoned by a NULL (which would silently match nothing).
        self.cur.execute(
            "UPDATE catalog.promotions SET is_active = false, updated_at = now() "
            "WHERE branch_id = %s AND is_active = true AND id NOT IN ("
            "  SELECT active_promotion_id FROM catalog.branch_product_current "
            "  WHERE branch_id = %s AND active_promotion_id IS NOT NULL)",
            (self.branch_id, self.branch_id))
        self.promos_deactivated = self.cur.rowcount
        log.info("full-branch sweep: %d products marked out_of_stock, "
                 "%d ended promotions deactivated", swept, self.promos_deactivated)
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


def reap_all_stale_runs(older_than: str = STALE_RUN_AFTER) -> int:
    """Sweep EVERY branch's abandoned 'running' import_runs, not just the one
    about to be imported.

    Importer.reap_stale_runs() is deliberately scoped to a single branch so 300+
    parallel workers don't contend on the same rows — but that means a branch
    only self-heals when it is next imported, and a branch that stops being
    imported leaves its orphan 'running' row forever (45 of them by 2026-07-16,
    some 1-3 days old). This runs ONCE, in the parent process, before any worker
    starts, so there is no contention and no in-flight run to steal: the age gate
    means a row must have been untouched for `older_than` to be reaped.

    Marks them 'stale' (ingest.import_runs.status has no CHECK constraint, so
    that is a legal value) rather than deleting anything.
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log.error("DATABASE_URL is not set — cannot reap stale runs.")
        return -1
    with db_connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ingest.import_runs SET status = 'stale', finished_at = now(), "
                "error_log = COALESCE(error_log, '') || "
                "  '[reaper] still ''running'' after ' || %s || '; process died without "
                "finishing (killed / OOM / lost connection)' "
                "WHERE status = 'running' AND started_at < now() - %s::interval",
                (older_than, older_than))
            n = cur.rowcount
        conn.commit()
    if n:
        log.warning("reaped %d stale 'running' import_run(s) older than %s "
                    "(marked status='stale')", n, older_than)
    else:
        log.info("no stale 'running' import_runs older than %s", older_than)
    return n


def _same_product_name(a: Optional[str], b: Optional[str]) -> bool:
    """Are these two product names the same item, ignoring punctuation/spacing/case?

    Used only to decide whether a NEW barcode on a known product is a second
    barcode for the same item or a sign the identity is wrong. Deliberately
    strict-ish and conservative: an empty/unknown name on either side returns
    False, which routes to human review rather than silently accepting.
    """
    def norm(s: Optional[str]) -> str:
        return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()
    na, nb = norm(a), norm(b)
    return bool(na) and na == nb


def _history_events(price_changed: bool, unit_changed: bool, stock_changed: bool,
                    promo_changed: bool) -> list[str]:
    """Which events a price_history row represents, most significant first.

    A single observation can carry several at once (a product going on special
    usually moves price AND promo together), so all of them are recorded and
    `events[0]` is the one a chart should filter on. Order is deliberate:
    price_change outranks the rest so price charts stay clean.
    """
    events = []
    if price_changed:
        events.append("price_change")
    if unit_changed:
        events.append("unit_price_change")
    if stock_changed:
        events.append("stock_change")
    if promo_changed:
        events.append("promo_change")
    return events


def _importer_branch_stats(imp) -> dict:
    """Snapshot the per-branch importer counters for the JSON run log."""
    return {
        "products_inserted": imp.inserted,
        "products_seen": imp.updated,
        "new_products": imp.new_products,
        "price_changes": imp.price_changes,
        "matched": imp.matched,
        "canonicals_created": imp.canonicals_created,
        "variants_created": imp.variants_created,
        "barcodes_linked": imp.identifiers_created,
        "barcodes_refreshed": imp.identifiers_touched,
        "promos_deactivated": imp.promos_deactivated,
        "barcodes_aliased": imp.barcodes_aliased,
        "barcode_conflicts": imp.barcode_conflicts,
        "failed_rows": imp.failed,
        "sku_conflicts": imp.sku_conflicts,
        "rows_written": dict(imp.rows_written),
        "rows_skipped": dict(imp.rows_skipped),
        "stage_seconds": {k: round(v, 1) for k, v in imp.stage_times.items()},
    }


class ImporterRunLog:
    """Accumulates per-branch importer stats for one command (single branch or a
    full --input-dir chain) and writes ONE JSON file + index entry at the end.
    Mirrors the scraper's ScraperRunLog schema (kind='importer'). Best-effort:
    when the shared run_log module isn't importable, finish() just no-ops."""

    def __init__(self, retailer: str, mode: str, total_branches: int = 0):
        self.retailer = retailer
        self.mode = mode
        self.total_branches = total_branches
        self._t0 = time.time()
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.branches: list[dict] = []
        # Fixed filename for the whole run — rewritten after each branch so an
        # interrupted or in-flight chain import still has a JSON on disk.
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.filename = f"importer_{self.retailer}_{stamp}.json"

    def _doc(self, status: str) -> dict:
        ok = sum(1 for b in self.branches if b["status"] == "success")
        failed = sum(1 for b in self.branches if b["status"] != "success")
        totals: dict[str, float] = {}
        for b in self.branches:
            for k, v in (b.get("stats") or {}).items():
                if isinstance(v, bool):
                    continue
                if isinstance(v, (int, float)):
                    totals[k] = totals.get(k, 0) + v
        return {
            "kind": "importer",
            "chain": self.retailer,
            "retailer": self.retailer,
            "mode": self.mode,
            "status": status,  # "running" until the whole run completes, then "complete"
            "started_at": self.started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(time.time() - self._t0, 1),
            "branches_total": self.total_branches or len(self.branches),
            "branches_done": len(self.branches),
            "branches_ok": ok,
            "branches_failed": failed,
            "totals": totals,
            "branches": self.branches,
        }

    def _flush(self, status: str):
        if _run_log is None:
            return None
        return _run_log.write_run(self._doc(status), self.filename)

    def add_branch(self, *, branch_slug, status, duration_seconds, stats, error=None):
        self.branches.append({
            "branch_slug": branch_slug,
            "status": status,
            "duration_seconds": round(float(duration_seconds), 1),
            "stats": stats,
            "error": error,
        })
        self._flush("running")  # flush after every branch for live progress

    def finish(self):
        return self._flush("complete")


def run_db(args, records: list[dict], rollback: bool) -> int:
    import psycopg
    _t0 = time.time()

    def _record_branch(status, error=None, have_imp=True):
        rl = getattr(args, "_runlog", None)
        if rl is None:
            return
        slug = (args.branch_slug
                or (args.input.stem if getattr(args, "input", None) else None)
                or args.retailer)
        stats = _importer_branch_stats(imp) if have_imp else {}
        rl.add_branch(branch_slug=slug, status=status,
                      duration_seconds=time.time() - _t0, stats=stats, error=error)

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
        _record_branch("failed",
                       error=f"aborted: {frac*100:.1f}% rows failed validation",
                       have_imp=False)
        return 3

    conn = db_connect(dsn)
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
        # Transient errors that leave the connection alive but abort the tx.
        # With parallelism removed these are infrastructure events, not
        # self-contention: a statement_timeout cancel, or the Supabase pooler
        # dropping us mid-batch. The deadlock/lock/unique entries stay as cheap
        # insurance in case another writer (a second importer run, the app)
        # touches the same rows. process_batch aborts and resets its side effects
        # atomically, so a batch is always safe to re-run from a clean slate.
        TRANSIENT = (psycopg.errors.DeadlockDetected,
                     psycopg.errors.LockNotAvailable,
                     psycopg.errors.QueryCanceled,
                     psycopg.errors.UniqueViolation)
        for batch in chunked(deduped, batch_size):
            before = (imp.inserted, imp.updated, imp.new_products, imp.matched)
            written_before = dict(imp.rows_written)
            skipped_before = dict(imp.rows_skipped)
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
            # Report what was actually WRITTEN, not how many records were looked
            # at. The old line printed the batch size as "updated" every time
            # (it counted records processed), which on a re-import is always the
            # full batch and says nothing about whether anything really changed.
            wrote = {t: n - written_before.get(t, 0) for t, n in imp.rows_written.items()
                     if n - written_before.get(t, 0) > 0}
            skipped = sum(n - skipped_before.get(t, 0) for t, n in imp.rows_skipped.items())
            detail = ", ".join(f"{t} {n}" for t, n in sorted(wrote.items())) or "none"
            log.info("[%d/%d] batch %s: %d rows updated (%s), %d unchanged/skipped, "
                     "%d new rows inserted, +%d new products, +%d newly matched",
                     done, total, label, sum(wrote.values()), detail, skipped,
                     imp.inserted - before[0],
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
        _record_branch("failed", error=str(exc)[:500])
        return 1
    finally:
        conn.close()

    summary_lines = [
        "=== IMPORT SUMMARY ===",
        f"  mode              : {run_type}{' (rolled back)' if rollback else ''}",
        # NB: `updated` counts records PROCESSED that matched an existing row —
        # not rows written. See the per-table breakdown below for real writes.
        f"  branch products   : inserted {imp.inserted}, seen/processed {imp.updated}",
        f"  new products      : {imp.new_products}",
        f"  price changes     : {imp.price_changes}",
        f"  matched           : {imp.matched}",
        f"  canonicals created: {imp.canonicals_created}",
        f"  variants created  : {imp.variants_created}",
        f"  barcodes linked   : {imp.identifiers_created}",
        f"  barcodes refreshed: {imp.identifiers_touched} (last_seen_at bumped)",
        f"  promos deactivated: {imp.promos_deactivated} (ended — no product still on them)",
        f"  extra barcodes kept: {imp.barcodes_aliased} (same name — added, nothing overwritten)",
        f"  barcode conflicts : {imp.barcode_conflicts} (different name — queued for review)",
        f"  reviews resolved  : {imp.reviews_resolved}",
        f"  review queued     : {imp.reviews_created}",
        f"  failed rows       : {imp.failed}",
        "  --- rows actually written (per table) ---",
        *[f"    {t:24} {imp.rows_written.get(t, 0):7d} updated, "
          f"{imp.rows_skipped.get(t, 0):7d} unchanged"
          for t in sorted(set(imp.rows_written) | set(imp.rows_skipped))],
        f"  freshness-only touches: {imp.unchanged_rows}",
        f"  sku conflicts     : {imp.sku_conflicts} (retailer_sku left unwritten, "
        f"already owned by another row)",
        "======================",
    ]
    if imp.stage_times:
        tot = sum(imp.stage_times.values())
        summary_lines.append(f"  --- stage profile (total {tot:.1f}s) ---")
        for k, v in sorted(imp.stage_times.items(), key=lambda kv: -kv[1]):
            summary_lines.append(f"    {k:24} {v:7.1f}s  {v / tot * 100:5.1f}%")
        summary_lines.append("======================")
    for line in summary_lines:
        log.info(line)
    _record_branch("success")
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
    p.add_argument("--reap-stale-only", action="store_true",
                   help="mark every abandoned 'running' import_run older than "
                        f"{STALE_RUN_AFTER} as 'stale', then exit without importing "
                        "anything. Use to clean up orphans left by killed/OOMed runs "
                        "on branches that aren't being re-imported. (--input-dir does "
                        "this sweep automatically before it starts; --no-reap skips it.)")
    p.add_argument("--no-reap", action="store_true",
                   help="skip the chain-wide stale-run sweep that --input-dir "
                        "normally performs before importing.")
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

    # One chain-wide sweep in the PARENT, before any worker connects: clears
    # orphan 'running' rows for branches that aren't in this run at all (the
    # per-branch reaper in start_run only heals branches it actually imports).
    if not (args.dry_run or args.no_reap):
        try:
            reap_all_stale_runs()
        except Exception:
            log.exception("stale-run sweep failed — continuing with the import anyway")

    log.info("=== full-chain import: %d branches found for retailer=%s ===",
             len(branches), args.retailer)
    ordered = sorted(branches.items())
    ok, failed = [], []
    args._runlog = None if args.dry_run else ImporterRunLog(args.retailer, "all-branches", len(branches))

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

    log.info("=== full-chain import done: %d/%d branches OK ===", len(ok), len(branches))
    if failed:
        log.error("FAILED branches (%d): %s", len(failed), ", ".join(failed))
        log.error("re-run just those with --input <file> --branch-slug <slug> --full-branch")
    if args._runlog is not None:
        log_path = args._runlog.finish()
        if log_path:
            log.info("[runlog] importer run log → %s", log_path)
    return 0 if not failed else 1


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%H:%M:%S")
    args = parse_args(argv)

    if args.reap_stale_only:
        return 0 if reap_all_stale_runs() >= 0 else 2

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
    args._runlog = None if args.dry_run else ImporterRunLog(args.retailer, "single-branch", 1)
    rc = _run_one_file(args, args.input, None)
    if args._runlog is not None:
        log_path = args._runlog.finish()
        if log_path:
            log.info("[runlog] importer run log → %s", log_path)
    return rc


if __name__ == "__main__":
    sys.exit(main())
