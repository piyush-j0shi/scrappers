"""Shared JSONL export for the Scraper Data Contract.

Stage 1 of the two-stage pipeline: a scraper produces ONE JSONL file per
retailer branch per completed run. Stage 2 (pico-prod/import_products.py) reads
that file and owns all database writes. The scraper must NOT write to the DB.

One line = one product observation. Field names follow the contract
(source_product_id, current_price_cents, comparison_price_cents, unit_label,
promo_type, card_required, required_loyalty_program_code, category_path,
observed_at, raw_row, ...). Money is integer cents. Timestamps are UTC ISO-8601.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Default output directory: <repo>/exports
EXPORT_DIR = Path(__file__).resolve().parent.parent / "exports"

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slug(text: Optional[str]) -> str:
    # Drop apostrophes/quotes ENTIRELY before hyphenating so the slug matches the
    # canonical branch slug in catalog.branches: "PAK'nSAVE Sylvia Park" ->
    # "paknsave-sylvia-park" (NOT "pak-nsave-sylvia-park", which fails branch
    # resolution in the importer). Everything else non-alnum collapses to "-".
    cleaned = (text or "").lower().replace("'", "").replace("’", "")
    s = _SLUG_RE.sub("-", cleaned).strip("-")
    return s or "unknown"


def export_filename(scraper_name: str, branch_label: Optional[str],
                    when: Optional[datetime] = None) -> str:
    """`{scraper}_{branch}_{UTC timestamp}.jsonl` — one file per branch+run."""
    when = when or datetime.now(timezone.utc)
    stamp = when.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{_slug(scraper_name)}_{_slug(branch_label)}_{stamp}.jsonl"


def write_jsonl(scraper_name: str, branch_label: Optional[str],
                records: list[dict], out_dir: Optional[Path] = None,
                when: Optional[datetime] = None) -> Optional[Path]:
    """Write observations to one JSONL file. Returns the path (or None if empty)."""
    if not records:
        logger.warning("[jsonl] no records to export — skipping file")
        return None
    out_dir = out_dir or EXPORT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / export_filename(scraper_name, branch_label, when)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")
    logger.info(f"[jsonl] wrote {len(records)} observations → {path}")
    return path


def to_cents(value: Optional[float]) -> Optional[int]:
    """Dollars (float) → integer cents. None-safe."""
    if value is None:
        return None
    try:
        return int(round(float(value) * 100))
    except (TypeError, ValueError):
        return None


def clean_record(rec: dict) -> dict:
    """Drop keys whose value is None so the file uses nulls only where meaningful
    inside raw_row; top-level absent keys are simply omitted."""
    return {k: v for k, v in rec.items() if v is not None}
