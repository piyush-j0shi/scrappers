"""Structured per-run JSON logs for the scrapers (and, mirrored, the importer).

One JSON file per run under ``<repo>/logs/``, plus a rolling ``runs-index.json``
that a frontend can list from (newest first). Everything here is best-effort:
a logging failure must never bring a scrape/import down, so every public call
swallows its own exceptions.

Schema (one scraper run file)::

    {
      "kind": "scraper",
      "chain": "woolworths",
      "mode": "all-branches",
      "started_at": "ISO-8601 UTC",
      "finished_at": "ISO-8601 UTC",
      "duration_seconds": 12345.6,
      "branches_total": 186,
      "branches_ok": 184,
      "branches_partial": 1,
      "branches_failed": 1,
      "totals": {"products": ..., "promo": ..., "multibuy": ..., "out_of_stock": ...},
      "branches": [
        {
          "branch_name": "Woolworths Ponsonby",
          "branch_slug": "woolworths-ponsonby",
          "status": "success",            # success | partial | failed
          "duration_seconds": 42.1,
          "totals": {"products": 8200, "promo": 640, "multibuy": 95, "out_of_stock": 210},
          "categories": [
            {"name": "fruit-veg", "status": "success",
             "products": 420, "promo": 55, "multibuy": 8, "out_of_stock": 12,
             "reason": null},
            ...
          ],
          "error": null
        }, ...
      ]
    }
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# <repo>/logs — run_log.py lives in "<repo>/claude scrapers/", so parent.parent
# is the repo root, matching jsonl_export's exports/ dir alongside it.
LOGS_DIR = Path(__file__).resolve().parent.parent / "logs"
INDEX_NAME = "runs-index.json"
INDEX_CAP = 500  # keep only the newest N runs in the index (files are never pruned)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def count_category(products) -> dict:
    """Per-category product-type tallies from a list of scraped products.

    Works for both scrapers' ``ScrapedProduct`` (identical fields): a product is
    ``promo`` if it carries a special price, ``multibuy`` if it has a multibuy
    threshold, ``out_of_stock`` if not in stock. Uses getattr so a partial/odd
    object can never crash the tally.
    """
    promo = multibuy = oos = 0
    for p in products:
        if getattr(p, "special_price", None) is not None:
            promo += 1
        if getattr(p, "multibuy_quantity", None):
            multibuy += 1
        if not getattr(p, "in_stock", True):
            oos += 1
    return {"promo": promo, "multibuy": multibuy, "out_of_stock": oos}


def category_record(name: str, status: str, products, reason: Optional[str] = None) -> dict:
    """Build one enriched category_results entry (count + status + breakdown)."""
    rec = {"name": name, "status": status, "products": len(products)}
    rec.update(count_category(products))
    if reason is not None:
        rec["reason"] = reason
    return rec


def _write_index(logs_dir: Path, entry: dict) -> None:
    """Upsert ONE summary entry per run into runs-index.json (keyed by "file"),
    newest first, under an exclusive file lock (scraper and importer runs can
    overlap, and the same run flushes repeatedly as branches complete). Atomic
    replace so a reader never sees a half-written index."""
    try:
        import fcntl
    except Exception:
        fcntl = None
    idx_path = logs_dir / INDEX_NAME
    lock_path = logs_dir / (INDEX_NAME + ".lock")
    lock = lock_path.open("w")
    try:
        if fcntl is not None:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX)
            except Exception:
                pass
        data = []
        if idx_path.exists():
            try:
                data = json.loads(idx_path.read_text(encoding="utf-8") or "[]")
                if not isinstance(data, list):
                    data = []
            except Exception:
                data = []
        # one entry per run: drop any prior entry for this run's file, re-insert
        # at the front so an in-progress run keeps its latest counts + position.
        data = [e for e in data if e.get("file") != entry.get("file")]
        data.insert(0, {**entry, "indexed_at": _now_iso()})
        data = data[:INDEX_CAP]
        tmp = idx_path.with_name(INDEX_NAME + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(idx_path)
    finally:
        try:
            if fcntl is not None:
                fcntl.flock(lock, fcntl.LOCK_UN)
        except Exception:
            pass
        lock.close()


def write_run(doc: dict, filename: str, logs_dir: Optional[Path] = None) -> Optional[Path]:
    """Write one run doc to ``logs/<filename>`` and index it. Never raises."""
    logs_dir = Path(logs_dir) if logs_dir else LOGS_DIR
    try:
        logs_dir.mkdir(parents=True, exist_ok=True)
        path = logs_dir / filename
        path.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
        _write_index(logs_dir, {
            "kind": doc.get("kind"),
            "chain": doc.get("chain") or doc.get("retailer"),
            "file": filename,
            "status": doc.get("status"),
            "started_at": doc.get("started_at"),
            "finished_at": doc.get("finished_at"),
            "duration_seconds": doc.get("duration_seconds"),
            "branches_total": doc.get("branches_total"),
            "branches_done": doc.get("branches_done"),
            "branches_ok": doc.get("branches_ok"),
            "branches_failed": doc.get("branches_failed"),
            "totals": doc.get("totals"),
        })
        return path
    except Exception as e:
        logger.warning("[runlog] failed to write run log %s: %s", filename, e)
        return None


class ScraperRunLog:
    """Accumulates per-branch results for one scraper process, then writes one
    JSON file + index entry when the whole run finishes."""

    def __init__(self, chain: str, mode: str = "full", total_branches: int = 0,
                 logs_dir: Optional[Path] = None):
        self.chain = chain
        self.mode = mode
        self.total_branches = total_branches
        self.started_at = _now_iso()
        self._t0 = time.time()
        self.branches: list[dict] = []
        self.logs_dir = Path(logs_dir) if logs_dir else LOGS_DIR
        self.run_stamp = _run_stamp()
        # Fixed filename for the whole run — the same file is rewritten after each
        # branch so an interrupted or in-flight run still has a JSON on disk.
        self.filename = f"scraper_{self.chain}_{self.run_stamp}.json"

    def _doc(self, status: str, overall: Optional[dict]) -> dict:
        ok = sum(1 for b in self.branches if b["status"] == "success")
        partial = sum(1 for b in self.branches if b["status"] == "partial")
        failed = sum(1 for b in self.branches if b["status"] not in ("success", "partial"))
        totals = {"products": 0, "promo": 0, "multibuy": 0, "out_of_stock": 0}
        for b in self.branches:
            for k in totals:
                totals[k] += (b.get("totals") or {}).get(k, 0)
        return {
            "kind": "scraper",
            "chain": self.chain,
            "mode": self.mode,
            "status": status,  # "running" until the whole run completes, then "complete"
            "started_at": self.started_at,
            "finished_at": _now_iso(),
            "duration_seconds": round(time.time() - self._t0, 1),
            "branches_total": self.total_branches or len(self.branches),
            "branches_done": len(self.branches),
            "branches_ok": ok,
            "branches_partial": partial,
            "branches_failed": failed,
            "totals": totals,
            "overall": overall or {},
            "branches": self.branches,
        }

    def add_branch(self, *, branch_name: Optional[str], branch_slug: Optional[str],
                   status: str, duration_seconds: float, categories: list[dict],
                   totals: Optional[dict] = None, error: Optional[str] = None) -> None:
        if totals is None:
            totals = {
                "products": sum(c.get("products", 0) for c in categories),
                "promo": sum(c.get("promo", 0) for c in categories),
                "multibuy": sum(c.get("multibuy", 0) for c in categories),
                "out_of_stock": sum(c.get("out_of_stock", 0) for c in categories),
            }
        self.branches.append({
            "branch_name": branch_name,
            "branch_slug": branch_slug,
            "status": status,
            "duration_seconds": round(float(duration_seconds), 1),
            "totals": totals,
            "categories": categories,
            "error": error,
        })
        # Flush after every branch so the file reflects progress live.
        write_run(self._doc("running", None), self.filename, self.logs_dir)

    def finish(self, overall: Optional[dict] = None) -> Optional[Path]:
        return write_run(self._doc("complete", overall), self.filename, self.logs_dir)
