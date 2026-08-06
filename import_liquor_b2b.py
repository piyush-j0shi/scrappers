"""Liquor → Pico B2B importer (RPC-only ingestion contract).

Target: Supabase project `pico-b2b` (yobfbsfiyxqpoajfihvb). UNLIKE the pico-prod
importer (import_products.py, direct Postgres writes), Pico B2B is written to
**only through public scraper RPC functions** — the DB resolves product identity,
offers, promotions and identifiers itself from staged raw observations.

Worker sequence (doc §3.2):
    scraper_enqueue_job -> scraper_claim_next_job -> scraper_start_run
    -> scraper_stage_observation_batch (<=1000 items, 500 default) [xN]
    -> scraper_process_pending_run_items (repeat until remaining=0)
    -> scraper_finish_run(status, catalogue_complete)

The DB derives branch/retailer from the claimed job/run, so branch IDs are NOT
sent per row — one run == one connector_branch.

Environment (doc §3.1):
    PICO_B2B_SUPABASE_URL=https://yobfbsfiyxqpoajfihvb.supabase.co
    PICO_B2B_SUPABASE_SERVICE_ROLE_KEY=<provided securely by Pico>   # never commit
    PICO_SCRAPER_WORKER_ID=<stable worker name>

Usage:
    python import_liquor_b2b.py --input exports/superliquor_super-liquor-hobsonville_*.jsonl --dry-run
    python import_liquor_b2b.py --input <file> --live          # needs env creds

NOTE: RPC *param names* below are inferred from the doc's "Key input" column and
must be confirmed against the live function signatures (piyush_scraper_rpc_example.py
/ the /rest/v1/ OpenAPI spec) before the first live run. All are isolated in the
RPC section for a one-place fix. --dry-run touches no network.
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Connector-branch registry (doc §4). ONLY active + branch_context_verified=true
# production branches may receive real observations. TEST_* are mock connectors
# for dry-run wiring checks — never send them production data on a live run.
# ---------------------------------------------------------------------------
READY: dict[str, dict] = {
    # our-export-slug -> connector branch. IDs from the "Pico B2B Scraper Setup and
    # Run Checklist" quick-reference (7 Aug 2026); all verified live as active.
    "liquorland_liquorland-hobsonville":     {"connector_branch_id": "047c3a09-fcc7-43bd-bc14-3433d8a6f288", "connector_code": "liquorland_playwright_catalogue", "branch": "Liquorland Hobsonville"},
    "liquorland_liquorland-albany":          {"connector_branch_id": "7ea42301-4af1-4658-a911-9f78a0d00409", "connector_code": "liquorland_playwright_catalogue", "branch": "Liquorland Albany"},
    "liquorland_liquorland-west-harbour":    {"connector_branch_id": "bf098728-7d3e-47df-978d-6228c6ac29de", "connector_code": "liquorland_playwright_catalogue", "branch": "Liquorland West Harbour"},
    "liquorland_liquorland-glenfield":       {"connector_branch_id": "7a6ffc02-b1f5-44e1-ad94-9b9b00864f32", "connector_code": "liquorland_playwright_catalogue", "branch": "Liquorland Glenfield"},
    "superliquor_super-liquor-hobsonville":  {"connector_branch_id": "c4100f84-1c8d-4678-8a7c-7939c7893db4", "connector_code": "super_liquor_nopcommerce",       "branch": "Super Liquor Hobsonville"},
    "thebottleo_the-bottle-o-glenfield":     {"connector_branch_id": "7e2f26f6-d8e5-4238-bea7-5dc447dc1732", "connector_code": "bottleo_myfoodlink_branch",       "branch": "The Bottle-O Glenfield"},
    "thebottleo_the-bottle-o-schnapper-rock":{"connector_branch_id": "351731d2-4560-4ba4-9f93-cd04a3c76d7a", "connector_code": "bottleo_myfoodlink_branch",       "branch": "The Bottle-O Schnapper Rock"},
    "newworld_new-world-hobsonville":        {"connector_branch_id": "6a2c6545-d1bc-460e-9251-73dea00eaacd", "connector_code": "newworld_foodstuffs_alcohol",    "branch": "New World Hobsonville (alcohol)"},
    "woolworths_woolworths-hobsonville":     {"connector_branch_id": "1822fd30-d284-46d1-9bab-cb3a9d2624fb", "connector_code": "woolworths_api_alcohol",         "branch": "Woolworths Hobsonville (alcohol)"},
    "woolworths_woolworths-glenfield":       {"connector_branch_id": "0d0a6add-c711-4fac-900a-b8846a712d88", "connector_code": "woolworths_api_alcohol",         "branch": "Woolworths Glenfield (alcohol)"},
}
BLOCKED: set[str] = set()  # all 10 branch contexts verified live 2026-08-06
CONNECTOR_VERSION = "1.0.0"  # our deployed scraper/connector version string

_ML_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(ml|l)\b", re.I)


def dollars(cents) -> Optional[float]:
    return round(cents / 100, 2) if isinstance(cents, (int, float)) else None


def unit_volume_ml(size_text: Optional[str]) -> Optional[float]:
    if not size_text:
        return None
    m = _ML_RE.search(size_text)
    if not m:
        return None
    v = float(m.group(1))
    return v * 1000 if m.group(2).lower() == "l" else v


def idempotency_key(r: dict) -> str:
    """SHA-256 of the strongest stable source identity (doc §3.4)."""
    ident = (r.get("source_record_key") or r.get("source_product_id")
             or r.get("retailer_sku") or r.get("internal_sku")
             or r.get("product_url") or r.get("raw_name") or "")
    return hashlib.sha256(str(ident).encode("utf-8")).hexdigest()


# Attribute keys the liquor scrapers may carry (Bottle-O rich cards etc.).
_ATTR_KEYS = ("abv", "standard_drinks", "region", "closure", "country", "vintage",
              "varietal", "liquor_style", "bottled_in", "tasting_notes", "sub_category")


def to_observation(r: dict) -> dict:
    """Map one liquor JSONL record -> a Pico B2B observation object (doc §3.3)."""
    price = dollars(r.get("current_price_cents"))
    compare = dollars(r.get("comparison_price_cents"))

    identifiers = []
    bc = r.get("barcode")
    if bc:
        identifiers.append({"type": "barcode", "value": str(bc)})

    attributes = {}
    for k in _ATTR_KEYS:
        v = r.get(k)
        if v not in (None, ""):
            attributes[k] = v

    obs = {
        "idempotency_key": idempotency_key(r),
        "observed_at": r.get("observed_at") or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        # identity (source_record_key preferred, then these)
        "source_product_id": r.get("source_product_id"),
        "retailer_sku": r.get("retailer_sku"),
        "internal_sku": r.get("internal_sku"),
        "product_url": r.get("product_url"),
        # product
        "product_name": r.get("raw_name"),
        "normalized_name": r.get("clean_name"),
        "brand": r.get("brand"),
        "size_text": r.get("size"),
        "pack_quantity": r.get("quantity"),
        "unit_volume_ml": unit_volume_ml(r.get("size")),
        "image_url": r.get("image_url"),
        # categories
        "source_category": r.get("category_path"),
        "identifiers": identifiers,
        "attributes": attributes,
        # ordinary offer: single_item_price = ordinary one-bottle price;
        # compare_at_price = displayed was/RRP (doc §3.3 price rules)
        "offer": {
            "currency": "NZD",
            "single_item_price": price,
            "compare_at_price": compare,
            "stock_status": r.get("stock_status") or "in_stock",
            "price_scope": "branch",
            "source_url": r.get("product_url"),
            "tags": [],
        },
    }

    # promotion: a special price OR a multibuy (doc §3.3). Ordinary price stays on
    # the offer; the special/multibuy terms go here.
    promo = None
    if r.get("multibuy_quantity") and r.get("multibuy_price_cents"):
        promo = {
            "type": "multibuy",
            "label": r.get("promo_text"),
            "qualifying_quantity": int(r["multibuy_quantity"]),
            "bundle_total": dollars(r["multibuy_price_cents"]),
        }
    elif r.get("special") or r.get("promo_type") == "special" or r.get("discount"):
        promo = {
            # Liquorland "Everyday Value" carries dates but == shelf price (no markdown);
            # flagged as a special promotion with the observed price.
            "type": "special",
            "label": r.get("promo_text"),
            "starts_at": r.get("promo_starts_at"),
            "ends_at": r.get("promo_ends_at"),
            "source_promotion_id": r.get("source_promotion_id"),
            "single_promotional_price": price,
        }
    if promo:
        obs["promotion"] = {k: v for k, v in promo.items() if v is not None}

    # drop null top-level keys (keep offer/attributes/identifiers as-is)
    return {k: v for k, v in obs.items() if v is not None}


def export_slug(path: str) -> Optional[str]:
    """`{prefix}_{branch-slug}_{stamp}.jsonl` -> `{prefix}_{branch-slug}`."""
    m = re.match(r"^(.*)_\d{8}T\d{6}Z\.jsonl$", os.path.basename(path))
    return m.group(1) if m else None


def load_records(path: str) -> list[dict]:
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def batched(seq, n):
    for i in range(0, len(seq), n):
        yield i // n + 1, seq[i:i + n]


# ---------------------------------------------------------------------------
# RPC layer — LIVE only. Param names inferred from doc §3.2/§3.3 "Key input";
# CONFIRM against the live signatures before first production run.
# ---------------------------------------------------------------------------
def _load_env():
    try:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parent / ".env")
    except Exception:
        pass


def _client():
    _load_env()
    url = os.environ.get("PICO_B2B_SUPABASE_URL")
    key = os.environ.get("PICO_B2B_SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise SystemExit("error: PICO_B2B_SUPABASE_URL / PICO_B2B_SUPABASE_SERVICE_ROLE_KEY "
                         "not set — cannot run --live (service-role secret required).")
    from supabase import create_client
    return create_client(url, key)


def run_live(sb, worker_id: str, cb: dict, observations: list[dict],
             batch_size: int, complete: bool, process_limit: int = 200) -> None:
    dedupe = f"liquor-{cb['connector_branch_id']}-{datetime.now(timezone.utc):%Y%m%d}"
    def rpc(fn, params):
        return sb.rpc(fn, params).execute().data

    def pick(d, *keys):
        if isinstance(d, dict):
            for k in keys:
                if d.get(k):
                    return d[k]
        return d

    job = rpc("scraper_enqueue_job", {
        "p_connector_branch_id": cb["connector_branch_id"],
        "p_mode": "full_catalogue",
        "p_priority": 5,
        "p_initiating_source": "customer_refresh",
        "p_deduplication_key": dedupe,
        "p_metadata": {},
    })
    job_id = pick(job, "job_id", "id")
    print(f"  enqueued job -> {job}")

    claimed = rpc("scraper_claim_next_job", {"p_worker_id": worker_id})
    print(f"  claimed -> {claimed}")
    job_id = pick(claimed, "job_id", "id") or job_id

    run = rpc("scraper_start_run", {"p_job_id": job_id, "p_connector_version": CONNECTOR_VERSION})
    run_id = pick(run, "run_id", "id")
    print(f"  started run -> {run_id}")

    for bn, chunk in batched(observations, batch_size):
        res = rpc("scraper_stage_observation_batch", {
            "p_run_id": run_id, "p_batch_number": bn,
            "p_batch_idempotency_key": f"{dedupe}-b{bn}",
            "p_contains_priority_targets": False, "p_items": chunk,
        })
        print(f"  staged batch {bn}: {len(chunk)} items -> {res}")

    from postgrest.exceptions import APIError
    plimit = process_limit
    for _ in range(100000):
        try:
            p = rpc("scraper_process_pending_run_items", {"p_run_id": run_id, "p_limit": plimit})
        except APIError as e:
            # per-item resolution is heavy; a big p_limit can hit the DB statement
            # timeout (57014). Back off and keep draining.
            if ("timeout" in str(e).lower() or "57014" in str(e)) and plimit > 25:
                plimit = max(25, plimit // 2)
                print(f"  process timed out -> reducing p_limit to {plimit}")
                continue
            raise
        print(f"  process -> {p}")
        remaining = p.get("remaining") if isinstance(p, dict) else None
        if not remaining:
            break

    fin = rpc("scraper_finish_run", {
        "p_run_id": run_id,
        "p_status": "completed" if complete else "partial",
        "p_catalogue_complete": bool(complete),
        "p_observed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "p_summary": {"items": len(observations)},
    })
    print(f"  finished run -> {fin}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Liquor -> Pico B2B RPC importer")
    ap.add_argument("--input", required=True, help="JSONL export path (glob ok)")
    ap.add_argument("--batch-size", type=int, default=500, help="observations per batch (<=1000)")
    ap.add_argument("--process-limit", type=int, default=200,
                    help="items per scraper_process_pending_run_items call; auto-halves on DB timeout (default 200)")
    ap.add_argument("--dry-run", action="store_true", default=True,
                    help="build + validate observations, print samples, NO network (default)")
    ap.add_argument("--live", dest="dry_run", action="store_false",
                    help="actually call the Pico B2B RPCs (needs env creds + verified branch)")
    ap.add_argument("--allow-blocked", action="store_true",
                    help="(dry-run only) build payloads for a not-yet-verified branch")
    ap.add_argument("--limit", type=int, default=None,
                    help="only the first N observations — forces partial / catalogue_complete=false "
                         "(use for a safe single-/few-product live smoke test)")
    args = ap.parse_args(argv)

    paths = sorted(glob.glob(os.path.expanduser(args.input)))
    if not paths:
        print(f"error: no files match {args.input}", file=sys.stderr); return 2

    for path in paths:
        slug = export_slug(path)
        cb = READY.get(slug)
        print(f"\n=== {os.path.basename(path)} ===")
        print(f"  export slug: {slug}")
        if not cb:
            state = "BLOCKED (branch context not verified)" if slug in BLOCKED else "UNKNOWN (no connector mapping)"
            print(f"  connector branch: {state}")
            if not args.dry_run:
                print("  -> refusing to import to an unverified/unknown branch. Skipping."); continue
            if not args.allow_blocked:
                print("  -> dry-run skip (pass --allow-blocked to build payloads anyway)."); continue

        records = load_records(path)
        obs = [to_observation(r) for r in records]
        if args.limit:
            obs = obs[:args.limit]
        priced = sum(1 for o in obs if o["offer"]["single_item_price"] is not None)
        promos = sum(1 for o in obs if "promotion" in o)
        multibuy = sum(1 for o in obs if o.get("promotion", {}).get("type") == "multibuy")
        barcodes = sum(1 for o in obs if o["identifiers"])
        no_name = sum(1 for o in obs if not o.get("product_name"))
        nbatches = (len(obs) + args.batch_size - 1) // args.batch_size
        print(f"  observations: {len(obs)}  (priced {priced}, promo {promos} [multibuy {multibuy}], "
              f"barcodes {barcodes}, missing name {no_name})")
        print(f"  batches: {nbatches} x {args.batch_size}")
        if cb:
            print(f"  -> connector branch: {cb['branch']}  ({cb['connector_code']} / {cb['connector_branch_id']})")

        if args.dry_run:
            sample = next((o for o in obs if "promotion" in o), obs[0] if obs else None)
            if sample:
                print("  --- sample observation ---")
                print("  " + json.dumps(sample, indent=2, ensure_ascii=False)[:1400].replace("\n", "\n  "))
        else:
            worker = os.environ.get("PICO_SCRAPER_WORKER_ID") or "piyush-liquor-importer"
            run_live(_client(), worker, cb, obs, args.batch_size,
                     complete=(no_name == 0 and not args.limit),
                     process_limit=args.process_limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
