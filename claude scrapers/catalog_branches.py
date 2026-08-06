"""Branch resolution against pico-prod's `catalog` schema (Stage-1 helper).

The grocery scrapers used to read a dedicated Supabase project's flat
`store_chains` / `store_branches` tables over the REST API. That project was
retired (its host no longer resolves), so the single source of truth for
branches is now pico-prod's `catalog` schema, reached over the Postgres pooler
(`DATABASE_URL`) — the same connection the importer already uses.

This module is strictly READ-ONLY: it resolves *which* store to scrape (id,
name, and the chain-specific external store id). It never writes — the importer
owns all catalog writes.

Tables:
  catalog.retailers(id, slug, name)
  catalog.branches(id, retailer_id, slug, name, is_active, ...)
  catalog.external_store_ids(branch_id, source_name, external_id)

The external store id the scraper pins to differs by chain:
  new-world / paknsave -> 'foodstuffs_api_store_id'  (POSTed as Algolia storeId)
  woolworths           -> 'woolworths_store_id'      (importer-only; WW pins via
                                                       its saved browser session)
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

# Which external_store_ids.source_name holds the id each chain's scraper pins to.
STORE_ID_SOURCE = {
    "new-world": "foodstuffs_api_store_id",
    "paknsave": "foodstuffs_api_store_id",
    "woolworths": "woolworths_store_id",
}

# Repo-root .env (../.env relative to this "claude scrapers/" dir), same file the
# scrapers already load. DATABASE_URL lives here (importer pooler DSN).
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


def _dsn() -> str:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        try:
            from dotenv import load_dotenv
            load_dotenv()            # cwd/.env, if any
            load_dotenv(_ENV_PATH)   # repo .env
        except Exception:
            pass
        dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError(f"DATABASE_URL not set (checked environment and {_ENV_PATH})")
    return dsn


def _connect():
    """Short-lived connection. Callers open/close per resolution — cheap, and safe
    under the scrapers' per-branch executor threads (no shared connection)."""
    dsn = _dsn()
    try:
        import psycopg  # psycopg3 (server + home venv)
        return psycopg.connect(dsn, connect_timeout=15)
    except ImportError:
        import psycopg2
        return psycopg2.connect(dsn, connect_timeout=15)


def _rows(cur) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


_BASE_SELECT = """
    select b.id::text        as id,
           b.name            as name,
           b.slug            as slug,
           b.retailer_id::text as retailer_id,
           b.is_active       as is_active,
           (select e.external_id from catalog.external_store_ids e
             where e.branch_id = b.id and e.source_name = %s
             limit 1)        as api_store_id
      from catalog.branches b
      join catalog.retailers r on r.id = b.retailer_id
     where r.slug = %s
"""


def list_branches(retailer_slug: str, *, active_only: bool = True,
                  require_store_id: bool = False) -> list[dict]:
    """All branches for a retailer as [{id, name, slug, retailer_id, is_active,
    api_store_id}], ordered by name. `id`/`retailer_id` are stringified UUIDs;
    `api_store_id` is the chain's external store id (None if the branch has none).

    With `require_store_id`, branches lacking that id are dropped — mirrors the
    old `[b for b in all_branches if b.get("api_store_id")]`.
    """
    source = STORE_ID_SOURCE.get(retailer_slug)
    sql = _BASE_SELECT + (" and b.is_active = true" if active_only else "") + " order by b.name"
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(sql, (source, retailer_slug))
        out = _rows(cur)
    finally:
        conn.close()
    if require_store_id:
        out = [b for b in out if b.get("api_store_id")]
    return out


def get_branch(retailer_slug: str, *, branch_id: Optional[str] = None,
               name: Optional[str] = None) -> Optional[dict]:
    """Resolve ONE branch by id (preferred) or case-insensitive name. Returns
    {id, name, slug, retailer_id, is_active, api_store_id} or None.

    A name can match more than one row (e.g. a duplicate 'PAK'nSAVE Sylvia Park'
    where only one carries an api_store_id) — we prefer the row that HAS the
    store id, then the active one.
    """
    source = STORE_ID_SOURCE.get(retailer_slug)
    if branch_id:
        sql = _BASE_SELECT + " and b.id::text = %s"
        params = (source, retailer_slug, branch_id)
    elif name:
        sql = _BASE_SELECT + " and lower(b.name) = lower(%s)"
        params = (source, retailer_slug, name)
    else:
        return None
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = _rows(cur)
    finally:
        conn.close()
    if not rows:
        return None
    rows.sort(key=lambda b: (b.get("api_store_id") is None, not b.get("is_active")))
    return rows[0]


if __name__ == "__main__":  # quick manual check: python catalog_branches.py [slug]
    import sys
    slug = sys.argv[1] if len(sys.argv) > 1 else "new-world"
    bs = list_branches(slug, active_only=True)
    withid = [b for b in bs if b.get("api_store_id")]
    print(f"{slug}: {len(bs)} active branches, {len(withid)} with {STORE_ID_SOURCE.get(slug)}")
    for b in bs[:5]:
        print(f"  {b['name']}  id={b['id']}  api_store_id={b['api_store_id']}")
