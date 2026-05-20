"""
Woolworths session bootstrapper.

Woolworths NZ binds the selected pickup store to the session cookies
(ASP.NET_SessionId + browserSessionId). There is no storeId query param or
header override on /api/v1/products — every call returns whatever store the
session is pinned to server-side.

Follow-up investigation (see investigate_woolworths_v9.py) found the exact API
to pin a store WITHOUT driving the UI:

    1. GET  /                                              # seed cookies (incl. XSRF-TOKEN)
    2. PUT  /api/v1/fulfilment/my/methods/pickup  body:{}  # flip to pickup mode
    3. PUT  /api/v1/fulfilment/my/pickup-addresses
            body: {"addressId": <api_store_id>}            # pin this specific store
    4. GET  /api/v1/products?...                           # now returns THIS store's prices

    The server signals which store is pinned by rewriting the cw-lrkswrdjp
    cookie's f- field to the fulfilmentStoreId. Different branches produce
    different f- values (e.g. Birkenhead=9101, Invercargill=9010).

    The XSRF-TOKEN cookie value MUST be sent back as header `x-xsrf-token`;
    without it, the PUT returns 400.

    `addressId` in this API = the same integer as store_branches.api_store_id
    in our Supabase schema (confirmed: Ponsonby = 1996677).

This script writes one Playwright storage_state per branch to:
    scrapers/sessions/woolworths/<branch_id>.json
plus a small index at:
    scrapers/sessions/woolworths/_index.json

The existing Woolworths scraper can later load a storage_state, skip the
bootstrap, and scrape that branch's prices.

Usage:
    python3 bootstrap_woolworths_sessions.py                         # all branches, 3 workers
    python3 bootstrap_woolworths_sessions.py --workers 5
    python3 bootstrap_woolworths_sessions.py --branch-id <uuid>      # single branch
    python3 bootstrap_woolworths_sessions.py --skip-existing         # skip fresh (<2h) sessions
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, BrowserContext, Page
from supabase import create_client, Client

# Import stealth constants from base_scraper — never modify base_scraper itself.
from base_scraper import USER_AGENT, EXTRA_HEADERS, STEALTH_SCRIPT
from config import SUPABASE_URL, SUPABASE_SERVICE_KEY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("bootstrap")

SESSIONS_DIR = Path(__file__).parent / "sessions" / "woolworths"
INDEX_PATH = SESSIONS_DIR / "_index.json"
SESSION_MAX_AGE = timedelta(hours=2)

WOOLWORTHS_BASE = "https://www.woolworths.co.nz"
HOMEPAGE = f"{WOOLWORTHS_BASE}/"
PUT_PICKUP_URL = f"{WOOLWORTHS_BASE}/api/v1/fulfilment/my/methods/pickup"
PUT_PICKUP_ADDRESS_URL = f"{WOOLWORTHS_BASE}/api/v1/fulfilment/my/pickup-addresses"
PRODUCTS_VERIFY_URL = (
    f"{WOOLWORTHS_BASE}/api/v1/products?"
    "dasFilter=Department%3B%3Bfruit-veg%3Bfalse&target=browse&inStockProductsOnly=false&size=48"
)


@dataclass
class Branch:
    id: str
    name: str
    api_store_id: int


# ---------------------------------------------------------------------------
# Index helpers. Access is serialised through an asyncio.Lock so parallel
# workers don't trample each other's writes.
# ---------------------------------------------------------------------------

_index_lock = asyncio.Lock()


def _load_index() -> dict:
    if INDEX_PATH.exists():
        try:
            return json.loads(INDEX_PATH.read_text())
        except Exception as e:
            logger.warning(f"Could not parse {INDEX_PATH}: {e}; rebuilding")
    return {}


def _write_index_atomic(index: dict) -> None:
    tmp = INDEX_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(index, indent=2, sort_keys=True))
    os.replace(tmp, INDEX_PATH)


async def _update_index(branch_id: str, entry: dict) -> None:
    async with _index_lock:
        idx = _load_index()
        idx[branch_id] = entry
        _write_index_atomic(idx)


# ---------------------------------------------------------------------------
# Supabase access
# ---------------------------------------------------------------------------

def fetch_branches(supabase: Client, branch_id: Optional[str] = None) -> list[Branch]:
    chain = (
        supabase.table("store_chains").select("id").eq("slug", "woolworths").limit(1).execute()
    )
    if not chain.data:
        raise RuntimeError("No Woolworths chain row found in store_chains")
    chain_id = chain.data[0]["id"]

    q = (
        supabase.table("store_branches")
        .select("id,name,api_store_id")
        .eq("chain_id", chain_id)
        .not_.is_("api_store_id", "null")
    )
    if branch_id:
        q = q.eq("id", branch_id)
    rows = q.execute().data or []

    branches: list[Branch] = []
    for r in rows:
        try:
            branches.append(Branch(id=r["id"], name=r["name"], api_store_id=int(r["api_store_id"])))
        except (TypeError, ValueError):
            logger.warning(f"Skipping branch {r.get('id')} — api_store_id not int: {r.get('api_store_id')!r}")
    return branches


# ---------------------------------------------------------------------------
# Session bootstrap for one branch
# ---------------------------------------------------------------------------

async def _extract_xsrf(ctx: BrowserContext) -> Optional[str]:
    for c in await ctx.cookies():
        if c["name"] == "XSRF-TOKEN":
            return c["value"]
    return None


async def _get_cookie(ctx: BrowserContext, name: str) -> Optional[str]:
    for c in await ctx.cookies():
        if c["name"] == name:
            return c["value"]
    return None


async def _wait_for_cookie_change(
    ctx: BrowserContext, name: str, initial: Optional[str], timeout_s: float = 5.0
) -> Optional[str]:
    """Poll context cookies every 500ms until `name` differs from `initial` or timeout."""
    elapsed = 0.0
    current = await _get_cookie(ctx, name)
    while elapsed < timeout_s:
        if current is not None and current != initial:
            return current
        await asyncio.sleep(0.5)
        elapsed += 0.5
        current = await _get_cookie(ctx, name)
    return current


async def bootstrap_branch(browser, branch: Branch) -> dict:
    """Drive the pickup-method API for one branch and persist the session.

    Returns an index entry dict describing the outcome.
    """
    log = logging.getLogger(f"branch:{branch.name}")
    start = datetime.now(timezone.utc)

    ctx: Optional[BrowserContext] = None
    try:
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="en-NZ",
            timezone_id="Pacific/Auckland",
            extra_http_headers=EXTRA_HEADERS,
        )
        await ctx.add_init_script(STEALTH_SCRIPT)
        page: Page = await ctx.new_page()
        page.set_default_timeout(60_000)

        # Step 1 — load homepage so Akamai/Cloudflare issue a session + XSRF-TOKEN.
        log.info("loading homepage to seed session")
        await page.goto(HOMEPAGE, wait_until="load", timeout=60_000)
        await page.wait_for_timeout(3000)

        xsrf = await _extract_xsrf(ctx)
        if not xsrf:
            raise RuntimeError("XSRF-TOKEN cookie missing after homepage load")

        common_headers = {
            "content-type": "application/json",
            "accept": "application/json, text/plain, */*",
            "x-requested-with": "OnlineShopping.WebApp",
            "x-ui-ver": "7.73.30",
            "x-xsrf-token": xsrf,
            "referer": HOMEPAGE,
        }

        initial_pin_cookie = await _get_cookie(ctx, "cw-lrkswrdjp")
        log.info(f"initial cw-lrkswrdjp before PUT 1: {initial_pin_cookie!r}")

        # Step 2 — switch session to pickup mode (empty body).
        r = await page.request.put(PUT_PICKUP_URL, headers=common_headers, data="{}")
        body1 = await r.text()
        if r.status != 200:
            raise RuntimeError(f"PUT pickup (empty) returned {r.status}: {body1[:200]}")
        log.info(f"PUT 1 status={r.status} body={body1[:300]}")
        log.info(f"PUT 1 response headers={dict(r.headers)}")
        try:
            log.info(f"PUT 1 headers_array={r.headers_array}")
        except Exception:
            pass
        cookie_after_put1 = await _get_cookie(ctx, "cw-lrkswrdjp")
        log.info(f"cw-lrkswrdjp after PUT 1: {cookie_after_put1!r}")

        # Step 3 — pin the specific pickup store by api_store_id. The real
        # store-pin endpoint is /pickup-addresses (not /methods/pickup, which
        # only flips the fulfilment mode). The server responds by updating
        # the cw-lrkswrdjp cookie's f- field to the new fulfilmentStoreId.
        payload = json.dumps({"addressId": branch.api_store_id})
        r = await page.request.put(PUT_PICKUP_ADDRESS_URL, headers=common_headers, data=payload)
        body2 = await r.text()
        if r.status != 200:
            raise RuntimeError(
                f"PUT pickup addressId={branch.api_store_id} returned {r.status}: "
                f"{body2[:200]}"
            )
        log.info(f"PUT 2 status={r.status} body={body2[:500]}")
        log.info(f"PUT 2 response headers={dict(r.headers)}")
        try:
            log.info(f"PUT 2 headers_array={r.headers_array}")
        except Exception:
            pass

        try:
            pin_resp = json.loads(body2)
        except Exception:
            pin_resp = {}
        if pin_resp.get("addressCanNotBeServiced"):
            raise RuntimeError(
                f"Woolworths says addressId={branch.api_store_id} cannot be serviced "
                f"(addressCanNotBeServiced=true). Branch name/id mismatch?"
            )

        # The PUT 2 response may embed fulfilment context with pickupAddressId.
        # If present, log it for diagnostics. The authoritative signal, however,
        # is the f- field of the cw-lrkswrdjp cookie, which the server rewrites
        # to the correct fulfilmentStoreId once the pickup address is updated.
        server_fulfilment = (pin_resp.get("context") or {}).get("fulfilment") or {}
        server_pickup_id = server_fulfilment.get("pickupAddressId")
        server_address = server_fulfilment.get("address")
        log.info(
            f"PUT 2 fulfilment context: address={server_address!r} "
            f"pickupAddressId={server_pickup_id} "
            f"fulfilmentStoreId={server_fulfilment.get('fulfilmentStoreId')}"
        )
        if server_pickup_id is not None and int(server_pickup_id) != int(branch.api_store_id):
            log.warning(
                f"PUT 2 response pickupAddressId={server_pickup_id} "
                f"differs from requested api_store_id={branch.api_store_id}; "
                f"will rely on cw-lrkswrdjp f- field for verification"
            )

        # Wait for cw-lrkswrdjp to update from its pre-PUT-2 value. The f-
        # field inside this cookie is the server's authoritative record of
        # which fulfilmentStoreId the session is pinned to.
        baseline = cookie_after_put1 if cookie_after_put1 is not None else initial_pin_cookie
        final_pin = await _wait_for_cookie_change(ctx, "cw-lrkswrdjp", baseline, timeout_s=5.0)
        log.info(f"final cw-lrkswrdjp after wait: {final_pin!r}")
        if final_pin == baseline:
            raise RuntimeError(
                f"cw-lrkswrdjp did not change after PUT pickup-addresses "
                f"(still {final_pin!r}); session not pinned to "
                f"api_store_id={branch.api_store_id}"
            )

        # Parse f- field (fulfilmentStoreId) from the cookie for reporting.
        # Format: "dm-Pickup,f-9010,a-674,s-38"
        pinned_f = None
        if final_pin:
            for part in final_pin.split(","):
                part = part.strip()
                if part.startswith("f-"):
                    pinned_f = part[2:]
                    break
        log.info(
            f"pinned store api_store_id={branch.api_store_id} "
            f"(cw-lrkswrdjp f-={pinned_f})"
        )
        await page.wait_for_timeout(500)

        # Step 4 — verify: /api/v1/products must now return > 0 items for a
        # common department (fruit-veg). A wedged/unfinished store binding
        # typically returns 400 or totalItems=0.
        r = await page.request.get(PRODUCTS_VERIFY_URL, headers={
            "accept": "application/json, text/plain, */*",
            "x-requested-with": "OnlineShopping.WebApp",
            "x-ui-ver": "7.73.30",
            "referer": f"{WOOLWORTHS_BASE}/shop/browse/fruit-veg",
        })
        if r.status != 200:
            raise RuntimeError(f"verify GET /api/v1/products returned {r.status}")
        vdata = await r.json()
        products_node = vdata.get("products") or {}
        total = products_node.get("totalItems") or 0
        if total <= 0:
            raise RuntimeError(f"verify GET /api/v1/products returned totalItems={total}")
        log.info(f"verified: fruit-veg totalItems={total}")

        # Step 5 — export storage state. Re-read the pinning cookie from the
        # exported state itself so the log reflects exactly what's persisted.
        SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
        session_path = SESSIONS_DIR / f"{branch.id}.json"
        state = await ctx.storage_state()
        persisted_pin = next(
            (c["value"] for c in state.get("cookies", []) if c["name"] == "cw-lrkswrdjp"),
            None,
        )
        log.info(
            f"persisted cw-lrkswrdjp in storage_state: {persisted_pin!r} "
            f"(api_store_id={branch.api_store_id})"
        )
        # Annotate so consumers know what store this state is pinned to.
        state["_pico_meta"] = {
            "branch_id": branch.id,
            "branch_name": branch.name,
            "api_store_id": branch.api_store_id,
            "fulfilment_store_id": pinned_f,
            "bootstrapped_at": start.isoformat(),
            "verified_total_items_fruit_veg": total,
        }
        session_path.write_text(json.dumps(state, indent=2))
        log.info(f"wrote {session_path}")

        return {
            "name": branch.name,
            "api_store_id": branch.api_store_id,
            "fulfilment_store_id": pinned_f,
            "bootstrapped_at": start.isoformat(),
            "status": "success",
            "verified_total_items_fruit_veg": total,
            "session_path": str(session_path.relative_to(Path(__file__).parent)),
        }

    except Exception as e:
        logger.exception(f"[{branch.name}] FAILED: {e}")
        return {
            "name": branch.name,
            "api_store_id": branch.api_store_id,
            "bootstrapped_at": start.isoformat(),
            "status": "failed",
            "error": str(e),
        }
    finally:
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

async def _worker(browser, branch: Branch, sem: asyncio.Semaphore) -> None:
    async with sem:
        entry = await bootstrap_branch(browser, branch)
        await _update_index(branch.id, entry)


def _is_fresh(branch_id: str, index: dict) -> bool:
    entry = index.get(branch_id)
    if not entry or entry.get("status") != "success":
        return False
    try:
        ts = datetime.fromisoformat(entry["bootstrapped_at"])
    except Exception:
        return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - ts
    session_path = SESSIONS_DIR / f"{branch_id}.json"
    return age < SESSION_MAX_AGE and session_path.exists()


async def run(workers: int, branch_id: Optional[str], skip_existing: bool) -> int:
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    branches = fetch_branches(supabase, branch_id)
    if not branches:
        logger.error("No Woolworths branches with api_store_id found — nothing to do")
        return 1
    logger.info(f"Bootstrapping {len(branches)} branches with {workers} workers")

    if skip_existing:
        index = _load_index()
        before = len(branches)
        branches = [b for b in branches if not _is_fresh(b.id, index)]
        logger.info(f"--skip-existing: {before - len(branches)} already fresh, {len(branches)} to do")

    if not branches:
        logger.info("Nothing to bootstrap.")
        return 0

    sem = asyncio.Semaphore(max(1, workers))

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--disable-http2",
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )
        try:
            tasks = [asyncio.create_task(_worker(browser, b, sem)) for b in branches]
            await asyncio.gather(*tasks)
        finally:
            await browser.close()

    # Report
    index = _load_index()
    succ = sum(1 for b in branches if index.get(b.id, {}).get("status") == "success")
    fail = len(branches) - succ
    logger.info(f"Done: {succ} success, {fail} failed (of {len(branches)} attempted)")
    return 0 if fail == 0 else 2


def main() -> int:
    ap = argparse.ArgumentParser(description="Bootstrap per-branch Woolworths Playwright sessions.")
    ap.add_argument("--workers", type=int, default=3, help="parallel workers (default 3)")
    ap.add_argument("--branch-id", type=str, default=None, help="bootstrap only this branch UUID")
    ap.add_argument("--skip-existing", action="store_true",
                    help="skip branches with a successful session <2h old")
    args = ap.parse_args()
    try:
        return asyncio.run(run(args.workers, args.branch_id, args.skip_existing))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
