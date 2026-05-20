"""
Bulk proxy tester — verifies a list of proxies against the 3 NZ supermarket
retailers in parallel. Designed for testing the 150 IPs from proxiesthatwork.com
or any other bulk provider.

Two-phase strategy:
  Phase 1 (fast HTTP)  — for each proxy, fire 3 lightweight HTTP HEAD requests
                          (one per retailer's homepage). 5-second timeout per
                          request. Filters out ~80% of dead/blocked proxies in
                          ~30 seconds total, no browser overhead.
  Phase 2 (Playwright) — full browser scrape against bakery category for
                          survivors only. ~30-60s per proxy.

Saves working proxies to .txt files per retailer, plus a combined summary CSV.

Usage:
  # File format: one proxy URL per line, e.g.
  #   http://192.46.205.10:12321
  #   http://user:pass@host:port
  # Comments (#) and blank lines are skipped.
  python3 test_proxy_bulk.py --proxies-file proxies.txt

  # Customise output, parallelism, filter pre-pass
  python3 test_proxy_bulk.py --proxies-file proxies.txt \\
    --output-dir results/ --concurrency 10 --skip-fast-filter

  # Test only one retailer
  python3 test_proxy_bulk.py --proxies-file proxies.txt --chain newworld
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# Reuse existing scraper code paths
THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR))

from woolworths_claude import WoolworthsClaudeScraper
from foodstuffs_claude import FoodstuffsScraper, BarcodeCache, CHAINS as FS_CHAINS

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------

# Phase 1 (fast HTTP): cheap reachability test per retailer.
# Phase 2 (full): bakery category, smallest verifiable scrape.
RETAILERS = {
    "woolworths": {
        "label": "Woolworths",
        "head_url": "https://www.woolworths.co.nz/",
        "scrape_url": "https://www.woolworths.co.nz/shop/browse/bakery",
    },
    "newworld": {
        "label": "New World",
        "head_url": "https://www.newworld.co.nz/",
        "scrape_url": "https://www.newworld.co.nz/shop/category/bakery",
        "fs_key": "newworld",
    },
    "paknsave": {
        "label": "Pak'nSave",
        "head_url": "https://www.paknsave.co.nz/",
        "scrape_url": "https://www.paknsave.co.nz/shop/category/bakery",
        "fs_key": "paknsave",
    },
}

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class ProxyResult:
    proxy: str
    fast: dict[str, int] = field(default_factory=dict)        # chain → http_status
    full: dict[str, dict] = field(default_factory=dict)       # chain → {products, barcoded, ok, err}

    def works_for(self, chain: str) -> bool:
        f = self.full.get(chain)
        return bool(f and f.get("ok"))


# ---------------------------------------------------------------------------
# Phase 1: fast HTTP reachability filter
# ---------------------------------------------------------------------------

async def fast_check(proxy: str, sem: asyncio.Semaphore, chains: list[str]) -> ProxyResult:
    """Fire one HTTP HEAD per retailer through the proxy. Just checks if traffic
    flows through — not whether the proxy bypasses bot detection."""
    import aiohttp
    res = ProxyResult(proxy=proxy)
    timeout = aiohttp.ClientTimeout(total=8, connect=4)
    connector = aiohttp.TCPConnector(ssl=False, limit=4)
    async with sem:
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                for ch in chains:
                    url = RETAILERS[ch]["head_url"]
                    try:
                        # Most providers want HTTP CONNECT; aiohttp passes proxy via the proxy=
                        async with session.get(url, proxy=proxy, allow_redirects=False) as r:
                            res.fast[ch] = r.status
                    except Exception:
                        res.fast[ch] = 0  # connection error / timeout / proxy refused
        except Exception:
            for ch in chains:
                res.fast.setdefault(ch, 0)
    return res


# ---------------------------------------------------------------------------
# Phase 2: full Playwright scrape
# ---------------------------------------------------------------------------

async def full_check_woolworths(proxy: str) -> dict:
    s = WoolworthsClaudeScraper(
        category_urls=[RETAILERS["woolworths"]["scrape_url"]],
        headless=True, dry_run=True, proxy_url=proxy,
    )
    s._resolve_branch()
    await s._start_browser()
    sp = s._session_path_for_branch()
    await s._new_context(storage_state=str(sp) if sp else None)
    try:
        result = await asyncio.wait_for(
            s.scrape_one_category(RETAILERS["woolworths"]["scrape_url"]),
            timeout=120,
        )
        products = result[0]  # supports 2-tuple or 3-tuple return
    except Exception as e:
        await s._close_browser()
        return {"products": 0, "barcoded": 0, "ok": False, "err": str(e)[:120]}
    await s._close_browser()
    have = sum(1 for p in products if p.barcode)
    return {"products": len(products), "barcoded": have, "ok": len(products) > 0 and have > 0}


async def full_check_foodstuffs(proxy: str, chain_key: str) -> dict:
    cache = BarcodeCache()
    s = FoodstuffsScraper(
        chain_key=chain_key,
        category_slugs=["bakery"],
        headless=True, dry_run=True, cache=cache, proxy_url=proxy,
    )
    s._resolve_branch()
    await s._start_browser()
    await s._new_context()
    url = f"{FS_CHAINS[chain_key]['base_url']}/shop/category/bakery"
    try:
        products, _ = await asyncio.wait_for(s.scrape_one_category(url), timeout=120)
        if products:
            await asyncio.wait_for(s.enrich_barcodes(products), timeout=60)
    except Exception as e:
        await s._close_browser()
        return {"products": 0, "barcoded": 0, "ok": False, "err": str(e)[:120]}
    await s._close_browser()
    have = sum(1 for p in products if p.barcode)
    return {"products": len(products), "barcoded": have, "ok": len(products) > 0}


async def full_check(proxy: str, sem: asyncio.Semaphore, chains: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    async with sem:
        for ch in chains:
            try:
                if ch == "woolworths":
                    r = await full_check_woolworths(proxy)
                else:
                    r = await full_check_foodstuffs(proxy, RETAILERS[ch]["fs_key"])
                out[ch] = r
            except Exception as e:
                out[ch] = {"products": 0, "barcoded": 0, "ok": False, "err": str(e)[:120]}
    return out


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def read_proxies(path: Path) -> list[str]:
    out: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Accept formats:
        #   ip:port
        #   user:pass@ip:port
        #   http(s)://ip:port
        #   http(s)://user:pass@ip:port
        if "://" not in line:
            line = f"http://{line}"
        out.append(line)
    return out


def write_results(results: list[ProxyResult], output_dir: Path, chains: list[str]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    # Per-chain working list
    for ch in chains:
        path = output_dir / f"working_{ch}.txt"
        with path.open("w") as f:
            f.write(f"# Proxies that returned valid {RETAILERS[ch]['label']} bakery data.\n")
            f.write(f"# Generated by test_proxy_bulk.py\n\n")
            for r in results:
                if r.works_for(ch):
                    f.write(f"{r.proxy}\n")

    # Per-proxy summary CSV
    csv_path = output_dir / "summary.csv"
    with csv_path.open("w", newline="") as f:
        w = csv.writer(f)
        header = ["proxy"]
        for ch in chains:
            header += [f"{ch}_fast", f"{ch}_products", f"{ch}_barcoded", f"{ch}_ok"]
        w.writerow(header)
        for r in results:
            row = [r.proxy]
            for ch in chains:
                row.append(r.fast.get(ch, 0))
                full = r.full.get(ch, {})
                row.append(full.get("products", 0))
                row.append(full.get("barcoded", 0))
                row.append(int(bool(full.get("ok"))))
            w.writerow(row)

    # ALL-WORKS list (passes all tested chains)
    all_works_path = output_dir / "working_all.txt"
    with all_works_path.open("w") as f:
        f.write("# Proxies that pass ALL tested chains.\n\n")
        for r in results:
            if all(r.works_for(ch) for ch in chains):
                f.write(f"{r.proxy}\n")


async def main_async() -> int:
    ap = argparse.ArgumentParser(description="Bulk test a list of proxies against NZ supermarkets")
    ap.add_argument("--proxies-file", required=True, help="Text file with one proxy URL per line")
    ap.add_argument("--output-dir", default="proxy_test_results", help="Where to write working_*.txt + summary.csv")
    ap.add_argument("--chain", choices=list(RETAILERS) + ["all"], default="all",
                    help="Which retailer(s) to test (default: all)")
    ap.add_argument("--fast-concurrency", type=int, default=20,
                    help="Phase 1 (HTTP HEAD) concurrency (default 20)")
    ap.add_argument("--full-concurrency", type=int, default=4,
                    help="Phase 2 (Playwright) concurrency (default 4 — Chromium is heavy)")
    ap.add_argument("--skip-fast-filter", action="store_true",
                    help="Skip phase 1, run full Playwright scrape on every proxy (slow)")
    ap.add_argument("--max-full", type=int, default=None,
                    help="Cap how many proxies advance to Phase 2 after the fast filter")
    args = ap.parse_args()

    proxies = read_proxies(Path(args.proxies_file))
    if not proxies:
        print(f"No proxies found in {args.proxies_file}", file=sys.stderr); return 2

    chains = [args.chain] if args.chain != "all" else list(RETAILERS.keys())
    print(f"Testing {len(proxies)} proxies against: {', '.join(chains)}")
    output_dir = Path(args.output_dir)

    t0 = time.time()
    results: list[ProxyResult] = [ProxyResult(proxy=p) for p in proxies]

    # ---- Phase 1: fast HTTP filter ----
    survivors = list(results)
    if not args.skip_fast_filter:
        print(f"\nPhase 1 (HTTP HEAD, concurrency={args.fast_concurrency}) — filtering dead proxies...")
        sem = asyncio.Semaphore(args.fast_concurrency)
        out = await asyncio.gather(*[fast_check(r.proxy, sem, chains) for r in results])
        for src, dst in zip(out, results):
            dst.fast = src.fast
        # A proxy "passes" Phase 1 for a chain if HEAD returned 2xx, 3xx, or 4xx
        # (a 4xx still means traffic flowed; 0 means timeout / refused).
        survivors = [
            r for r in results
            if any(100 <= r.fast.get(ch, 0) < 600 for ch in chains)
        ]
        passed = len(survivors)
        print(f"Phase 1 done in {time.time() - t0:.1f}s: {passed}/{len(proxies)} reachable")
        if args.max_full and passed > args.max_full:
            survivors = survivors[:args.max_full]
            print(f"  (capped to first {args.max_full} for Phase 2)")

    # ---- Phase 2: full browser scrape ----
    if survivors:
        print(f"\nPhase 2 (Playwright, concurrency={args.full_concurrency}) — verifying {len(survivors)} proxies...")
        sem2 = asyncio.Semaphore(args.full_concurrency)
        async def _do(r: ProxyResult) -> ProxyResult:
            r.full = await full_check(r.proxy, sem2, chains)
            return r
        done = 0
        for coro in asyncio.as_completed([_do(r) for r in survivors]):
            r = await coro
            done += 1
            short = r.proxy.split("@")[-1] if "@" in r.proxy else r.proxy
            chains_ok = [ch for ch in chains if r.works_for(ch)]
            print(f"  [{done}/{len(survivors)}] {short:30}  ok={','.join(chains_ok) or 'NONE'}")

    # ---- Output ----
    write_results(results, output_dir, chains)
    elapsed = time.time() - t0

    print(f"\n=== Done in {elapsed:.1f}s ===")
    for ch in chains:
        ok_count = sum(1 for r in results if r.works_for(ch))
        print(f"  {RETAILERS[ch]['label']:12}  {ok_count}/{len(proxies)} working")
    all_ok = sum(1 for r in results if all(r.works_for(ch) for ch in chains))
    print(f"  ALL chains:    {all_ok}/{len(proxies)}")
    print(f"\nResults written to: {output_dir.absolute()}/")
    print(f"  working_<chain>.txt — per-retailer working lists")
    print(f"  working_all.txt    — proxies that pass every chain")
    print(f"  summary.csv         — full per-proxy status table")

    return 0 if any(r.works_for(ch) for r in results for ch in chains) else 1


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
