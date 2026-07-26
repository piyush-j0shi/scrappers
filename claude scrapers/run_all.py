"""
Orchestrator — runs all 3 retailer scrapers in parallel as subprocesses.

Default config implements the recommended hybrid:
  - Woolworths via PTW proxy round-robin, concurrency 25 (~1.1 hr)
  - New World from home IP, concurrency 4 (~3.7 hr) — bottleneck
  - Pak'nSave from home IP, concurrency 2 (~2.4 hr)
  → total wall ~3.7 hrs for the full daily run

Each chain runs as its own Python process so:
  - Their async event loops don't interfere
  - One chain crashing doesn't take down the others
  - Logs are interleaved but tagged per chain

Output:
  Each chain's stdout is captured, prefixed with [chain] and streamed live.
  Final summary printed at end.

Usage:
  # Default config
  python3 run_all.py --proxy-file proxiesthatwork.txt

  # Conservative (no proxies for any chain — slower but safer)
  python3 run_all.py --ww-concurrency 2 --nw-concurrency 2 --ps-concurrency 1

  # Skip a chain
  python3 run_all.py --skip paknsave --proxy-file proxies.txt

  # Test mode — only 3 categories per chain
  python3 run_all.py --test --proxy-file proxies.txt

  # Dry run — no Supabase writes
  python3 run_all.py --dry-run --proxy-file proxies.txt
"""
from __future__ import annotations

import argparse
import asyncio
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# Reuse Supabase env config from the scrapers
from dotenv import load_dotenv

THIS_DIR = Path(__file__).resolve().parent
SCRAPERS_DIR = THIS_DIR.parent
load_dotenv(SCRAPERS_DIR / ".env")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

CHAIN_DEFAULTS = {
    "woolworths": {
        "script": "woolworths_claude.py",
        "concurrency": 25,
        "expected_duration_hr": 1.1,
        "uses_proxy_file": True,
    },
    "newworld": {
        "script": "newworld_claude.py",
        "concurrency": 4,
        "expected_duration_hr": 3.7,
        "uses_proxy_file": False,  # Cloudflare blocks datacenter IPs
    },
    "paknsave": {
        "script": "paknsave_claude.py",
        "concurrency": 2,
        "expected_duration_hr": 2.4,
        "uses_proxy_file": False,
    },
}


def sweep_stale_running(min_age_hours: float = 2.0) -> int:
    """Mark orphaned 'running' scraper_runs rows as 'failed'.

    Runs once at orchestrator startup before launching subprocesses. Any row
    older than `min_age_hours` AND status='running' is treated as orphaned
    (real runs finish in <1.5h per branch — anything older is from a crashed
    or kill-9'd process whose finally block never ran).

    Belt-and-braces: also checks `pgrep` for active scraper processes; if any
    are detected, sweep is SKIPPED to avoid clobbering a legit concurrent run.

    Returns the number of rows updated.
    """
    if not (SUPABASE_URL and SUPABASE_SERVICE_KEY):
        print("[sweep] no Supabase env — skipping stale-row sweep")
        return 0

    # Process safety check
    try:
        r = subprocess.run(
            ["pgrep", "-f",
             "python3.*(woolworths|paknsave|newworld|foodstuffs)_claude"],
            capture_output=True, text=True, timeout=5,
        )
        active_pids = [p for p in r.stdout.strip().split("\n") if p.strip()]
        # Filter out our own PID and any wrapper PIDs from this orchestrator
        my_pid = str(os.getpid())
        active_pids = [p for p in active_pids if p != my_pid]
        if active_pids:
            print(f"[sweep] {len(active_pids)} active scraper process(es) detected — skipping sweep")
            return 0
    except Exception as e:
        print(f"[sweep] pgrep check failed ({e}) — proceeding with cautious sweep anyway")

    try:
        from supabase import create_client
        sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    except Exception as e:
        print(f"[sweep] supabase client init failed: {e}")
        return 0

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=min_age_hours)).isoformat()
    try:
        stale = sb.table("scraper_runs").select("id").eq("status", "running").lt(
            "started_at", cutoff
        ).execute().data
    except Exception as e:
        print(f"[sweep] fetch stale rows failed: {e}")
        return 0

    if not stale:
        print(f"[sweep] no stale running rows older than {min_age_hours}h")
        return 0

    print(f"[sweep] found {len(stale)} stale running rows — marking as failed")
    cleaned = 0
    ids = [s["id"] for s in stale]
    now_iso = datetime.now(timezone.utc).isoformat()
    for i in range(0, len(ids), 200):
        chunk = ids[i : i + 200]
        try:
            r = sb.table("scraper_runs").update({
                "status": "failed",
                "finished_at": now_iso,
                "error_log": f"[auto-sweep at startup of run_all.py {now_iso}: orphaned running row]",
            }).in_("id", chunk).execute()
            cleaned += len(r.data)
        except Exception as e:
            print(f"[sweep] chunk {i // 200 + 1} failed: {e}")
    print(f"[sweep] cleaned {cleaned} orphan rows")
    return cleaned


async def stream_subprocess(label: str, cmd: list[str], log_file: Optional[Path] = None) -> int:
    """Spawn a subprocess, stream its stdout/stderr line-by-line with [label] prefix."""
    print(f"[orchestrator] launching [{label}]: {' '.join(cmd)}")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=str(THIS_DIR),
    )
    fh = log_file.open("w") if log_file else None
    try:
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").rstrip()
            print(f"[{label}] {text}")
            if fh:
                fh.write(text + "\n")
                fh.flush()
        rc = await proc.wait()
        return rc
    finally:
        if fh:
            fh.close()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run all 3 NZ supermarket scrapers in parallel")
    ap.add_argument("--proxy-file", default=None,
                    help="Proxy file (used by Woolworths only — Foodstuffs runs from home IP)")
    ap.add_argument("--ww-concurrency", type=int, default=CHAIN_DEFAULTS["woolworths"]["concurrency"])
    ap.add_argument("--nw-concurrency", type=int, default=CHAIN_DEFAULTS["newworld"]["concurrency"])
    ap.add_argument("--ps-concurrency", type=int, default=CHAIN_DEFAULTS["paknsave"]["concurrency"])
    # Left as None = keep the scraper's own default rather than silently
    # re-asserting a number here that could drift from it.
    ap.add_argument("--nw-rate", type=float, default=None,
                    help="New World req/sec ceiling (default: scraper's own, 12)")
    ap.add_argument("--ps-rate", type=float, default=None,
                    help="Pak'nSave req/sec ceiling (default: scraper's own, 12)")
    ap.add_argument("--skip", action="append", default=[],
                    choices=list(CHAIN_DEFAULTS.keys()),
                    help="Skip a chain (can repeat: --skip paknsave)")
    ap.add_argument("--test", action="store_true",
                    help="Run only 3 categories per chain (smoke test)")
    ap.add_argument("--dry-run", action="store_true", help="No Supabase writes")
    ap.add_argument("--no-headless", action="store_true",
                    help="Show browsers (debugging only — won't scale to high concurrency)")
    ap.add_argument("--no-auto-bootstrap", action="store_true",
                    help="Disable WW session auto-refresh")
    ap.add_argument("--log-dir", default=None,
                    help="Optional dir to write per-chain log files")
    ap.add_argument("--sweep-stale-hours", type=float, default=2.0,
                    help="Sweep orphaned 'running' rows older than this many hours at startup (default 2)")
    return ap.parse_args()


def build_cmd_for_chain(chain: str, args: argparse.Namespace) -> list[str]:
    cfg = CHAIN_DEFAULTS[chain]
    cmd: list[str] = ["python3", cfg["script"], "--all-branches"]
    conc_map = {
        "woolworths": args.ww_concurrency,
        "newworld": args.nw_concurrency,
        "paknsave": args.ps_concurrency,
    }
    conc = conc_map[chain]
    cmd += ["--concurrency", str(conc)]

    # req/sec ceiling — Foodstuffs chains only (the WW scraper has no --rate;
    # its throughput is governed by --concurrency over the proxy pool).
    rate_map = {"newworld": args.nw_rate, "paknsave": args.ps_rate}
    if chain in rate_map and rate_map[chain] is not None:
        cmd += ["--rate", str(rate_map[chain])]

    if args.test:
        cmd.append("--test")
        cmd.remove("--all-branches")  # --test selects categories not branches
    if args.dry_run:
        cmd.append("--dry-run")
    if args.no_headless:
        cmd.append("--no-headless")

    # Proxies only for Woolworths (Foodstuffs blocks datacenter IPs)
    if cfg["uses_proxy_file"] and args.proxy_file:
        cmd += ["--proxy-file", args.proxy_file]

    if chain == "woolworths" and args.no_auto_bootstrap:
        cmd.append("--no-auto-bootstrap")

    return cmd


async def main_async() -> int:
    args = parse_args()

    chains_to_run = [c for c in CHAIN_DEFAULTS if c not in args.skip]
    if not chains_to_run:
        print("[orchestrator] all chains skipped", file=sys.stderr)
        return 2

    print("=" * 70)
    print(" PICO MULTI-CHAIN SCRAPER ORCHESTRATOR")
    print("=" * 70)
    print(f"  Chains:       {', '.join(chains_to_run)}")
    print(f"  Mode:         {'TEST (3 cats)' if args.test else 'FULL (all categories)'}"
          f" {'(DRY RUN)' if args.dry_run else ''}")
    if args.proxy_file:
        print(f"  Proxy file:   {args.proxy_file} (Woolworths only)")
    chain_concurrency = {
        "woolworths": args.ww_concurrency,
        "newworld": args.nw_concurrency,
        "paknsave": args.ps_concurrency,
    }
    for c in chains_to_run:
        cfg = CHAIN_DEFAULTS[c]
        conc = chain_concurrency[c]
        print(f"  {c:12} concurrency={conc:>3}  expected ~{cfg['expected_duration_hr']:.1f}h")
    print("=" * 70)
    print()

    # Pre-flight: sweep any orphaned 'running' rows from prior crashed runs.
    # Also gates the run via process-detection so it's safe even if you re-launch
    # while a different orchestrator instance is mid-flight.
    sweep_stale_running(min_age_hours=args.sweep_stale_hours)
    print()

    log_dir = Path(args.log_dir) if args.log_dir else None
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    tasks = []
    for c in chains_to_run:
        cmd = build_cmd_for_chain(c, args)
        log_file = (log_dir / f"{c}.log") if log_dir else None
        tasks.append(asyncio.create_task(stream_subprocess(c, cmd, log_file)))

    # Handle Ctrl+C gracefully — propagate to all subprocesses
    loop = asyncio.get_event_loop()
    stop = asyncio.Event()
    def _on_signal():
        print("\n[orchestrator] received interrupt — terminating all chains")
        stop.set()
    if os.name != "nt":
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _on_signal)
            except NotImplementedError:
                pass

    results = await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.time() - t0

    print()
    print("=" * 70)
    print(f" DONE in {elapsed/3600:.2f}h ({elapsed:.0f}s)")
    print("=" * 70)
    rc_total = 0
    for chain, rc in zip(chains_to_run, results):
        status = "✅" if (isinstance(rc, int) and rc == 0) else f"❌ rc={rc}"
        print(f"  {chain:12} {status}")
        if not (isinstance(rc, int) and rc == 0):
            rc_total = 1
    return rc_total


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
