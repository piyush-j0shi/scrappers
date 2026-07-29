#!/usr/bin/env python3
"""Daily pipeline orchestrator — scrape + import, interleaved.

Flow (as designed with the user):
  Phase 1: launch New World and Woolworths scrapers in PARALLEL.
  Ongoing: as each chain's completed branch files land on disk and go stable,
           import them one-by-one — so import runs alongside the still-running
           scrape. A chain's import is gated shut until it has at least
           --import-start-min completed branches (default 5).
  Phase 2: the moment the NW scraper exits, launch the Pak'nSave scraper (+ its
           own interleaved import loop).
  Done:    when every scraper has exited AND every landed file is imported.

Why this shape is safe/fast (established earlier):
  - Cross-retailer imports run concurrently — they touch disjoint retailer rows,
    which is proven safe and faster. Within one chain, files import sequentially
    (one at a time) to avoid same-store contention on retailer_products.
  - The importer's product-matching (barcode -> canonical/variant, name+size
    fallback, review queue) runs automatically per file — no extra step.

Where it runs:
  ON THE SERVER, from the repo root (~/scrapers). The NW/PnS scrapers reach
  Cloudflare through the home tunnel (reverse SSH :8890, brought up by
  setup_local.sh on the home box); WW uses its own proxy/session path. The
  importer talks to pico-prod directly. Nothing here needs the home box beyond
  that tunnel staying up for NW/PnS.

Usage:
  python orchestrator.py                     # full run: NW+WW then PnS, with imports
  python orchestrator.py --skip ww           # drop a chain
  python orchestrator.py --no-import         # scrape only
  python orchestrator.py --dry-run-import    # scrape for real, imports parse-only (no DB writes)
  python orchestrator.py --import-start-min 1 --poll-sec 20   # tune gating/cadence

Logs: orchestrator_logs/{orchestrator,<chain>_scrape,<chain>_import}.log
"""
from __future__ import annotations

import argparse
import asyncio
import re
import signal
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

BASE = Path(__file__).resolve().parent
SCRAPERS_DIR = BASE / "claude scrapers"
EXPORTS_DIR = BASE / "exports"
LOG_DIR = BASE / "orchestrator_logs"
IMPORTER = str(BASE / "import_products.py")
_VENV_PY = BASE / ".venv" / "bin" / "python"
PYTHON = str(_VENV_PY if _VENV_PY.exists() else sys.executable)

TUNNEL_PROXY = "http://127.0.0.1:8890"  # reverse SSH tunnel -> home proxy (CF)

# Per-chain config. scrape_args are appended after the script name. Edit these
# to change concurrency/rate, or to point WW at a proxy file (see WW note).
CHAINS: dict[str, dict] = {
    "nw": {
        "label": "New World",
        "script": "newworld_claude.py",
        "prefix": "newworld",
        "retailer": "new-world",
        "source_system": "newworld_scraper",
        "scrape_args": ["--all-branches", "--concurrency", "10", "--rate", "30",
                        "--proxy", TUNNEL_PROXY],
    },
    "ww": {
        "label": "Woolworths",
        "script": "woolworths_claude.py",
        "prefix": "woolworths",
        "retailer": "woolworths",
        "source_system": "woolworths_scraper",
        # WW is Akamai, not Cloudflare — it does NOT use the home CF tunnel.
        # Default here uses the bootstrapped saved sessions. If you scrape WW
        # through rotating proxies, add: "--proxy-file", "proxiesthatwork.txt".
        "scrape_args": ["--all-branches", "--concurrency", "10"],
    },
    "pns": {
        "label": "Pak'nSave",
        "script": "paknsave_claude.py",
        "prefix": "paknsave",
        "retailer": "paknsave",
        "source_system": "paknsave_scraper",
        "scrape_args": ["--all-branches", "--concurrency", "10", "--rate", "30",
                        "--proxy", TUNNEL_PROXY],
    },
}

ARGS: argparse.Namespace  # filled in main()
RUN_START: float          # epoch seconds; only files newer than this are ours
RUNNING: set[asyncio.subprocess.Process] = set()  # for clean shutdown
ACTIVE: list[str] = []            # chains running this session (for the progress line)
STATS: dict[str, dict] = {}       # per-chain live counters
_WORK: Optional[asyncio.Future] = None  # top-level gather; cancelled on signal for graceful stop


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}"
    print(line, flush=True)
    try:
        with (LOG_DIR / "orchestrator.log").open("a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def _norm_slug(key: str, slug: str) -> str:
    """Defensive Pak'nSave normalization: old export files used the `pak-nsave-`
    branch slug, but the pico-prod branch slug is `paknsave-`. Current scrapers
    already emit `paknsave-` (apostrophe dropped), so this is a no-op for them —
    it only rescues an older-style filename if the server's scraper copy lags."""
    if key == "pns" and slug.startswith("pak-nsave-"):
        return "paknsave-" + slug[len("pak-nsave-"):]
    return slug


def _slug_from(prefix: str, fname: str) -> Optional[str]:
    m = re.match(rf"^{re.escape(prefix)}_(.+)_\d{{8}}T\d{{6}}Z\.jsonl$", fname)
    return m.group(1) if m else None


def _chain_files(prefix: str) -> list[Path]:
    """Files for this chain that belong to THIS run (mtime after RUN_START)."""
    out = []
    for p in EXPORTS_DIR.glob(f"{prefix}_*.jsonl"):
        try:
            if p.stat().st_mtime >= RUN_START:
                out.append(p)
        except OSError:
            pass
    return sorted(out)


def _is_stable(p: Path) -> bool:
    try:
        return (time.time() - p.stat().st_mtime) >= ARGS.file_stable_sec
    except OSError:
        return False


async def _spawn(cmd: list[str], cwd: Path, logf) -> int:
    proc = await asyncio.create_subprocess_exec(
        *cmd, cwd=str(cwd), stdout=logf, stderr=asyncio.subprocess.STDOUT)
    RUNNING.add(proc)
    try:
        return await proc.wait()
    finally:
        RUNNING.discard(proc)


async def run_scraper(key: str) -> int:
    c = CHAINS[key]
    cmd = [PYTHON, c["script"], *c["scrape_args"]]
    log(f"[{key}] scraper START: {' '.join(cmd)}")
    with (LOG_DIR / f"{key}_scrape.log").open("a") as logf:
        logf.write(f"\n===== scrape start {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%SZ} =====\n")
        logf.flush()
        rc = await _spawn(cmd, SCRAPERS_DIR, logf)
    log(f"[{key}] scraper EXIT rc={rc}")
    return rc


async def import_one(key: str, path: Path, slug: str) -> int:
    c = CHAINS[key]
    cmd = [PYTHON, IMPORTER, "--input", str(path),
           "--retailer", c["retailer"], "--source-system", c["source_system"],
           "--branch-slug", slug, "--full-branch"]
    if ARGS.dry_run_import:
        cmd.append("--dry-run")
    with (LOG_DIR / f"{key}_import.log").open("a") as logf:
        logf.write(f"\n=== import {path.name} (slug={slug}) "
                   f"{datetime.now(timezone.utc):%H:%M:%SZ} ===\n")
        logf.flush()
        return await _spawn(cmd, BASE, logf)


async def import_loop(key: str, scraper_started: asyncio.Event,
                      scraper_done: asyncio.Event) -> None:
    """Import this chain's completed branch files as they land, sequentially,
    until the scraper is done and every file has been imported.

    Two-stage start: (1) wait for this chain's scraper to actually launch, then
    hold for --import-warmup-sec (default 5 min) so the scraper has time to land
    its first branches before we even look; (2) open the gate once at least
    --import-start-min branches are on disk (or the scraper has already exited)."""
    c = CHAINS[key]
    prefix = c["prefix"]
    imported: set[str] = set()   # filenames done (incl. given-up failures)
    attempts: dict[str, int] = {}
    opened = False

    await scraper_started.wait()
    if ARGS.import_warmup_sec > 0:
        log(f"[{key}] warmup: waiting {ARGS.import_warmup_sec}s after scraper "
            f"start before importing")
        await asyncio.sleep(ARGS.import_warmup_sec)
    log(f"[{key}] import loop armed (gate opens at "
        f"{ARGS.import_start_min} branches or scraper exit)")

    while True:
        files = _chain_files(prefix)
        stable = [p for p in files if _is_stable(p)]

        if not opened:
            if len(stable) >= ARGS.import_start_min or scraper_done.is_set():
                opened = True
                log(f"[{key}] import gate OPEN ({len(stable)} branches ready)")
            else:
                await asyncio.sleep(ARGS.poll_sec)
                continue

        for p in stable:
            if p.name in imported:
                continue
            slug = _slug_from(prefix, p.name)
            if not slug:
                log(f"[{key}] SKIP unparseable filename: {p.name}")
                imported.add(p.name)
                continue
            slug = _norm_slug(key, slug)
            rc = await import_one(key, p, slug)
            if rc == 0:
                imported.add(p.name)
                STATS[key]["imported"] += 1
                log(f"[{key}] imported {slug}  ({len(imported)} branches done)")
            else:
                n = attempts[p.name] = attempts.get(p.name, 0) + 1
                if n >= ARGS.import_retries:
                    imported.add(p.name)
                    STATS[key]["failed"] += 1
                    log(f"[{key}] IMPORT FAILED {slug} rc={rc} after {n} tries "
                        f"— SKIPPING (see {key}_import.log)")
                else:
                    log(f"[{key}] import {slug} rc={rc} — retry {n}/{ARGS.import_retries}")

        # completion: scraper finished AND every file (stable or still
        # stabilizing) has been imported.
        if scraper_done.is_set():
            names = {p.name for p in _chain_files(prefix)}
            if names <= imported:
                ok = len([n for n in imported if attempts.get(n, 0) < ARGS.import_retries])
                bad = len(imported) - ok
                log(f"[{key}] import loop DONE — {ok} imported"
                    + (f", {bad} failed" if bad else ""))
                return

        await asyncio.sleep(ARGS.poll_sec)


def _tunnel_up(port: int = 8890) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=3):
            return True
    except OSError:
        return False


async def progress_reporter() -> None:
    """Print one compact status line every --progress-sec so the run is watchable
    right in the terminal, without tailing the per-chain log files."""
    try:
        while True:
            await asyncio.sleep(ARGS.progress_sec)
            parts = []
            for k in ACTIVE:
                scraped = len(_chain_files(CHAINS[k]["prefix"]))
                s = STATS[k]
                seg = f"{k}: {scraped} scraped"
                if not ARGS.no_import:
                    seg += f"/{s['imported']} imported"
                    if s["failed"]:
                        seg += f" ({s['failed']} failed)"
                seg += f" [{s['scraper']}]"
                parts.append(seg)
            log("[progress] " + "  |  ".join(parts))
    except asyncio.CancelledError:
        return


async def run() -> None:
    global ACTIVE, STATS, _WORK
    LOG_DIR.mkdir(exist_ok=True)
    EXPORTS_DIR.mkdir(exist_ok=True)
    ACTIVE = [k for k in ("nw", "ww", "pns") if k not in ARGS.skip]
    STATS = {k: {"scraper": "pending", "imported": 0, "failed": 0} for k in ACTIVE}
    active = ACTIVE

    log("================ ORCHESTRATOR START ================")
    log(f"chains: {', '.join(CHAINS[k]['label'] for k in active)}"
        f"{'  (import DISABLED)' if ARGS.no_import else ''}"
        f"{'  (import DRY-RUN)' if ARGS.dry_run_import else ''}")
    log(f"gate={ARGS.import_start_min} branches  stable={ARGS.file_stable_sec}s  "
        f"poll={ARGS.poll_sec}s  retries={ARGS.import_retries}")

    if ({"nw", "pns"} & set(active)) and not _tunnel_up():
        log("WARNING: reverse tunnel 127.0.0.1:8890 is NOT reachable — NW/PnS "
            "will fail Cloudflare. Bring up the home tunnel (setup_local.sh).")

    started = {k: asyncio.Event() for k in CHAINS}
    done = {k: asyncio.Event() for k in CHAINS}
    tasks: list[asyncio.Task] = []

    async def scrape_then_signal(key: str) -> None:
        started[key].set()  # unblocks this chain's import warmup timer
        STATS[key]["scraper"] = "running"
        try:
            rc = await run_scraper(key)
            STATS[key]["scraper"] = "done" if rc == 0 else f"exit {rc}"
        except asyncio.CancelledError:
            STATS[key]["scraper"] = "stopped"
            raise
        finally:
            done[key].set()

    # Phase 1 — NW + WW together.
    for key in ("nw", "ww"):
        if key in active:
            tasks.append(asyncio.create_task(scrape_then_signal(key)))
            if not ARGS.no_import:
                tasks.append(asyncio.create_task(import_loop(key, started[key], done[key])))

    # Phase 2 — PnS after the NW scraper exits.
    if "pns" in active:
        async def pns_after_nw() -> None:
            if "nw" in active:
                await done["nw"].wait()
                log("[pns] NW scraping finished — starting Pak'nSave")
            await scrape_then_signal("pns")
        tasks.append(asyncio.create_task(pns_after_nw()))
        if not ARGS.no_import:
            tasks.append(asyncio.create_task(import_loop("pns", started["pns"], done["pns"])))

    prog = asyncio.create_task(progress_reporter())
    _WORK = asyncio.gather(*tasks)
    try:
        await _WORK
        log("================ PIPELINE COMPLETE ================")
    except asyncio.CancelledError:
        log("================ ORCHESTRATOR STOPPED (signal) ================")
    finally:
        prog.cancel()


def _terminate_children() -> None:
    for proc in list(RUNNING):
        try:
            proc.terminate()
        except ProcessLookupError:
            pass


def main() -> int:
    global ARGS, RUN_START
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--skip", nargs="*", default=[], choices=["nw", "ww", "pns"],
                    help="chains to skip")
    ap.add_argument("--no-import", action="store_true", help="scrape only, no imports")
    ap.add_argument("--dry-run-import", action="store_true",
                    help="run importers in --dry-run (parse-only, no DB writes)")
    ap.add_argument("--import-warmup-sec", type=int, default=300,
                    help="wait this long after a chain's scraper starts before "
                         "importing anything (default 300 = 5 min)")
    ap.add_argument("--import-start-min", type=int, default=5,
                    help="branches on disk before a chain's import gate opens")
    ap.add_argument("--file-stable-sec", type=int, default=60,
                    help="a file must be untouched this long before it's imported")
    ap.add_argument("--poll-sec", type=int, default=30, help="watch/import poll interval")
    ap.add_argument("--import-retries", type=int, default=2,
                    help="per-file import attempts before giving up on that branch")
    ap.add_argument("--progress-sec", type=int, default=60,
                    help="print a one-line progress summary this often (default 60s)")
    ARGS = ap.parse_args()
    RUN_START = time.time()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _shutdown(signame: str) -> None:
        # Terminate every child (scrapers + any in-flight importer), then cancel
        # the top-level gather so run() unwinds and the process actually EXITS.
        log(f"{signame} received — terminating scrapers/importers and stopping")
        _terminate_children()
        if _WORK is not None:
            _WORK.cancel()

    for sig, name in ((signal.SIGINT, "SIGINT"), (signal.SIGTERM, "SIGTERM")):
        try:
            loop.add_signal_handler(sig, _shutdown, name)
        except (NotImplementedError, RuntimeError):
            pass

    try:
        loop.run_until_complete(run())
        return 0
    except KeyboardInterrupt:  # fallback if signal handlers couldn't register
        log("KeyboardInterrupt — terminating child processes")
        _terminate_children()
        return 130
    finally:
        loop.close()


if __name__ == "__main__":
    sys.exit(main())
