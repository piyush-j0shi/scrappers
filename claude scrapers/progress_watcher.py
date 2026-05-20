"""Live progress watcher.

Watches per-chain log files written by run_all.py --log-dir and writes a
human-readable PROGRESS.md as branches complete. Re-parses every 30s.

Format:
  Top: overall summary (X/391 done, products scraped, blocks/retries)
  Per chain: branch table with status, categories, products, time, notes

Usage:
  python3 progress_watcher.py --log-dir runs/2026-05-07/
  python3 progress_watcher.py --log-dir runs/2026-05-07/ --once   # one-shot, no loop
"""
from __future__ import annotations

import argparse
import re
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

CHAINS = ("woolworths", "newworld", "paknsave")

# ---------------------------------------------------------------------------
# Log line patterns
# ---------------------------------------------------------------------------

# Look for per-branch start signal:
#   "chain=New World  branch=New World Te Rapa  branch_id=<uuid>  api_store_id=..."
RE_BRANCH_START = re.compile(
    r"^(?P<ts>\d{2}:\d{2}:\d{2}) \[INFO\] chain=(?P<chain>[^ ]+(?: [^ ]+)?)\s+"
    r"branch(?:_name)?=(?P<branch>[^ ]+(?: [^ ]+)*?)\s+branch_id=(?P<branch_id>[a-f0-9-]+)"
)

# WW form is similar but the words "branch_id=..." come without a chain= prefix.
RE_WW_BRANCH_START = re.compile(
    r"^(?P<ts>\d{2}:\d{2}:\d{2}) \[INFO\] chain_id=[a-f0-9-]+\s+branch_id=(?P<branch_id>[a-f0-9-]+)\s+branch=(?P<branch>.+)$"
)

# Category navigation:  "  → https://...  [cat]"
RE_CAT_NAV = re.compile(r"\s*→ (?P<url>https?://\S+)\s+\[(?P<cat>\w+)\]")

# Products parsed:       "  parsed 332 products from 8 responses"
RE_PARSED = re.compile(r"\s+parsed (?P<n>\d+) products from (?P<r>\d+) responses")

# Final summary per branch:
#   WW: "TOTAL scraped: 8120 products"
#   FS: "DONE  chain=New World  branches=1  updated=1495  new=21  changes=227  failed=0  ..."
RE_TOTAL = re.compile(r"TOTAL scraped:\s+(?P<n>\d+) products")

# Block detection:
RE_BLOCK = re.compile(r"\[block\] (?P<kind>visible challenge|silent challenge[^—]*) on (?P<url>\S+)")
RE_RETRY_OK = re.compile(r"\[block\] retry succeeded \((?P<n>\d+) products\)")
RE_RETRY_FAIL = re.compile(r"\[block\] retry STILL empty")

# Per-branch supabase write summary:
#   Foodstuffs: "DONE  chain=...  branches=1  updated=N  ..."
#   WW:         "DONE  branches=1/1  updated=N  new=N  changes=N  failed=N  blocks=N  retries=N  elapsed=Xs"
RE_DONE = re.compile(
    r"DONE\s+(?:chain=[^ ]+\s+)?branches=\d+(?:/\d+)?\s+updated=(?P<u>\d+)\s+new=(?P<new>\d+)\s+"
    r"changes=(?P<chg>\d+)\s+(?:failed=(?P<fail>\d+))?"
    r"(?:.*?elapsed=(?P<sec>[\d.]+)s)?"
)

# Saved/Upserted counts per branch:
#   "saving: 1496 unique  (1496 barcoded, 0 need name match)"
RE_SAVING = re.compile(r"saving:\s+(?P<unique>\d+) unique")
RE_UPSERTED = re.compile(r"upserted (?P<n>\d+) store_products rows")
RE_PRICE_CHG = re.compile(r"recorded (?P<n>\d+) price changes")

# Progress marker from concurrency wrapper:
#   "=== progress: 12/186 branches done (6.5%) ==="
RE_PROGRESS = re.compile(r"=== progress:\s+(?P<done>\d+)/(?P<total>\d+) branches done")

# Adaptive concurrency drop (Foodstuffs):
#   "[adaptive] 3 total blocks — dropping concurrency 5 -> 4"
RE_ADAPTIVE_DROP = re.compile(
    r"\[adaptive\] (?P<blocks>\d+) total blocks.*dropping concurrency (?P<old>\d+) -> (?P<new>\d+)"
)

# Branch failure:
#   "branch PAK'nSAVE Glenfield failed: TimeoutError: ..."
RE_BRANCH_FAIL = re.compile(r"\[ERROR\] branch (?P<branch>.+?) failed: (?P<err>.+)")

# Category-level failure (Foodstuffs):
#   "category failed: https://...: Page.goto: Timeout 60000ms exceeded."
RE_CAT_FAIL = re.compile(r"category failed: (?P<url>https?://\S+):\s+(?P<err>.+)")

# Foodstuffs Cloudflare block (from new self.blocks counter):
#   "[block] HTTP 403 on https://..."
#   "[block] challenge page 'just a moment' on https://..."
RE_FS_BLOCK = re.compile(r"\[block\] (?P<kind>HTTP \d+|challenge page [^o]+)on (?P<url>https?://\S+)")

# Category retry outcome:
#   "[retry] succeeded: 332 products"
#   "[retry] STILL empty for https://..."
RE_RETRY_CAT_OK = re.compile(r"\[retry\] succeeded: (?P<n>\d+) products")
RE_RETRY_CAT_FAIL = re.compile(r"\[retry\] STILL empty for (?P<url>https?://\S+)")


class BranchState:
    __slots__ = ("name", "branch_id", "categories", "started", "finished",
                 "blocks", "retries", "total", "saved", "price_changes", "status", "error")

    def __init__(self, name: str, branch_id: str | None = None) -> None:
        self.name = name
        self.branch_id = branch_id
        self.categories: dict[str, dict] = {}  # cat -> {products, responses}
        self.started: str | None = None
        self.finished: str | None = None
        self.blocks = 0
        self.retries = 0
        self.total = 0
        self.saved = 0
        self.price_changes = 0
        self.status = "running"  # running | done | failed
        self.error: str | None = None


def parse_chain_log(path: Path) -> tuple[list[BranchState], dict]:
    """Parse a chain's log file. Returns (branch states, overall progress)."""
    branches: list[BranchState] = []
    by_id: dict[str, BranchState] = {}
    cur: BranchState | None = None
    pending_cat: str | None = None
    overall = {"total": 0, "done_marker": 0}

    if not path.exists():
        return branches, overall

    for raw in path.read_text(errors="replace").splitlines():
        line = raw

        # Branch start (Foodstuffs format)
        m = RE_BRANCH_START.match(line)
        if m:
            bid = m.group("branch_id")
            name = m.group("branch")
            if bid not in by_id:
                cur = BranchState(name=name, branch_id=bid)
                cur.started = m.group("ts")
                branches.append(cur)
                by_id[bid] = cur
            else:
                cur = by_id[bid]
            pending_cat = None  # interleaved parallel logs — don't carry cat across branches
            continue

        # Branch start (Woolworths format)
        m = RE_WW_BRANCH_START.match(line)
        if m:
            bid = m.group("branch_id")
            name = m.group("branch").strip()
            if bid not in by_id:
                cur = BranchState(name=name, branch_id=bid)
                cur.started = m.group("ts")
                branches.append(cur)
                by_id[bid] = cur
            else:
                cur = by_id[bid]
            pending_cat = None  # interleaved parallel logs — don't carry cat across branches
            continue

        if cur is None:
            continue

        m = RE_CAT_NAV.search(line)
        if m:
            pending_cat = m.group("cat")
            cur.categories.setdefault(pending_cat, {"products": 0, "responses": 0})
            continue

        m = RE_PARSED.search(line)
        if m and pending_cat:
            cur.categories[pending_cat]["products"] = int(m.group("n"))
            cur.categories[pending_cat]["responses"] = int(m.group("r"))
            pending_cat = None
            continue

        m = RE_BLOCK.search(line)
        if m:
            cur.blocks += 1
            continue
        if RE_RETRY_OK.search(line) or RE_RETRY_FAIL.search(line):
            cur.retries += 1
            continue

        m = RE_TOTAL.search(line)
        if m:
            cur.total = int(m.group("n"))
            continue

        m = RE_UPSERTED.search(line)
        if m:
            cur.saved = int(m.group("n"))
            continue
        m = RE_PRICE_CHG.search(line)
        if m:
            cur.price_changes = int(m.group("n"))
            continue

        m = RE_DONE.search(line)
        if m:
            cur.finished = line[:8]  # "HH:MM:SS"
            cur.status = "done"
            cur = None
            continue

        m = RE_PROGRESS.search(line)
        if m:
            overall["done_marker"] = int(m.group("done"))
            overall["total"] = int(m.group("total"))
            continue

    return branches, overall


def render_md(log_dir: Path, started_at: datetime | None = None) -> str:
    """Build the full PROGRESS.md from per-chain logs."""
    out: list[str] = []
    now = datetime.now()
    out.append(f"# Pico scrape — live progress")
    out.append("")
    out.append(f"_Last refreshed:_ **{now.strftime('%Y-%m-%d %H:%M:%S')}**")
    if started_at:
        elapsed = now - started_at
        h, rem = divmod(elapsed.total_seconds(), 3600)
        m, s = divmod(rem, 60)
        out.append(f"_Elapsed:_ **{int(h)}h {int(m)}m {int(s)}s**")
    out.append("")

    # Per-chain parse
    chain_states: dict[str, list[BranchState]] = {}
    chain_overall: dict[str, dict] = {}
    grand_total = {"branches_done": 0, "branches_total": 0, "products": 0, "saved": 0,
                   "price_changes": 0, "blocks": 0, "retries": 0}

    for chain in CHAINS:
        branches, overall = parse_chain_log(log_dir / f"{chain}.log")
        chain_states[chain] = branches
        chain_overall[chain] = overall
        grand_total["branches_done"] += sum(1 for b in branches if b.status == "done")
        grand_total["branches_total"] += overall.get("total") or len(branches)
        grand_total["products"] += sum(b.total for b in branches)
        grand_total["saved"] += sum(b.saved for b in branches)
        grand_total["price_changes"] += sum(b.price_changes for b in branches)
        grand_total["blocks"] += sum(b.blocks for b in branches)
        grand_total["retries"] += sum(b.retries for b in branches)

    # ---- Overall summary ----
    out.append("## Overall")
    out.append("")
    out.append(f"| Metric | Count |")
    out.append(f"|---|---:|")
    pct = (grand_total["branches_done"] / max(1, grand_total["branches_total"])) * 100
    out.append(f"| Branches done | {grand_total['branches_done']}/{grand_total['branches_total']} ({pct:.1f}%) |")
    out.append(f"| Products scraped (all chains) | {grand_total['products']:,} |")
    out.append(f"| `store_products` rows upserted | {grand_total['saved']:,} |")
    out.append(f"| Price changes recorded | {grand_total['price_changes']:,} |")
    out.append(f"| Blocks detected | {grand_total['blocks']} |")
    out.append(f"| Retries | {grand_total['retries']} |")
    out.append("")

    # ---- Per-chain summary ----
    out.append("## Per-chain summary")
    out.append("")
    out.append("| Chain | Done / Total | Products | Upserted | Changes | Blocks | Retries |")
    out.append("|---|---|---:|---:|---:|---:|---:|")
    for chain in CHAINS:
        bs = chain_states[chain]
        total = chain_overall[chain].get("total") or len(bs)
        done = sum(1 for b in bs if b.status == "done")
        prod = sum(b.total for b in bs)
        saved = sum(b.saved for b in bs)
        chg = sum(b.price_changes for b in bs)
        blocks = sum(b.blocks for b in bs)
        retries = sum(b.retries for b in bs)
        out.append(f"| {chain} | {done}/{total} | {prod:,} | {saved:,} | {chg:,} | {blocks} | {retries} |")
    out.append("")

    # ---- Per-branch detail per chain ----
    for chain in CHAINS:
        bs = chain_states[chain]
        if not bs:
            out.append(f"## {chain} — no branches yet")
            out.append("")
            continue
        out.append(f"## {chain} — {len(bs)} branches seen")
        out.append("")
        out.append("| Branch | Status | Cats | Products | Upserted | Changes | Blocks | Retries | Started | Finished |")
        out.append("|---|---|---:|---:|---:|---:|---:|---:|---|---|")
        for b in bs:
            cat_count = len(b.categories)
            cat_with_zero = sum(1 for c in b.categories.values() if c["products"] == 0)
            cat_str = f"{cat_count}" + (f" (⚠️{cat_with_zero} empty)" if cat_with_zero else "")
            status_emoji = {"done": "✅", "running": "⏳", "failed": "❌"}.get(b.status, "?")
            out.append(
                f"| {b.name} | {status_emoji} {b.status} | {cat_str} | {b.total:,} | "
                f"{b.saved:,} | {b.price_changes:,} | {b.blocks} | {b.retries} | "
                f"{b.started or '-'} | {b.finished or '-'} |"
            )
        out.append("")
        # Surface failed branches
        empties = [b for b in bs if b.status == "done" and b.total == 0]
        if empties:
            out.append(f"### ⚠️ {chain} branches that completed with 0 products")
            out.append("")
            for b in empties:
                out.append(f"- `{b.name}` ({b.branch_id}) — likely silent block, retried but recovered nothing")
            out.append("")

    return "\n".join(out) + "\n"


def compute_summary(log_dir: Path) -> dict:
    """Cheap summary used for ntfy delta-detection (no markdown render)."""
    summary = {
        "branches_done": 0,
        "branches_total": 0,
        "products": 0,
        "saved": 0,
        "blocks": 0,
        "retries": 0,
        "per_chain": {},
    }
    for chain in CHAINS:
        branches, overall = parse_chain_log(log_dir / f"{chain}.log")
        done = sum(1 for b in branches if b.status == "done")
        total = overall.get("total") or len(branches)
        prod = sum(b.total for b in branches)
        saved = sum(b.saved for b in branches)
        blocks = sum(b.blocks for b in branches)
        retries = sum(b.retries for b in branches)
        summary["per_chain"][chain] = {
            "done": done, "total": total, "products": prod,
            "saved": saved, "blocks": blocks, "retries": retries,
        }
        summary["branches_done"] += done
        summary["branches_total"] += total
        summary["products"] += prod
        summary["saved"] += saved
        summary["blocks"] += blocks
        summary["retries"] += retries
    return summary


def scan_new_events(log_dir: Path, last_seen: dict[str, int]) -> list[dict]:
    """Scan for adaptive-drop and branch-failure lines we haven't seen yet.

    last_seen maps chain name -> number of lines already processed.
    Mutates last_seen in place so the next call only reads new lines.
    Returns a list of event dicts: {type, chain, ...fields}.
    """
    events: list[dict] = []
    for chain in CHAINS:
        path = log_dir / f"{chain}.log"
        if not path.exists():
            continue
        lines = path.read_text(errors="replace").splitlines()
        new_lines = lines[last_seen.get(chain, 0):]
        last_seen[chain] = len(lines)
        for line in new_lines:
            m = RE_ADAPTIVE_DROP.search(line)
            if m:
                events.append({
                    "type": "drop",
                    "chain": chain,
                    "old": int(m.group("old")),
                    "new": int(m.group("new")),
                    "blocks": int(m.group("blocks")),
                })
                continue
            m = RE_BRANCH_FAIL.search(line)
            if m:
                events.append({
                    "type": "fail",
                    "chain": chain,
                    "branch": m.group("branch").strip(),
                    "err": m.group("err")[:120].strip(),
                })
                continue
            m = RE_FS_BLOCK.search(line)
            if m:
                events.append({
                    "type": "cat_block",
                    "chain": chain,
                    "kind": m.group("kind").strip(),
                    "url": m.group("url"),
                })
                continue
            m = RE_CAT_FAIL.search(line)
            if m:
                events.append({
                    "type": "cat_fail",
                    "chain": chain,
                    "url": m.group("url"),
                    "err": m.group("err")[:120].strip(),
                })
                continue
            m = RE_RETRY_CAT_OK.search(line)
            if m:
                events.append({
                    "type": "cat_retry_ok",
                    "chain": chain,
                    "n": int(m.group("n")),
                })
                continue
            m = RE_RETRY_CAT_FAIL.search(line)
            if m:
                events.append({
                    "type": "cat_retry_fail",
                    "chain": chain,
                    "url": m.group("url"),
                })
    return events


def main() -> int:
    import os, sys
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    try:
        from notify import push as ntfy_push
    except Exception:
        ntfy_push = None  # graceful if notify.py missing

    ap = argparse.ArgumentParser(description="Watch run_all.py logs and write PROGRESS.md")
    ap.add_argument("--log-dir", required=True, help="Directory holding {chain}.log files")
    ap.add_argument("--out", default=None,
                    help="Path for the markdown file (default: <log-dir>/PROGRESS.md)")
    ap.add_argument("--interval", type=int, default=30, help="Refresh interval in seconds")
    ap.add_argument("--once", action="store_true", help="Render once and exit (no loop)")
    ap.add_argument("--ntfy-topic", default=os.environ.get("NTFY_TOPIC"),
                    help="Push notification topic (overrides $NTFY_TOPIC). Disabled if not set.")
    ap.add_argument("--ntfy-milestone-pct", type=int, default=10,
                    help="Send a push every N%% completion (default 10)")
    ap.add_argument("--ntfy-block-threshold", type=int, default=20,
                    help="Send a HIGH priority alert when block count crosses this threshold (default 20)")
    args = ap.parse_args()

    log_dir = Path(args.log_dir).expanduser().resolve()
    out_path = Path(args.out) if args.out else log_dir / "PROGRESS.md"
    started_at = datetime.now()

    candidates = list(log_dir.glob("*.log"))
    if candidates:
        earliest = min(p.stat().st_mtime for p in candidates)
        started_at = datetime.fromtimestamp(earliest)

    if args.once:
        out_path.write_text(render_md(log_dir, started_at))
        print(f"wrote {out_path}")
        return 0

    # Set env var so push() picks it up if --ntfy-topic was provided
    if args.ntfy_topic:
        os.environ["NTFY_TOPIC"] = args.ntfy_topic

    def maybe_push(*a, **kw):
        if ntfy_push and args.ntfy_topic:
            try:
                ntfy_push(*a, **kw)
            except Exception as e:
                print(f"[watcher] ntfy push failed: {e}")

    # Notification state — track what we've already announced
    last_milestone = 0  # last percentage we announced
    last_per_chain_done = {c: 0 for c in CHAINS}
    last_block_alert = 0
    started_announced = False
    last_seen_lines: dict[str, int] = {c: 0 for c in CHAINS}  # for event scanning

    if args.ntfy_topic:
        print(f"[watcher] ntfy push enabled → topic={args.ntfy_topic}")
    print(f"watching {log_dir} → {out_path}  every {args.interval}s. Ctrl+C to stop.")
    try:
        while True:
            try:
                out_path.write_text(render_md(log_dir, started_at))
                # ---- ntfy notifications based on summary diff ----
                summ = compute_summary(log_dir)

                # Run-started: first time we see ANY products scraped or any chain-DONE
                if not started_announced and summ["products"] > 0:
                    started_announced = True
                    maybe_push(
                        "First products are flowing — scrape is healthy.",
                        title="Pico — run started",
                        priority="default",
                        tags=["rocket"],
                    )

                # Milestone: every N% completion (overall)
                if summ["branches_total"] > 0:
                    pct = (summ["branches_done"] / summ["branches_total"]) * 100
                    next_milestone = (
                        (last_milestone // args.ntfy_milestone_pct + 1)
                        * args.ntfy_milestone_pct
                    )
                    if pct >= next_milestone and pct < 100:
                        last_milestone = int(pct // args.ntfy_milestone_pct) * args.ntfy_milestone_pct
                        maybe_push(
                            f"{summ['branches_done']}/{summ['branches_total']} "
                            f"branches done · {summ['products']:,} products · "
                            f"{summ['blocks']} blocks",
                            title=f"Pico — {int(pct)}% complete",
                            priority="default",
                            tags=["chart_increasing"],
                        )

                # Per-chain completion (when a chain hits 100%)
                for c in CHAINS:
                    pc = summ["per_chain"][c]
                    if (pc["done"] >= pc["total"] > 0
                            and last_per_chain_done[c] < pc["total"]):
                        last_per_chain_done[c] = pc["done"]
                        maybe_push(
                            f"{pc['done']}/{pc['total']} done · "
                            f"{pc['products']:,} products · "
                            f"{pc['saved']:,} upserted · "
                            f"{pc['blocks']} blocks · {pc['retries']} retries",
                            title=f"Pico — {c} done ✅",
                            priority="high",
                            tags=["white_check_mark"],
                        )

                # Block-spike alert
                if summ["blocks"] >= last_block_alert + args.ntfy_block_threshold:
                    last_block_alert = (
                        (summ["blocks"] // args.ntfy_block_threshold)
                        * args.ntfy_block_threshold
                    )
                    maybe_push(
                        f"Total blocks: {summ['blocks']} · retries: {summ['retries']} · "
                        f"products so far: {summ['products']:,}",
                        title=f"Pico — block count crossed {last_block_alert}",
                        priority="high",
                        tags=["warning"],
                    )

                # Adaptive drops, branch fails, category fails/blocks/retries (event-driven)
                for ev in scan_new_events(log_dir, last_seen_lines):
                    if ev["type"] == "drop":
                        maybe_push(
                            f"{ev['blocks']} cumulative blocks -- "
                            f"concurrency dropped {ev['old']} -> {ev['new']}",
                            title=f"Pico [{ev['chain']}] -- conc {ev['old']} -> {ev['new']}",
                            priority="high",
                            tags=["arrow_down", "warning"],
                        )
                    elif ev["type"] == "fail":
                        maybe_push(
                            f"{ev['err']}",
                            title=f"Pico [{ev['chain']}] branch failed: {ev['branch']}",
                            priority="high",
                            tags=["x", "warning"],
                        )
                    elif ev["type"] == "cat_block":
                        maybe_push(
                            f"{ev['kind']} on {ev['url']}",
                            title=f"Pico [{ev['chain']}] -- category blocked, retrying",
                            priority="default",
                            tags=["construction"],
                        )
                    elif ev["type"] == "cat_fail":
                        maybe_push(
                            f"{ev['err']} on {ev['url']}",
                            title=f"Pico [{ev['chain']}] -- category failed, retrying",
                            priority="default",
                            tags=["construction"],
                        )
                    elif ev["type"] == "cat_retry_ok":
                        maybe_push(
                            f"recovered {ev['n']} products on retry",
                            title=f"Pico [{ev['chain']}] -- category retry OK",
                            priority="low",
                            tags=["recycle"],
                        )
                    elif ev["type"] == "cat_retry_fail":
                        maybe_push(
                            f"category gave up after retry: {ev['url']}",
                            title=f"Pico [{ev['chain']}] -- category retry FAILED",
                            priority="high",
                            tags=["x"],
                        )

                # All chains done → final summary push
                if (summ["branches_total"] > 0
                        and summ["branches_done"] >= summ["branches_total"]):
                    maybe_push(
                        f"branches: {summ['branches_done']}/{summ['branches_total']} · "
                        f"products: {summ['products']:,} · "
                        f"upserted: {summ['saved']:,} · "
                        f"blocks: {summ['blocks']} · "
                        f"retries: {summ['retries']}",
                        title="Pico — full run complete 🎉",
                        priority="max",
                        tags=["tada"],
                    )
                    # one-shot — break the loop
                    print("[watcher] all chains done — exiting")
                    break
            except Exception as e:
                print(f"[watcher] render failed: {e}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[watcher] stopped")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
