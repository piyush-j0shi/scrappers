# Scraper Operations Guide

## Folder to run from

**Always `cd` into the `claude scrapers` directory first:**

```bash
cd "/home/boiledpotato/Downloads/scrapers/claude scrapers"
```

All three scrapers read `../.env` (i.e. `scrapers/.env`) for Supabase credentials. Running from any other directory breaks the env path lookup.

---

## Running the full stack

Three things to run. Start them in this order:

**Terminal 1 — Monitor API:**
```bash
uvicorn scraper_api:app --host 0.0.0.0 --port 8765
```

**Terminal 2 — React dashboard:**
```bash
cd ../monitor-ui && npm run dev
```
Open `http://localhost:5173` — auto-refreshes every 5 seconds as branches complete.

**Terminal 3+ — Scrapers (see below)**

---

## 1. Woolworths NZ (`woolworths_claude.py`)

### How to run

```bash
# Full production run — 186 branches, 5 in parallel
python3 woolworths_claude.py --all-branches --concurrency 5 --fast-categories

# With proxy pool (recommended — reduces Akamai block rate)
python3 woolworths_claude.py --all-branches --concurrency 5 --fast-categories --proxy-file proxiesthatwork.txt

# Single branch test, no DB writes
python3 woolworths_claude.py --test --fast-categories --dry-run

# Single named branch
python3 woolworths_claude.py --branch "Woolworths Ponsonby" --fast-categories
```

**Key flags:**

| Flag | What it does |
|---|---|
| `--all-branches` | Scrape all 186 branches |
| `--concurrency N` | Run N branches in parallel (uses `asyncio.Semaphore(N)`) |
| `--fast-categories` | Use direct-API fast path for pages 2..N — skip browser navigation |
| `--proxy-file FILE` | Round-robin proxy pool (one URL per line) |
| `--test` | 3 categories, default branch only (Ponsonby) |
| `--dry-run` | Scrape but skip all Supabase writes |
| `--no-headless` | Show the browser window |

**Log location:** `logs/woolworths_YYYY-MM-DD.log`

**Latest run result (2026-06-08):**
```
branches=186/186  updated=180,596  new=54  changes=72,977
failed=0  blocks=22  retries=22  elapsed=3h 5min
```

---

### What was the problem

The full run was taking **~4 hours 51 minutes** and finishing with **4 failed branches**.

The bottleneck was **sequential page fetching inside `_scrape_category_direct`**: pages 2 through N were fetched one at a time with a 400ms sleep between each. With ~342 total pages per branch (pantry = 97 pages, health-body = 60, household = 45, etc.), this meant ~342 sequential API calls × ~1 second each = **~5–6 minutes just in page fetching per branch**. At 5-way concurrency across 186 branches, almost all wall-clock time was spent waiting.

---

### What was fixed and how

**1. Parallel page fetching — primary speedup**

The `for` loop + `asyncio.sleep(0.4)` in `_scrape_category_direct` was replaced with `asyncio.gather` + `asyncio.Semaphore(3)`:

```python
# OLD — sequential, one page at a time
for page_num in range(2, total_pages + 1):
    r = await self._page.request.get(page_url, headers=self._fast_headers)
    all_data.append(await r.json())
    await asyncio.sleep(0.4)

# NEW — up to 3 pages fetched simultaneously
_sem = asyncio.Semaphore(3)

async def _fetch_page(page_num):
    page_url = re.sub(r"\bpage=\d+\b", f"page={page_num}", direct_url)
    async with _sem:
        r = await self._page.request.get(page_url, headers=self._fast_headers)
        if r.ok:
            return page_num, await r.json()
    return page_num, None

page_results = await asyncio.gather(
    *[_fetch_page(p) for p in range(2, total_pages + 1)]
)
for _, page_data in sorted(page_results, key=lambda x: x[0]):
    if page_data is not None:
        all_data.append(page_data)
```

The `Semaphore(3)` limits concurrent requests to 3 per category to avoid 429s. `sorted(...)` ensures products come out in correct page order regardless of which request finished first.

**2. Fast path (`--fast-categories` flag)**

The first category per branch uses full browser navigation to load the page and capture the internal API URL + auth headers (`_fast_template_url`, `_fast_headers`). Every subsequent category then uses `_scrape_category_direct` — it calls the captured API directly, skipping browser navigation entirely. This saves the 2–5 second random delay per category that the browser path requires.

If `_scrape_category_direct` returns `None` (0 products, HTTP error, or missing template), the caller automatically falls back to the full browser navigation path (`scrape_one_category`). Fallback is transparent — no data is lost.

**3. Non-blocking Supabase writes**

The Supabase Python SDK is synchronous. Instead of blocking the asyncio event loop while writing ~20,000 products per branch, the write is offloaded to a thread pool:

```python
await loop.run_in_executor(None, self._save_to_supabase, all_products, stats)
```

This means 5 branches can be scraping simultaneously while their individual writes run in threads — the event loop is never stalled waiting for a DB round-trip.

**4. Query optimization**

- Existing `store_products` are fetched in paginated chunks of 1000 rows (not one giant query)
- Products table bulk-upserted in chunks of 200 on `conflict=barcode`
- Barcode snapshot uses `.in_("barcode", chunk)` in 500-item batches (indexed lookup, not a full table scan)

**5. Branch concurrency via Semaphore**

`asyncio.Semaphore(--concurrency)` gates how many branches run at once. At `--concurrency 5`, 5 full browser sessions + their page-fetch goroutines run simultaneously.

**Result:** 4h 51min → **3h 5min** (37% faster), 4 failed branches → **0 failed**.

---

## 2. New World (`newworld_claude.py`) & Pak'nSave (`paknsave_claude.py`)

Both chains run off one shared module (`foodstuffs_claude.py`). `newworld_claude.py` and `paknsave_claude.py` are thin wrappers that preset `--chain newworld` or `--chain paknsave` respectively. All flags are identical.

### How to run

```bash
# New World — full run, 148 branches, 5 parallel
python3 newworld_claude.py --all-branches --concurrency 5 --fast-categories

# Pak'nSave — full run, 58 branches, 5 parallel
python3 paknsave_claude.py --all-branches --concurrency 5 --fast-categories

# Single branch test, no DB writes
python3 newworld_claude.py --test --fast-categories --dry-run
python3 paknsave_claude.py --test --fast-categories --dry-run

# Resume an interrupted run (skips already-finished branches)
python3 newworld_claude.py --all-branches --concurrency 5 --fast-categories --resume
python3 paknsave_claude.py --all-branches --concurrency 5 --fast-categories --resume

# Single named branch
python3 newworld_claude.py --branch "New World New Lynn" --fast-categories
python3 paknsave_claude.py --branch "PAK'nSAVE Sylvia Park" --fast-categories
```

**Key flags:**

| Flag | What it does |
|---|---|
| `--all-branches` | Scrape all branches with a valid `api_store_id` |
| `--concurrency N` | Branches in parallel (uses `AdaptiveSemaphore`) |
| `--fast-categories` | Direct POST path for categories 2..N — skip browser navigation |
| `--resume` | Skip already-completed branches from checkpoint file |
| `--no-adaptive-drop` | Keep concurrency fixed even when CF blocks hit |
| `--test` | 3 categories, default branch only |
| `--dry-run` | Scrape but skip Supabase writes |

**Log location:** `logs/newworld_YYYY-MM-DD.log` / `logs/paknsave_YYYY-MM-DD.log`

**Latest run results:**

New World (2026-06-08):
```
chain=New World  branches=148  updated=1,120,948  new=107
failed=0  blocks=0  final_concurrency=5  elapsed=~2h 19min
```

Pak'nSave (2026-06-04):
```
chain=Pak'nSave  branches=58  updated=273,129  new=127
failed=0  blocks=934  final_concurrency=5  elapsed=~1h 28min
```
(`blocks=934` is normal for Pak'nSave — the site rate-limits barcode enrichment aggressively. `failed=0` means everything resolved.)

---

### What was the problem

Both New World and Pak'nSave are behind **Cloudflare Turnstile** — a JS challenge that detects and blocks headless browsers. Standard Playwright fails immediately with a 403 or challenge page before even reaching a category. Additionally, a solved CF challenge (the `cf_clearance` cookie) expires after ~30 minutes, so in a multi-hour run it would expire mid-run and start failing.

The secondary bottleneck was **serial barcode enrichment**: neither product listing endpoint includes the barcode directly. Each product needs a separate detail API call (`/v1/edge/store/{storeId}/product/{productId}`). With ~1,500 products per branch × 148 branches, doing this serially would take hours.

---

### What was fixed and how

**1. Cloudflare bypass — `patchright` + `nodriver`**

Standard `playwright` is used for Woolworths. For both New World and Pak'nSave, `patchright` is used instead — it patches browser fingerprints at the CDP level so Cloudflare's bot detection does not flag the browser.

Before the first category page loads, `nodriver` solves the Turnstile challenge and extracts `cf_clearance`, `__cf_bm`, `_cfuvid` cookies. These are injected into every patchright browser context at startup.

**2. One shared CF solve — reused across all workers**

A single `CfState` object is solved once before any branches start, then shared across all worker coroutines. When a worker hits a 403 mid-run, it calls `cf_state.ensure_fresh(cfg)` which re-solves behind an asyncio lock — all other waiting workers immediately reuse the fresh cookies without solving again.

**3. CF startup retry**

If the initial startup CF solve returns empty cookies (Chrome failed to connect on first attempt), the scraper waits 3 seconds and retries before any workers start. Previously, workers would start with empty cookies, the first worker to run would re-solve while all others stalled waiting behind the lock — causing a visible 19-second stall at the start of every run.

```python
await cf_state.solve(cfg)
if not cf_state.cookies:
    await asyncio.sleep(3)
    await cf_state.solve(cfg)   # retry once before workers start
```

**4. Automatic CF refresh every 25 minutes**

A background task re-solves automatically every 25 minutes so no worker ever gets a stale cookie mid-run.

**5. Fast path for categories (`--fast-categories`)**

The first category per branch captures the POST body + headers from browser navigation. Every subsequent category replays that captured request with only the category filter changed — no browser navigation needed. Falls back silently to browser if the direct path returns nothing.

**6. Non-blocking Supabase writes**

Synchronous Supabase SDK offloaded to thread pool so the event loop stays free for other branches during DB writes.

**7. AdaptiveSemaphore — concurrency drops on CF block waves, recovers after**

Unlike Woolworths which uses a plain `asyncio.Semaphore`, Foodstuffs uses an `AdaptiveSemaphore`:

- Starts at `--concurrency` (e.g. 5)
- When cumulative CF blocks exceed a threshold, `.downgrade()` reduces active concurrency by 1 to back off
- After **5 consecutive clean branches** with no blocks, `.upgrade()` restores concurrency directly back to the original max — no gradual climb
- `--no-adaptive-drop` disables this entirely and keeps concurrency fixed

```
CF wave hits → drops to conc=2 → 5 clean branches → jumps straight back to conc=5
```

**8. `--resume` checkpoint**

After each branch completes with no category failures, its UUID is written to a checkpoint file:
```
.newworld_checkpoint.json
.paknsave_checkpoint.json
```

Restarting with `--resume` skips already-completed branches. Without `--resume`, the checkpoint is deleted and the run starts clean.

**When to use `--resume`:** Only add `--resume` when restarting after an interrupted run (crash, power cut, manual `Ctrl+C`). Do NOT use it on a fresh day's run — it will skip branches that were completed yesterday. Without `--resume`, the checkpoint file is deleted automatically and the run starts from scratch.

**Checkpoint fix:** Previously the checkpoint was never written because the condition checked `records_failed == 0`, but every branch has ~7 unresolvable products (no barcode, no name match) which set `records_failed > 0`. Fixed to only check `categories_failed == 0` — a branch is checkpointed if all its categories returned data, regardless of individual product-level failures.

**9. Category failure detection fix**

Previously `if not products:` after the retry loop would count legitimately empty categories (e.g. `meat-and-seafood-deals` with no current deals) as failures. This caused `categories_failed` to increment on every branch, which blocked checkpointing entirely.

Fixed to only count as a failure when the empty result was caused by an actual block or exception:

```python
# Before — false positives on empty-but-valid categories
if not products:
    stats["categories_failed"] += 1

# After — only real failures
if not products and (exc is not None or blocked):
    stats["categories_failed"] += 1
```

**10. Price change detection fix**

New World and Pak'nSave were always reporting `changes=0`. Root cause: the code read the "old" price from the upsert response, but Supabase returns the row *after* the update — so `old_price` always equalled `new_price`.

Fix: snapshot existing prices with a `SELECT` before doing any upserts, then compare against those genuine old values.

**11. Parallel barcode enrichment + disk cache**

- Up to 12 barcode detail calls fire simultaneously per batch via `asyncio.gather`
- Every `productId → barcode` mapping is persisted to `.foodstuffs_cache.json` and reused on future runs
- Cache is shared between New World and Pak'nSave (same Foodstuffs catalogue)

> **Important:** Do NOT delete `.foodstuffs_cache.json`. Deleting it forces a full re-enrichment on the next run.

---

## Monitor API & Dashboard

Branch results are POSTed to a local FastAPI server after every branch completes across all three scrapers.

**Files:**
- `scraper_api.py` — FastAPI server
- `report_client.py` — sync HTTP client used by all scrapers
- `../monitor-ui/` — React dashboard

**API endpoints:**

| Endpoint | What it returns |
|---|---|
| `POST /branch-complete` | Receives a branch report from a scraper |
| `GET /reports` | All branch reports this session |
| `GET /reports/{chain}` | Reports filtered by chain name |
| `GET /summary` | Total count, chains seen, last received time |
| `DELETE /reports` | Clear all in-memory reports |
| `GET /docs` | Swagger UI (FastAPI auto-generated) |

**Payload per branch report:**
- `chain`, `branch_name`, `branch_id`, `store_id`
- `status` — `success` / `partial` / `failed`
- `total_products`, `price_changes`, `specials`, `out_of_stock`
- `categories` — array of `{name, status, products, reason}` where status is `success` / `empty` / `failed`

**Note:** Reports are held in memory only. Restarting the API server clears all reports.

---

## Recommended run order

Run in this order so New World warms the barcode cache before Pak'nSave. **Wait ~30 minutes between each scraper** — starting them back-to-back hammers the sites simultaneously and increases block rates.

1. Start monitor API + dashboard
2. Woolworths → wait for it to finish, then wait 30 min
3. New World → wait for it to finish, then wait 30 min
4. Pak'nSave

If a run is interrupted mid-way, restart with `--resume` to pick up where it left off:
```bash
python3 newworld_claude.py --all-branches --concurrency 5 --fast-categories --resume
python3 paknsave_claude.py --all-branches --concurrency 5 --fast-categories --resume
```
Woolworths does not have `--resume` — it is stateless and re-runs all branches from scratch.

---

## After a run completes — scraping missing data

When a run finishes, check the monitor dashboard (`http://localhost:5173`) for any branches marked `partial` or `failed`, or any red category pills (CF blocked categories).

### New World & Pak'nSave

Branches that had any category failure are **not written to the checkpoint file**, so they will be picked up automatically on the next `--resume` run. Simply re-run with `--resume` after the full run finishes:

```bash
python3 newworld_claude.py --all-branches --concurrency 5 --fast-categories --resume
python3 paknsave_claude.py --all-branches --concurrency 5 --fast-categories --resume
```

This will skip all cleanly completed branches and only re-scrape the ones that had failures. Repeat until the monitor shows all branches as `success`.

### Woolworths

Woolworths has no checkpoint. To re-scrape a specific branch that failed, run it by name:

```bash
python3 woolworths_claude.py --branch "Woolworths Ponsonby" --fast-categories
```

To re-scrape all branches (e.g. if the block rate was high and many branches got partial data), just run the full command again — it starts from scratch:

```bash
python3 woolworths_claude.py --all-branches --concurrency 5 --fast-categories
```

---

## Monitoring a live run

```bash
# Follow live progress
tail -f logs/woolworths_$(date +%Y-%m-%d).log | grep -E "progress:|DONE|WARNING|ERROR"
tail -f logs/newworld_$(date +%Y-%m-%d).log | grep -E "progress:|DONE|checkpoint|WARNING|ERROR"

# Check specials and OOS samples for a completed branch
grep -E "\[specials\]|\[oos\]" logs/newworld_$(date +%Y-%m-%d).log

# Get final summary line
grep "DONE" logs/woolworths_2026-06-08.log
```

**DONE line fields:**

| Field | Meaning |
|---|---|
| `branches=X/Y` | Branches completed / total |
| `failed=N` | Branches that errored out (0 = clean run) |
| `blocks=N` | CF/Akamai challenges hit (auto-recovered) |
| `changes=N` | Price changes recorded to `price_history` |
| `final_concurrency=N` | Foodstuffs: concurrency after any adaptive drops |
| `barcodes(cache/fetched)=X/Y` | Foodstuffs: cache hits vs API calls |
| `elapsed=` | Total wall time |

**Checkpoint files:**

| File | Chain |
|---|---|
| `.newworld_checkpoint.json` | New World |
| `.paknsave_checkpoint.json` | PAK'nSAVE |
