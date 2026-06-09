# Scraper Operations Guide

## Folder to run from

**Always `cd` into the `claude scrapers` directory first:**

```bash
cd "/home/boiledpotato/Downloads/scrapers/claude scrapers"
```

All three scrapers read `../.env` (i.e. `scrapers/.env`) for Supabase credentials. Running from any other directory breaks the env path lookup.

---

## 1. Woolworths NZ (`woolworths_claude.py`)

### How to run

```bash
# Full production run — 186 branches, 5 in parallel
python3 woolworths_claude.py --all-branches --concurrency 5 --fast-categories

# With proxy pool (recommended — reduces Akamai block rate)
python3 woolworths_claude.py --all-branches --concurrency 5 --fast-categories --proxy-file proxiesthatwork.txt

# Single branch test, no DB writes
python3 woolworths_claude.py --test --dry-run

# Single named branch
python3 woolworths_claude.py --branch "Woolworths Ponsonby"
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
        # retry once on HTTP 500
    return page_num, None

page_results = await asyncio.gather(
    *[_fetch_page(p) for p in range(2, total_pages + 1)]
)
# reassemble in sorted page order — no data mixing
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

`asyncio.Semaphore(--concurrency)` gates how many branches run at once. At `--concurrency 5`, 5 full browser sessions + their page-fetch goroutines run simultaneously. The scraper at line 1389 creates this semaphore and wraps each `run_one(branch)` call with it.

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
python3 newworld_claude.py --test --dry-run
python3 paknsave_claude.py --test --dry-run

# Resume an interrupted run (skips already-finished branches)
python3 newworld_claude.py --all-branches --concurrency 5 --fast-categories --resume

# Single named branch
python3 newworld_claude.py --branch "New World New Lynn"
python3 paknsave_claude.py --branch "PAK'nSAVE Sylvia Park"
```

**Key flags:**

| Flag | What it does |
|---|---|
| `--all-branches` | Scrape all branches with a valid `api_store_id` |
| `--concurrency N` | Branches in parallel (uses `AdaptiveSemaphore`) |
| `--fast-categories` | Direct POST path for categories 2..N — skip browser navigation |
| `--resume` | Skip already-completed branches from checkpoint file |
| `--no-adaptive-drop` | Keep concurrency fixed even when 429s hit |
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

Standard `playwright` is used for Woolworths. For both New World and Pak'nSave, `patchright` is used instead:

```python
# foodstuffs_claude.py line 47
from patchright.async_api import async_playwright, Browser, BrowserContext, Page
```

`patchright` patches browser fingerprints at the CDP level (specifically `Runtime.enable`) so Cloudflare's bot detection does not flag the browser. No manual stealth scripts are needed.

Before the first category page loads, `nodriver` is used to solve the Turnstile challenge:

```python
async def _solve_cf_clearance(cfg: dict) -> tuple[list[dict], Optional[str]]:
    import nodriver as uc
    # nodriver opens a real Chromium, passes the Turnstile, extracts:
    #   cf_clearance, __cf_bm, _cfuvid cookies
    ...
    return cookies, user_agent
```

These CF cookies are injected into every patchright browser context at startup. Because they were obtained by a real Chrome solving the challenge, Cloudflare accepts them.

**2. One shared CF solve — reused across all workers**

The CF solve is expensive (~4–8 seconds). Rather than solving once per branch (× 148 branches = ~15 minutes of just solving CAPTCHAs), a single `CfState` object is created and shared across all worker coroutines:

```python
cf_state = CfState()
await cf_state.solve(cfg)   # solved once before any branches start
```

When a worker starts, it reads from `cf_state.cookies` — no extra solve. If a worker hits a 403 mid-run, it calls `cf_state.ensure_fresh(cfg)` which re-solves behind an asyncio lock, and all other workers waiting for the lock immediately reuse the fresh cookies without solving again.

**3. Automatic CF refresh every 25 minutes**

CF cookies expire. A background task re-solves automatically every 25 minutes so no worker ever gets a stale cookie:

```python
async def _periodic_cf_refresh():
    while True:
        await asyncio.sleep(25 * 60)   # 25 minutes
        await cf_state.solve(cfg)

refresh_task = asyncio.create_task(_periodic_cf_refresh())
```

The task runs concurrently with all branch workers and is cancelled after the last branch finishes.

**4. Fast path for categories (`--fast-categories`)**

The first category per branch uses full browser navigation (POST request intercepted and captured). Every subsequent category uses `_scrape_category_direct`, which replays the captured POST body with a modified `category0SI` filter — no browser navigation needed:

```python
# Template captured from first browser category
self._direct_template = captured_post_body
self._direct_url = captured_post_url
self._direct_headers = captured_headers

# Direct POST for categories 2..N
resp = await self._page.request.post(
    self._direct_url,
    headers=self._direct_headers,
    data=modified_body   # only category0SI changes per category
)
```

Pages 2..N within each category are fetched sequentially via direct POST (governed by the `TokenBucketLimiter` — 4 req/sec max globally across all workers).

**5. Fallback if fast path fails**

If `_scrape_category_direct` returns `None` (0 products, HTTP error, unknown category slug, or template not yet captured), the caller automatically falls back to the full browser navigation path:

```python
used_direct = False
if fast_categories and self._direct_template:
    direct_products = await self._scrape_category_direct(url)
    if direct_products is not None:
        products = direct_products
        used_direct = True

if not used_direct:
    products, did_paginate = await self.scrape_one_category(url)  # full browser fallback
```

No data is lost — the fallback is silent and automatic.

**6. Non-blocking Supabase writes**

Same pattern as Woolworths — the synchronous Supabase SDK is offloaded to a thread pool so the asyncio event loop stays free for other branches while the DB write runs:

```python
await loop.run_in_executor(None, self._save_to_supabase, all_products, stats)
await loop.run_in_executor(None, lambda: self._end_run(run_id, status, stats))
```

**7. Branch concurrency — `AdaptiveSemaphore`**

Unlike Woolworths which uses a plain `asyncio.Semaphore`, Foodstuffs uses an `AdaptiveSemaphore` that can reduce concurrency at runtime:

- Starts at `--concurrency` (e.g. 5)
- Counts cumulative CF blocks across all running branches
- When blocks exceed a threshold (scales with concurrency), calls `.downgrade()` — reduces active concurrency by 1
- Concurrency is logged in the `DONE` line as `final_concurrency=N`

This means a heavy run that triggers many blocks automatically slows itself down to avoid triggering more, instead of crashing.

**8. `--resume` flag**

After each branch completes successfully, its branch UUID is appended to a checkpoint file:

```
.newworld_checkpoint.json   (New World)
.paknsave_checkpoint.json   (Pak'nSave)
```

If a run is interrupted (power cut, crash, manual stop), restarting with `--resume` reads the checkpoint and skips any branch already in it:

```python
# On --resume startup:
completed_ids = set(json.loads(checkpoint_file.read_text()))
branches = [b for b in branches if b.get("id") not in completed_ids]
# logs: "[resume] skipping 73 already-completed branches, 75 remaining"
```

Without `--resume`, the checkpoint file is deleted at startup so the run starts clean.

**10. Price change detection fix**

**Problem:** New World and Pak'nSave were always reporting `changes=0` — no price history was ever written.

**Root cause:** The original code tried to read the "old" price from the Supabase upsert response:

```python
# BROKEN — upsert returns post-update rows, so old_price == new_price always
r = self.supabase.table("store_products").upsert(chunk, ...).execute()
for row in r.data:
    old_price = row.get("current_price")   # this is already the NEW value
    new_price = new_price_map.get(pid)
    if abs(float(old_price) - new_price) > 0.001:  # always False
```

Supabase returns the row **after** the update, not before. So `old_price` always equalled `new_price`, the diff was always 0, and `price_history` was never written for either chain.

**Fix:** Snapshot existing prices with a `SELECT` **before** doing any upserts — the same approach Woolworths already used:

```python
# FIXED — fetch existing prices first, then compare against those
existing_map: dict[str, dict] = {}
while True:
    r = (self.supabase.table("store_products")
         .select("id,product_id,current_price,unit_price")
         .eq("store_id", self.branch_id)
         .range(offset, offset + 999).execute())
    for row in r.data:
        existing_map[row["product_id"]] = row
    if len(r.data) < 1000:
        break
    offset += 1000

# Now compare new prices against genuine old prices
for product_id, effective in new_price_map.items():
    existing = existing_map.get(product_id)
    if existing and existing.get("current_price") is not None:
        old_price = float(existing["current_price"])
        if abs(old_price - effective) > 0.001:
            ph_rows.append({...})   # correctly records the real price change
```

The upsert and price_history insert are now separate steps. Adds one paginated `SELECT` per branch (~5 extra queries for a typical branch with 5,000 products), which is negligible against the rest of the run time.

---

**9. Parallel barcode enrichment + disk cache**

Barcodes are not in the product listing response — each product needs a separate detail API call. Two optimisations:

- **Parallel fetching**: up to 12 barcode detail calls fire simultaneously per batch (`asyncio.gather` over batches)
- **Disk cache**: every `productId → barcode` mapping is persisted to `.foodstuffs_cache.json`. On the next run, cached products skip the API call entirely

The cache is shared between New World and Pak'nSave (same Foodstuffs catalogue, same productIds). The `DONE` line reports `barcodes(cache/fetched)=X/Y` — cache hits vs actual API calls.

> **Important:** Do NOT delete `.foodstuffs_cache.json`. It contains hundreds of thousands of barcode mappings. Deleting it forces a full re-enrichment on the next run.

---

## Recommended run order

Run in this order so New World warms the barcode cache before Pak'nSave:

1. Woolworths
2. New World
3. Pak'nSave

---

## Monitoring a live run

```bash
# Follow live progress
tail -f logs/woolworths_$(date +%Y-%m-%d).log | grep -E "progress:|DONE|WARNING|ERROR"

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
