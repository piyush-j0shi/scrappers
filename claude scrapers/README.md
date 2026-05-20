# Pico Scrapers — Claude build

Self-contained scrapers for the three big NZ supermarket chains, all in
`scrapers/claude scrapers/`. None of these files modify or import the existing
scraper code in the parent directory. All three write to the live Supabase
schema using safe upserts — nothing destructive.

| Chain | Entry point | Wrapper(s) |
|---|---|---|
| Woolworths NZ | `woolworths_claude.py` | — |
| New World (Foodstuffs) | `foodstuffs_claude.py --chain newworld` | `newworld_claude.py` |
| Pak'nSave (Foodstuffs) | `foodstuffs_claude.py --chain paknsave` | `paknsave_claude.py` |

Orchestrator + tooling:

| File | Purpose |
|---|---|
| **`run_all.py`** | **One-shot orchestrator — runs all 3 chains in parallel as subprocesses** |
| `requirements.txt` | Playwright 1.44, supabase 2.5, python-dotenv, aiohttp |
| `test_proxy.py` | Single-proxy verification |
| `test_proxy_bulk.py` | Bulk-test a list of proxies against all 3 retailers |
| `sample_check.py` | Sanity utility: prints 3 sample Woolworths products from bakery |
| `.foodstuffs_cache.json` | Auto-generated productId→barcode cache (DO NOT delete) |
| `PROXY_TEST_GUIDE.md` | Step-by-step proxy purchase + test flow |

## TL;DR — daily run command

```bash
# The recommended hybrid: WW via proxies, NW + PS from home IP, all in parallel
python3 run_all.py --proxy-file proxiesthatwork.txt
# expected wall time: ~3.7 hours
```

---

## 1. Woolworths NZ

The Woolworths storefront is a React SPA backed by `https://www.woolworths.co.nz/api/v1/products`.
The scraper captures that endpoint as the page loads, then paginates pages 2..N
directly using the captured auth headers.

**API field → Pico field**

| API | Pico |
|---|---|
| `name` | `raw_name` / `clean_name` |
| `brand` | `brand` |
| `barcode` | `barcode` |
| `price.originalPrice` | `price` |
| `price.salePrice` | `special_price` |
| `size.volumeSize` | `weight` |
| `images.big` | `image_url` |
| `availabilityStatus`, `stockLevel` | `in_stock` |

**Branch pinning:** uses saved Playwright session JSONs at
`scrapers/sessions/woolworths/<branch_uuid>.json` (186 branches available).
Each session was captured by `bootstrap_woolworths_sessions.py`.

### CLI

```bash
python3 woolworths_claude.py --test                       # 3 categories, default branch (Ponsonby)
python3 woolworths_claude.py --test --dry-run             # parse only, no DB writes
python3 woolworths_claude.py                              # all 12 categories, default branch
python3 woolworths_claude.py --branch "Woolworths Ponsonby"
python3 woolworths_claude.py --branch-id <uuid>
python3 woolworths_claude.py --all-branches               # loops every branch with a saved session
python3 woolworths_claude.py --categories fruit-veg,bakery
python3 woolworths_claude.py --no-headless                # show browser
```

### Verified test run (2026-05-05, Ponsonby)

| Categories | Products | Time | Result |
|---|---:|---:|---|
| fruit-veg + bakery + drinks | 2,125 | 137 s | 2,112 store_products written, 40 new, 758 price changes, 0 failures, 100% barcoded |

---

## 2. New World + Pak'nSave (Foodstuffs)

Both chains share the same Foodstuffs backend, so they're driven by one shared
module (`foodstuffs_claude.py`) with two thin wrappers (`newworld_claude.py`,
`paknsave_claude.py`).

### How it works

The site is a Next.js SPA backed by a `paginated/products` POST endpoint. The
scraper:

1. Loads a category page → captures the POST request + body + auth headers
2. **Overrides `storeId` in the POST body** to pin to a specific branch — no
   saved session required (uses `api_store_id` from `store_branches`)
3. Paginates pages 2..N via direct `page.request.post`
4. **Enriches barcodes** by hitting
   `https://api-prod.newworld.co.nz/v1/edge/store/{store_id}/product/{productId}`
   for each unique productId — the barcode (`sku`) lives only in that detail call

### Parallel barcode enrichment with adaptive concurrency

Foodstuffs barcode enrichment is the historical bottleneck (the existing
scrapers do it serially). This build:

- **Starts at concurrency=12** (12 parallel detail calls)
- **Downscales** to 6 → 3 → 1 if it sees ≥3 HTTP 429s in a single batch
- **Persistent disk cache** at `.foodstuffs_cache.json` — `{productId: barcode}`
- **Cross-chain shared** — same cache serves both NW and PS (huge overlap on
  Foodstuffs catalogue)

Effect on a real run (verified):
- First NW `--test` run: 1,497 enrichment calls in **12 s**
- First PS `--test` run after cache warmed: 834/1,352 cache hits, only 518 API calls (5 s)
- Second NW `--test` run: 1,483/1,496 cache hits, only 13 API calls (0.4 s)
- Second PS `--test` run: **100% cache hits, zero API calls**

### Asset blocking

Because Foodstuffs page navigations are heavier than Woolworths', this build
aborts requests for `*.png/jpg/webp/woff/css` and known analytics/ad domains
before they hit the network. Cuts page-load weight roughly 70% with no
functional impact.

### CLI

```bash
# Two equivalent forms:
python3 foodstuffs_claude.py --chain newworld --test
python3 newworld_claude.py --test                          # convenience wrapper

python3 foodstuffs_claude.py --chain paknsave --test
python3 paknsave_claude.py --test                          # convenience wrapper

# All flags work on both:
python3 newworld_claude.py --branch "New World New Lynn"
python3 paknsave_claude.py --branch-id <uuid>
python3 newworld_claude.py --all-branches
python3 paknsave_claude.py --categories fruit-and-vegetables,bakery
python3 newworld_claude.py --no-headless --dry-run
```

### Default branches

| Chain | Default branch |
|---|---|
| New World | New World New Lynn |
| Pak'nSave | PAK'nSAVE Sylvia Park (uppercase — there's a duplicate row with mixed case that lacks `api_store_id`) |

### Verified test runs (2026-05-06, default branches)

| Chain | Categories | Scraped | Saved | New | Changes | Failed | Time |
|---|---|---:|---:|---:|---:|---:|---:|
| New World | fruit-veg + bakery + drinks | 1,496 | **1,495** | 21 | 227 | 0 | **97 s** |
| Pak'nSave | fruit-veg + bakery + drinks | 1,352 | **1,351** | 18 | 643 | 0 | **75 s** |

100% barcoded for both (after enrichment). Cache state after both runs:
**2,028 productId→barcode entries** persisted.

---

## 3. Live Supabase schema

All three scrapers write to the same auto-detected new-schema tables:

| Table | Operation | Conflict key |
|---|---|---|
| `store_chains` | upsert if missing | `slug` |
| `store_branches` | upsert if missing + `last_scraped_at` update | `chain_id, name` |
| `scraper_runs` | insert (running) → update (final) | n/a |
| `products` | upsert / insert | `barcode` (then `name` fallback) |
| `store_products` | upsert | `product_id, store_id` |
| `price_history` | insert | n/a |

The matching pipeline is identical across chains:

1. **Barcode-first bulk upsert** — chunks of 200 with `on_conflict=barcode`
2. **Name-fallback** for items without a barcode (rare for these scrapers
   after the kiwisquare seed) or when a name UNIQUE conflict trips the upsert
3. **Price-change detection** — compare new vs existing `current_price`,
   record diffs in `price_history`

Old tables (`stores`, `prices`, `grocery_lists`) do not exist in the live DB.

---

## 4. Install (one-time)

```bash
cd "scrapers/claude scrapers"
pip install -r requirements.txt
playwright install chromium
```

The `.env` at `scrapers/.env` is reused — `SUPABASE_URL` and
`SUPABASE_SERVICE_KEY` must be set there. Same `.env` works for all three
scrapers + the kiwisquare seed.

---

## 5. Design notes & known limits

- **No automatic out-of-stock sweep on partial runs.** Marking every product
  outside the scraped categories as OOS would be wrong, so the scrapers only
  flip `in_stock=false` for items that explicitly report OOS in the API.
- **Name-conflict warnings during upsert.** Postgres' `products_name_key`
  unique constraint sometimes trips when a chunk includes a name that already
  exists with a different barcode. The scraper falls back to per-row name
  resolution and continues — `failed=0` at the end means every item resolved.
- **Foodstuffs cache invalidation.** If a product's `productId` ever gets
  reassigned to a different barcode (rare), delete `.foodstuffs_cache.json`
  to force a refetch. The cache also survives chain swaps — same productId
  across NW and PS resolves to the same barcode.
- **Cloudflare burst sensitivity (Woolworths).** After heavy pagination we
  refresh the browser context. Categories with one page skip the refresh,
  saving wall-clock time.
- **Foodstuffs duplicate branches in DB.** There are some store_branches
  rows with mixed casing (e.g. two "Sylvia Park" entries). The default branch
  config matches the row that has `api_store_id` populated.

---

## 6. Orchestrator (`run_all.py`)

Runs all 3 chains in parallel, each as its own subprocess so that:

- their async event loops don't interfere
- one chain crashing doesn't take down the others
- per-chain logs are tagged and streamed live (`[woolworths] ...`, `[newworld] ...`)

### Usage

```bash
# Recommended hybrid (Woolworths via 150-IP PTW pool + Foodstuffs from home IP)
python3 run_all.py --proxy-file proxiesthatwork.txt

# Conservative — no proxies anywhere, lower concurrency, slower but safer
python3 run_all.py --ww-concurrency 2 --nw-concurrency 2 --ps-concurrency 1

# Test mode — 3 categories per chain (smoke test the whole pipeline in ~5 min)
python3 run_all.py --test --proxy-file proxiesthatwork.txt --dry-run

# Skip a chain you've already done
python3 run_all.py --skip paknsave --proxy-file proxiesthatwork.txt

# Write per-chain log files
python3 run_all.py --proxy-file proxiesthatwork.txt --log-dir runs/2026-05-06/
```

### Default tuning (verified to fit i5-13600K + 32 GB)

| Chain | Concurrency | Strategy | Wall time |
|---|---:|---|---:|
| Woolworths | 25 | Proxy round-robin across PTW pool | ~1.1 hr |
| New World | 4 | Home IP, parallel branches | ~3.7 hr ← bottleneck |
| Pak'nSave | 2 | Home IP, parallel branches | ~2.4 hr |
| **Total wall** | — | — | **~3.7 hr** |

RAM use: ~11 GB / 32 GB (35%). PC remains usable while running.

---

## 7. New flags reference

### Woolworths (`woolworths_claude.py`)

| Flag | Default | Meaning |
|---|---|---|
| `--branch <name>` | "Woolworths Ponsonby" | Single branch by name |
| `--branch-id <uuid>` | — | Single branch by UUID |
| `--all-branches` | — | Loop every branch with a saved session |
| `--categories slug,slug` | (all 12) | Comma-separated category slugs |
| `--test` | — | Sanity-check mode: 3 categories on default branch |
| `--no-headless` | — | Show the browser window |
| `--dry-run` | — | Skip Supabase writes |
| `--proxy URL` | — | Single proxy URL |
| **`--proxy-file PATH`** | — | **File of proxies (one per line) — round-robin across branches** |
| **`--concurrency N`** | 1 | **Branches in parallel** |
| **`--max-session-age N`** | 90 min | **Refresh session if older than N minutes (no-proxy mode only)** |
| **`--no-auto-bootstrap`** | — | **Disable session auto-refresh** |

When `--proxy*` is set, auto-bootstrap is **automatically suppressed** to avoid creating
home-IP-tied sessions that get challenged when used through a proxy. Bootstrap from
home IP first (without `--proxy`) so the saved session is established, then run with
proxies.

### Foodstuffs (`foodstuffs_claude.py`, `newworld_claude.py`, `paknsave_claude.py`)

Same flags as Woolworths plus:

| Flag | Default | Meaning |
|---|---|---|
| `--chain newworld\|paknsave` | required | Which Foodstuffs chain |

No `--proxy-file` for Foodstuffs (datacenter IPs hit Cloudflare 403 — use home IP or ISP/residential).

| Flag | Default | Meaning |
|---|---|---|
| **`--concurrency N`** | 1 | **Branches in parallel** |

---

## 8. Block detection + auto-recovery (Woolworths only)

The Woolworths scraper now automatically detects Akamai challenge pages mid-run and
attempts to recover:

```
1. After scraping a category, check if zero products were captured
2. If yes, peek at the rendered page title/body
3. If it looks like Cloudflare/Akamai challenge ("Just a moment...", "Access Denied"):
    a. If proxy in use:    refresh browser context + retry once
    b. If no proxy:        re-bootstrap session from home IP + retry once
4. If retry succeeds → continue normally
5. If retry fails → log, mark category empty, continue to next category
```

This is logged in the run summary as `blocks=N retries=M`.

---

## 9. What these scrapers do NOT touch

- Existing files in `scrapers/` are not modified.
- `woolworths_scraper.py`, `newworld_scraper.py`, `paknsave_scraper.py`,
  `base_scraper.py`, `config.py` are left intact.
- No Supabase schema migrations are run.
- No data is deleted — all writes are upserts or appends.
- The shared `.env` is read but never written.
