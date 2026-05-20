# Proxy testing guide — IPRoyal + ProxiesThatWork

End-to-end: spend $6 total to get hard data on whether either provider works
for Pico. Should take 30–45 minutes including waiting for both purchases to
provision.

## Strategy

| Provider | Cost | Type | What you get |
|---|---:|---|---|
| **IPRoyal** (1 IP plan) | ~$3 | ISP, US | 1 IP, high confidence, unlimited bandwidth |
| **ProxiesThatWork** (150 plan) | ~$3 | Datacenter, US | 150 IPs, low confidence, fast filter needed |

Test both, see which (or which combination) actually works for the 3 retailers.

---

## Step 1 — Buy both ($6 total)

### IPRoyal
1. https://iproyal.com/isp-proxies/ → smallest plan, 1 IP × 1 month
2. Pick **United States**
3. After payment, dashboard shows: `host:port`, `username`, `password`
4. Build URL: `http://username:password@host:port`

### ProxiesThatWork
1. https://www.proxiesthatwork.com/ → 150-proxy plan
2. After payment, log in → dashboard
3. Add your **home IP** to their whitelist (this is how they auth — no user:pass)
4. Download / copy the proxy list as `host:port` lines (one per line)

## Step 2 — Test IPRoyal first (single IP)

```bash
cd "scrapers/claude scrapers"
python3 test_proxy.py --proxy "http://USER:PASS@host:port"
```

Expected good outcome:

```
[Woolworths] ✅ OK  products=490  barcoded=490
[New World]  ✅ OK  products=325  barcoded=325
[Pak'nSave]  ✅ OK  products=256  barcoded=256
Verdict: ✅ proxy works
```

## Step 3 — Test ProxiesThatWork bulk (150 IPs)

Save the list from their dashboard to a file, e.g. `proxiesthatwork.txt`:

```text
# one proxy URL per line
192.46.205.10:12321
192.46.205.11:12321
http://192.46.205.12:12321
# ... 147 more ...
```

Then run the bulk tester:

```bash
python3 test_proxy_bulk.py --proxies-file proxiesthatwork.txt
```

What it does:
1. **Phase 1 (~30 sec):** fires HTTP HEAD against each retailer's homepage
   through every proxy in parallel (20 at a time). Filters out dead/refused
   IPs without launching any browsers.
2. **Phase 2 (~5–15 min):** for survivors, runs the actual bakery scrape
   through Playwright (4 in parallel — Chromium is heavy).

Output written to `proxy_test_results/`:

| File | Contents |
|---|---|
| `working_woolworths.txt` | proxies that returned valid Woolworths products |
| `working_newworld.txt` | …same for New World |
| `working_paknsave.txt` | …same for Pak'nSave |
| `working_all.txt` | proxies that passed all 3 chains |
| `summary.csv` | per-proxy status across every chain |

## Step 4 — Read the verdict

**Realistic outcomes:**

| Pattern | Interpretation | Next step |
|---|---|---|
| IPRoyal ✅ all 3 + PTW 0 working | ISP works, datacenter doesn't | Buy 5 more IPRoyal IPs (~$15/mo) |
| IPRoyal ✅ all 3 + PTW 5–20 working for Foodstuffs | ISP for WW, datacenter as Foodstuffs fleet backup | Use both: IPRoyal for WW + PTW survivors for NW/PS |
| IPRoyal ❌ WW + PTW ❌ all WW | Akamai is hard. Both options fail Woolworths | Try IPRoyal UK or Spain region; also try Smartproxy NZ residential trial |
| IPRoyal ✅ all + PTW 50+ working | Both work, datacenter fleet usable | $3/mo for 50 working IPs is incredible value — start with that |
| IPRoyal ❌ all + PTW ❌ all | Network or auth issue, not proxy quality | Verify your home IP is whitelisted on PTW, verify IPRoyal credentials, retry |

## Step 5 — Use working proxies in the real scrapers

```bash
# Woolworths via IPRoyal
python3 woolworths_claude.py --test \\
  --proxy "http://USER:PASS@iproyal_host:port"

# Pak'nSave via a working PTW proxy
python3 paknsave_claude.py --test \\
  --proxy "http://192.46.205.10:12321"

# Real all-branches run via proxy
python3 newworld_claude.py --all-branches \\
  --proxy "http://USER:PASS@iproyal_host:port"
```

To rotate through multiple working proxies, you'd run separate processes
(one per proxy) — the existing `--all-branches` doesn't yet do round-robin
across an IP pool. That's a future enhancement when you scale to 5+ IPs.

## Costs at each stage

| Stage | Spend | What you'd have |
|---|---:|---|
| Today | **$6** | Hard data on what works |
| Bootstrap (after testing) | **$15–30/mo** | 5–10 working IPs, daily 4–5 hr full scrape |
| Production (5–6 retailers) | **$30–60/mo** | 10–20 working IPs |

Compare to per-GB residential at $200–800/mo for the same volume — flat-rate
ISP/datacenter is the bootstrap-friendly economics.

## Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| Phase 1 timeouts on every PTW proxy | Home IP not whitelisted | Add home IP in PTW dashboard, retry |
| Phase 1 ✅ but Phase 2 ❌ for everything | Proxies leak headers / fail TLS handshake during scraping | Try `--no-headless` on a single proxy via `test_proxy.py` to see what's happening |
| `bad proxy URL` errors | Wrong format | Should be `http://[user:pass@]host:port`, no trailing slash |
| Phase 2 hangs on a few specific proxies | Slow proxies | Already capped at 120s timeout per scrape; failed ones get marked `ok=False` |
| Most PTW proxies pass Foodstuffs but fail Woolworths | **Expected** — Akamai blocks datacenter IPs | Use PTW for NW/PS only, IPRoyal/residential for WW |
