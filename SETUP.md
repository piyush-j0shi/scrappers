# Local Setup (home machine)

The scrape runs on the server, but it needs your **home IP** for Cloudflare.
This sets up the tunnels that carry the CF solve + token traffic out through home.

## 1. Start the tunnels
```bash
cd ~/Downloads/scrapers
./setup_local.sh
```
Installs `proxy.py` + `bore` if missing, then starts:
- forward proxy on `127.0.0.1:8888`
- bore relay `bore.pub:24385` (CapSolver → home)
- reverse SSH `server:8890 → home:8888` (scrape browser → home)

It prints `tunnel exit IP:` — that must be your home IP.

## 2. Verify (optional)
```bash
curl -x http://bore.pub:24385 https://api.ipify.org        # home IP
ssh piyush@hetzner 'curl -x http://127.0.0.1:8890 https://api.ipify.org'   # same home IP
```

## Notes
- Re-run `./setup_local.sh` any time a tunnel drops (e.g. home IP change) — it only starts what isn't already running.
- Keep bore on port **24385** (server `.env` has `CAPSOLVER_PROXY=http://bore.pub:24385`).
