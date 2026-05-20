import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# Chain config — slug must match store_chains.slug in Supabase.
# branch_name identifies which physical store this scraper targets.
CHAIN_CONFIG = {
    "new_world": {
        "slug": "new-world",
        "name": "New World",
        "branch_name": "New World New Lynn",
    },
    "paknsave": {
        "slug": "paknsave",
        "name": "Pak'nSave",
        "branch_name": "PAK'nSAVE Mt Albert",
    },
    "woolworths": {
        "slug": "woolworths",
        "name": "Woolworths",
        "branch_name": "Woolworths Ponsonby",
    },
}

# Scraping behaviour
HEADLESS = False
SLOW_MO_MS = 50          # ms between actions (be polite to servers)
REQUEST_TIMEOUT_MS = 30_000
