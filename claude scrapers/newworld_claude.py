"""New World scraper — thin wrapper around foodstuffs_claude.

CLI is identical to foodstuffs_claude.py except --chain is preset to 'newworld'.
"""
import asyncio, sys
from foodstuffs_claude import main_async

if __name__ == "__main__":
    sys.exit(asyncio.run(main_async(default_chain="newworld")))
