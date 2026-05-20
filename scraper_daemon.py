#!/usr/bin/env python3
"""
scraper_daemon.py — long-running process that polls scraper_commands and dispatches runs.

Supported commands (scraper_commands.command enum):
    run_all     — scrape every active branch
    run_chain   — scrape all branches for a specific chain (requires chain_id)
    run_branch  — scrape one branch (requires branch_id); runs concurrently with full runs
    run_stale   — scrape branches with last_scraped_at NULL or > 24h ago
    stop        — gracefully stop the active full run (shutdown flag); immediate

Queue rules:
    - Only one full run (run_all / run_chain / run_stale) active at a time
    - run_branch can execute alongside a full run as an independent task
    - stop is processed immediately regardless of active run state

Usage:
    python scraper_daemon.py
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Optional

from supabase import create_client, Client

from config import SUPABASE_URL, SUPABASE_SERVICE_KEY
from run_all_branches import fetch_branches, scrape_branch

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POLL_INTERVAL_S: int = 5
DEFAULT_CONCURRENCY: int = 6
DEFAULT_MAX_RETRIES: int = 3

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Daemon state  (module-level; safe — single async thread)
# ---------------------------------------------------------------------------

# True while a full run (run_all / run_chain / run_stale) is executing.
_full_run_active: bool = False

# The shutdown flag owned by the currently active full run, if any.
_active_shutdown_flag: Optional[asyncio.Event] = None


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mark_picked_up(client: Client, command_id: str) -> None:
    client.table("scraper_commands").update({
        "status": "picked_up",
        "picked_up_at": _now_iso(),
    }).eq("id", command_id).execute()


def _mark_completed(client: Client, command_id: str) -> None:
    client.table("scraper_commands").update({
        "status": "completed",
        "completed_at": _now_iso(),
    }).eq("id", command_id).execute()


def _mark_failed(client: Client, command_id: str, error: str) -> None:
    client.table("scraper_commands").update({
        "status": "failed",
        "completed_at": _now_iso(),
        "error_log": error,
    }).eq("id", command_id).execute()


def _resolve_chain_slug(client: Client, chain_id: str) -> Optional[str]:
    """Look up store_chains.slug for a given chain UUID."""
    result = (
        client.table("store_chains")
        .select("slug")
        .eq("id", chain_id)
        .execute()
    )
    if result.data:
        return result.data[0]["slug"]
    return None


# ---------------------------------------------------------------------------
# Startup cleanup
# ---------------------------------------------------------------------------

def _cleanup_orphaned_commands(client: Client) -> None:
    """
    Mark any commands stuck in 'picked_up' from a previous daemon crash as failed.
    Called once at startup before the poll loop begins.
    """
    result = (
        client.table("scraper_commands")
        .select("id")
        .eq("status", "picked_up")
        .execute()
    )
    orphans = result.data or []
    if not orphans:
        return
    for row in orphans:
        _mark_failed(client, row["id"], "Daemon restarted — command was orphaned")
    logger.info(f"Marked {len(orphans)} orphaned command(s) as failed on startup")


# ---------------------------------------------------------------------------
# Core orchestration loop
# ---------------------------------------------------------------------------

async def _run_orchestration(
    client: Client,
    chain_slug: Optional[str],
    branch_id: Optional[str],
    stale_only: bool,
    shutdown_flag: asyncio.Event,
    concurrency: int = DEFAULT_CONCURRENCY,
    max_retries: int = DEFAULT_MAX_RETRIES,
    proxy_url: Optional[str] = None,
) -> None:
    """
    Orchestration loop that mirrors run_all_branches.run() but accepts an
    externally-owned shutdown_flag so the daemon can trigger graceful stop.

    Reuses fetch_branches() and scrape_branch() from run_all_branches.
    """
    branches = fetch_branches(
        client,
        chain_slug=chain_slug,
        branch_id=branch_id,
        stale_only=stale_only,
    )
    logger.info(f"Found {len(branches)} branches to scrape")

    if not branches:
        return

    semaphore = asyncio.Semaphore(concurrency)
    tasks: list[asyncio.Task] = []

    for index, branch in enumerate(branches):
        if shutdown_flag.is_set():
            logger.info("Shutdown flag set — not launching further branch tasks")
            break

        # Stagger first <concurrency> launches by 10s; after that the semaphore
        # acts as the natural throttle as slots free up.
        if 0 < index < concurrency:
            await asyncio.sleep(10)

        if shutdown_flag.is_set():
            break

        task = asyncio.create_task(
            scrape_branch(
                branch=branch,
                semaphore=semaphore,
                proxy_url=proxy_url,
                max_retries=max_retries,
                shutdown_flag=shutdown_flag,
            ),
            name=branch.get("name", branch["id"]),
        )
        tasks.append(task)

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# ---------------------------------------------------------------------------
# Command executors  (run as background asyncio Tasks)
# ---------------------------------------------------------------------------

async def _execute_full_run(
    client: Client,
    command: dict,
    chain_slug: Optional[str],
    stale_only: bool,
) -> None:
    """Execute a full run (run_all / run_chain / run_stale)."""
    global _full_run_active, _active_shutdown_flag

    command_id = command["id"]
    shutdown_flag = asyncio.Event()
    _active_shutdown_flag = shutdown_flag

    try:
        logger.info(
            f"[cmd:{command_id}] Starting full run "
            f"(chain={chain_slug or 'all'}, stale_only={stale_only})"
        )
        await _run_orchestration(
            client,
            chain_slug=chain_slug,
            branch_id=None,
            stale_only=stale_only,
            shutdown_flag=shutdown_flag,
        )
        _mark_completed(client, command_id)
        logger.info(f"[cmd:{command_id}] Full run completed")

    except Exception as exc:
        logger.error(f"[cmd:{command_id}] Full run failed: {exc}")
        _mark_failed(client, command_id, str(exc))

    finally:
        _full_run_active = False
        _active_shutdown_flag = None


async def _execute_branch_run(client: Client, command: dict, branch_id: str) -> None:
    """Execute a single-branch run; runs independently of full runs."""
    command_id = command["id"]
    # Own shutdown flag — not wired to the global stop; branch runs are
    # short enough that stop-command semantics target the full run only.
    shutdown_flag = asyncio.Event()

    try:
        logger.info(f"[cmd:{command_id}] Starting branch run (branch_id={branch_id})")
        await _run_orchestration(
            client,
            chain_slug=None,
            branch_id=branch_id,
            stale_only=False,
            shutdown_flag=shutdown_flag,
        )
        _mark_completed(client, command_id)
        logger.info(f"[cmd:{command_id}] Branch run completed")

    except Exception as exc:
        logger.error(f"[cmd:{command_id}] Branch run failed: {exc}")
        _mark_failed(client, command_id, str(exc))


# ---------------------------------------------------------------------------
# Poll cycle
# ---------------------------------------------------------------------------

async def _poll_once(client: Client) -> None:
    """Fetch pending commands and dispatch the first eligible one."""
    global _full_run_active

    logger.debug("Polling scraper_commands...")

    result = (
        client.table("scraper_commands")
        .select("*")
        .eq("status", "pending")
        .order("created_at", desc=False)
        .execute()
    )
    commands = result.data or []

    if not commands:
        return

    for command in commands:
        cmd_type: str = command.get("command", "")
        command_id: str = command["id"]

        # ── stop ──────────────────────────────────────────────────────────
        if cmd_type == "stop":
            _mark_picked_up(client, command_id)
            logger.info(f"[cmd:{command_id}] Processing stop command")

            if _active_shutdown_flag is not None:
                _active_shutdown_flag.set()
                logger.info(f"[cmd:{command_id}] Shutdown flag set on active full run")
            else:
                logger.info(f"[cmd:{command_id}] No active full run to stop")

            _mark_completed(client, command_id)
            break  # One command per cycle

        # ── run_branch ────────────────────────────────────────────────────
        elif cmd_type == "run_branch":
            branch_id = command.get("branch_id")
            if not branch_id:
                logger.error(f"[cmd:{command_id}] run_branch missing branch_id — failing")
                _mark_picked_up(client, command_id)
                _mark_failed(client, command_id, "run_branch command is missing branch_id")
                break

            _mark_picked_up(client, command_id)
            logger.info(
                f"[cmd:{command_id}] Dispatching run_branch (branch_id={branch_id})"
            )
            asyncio.create_task(_execute_branch_run(client, command, branch_id))
            break

        # ── run_all / run_chain / run_stale ───────────────────────────────
        elif cmd_type in ("run_all", "run_chain", "run_stale"):
            if _full_run_active:
                # Can't start another full run — skip this command and look for
                # a run_branch or stop further down the queue.
                logger.debug(
                    f"[cmd:{command_id}] Full run active — leaving '{cmd_type}' pending"
                )
                continue

            chain_slug: Optional[str] = None

            if cmd_type == "run_chain":
                chain_id = command.get("chain_id")
                if not chain_id:
                    logger.error(f"[cmd:{command_id}] run_chain missing chain_id — failing")
                    _mark_picked_up(client, command_id)
                    _mark_failed(client, command_id, "run_chain command is missing chain_id")
                    break
                chain_slug = _resolve_chain_slug(client, chain_id)
                if chain_slug is None:
                    logger.error(
                        f"[cmd:{command_id}] run_chain: unknown chain_id {chain_id} — failing"
                    )
                    _mark_picked_up(client, command_id)
                    _mark_failed(client, command_id, f"Unknown chain_id: {chain_id}")
                    break

            _full_run_active = True
            _mark_picked_up(client, command_id)
            logger.info(
                f"[cmd:{command_id}] Dispatching {cmd_type} "
                f"(chain={chain_slug or 'all'}, stale_only={cmd_type == 'run_stale'})"
            )
            asyncio.create_task(
                _execute_full_run(
                    client,
                    command,
                    chain_slug=chain_slug,
                    stale_only=(cmd_type == "run_stale"),
                )
            )
            break

        else:
            logger.warning(
                f"[cmd:{command_id}] Unknown command type '{cmd_type}' — skipping"
            )


# ---------------------------------------------------------------------------
# Daemon entry point
# ---------------------------------------------------------------------------

async def _daemon_loop() -> None:
    client: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    _cleanup_orphaned_commands(client)
    logger.info(f"Daemon started, polling scraper_commands every {POLL_INTERVAL_S}s")

    daemon_stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, daemon_stop.set)
        except (NotImplementedError, OSError):
            # Windows doesn't support add_signal_handler for all signals.
            pass

    while not daemon_stop.is_set():
        try:
            await _poll_once(client)
        except Exception as exc:
            logger.error(f"Poll cycle error: {exc}")

        # Interruptible sleep — wakes immediately on daemon shutdown signal.
        try:
            await asyncio.wait_for(daemon_stop.wait(), timeout=float(POLL_INTERVAL_S))
        except asyncio.TimeoutError:
            pass  # Normal path: interval elapsed, loop again

    logger.info("Daemon shutting down.")


def main() -> None:
    try:
        asyncio.run(_daemon_loop())
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
