"""
ntfy.sh push notification helper.

Reads NTFY_TOPIC from env var (or --topic flag) and POSTs a message to
https://ntfy.sh/<topic>. Used by the scrapers + orchestrator to push
critical events to your phone.

Usage:
  Standalone (ad hoc):
    python3 notify.py "Run started"
    python3 notify.py --title "WW done" --priority high "186/186 branches"
    python3 notify.py --tags warning,robot "Block rate climbing"

  As library:
    from notify import push
    push("title", "body", priority="high", tags=["bell"])

ntfy priorities:
  min  (1)  silent ish
  low  (2)  default chime
  default (3)  same as low
  high (4)  louder, may bypass DND
  max  (5)  always wakes you up
"""
from __future__ import annotations
import argparse
import os
import sys
import urllib.request
from typing import Optional

DEFAULT_TOPIC_ENV = "NTFY_TOPIC"
DEFAULT_BASE = "https://ntfy.sh"


def push(message: str, title: Optional[str] = None, priority: str = "default",
         tags: Optional[list[str]] = None, topic: Optional[str] = None) -> bool:
    """POST a message to ntfy.sh. Returns True on success.

    Never raises — push failures are logged to stderr but don't break callers."""
    topic = topic or os.environ.get(DEFAULT_TOPIC_ENV)
    if not topic:
        print(f"[notify] no topic set (env {DEFAULT_TOPIC_ENV} or --topic)", file=sys.stderr)
        return False
    url = f"{DEFAULT_BASE}/{topic}"
    headers = {"Content-Type": "text/plain; charset=utf-8"}
    if title:
        # ntfy header values must be latin-1 safe — strip non-ASCII
        safe = title[:200].replace("\n", " ").replace("\r", " ")
        headers["Title"] = safe.encode("latin-1", errors="replace").decode("latin-1")
    if priority:
        headers["Priority"] = priority
    if tags:
        headers["Tags"] = ",".join(tags)
    try:
        req = urllib.request.Request(
            url,
            data=message.encode("utf-8"),
            headers=headers,
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f"[notify] failed to push to {url}: {e}", file=sys.stderr)
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Send a ntfy.sh notification")
    ap.add_argument("message", help="Notification body")
    ap.add_argument("--title", default=None)
    ap.add_argument(
        "--priority",
        choices=["min", "low", "default", "high", "max"],
        default="default",
    )
    ap.add_argument("--tags", default=None, help="comma-separated emoji/keyword tags")
    ap.add_argument("--topic", default=None, help=f"override $NTFY_TOPIC env var")
    args = ap.parse_args()

    tags = [t.strip() for t in args.tags.split(",")] if args.tags else None
    ok = push(args.message, title=args.title, priority=args.priority,
              tags=tags, topic=args.topic)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
