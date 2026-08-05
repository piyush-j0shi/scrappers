"""Shared parsing helpers for the liquor scrapers so Quantity/Size splitting and
packaging Type are IDENTICAL across Super Liquor, Liquorland and The Bottle-O."""
from __future__ import annotations

import re
from typing import Optional

# qty x size, e.g. "12 x 330ml", "24x500", "6 x 1L"
_PACK = re.compile(r"(\d+)\s*[xX]\s*([0-9]+(?:\.[0-9]+)?)\s*(ml|l|litre)?\b", re.I)
# single size, e.g. "700ml", "1L", "1.5 litre"
_SIZE = re.compile(r"\b([0-9]+(?:\.[0-9]+)?)\s*(ml|l|litre)\b", re.I)
# pack count words, e.g. "15 Pack", "12pk", "10 Cans", "6 Bottles"
_PACKWORD = re.compile(r"\b(\d+)\s*(?:pack|pk|cans?|bottles?|pcs?|pods?)\b", re.I)


def parse_qty_size(name: str, extra: str = "") -> tuple[Optional[int], Optional[str]]:
    """Return (quantity, size) e.g. (12, '330mL') / (1, '700mL') / (15, '330mL').
    Prefer an explicit 'N x SIZE' pack; else a pack-count word + a standalone size;
    else a single size -> qty 1. `extra` is an optional secondary string (e.g. a
    URL slug) to also scan."""
    for text in (name, extra):
        if not text:
            continue
        m = _PACK.search(text)
        if m:
            num, unit = m.group(2), (m.group(3) or "").lower()
            unit = "L" if unit in ("l", "litre") else "mL"
            return int(m.group(1)), f"{num}{unit}"
    # pack-count word (e.g. "15 Pack ... 330ml") combined with a standalone size
    qty = None
    for text in (name, extra):
        if not text:
            continue
        pw = _PACKWORD.search(text)
        if pw:
            qty = int(pw.group(1))
            break
    for text in (name, extra):
        if not text:
            continue
        m = _SIZE.search(text)
        if m:
            unit = "L" if m.group(2).lower() in ("l", "litre") else "mL"
            return (qty or 1), f"{m.group(1)}{unit}"
    return qty, None


def classify_type(closure: Optional[str], name: str, size: Optional[str]) -> str:
    """Packaging type. Prefer a detail 'Closure' field; else whole-word name signals.
    Word boundaries avoid false hits ('Caskmates', 'Canadian'). Cans/kegs/casks are
    always named explicitly, so a product with a real size and no such signal is a
    Bottle; only truly sizeless/unknown -> Other."""
    c = (closure or "").lower()
    if "can" in c:
        return "Can"          # "Can Closure"
    if "keg" in c:
        return "Keg"
    if "cask" in c:
        return "Cask"
    if any(x in c for x in ("screw", "cork", "crown", "cap", "bottle")):
        return "Bottle"
    n = (name or "").lower()
    if re.search(r"\bkegs?\b", n):
        return "Keg"
    if re.search(r"\bcask\b", n):
        return "Cask"
    if re.search(r"\bcans?\b", n):
        return "Can"
    if re.search(r"\bbottles?\b", n):
        return "Bottle"
    return "Bottle" if size else "Other"
