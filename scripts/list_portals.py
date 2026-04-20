#!/usr/bin/env python3
# =============================================================================
# scripts/list_portals.py — Portal inventory debug tool
#
# Usage:
#   python3 scripts/list_portals.py              # full table
#   python3 scripts/list_portals.py --enabled    # only enabled portals
#   python3 scripts/list_portals.py --disabled   # only disabled portals
#   python3 scripts/list_portals.py --group api  # filter by group
#   python3 scripts/list_portals.py --enable wb  # enable a portal
#   python3 scripts/list_portals.py --disable sidbi  # disable a portal
#
# Output columns:
#   FLAG      CLI flag used with  main.py --portal <flag>
#   LABEL     Human-readable portal name
#   GROUP     api | requests | selenium | captcha
#   AUTO      ✓ = runs in default (no-flag) batch  |  – = explicit only
#   ENABLED   ✓ = active  |  ✗ = disabled in enabled_portals.json
#   SOURCE    meta = defined via SCRAPER_META  |  static = hardcoded registry
# =============================================================================

import argparse
import os
import sys

# ── Ensure package root is on path ────────────────────────────────────────────
_BASE = os.path.expanduser("~/tender_system")
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)
os.chdir(_BASE)


def _col(text: str, width: int, colour_code: str = "") -> str:
    """Left-pad text to width, optionally wrap in ANSI colour."""
    s = str(text)[:width].ljust(width)
    if colour_code and sys.stdout.isatty():
        return f"\033[{colour_code}m{s}\033[0m"
    return s


_GREEN  = "32"
_RED    = "31"
_YELLOW = "33"
_CYAN   = "36"
_BOLD   = "1"
_DIM    = "2"


def _tick(value: bool, true_col: str = _GREEN, false_col: str = _RED) -> str:
    sym  = "✓" if value else "✗"
    code = true_col if value else false_col
    if sys.stdout.isatty():
        return f"\033[{code}m{sym}\033[0m"
    return sym


def _print_table(rows: list) -> None:
    if not rows:
        print("  (no portals match the current filter)")
        return

    # Header
    sep = (
        f"{'─'*12}─{'─'*30}─{'─'*10}─{'─'*6}─{'─'*8}─{'─'*8}"
    )
    hdr_fmt = "{:<12}  {:<30}  {:<10}  {:<6}  {:<8}  {:<8}"
    print()
    print(hdr_fmt.format("FLAG", "LABEL", "GROUP", "AUTO", "ENABLED", "SOURCE"))
    print(sep)

    for r in rows:
        flag    = _col(r["flag"],    12, _CYAN if r["enabled"] else _DIM)
        label   = _col(r["label"],   30)
        group   = _col(r["group"],   10, _YELLOW)
        auto    = _col("✓" if r["auto"] else "–", 6,
                       _GREEN if r["auto"] else _DIM)
        enabled = _col(_tick(r["enabled"]), 8)
        source  = _col(r["source"],  8,  _DIM if r["source"] == "static" else _GREEN)

        # Plain formatter (no colour codes in width calculation)
        if sys.stdout.isatty():
            print(f"{flag}  {label}  {group}  {auto}    {enabled}  {source}")
        else:
            print(
                f"{r['flag']:<12}  {r['label']:<30}  {r['group']:<10}  "
                f"{'✓' if r['auto'] else '–':<6}  "
                f"{'✓' if r['enabled'] else '✗':<8}  "
                f"{r['source']:<8}"
            )

    print(sep)
    enabled_count  = sum(1 for r in rows if r["enabled"])
    disabled_count = len(rows) - enabled_count
    meta_count     = sum(1 for r in rows if r["source"] == "meta")
    print(
        f"  {len(rows)} portal(s)  •  "
        f"{enabled_count} enabled  •  "
        f"{disabled_count} disabled  •  "
        f"{meta_count} auto-discovered via SCRAPER_META"
    )
    print()


def _toggle(flag: str, enable: bool) -> None:
    """Enable or disable a single portal in enabled_portals.json."""
    from core.registry import load_enabled_portals, save_enabled_portals, _STATIC_REGISTRY

    all_flags = [j.flag for j in _STATIC_REGISTRY]
    enabled   = load_enabled_portals(all_flags)

    if flag not in enabled and flag not in all_flags:
        print(f"  ✗  Unknown portal flag: '{flag}'")
        print(f"     Run without arguments to see all valid flags.")
        sys.exit(1)

    enabled[flag] = enable
    save_enabled_portals(enabled)
    state = "ENABLED" if enable else "DISABLED"
    print(f"  ✓  Portal '{flag}' is now {state} in enabled_portals.json")
    print(f"     Changes take effect on the next run.")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="TenderRadar — Portal Inventory Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python3 scripts/list_portals.py                  full table (all portals)
  python3 scripts/list_portals.py --enabled        only enabled portals
  python3 scripts/list_portals.py --group api      API portals only
  python3 scripts/list_portals.py --enable wb      enable World Bank
  python3 scripts/list_portals.py --disable sidbi  disable SIDBI
        """,
    )
    ap.add_argument("--enabled",  action="store_true", help="Show only enabled portals")
    ap.add_argument("--disabled", action="store_true", help="Show only disabled portals")
    ap.add_argument("--group",    metavar="G",         help="Filter by group (api/requests/selenium/captcha)")
    ap.add_argument("--enable",   metavar="FLAG",      help="Enable a portal by flag")
    ap.add_argument("--disable",  metavar="FLAG",      help="Disable a portal by flag")
    args = ap.parse_args()

    # ── Mutate operations ─────────────────────────────────────────────────
    if args.enable:
        _toggle(args.enable.lower(), True)
        return
    if args.disable:
        _toggle(args.disable.lower(), False)
        return

    # ── List / display ────────────────────────────────────────────────────
    from core.registry import portal_info_table

    rows = portal_info_table()

    if args.enabled:
        rows = [r for r in rows if r["enabled"]]
    elif args.disabled:
        rows = [r for r in rows if not r["enabled"]]

    if args.group:
        rows = [r for r in rows if r["group"] == args.group.lower()]

    title = "TenderRadar — Portal Inventory"
    if args.enabled:
        title += "  (enabled only)"
    elif args.disabled:
        title += "  (disabled only)"
    if args.group:
        title += f"  [group={args.group}]"

    print(f"\n  {title}")
    _print_table(rows)

    if not args.enabled and not args.disabled and not args.group:
        print(
            "  Tip: to disable a portal →  "
            "python3 scripts/list_portals.py --disable <flag>\n"
            "       to enable  a portal →  "
            "python3 scripts/list_portals.py --enable <flag>"
        )
        print()


if __name__ == "__main__":
    main()
