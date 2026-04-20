#!/usr/bin/env python3
"""
Manual CLI for the TenderRadar daily email digest.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from notifier.daily_digest import send_daily_digest


def main() -> int:
    parser = argparse.ArgumentParser(description="Send TenderRadar daily email digest")
    parser.add_argument("--date", default="", help="Report date in YYYY-MM-DD format")
    parser.add_argument("--workbook", default="", help="Path to Tender_Monitor_Master.xlsx")
    parser.add_argument("--top", type=int, default=5, help="Top tenders to include in email body (1-10)")
    parser.add_argument("--max-packages", type=int, default=5, help="Max relevant package folders to zip (0-10)")
    parser.add_argument("--no-packages", action="store_true", help="Skip package ZIP attachment")
    parser.add_argument("--dry-run", action="store_true", help="Build digest but do not send email")
    parser.add_argument("--force", action="store_true", help="Allow resend even if this date was already sent")
    parser.add_argument("--json", action="store_true", help="Print full result as JSON")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    result = send_daily_digest(
        workbook_path=Path(args.workbook) if args.workbook else None,
        report_date=args.date or None,
        top_tenders_limit=args.top,
        max_package_count=args.max_packages,
        include_packages=not args.no_packages,
        dry_run=args.dry_run,
        force=args.force,
    )

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"Subject: {result['subject']}")
        print(f"New tenders: {result['total_new']}")
        print(f"Relevant: {result['relevant_count']}")
        print(f"Borderline: {result['borderline_count']}")
        print("Attachments:")
        for path in result["attachments"]:
            print(f"  - {path}")
        if result.get("duplicate_blocked"):
            print("Status: duplicate blocked")
        else:
            print(f"Status: {'dry-run' if result['dry_run'] else ('sent' if result['ok'] else 'failed')}")

    return 0 if (result["ok"] or result["dry_run"] or result.get("duplicate_blocked")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
