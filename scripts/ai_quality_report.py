#!/usr/bin/env python3
"""
Weekly AI quality report from logged chat answers + feedback labels.

Usage:
  ./venv_stable/bin/python scripts/ai_quality_report.py
  ./venv_stable/bin/python scripts/ai_quality_report.py --days 14
  ./venv_stable/bin/python scripts/ai_quality_report.py --mode grounding
"""

from __future__ import annotations

import argparse
import json
import os
import sys

BASE = os.path.expanduser("~/tender_system")
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from intelligence.ai_quality_loop import (
    chat_grounding_report,
    weekly_combined_report,
    weekly_report,
)


def main() -> int:
    ap = argparse.ArgumentParser(description="TenderRadar AI quality weekly report")
    ap.add_argument("--days", type=int, default=7, help="lookback window in days")
    ap.add_argument(
        "--mode",
        choices=("feedback", "grounding", "combined"),
        default="combined",
        help="report mode",
    )
    ap.add_argument("--json-out", default="", help="optional output path for JSON report")
    args = ap.parse_args()

    try:
        days = max(1, int(args.days))
        if args.mode == "feedback":
            report = weekly_report(days=days)
        elif args.mode == "grounding":
            report = chat_grounding_report(days=days)
        else:
            report = weekly_combined_report(days=days)
        print(json.dumps(report, indent=2))
        if args.json_out:
            with open(args.json_out, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)
        return 0
    except Exception as exc:
        print(json.dumps({
            "ok": False,
            "error": "db_unavailable",
            "message": str(exc),
        }, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
