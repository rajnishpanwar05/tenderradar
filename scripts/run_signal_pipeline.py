#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from signals.pipeline import run_signal_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="Run TenderRadar opportunity signals pipeline")
    parser.add_argument("--source", choices=["all", "world_bank", "aiib", "jica_india"], default="all")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    result = run_signal_pipeline(source=args.source, debug=args.debug)

    print(f"[signals] Source               : {result['source']}")
    print(f"[signals] Signals captured     : {result['captured']}")
    print(f"[signals] Inserted             : {result['inserted']}")
    print(f"[signals] Updated              : {result['updated']}")
    print(f"[signals] Per-source counts    : {json.dumps(result['per_source_counts'], sort_keys=True)}")
    print(f"[signals] Stage distribution   : {json.dumps(result['stage_counts'], sort_keys=True)}")
    print(f"[signals] Consulting signals   : {json.dumps(result['consulting_counts'], sort_keys=True)}")
    print(f"[signals] Output artifact      : {result['artifact_path']}")
    for idx, row in enumerate(result["sample_rows"][:3], start=1):
        print(
            f"[signals] Sample {idx}: {row.get('signal_stage')} | "
            f"{row.get('confidence_score')} | {row.get('title')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
