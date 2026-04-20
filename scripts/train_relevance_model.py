#!/usr/bin/env python3
"""
Train IDCG relevance stack artifacts:
1) ML relevance model
2) Portfolio similarity scorer
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime

BASE_DIR = os.path.expanduser("~/tender_system")
sys.path.insert(0, BASE_DIR)
os.chdir(BASE_DIR)


def _default_opl_path() -> str:
    return "/Users/rajnishpanwar/Downloads/OPL Sheet 2021_Nov21.xlsx"


def main() -> int:
    p = argparse.ArgumentParser(description="Train TenderRadar relevance artifacts")
    p.add_argument("--opl", default=_default_opl_path(), help="Path to OPL workbook")
    args = p.parse_args()

    opl_path = os.path.abspath(os.path.expanduser(args.opl))
    if not os.path.exists(opl_path):
        print(f"ERROR: OPL workbook not found: {opl_path}")
        return 1

    print("=" * 70)
    print("TenderRadar Relevance Training")
    print(f"Workbook: {opl_path}")
    print("=" * 70)

    from intelligence.relevance_model import get_model
    from intelligence.portfolio_similarity import get_portfolio_scorer

    model = get_model()
    metrics = model.train(opl_path)
    model.save()

    scorer = get_portfolio_scorer()
    portfolio_meta = scorer.build(opl_path)
    scorer.save()

    summary = {
        "trained_at": datetime.utcnow().isoformat() + "Z",
        "opl_path": opl_path,
        "weights": {"ml": 0.50, "portfolio": 0.30, "keywords": 0.20},
        "relevance_model": metrics,
        "portfolio_similarity": portfolio_meta,
    }

    out = os.path.join(BASE_DIR, "artifacts", "relevance_training_summary.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)

    print("Training complete:")
    print(f"  - CV ROC-AUC: {metrics.get('cv_roc_auc_mean')} ± {metrics.get('cv_roc_auc_std')}")
    print(f"  - Training rows: {metrics.get('training_rows')}")
    print(f"  - Portfolio corpus size: {portfolio_meta.get('corpus_size')}")
    print(f"  - Saved summary: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

