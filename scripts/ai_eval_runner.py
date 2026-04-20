#!/usr/bin/env python3
"""
AI retrieval quality evaluator for TenderRadar.

Runs a gold query set against intelligence.query_engine.search() and reports:
- retrieval hit rate
- constraint satisfaction (required / forbidden tokens)
- portal/sector targeting quality
- evidence richness in top-k results
- pass/fail per case + overall gate

Usage:
  ./venv_stable/bin/python scripts/ai_eval_runner.py
  ./venv_stable/bin/python scripts/ai_eval_runner.py --gold artifacts/ai_eval_gold.jsonl
  ./venv_stable/bin/python scripts/ai_eval_runner.py --top-k 12 --json-out artifacts/ai_eval_last.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List

BASE = os.path.expanduser("~/tender_system")
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from intelligence.query_engine import search


@dataclass
class EvalCase:
    case_id: str
    query: str
    top_k: int = 10
    min_results: int = 1
    require_any_tokens: List[str] = None
    forbid_tokens: List[str] = None
    require_any_portals: List[str] = None
    require_any_sectors: List[str] = None
    min_portal_hits: int = 0
    min_sector_hits: int = 0
    min_evidence_ratio: float = 0.30
    pass_score: int = 70

    @staticmethod
    def from_dict(d: Dict[str, Any], default_top_k: int) -> "EvalCase":
        return EvalCase(
            case_id=str(d.get("id") or d.get("case_id") or "case"),
            query=str(d.get("query") or "").strip(),
            top_k=max(1, int(d.get("top_k") or default_top_k)),
            min_results=max(0, int(d.get("min_results") or 1)),
            require_any_tokens=[str(x).lower() for x in (d.get("require_any_tokens") or []) if str(x).strip()],
            forbid_tokens=[str(x).lower() for x in (d.get("forbid_tokens") or []) if str(x).strip()],
            require_any_portals=[str(x).lower() for x in (d.get("require_any_portals") or []) if str(x).strip()],
            require_any_sectors=[str(x).lower() for x in (d.get("require_any_sectors") or []) if str(x).strip()],
            min_portal_hits=max(0, int(d.get("min_portal_hits") or 0)),
            min_sector_hits=max(0, int(d.get("min_sector_hits") or 0)),
            min_evidence_ratio=float(d.get("min_evidence_ratio") if d.get("min_evidence_ratio") is not None else 0.30),
            pass_score=max(1, min(100, int(d.get("pass_score") or 70))),
        )


def _safe_lower(v: Any) -> str:
    return str(v or "").lower()


def _tender_text(row: Dict[str, Any]) -> str:
    parts = [
        _safe_lower(row.get("title")),
        _safe_lower(row.get("organization")),
        _safe_lower(row.get("sector")),
        _safe_lower(row.get("region")),
        _safe_lower(row.get("source_site")),
        _safe_lower(row.get("opportunity_insight")),
        _safe_lower(row.get("description")),
        _safe_lower(row.get("deep_scope")),
        _safe_lower(row.get("deep_ai_summary")),
    ]
    return " ".join(x for x in parts if x)


def _has_evidence(row: Dict[str, Any]) -> bool:
    if any(
        bool(str(row.get(k) or "").strip())
        for k in ("description", "deep_scope", "deep_ai_summary", "opportunity_insight")
    ):
        return True
    links = row.get("deep_document_links")
    if isinstance(links, str):
        try:
            links = json.loads(links)
        except Exception:
            links = []
    if isinstance(links, list):
        return any(bool(d.get("extracted")) for d in links if isinstance(d, dict))
    return False


def _load_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for i, raw in enumerate(f, start=1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            try:
                obj = json.loads(line)
            except Exception as exc:
                raise ValueError(f"Invalid JSONL at line {i}: {exc}") from exc
            if not isinstance(obj, dict):
                raise ValueError(f"Invalid JSONL at line {i}: expected object")
            rows.append(obj)
    return rows


def evaluate_case(case: EvalCase) -> Dict[str, Any]:
    result = search(case.query, limit=case.top_k)
    rows = result.get("results") or []
    top_rows = rows[: case.top_k]

    joined = "\n".join(_tender_text(r) for r in top_rows)
    total = len(rows)
    evidence_count = sum(1 for r in top_rows if _has_evidence(r))
    evidence_ratio = (evidence_count / max(1, len(top_rows))) if top_rows else 0.0

    required_token_hit = True
    if case.require_any_tokens:
        required_token_hit = any(tok in joined for tok in case.require_any_tokens)

    forbid_hits = [tok for tok in case.forbid_tokens if tok in joined]

    portal_hits = 0
    if case.require_any_portals:
        for r in top_rows:
            src = _safe_lower(r.get("source_site"))
            if any(p in src for p in case.require_any_portals):
                portal_hits += 1

    sector_hits = 0
    if case.require_any_sectors:
        for r in top_rows:
            sec = _safe_lower(r.get("sector"))
            if any(s in sec for s in case.require_any_sectors):
                sector_hits += 1

    score = 100.0
    failures: List[str] = []

    if total < case.min_results:
        score -= 35.0
        failures.append(f"min_results_not_met({total}<{case.min_results})")

    if not required_token_hit:
        score -= 30.0
        failures.append("required_tokens_missing")

    if forbid_hits:
        score -= min(30.0, 10.0 * len(forbid_hits))
        failures.append(f"forbidden_tokens_present({','.join(forbid_hits[:3])})")

    req_portal_min = int(case.min_portal_hits or (1 if case.require_any_portals else 0))
    if req_portal_min > 0 and portal_hits < req_portal_min:
        score -= 20.0
        failures.append(f"required_portal_hits_low({portal_hits}<{req_portal_min})")

    req_sector_min = int(case.min_sector_hits or (1 if case.require_any_sectors else 0))
    if req_sector_min > 0 and sector_hits < req_sector_min:
        score -= 20.0
        failures.append(f"required_sector_hits_low({sector_hits}<{req_sector_min})")

    if evidence_ratio < case.min_evidence_ratio:
        score -= 15.0
        failures.append(
            f"evidence_ratio_low({evidence_ratio:.2f}<{case.min_evidence_ratio:.2f})"
        )

    score = max(0.0, min(100.0, score))
    passed = score >= case.pass_score and not failures

    return {
        "id": case.case_id,
        "query": case.query,
        "passed": passed,
        "score": round(score, 1),
        "pass_score": case.pass_score,
        "top_k": case.top_k,
        "total_results": total,
        "evidence_ratio_top_k": round(evidence_ratio, 3),
        "required_token_hit": required_token_hit,
        "required_portal_hits": portal_hits,
        "required_sector_hits": sector_hits,
        "forbidden_token_hits": forbid_hits,
        "failures": failures,
        "query_ms": result.get("query_ms", 0.0),
        "vector_candidates": result.get("vector_candidates", 0),
        "top_titles": [str(r.get("title") or "")[:140] for r in top_rows[:5]],
    }


def _runtime_diagnostics() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    try:
        import chromadb  # noqa: F401
        out["chromadb_available"] = True
    except Exception:
        out["chromadb_available"] = False
    try:
        import mysql.connector  # noqa: F401
        out["mysql_connector_available"] = True
    except Exception:
        out["mysql_connector_available"] = False
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Run retrieval eval against gold query set")
    ap.add_argument("--gold", default="artifacts/ai_eval_gold.jsonl", help="path to JSONL eval cases")
    ap.add_argument("--top-k", type=int, default=10, help="default top-k per case if omitted")
    ap.add_argument("--json-out", default="", help="optional path to save full JSON report")
    ap.add_argument("--allow-fail", action="store_true", help="return exit 0 even when cases fail")
    ap.add_argument(
        "--disable-vector",
        action="store_true",
        help="skip vector store and evaluate DB/reranker path only",
    )
    args = ap.parse_args()

    if args.disable_vector:
        os.environ["DISABLE_VECTOR_SEARCH"] = "1"

    if not os.path.exists(args.gold):
        print(json.dumps({
            "ok": False,
            "error": "gold_not_found",
            "gold_path": args.gold,
        }, indent=2))
        return 1

    raw_cases = _load_jsonl(args.gold)
    cases = [EvalCase.from_dict(d, default_top_k=max(1, int(args.top_k))) for d in raw_cases]
    cases = [c for c in cases if c.query]

    reports = [evaluate_case(c) for c in cases]
    passed = [r for r in reports if r["passed"]]
    failed = [r for r in reports if not r["passed"]]
    avg_score = round(sum(r["score"] for r in reports) / max(1, len(reports)), 2)
    avg_latency = round(sum(float(r.get("query_ms") or 0.0) for r in reports) / max(1, len(reports)), 1)

    diagnostics = _runtime_diagnostics()
    env_ready = bool(
        diagnostics.get("chromadb_available")
        and diagnostics.get("mysql_connector_available")
    )

    summary = {
        "ok": len(failed) == 0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "gold_path": args.gold,
        "cases": len(reports),
        "passed": len(passed),
        "failed": len(failed),
        "pass_rate_pct": round(100.0 * len(passed) / max(1, len(reports)), 2),
        "avg_score": avg_score,
        "avg_query_ms": avg_latency,
        "failed_case_ids": [r["id"] for r in failed],
        "runtime": diagnostics,
        "environment_ready": env_ready,
    }

    payload = {"summary": summary, "cases": reports}
    print(json.dumps(payload, indent=2))

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    if summary["ok"] or args.allow_fail:
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
