#!/usr/bin/env python3
"""
Deterministic chat guardrail evaluator (no external LLM/API usage).

This validates end-to-end behavior of api.routes.chat.chat_endpoint by mocking:
  - retrieval output (search)
  - LLM JSON payloads
  - feedback logging side effects

Focus:
  - fact-query abstain policy
  - citation + filter-grounding enforcement
  - happy-path grounded response
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

BASE = os.getenv("TENDERRADAR_BASE") or str(Path(__file__).resolve().parents[1])
if BASE and BASE not in sys.path:
    sys.path.insert(0, BASE)

from api.routes import chat as chatmod


def _mk_tender(
    title: str,
    source_site: str,
    sector: str,
    region: str,
    similarity: float,
    composite_score: float,
    with_evidence: bool = True,
) -> Dict[str, Any]:
    return {
        "tender_id": f"T-{abs(hash((title, source_site))) % 10_000_000}",
        "title": title,
        "source_site": source_site,
        "organization": "Test Org",
        "sector": sector,
        "region": region,
        "priority_score": 72,
        "bid_fit_score": 68,
        "competition_level": "medium",
        "opportunity_size": "medium",
        "similarity": similarity,
        "composite_score": composite_score,
        "opportunity_insight": "Grounded insight" if with_evidence else "",
        "description": "Grounded description from source." if with_evidence else "",
        "deep_scope": "Scope from tender text." if with_evidence else "",
        "deep_ai_summary": "Evidence summary." if with_evidence else "",
        "deep_document_links": [{"url": "https://example.org/doc.pdf", "extracted": with_evidence}] if with_evidence else [],
        "url": "https://example.org/tender",
        "date_first_seen": "2026-04-07",
    }


@dataclass
class ChatEvalCase:
    case_id: str
    query: str
    tenders: List[Dict[str, Any]]
    filters: Dict[str, Any]
    llm_payload: Dict[str, Any]
    expect_abstain: bool
    expect_sources_suffix: bool


def _run_case(case: ChatEvalCase) -> Dict[str, Any]:
    original_search = chatmod.search
    original_call_llm = chatmod._call_llm
    original_log_answer = chatmod.log_answer_event

    try:
        chatmod.search = lambda q, limit=20: {  # type: ignore[assignment]
            "results": case.tenders,
            "filters_extracted": case.filters,
            "vector_candidates": 0,
            "query_ms": 0.0,
        }
        chatmod._call_llm = lambda messages, temperature=0.0: json.dumps(case.llm_payload)  # type: ignore[assignment]
        chatmod.log_answer_event = lambda **kwargs: 1  # type: ignore[assignment]

        req = chatmod.ChatRequest(messages=[chatmod.ChatMessage(role="user", content=case.query)])
        out = chatmod.chat_endpoint(req)
        reply = str(out.get("reply") or "")

        has_abstain = "Not found in retrieved tenders." in reply
        has_sources = "Sources:" in reply
        passed = (
            (has_abstain == case.expect_abstain)
            and (has_sources == case.expect_sources_suffix)
        )

        return {
            "id": case.case_id,
            "passed": passed,
            "expect_abstain": case.expect_abstain,
            "actual_abstain": has_abstain,
            "expect_sources_suffix": case.expect_sources_suffix,
            "actual_sources_suffix": has_sources,
            "citations": out.get("citations") or [],
            "reply_preview": reply[:220],
        }
    finally:
        chatmod.search = original_search  # type: ignore[assignment]
        chatmod._call_llm = original_call_llm  # type: ignore[assignment]
        chatmod.log_answer_event = original_log_answer  # type: ignore[assignment]


def main() -> int:
    ap = argparse.ArgumentParser(description="Run deterministic chat guardrail eval")
    ap.add_argument("--json-out", default="artifacts/chat_eval_last.json", help="Path to write JSON report")
    args = ap.parse_args()

    undp_good = _mk_tender(
        "UNDP Health Systems Technical Assistance",
        "UNDP",
        "health",
        "Africa",
        similarity=0.41,
        composite_score=0.52,
        with_evidence=True,
    )
    wb_other = _mk_tender(
        "World Bank Governance Advisory",
        "World Bank",
        "governance",
        "South Asia",
        similarity=0.35,
        composite_score=0.44,
        with_evidence=True,
    )
    weak_no_evidence = _mk_tender(
        "Generic notice",
        "UNDP",
        "health",
        "Africa",
        similarity=0.12,
        composite_score=0.18,
        with_evidence=False,
    )

    cases: List[ChatEvalCase] = [
        ChatEvalCase(
            case_id="fact_no_citations_abstain",
            query="What is the deadline and budget for this UNDP health tender?",
            tenders=[undp_good],
            filters={"source_portals": ["UNDP"], "sectors": ["health"], "regions": ["Africa"]},
            llm_payload={"answer": "Budget is high.", "citations": []},
            expect_abstain=True,
            expect_sources_suffix=False,
        ),
        ChatEvalCase(
            case_id="fact_filter_mismatch_abstain",
            query="Show me only UNDP health tenders in Africa",
            tenders=[wb_other],
            filters={"source_portals": ["UNDP"], "sectors": ["health"], "regions": ["Africa"]},
            llm_payload={"answer": "Found a match.", "citations": [1]},
            expect_abstain=True,
            expect_sources_suffix=False,
        ),
        ChatEvalCase(
            case_id="fact_weak_signal_abstain",
            query="Give me exact eligibility and budget details",
            tenders=[weak_no_evidence],
            filters={"source_portals": ["UNDP"], "sectors": ["health"], "regions": ["Africa"]},
            llm_payload={"answer": "Here are details.", "citations": [1]},
            expect_abstain=True,
            expect_sources_suffix=False,
        ),
        ChatEvalCase(
            case_id="fact_grounded_pass",
            query="Show me only UNDP health consulting opportunities in Africa",
            tenders=[undp_good, wb_other],
            filters={"source_portals": ["UNDP"], "sectors": ["health"], "regions": ["Africa"]},
            llm_payload={"answer": "UNDP health consulting opportunity identified.", "citations": [1]},
            expect_abstain=False,
            expect_sources_suffix=True,
        ),
        ChatEvalCase(
            case_id="nonfact_allows_response",
            query="Compare overall trend of opportunities this week",
            tenders=[undp_good, wb_other],
            filters={"source_portals": [], "sectors": [], "regions": []},
            llm_payload={"answer": "Overall trend is stable.", "citations": []},
            expect_abstain=False,
            expect_sources_suffix=False,
        ),
    ]

    reports = [_run_case(c) for c in cases]
    passed = [r for r in reports if r["passed"]]
    failed = [r for r in reports if not r["passed"]]

    summary = {
        "ok": len(failed) == 0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cases": len(reports),
        "passed": len(passed),
        "failed": len(failed),
        "pass_rate_pct": round(100.0 * len(passed) / max(1, len(reports)), 2),
        "failed_case_ids": [r["id"] for r in failed],
    }

    payload = {"summary": summary, "cases": reports}
    print(json.dumps(payload, indent=2))

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    return 0 if summary["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
