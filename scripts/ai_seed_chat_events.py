#!/usr/bin/env python3
"""
Seed llm_answer_events with realistic chat traffic by calling the real chat route.

This is useful to bootstrap grounding/quality metrics before live user traffic is high.

Usage:
  ./venv_stable/bin/python scripts/ai_seed_chat_events.py
  ./venv_stable/bin/python scripts/ai_seed_chat_events.py --limit 12
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import List

BASE = os.path.expanduser("~/tender_system")
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from api.routes.chat import ChatMessage, ChatRequest, chat_endpoint


DEFAULT_QUERIES: List[str] = [
    "Show top 5 monitoring and evaluation tenders in Africa this month",
    "Which UNDP health consulting opportunities are open now?",
    "Give me urgent education tenders in South Asia",
    "List World Bank governance advisory opportunities this week",
    "What are the best water sanitation consulting tenders currently?",
    "Show tenders with deadline in next 14 days for research firms",
    "Which tenders mention baseline survey or endline evaluation?",
    "Find opportunities from UNGM related to nutrition and health systems",
    "Top climate adaptation consulting tenders in Asia",
    "Any opportunities requiring third party monitoring experience?",
    "Show me high priority tenders from EC and TED portals",
    "Find consulting tenders with strong fit for IDCG in education",
]


def _run_query(q: str) -> dict:
    req = ChatRequest(messages=[ChatMessage(role="user", content=q)])
    out = chat_endpoint(req)
    return {
        "query": q,
        "answer_id": out.get("answer_id"),
        "citations": out.get("citations") or [],
        "source_count": len(out.get("source_tenders") or []),
        "reply_preview": str(out.get("reply") or "")[:220],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Seed chat quality events via real chat endpoint")
    ap.add_argument("--limit", type=int, default=12, help="number of seed queries to run")
    args = ap.parse_args()

    limit = max(1, min(int(args.limit), len(DEFAULT_QUERIES)))
    rows = []
    for q in DEFAULT_QUERIES[:limit]:
        try:
            rows.append(_run_query(q))
        except Exception as exc:
            rows.append({
                "query": q,
                "error": str(exc),
                "answer_id": None,
                "citations": [],
                "source_count": 0,
            })

    success = sum(1 for r in rows if r.get("answer_id"))
    out = {
        "ok": success > 0,
        "attempted": limit,
        "logged_events": success,
        "failed": limit - success,
        "results": rows,
    }
    print(json.dumps(out, indent=2))
    return 0 if success > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

