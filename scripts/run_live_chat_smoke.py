#!/usr/bin/env python3
"""
Run a live chat smoke suite against api.routes.chat.chat_endpoint.

Input JSONL format (one object per line):
{
  "id": "case_id",
  "query": "user prompt",
  "expect_abstain": false,      # optional
  "expect_portal": "UNDP",      # optional
  "min_sources": 2              # optional
}
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

BASE = os.getenv("TENDERRADAR_BASE") or str(Path(__file__).resolve().parents[1])
if BASE and BASE not in sys.path:
    sys.path.insert(0, BASE)

from api.routes.chat import ChatMessage, ChatRequest, chat_endpoint


def _load_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for i, raw in enumerate(f, start=1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            obj = json.loads(line)
            if not isinstance(obj, dict):
                raise ValueError(f"Line {i}: expected object")
            rows.append(obj)
    return rows


def _norm(s: Any) -> str:
    return " ".join(str(s or "").strip().lower().split())


def _case_eval(case: Dict[str, Any]) -> Dict[str, Any]:
    cid = str(case.get("id") or "case")
    query = str(case.get("query") or "").strip()
    if not query:
        return {"id": cid, "passed": False, "error": "empty_query"}

    req = ChatRequest(messages=[ChatMessage(role="user", content=query)])
    out = chat_endpoint(req)
    reply = str(out.get("reply") or "")
    cites = list(out.get("citations") or [])
    src_rows = list(out.get("source_tenders") or [])
    cited_rows = [src_rows[i - 1] for i in cites if isinstance(i, int) and 1 <= i <= len(src_rows)]

    has_abstain = "not found in retrieved tenders" in _norm(reply)
    src_suffix = "sources:" in _norm(reply)
    source_count = len(src_rows)

    expected_abstain = case.get("expect_abstain")
    expected_portal = str(case.get("expect_portal") or "").strip()
    min_sources = int(case.get("min_sources") or 0)

    failures: List[str] = []

    if isinstance(expected_abstain, bool) and has_abstain != expected_abstain:
        failures.append(f"abstain_mismatch(expected={expected_abstain},actual={has_abstain})")

    if min_sources > 0 and source_count < min_sources:
        failures.append(f"min_sources_not_met({source_count}<{min_sources})")

    if expected_portal:
        p = _norm(expected_portal)
        cited_portals = [_norm(r.get("source_site")) for r in cited_rows]
        if cited_portals:
            if not any(p in c or c in p for c in cited_portals):
                failures.append(f"cited_portal_mismatch(expected~{expected_portal})")
        else:
            # if no citations, fallback to top retrieved rows for portal check
            top_portals = [_norm(r.get("source_site")) for r in src_rows[:5]]
            if not any(p in c or c in p for c in top_portals):
                failures.append(f"top_portal_mismatch(expected~{expected_portal})")

    # If abstaining, sources suffix should not be present.
    if has_abstain and src_suffix:
        failures.append("abstain_with_sources_suffix")

    return {
        "id": cid,
        "query": query,
        "passed": len(failures) == 0,
        "failures": failures,
        "has_abstain": has_abstain,
        "has_sources_suffix": src_suffix,
        "citations": cites,
        "source_count": source_count,
        "top_source_sites": [str(r.get("source_site") or "") for r in src_rows[:5]],
        "reply_preview": reply[:260],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run live chat smoke cases")
    ap.add_argument("--cases", default="artifacts/live_chat_smoke_queries.jsonl", help="JSONL cases file")
    ap.add_argument("--limit", type=int, default=0, help="limit number of cases (0=all)")
    ap.add_argument("--json-out", default="artifacts/live_chat_smoke_last.json", help="output JSON path")
    args = ap.parse_args()

    rows = _load_jsonl(args.cases)
    if args.limit and args.limit > 0:
        rows = rows[: int(args.limit)]

    reports = []
    for c in rows:
        try:
            reports.append(_case_eval(c))
        except Exception as exc:
            reports.append({
                "id": str(c.get("id") or "case"),
                "query": str(c.get("query") or ""),
                "passed": False,
                "failures": [f"exception:{exc}"],
                "has_abstain": None,
                "has_sources_suffix": None,
                "citations": [],
                "source_count": 0,
                "top_source_sites": [],
                "reply_preview": "",
            })

    passed = [r for r in reports if r.get("passed")]
    failed = [r for r in reports if not r.get("passed")]

    payload = {
        "summary": {
            "ok": len(failed) == 0,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "cases": len(reports),
            "passed": len(passed),
            "failed": len(failed),
            "pass_rate_pct": round(100.0 * len(passed) / max(1, len(reports)), 2),
            "failed_case_ids": [str(r.get("id") or "") for r in failed],
        },
        "cases": reports,
    }
    print(json.dumps(payload, indent=2))
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    return 0 if len(failed) == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

