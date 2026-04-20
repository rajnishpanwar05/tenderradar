from __future__ import annotations

import json


def validate_chat_payload(raw_text: str, max_source_idx: int) -> dict:
    """
    Enforce grounded chat response contract:
      {"answer": str, "citations": [int]}
    """
    default = {
        "answer": "Insufficient grounded evidence in retrieved tenders.",
        "citations": [],
    }
    try:
        payload = json.loads(raw_text or "{}")
        if not isinstance(payload, dict):
            return default
        answer = str(payload.get("answer") or "").strip()
        if not answer:
            answer = default["answer"]
        raw_cites = payload.get("citations") or []
        if not isinstance(raw_cites, list):
            raw_cites = []
        valid = []
        for c in raw_cites:
            try:
                i = int(c)
            except Exception:
                continue
            if 1 <= i <= max_source_idx and i not in valid:
                valid.append(i)
        return {"answer": answer, "citations": valid}
    except Exception:
        return default
