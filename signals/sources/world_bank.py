from __future__ import annotations

import hashlib
import json
from typing import Dict, List

from scrapers.portals import wb_early_pipeline as wb_early
from signals.classifier import classify_signal


SOURCE_NAME = "World Bank Early Projects"
_MIN_GENERIC_CONFIDENCE = 30


def fetch_signal_rows(debug: bool = False) -> List[Dict]:
    raw_projects = wb_early._fetch_projects(debug=debug)
    rows: List[Dict] = []
    for record in raw_projects:
        project_id = wb_early._resolve(record, ["id"])
        title = wb_early._resolve(record, ["project_name"])
        sector = wb_early._extract_sector_text(record)
        summary = wb_early._extract_description(record)
        published_date = str(wb_early._resolve(record, ["boardapprovaldate"]))[:10] or None
        raw_stage = wb_early._classify_stage(
            wb_early._resolve(record, ["status"]),
            published_date,
        )
        combined_text = " ".join([summary, title, sector])
        signal_raw, matched = wb_early._consulting_signal_strength(combined_text)
        confidence_score, score_reason = wb_early.compute_base_score(
            approval_date=published_date,
            sectors=sector,
            country=wb_early._resolve(record, ["countryshortname"]),
            region=wb_early._resolve(record, ["regionname"]),
            description=combined_text,
        )

        normalized = {
            "source": SOURCE_NAME,
            "source_record_id": project_id,
            "title": title,
            "organization": wb_early._resolve(record, ["impagency"], "World Bank") or "World Bank",
            "geography": wb_early._resolve(record, ["countryshortname"]) or wb_early._resolve(record, ["regionname"]),
            "sector": sector,
            "summary": summary,
            "confidence_score": int(confidence_score or 0),
            "url": wb_early._resolve(record, ["url"]),
            "published_date": published_date,
            "captured_at": None,
            "raw_stage": raw_stage,
            "procurement_signal": 1 if published_date or raw_stage in {"approved", "active"} else 0,
            "existing_consulting_signal": ", ".join(matched[:8]),
            "metadata": {
                "status": wb_early._resolve(record, ["status"]),
                "lending_instrument": wb_early._resolve(record, ["lendinginstr"]),
                "score_reason": score_reason,
                "signal_raw": signal_raw,
                "region": wb_early._resolve(record, ["regionname"]),
            },
        }
        classified = classify_signal(normalized)
        if int(classified.get("consulting_signal") or 0) != 1:
            continue
        if int(classified.get("confidence_score") or 0) < _MIN_GENERIC_CONFIDENCE:
            continue
        classified["metadata_json"] = json.dumps(classified.get("metadata") or {}, ensure_ascii=True)
        classified["content_hash"] = hashlib.md5(
            (
                str(classified.get("title") or "")
                + "|"
                + str(classified.get("summary") or "")
                + "|"
                + str(classified.get("signal_stage") or "")
                + "|"
                + str(classified.get("confidence_score") or 0)
            ).encode("utf-8")
        ).hexdigest()
        rows.append(classified)
    return rows
