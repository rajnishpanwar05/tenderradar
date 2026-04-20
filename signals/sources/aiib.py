from __future__ import annotations

import hashlib
import json
import re
from typing import Dict, List

import requests

from signals.classifier import classify_signal

SOURCE_NAME = "AIIB Project Pipeline"
_DATA_URL = "https://www.aiib.org/en/projects/list/.content/all-projects-data.js"
_TARGET_ECONOMIES = {"India", "Bangladesh", "Nepal", "Sri Lanka", "Bhutan", "Pakistan", "Maldives"}
_ALLOWED_STATUS = {"Proposed", "Approved"}
_MIN_CONFIDENCE = 28
_HEADERS = {"User-Agent": "Mozilla/5.0 (TenderRadar signal adapter)"}


def _load_records() -> List[Dict]:
    text = requests.get(_DATA_URL, timeout=30, headers=_HEADERS).text
    match = re.search(r"var data=\[(.*)\];?\s*$", text, re.S)
    if not match:
        return []
    body = re.sub(r",\s*\Z", "", match.group(1).strip())
    return json.loads("[" + body + "]")


def _seed_consulting_signal(name: str, sector: str, project_type: str, status: str) -> str:
    text = " ".join([name, sector, project_type, status]).lower()
    matches: List[str] = []
    patterns = {
        "capacity": ["capacity", "institutional", "reform", "modernization", "readiness"],
        "assessment": ["assessment", "resilience", "preparedness", "planning", "design"],
        "social_sector": ["education", "health", "water", "sanitation", "climate", "urban", "agriculture"],
        "advisory": ["program", "policy", "management", "sustainable", "transition"],
    }
    for label, terms in patterns.items():
        if any(term in text for term in terms):
            matches.append(label)
    return ", ".join(dict.fromkeys(matches))


def fetch_signal_rows(debug: bool = False) -> List[Dict]:
    rows: List[Dict] = []
    for record in _load_records():
        economy = str(record.get("economy") or "").strip()
        status = str(record.get("status") or "").strip()
        if economy not in _TARGET_ECONOMIES or status not in _ALLOWED_STATUS:
            continue
        title = str(record.get("name") or "").strip()
        sector = str(record.get("sector") or "").strip()
        project_type = str(record.get("project_type") or "").strip()
        published_year = str(record.get("date") or "").strip()
        url = "https://www.aiib.org" + str(record.get("path") or "").strip()
        summary = f"{status} AIIB project in {economy}. Sector: {sector or 'Unknown'}. Financing: {record.get('financing_type') or 'Unknown'}."
        raw_stage = "pipeline" if status == "Proposed" else "approved"
        confidence = 38 if status == "Proposed" else 33
        normalized = {
            "source": SOURCE_NAME,
            "source_record_id": title,
            "title": title,
            "organization": "Asian Infrastructure Investment Bank",
            "geography": economy,
            "sector": sector,
            "summary": summary,
            "confidence_score": confidence,
            "url": url,
            "published_date": f"{published_year}-01-01" if published_year.isdigit() and len(published_year) == 4 else None,
            "captured_at": None,
            "raw_stage": raw_stage,
            "procurement_signal": 1 if status == "Approved" else 0,
            "existing_consulting_signal": _seed_consulting_signal(title, sector, project_type, status),
            "metadata": {
                "status": status,
                "project_type": project_type,
                "financing_type": record.get("financing_type"),
                "approved_funding": record.get("approved_funding"),
                "proposed_funding": record.get("proposed_funding"),
            },
        }
        classified = classify_signal(normalized)
        if int(classified.get("consulting_signal") or 0) != 1:
            continue
        if int(classified.get("confidence_score") or 0) < _MIN_CONFIDENCE:
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
