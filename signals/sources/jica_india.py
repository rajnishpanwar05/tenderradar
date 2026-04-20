from __future__ import annotations

import hashlib
import json
from typing import Dict, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from signals.classifier import classify_signal

SOURCE_NAME = "JICA India Programs"
_ACTIVITIES_URL = "https://www.jica.go.jp/english/overseas/india/activities/index.html"
_COUNTRY_URL = "https://www.jica.go.jp/english/overseas/india/index.html"
_HEADERS = {"User-Agent": "Mozilla/5.0 (TenderRadar signal adapter)"}


def _get_html(url: str) -> BeautifulSoup:
    html = requests.get(url, timeout=30, headers=_HEADERS).text
    return BeautifulSoup(html, "html.parser")


def _clean(text: str) -> str:
    return " ".join((text or "").split())


def fetch_signal_rows(debug: bool = False) -> List[Dict]:
    rows: List[Dict] = []

    activities = _get_html(_ACTIVITIES_URL)
    country = _get_html(_COUNTRY_URL)

    country_text = _clean(country.get_text(" ", strip=True))
    overview_summary = country_text[:1400]

    records = [
        {
            "title": "JICA Operations and Activities in India 2025-2026",
            "organization": "Japan International Cooperation Agency",
            "geography": "India",
            "sector": "Development cooperation, infrastructure, skills, health, education",
            "summary": overview_summary,
            "url": urljoin(_ACTIVITIES_URL, "/english/overseas/india/activities/__icsFiles/afieldfile/2026/04/09/JICA_Brochure_FY2025-FY2026.pdf"),
            "raw_stage": "pipeline",
            "existing_consulting_signal": "capacity, advisory, social_sector",
            "confidence_score": 34,
        },
        {
            "title": "JICA Training and Dialogue Programs in India",
            "organization": "Japan International Cooperation Agency",
            "geography": "India",
            "sector": "Capacity building, training, human resource development",
            "summary": "JICA India activities page highlights training, dialogue programs, human resource development, and quality education support in India.",
            "url": urljoin(_ACTIVITIES_URL, "/india/english/activities/training.html"),
            "raw_stage": "pipeline",
            "existing_consulting_signal": "capacity, training, advisory",
            "confidence_score": 36,
        },
        {
            "title": "JICA Major Projects in India Map",
            "organization": "Japan International Cooperation Agency",
            "geography": "India",
            "sector": "Infrastructure planning, transport, water, urban development",
            "summary": "JICA India activities page links a major projects map that signals pipeline and program activity across infrastructure and public-sector development in India.",
            "url": "https://libportal.jica.go.jp/library/Data/PlanInOperation-e/EastSouthAsia/054_India-e.pdf",
            "raw_stage": "pipeline",
            "existing_consulting_signal": "assessment, capacity, social_sector",
            "confidence_score": 31,
        },
    ]

    for record in records:
        normalized = {
            "source": SOURCE_NAME,
            "source_record_id": record["title"],
            "title": record["title"],
            "organization": record["organization"],
            "geography": record["geography"],
            "sector": record["sector"],
            "summary": record["summary"],
            "confidence_score": record["confidence_score"],
            "url": record["url"],
            "published_date": None,
            "captured_at": None,
            "raw_stage": record["raw_stage"],
            "procurement_signal": 0,
            "existing_consulting_signal": record["existing_consulting_signal"],
            "metadata": {"source_page": _ACTIVITIES_URL},
        }
        classified = classify_signal(normalized)
        if int(classified.get("consulting_signal") or 0) != 1:
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
