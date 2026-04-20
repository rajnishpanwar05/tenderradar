from __future__ import annotations

import re
from typing import Any, Dict, List

_POSITIVE_CONSULTING_PATTERNS = {
    "evaluation": re.compile(
        r"\b(evaluation|impact evaluation|mid[- ]term review|endline|baseline|M&E|MEL|monitoring and evaluation)\b",
        re.I,
    ),
    "assessment": re.compile(
        r"\b(assessment|diagnostic|survey|feasibility|feasibility study|needs assessment|market study)\b",
        re.I,
    ),
    "advisory": re.compile(
        r"\b(advisory|technical assistance|TA\b|policy support|strategy|roadmap|reform support)\b",
        re.I,
    ),
    "institutional": re.compile(
        r"\b(institutional strengthening|system strengthening|capacity development|organizational development)\b",
        re.I,
    ),
    "capacity": re.compile(
        r"\b(capacity building|training|training of trainers|knowledge support)\b",
        re.I,
    ),
    "pmu": re.compile(
        r"\b(program management|project management unit|PMU|implementation support consultant|transaction advisory)\b",
        re.I,
    ),
    "social_sector": re.compile(
        r"\b(education|health|nutrition|livelihood|social protection|gender|governance|agriculture|climate|water|sanitation)\b",
        re.I,
    ),
}

_NEGATIVE_CONSULTING_PATTERNS = {
    "construction": re.compile(
        r"\b(construction|civil works|rehabilitation works|road works|metro rail|rail corridor|bridge project|highway|expressway|transmission line|substation|hydropower|wastewater project|sewerage services improvement)\b",
        re.I,
    ),
    "epc_turnkey": re.compile(
        r"\b(EPC|engineering[, /-]*procurement[, /-]*construction|turnkey|design-build|build[- ]operate)\b",
        re.I,
    ),
    "equipment": re.compile(
        r"\b(equipment procurement|supply and installation|installation of|plant installation|manufacturing|on-lending facility|financing project|solar power project|bus project|electric mobility|transmission infrastructure)\b",
        re.I,
    ),
    "engineering_delivery": re.compile(
        r"\b(contractor|developer|operation and maintenance|O&M|commissioning|distribution modernization|grid expansion|power distribution system)\b",
        re.I,
    ),
}

_NEAR_TENDER_PATTERNS = [
    re.compile(r"\b(expressions? of interest|EOI|request for expressions? of interest)\b", re.I),
    re.compile(r"\b(consulting services|selection of (firm|consultant)|tender packages?)\b", re.I),
    re.compile(r"\b(procurement plan|procurement notice|tender documentation|draft TOR|terms of reference)\b", re.I),
]

_PRE_TENDER_PATTERNS = [
    re.compile(r"\b(approved|approval|pipeline|advance procurement|upcoming procurement)\b", re.I),
    re.compile(r"\b(project preparation|implementation support|feasibility|design|supervision)\b", re.I),
]


def _combined_text(row: Dict[str, Any]) -> str:
    parts = [
        row.get("title"),
        row.get("summary"),
        row.get("sector"),
        row.get("organization"),
        row.get("source"),
        row.get("existing_consulting_signal"),
    ]
    return " ".join(str(p or "") for p in parts)


def _matched_labels(text: str, patterns: Dict[str, re.Pattern]) -> List[str]:
    matches: List[str] = []
    for label, pattern in patterns.items():
        if pattern.search(text):
            matches.append(label)
    return matches


def detect_consulting_signal(row: Dict[str, Any]) -> Dict[str, Any]:
    text = _combined_text(row)
    positive_matches = _matched_labels(text, _POSITIVE_CONSULTING_PATTERNS)
    negative_matches = _matched_labels(text, _NEGATIVE_CONSULTING_PATTERNS)

    existing = str(row.get("existing_consulting_signal") or "").strip()
    if existing:
        positive_matches.extend([part.strip() for part in existing.split(",") if part.strip()])

    deduped_positive = list(dict.fromkeys(positive_matches))
    deduped_negative = list(dict.fromkeys(negative_matches))

    strong_positive = any(
        tag in deduped_positive
        for tag in ("evaluation", "assessment", "advisory", "institutional", "pmu")
    )
    medium_positive = any(tag in deduped_positive for tag in ("capacity",)) and any(
        tag in deduped_positive for tag in ("advisory", "institutional", "pmu", "social_sector")
    )

    negative_strength = len(deduped_negative)

    tier = "LOW"
    consulting_signal = 0
    if strong_positive and negative_strength == 0:
        tier = "HIGH"
        consulting_signal = 1
    elif strong_positive and negative_strength == 1:
        tier = "MEDIUM"
        consulting_signal = 1
    elif medium_positive and negative_strength == 0:
        tier = "MEDIUM"
        consulting_signal = 1

    adjusted_confidence = int(row.get("confidence_score") or 0)
    if tier == "HIGH":
        adjusted_confidence = min(100, adjusted_confidence + 8)
    elif tier == "MEDIUM":
        adjusted_confidence = min(100, adjusted_confidence + 2)
    else:
        adjusted_confidence = max(0, adjusted_confidence - 18 - (5 * negative_strength))

    if negative_strength >= 2 and not strong_positive:
        consulting_signal = 0
        tier = "LOW"

    reasons: List[str] = [f"tier:{tier}"]
    if deduped_positive:
        reasons.append("positive=" + ", ".join(deduped_positive[:5]))
    if deduped_negative:
        reasons.append("negative=" + ", ".join(deduped_negative[:4]))
    return {
        "consulting_signal": consulting_signal,
        "consulting_signal_reason": " | ".join(reasons),
        "confidence_score": adjusted_confidence,
    }


def classify_signal_stage(row: Dict[str, Any]) -> str:
    text = _combined_text(row)
    raw_stage = str(row.get("raw_stage") or "").lower()
    confidence = int(row.get("confidence_score") or 0)
    has_procurement_intent = bool(row.get("procurement_signal"))
    has_consulting_signal = int(row.get("consulting_signal") or 0) == 1

    if any(p.search(text) for p in _NEAR_TENDER_PATTERNS):
        return "NEAR_TENDER"

    if raw_stage == "active" and confidence >= 55 and has_consulting_signal:
        return "NEAR_TENDER"

    if raw_stage in {"active", "approved"}:
        return "PRE_TENDER"

    if has_procurement_intent or any(p.search(text) for p in _PRE_TENDER_PATTERNS):
        return "PRE_TENDER"

    return "EARLY_SIGNAL"


def recommended_action(row: Dict[str, Any]) -> str:
    stage = str(row.get("signal_stage") or "")
    consulting_signal = int(row.get("consulting_signal") or 0)
    confidence = int(row.get("confidence_score") or 0)

    if stage == "NEAR_TENDER":
        return "Prepare early outreach" if consulting_signal else "Watch for TOR"
    if stage == "PRE_TENDER":
        return "Watch for TOR" if confidence >= 60 else "Review source page weekly"
    if consulting_signal:
        return "Likely future consulting opportunity"
    return "Monitor"


def classify_signal(row: Dict[str, Any]) -> Dict[str, Any]:
    enriched = dict(row)
    enriched.update(detect_consulting_signal(enriched))
    enriched["signal_stage"] = classify_signal_stage(enriched)
    enriched["recommended_action"] = recommended_action(enriched)
    return enriched
