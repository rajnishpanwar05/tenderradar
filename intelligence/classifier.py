# =============================================================================
# intelligence/classifier.py — Rule-Based Sector & Service Classifier
#
# Zero-cost, deterministic classification using FIRM_EXPERTISE keyword bank.
# No API calls. Works as a pre-filter before (or instead of) GPT extraction.
#
# Two classification axes:
#
#   Sector  — WHAT domain the tender addresses
#     health, education, environment, agriculture, water_sanitation,
#     urban_development, energy, governance, gender_inclusion,
#     infrastructure, research, finance, communications, tourism,
#     circular_economy
#
#   Service — WHAT type of work is being procured
#     evaluation_monitoring   ← M&E / baselines / TPM
#     consulting_advisory     ← advisory, TA, expert services
#     research_study          ← research, surveys, data collection
#     capacity_building       ← training, mentoring, CB programmes
#     audit_finance           ← audit, financial management
#     communications_media    ← IEC, media, BCC, creative
#     project_management      ← PMC, coordination, programme management
#
# Usage:
#   from intelligence.classifier import classify_tender, Classification
#   cls = classify_tender(title="Baseline Survey for WASH Programme",
#                         description="...")
#   cls.sectors        # ["water_sanitation"]
#   cls.service_types  # ["evaluation_monitoring", "research_study"]
#   cls.primary_sector # "water_sanitation"  (highest-scoring sector)
# =============================================================================

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from intelligence.keywords import FIRM_EXPERTISE


# =============================================================================
# SECTOR MAPPING
# Map each FIRM_EXPERTISE category → canonical sector slug
# =============================================================================

_SECTOR_MAP: Dict[str, str] = {
    "M&E / Evaluation":              "evaluation_monitoring",   # service, not sector
    "Environment & Social":          "environment",
    "Education & Skills":            "education",
    "Agriculture & Rural":           "agriculture",
    "Water & Sanitation":            "water_sanitation",
    "Social Protection & Health":    "health",
    "Urban Development":             "urban_development",
    "Energy & Power":                "energy",
    "Governance & Institutional":    "governance",
    "Research & Documentation":      "research",
    "Gender & Inclusion":            "gender_inclusion",
    "Capacity Building & Advisory":  "capacity_building",       # service, not sector
    "Communications & Media":        "communications",
    "Finance & Audit":               "finance",
    "Circular Economy & Waste":      "circular_economy",
    "Tourism & Ecology":             "tourism",
    "Infrastructure & Construction": "infrastructure",
}

# Categories that map to service_types (not sectors)
_SERVICE_CATEGORIES = {
    "M&E / Evaluation",
    "Capacity Building & Advisory",
    "Communications & Media",
    "Finance & Audit",
    "Research & Documentation",
}


# =============================================================================
# SERVICE-TYPE KEYWORD RULES
# These supplement FIRM_EXPERTISE with service-intent signals.
# =============================================================================

_SERVICE_RULES: List[Tuple[str, List[str]]] = [
    ("evaluation_monitoring", [
        "evaluation", "monitoring", "m&e", "baseline", "endline", "end-line",
        "mid-term", "midterm", "impact assessment", "impact evaluation",
        "performance review", "tpm", "third party monitoring", "iva",
        "concurrent monitoring", "real time monitoring", "kap survey",
        "rapid assessment", "verification", "review", "beneficiary feedback",
    ]),
    ("consulting_advisory", [
        "advisory", "technical assistance", "consultant", "consultancy",
        "expert", "specialist", "strategic", "advisor", "ta ",
        "knowledge partner", "hand-holding", "organizational development",
        "system strengthening", "rfp", "request for proposal",
        "terms of reference", "scope of work",
    ]),
    ("research_study", [
        "research", "study", "survey", "data collection", "analysis",
        "mapping", "scoping", "feasibility", "due diligence", "census",
        "enumeration", "profiling", "documentation", "gis", "dpr",
        "white paper", "concept note", "report",
    ]),
    ("capacity_building", [
        "training", "capacity building", "capacity development",
        "workshop", "mentoring", "coaching", "skill development",
        "knowledge transfer", "tvet", "teacher training", "iti",
        "curriculum", "learning programme",
    ]),
    ("audit_finance", [
        "audit", "internal audit", "forensic audit", "social audit",
        "financial management", "accounting", "fund utilization",
        "expenditure", "budget", "fiduciary", "cost-benefit",
    ]),
    ("communications_media", [
        "communication", "media", "iec", "branding", "content",
        "creative", "public relations", "film", "advocacy",
        "behavior change communication", "bcc", "outreach",
        "awareness campaign", "social media",
    ]),
    ("project_management", [
        "project management", "programme management", "program management",
        "pmc", "project management consultant", "coordination",
        "implementation support", "programme officer", "project officer",
    ]),
]


# =============================================================================
# Output dataclass
# =============================================================================

@dataclass
class Classification:
    """
    Result of classify_tender().

    Attributes:
        sectors       : List of matched sector slugs (deduplicated, ordered by score).
        service_types : List of matched service-type slugs.
        primary_sector: Highest-scoring sector, or None.
        raw_categories: Original FIRM_EXPERTISE category names that matched.
        scores        : {category_name: hit_count} for debugging.
    """
    sectors:        List[str] = field(default_factory=list)
    service_types:  List[str] = field(default_factory=list)
    primary_sector: Optional[str] = None
    raw_categories: List[str] = field(default_factory=list)
    scores:         Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "sectors":        self.sectors,
            "service_types":  self.service_types,
            "primary_sector": self.primary_sector,
        }


# =============================================================================
# Core classifier
# =============================================================================

def classify_tender(
    title: str,
    description: str = "",
    boost_title: int = 3,
) -> Classification:
    """
    Classify a tender by sector and service type.

    Title matches are weighted `boost_title` times more than body matches
    (default 3×) to avoid noisy description text overwhelming clear titles.

    Args:
        title       : Tender title (required).
        description : Full description / summary (optional but recommended).
        boost_title : Multiplier for title-level keyword hits.

    Returns:
        Classification dataclass with sectors, service_types, primary_sector.
    """
    title_low = _normalize_text(title)
    desc_low  = _normalize_text(description)

    # ── Step 1: score each FIRM_EXPERTISE category ────────────────────────────
    category_scores: Dict[str, int] = {}
    for category, keywords in FIRM_EXPERTISE.items():
        score = 0
        for kw in keywords:
            if kw in title_low:
                score += boost_title
            elif kw in desc_low:
                score += 1
        if score > 0:
            category_scores[category] = score

    matched_categories = sorted(
        category_scores.keys(),
        key=lambda c: category_scores[c],
        reverse=True,
    )

    # ── Step 2: derive sectors and service types from matched categories ───────
    sector_scores: Dict[str, int] = {}
    service_set: set = set()

    for cat in matched_categories:
        slug  = _SECTOR_MAP.get(cat)
        score = category_scores[cat]

        if cat in _SERVICE_CATEGORIES:
            # These categories always contribute to service_types
            _add_service_from_category(cat, service_set)
        else:
            # Pure sector categories
            if slug:
                sector_scores[slug] = sector_scores.get(slug, 0) + score

    # ── Step 3: score service types directly from text ────────────────────────
    combined_text = title_low * boost_title + " " + desc_low
    service_scores: Dict[str, int] = {}
    for stype, keywords in _SERVICE_RULES:
        hits = sum(1 for kw in keywords if kw in combined_text)
        if hits > 0:
            service_scores[stype] = hits
            service_set.add(stype)

    # ── Step 4: assemble result ───────────────────────────────────────────────
    ordered_sectors = sorted(sector_scores, key=lambda s: sector_scores[s], reverse=True)
    ordered_services = sorted(service_set, key=lambda s: service_scores.get(s, 0), reverse=True)

    return Classification(
        sectors        = ordered_sectors,
        service_types  = ordered_services,
        primary_sector = ordered_sectors[0] if ordered_sectors else None,
        raw_categories = matched_categories,
        scores         = category_scores,
    )


# =============================================================================
# Helpers
# =============================================================================

def _normalize_text(text: str) -> str:
    """Lowercase + collapse whitespace."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.lower().strip())


def _add_service_from_category(category: str, service_set: set) -> None:
    """Map service-oriented FIRM_EXPERTISE categories to service_type slugs."""
    _CAT_TO_SERVICE = {
        "M&E / Evaluation":             "evaluation_monitoring",
        "Capacity Building & Advisory": "capacity_building",
        "Communications & Media":       "communications_media",
        "Finance & Audit":              "audit_finance",
        "Research & Documentation":     "research_study",
    }
    slug = _CAT_TO_SERVICE.get(category)
    if slug:
        service_set.add(slug)


# =============================================================================
# Convenience: batch classify
# =============================================================================

def classify_batch(tenders: List[dict]) -> List[Classification]:
    """
    Classify a list of tender dicts.
    Each dict must have at least 'title'; 'description' is optional.
    """
    return [
        classify_tender(
            title       = t.get("title", ""),
            description = t.get("description", "") or t.get("summary", ""),
        )
        for t in tenders
    ]
