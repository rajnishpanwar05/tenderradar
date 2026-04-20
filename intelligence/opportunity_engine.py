# =============================================================================
# intelligence/opportunity_engine.py — Opportunity Intelligence Layer
#
# Computes four opportunity-level scores for every tender and persists them
# as additional columns on the existing `tender_structured_intel` table.
#
#   priority_score    — 0–100 composite opportunity priority
#                       = relevance×0.40 + bid_fit×0.30
#                         + deadline_urgency×0.20 + client_importance×0.10
#
#   competition_level — low | medium | high
#                       Heuristic: multilateral donors → high;
#                       national govt / regional orgs → medium;
#                       niche NGOs / unknown → low
#
#   opportunity_size  — small | medium | large
#                       Keyword heuristics from title+description
#
#   complexity_score  — 0–100
#                       Derived from document length + requirements density
#                       + multi-stakeholder / scope keywords
#
# Design constraints:
#   • NO scrapers, runner, vector store, or Excel exporter modified
#   • Entirely optional — any failure degrades to a non-fatal warning
#   • Reads intel from tender_structured_intel when available (avoids
#     re-computing relevance_score + org/deadline attributes)
#   • Idempotent: re-scoring the same tender_id overwrites silently
#   • Runs after tender_intelligence.py in the same pipeline run
#
# Public API:
#   score_one(tender, intel=None)   → dict  (4 opportunity scores + tender_id)
#   score_batch(tenders, intel_map) → list[dict]
#   store_scores(scored)            → int   (rows updated in DB)
#   score_and_store_batch(tenders)  → int   (convenience: fetch intel → score → store)
#   extend_schema()                 → None  (adds 4 columns if absent)
#
# CLI test:
#   python3 intelligence/opportunity_engine.py
#   python3 intelligence/opportunity_engine.py --backfill [--limit N]
# =============================================================================

import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger("tenderradar.opportunity_engine")

# ── Ensure package root is on sys.path when run directly ──────────────────────
_BASE = os.path.expanduser("~/tender_system")
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)

# ── Shadow label model (lazy, fail-safe import) ────────────────────────────────
try:
    from intelligence.label_model import predict_shadow_score as _predict_shadow_score
    from intelligence.label_model import get_shadow_note as _get_shadow_note
    _SHADOW_MODEL_AVAILABLE = True
except Exception:
    _SHADOW_MODEL_AVAILABLE = False
    def _predict_shadow_score(row):  # type: ignore[misc]
        return 50.0
    def _get_shadow_note(row):  # type: ignore[misc]
        return ""


# =============================================================================
# FIRM PROFILE — loaded once at import time
# =============================================================================

def _load_firm_profile() -> Dict[str, Any]:
    """Load config/firm_profile.json. Returns safe defaults if file missing."""
    profile_path = os.path.join(_BASE, "config", "firm_profile.json")
    try:
        with open(profile_path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "preferred_sectors":          ["education", "health", "governance"],
            "preferred_regions":          ["South Asia", "Africa"],
            "preferred_clients":          ["World Bank", "UNDP", "UNICEF", "GIZ"],
            "preferred_consulting_types": ["evaluation", "research", "capacity building"],
            "avoid_sectors":              ["infrastructure", "transport"],
            "avoid_keywords":             ["supply of", "civil works", "construction of"],
            "score_boosts":   {"preferred_sector": 10, "preferred_region": 6,
                               "preferred_client": 15, "preferred_consulting_type": 5},
            "score_penalties":{"avoid_sector": -15, "avoid_keyword_in_title": -20,
                               "unknown_organization": -8, "expired_deadline": -999,
                               "unknown_consulting_type": -5},
        }

_FIRM_PROFILE = _load_firm_profile()

# Hybrid scoring weights (can be overridden by env)
_W_ML = float(os.environ.get("RELEVANCE_WEIGHT_ML", "0.50"))
_W_PORTFOLIO = float(os.environ.get("RELEVANCE_WEIGHT_PORTFOLIO", "0.30"))
_W_KEYWORDS = float(os.environ.get("RELEVANCE_WEIGHT_KEYWORDS", "0.20"))
_W_SUM = max(0.0001, (_W_ML + _W_PORTFOLIO + _W_KEYWORDS))
_W_ML /= _W_SUM
_W_PORTFOLIO /= _W_SUM
_W_KEYWORDS /= _W_SUM
_ENABLE_SEMANTIC_RERANK = os.environ.get("OPP_ENABLE_SEMANTIC_RERANK", "0").strip().lower() in (
    "1", "true", "yes", "on"
)


def _score_ml(tender: Dict[str, Any]) -> float:
    try:
        from intelligence.relevance_model import get_model
        m = get_model()
        return float(m.score(tender))
    except Exception as exc:
        logger.debug("[opportunity_engine] ML score fallback: %s", exc)
        # Conservative fallback (not neutral 50) so unscored tenders don’t
        # artificially inflate priority via the ML weight component.
        return 40.0


def _score_portfolio_similarity(tender: Dict[str, Any]) -> float:
    try:
        from intelligence.portfolio_similarity import get_portfolio_scorer
        s = get_portfolio_scorer()
        return float(s.score(tender))
    except Exception as exc:
        logger.debug("[opportunity_engine] portfolio similarity fallback: %s", exc)
        # Same conservative fallback as ML — do not inflate unscored tenders.
        return 40.0


# =============================================================================
# HARD CONSULTING GATE
# Detects and excludes goods / civil-works / manpower tenders before scoring.
# Returns (is_relevant: bool, reason: str) — False means exclude (score → 0).
# Errs on the side of INCLUSION: only excludes when evidence is unambiguous.
# =============================================================================

# Anchor phrases strong enough that a single hit in the *title* is conclusive.
_HARD_EXCLUDE_ANCHOR_PHRASES: List[str] = [
    # Supply / goods
    "supply of ",           "supply and delivery",      "supply & delivery",
    "procurement of goods", "procurement of material",  "purchase of ",
    "rate contract for supply", "empanelment of vendor", "empanelment of supplier",
    # Construction / civil works
    "construction of ",     "civil construction",        "civil works contract",
    "road construction",    "road repair",               "road widening",
    "bridge construction",  "dam construction",          "building construction",
    "renovation of building", "erection and commissioning",
    "installation of solar panel", "installation of pump", "pipe laying work",
    # Manpower / housekeeping
    "manpower supply",      "supply of manpower",        "deployment of security guard",
    "housekeeping services", "sweeping and cleaning services",
    "data entry operator supply",
    # Clearly non-consulting professional services
    "empanelment of advocate", "empanelment of ca firm",
    "appointment of statutory auditor", "statutory audit of",
    "software development for", "development of mobile app",
    "website development for", "erp implementation for",
]

# Lighter signals — two or more in the title = exclude (if no consulting override)
_SOFT_NEGATIVE_TITLE_WORDS: List[str] = [
    "supply of", "civil work", "road repair", "maintenance contract",
    " amc ", "equipment supply", "hardware supply", "printed material",
    "stationery", "printing of", "labour supply", "security services",
    "housekeeping", "excavation", "earthwork", "electrical work", "plumbing",
    "installation of", "erection of", "boring of well",
]

# Any of these in title or description overrides negative signals — keep tender.
_CONSULTING_OVERRIDE_WORDS: List[str] = [
    "evaluation", "evaluating", "evaluat", "assessment", "consulting",
    "consultant", "study", "survey", "advisory", "technical assistance",
    "capacity building", "monitoring", "review", "feasibility", "research",
    "baseline", "endline", "mid-term", "rfp", "expression of interest", "eoi",
    "terms of reference", "tpm", "third party monitoring",
    "independent verification", "impact assessment",
]


def _is_consulting_relevant(
    title: str,
    description: str,
    consulting_type: str,
    text: str,
) -> tuple:
    """
    Hard gate: return (is_relevant, exclusion_reason).
    is_relevant=False forces priority_score → 0 in score_one().
    The row is kept; it just sinks to the bottom of the ranked output.
    """
    title_lower = title.lower().strip()
    text_lower  = text.lower()

    # ── 1. Consulting override: if present anywhere, always keep ────────
    has_consulting = any(w in text_lower for w in _CONSULTING_OVERRIDE_WORDS)

    # ── 2. Hard anchor phrase in title → exclude unless override present ───
    for phrase in _HARD_EXCLUDE_ANCHOR_PHRASES:
        if phrase in title_lower:
            if has_consulting:
                return True, ""   # e.g. "Supply of consulting services" — keep
            return False, f"goods/works anchor in title: ‘{phrase.strip()}’"

    # ── 3. Multiple soft-negative signals without any consulting signal ────
    if not has_consulting:
        soft_hits = [w for w in _SOFT_NEGATIVE_TITLE_WORDS if w in title_lower]
        if len(soft_hits) >= 2:
            return False, (
                f"multiple non-consulting signals in title "
                f"({', '.join(h.strip() for h in soft_hits[:3])})"
            )
        # Single soft-negative + consulting_type unknown = confident exclusion
        if soft_hits and consulting_type == "unknown":
            return False, (
                f"non-consulting signal ‘{soft_hits[0].strip()}’ "
                f"+ unknown consulting type"
            )

    return True, ""


# Consulting types that represent core IDCG capability (for scoring notes)
_POSITIVE_CONSULTING_TYPES = {
    "evaluation", "research", "technical assistance",
    "capacity building", "policy", "advisory", "feasibility study",
}

_CONSULTING_SIGNAL_PATTERNS = [
    r"\bevaluation\b", r"\bimpact evaluation\b", r"\bassessment\b",
    r"\bbaseline\b", r"\bendline\b", r"\bmid[- ]?term\b", r"\badvisory\b",
    r"\bconsult(?:ant|ancy|ing)?\b", r"\btechnical assistance\b",
    r"\bresearch\b", r"\bstudy\b", r"\bsurvey\b", r"\bmonitoring\b",
    r"\bcapacity building\b", r"\bthird party monitoring\b", r"\btpm\b",
    r"\biva\b", r"\bterms of reference\b", r"\bexpression of interest\b", r"\beoi\b",
]

_PROCUREMENT_NEGATIVE_PATTERNS = [
    r"\bprocurement of goods\b", r"\bsupply of\b", r"\bcivil works?\b",
    r"\bconstruction of\b", r"\bmanpower supply\b", r"\bhousekeeping\b",
    r"\bsecurity services?\b", r"\bsoftware development\b", r"\blegal services?\b",
    r"\bchartered accountant\b", r"\bstatutory audit(?:or)?\b", r"\bequipment\b",
]

_IC_ROLE_PATTERNS = [
    # Core IC / individual-only engagement patterns
    r"\bindividual consultant\b",
    r"\bindividual contractor\b",
    r"\bindividual expert\b",
    r"\bshort[- ]?term consultant\b",
    r"\bnational consultant\b",
    r"\binternational consultant\b",
    # Standalone UNDP/UN vacancy abbreviations — only as standalone tokens
    # Use lookahead/lookbehind so "IDCG" and "IC&A" are not caught
    r"(?<!\w)IC(?!\w)",       # matches " IC " / "[IC]" / "- IC " but not "IDCG" or "IC&A"
    r"(?<!\w)STC(?!\w)",      # short-term contractor (UN abbreviation)
    r"(?<!\w)LTC(?!\w)",      # long-term contractor (some UN portals)
    # External collaborator / non-firm role labels
    r"\bexternal collaborator\b",
    r"\bexternal expert\b",
    r"\bfreelance\b",
    # Pure translation / editorial / media-only (not IDCG consulting work)
    r"\btranslat(?:ion services?|ors?)\b",
    r"\binterpreter\b",
    r"\beditorial service\b",
    r"\bmedia buy(?:ing|er)\b",
    r"\bphotographer\b",
    r"\bvideograph(?:er|y)\b",
]

# ── CAPSTAT-derived scoring patterns ──────────────────────────────────────────
# Service match: based on IDCG CAPSTAT — M&E, advisory, TA, IVA, research
_CAPSTAT_SERVICE_STRONG = [
    r"\bevaluation\b", r"\bM&E\b", r"\bMEL\b", r"\bmonitoring.*evaluation\b",
    r"\bimpact assessment\b", r"\bimpact evaluation\b",
    r"\bbaseline\b", r"\bmidline\b", r"\bendline\b",
    r"\bindependent verification\b", r"\bIVA\b", r"\bTPM\b",
    r"\bthird.?party monitoring\b", r"\binstitutional strengthening\b",
]
_CAPSTAT_SERVICE_MODERATE = [
    r"\badvisory\b", r"\btechnical assistance\b", r"\bcapacity building\b",
    r"\bpolicy\b", r"\bgovernance support\b",
    r"\bfeasibility\b", r"\bdiagnostic\b",
    r"\bsurvey\b", r"\bmixed methods\b", r"\bdata collection\b",
    r"\bresearch\b", r"\bstudy\b",
]

# Sector match: sectors well-represented in IDCG CAPSTAT
_CAPSTAT_SECTOR_STRONG = [
    r"\beducation\b", r"\bhealth\b", r"\bnutrition\b",
    r"\bgovernance\b", r"\bpublic policy\b",
    r"\bagriculture\b", r"\blivelihoods?\b",
    r"\bclimate\b", r"\benvironment\b", r"\bforestry\b",
    r"\benergy\b", r"\brenewable energy\b",
    r"\brural development\b", r"\bskills\b", r"\bMSME\b",
    r"\bsocial protection\b", r"\bgender\b", r"\binclusion\b",
    r"\bwater\b", r"\bsanitation\b",
]
_CAPSTAT_SECTOR_MODERATE = [
    r"\burban development\b", r"\bfinancial inclusion\b",
    r"\bdigital\b", r"\bdisaster\b", r"\bresilience\b",
    r"\bvocational training\b", r"\bwaste management\b",
]

# Client match: broad IDCG-relevant client universe
_CAPSTAT_CLIENT_TIER1 = {
    "World Bank", "IFC", "ADB", "AfDB", "UNDP", "UNICEF", "FAO", "WFP",
    "WHO", "ILO", "GIZ", "KfW", "AFD", "JICA", "MCC", "USAID", "FCDO",
    "DFID", "European Commission", "European Union", "IFAD",
}
_CAPSTAT_CLIENT_TIER2_PATTERNS = [
    r"\bIUCN\b", r"\bWinrock\b", r"\bTNC\b", r"\bRoom to Read\b",
    r"\bSave the Children\b", r"\bTata Trust\b", r"\bHans Foundation\b",
    r"\bMicroSave\b", r"\bMSC\b", r"\bCUTS\b", r"\bCLASP\b",
    r"\bWRI\b", r"\bSELCO\b", r"\bOxfam\b", r"\bCARE\b",
    r"\bPlan International\b", r"\bBritish Council\b", r"\bCEGA\b",
    r"\bLeadership for Equity\b", r"\bReliance Foundation\b",
]

_CLIENT_FIT_BOOST = {
    "World Bank": 18, "UNDP": 16, "UNICEF": 14, "AfDB": 12, "ADB": 12, "GIZ": 10,
}

# ── Blended scoring weights (configurable via env vars) ───────────────────────
# existing engine: 45%, shadow ML label model: 35%, CAPSTAT fit signal: 20%
_BLEND_WEIGHT_ENGINE  = float(os.getenv("BLEND_WEIGHT_ENGINE",  "0.45"))
_BLEND_WEIGHT_SHADOW  = float(os.getenv("BLEND_WEIGHT_SHADOW",  "0.35"))
_BLEND_WEIGHT_CAPSTAT = float(os.getenv("BLEND_WEIGHT_CAPSTAT", "0.20"))


def _count_pattern_hits(text: str, patterns: List[str]) -> int:
    if not text:
        return 0
    hits = 0
    for pat in patterns:
        if re.search(pat, text, flags=re.IGNORECASE):
            hits += 1
    return hits


def _capstat_service_match(title: str, text: str, rich_text: str) -> int:
    """
    CAPSTAT-derived service fit.
    Returns 5 (strong), 3 (moderate), or 0 (none).
    """
    combined = title + " " + text + " " + rich_text
    if _count_pattern_hits(combined, _CAPSTAT_SERVICE_STRONG) >= 1:
        return 5
    if _count_pattern_hits(combined, _CAPSTAT_SERVICE_MODERATE) >= 1:
        return 3
    return 0


def _capstat_sector_match(title: str, text: str, rich_text: str, sector: str) -> int:
    """
    CAPSTAT-derived sector fit.
    Returns 4 (strong), 2 (moderate), or 0 (none).
    """
    combined = title + " " + text + " " + rich_text + " " + (sector or "")
    if _count_pattern_hits(combined, _CAPSTAT_SECTOR_STRONG) >= 1:
        return 4
    if _count_pattern_hits(combined, _CAPSTAT_SECTOR_MODERATE) >= 1:
        return 2
    return 0


def _capstat_client_match(organization: str, text: str) -> int:
    """
    CAPSTAT-derived client fit using a broader IDCG-relevant client universe.
    Returns 8 (Tier-1 donor), 3 (Tier-2 partner), or 0 (none).
    """
    if (organization or "").strip() in _CAPSTAT_CLIENT_TIER1:
        return 8
    combined = (organization or "") + " " + (text or "")
    if any(re.search(pat, combined, flags=re.IGNORECASE) for pat in _CAPSTAT_CLIENT_TIER2_PATTERNS):
        return 3
    return 0


def _compute_consulting_confidence(
    consulting_type: str,
    title: str,
    text: str,
    rich_text: str,
) -> int:
    """
    0-100 confidence that this tender is true consulting demand (not goods/works).
    Uses current type classification + keyword evidence from shallow + rich text.
    """
    ctype_base = {
        "evaluation": 86,
        "research": 80,
        "technical assistance": 78,
        "capacity building": 76,
        "policy": 72,
        "advisory": 68,
        "feasibility study": 64,
        "implementation support": 52,
        "unknown": 30,
    }.get((consulting_type or "unknown").strip().lower(), 40)

    shallow_hits = _count_pattern_hits((title + " " + text), _CONSULTING_SIGNAL_PATTERNS)
    deep_hits = _count_pattern_hits(rich_text, _CONSULTING_SIGNAL_PATTERNS)
    neg_hits = _count_pattern_hits((title + " " + text + " " + rich_text), _PROCUREMENT_NEGATIVE_PATTERNS)

    score = float(ctype_base)
    score += min(18, shallow_hits * 4.0)
    score += min(12, deep_hits * 3.0)
    score -= min(30, neg_hits * 6.0)
    return int(max(0, min(100, round(score))))


def _compute_service_fit(consulting_type: str, consulting_confidence: int) -> int:
    """
    Explicit service-fit score with confidence modulation.
    """
    base = _compute_bid_fit(consulting_type)
    adjusted = (0.8 * base) + (0.2 * consulting_confidence)
    return int(max(0, min(100, round(adjusted))))


def _compute_client_fit(organization: str, region: str = "global") -> int:
    """
    Explicit client-fit score aligned to firm profile + donor priority.
    """
    base = _compute_client_importance(organization)
    bonus = _CLIENT_FIT_BOOST.get(organization, 0)

    preferred_clients = {
        str(c).strip().lower() for c in (_FIRM_PROFILE.get("preferred_clients") or [])
    }
    if organization and organization.strip().lower() in preferred_clients:
        bonus += 8

    preferred_regions = {
        str(r).strip().lower() for r in (_FIRM_PROFILE.get("preferred_regions") or [])
    }
    if region and str(region).strip().lower() in preferred_regions:
        bonus += 4

    return int(max(0, min(100, base + bonus)))


def _compute_procurement_penalty(
    title: str,
    text: str,
    rich_text: str,
    consulting_confidence: int,
) -> int:
    """
    Penalty for procurement-style language that should push rank down.
    """
    neg_hits = _count_pattern_hits((title + " " + text + " " + rich_text), _PROCUREMENT_NEGATIVE_PATTERNS)
    penalty = min(28, neg_hits * 6)
    if consulting_confidence < 45:
        penalty += 4
    return int(max(0, min(35, penalty)))


def _semantic_rerank_boost(
    query_text: str,
    keyword_score: float,
    consulting_confidence: int,
) -> float:
    """
    Optional targeted semantic rerank.
    Disabled by default; enabled via OPP_ENABLE_SEMANTIC_RERANK=1.
    Only runs for shortlist candidates to keep cost bounded.
    """
    if not _ENABLE_SEMANTIC_RERANK:
        return 0.0
    if keyword_score < 45 or consulting_confidence < 55 or len(query_text.strip()) < 30:
        return 0.0
    try:
        from intelligence.vector_store import find_similar_tenders
        sims = find_similar_tenders(query_text, top_k=3)
        if not sims:
            return 0.0
        avg_sim = sum(float(s.get("similarity", 0.0)) for s in sims[:3]) / max(1, min(3, len(sims)))
        # Max +6 boost at very strong semantic agreement.
        return float(max(0.0, min(6.0, (avg_sim - 0.45) * 14.0)))
    except Exception as exc:
        logger.debug("[opportunity_engine] semantic rerank fallback: %s", exc)
        return 0.0


def _build_scoring_note(
    is_consulting: bool,
    exclusion_reason: str,
    priority_score: int,
    consulting_type: str,
    organization: str,
    sector: str,
    is_low_confidence: bool,
    client_fit_score: int = 0,
    service_fit_score: int = 0,
    consulting_confidence_score: int = 0,
    procurement_penalty_score: int = 0,
) -> str:
    """1-line analyst-readable explanation of why a tender received its score."""
    if not is_consulting:
        return f"Excluded: {exclusion_reason}."
    if is_low_confidence:
        return "Low confidence: no enrichment data available for this tender."
    if priority_score >= 70:
        parts = []
        if consulting_type in _POSITIVE_CONSULTING_TYPES:
            parts.append(consulting_type)
        if organization and organization not in ("unknown", "Government Agency"):
            parts.append(organization)
        if sector and sector not in ("unknown",):
            parts.append(sector)
        detail = " + ".join(parts) or consulting_type or sector or "consulting match"
        return (
            f"Strong consulting fit: {detail}. "
            f"(client={client_fit_score}, service={service_fit_score}, confidence={consulting_confidence_score})"
        )[:390]
    if priority_score >= 40:
        ct = consulting_type if consulting_type != "unknown" else "advisory"
        return (
            f"Medium consulting fit: {ct} ({sector}). "
            f"(client={client_fit_score}, service={service_fit_score}, confidence={consulting_confidence_score})"
        )[:390]
    if priority_score > 0:
        if consulting_type not in _POSITIVE_CONSULTING_TYPES:
            return (
                f"Weak: sector match only, limited consulting signal "
                f"(confidence={consulting_confidence_score}, penalty={procurement_penalty_score})."
            )[:390]
        return (
            f"Low priority: {consulting_type} in {sector} "
            f"(confidence={consulting_confidence_score}, penalty={procurement_penalty_score})."
        )[:390]
    return "Unscored: consulting type unclear or no enrichment data."


# =============================================================================
# SECTION 1 — Scoring Lookup Tables
# =============================================================================

# ---------------------------------------------------------------------------
# 1A  BID-FIT SCORE by consulting_type
#     Reflects how well each engagement type aligns with IDCG's core
#     capability set (evaluation, research, TA, policy).
#     Redesigned for a wider spread — unknown types now score very low.
# ---------------------------------------------------------------------------
_BID_FIT: Dict[str, int] = {
    "evaluation":              100,   # core IDCG competency
    "research":                 92,
    "capacity building":        88,
    "policy":                   84,
    "technical assistance":     82,
    "advisory":                 74,
    "feasibility study":        68,
    "implementation support":   52,
    "unknown":                  22,   # cannot assess fit — strong penalty
}

# ---------------------------------------------------------------------------
# 1B  DEADLINE URGENCY SCORE
#     "soon" (7–30 days) = sweet spot.  "normal" reduced to 50 (not urgent).
#     "unknown" reduced to 35 to avoid inflating scores for undated tenders.
# ---------------------------------------------------------------------------
_URGENCY_SCORE: Dict[str, int] = {
    # ── Keys produced by tender_intelligence._classify_deadline() ────────────
    # These are the ACTUAL values stored in tender_structured_intel.deadline_category.
    "closing_soon":  82,   # 0–7 days: tight window, still actionable
    "needs_action": 100,   # 8–21 days: sweet spot — urgent but workable
    "plan_ahead":    52,   # 22+ days: plenty of time, not pressing
    "expired":        0,   # hard zero — never bid on expired
    "unknown":       35,   # no deadline info — penalise uncertainty
    # ── Legacy aliases (backward compat with any old DB rows) ────────────────
    "soon":         100,   # old name for needs_action
    "urgent":        82,   # old name for closing_soon
    "normal":        52,   # old name for plan_ahead
}

# ---------------------------------------------------------------------------
# 1C  CLIENT IMPORTANCE
#     Tier 1 (multilateral donors) → 100
#     Tier 2 (regional/national bodies) → 58
#     Government Agency / Ministry → 38
#     Unknown → 15  (significantly penalised for wider spread)
# ---------------------------------------------------------------------------
_HIGH_IMPORTANCE_ORGS = {
    "World Bank", "UNDP", "UNICEF", "ADB", "AfDB", "AFD",
    "European Union", "USAID", "FCDO/DFID", "WFP", "FAO",
    "IFAD", "GIZ", "WHO",
}

_MEDIUM_IMPORTANCE_ORGS = {
    "IOM", "UNHCR", "IDB", "EIB", "IFC",
    "NITI Aayog", "SAM.gov", "GeM India", "ICFRE", "SIDBI", "PHFI",
    "NHM India",
}

# ---------------------------------------------------------------------------
# 1D  COMPETITION LEVEL
#     High: large multilateral tenders attract 50–200+ bids globally.
#     Medium: regional/national tenders attract a moderate field.
#     Low:  niche NGOs, CAPTCHA-gated portals, restricted tenders.
# ---------------------------------------------------------------------------
_HIGH_COMPETITION_ORGS = {
    "World Bank", "UNDP", "UNICEF", "ADB", "AfDB",
    "European Union", "USAID", "FCDO/DFID", "WFP",
    "FAO", "GIZ", "WHO",
}

_MEDIUM_COMPETITION_ORGS = {
    "AFD", "IOM", "UNHCR", "IDB", "EIB", "IFC", "IFAD",
    "NITI Aayog", "SAM.gov", "GeM India", "Government Agency",
}

# Text signals that explicitly signal competition level
_HIGH_COMP_SIGNALS = [
    "international competitive bidding", "open international",
    "global rfp", "open competition",
]
_LOW_COMP_SIGNALS = [
    "restricted tender", "sole source", "direct contracting",
    "limited competition", "single source",
]

# ---------------------------------------------------------------------------
# 1E  OPPORTUNITY SIZE keywords
# ---------------------------------------------------------------------------
_LARGE_SIZE_KEYWORDS = [
    "multi-year", "multi year", "national program", "national programme",
    "framework contract", "technical assistance facility", "multi-state",
    "country-wide", "nationwide", "national level", "strategic framework",
    "multi-phase", "long-term contract", "blanket purchase",
    "indefinite delivery", "roster of firms", "roaster of firms",
    "consortium required", "sub-contractor",
]

_SMALL_SIZE_KEYWORDS = [
    "pilot project", "one district", "single district",
    "community level", "community-level", "individual consultant",
    "individual consultancy", "3 months", "three months",
    "short assignment", "small grant", "micro-contract",
]

# ---------------------------------------------------------------------------
# 1F  COMPLEXITY — requirements and scope keywords
# ---------------------------------------------------------------------------
_REQ_KEYWORDS = [
    "requirement", "deliverable", "milestone", "output", "component",
    "workplan", "work plan", "methodology", "inception report",
    "final report", "progress report", "workshop report",
    "data collection tool", "sampling plan",
]

_SCOPE_KEYWORDS = [
    "multi-sector", "integrated approach", "cross-cutting",
    "multidisciplinary", "multi-stakeholder", "whole-of-government",
    "systems approach", "multi-component", "multi-country",
    "theory of change", "log frame", "logframe", "results framework",
    "multi-layer", "nested sample", "mixed method",
]


# =============================================================================
# SECTION 2 — Helper: text extraction
# =============================================================================

def _text(tender: Dict[str, Any]) -> str:
    """
    Combine title + description into a single lowercase blob.
    Handles common field-name variants across all scrapers.
    """
    parts = [
        tender.get("title")       or tender.get("Title")       or "",
        tender.get("description") or tender.get("Description") or "",
        tender.get("relevance")   or "",
        tender.get("country")     or tender.get("Country")     or "",
    ]
    return " ".join(str(p) for p in parts if p).lower()


# =============================================================================
# SECTION 3 — Sub-score computation functions
# =============================================================================

def _compute_bid_fit(consulting_type: str) -> int:
    """0–100: how well the engagement type matches our consulting capabilities."""
    return _BID_FIT.get(consulting_type, 30)


def _compute_client_importance(organization: str) -> int:
    """0–100: tier-based importance of the issuing client."""
    if organization in _HIGH_IMPORTANCE_ORGS:
        return 100
    if organization in _MEDIUM_IMPORTANCE_ORGS:
        return 58
    if organization.startswith("Ministry of") or organization == "Government Agency":
        return 38
    return 15   # Unknown org — significant penalty for spread


def _compute_priority(
    tender: Dict[str, Any],
    relevance_score: int,
    consulting_type: str,
    deadline_category: str,
    organization: str,
    sector: str = "unknown",
    region: str = "global",
    title: str = "",
    text: str = "",
    rich_text: str = "",
) -> Dict[str, float]:
    """
    Hybrid relevance scoring (0-100):
      final = ML*50% + PortfolioSimilarity*30% + Keywords*20%

    We keep hard gates and penalties for obvious non-fit and expiry.
    """
    keyword_score = float(max(0, min(100, int(relevance_score))))
    ml_score = _score_ml(tender)
    portfolio_score = _score_portfolio_similarity(tender)
    consulting_confidence = _compute_consulting_confidence(
        consulting_type=consulting_type,
        title=title,
        text=text,
        rich_text=rich_text,
    )
    service_fit = _compute_service_fit(consulting_type, consulting_confidence)
    client_fit = _compute_client_fit(organization, region)
    procurement_penalty = _compute_procurement_penalty(
        title=title,
        text=text,
        rich_text=rich_text,
        consulting_confidence=consulting_confidence,
    )
    semantic_boost = _semantic_rerank_boost(
        query_text=(title + " " + rich_text + " " + text),
        keyword_score=keyword_score,
        consulting_confidence=consulting_confidence,
    )

    raw = (
        ml_score * _W_ML
        + portfolio_score * _W_PORTFOLIO
        + keyword_score * _W_KEYWORDS
    )

    if deadline_category == "expired":
        raw = 0.0

    if consulting_type in ("unknown", ""):
        raw -= 4.0

    # Preserve strong non-fit suppression from firm profile.
    avoid_sectors = set(_FIRM_PROFILE.get("avoid_sectors", []))
    avoid_keywords = [kw.lower() for kw in _FIRM_PROFILE.get("avoid_keywords", [])]
    title_lower = title.lower()
    if sector in avoid_sectors:
        raw -= 12.0
    if any(kw in title_lower for kw in avoid_keywords):
        raw -= 18.0

    # ── Boosts ────────────────────────────────────────────────────────────────
    # +10 preferred donor/multilateral client
    _PREFERRED_ORGS = {
        "World Bank", "UNDP", "ADB", "AfDB", "GIZ",
        "European Union", "UNICEF", "FAO", "IFAD", "WHO",
    }
    if organization in _PREFERRED_ORGS:
        raw += 10.0

    # +5 high consulting confidence (strong evidence this is real consulting work)
    if consulting_confidence >= 70:
        raw += 5.0

    # +5 high service fit (engagement type well-matched to firm capability)
    if service_fit >= 70:
        raw += 5.0

    # ── Penalties ─────────────────────────────────────────────────────────────
    # -15 strong procurement language
    if procurement_penalty >= 15:
        raw -= 15.0

    # -10 individual consultant / IC role (firm cannot bid)
    _combined_text = (title + " " + text + " " + rich_text)
    if any(re.search(pat, _combined_text, flags=re.IGNORECASE) for pat in _IC_ROLE_PATTERNS):
        raw -= 10.0

    # ── Precision signals (reduced weight, no longer the primary driver) ──────
    raw += (service_fit - 50) * 0.06
    raw += (client_fit - 50) * 0.06
    raw += (consulting_confidence - 50) * 0.06
    raw += float(semantic_boost)

    # ── CAPSTAT light boost (max +12 total) ───────────────────────────────────
    # service: 0/3/5 → up to +5
    # sector:  0/2/4 → up to +4
    # client:  0/3/8 → scaled to up to +5
    _cap_svc = _capstat_service_match(title, text, rich_text)
    _cap_sec = _capstat_sector_match(title, text, rich_text, sector)
    _cap_cli = _capstat_client_match(organization, text)
    _cap_boost = min(12.0, float(_cap_svc + _cap_sec + (_cap_cli / 8.0) * 5.0))
    raw += _cap_boost

    # ── Urgency modulation ────────────────────────────────────────────────────
    urgency = _URGENCY_SCORE.get(deadline_category, 35)
    raw += (urgency - 50) * 0.08

    # ── Normalize to 0–100 ────────────────────────────────────────────────────
    # Shift and scale so that a strong-signal tender (ml~92, keyword~80,
    # preferred org, high confidence) reaches 85–95, mid tenders 50–70,
    # and weak tenders stay below 40.
    # Calibration: typical raw for top tender ≈ 46+24+16+10+5+5+3+3+4 = ~116
    # Divide by 1.2 to compress into 0–100 range with headroom at top.
    _CALIBRATION = 1.20
    raw = raw / _CALIBRATION

    final = max(0.0, min(100.0, raw))
    return {
        "priority_score": float(round(final, 1)),
        "ml_relevance_score": float(round(max(0.0, min(100.0, ml_score)), 1)),
        "portfolio_similarity_score": float(
            round(max(0.0, min(100.0, portfolio_score)), 1)
        ),
        "keyword_score": float(round(keyword_score, 1)),
        "client_fit_score": float(round(client_fit, 1)),
        "service_fit_score": float(round(service_fit, 1)),
        "consulting_confidence_score": float(round(consulting_confidence, 1)),
        "procurement_penalty_score": float(round(procurement_penalty, 1)),
        "semantic_rerank_score": float(round(semantic_boost, 2)),
        # CAPSTAT-derived sub-scores (for note building; not stored as columns)
        "capstat_service_match_score": _cap_svc,
        "capstat_sector_match_score":  _cap_sec,
        "capstat_client_match_score":  _cap_cli,
    }


def _compute_competition(organization: str, text: str) -> str:
    """
    Return competition level: 'low' | 'medium' | 'high'.
    Organisation tier takes precedence over text signals.
    """
    if organization in _HIGH_COMPETITION_ORGS:
        return "high"
    if organization in _MEDIUM_COMPETITION_ORGS:
        return "medium"

    # Text-based fallback
    if any(sig in text for sig in _HIGH_COMP_SIGNALS):
        return "high"
    if any(sig in text for sig in _LOW_COMP_SIGNALS):
        return "low"

    # Niche / unknown orgs → low competition
    return "low"


def _compute_opportunity_size(text: str) -> str:
    """
    Return 'small' | 'medium' | 'large' using keyword heuristics.
    Large keywords override small keywords when both appear.
    """
    large_hits = sum(1 for kw in _LARGE_SIZE_KEYWORDS if kw in text)
    small_hits = sum(1 for kw in _SMALL_SIZE_KEYWORDS if kw in text)

    if large_hits > small_hits and large_hits > 0:
        return "large"
    if small_hits > 0 and large_hits == 0:
        return "small"
    return "medium"


def _compute_complexity(text: str) -> int:
    """
    Complexity score 0–100 built from three components:

    Component A — Document length  (0–40 pts)
        Longer briefs indicate more complex procurement requirements.
        1 point per 100 chars of combined title+description, capped at 40.

    Component B — Requirements density  (0–30 pts)
        Each occurrence of a deliverable/milestone keyword = 3 pts, cap 30.

    Component C — Multi-stakeholder / scope keywords  (0–30 pts)
        Each occurrence of a cross-cutting scope keyword = 5 pts, cap 30.
    """
    # A: length
    length_score = min(40, len(text) // 100)

    # B: requirements density (deduplicated hits)
    req_hits  = sum(1 for kw in _REQ_KEYWORDS if kw in text)
    req_score = min(30, req_hits * 3)

    # C: scope / multi-stakeholder keywords
    scope_hits  = sum(1 for kw in _SCOPE_KEYWORDS if kw in text)
    scope_score = min(30, scope_hits * 5)

    return min(100, length_score + req_score + scope_score)


# =============================================================================
# SECTION 4 — Public scoring API
# =============================================================================

def score_one(
    tender: Dict[str, Any],
    intel: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Compute all four opportunity scores for a single tender.

    Parameters
    ----------
    tender : raw tender dict from any scraper
    intel  : pre-fetched row from tender_structured_intel (optional).
             When provided avoids re-running the heavy enrich_one() call.
             Expected keys: relevance_score, consulting_type,
                            organization, deadline_category.

    Returns
    -------
    dict with keys:
        tender_id, priority_score, competition_level,
        opportunity_size, complexity_score
    """
    # ── Resolve tender_id ─────────────────────────────────────────────────
    tender_id = str(
        tender.get("tender_id")
        or tender.get("id")
        or tender.get("sol_num")
        or tender.get("Bid Number")
        or ""
    )[:255]

    # ── Get structured attributes (from cache or fresh enrichment) ────────
    if intel is not None:
        relevance_score  = int(intel.get("relevance_score",  0))
        consulting_type  = str(intel.get("consulting_type",  "unknown"))
        organization     = str(intel.get("organization",     "unknown"))
        deadline_cat     = str(intel.get("deadline_category","unknown"))
        sector           = str(intel.get("sector",           "unknown"))
        region           = str(intel.get("region",           "global"))
        deep_scope       = str(intel.get("deep_scope", "") or "")
        deep_eval        = str(intel.get("deep_eval_criteria", "") or "")
        ai_summary       = str(intel.get("ai_summary", "") or "")
    else:
        # Fall back to inline enrichment (slower path)
        try:
            from intelligence.tender_intelligence import enrich_one
            attrs = enrich_one(tender)
        except Exception:
            attrs = {
                "relevance_score": 0, "consulting_type": "unknown",
                "organization": "unknown", "deadline_category": "unknown",
                "sector": "unknown", "region": "global",
            }
        relevance_score  = int(attrs.get("relevance_score",  0))
        consulting_type  = str(attrs.get("consulting_type",  "unknown"))
        organization     = str(attrs.get("organization",     "unknown"))
        deadline_cat     = str(attrs.get("deadline_category","unknown"))
        sector           = str(attrs.get("sector",           "unknown"))
        region           = str(attrs.get("region",           "global"))
        deep_scope       = ""
        deep_eval        = ""
        ai_summary       = ""

    title       = str(tender.get("title") or tender.get("Title") or "")
    description = str(tender.get("description") or tender.get("Description") or "")
    text        = _text(tender)
    rich_text   = " ".join([description, deep_scope, deep_eval, ai_summary]).strip()

    tender_for_models = dict(tender)
    # Use already-available rich content where present; do not trigger new crawling.
    if rich_text:
        tender_for_models["description"] = rich_text[:5000]
    if organization and organization != "unknown":
        tender_for_models["organization"] = organization
    if sector and sector != "unknown":
        tender_for_models["sector"] = sector
    if region and region != "global":
        tender_for_models["country"] = region

    # ── Hard consulting relevance gate ────────────────────────────────────────
    # Applied before ML scoring so excluded tenders do not waste compute and
    # their priority_score is forced to 0 regardless of what the model returns.
    is_consulting, exclusion_reason = _is_consulting_relevant(
        title, description, consulting_type, text
    )

    _priority = _compute_priority(
        tender=tender_for_models,
        relevance_score=relevance_score,
        consulting_type=consulting_type,
        deadline_category=deadline_cat,
        organization=organization,
        sector=sector,
        region=region,
        title=title,
        text=text,
        rich_text=rich_text,
    )

    raw_priority = int(round(_priority["priority_score"]))

    # Force priority to 0 when the hard gate excludes the tender.
    # The row is NOT deleted — it remains in the DB / Excel ranked at the bottom.
    if not is_consulting:
        raw_priority = 0

    # Hard gate: expired deadline — tenders past their deadline cannot be bid on
    if deadline_cat == "expired":
        raw_priority = 0

    # Firm eligibility: detect Individual Consultant / IC roles — firm cannot bid.
    # Hard-cap at 10 (well below _MIN_PRIORITY=20) rather than a -20 penalty,
    # so that blending cannot resurrect IC-role tenders above the export gate.
    _combined_text = (title + " " + text + " " + rich_text)
    _is_ic_role = any(
        re.search(pat, _combined_text, flags=re.IGNORECASE)
        for pat in _IC_ROLE_PATTERNS
    )
    if _is_ic_role:
        raw_priority = min(raw_priority, 10)
    is_firm_eligible = int(not _is_ic_role)

    # Zero-score detection: both priority and relevance at 0 — no useful data.
    is_low_confidence = int(raw_priority == 0 and relevance_score == 0)

    # 1-line explanation for analysts
    note = _build_scoring_note(
        is_consulting=is_consulting,
        exclusion_reason=exclusion_reason,
        priority_score=raw_priority,
        consulting_type=consulting_type,
        organization=organization,
        sector=sector,
        is_low_confidence=bool(is_low_confidence),
        client_fit_score=int(_priority.get("client_fit_score", 0)),
        service_fit_score=int(_priority.get("service_fit_score", 0)),
        consulting_confidence_score=int(_priority.get("consulting_confidence_score", 0)),
        procurement_penalty_score=int(_priority.get("procurement_penalty_score", 0)),
    )
    if _is_ic_role:
        note = (note.rstrip(". ") + ". IC role — not firm eligible.").lstrip(". ")
    elif not is_consulting:
        note = (note.rstrip(". ") + ". Goods/works — excluded.").lstrip(". ")

    # Append compact CAPSTAT match tags (only when score is non-zero)
    _cap_tags: list = []
    if _priority.get("capstat_service_match_score", 0) >= 5:
        _cap_tags.append("IDCG service match")
    if _priority.get("capstat_sector_match_score", 0) >= 4:
        _cap_tags.append("IDCG sector match")
    if _priority.get("capstat_client_match_score", 0) >= 8:
        _cap_tags.append("Strong donor/client fit")
    elif _priority.get("capstat_client_match_score", 0) >= 3:
        _cap_tags.append("Partner client fit")
    if _cap_tags:
        note = (note.rstrip(". ") + ". " + "; ".join(_cap_tags) + ".").lstrip(". ")

    # ── Shadow ML label model + blended scoring ───────────────────────────────
    # Build a lightweight row dict for the shadow model from already-resolved fields.
    _shadow_row = {
        "title":          title,
        "sector":         sector,
        "service_type":   consulting_type,
        "org":            organization,
        "country":        region,
        "portal":         str(tender.get("portal") or tender.get("Portal") or ""),
        "priority_score": float(raw_priority),
        "relevance_score": float(relevance_score),
        "deep_scope":     rich_text[:500] if rich_text else "",
        "ai_summary":     ai_summary[:300] if ai_summary else "",
    }

    # Shadow ML score: 0–100  (fallback = 50 / neutral if model not ready)
    _shadow_score = float(_predict_shadow_score(_shadow_row))

    # CAPSTAT fit signal: normalized 0–100, separate from the boost already in raw_priority
    # Max raw CAPSTAT: service(5) + sector(4) + client(8) = 17
    _cap_raw = (
        _priority.get("capstat_service_match_score", 0) +
        _priority.get("capstat_sector_match_score", 0) +
        _priority.get("capstat_client_match_score", 0)
    )
    _capstat_fit = min(100.0, float(_cap_raw) / 17.0 * 100.0)

    # Blended priority score:
    #   45% existing engine  +  35% shadow ML  +  20% CAPSTAT fit
    # Hard-gated tenders (goods/works or expired deadline) always stay at 0.
    # Do not resurrect zero-scored tenders via blending of shadow/capstat signals.
    if raw_priority == 0:
        blended_priority = 0
    else:
        blended_priority = int(round(
            _BLEND_WEIGHT_ENGINE  * float(raw_priority) +
            _BLEND_WEIGHT_SHADOW  * _shadow_score +
            _BLEND_WEIGHT_CAPSTAT * _capstat_fit
        ))
        blended_priority = max(0, min(100, blended_priority))

    # IC / non-firm roles: hard-cap blended priority below the export gate (20).
    # The raw_priority cap at 10 reduces the engine signal, but blending with the
    # neutral shadow score (50) can still push the final blend above the gate.
    # This post-blend cap ensures IC tenders never appear in the master workbook.
    if not is_firm_eligible:
        blended_priority = min(blended_priority, 15)

    # Re-check low-confidence against blended score
    is_low_confidence = int(blended_priority == 0 and relevance_score == 0)

    # Append shadow ML note fragment (explainability)
    _shadow_note = _get_shadow_note(_shadow_row)
    if _shadow_note:
        note = (note.rstrip(". ") + ". " + _shadow_note + ".").lstrip(". ")

    return {
        "tender_id":                  tender_id,
        "priority_score":             blended_priority,
        "ml_relevance_score":         _priority["ml_relevance_score"],
        "portfolio_similarity_score": _priority["portfolio_similarity_score"],
        "keyword_score":              _priority["keyword_score"],
        "competition_level":          _compute_competition(organization, text),
        "opportunity_size":           _compute_opportunity_size(text),
        "complexity_score":           _compute_complexity(text),
        # New gating / explainability fields
        "is_consulting_relevant":     int(is_consulting),
        "is_firm_eligible":           is_firm_eligible,
        "is_low_confidence":          is_low_confidence,
        "scoring_note":               note,
        "client_fit_score":           int(round(_priority.get("client_fit_score", 0))),
        "service_fit_score":          int(round(_priority.get("service_fit_score", 0))),
        "consulting_confidence_score": int(round(_priority.get("consulting_confidence_score", 0))),
        "procurement_penalty_score":  int(round(_priority.get("procurement_penalty_score", 0))),
        "semantic_rerank_score":      float(_priority.get("semantic_rerank_score", 0.0)),
    }


def score_batch(
    tenders: List[Dict[str, Any]],
    intel_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    Score a list of tender dicts.
    Skips entries with no resolvable tender_id.

    Parameters
    ----------
    tenders     : raw tender dicts
    intel_by_id : {tender_id: intel_row} pre-fetched from DB (optional)

    Returns
    -------
    list of scored dicts in input order (entries with empty tender_id dropped)
    """
    if intel_by_id is None:
        intel_by_id = {}

    scored = []
    for t in tenders:
        try:
            tid   = str(
                t.get("tender_id") or t.get("id")
                or t.get("sol_num") or t.get("Bid Number") or ""
            )[:255]
            intel = intel_by_id.get(tid)
            s     = score_one(t, intel)
            if s["tender_id"]:
                scored.append(s)
        except Exception as exc:
            logger.debug(f"[opportunity_engine] score_one skipped: {exc}")

    logger.info(
        f"[opportunity_engine] Scored {len(scored)}/{len(tenders)} tenders"
    )
    return scored


# =============================================================================
# SECTION 5 — Database layer
# =============================================================================

_TABLE = "tender_structured_intel"

_NEW_COLUMNS = [
    ("priority_score",             "SMALLINT      NOT NULL DEFAULT 0"),
    ("ml_relevance_score",          "DECIMAL(5,2)  NOT NULL DEFAULT 0"),
    ("portfolio_similarity_score",  "DECIMAL(5,2)  NOT NULL DEFAULT 0"),
    ("keyword_score",               "DECIMAL(5,2)  NOT NULL DEFAULT 0"),
    ("competition_level",           "VARCHAR(20)   NOT NULL DEFAULT 'medium'"),
    ("opportunity_size",            "VARCHAR(20)   NOT NULL DEFAULT 'medium'"),
    ("complexity_score",            "SMALLINT      NOT NULL DEFAULT 0"),
    # ─ added by gating / explainability layer ──────────────────────────────
    ("is_consulting_relevant",      "TINYINT(1)    NOT NULL DEFAULT 1"),
    ("is_firm_eligible",            "TINYINT(1)    NOT NULL DEFAULT 1"),
    ("is_low_confidence",           "TINYINT(1)    NOT NULL DEFAULT 0"),
    ("scoring_note",                "VARCHAR(400)  NOT NULL DEFAULT ''"),
    ("client_fit_score",            "SMALLINT      NOT NULL DEFAULT 0"),
    ("service_fit_score",           "SMALLINT      NOT NULL DEFAULT 0"),
    ("consulting_confidence_score", "SMALLINT      NOT NULL DEFAULT 0"),
    ("procurement_penalty_score",   "SMALLINT      NOT NULL DEFAULT 0"),
    ("semantic_rerank_score",       "DECIMAL(5,2)  NOT NULL DEFAULT 0"),
]


def extend_schema() -> None:
    """
    Add the four opportunity columns to tender_structured_intel if they
    do not already exist.  Safe to call on every run; non-fatal on error.
    """
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor()

        for col_name, col_def in _NEW_COLUMNS:
            # Check via information_schema (MySQL-compatible, no DDL exceptions)
            cur.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME   = %s
                  AND COLUMN_NAME  = %s
                """,
                (_TABLE, col_name),
            )
            (count,) = cur.fetchone()
            if count == 0:
                cur.execute(
                    f"ALTER TABLE `{_TABLE}` "
                    f"ADD COLUMN `{col_name}` {col_def}"
                )
                logger.info(
                    f"[opportunity_engine] Column '{col_name}' added to {_TABLE}"
                )

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"[opportunity_engine] Schema ready for '{_TABLE}'")
    except Exception as exc:
        logger.warning(
            f"[opportunity_engine] extend_schema failed (non-fatal): {exc}"
        )


def _fetch_intel(
    tenders: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Query tender_structured_intel for all tender_ids in `tenders`.
    Returns a {tender_id → row_dict} map.
    Returns empty dict on any DB error (graceful degradation).
    """
    ids: List[str] = []
    for t in tenders:
        tid = str(
            t.get("tender_id") or t.get("id")
            or t.get("sol_num") or t.get("Bid Number") or ""
        )[:255]
        if tid:
            ids.append(tid)

    if not ids:
        return {}

    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        placeholders = ", ".join(["%s"] * len(ids))
        cur.execute(
            f"SELECT tender_id, sector, consulting_type, organization, "
            f"deadline_category, relevance_score, region, "
            f"deep_scope, deep_eval_criteria, ai_summary "
            f"FROM `{_TABLE}` "
            f"WHERE tender_id IN ({placeholders})",
            ids,
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()

        logger.debug(
            f"[opportunity_engine] Fetched {len(rows)}/{len(ids)} intel rows from DB"
        )
        return {r["tender_id"]: r for r in rows}

    except Exception as exc:
        logger.debug(
            f"[opportunity_engine] _fetch_intel failed (will enrich inline): {exc}"
        )
        return {}


def store_scores(scored: List[Dict[str, Any]]) -> int:
    """
    UPDATE tender_structured_intel rows with the four opportunity scores.
    Uses executemany for performance.  Only updates rows that already exist
    (created by tender_intelligence.py).

    Returns the number of rows actually updated (rowcount).
    Returns 0 on any DB error.
    """
    if not scored:
        return 0

    try:
        from database.db import get_connection, DRY_RUN
        if DRY_RUN:
            logger.info(
                f"[opportunity_engine] DRY-RUN: skipping {len(scored)} DB writes"
            )
            return 0

        conn = get_connection()
        cur  = conn.cursor()

        sql = f"""
            UPDATE `{_TABLE}`
            SET priority_score             = %s,
                ml_relevance_score         = %s,
                portfolio_similarity_score = %s,
                keyword_score              = %s,
                competition_level          = %s,
                opportunity_size           = %s,
                complexity_score           = %s,
                is_consulting_relevant     = %s,
                is_firm_eligible           = %s,
                is_low_confidence          = %s,
                scoring_note               = %s,
                client_fit_score           = %s,
                service_fit_score          = %s,
                consulting_confidence_score= %s,
                procurement_penalty_score  = %s,
                semantic_rerank_score      = %s
            WHERE tender_id = %s
        """
        rows = [
            (
                s["priority_score"],
                s.get("ml_relevance_score", 0),
                s.get("portfolio_similarity_score", 0),
                s.get("keyword_score", 0),
                s["competition_level"],
                s["opportunity_size"],
                s["complexity_score"],
                s.get("is_consulting_relevant", 1),
                s.get("is_firm_eligible", 1),
                s.get("is_low_confidence", 0),
                (s.get("scoring_note") or "")[:400],
                int(s.get("client_fit_score", 0) or 0),
                int(s.get("service_fit_score", 0) or 0),
                int(s.get("consulting_confidence_score", 0) or 0),
                int(s.get("procurement_penalty_score", 0) or 0),
                float(s.get("semantic_rerank_score", 0.0) or 0.0),
                s["tender_id"],
            )
            for s in scored
        ]
        cur.executemany(sql, rows)
        conn.commit()
        written = cur.rowcount
        cur.close()
        conn.close()

        logger.info(
            f"[opportunity_engine] Updated {written}/{len(scored)} rows in '{_TABLE}'"
        )
        return written

    except Exception as exc:
        logger.warning(
            f"[opportunity_engine] store_scores failed (non-fatal): {exc}"
        )
        return 0


def score_and_store_batch(tenders: List[Dict[str, Any]]) -> int:
    """
    One-call convenience: extend schema → fetch intel → score → store.

    Pipeline integration point (called from main.py after
    tender_intelligence.enrich_and_store_batch).

    Returns count of rows updated.  Guarantees non-fatal execution.
    """
    try:
        extend_schema()
        intel_by_id = _fetch_intel(tenders)
        scored      = score_batch(tenders, intel_by_id)
        return store_scores(scored)
    except Exception as exc:
        logger.warning(
            f"[opportunity_engine] score_and_store_batch failed (non-fatal): {exc}"
        )
        return 0


# =============================================================================
# SECTION 6 — Backfill utility
# =============================================================================

def backfill_from_intel_table(limit: int = 10_000) -> int:
    """
    Score all rows in tender_structured_intel that have no priority_score
    yet (i.e., priority_score = 0 AND competition_level = 'medium').

    NOTE: We can't join back to seen_tenders for the full raw tender text,
    so the raw text available is just what's stored in intel (title from
    tender_id pattern + org/sector hints).  This is best-effort backfill.

    Usage:
        python3 intelligence/opportunity_engine.py --backfill
    """
    try:
        extend_schema()
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        # Fetch rows that haven't been scored yet
        cur.execute(
            f"""
            SELECT si.tender_id, si.consulting_type, si.organization,
                   si.deadline_category, si.relevance_score,
                   si.sector, si.region,
                   st.title, st.url
            FROM `{_TABLE}` si
            LEFT JOIN seen_tenders st USING (tender_id)
            WHERE si.priority_score = 0
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()

        logger.info(
            f"[opportunity_engine] Backfilling {len(rows)} un-scored tenders…"
        )
        if not rows:
            return 0

        # Build minimal "tender" dicts (we have intel inline)
        intel_by_id: Dict[str, Dict] = {}
        tender_dicts: List[Dict] = []
        for r in rows:
            tid = r["tender_id"]
            intel_by_id[tid] = {
                "consulting_type":   r["consulting_type"],
                "organization":      r["organization"],
                "deadline_category": r["deadline_category"],
                "relevance_score":   r["relevance_score"],
                "sector":            r.get("sector", "unknown"),
                "region":            r.get("region", "global"),
            }
            tender_dicts.append({
                "tender_id":   tid,
                "title":       r.get("title") or tid,
                "description": "",
            })

        scored = score_batch(tender_dicts, intel_by_id)
        return store_scores(scored)

    except Exception as exc:
        logger.warning(
            f"[opportunity_engine] backfill failed: {exc}"
        )
        return 0


def rescore_all(batch_size: int = 5_000) -> int:
    """
    Re-score ALL tenders in tender_structured_intel regardless of their current
    priority_score value.

    Use this after fixing scoring bugs (e.g. the deadline_category key mismatch
    that caused urgency to always default to 35).  Safe to run at any time —
    just overwrites existing scores with freshly computed ones.

    Usage:
        python3 -c "from intelligence.opportunity_engine import rescore_all; rescore_all()"

    Returns total rows updated.
    """
    try:
        extend_schema()
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        total = 0
        offset = 0

        while True:
            cur.execute(
                f"""
                SELECT si.tender_id, si.consulting_type, si.organization,
                       si.deadline_category, si.relevance_score,
                       si.sector, si.region,
                       st.title, st.url
                FROM `{_TABLE}` si
                LEFT JOIN seen_tenders st USING (tender_id)
                ORDER BY si.id ASC
                LIMIT %s OFFSET %s
                """,
                (batch_size, offset),
            )
            rows = cur.fetchall() or []
            if not rows:
                break

            logger.info(
                "[opportunity_engine] rescore_all: re-scoring batch offset=%d size=%d…",
                offset,
                len(rows),
            )
            intel_by_id: Dict[str, Dict] = {}
            tender_dicts: List[Dict] = []
            for r in rows:
                tid = r["tender_id"]
                intel_by_id[tid] = {
                    "consulting_type":   r["consulting_type"],
                    "organization":      r["organization"],
                    "deadline_category": r["deadline_category"],
                    "relevance_score":   r["relevance_score"],
                    "sector":            r.get("sector", "unknown"),
                    "region":            r.get("region", "global"),
                }
                tender_dicts.append({
                    "tender_id":   tid,
                    "title":       r.get("title") or tid,
                    "description": "",
                })

            scored = score_batch(tender_dicts, intel_by_id)
            total += store_scores(scored)
            offset += len(rows)

        cur.close()
        conn.close()
        logger.info("[opportunity_engine] rescore_all: %d rows updated.", total)
        return total

    except Exception as exc:
        logger.warning("[opportunity_engine] rescore_all failed: %s", exc)
        return 0


# =============================================================================
# SECTION 7 — CLI test / backfill entry point
# =============================================================================

# Sample tenders shared with tender_intelligence.py CLI
_SAMPLE_TENDERS = [
    {
        "tender_id":   "TEST_WB_001",
        "title":       "Baseline Survey and Impact Evaluation of WASH Programme in Bihar",
        "description": "UNICEF India seeks a consulting firm to conduct a baseline survey "
                       "and impact evaluation of its water, sanitation and hygiene (WASH) "
                       "programme across 10 districts of Bihar. Includes KAP survey, "
                       "data collection and report preparation. The assignment spans 12 months "
                       "and requires delivery of an inception report, data collection tools, "
                       "a final evaluation report and a dissemination workshop.",
        "deadline":    (datetime.utcnow() + timedelta(days=18)).strftime("%Y-%m-%d"),
        "country":     "India",
    },
    {
        "tender_id":   "TEST_AFDB_001",
        "title":       "Capacity Building and Training for Climate Adaptation in Kenya",
        "description": "African Development Bank — Technical Assistance for capacity "
                       "development of county-level environment officers in climate "
                       "change adaptation and natural resource management. Multi-phase "
                       "programme with national roll-out across 47 counties. Consortium "
                       "of firms preferred. Inception report and progress milestones required.",
        "deadline":    (datetime.utcnow() + timedelta(days=12)).strftime("%Y-%m-%d"),
        "country":     "Kenya",
    },
    {
        "tender_id":   "TEST_USAID_001",
        "title":       "Request for Proposal: Policy Advisory Services for Health Governance",
        "description": "USAID seeks advisory services to support policy development "
                       "and regulatory reform for the national health governance framework "
                       "in Bangladesh. Includes stakeholder consultations and policy briefs.",
        "deadline":    (datetime.utcnow() + timedelta(days=4)).strftime("%Y-%m-%d"),
        "country":     "Bangladesh",
    },
    {
        "tender_id":   "TEST_GEM_001",
        "title":       "Supply of Laboratory Equipment",
        "description": "Procurement of laboratory instruments and equipment for CSIR. "
                       "Single district pilot. One-time purchase order. Individual vendor.",
        "deadline":    (datetime.utcnow() - timedelta(days=5)).strftime("%Y-%m-%d"),
        "country":     "India",
    },
]


def _print_banner(text: str) -> None:
    print("\n" + "─" * 72)
    print(f"  {text}")
    print("─" * 72)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="TenderRadar — Opportunity Intelligence Engine CLI"
    )
    ap.add_argument(
        "--backfill",
        action="store_true",
        help="Score all un-scored rows in tender_structured_intel",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=10_000,
        help="Max rows to backfill (default: 10000)",
    )
    args = ap.parse_args()

    # ── Configure console logger ───────────────────────────────────────────
    logging.basicConfig(
        format="%(levelname)s  %(name)s — %(message)s",
        level=logging.INFO,
    )

    # ── Backfill mode ──────────────────────────────────────────────────────
    if args.backfill:
        _print_banner("Backfilling opportunity scores from tender_structured_intel…")
        extend_schema()
        n = backfill_from_intel_table(limit=args.limit)
        print(f"\n✅  Backfill complete — {n} rows updated in '{_TABLE}'")
        sys.exit(0)

    # ── Sample test ────────────────────────────────────────────────────────
    _print_banner("Opportunity Intelligence Engine — sample scoring test")

    # Step 1: enrichment (needed to populate tender_structured_intel first)
    _print_banner("Step 1 — Structured enrichment (tender_intelligence)")
    try:
        from intelligence.tender_intelligence import enrich_one
        _intel_map: Dict[str, Dict] = {}
        for t in _SAMPLE_TENDERS:
            attrs = enrich_one(t)
            _intel_map[t["tender_id"]] = attrs
        print(f"  ✅  Enriched {len(_SAMPLE_TENDERS)} sample tenders in-memory")
    except Exception as _e:
        print(f"  ⚠   tender_intelligence import failed — falling back to inline: {_e}")
        _intel_map = {}

    # Step 2: compute opportunity scores
    _print_banner("Step 2 — Opportunity scoring")

    hdr = (
        f"{'TENDER':<44} "
        f"{'PRIORITY':>8} "
        f"{'COMPETITION':<11} "
        f"{'SIZE':<8} "
        f"{'COMPLEXITY':>10}"
    )
    print()
    print(hdr)
    print("─" * len(hdr))

    _all_scored = []
    for t in _SAMPLE_TENDERS:
        intel = _intel_map.get(t["tender_id"])
        s     = score_one(t, intel)
        _all_scored.append(s)

        title_s = t["title"][:43].ljust(44)
        print(
            f"{title_s} "
            f"{s['priority_score']:>8} "
            f"{s['competition_level']:<11} "
            f"{s['opportunity_size']:<8} "
            f"{s['complexity_score']:>10}"
        )

    # Step 3: detailed dump for first sample
    _print_banner(f"Detailed scores — '{_SAMPLE_TENDERS[0]['title'][:55]}…'")
    s0 = _all_scored[0]
    for k, v in s0.items():
        print(f"  {k:<22}: {v}")

    # Step 4: schema extension + DB write test
    _print_banner("Step 3 — DB schema extension + write test")
    try:
        extend_schema()
        print("  ✅  extend_schema() OK — 4 columns present in tender_structured_intel")

        # First write structured intel so the UPDATE has rows to hit
        from intelligence.tender_intelligence import (
            init_schema as _ti_init, store_batch as _ti_store,
        )
        _ti_init()
        enriched_for_db = [_intel_map[t["tender_id"]] for t in _SAMPLE_TENDERS
                           if t["tender_id"] in _intel_map]
        if enriched_for_db:
            _ti_store(enriched_for_db)

        written = store_scores(_all_scored)
        print(f"  ✅  store_scores() OK — {written} row(s) updated in '{_TABLE}'")
    except Exception as _e:
        print(f"  ⚠   DB test skipped (not required for local testing): {_e}")

    print()
