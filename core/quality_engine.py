# =============================================================================
# core/quality_engine.py — Shared Quality Intelligence Engine
#
# Single source of truth for ALL portal scrapers. No portal file should
# contain quality scoring, signal detection, or filtering logic.
#
# Public API:
#   compute_quality_score(row)              → int  raw completeness 0-100
#   compute_consulting_confidence(row)      → float 0.0-1.0
#   classify_decision_tier(score, conf)     → str  BID_NOW|STRONG_CONSIDER|…
#   passes_quality_filter(row, score, thr)  → (bool, str)
#   detect_consulting_signals(row)          → SignalResult dict
#   apply_intelligence_filter(rows, thr)    → (accepted, rejected, reasons)
#   TenderResult                            → TypedDict for standard output
#
# SCORING MODEL (v2)
# ──────────────────
#   raw_quality_score  = data completeness  (0-100)
#                        richness + keywords + method + amount + deadline
#   consulting_confidence = how likely this is a consulting opportunity
#                        (0.0-1.0, based on notice type + keyword density)
#   quality_score      = round(raw_score × confidence), capped at
#                        GOODS_SCORE_CAP for goods-type notices
#
# DECISION TIERS (v1)
# ────────────────────
#   BID_NOW        : quality_score ≥ 75  AND  consulting_confidence ≥ 0.70
#   STRONG_CONSIDER: quality_score 60–74
#   WEAK_CONSIDER  : quality_score 40–59
#   IGNORE         : quality_score < 40  OR   consulting_confidence < 0.40
#
# This ensures:
#   consulting + weak data  → moderate final score (still passes)
#   goods + strong agency   → low final score     (suppressed)
#   consulting + strong data → high final score   (prioritised → BID_NOW)
#
# Portals only call apply_intelligence_filter() — everything else is internal.
# =============================================================================

from __future__ import annotations

import json
import os
import re
from typing import TypedDict


# =============================================================================
# SECTION 1 — Standard Output Format
# =============================================================================

class TenderResult(TypedDict):
    title:            str
    url:              str
    deadline:         str
    organization:     str
    sector:           str
    consulting_type:  str
    quality_score:    int
    source:           str
    decision_tag:     str   # BID_NOW | STRONG_CONSIDER | WEAK_CONSIDER | IGNORE


def make_tender_result(
    title:           str = "",
    url:             str = "",
    deadline:        str = "",
    organization:    str = "",
    sector:          str = "",
    consulting_type: str = "",
    quality_score:   int = 0,
    source:          str = "",
    decision_tag:    str = "",
    **_extra,
) -> TenderResult:
    return TenderResult(
        title           = str(title)[:200].strip(),
        url             = str(url).strip(),
        deadline        = str(deadline).strip(),
        organization    = str(organization).strip(),
        sector          = str(sector).strip(),
        consulting_type = str(consulting_type).strip(),
        quality_score   = int(quality_score or 0),
        source          = str(source).strip(),
        decision_tag    = str(decision_tag).strip(),
    )


# =============================================================================
# SECTION 2 — Vocabulary
# =============================================================================

# ── Thresholds ────────────────────────────────────────────────────────────────
QUALITY_THRESHOLD: int = 20   # minimum quality_score to pass the intelligence filter
GOODS_SCORE_CAP:   int = 30   # hard ceiling for goods-type tenders (ITB / RFQ)

# ── Decision tier thresholds (base values — may be overridden by calibration) ─
# Tiers convert (quality_score, consulting_confidence) into an actionable label.
# Used in Excel output, Telegram grouping, and DB storage (tender_structured_intel).
TIER_BID_NOW_SCORE:      int   = 75   # score floor for BID_NOW
TIER_BID_NOW_CONFIDENCE: float = 0.70 # confidence floor for BID_NOW
TIER_STRONG_MIN:         int   = 60   # STRONG_CONSIDER lower bound
TIER_WEAK_MIN:           int   = 40   # WEAK_CONSIDER lower bound (= quality filter +20)
TIER_IGNORE_CONFIDENCE:  float = 0.40 # confidence below this → IGNORE regardless of score

# Human-readable emoji labels (used in Telegram messages)
TIER_LABELS: dict[str, str] = {
    "BID_NOW":         "🔥 BID NOW",
    "STRONG_CONSIDER": "⭐ STRONG CONSIDER",
    "WEAK_CONSIDER":   "📌 WEAK CONSIDER",
    "IGNORE":          "🔇 IGNORE",
}

# ── Adaptive threshold override (written by decision_calibrator.py) ────────────
# If calibration_config.json exists alongside the project root, load it and
# override the threshold constants so applied tuning takes effect immediately
# without any code change.
def _load_calibration_overrides() -> None:
    """
    Read calibration_config.json (one level up from this file's directory)
    and override tier threshold globals if present.

    Called once at module import.  Silent on any error — base values used.
    """
    global TIER_BID_NOW_SCORE, TIER_BID_NOW_CONFIDENCE
    global TIER_STRONG_MIN, TIER_WEAK_MIN

    config_path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "calibration_config.json")
    )
    if not os.path.exists(config_path):
        return
    try:
        with open(config_path) as fh:
            cfg = json.load(fh)
        thresholds = cfg.get("thresholds", {})
        if "TIER_BID_NOW_SCORE" in thresholds:
            TIER_BID_NOW_SCORE      = int(thresholds["TIER_BID_NOW_SCORE"])
        if "TIER_BID_NOW_CONFIDENCE" in thresholds:
            TIER_BID_NOW_CONFIDENCE = float(thresholds["TIER_BID_NOW_CONFIDENCE"])
        if "TIER_STRONG_MIN" in thresholds:
            TIER_STRONG_MIN         = int(thresholds["TIER_STRONG_MIN"])
        if "TIER_WEAK_MIN" in thresholds:
            TIER_WEAK_MIN           = int(thresholds["TIER_WEAK_MIN"])
        import logging as _logging
        _logging.getLogger("tenderradar.quality_engine").info(
            "[quality_engine] Calibration overrides loaded from %s: %s",
            config_path, thresholds,
        )
    except Exception:
        pass   # any parse/IO error → keep base values


_load_calibration_overrides()

# ── Consulting whitelist (any match passes the keyword gate in the filter) ────
CONSULTING_WHITELIST: list[str] = [
    "consulting", "consultant", "technical assistance", "evaluation",
    "advisory", "capacity building", "assessment", "review", "audit",
    "research", "training", "analysis", "study", "survey", "feasibility",
    "diagnostic", "mapping", "appraisal", "policy",
]

# ── Generic stop-words (rows consisting ONLY of these are rejected) ───────────
GENERIC_STOP_WORDS: frozenset[str] = frozenset({
    "project", "support", "services", "india", "program", "programme",
    "activities", "component", "work", "task", "assignment", "contract",
    "staff", "team", "unit", "centre", "cell", "office", "department",
    "tbd", "n/a", "na", "nil", "none", "pending", "-", "various",
    "general", "miscellaneous", "other",
})

# ── Notice type classification ─────────────────────────────────────────────────
# Used to apply GOODS_SCORE_CAP and to seed consulting_confidence

_GOODS_NOTICE_TYPES: frozenset[str] = frozenset({
    "itb", "invitation to bid",
    "rfq", "request for quotation",
    "ltpo", "pca", "goods", "supply",
})

_CONSULTING_NOTICE_TYPES: frozenset[str] = frozenset({
    "rfp", "request for proposal", "request for proposals",
    "eoi", "expression of interest",
    "ic", "individual contractor", "individual consultant",
    "ictb", "rfi", "request for information", "itp",
    # World Bank procurement methods
    "qcbs", "cqs", "qbs", "lcs", "ssss",
})

# ── Confidence lookup by notice type / method ─────────────────────────────────
# Exact short-code → confidence base value
_CONFIDENCE_EXACT: dict[str, float] = {
    # WB procurement methods (certain consulting)
    "qcbs": 0.95, "cqs": 0.93, "qbs": 0.91, "lcs": 0.90, "ssss": 0.90,
    # Individual consultant / contractor
    "ic":   0.85,
    # UNGM / HTML portal consulting notice types
    "rfp":  0.75,
    "ictb": 0.80,
    "eoi":  0.70,
    "itp":  0.65,
    "rfi":  0.60,
    # Ambiguous
    "lta":  0.45,
    "direct": 0.55,
    "sole":   0.55,
    # Goods notice types → low consulting probability
    "itb":  0.15,
    "rfq":  0.10,
    "ltpo": 0.05,
    "pca":  0.05,
}

# Phrase substring → confidence base value  (checked ONLY for phrases, not short codes)
_CONFIDENCE_PHRASES: dict[str, float] = {
    "individual consultant":   0.85,
    "individual contractor":   0.85,
    "request for proposal":    0.75,
    "expression of interest":  0.70,
    "request for information": 0.60,
    "invitation to bid":       0.15,
    "request for quotation":   0.10,
    "sole source":             0.55,
    "direct selection":        0.60,
}

# ── Method scoring for raw completeness (exact codes first, phrases second) ───
_METHOD_EXACT_SCORES: dict[str, int] = {
    # WB procurement methods
    "qcbs": 20, "cqs": 18, "qbs": 16, "lcs": 15, "ssss": 14,
    "ic":   15,
    # UNGM / HTML portal notice types
    "rfp":  15, "eoi": 12, "ictb": 14, "itp": 12, "rfi": 8,
    # Goods (low — goods cap further constrains these anyway)
    "itb": 5, "lta": 7,
}

_METHOD_PHRASE_SCORES: dict[str, int] = {
    "individual consultant":  15,
    "individual contractor":  14,
    "request for proposal":   15,
    "expression of interest": 12,
    "request for information": 8,
    "invitation to bid":       5,
    "direct selection":       12,
    "sole source":            10,
}

# ── Scoring keyword bank (each unique hit = 5 pts, capped at 25) ──────────────
_SCORE_KWS: list[str] = [
    "consulting", "consultant", "technical assistance", "evaluation",
    "advisory", "capacity building", "qcbs", "cqs", "qbs", "lcs", "ssss",
    "assessment", "review", "audit", "research", "training", "analysis",
    "survey", "procurement plan", "selection", "feasibility", "diagnostic",
]


# =============================================================================
# SECTION 3 — Signal Detection Vocabulary
# =============================================================================

_CTYPE_PATTERNS: list[tuple[str, list[str]]] = [
    ("Evaluation", [
        "evaluat", "assessment", "appraisal", "baseline", "endline",
        "mid-term", "mid term", "impact assessment", "review of",
        "performance review", "ppa", "programme evaluation",
    ]),
    ("Technical Assistance", [
        "technical assistance", "ta for", "ta to",
        "institutional support", "system strengthening",
        "capacity development", "programme support",
    ]),
    ("Capacity Building", [
        "capacity building", "training", "human resource",
        "skill development", "workshop", "coaching",
        "mentoring", "knowledge transfer",
    ]),
    ("Research/Study", [
        "research", "study", "survey", "mapping",
        "analysis of", "feasibility", "diagnostic",
        "situational analysis", "scoping",
    ]),
    ("Audit/Fiduciary", [
        "audit", "financial management", "fiduciary",
        "internal audit", "procurement audit", "social audit",
    ]),
    ("Advisory/Policy", [
        "advisory", "policy advice", "policy reform",
        "strategy", "strategic plan", "reform",
        "regulatory", "legal framework",
    ]),
    ("Individual Consultant", [
        "individual consultant", " ic ", "individual expert",
        "national consultant", "international consultant",
        "senior consultant", "specialist consultant",
        "individual contractor",
    ]),
]

_SECTOR_PATTERNS: list[tuple[str, list[str]]] = [
    ("Education", [
        "education", "school", "learning", "literacy",
        "teaching", "curriculum", "student", "higher education",
    ]),
    ("Health", [
        "health", "medical", "nutrition", "hiv", "malaria",
        "maternal", "nhm", "nhp", "hospital", "primary health",
        "public health", "disease",
    ]),
    ("Water/Sanitation", [
        "water", "sanitation", "wash", "sewage",
        "irrigation", "dam", "drainage", "groundwater",
    ]),
    ("Climate/Environment", [
        "climate", "environment", "green", "resilience",
        "disaster", "renewable", "biodiversity",
        "carbon", "emission", "forest",
    ]),
    ("Agriculture", [
        "agriculture", "rural", "farming", "food",
        "fisheries", "livestock", "crop", "agri", "watershed",
    ]),
    ("Urban/Infrastructure", [
        "urban", "city", "municipal", "smart city",
        "road", "highway", "transport", "bridge",
        "construction", "infrastructure",
    ]),
    ("Finance/Economics", [
        "finance", "fiscal", "tax", "budget", "banking",
        "microfinance", "credit", "insurance", "msme",
        "financial inclusion",
    ]),
    ("Social Protection", [
        "social protection", "poverty", "inclusion",
        "gender", "women", "child", "disability",
        "vulnerable", "tribal", "scheduled",
    ]),
    ("Governance", [
        "governance", "public administration", "institutional",
        "policy", "regulatory", "e-governance", "g2c",
        "anti-corruption", "civil service",
    ]),
    ("Digital/Technology", [
        "digital", "ict", "technology", "data", "software",
        "platform", "system", "database", "mis", "it ",
    ]),
]

_INDIA_STATES: list[str] = [
    "andhra", "arunachal", "assam", "bihar", "chhattisgarh", "goa",
    "gujarat", "haryana", "himachal", "jharkhand", "karnataka", "kerala",
    "madhya pradesh", "maharashtra", "manipur", "meghalaya", "mizoram",
    "nagaland", "odisha", "punjab", "rajasthan", "sikkim", "tamil",
    "telangana", "tripura", "uttar pradesh", "uttarakhand", "west bengal",
    "delhi", "j&k", "jammu", "kashmir", "ladakh", "northeast", "north east",
]


# =============================================================================
# SECTION 4 — Signal Detection
# =============================================================================

class SignalResult(TypedDict):
    consulting_type: str
    contract_size:   str
    sector:          str
    geography:       str


def detect_consulting_signals(row: dict) -> SignalResult:
    def _get(*keys: str) -> str:
        for k in keys:
            v = row.get(k)
            if v and str(v).strip() not in ("", "N/A", "TBD", "-", "None"):
                return str(v).strip()
        return ""

    desc   = _get("Description", "description", "title", "Title", "subject")
    method = _get("Method", "method", "procurement_method")
    amount = _get("Estimated Amount (US$)", "value", "amount", "budget",
                  "contract_value")
    ctx    = _get("Project Name", "project_name", "organization",
                  "Organization", "client")

    text = (desc + " " + method + " " + ctx).lower()

    ctype = "General Consulting"
    for label, patterns in _CTYPE_PATTERNS:
        if any(p in text for p in patterns):
            ctype = label
            break

    sector = "General Development"
    for label, patterns in _SECTOR_PATTERNS:
        if any(p in text for p in patterns):
            sector = label
            break

    contract_size = _parse_contract_size(amount, text)

    geography = "India"
    for state in _INDIA_STATES:
        if state in text:
            geography = state.title()
            break

    return SignalResult(
        consulting_type = ctype,
        contract_size   = contract_size,
        sector          = sector,
        geography       = geography,
    )


def _parse_contract_size(amount_raw: str, text: str) -> str:
    if amount_raw and amount_raw not in ("", "0", "N/A", "TBD", "-"):
        clean = re.sub(r"[^\d.]", "", amount_raw.replace(",", ""))
        try:
            base_float = float(clean) if clean else 0
            if "m" in amount_raw.lower() and base_float < 10_000:
                base_float *= 1_000_000
            if base_float > 0:
                return _size_from_usd(base_float)
        except ValueError:
            pass

    if any(kw in text for kw in ["nationwide", "national programme",
                                  "large scale", "multi-year",
                                  "programme wide", "major"]):
        return "Large"
    if any(kw in text for kw in ["pilot", "district", "small scale",
                                  "community", "village level"]):
        return "Small"
    return "Medium"


def _size_from_usd(usd: float) -> str:
    if usd >= 500_000: return "Large"
    if usd >= 100_000: return "Medium"
    return "Small"


# =============================================================================
# SECTION 5 — Notice Type Classification
# =============================================================================

def _classify_notice_type(row: dict) -> str:
    """
    Classify a row's notice type as 'consulting', 'goods', or 'unknown'.
    Uses Method / method / Type / type field.
    """
    method = str(
        row.get("Method") or row.get("method") or
        row.get("Type")   or row.get("type") or ""
    ).lower().strip()

    if not method:
        return "unknown"
    if any(t in method for t in _CONSULTING_NOTICE_TYPES):
        return "consulting"
    if any(t in method for t in _GOODS_NOTICE_TYPES):
        return "goods"
    return "unknown"


# =============================================================================
# SECTION 5b — Decision Tier Classification
# =============================================================================

def classify_decision_tier(
    quality_score:         int,
    consulting_confidence: float,
) -> str:
    """
    Convert (quality_score, consulting_confidence) → actionable decision tag.

    Rules (evaluated in order):
      1. confidence < 0.40 OR score < 40  → IGNORE
         (too uncertain or too little data regardless of notice type)
      2. score ≥ 75  AND confidence ≥ 0.70 → BID_NOW
         (strong data + high consulting probability — act immediately)
      3. score ≥ 60                         → STRONG_CONSIDER
         (good opportunity, confidence already ≥ 0.40 from rule 1)
      4. score 40–59                        → WEAK_CONSIDER
         (marginal — review manually before investing time)

    The dual gate on BID_NOW (score AND confidence) prevents goods tenders
    with high raw completeness from leaking into the top tier.
    """
    if consulting_confidence < TIER_IGNORE_CONFIDENCE or quality_score < TIER_WEAK_MIN:
        return "IGNORE"
    if quality_score >= TIER_BID_NOW_SCORE and consulting_confidence >= TIER_BID_NOW_CONFIDENCE:
        return "BID_NOW"
    if quality_score >= TIER_STRONG_MIN:
        return "STRONG_CONSIDER"
    return "WEAK_CONSIDER"


# =============================================================================
# SECTION 6 — Consulting Confidence Score
# =============================================================================

def compute_consulting_confidence(row: dict) -> float:
    """
    Compute consulting confidence: how likely this tender is a consulting /
    advisory opportunity rather than goods procurement.

    Returns a float in [0.0, 1.0].

    Components
    ──────────
    1. Notice type / method     (primary signal, 0.05–0.95)
       Exact code match checked first (fast, no false positives for short
       codes like "IC");  phrase substring checked for longer strings.

    2. Consulting keyword density  (+0.05 per hit, capped at +0.20)
       Measured against the real description/title field only — no synthetic
       text injection from portal scrapers.

    This function is called by apply_intelligence_filter() for EVERY row
    across ALL portals.  Portal scrapers must NOT call it directly.
    """
    def _get(*keys: str) -> str:
        for k in keys:
            v = row.get(k)
            if v and str(v).strip() not in ("", "N/A", "TBD", "-", "None"):
                return str(v).strip().lower()
        return ""

    method = _get("Method", "method", "Type", "type", "procurement_method")
    desc   = _get("Description", "description", "title", "Title")

    # ── 1. Notice type base confidence ────────────────────────────────────────
    # Priority: exact match (avoids "ic" matching "technical" as substring)
    base = _CONFIDENCE_EXACT.get(method)

    if base is None:
        # Phrase match: only attempt for strings longer than 3 chars
        for phrase, conf in _CONFIDENCE_PHRASES.items():
            if phrase in method:
                base = conf
                break

    if base is None:
        base = 0.50 if method else 0.40   # method present but unrecognized / absent

    # ── 2. Keyword density boost ──────────────────────────────────────────────
    kw_hits = sum(1 for kw in CONSULTING_WHITELIST if kw in desc)
    confidence = min(1.0, base + min(0.20, kw_hits * 0.05))

    return round(confidence, 3)


# =============================================================================
# SECTION 7 — Quality Scoring (raw data completeness)
# =============================================================================

def compute_quality_score(row: dict) -> int:
    """
    Compute raw completeness score 0-100.

    Measures how much useful data the row contains — NOT consulting relevance.
    Consulting relevance is captured separately by compute_consulting_confidence().

    Breakdown
    ─────────
      Description richness   : 0-30 pts
      Consulting keyword hits : 0-25 pts  (5 pts each, capped)
      Method/procedure        : 0-20 pts
      Amount present & valid  : 0-15 pts
      Deadline present        : 0-10 pts
    """
    def _get(*keys: str) -> str:
        for k in keys:
            v = row.get(k)
            if v:
                s = str(v).strip()
                if s not in ("", "N/A", "TBD", "-", "None", "0"):
                    return s
        return ""

    desc     = _get("Description", "description", "title", "Title")
    method   = _get("Method", "method", "Type", "type", "procurement_method")
    amount   = _get("Estimated Amount (US$)", "value", "amount", "budget")
    deadline = _get("Contract Completion", "deadline", "Deadline",
                    "closing_date", "Last Date")

    text_lower = (desc + " " + method).lower()
    score      = 0

    # 1. Description richness (0-30)
    word_count = len(desc.split())
    if   word_count >= 10: score += 30
    elif word_count >= 6:  score += 20
    elif word_count >= 3:  score += 12
    elif word_count >= 1:  score +=  5

    # 2. Consulting keyword hits (0-25)
    kw_hits = sum(1 for kw in _SCORE_KWS if kw in text_lower)
    score  += min(25, kw_hits * 5)

    # 3. Method / selection procedure (0-20)
    method_lower = method.lower().strip()
    # Exact code match first (avoids false positives)
    method_pts = _METHOD_EXACT_SCORES.get(method_lower, 0)
    if method_pts == 0:
        method_pts = max(
            (v for k, v in _METHOD_PHRASE_SCORES.items() if k in method_lower),
            default=0,
        )
    if method_pts == 0 and method_lower:
        method_pts = 5   # method present but unrecognized — minimal credit
    score += method_pts

    # 4. Amount present (0-15)
    if amount:
        clean = re.sub(r"[^\d]", "", amount)
        if clean and int(clean) > 0:
            score += 15
        else:
            score += 8

    # 5. Deadline present (0-10)
    if deadline:
        score += 10

    return min(100, score)


# =============================================================================
# SECTION 8 — Quality Filtering
# =============================================================================

def is_generic_only(text: str) -> bool:
    words = re.sub(r"[^a-z\s]", "", text.lower()).split()
    if not words:
        return True
    meaningful = [w for w in words if len(w) > 2 and w not in GENERIC_STOP_WORDS]
    return len(meaningful) == 0


def passes_quality_filter(
    row:           dict,
    quality_score: int,
    threshold:     int = QUALITY_THRESHOLD,
) -> tuple[bool, str]:
    """
    Three-gate quality filter applied against the confidence-weighted
    quality_score (= raw_score × consulting_confidence).

    Gate 1: description length < 10 chars
    Gate 2: description consists only of generic stop-words
    Gate 3: no consulting whitelist keyword AND score < threshold
    Gate 4: hard floor — score < threshold
    """
    def _get_desc() -> str:
        for k in ("Description", "description", "title", "Title"):
            v = row.get(k)
            if v:
                return str(v).strip()
        return ""

    def _get_text() -> str:
        method = str(row.get("Method") or row.get("method") or
                     row.get("Type")   or row.get("type") or "").lower()
        return (_get_desc() + " " + method).lower()

    desc = _get_desc()
    text = _get_text()

    if len(desc) < 10:
        return False, f"desc too short ({len(desc)} chars)"

    if is_generic_only(desc):
        return False, "generic-only description"

    has_whitelist_kw = any(kw in text for kw in CONSULTING_WHITELIST)
    if not has_whitelist_kw and quality_score < threshold:
        return False, f"no consulting keyword + low score ({quality_score})"

    if quality_score < threshold:
        return False, f"quality score {quality_score} < threshold {threshold}"

    return True, ""


# =============================================================================
# SECTION 9 — Batch Intelligence Filter
# =============================================================================

def apply_intelligence_filter(
    rows:      list[dict],
    threshold: int = QUALITY_THRESHOLD,
) -> tuple[list[dict], list[dict], list[str]]:
    """
    Run the full quality + confidence + filter pipeline on a list of rows.

    For each row:
      1. Compute raw completeness score (compute_quality_score)
      2. Compute consulting confidence   (compute_consulting_confidence)
      3. quality_score = round(raw × confidence)
      4. Apply GOODS_SCORE_CAP for goods-type notices
      5. Run quality filter gates        (passes_quality_filter)
      6. Detect consulting signals       (detect_consulting_signals)
      7. Inject all intelligence fields into enriched copy

    Enriched rows carry:
      quality_score        — final confidence-weighted score (primary ranking key)
      raw_quality_score    — raw completeness score (for debug / transparency)
      consulting_confidence— float 0.0-1.0 (for debug / validation)
      decision_tag         — BID_NOW | STRONG_CONSIDER | WEAK_CONSIDER | IGNORE
      consulting_type, sector, contract_size, geography (signal detection)

    Returns:
        accepted       — enriched rows that passed all gates
        rejected       — rows that failed (for monitoring stats)
        reject_reasons — parallel list of human-readable reasons
    """
    accepted:       list[dict] = []
    rejected:       list[dict] = []
    reject_reasons: list[str]  = []

    for row in rows:
        # ── Compute scores ────────────────────────────────────────────────────
        raw_score  = compute_quality_score(row)
        confidence = compute_consulting_confidence(row)

        # ── Priority score = completeness × consulting confidence ─────────────
        quality_score = max(0, round(raw_score * confidence))

        # ── Hard goods cap — prevents goods from outranking consulting ─────────
        if _classify_notice_type(row) == "goods":
            quality_score = min(quality_score, GOODS_SCORE_CAP)

        # ── Filter ────────────────────────────────────────────────────────────
        passes, reason = passes_quality_filter(row, quality_score, threshold)
        if not passes:
            rejected.append(row)
            reject_reasons.append(reason)
            continue

        # ── Enrich ────────────────────────────────────────────────────────────
        signals  = detect_consulting_signals(row)
        tier     = classify_decision_tier(quality_score, confidence)
        enriched = {
            **row,
            # Primary score (confidence-weighted — used for all ranking/filtering)
            "quality_score":         quality_score,
            "Quality Score":         quality_score,
            # Raw completeness score (transparent, for debug)
            "raw_quality_score":     raw_score,
            "Raw Quality Score":     raw_score,
            # Confidence (for debug and portal enrich_fields)
            "consulting_confidence": confidence,
            "Consulting Confidence": round(confidence * 100),
            # Decision tier tag (actionable label for notifications + Excel)
            "decision_tag":          tier,
            "Decision":              tier,
            # Consulting signals
            "consulting_type":       signals["consulting_type"],
            "Consulting Type":       signals["consulting_type"],
            "contract_size":         signals["contract_size"],
            "Contract Size":         signals["contract_size"],
            "sector":                signals["sector"],
            "Sector":                signals["sector"],
            "geography":             signals["geography"],
            "Geography":             signals["geography"],
        }
        accepted.append(enriched)

    return accepted, rejected, reject_reasons
