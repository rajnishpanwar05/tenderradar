# =============================================================================
# keywords.py — Shared FIRM_EXPERTISE keyword bank  (MASTER / SUPERSET)
#
# Merged from ALL pipeline files:
#   worldbank_pipeline, gem_pipeline, devnet_pipeline,
#   cg_eproc_scraper, giz_india_scraper, sikkim_tender_scraper
#
# Every pipeline imports score_relevance (and optionally FIRM_EXPERTISE)
# from here — single source of truth, maximum keyword coverage.
# =============================================================================

import json
import os
import re
from typing import Dict


_BASE_DIR = os.path.expanduser("~/tender_system")


def _load_json_config(filename: str) -> Dict:
    """Load a JSON config file from config/ directory safely."""
    path = os.path.join(_BASE_DIR, "config", filename)
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


_FIRM_PROFILE = _load_json_config("firm_profile.json")
_IDCG_KEYWORDS = _load_json_config("idcg_keywords.json")


FIRM_EXPERTISE = {
    # ── M&E / Evaluation ──────────────────────────────────────────────────────
    "M&E / Evaluation": [
        # Core terms
        "monitoring", "evaluation", "m&e", "impact", "assessment",
        "outcome", "indicator", "review", "learning", "verification",
        "survey", "formative",
        "baseline", "baseline survey", "endline", "end-line", "mid-term",
        "mid term review", "impact assessment", "impact evaluation",
        "final evaluation", "performance review", "program evaluation",
        "process monitoring", "rapid assessment", "concurrent monitoring",
        "needs assessment", "customer satisfaction survey",
        "iva", "tpm", "rtm", "kap survey", "portfolio review",
        "independent verification", "iva", "beneficiary feedback",
        "third party monitoring", "real time monitoring",
        "knowledge attitude", "learning exercise",
    ],

    # ── Environment & Social ──────────────────────────────────────────────────
    "Environment & Social": [
        "environment", "social assessment", "esa", "environmental audit",
        "ecosystem", "forest", "climate", "climate change", "coastal",
        "pollution", "green", "biodiversity", "nature", "carbon", "emission",
        "adaptation", "esia", "environmental impact", "eia", "iesa",
        "integrated environmental", "disaster risk", "drr",
        "afforestation", "deforestation", "land degradation", "conservation",
        "wetland", "agroforestry", "ntfp", "natural resource", "wildlife", "forestry",
    ],

    # ── Education & Skills ────────────────────────────────────────────────────
    "Education & Skills": [
        "education", "school", "skill", "training", "learning", "literacy",
        "vocational", "higher education", "student", "reading", "capacity",
        "curriculum", "teacher training", "edtech", "education technology",
        "dropout", "enrolment", "retention", "digital literacy", "foundational",
        "scholarship", "iti", "tvet", "college", "school management",
        "midday meal", "coaching",
    ],

    # ── Agriculture & Rural ───────────────────────────────────────────────────
    "Agriculture & Rural": [
        "agriculture", "rural", "farmer", "irrigation",
        "livelihood", "tribal", "village", "crop", "food security",
        "horticulture", "fisheries", "animal husbandry", "dairy",
        "value chain", "fpo", "farmer producer", "agricultural marketing",
        "organic", "agri", "kisan", "cold chain", "mandi",
    ],

    # ── Water & Sanitation ────────────────────────────────────────────────────
    "Water & Sanitation": [
        "water", "sanitation", "wss", "wastewater", "drinking water",
        "groundwater", "sewage", "wash", "hygiene", "odf",
        "open defecation", "toilet", "jal jeevan", "jjm",
        "water quality", "piped water", "pipeline", "fluoride", "arsenic",
        "cwrm", "phed", "phsc", "rwss",
    ],

    # ── Social Protection & Health ────────────────────────────────────────────
    "Social Protection & Health": [
        "social protection", "nutrition", "health", "icds", "beneficiary",
        "cash transfer", "poverty", "welfare", "hiv", "tb", "aids",
        "family planning", "reproductive", "maternal", "child",
        "anganwadi", "asha", "public health", "immunization", "vaccination",
        "mental health", "disability", "elderly", "trafficking",
        "hospital", "medicine", "nrhm", "nmhp",
        "disease surveillance", "ngo", "csr",
    ],

    # ── Urban Development ─────────────────────────────────────────────────────
    "Urban Development": [
        "urban", "municipal", "city", "metro",
        "waste management", "infrastructure", "housing", "affordable housing",
        "slum", "amrut", "transport", "mobility", "solid waste",
        "smart city", "nagar", "uda", "swachh bharat",
        "town planning", "zoning", "msme",
    ],

    # ── Energy & Power ────────────────────────────────────────────────────────
    "Energy & Power": [
        "energy", "power", "electricity", "solar", "renewable",
        "thermal", "clean energy", "off-grid", "biomass", "wind", "geothermal",
        "energy efficiency", "energy audit", "transmission", "distribution",
        "metering", "creda", "cspdcl", "cspgcl", "electrification",
        "hydro", "grid", "photovoltaic", "net zero", "decarboni", "pump",
    ],

    # ── Governance & Institutional ────────────────────────────────────────────
    "Governance & Institutional": [
        "governance", "institutional", "reform", "policy", "rule of law",
        "anti-corruption", "transparency", "accountability", "democratic",
        "public administration", "decentralization", "pmo",
        "change management", "strategic planning", "regulatory",
        "compliance", "digital public", "e-governance", "chips", "nic",
        "digital", "policy research",
    ],

    # ── Research & Documentation ──────────────────────────────────────────────
    "Research & Documentation": [
        "research", "study", "documentation", "data collection",
        "report", "publication", "analysis", "mapping", "scoping",
        "scoping study", "feasibility study", "feasibility", "due diligence",
        "profiling", "census", "enumeration", "process documentation",
        "gis", "dpr", "concept note", "white paper",
        "scope of work", "terms of reference",
        "consultancy",
    ],

    # ── Gender & Inclusion ────────────────────────────────────────────────────
    "Gender & Inclusion": [
        "gender", "women", "inclusion", "disability", "youth", "minority",
        "marginalized", "vulnerable", "equality", "women empowerment",
        "child protection", "gender mainstreaming",
    ],

    # ── Capacity Building & Advisory ──────────────────────────────────────────
    "Capacity Building & Advisory": [
        "capacity building", "advisory", "technical assistance",
        "consultancy", "consultant", "knowledge", "knowledge partner",
        "mentoring", "coaching", "hand-holding",
        "organizational development", "system strengthening",
        "project management", "project management consultant", "pmc",
        "rfp", "request for proposal", "proposal",
    ],

    # ── Communications & Media ────────────────────────────────────────────────
    "Communications & Media": [
        "communication", "media", "public relations", "film",
        "iec material", "iec", "content", "creative",
        "branding", "outreach", "awareness campaign", "social media",
        "behavior change communication", "bcc", "advocacy",
    ],

    # ── Finance & Audit ───────────────────────────────────────────────────────
    "Finance & Audit": [
        "audit", "internal audit", "finance", "financial",
        "accounting", "compliance", "forensic audit", "social audit",
        "cost-benefit", "financial management", "fund utilization",
        "expenditure", "budget", "fiduciary",
    ],

    # ── Circular Economy & Waste ──────────────────────────────────────────────
    "Circular Economy & Waste": [
        "circular economy", "plastic", "e-waste", "waste",
        "recycling", "battery", "second life",
    ],

    # ── Tourism & Ecology ─────────────────────────────────────────────────────
    "Tourism & Ecology": [
        "tourism", "eco tourism", "ecotourism", "trekking",
        "heritage", "culture", "nature", "inspires",
    ],

    # ── Infrastructure & Construction ─────────────────────────────────────────
    "Infrastructure & Construction": [
        "construction", "road", "bridge", "building",
        "structure", "civil works", "pwd", "pmgsy",
        "adb", "nhidcl",
    ],
}


def score_relevance(title: str, description: str = "") -> str:
    """
    Return comma-separated matched expertise categories.

    Honors CAP STAT hard-block phrases so goods/works/non-consulting tenders
    do not get marked relevant by generic keyword overlap.
    """
    if is_hard_blocked(title, description):
        return ""

    text = (title + " " + description).lower()
    matched = [cat for cat, kws in FIRM_EXPERTISE.items()
               if any(kw in text for kw in kws)]
    return ", ".join(matched) if matched else ""


# =============================================================================
# NUMERIC RELEVANCE SCORER  (0 – 100)
# =============================================================================
# Each tier has an explicit cap so no single category can monopolise the score.
#
# Layer 1 — Service/consulting type keywords  (cap: 50 pts)
#   Consulting, evaluation, advisory, M&E, capacity building …
# Layer 2 — Negative penalty keywords         (floor: -35 pts)
#   Supply of goods, construction, equipment, hardware …
# Layer 3 — Development sector bonus          (cap: 20 pts)
#   Agriculture, health, education, climate, governance …
# Layer 4 — Geography bonus                   (cap: 12 pts)
#   India, South Asia, Africa, Southeast Asia, developing countries …
#
# Raw range ≈ -35 → +82.  Calibration divisor = 65 so a strong consulting
# tender (e.g. TA + evaluation + climate + India) lands near 80-90.
# =============================================================================

# Long phrases must appear BEFORE shorter sub-phrases so the scoring loop
# (which checks all keys) adds the correct larger weight first.  The cap
# prevents double-counting from inflating beyond _MAX_CONSULTING.
_CONSULTING_KW: dict = {
    # Top-tier consulting engagement types
    "monitoring and evaluation": 18,
    "independent verification agency": 16,
    "third party monitoring": 18,
    "technical assistance": 15,
    "impact evaluation": 15,
    "impact assessment": 18,
    "consultancy services": 14,
    "advisory services": 13,
    "final evaluation": 13,
    "baseline survey": 18,
    "endline": 18,
    "end-line": 18,
    "needs assessment": 15,
    "customer satisfaction survey": 12,
    "mid-term review": 16,
    "mid term review": 16,
    "capacity development": 12,
    "capacity building": 12,
    "project management consultant": 12,
    "social audit": 11,
    "feasibility study": 11,
    "expression of interest": 10,
    "scoping study": 10,
    "consultancy": 10,
    "consultant": 10,
    "evaluation": 10,
    "monitoring": 10,
    "advisory": 10,
    "baselines": 12,
    "baseline": 12,
    "m&e": 9,
    "iva": 9,
    "tpm": 15,
    "pmc": 9,
    "training": 8,
    "feasibility": 8,
    "request for proposal": 8,
    "scoping": 7,
    "research": 7,
    "survey": 7,
    "assessment": 7,
    "audit": 7,
    "study": 7,
    "rfp": 7,
    "eoi": 7,
    "review": 6,
    "terms of reference": 6,
    "workshop": 6,
    "mapping": 6,
    "data collection": 5,
    "documentation": 5,
    "analysis": 5,
}

_NEGATIVE_KW: dict = {
    # ── Hard-irrelevant: not consulting at all ────────────────────────────────
    "procurement of goods": -25,
    "supply of goods":      -22,
    "supply of":            -15,
    "civil works":          -20,
    "construction work":    -20,
    "construction of":      -18,
    "construction":         -15,
    "hardware":             -14,
    "equipment":            -12,
    "vehicle":              -12,
    "machinery":            -10,
    "furniture":            -10,
    "printed material":     -9,
    "printing of":          -8,
    "rate contract":        -8,
    "annual maintenance":   -6,
    "amc":                  -5,
    "computer":             -5,
    "laptop":               -5,
    # ── Non-IDCG professional services (law, IT, architecture, CA, medical) ──
    # These often contain "consultant" or "advisory" but are NOT development
    # consulting. Experts call these "false-positive consulting signals".
    "law firm":             -30,
    "legal consultant":     -25,
    "legal advisory":       -25,
    "legal services":       -22,
    "legal counsel":        -22,
    "advocate":             -20,
    "lawyer":               -20,
    "attorney":             -20,
    "empanelment of advocates": -30,
    "chartered accountant": -25,
    "ca firm":              -22,
    "statutory auditor":    -20,
    "tax consultant":       -18,
    "company secretary":    -18,
    "architecture firm":    -22,
    "architectural services": -22,
    "architect":            -18,
    "interior designer":    -18,
    "software development": -22,
    "software consultant":  -20,
    "app development":      -20,
    "website development":  -20,
    "web development":      -18,
    "it consultant":        -18,
    "erp implementation":   -18,
    "system integrator":    -15,
    "medical equipment":    -20,
    "pharmaceutical":       -18,
    "drug supply":          -18,
    "laboratory equipment": -15,
    "reagent":              -12,
    # ── Manpower / outsourcing (not consulting) ──────────────────────────────
    "manpower supply":      -22,
    "outsourcing of":       -20,
    "security services":    -18,
    "security guard":       -18,
    "housekeeping":         -18,
    "cleaning services":    -15,
    "data entry operator":  -15,
    "stenographer":         -12,
    "driver":               -10,
}

_SECTOR_BONUS: dict = {
    # ── IDCG core sectors — strong positive ──────────────────────────────────
    "monitoring and evaluation":    12,
    "impact evaluation":            12,
    "baseline survey":              12,
    "third party monitoring":       12,
    "program evaluation":           11,
    "climate change":               10,
    "social protection":             9,
    "agriculture":                  10,
    "agricultural":                 10,
    "health":                        8,
    "education":                    10,
    "governance":                    7,
    "livelihoods":                  10,
    "livelihood":                   10,
    "forestry":                     10,
    "water":                         6,
    "sanitation":                    6,
    "wash":                          7,
    "environment":                   7,
    "gender":                        6,
    "nutrition":                     6,
    "rural":                         5,
    "tribal":                        6,
    "forest":                        5,
    "women":                         5,
    "school":                        5,
    "learning":                      5,
    "climate":                       7,
    # ── Sectors IDCG does NOT work in — penalty ──────────────────────────────
    # "energy" and "renewable energy" removed — they cover solar panel SUPPLY
    # tenders just as much as energy ACCESS evaluation tenders. Without a way
    # to tell them apart at keyword level, the bonus produces false-positives.
    # "urban" removed — covers infrastructure/construction equally.
    # These sectors remain neutral (0 bonus) unless consulting signals appear.
}

_GEO_BONUS: dict = {
    "developing countries": 7,
    "developing country":   7,
    "global south":         7,
    "sub-saharan":          7,
    "south asia":           7,
    "southeast asia":       6,
    "least developed":      6,
    "india":                7,
    "africa":               6,
    "east africa":          6,
    "west africa":          6,
    "south asian":          6,
    "developing":           4,
    "low-income":           5,
    "low income":           5,
    "bangladesh":           5,
    "nepal":                5,
    "kenya":                5,
    "tanzania":             5,
    "ethiopia":             5,
    "uganda":               5,
    "ghana":                5,
    "nigeria":              5,
    "mozambique":           5,
    "rwanda":               5,
    "cambodia":             5,
    "myanmar":              5,
    "vietnam":              5,
    "indonesia":            5,
    "sri lanka":            5,
    "pakistan":             5,
}

# Max points per layer (prevents runaway stacking)
_MAX_CONSULTING = 50
_MAX_NEGATIVE   = -35
_MAX_SECTOR     = 20
_MAX_GEO        = 12
_MAX_PROFILE_POS = 28
_MAX_PROFILE_NEG = -30

# Raw score of a near-perfect consulting tender → maps to ≈ 100
# Layers 1-5 max = 50+20+12+28 = 110, Layer 6 adds up to 40 more → 150 theoretical max.
# A strong IDCG-fit tender (TA + evaluation + climate + India + World Bank) ≈ 110 raw.
# _CALIBRATION raised to 160 so strong-but-not-perfect tenders no longer all
# saturate at 100.  Target: only the top ~10-15% of tenders reach ≥ 85.
_CALIBRATION = 160


def _compile_kw_pattern(keyword: str):
    """
    Compile boundary-aware regex for keyword matching.
    Uses \b boundaries and flexible spaces for multi-word phrases.
    """
    kw = (keyword or "").strip().lower()
    if not kw:
        return None
    # Treat spaces in phrases as one-or-more whitespace.
    escaped = re.escape(kw).replace(r"\ ", r"\s+")
    try:
        return re.compile(rf"\b{escaped}\b", re.IGNORECASE)
    except Exception:
        return None


def _kw_present(text: str, keyword: str) -> bool:
    pat = _compile_kw_pattern(keyword)
    if pat is None:
        return False
    return bool(pat.search(text))


def _profile_weighted_map(keys: tuple) -> dict:
    """
    Merge profile keyword sources into a single weighted map:
      - list[str]  -> default weight 6
      - dict[str,int/float] -> explicit weights
    """
    out = {}
    for key in keys:
        val = _FIRM_PROFILE.get(key)
        if isinstance(val, dict):
            for k, w in val.items():
                if not str(k).strip():
                    continue
                try:
                    out[str(k).strip().lower()] = int(round(float(w)))
                except Exception:
                    continue
        elif isinstance(val, list):
            for k in val:
                if str(k).strip():
                    out[str(k).strip().lower()] = 6
    return out


_PROFILE_POSITIVE_KW = _profile_weighted_map(
    ("preferred_keywords", "preferred_consulting_types")
)
_PROFILE_NEGATIVE_KW = _profile_weighted_map(
    ("negative_keywords", "avoid_keywords")
)


# =============================================================================
# CAP STAT KEYWORD INTEGRATION  (Layer 6 — from config/idcg_keywords.json)
# =============================================================================
# Loads keyword intelligence derived from IDCG's 184-project Capability
# Statement.  Adds three scoring layers:
#   6A: Hard-block — tenders matching "hard_block" negative keywords → score = 0
#   6B: CAP STAT service/sector/methodology/differentiator boost (capped)
#   6C: CAP STAT client/geography boost (capped)
# =============================================================================

def _load_capstat_flat(section_key: str) -> Dict[str, int]:
    """Extract flat keyword→weight map from a CAP STAT section (handles nested groups)."""
    section = _IDCG_KEYWORDS.get(section_key, {})
    result: Dict[str, int] = {}
    for group_key, group_val in section.items():
        if group_key.startswith("_"):
            continue
        if isinstance(group_val, dict):
            weight = int(group_val.get("weight", 6))
            for kw in group_val.get("keywords", []):
                if str(kw).strip():
                    result[str(kw).strip().lower()] = weight
        elif isinstance(group_val, list):
            for kw in group_val:
                if str(kw).strip():
                    result[str(kw).strip().lower()] = 6
    return result


def _load_capstat_keyword_list(section_key: str) -> list:
    """Extract flat keyword list from a CAP STAT section with direct keywords list."""
    section = _IDCG_KEYWORDS.get(section_key, {})
    keywords = section.get("keywords", [])
    weight = int(section.get("weight", 6))
    return [(str(kw).strip().lower(), weight) for kw in keywords if str(kw).strip()]


# ── CAP STAT hard-block keywords (score → 0 immediately) ───────────────────
_CAPSTAT_HARD_BLOCK: list = []
neg_section = _IDCG_KEYWORDS.get("negative_keywords", {})
hard_block = neg_section.get("hard_block", {})
if isinstance(hard_block, dict):
    _CAPSTAT_HARD_BLOCK = [
        str(kw).strip().lower()
        for kw in hard_block.get("keywords", [])
        if str(kw).strip()
    ]

# ── CAP STAT positive scoring maps ─────────────────────────────────────────
_CAPSTAT_SERVICE_KW = _load_capstat_flat("core_service_keywords")
_CAPSTAT_SECTOR_KW = _load_capstat_flat("sector_keywords")
_CAPSTAT_CLIENT_KW = _load_capstat_flat("client_keywords")
_CAPSTAT_GEO_KW = _load_capstat_flat("geography_keywords")
_CAPSTAT_METHOD_KW = dict(_load_capstat_keyword_list("methodology_keywords"))
_CAPSTAT_DIFF_KW = dict(_load_capstat_keyword_list("idcg_differentiators"))

# ── Combined CAP STAT positive map (for Layer 6B) ──────────────────────────
_CAPSTAT_ALL_POSITIVE: Dict[str, int] = {}
for _src in (_CAPSTAT_SERVICE_KW, _CAPSTAT_SECTOR_KW, _CAPSTAT_METHOD_KW, _CAPSTAT_DIFF_KW):
    for _k, _w in _src.items():
        if _k not in _CAPSTAT_ALL_POSITIVE or _w > _CAPSTAT_ALL_POSITIVE[_k]:
            _CAPSTAT_ALL_POSITIVE[_k] = _w

# ── Combined CAP STAT client+geo map (for Layer 6C) ────────────────────────
_CAPSTAT_CLIENT_GEO: Dict[str, int] = {}
for _src in (_CAPSTAT_CLIENT_KW, _CAPSTAT_GEO_KW):
    for _k, _w in _src.items():
        if _k not in _CAPSTAT_CLIENT_GEO or _w > _CAPSTAT_CLIENT_GEO[_k]:
            _CAPSTAT_CLIENT_GEO[_k] = _w

# CAP STAT layer caps
_MAX_CAPSTAT_POSITIVE = 25
_MAX_CAPSTAT_CLIENT_GEO = 15


def is_hard_blocked(title: str, description: str = "") -> bool:
    """
    Check if a tender matches hard-block negative keywords from CAP STAT.
    Returns True if the tender should be auto-scored at 0 (not consulting).
    """
    text = (title + " " + description).lower()
    return any(_kw_present(text, kw) for kw in _CAPSTAT_HARD_BLOCK)


def score_tender_numeric(title: str, description: str = "",
                         country: str = "") -> tuple:
    """
    Score a tender on a 0-100 relevance scale for a consulting firm.

    Args:
        title:       Tender title (always checked).
        description: Body text / relevance string (optional).
        country:     Country field from the row (optional, for geo bonus).

    Returns:
        (score: int, reason: str)
        score  — 0-100 integer.  80+ = HIGH, 50-79 = MEDIUM, 25-49 = LOW, <25 = IRRELEVANT
        reason — one-sentence explanation, e.g.
                 "High relevance — service type: Technical Assistance;
                  sector: Climate; geography: India."
    """
    text = (title + " " + description + " " + country).lower()

    # ── Layer 0: CAP STAT hard-block check ──────────────────────────────────
    # If title+desc matches hard-block keywords (supply of goods, civil works
    # contract, etc.), immediately return score=0. These are never consulting.
    if is_hard_blocked(title, description):
        blocked_kw = next(
            (kw for kw in _CAPSTAT_HARD_BLOCK if _kw_present(text, kw)), "blocked"
        )
        return 0, f"Not relevant — hard-blocked by '{blocked_kw}' (not consulting)."

    # ── Layer 1: consulting keyword score (capped) ────────────────────────────
    consulting_raw  = 0
    matched_service = []
    for kw, weight in sorted(_CONSULTING_KW.items(), key=lambda x: -x[1]):
        if _kw_present(text, kw):
            consulting_raw += weight
            matched_service.append(kw)
    consulting_score = min(_MAX_CONSULTING, consulting_raw)

    # ── Layer 2: negative penalty (floored) ───────────────────────────────────
    neg_raw     = 0
    matched_neg = []
    for kw, weight in sorted(_NEGATIVE_KW.items(), key=lambda x: x[1]):
        if _kw_present(text, kw):
            neg_raw += weight
            matched_neg.append(kw)
    neg_score = max(_MAX_NEGATIVE, neg_raw)

    # ── Layer 3: development sector bonus (capped) ────────────────────────────
    # GATE: sector bonus only applies when Layer 1 matched at least one consulting
    # keyword.  Without this gate, supply/goods tenders that merely mention a
    # development sector ("supply of health equipment", "water pipe installation")
    # were receiving an unearned relevance boost and leaking into the output.
    sector_raw     = 0
    matched_sector = []
    if consulting_raw > 0:   # consulting signal required before sector bonus fires
        for kw, weight in sorted(_SECTOR_BONUS.items(), key=lambda x: -x[1]):
            if _kw_present(text, kw) and kw not in matched_sector:
                sector_raw += weight
                matched_sector.append(kw)
    sector_score = min(_MAX_SECTOR, sector_raw)

    # ── Layer 4: geography bonus (capped) ─────────────────────────────────────
    geo_raw     = 0
    matched_geo = []
    for kw, weight in sorted(_GEO_BONUS.items(), key=lambda x: -x[1]):
        if _kw_present(text, kw) and kw not in matched_geo:
            geo_raw += weight
            matched_geo.append(kw)
    geo_score = min(_MAX_GEO, geo_raw)

    # ── Layer 5: firm-profile weighted keyword boost/penalty ──────────────────
    profile_pos_raw = 0
    profile_neg_raw = 0
    matched_profile_pos = []
    matched_profile_neg = []

    for kw, wt in sorted(_PROFILE_POSITIVE_KW.items(), key=lambda x: -x[1]):
        if wt > 0 and _kw_present(text, kw):
            profile_pos_raw += wt
            matched_profile_pos.append(kw)

    for kw, wt in sorted(_PROFILE_NEGATIVE_KW.items(), key=lambda x: -abs(x[1])):
        # negative keywords should reduce score even if configured as positive by mistake
        penalty = -abs(int(wt)) if wt else -6
        if _kw_present(text, kw):
            profile_neg_raw += penalty
            matched_profile_neg.append(kw)

    profile_pos_score = min(_MAX_PROFILE_POS, profile_pos_raw)
    profile_neg_score = max(_MAX_PROFILE_NEG, profile_neg_raw)

    # ── Layer 6A: CAP STAT service/sector/methodology boost (capped) ────────
    capstat_pos_raw = 0
    matched_capstat = []
    for kw, wt in sorted(_CAPSTAT_ALL_POSITIVE.items(), key=lambda x: -x[1]):
        if _kw_present(text, kw) and kw not in matched_capstat:
            capstat_pos_raw += wt
            matched_capstat.append(kw)
    capstat_pos_score = min(_MAX_CAPSTAT_POSITIVE, capstat_pos_raw)

    # ── Layer 6B: CAP STAT client/geography boost (capped) ──────────────────
    capstat_cg_raw = 0
    matched_capstat_cg = []
    for kw, wt in sorted(_CAPSTAT_CLIENT_GEO.items(), key=lambda x: -x[1]):
        if _kw_present(text, kw) and kw not in matched_capstat_cg:
            capstat_cg_raw += wt
            matched_capstat_cg.append(kw)
    capstat_cg_score = min(_MAX_CAPSTAT_CLIENT_GEO, capstat_cg_raw)

    # ── Normalise to 0-100 ────────────────────────────────────────────────────
    raw_total = (
        consulting_score + neg_score + sector_score + geo_score +
        profile_pos_score + profile_neg_score +
        capstat_pos_score + capstat_cg_score
    )
    score     = min(100, max(0, round(raw_total / _CALIBRATION * 100)))

    # ── Build one-sentence reason ─────────────────────────────────────────────
    if score >= 75:
        label = "High relevance"
    elif score >= 50:
        label = "Medium relevance"
    elif score >= 25:
        label = "Low relevance"
    else:
        label = "Not relevant"

    parts = []
    if matched_service:
        parts.append(f"service: {matched_service[0].title()}")
    if matched_sector:
        parts.append(f"sector: {matched_sector[0].title()}")
    if matched_geo:
        parts.append(f"geography: {matched_geo[0].title()}")
    if matched_neg:
        parts.append(f"penalised for '{matched_neg[0]}'")
    if matched_profile_pos:
        parts.append(f"firm-fit: {matched_profile_pos[0].title()}")
    if matched_profile_neg:
        parts.append(f"firm-mismatch: {matched_profile_neg[0]}")
    if matched_capstat:
        parts.append(f"CAP-STAT: {matched_capstat[0].title()}")
    if matched_capstat_cg:
        parts.append(f"client/geo: {matched_capstat_cg[0].title()}")

    if parts:
        reason = f"{label} — {'; '.join(parts[:5])}."
    else:
        reason = f"{label} — no specific consulting keywords matched."

    return score, reason


# Flat list for pre-filtering by title (skip goods/supply tenders early)
TITLE_FILTER_KEYWORDS = [
    "consultant", "consultancy", "advisory", "technical assistance",
    "service provider", "expert", "specialist",
    "evaluation", "assessment", "monitoring", "review", "baseline",
    "mid-term", "endline", "end-line", "impact", "verification",
    "third party", "tpm", "concurrent monitoring", "rapid assessment",
    "research", "study", "survey", "analysis", "documentation",
    "mapping", "feasibility", "scoping", "profiling", "needs assessment",
    "customer satisfaction", "training", "capacity building", "capacity development", "mentoring",
    "audit", "financial management", "fiduciary",
    "program management", "project management", "coordination",
    "communication", "iec", "awareness", "media", "content",
    "gender", "inclusion", "social", "environment", "climate",
    "rfp", "request for proposal", "terms of reference",
]


def title_is_relevant(title: str) -> bool:
    """Return True if title suggests a consulting/services tender (not goods/works)."""
    t = title.lower()
    return any(kw in t for kw in TITLE_FILTER_KEYWORDS)


# ---------------------------------------------------------------------------
# Consulting signal helper — used by opportunity_engine hard gate
# ---------------------------------------------------------------------------
_CONSULTING_SIGNAL_WORDS = [
    "evaluat", "assessment", "consult", "advisory", "study", "survey",
    "technical assistance", "capacity building", "research", "monitoring",
    "review", "feasibility", "rfp", "eoi", "expression of interest",
    "terms of reference", "tpm", "third party", "independent verification",
    "baseline", "endline", "mid-term", "due diligence",
]


def has_consulting_signal(title: str, description: str = "") -> bool:
    """
    Return True if title or description contains at least one consulting-type
    keyword.  Used to guard the sector bonus (score_tender_numeric) and the
    hard-exclusion gate in opportunity_engine._is_consulting_relevant().
    """
    text = (title + " " + description).lower()
    return any(kw in text for kw in _CONSULTING_SIGNAL_WORDS)
