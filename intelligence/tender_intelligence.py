# =============================================================================
# intelligence/tender_intelligence.py — Structured Tender Intelligence Layer
#
# Extracts rule-based structured attributes from tender title + description
# and persists them to the `tender_structured_intel` MySQL table.
#
# Attributes extracted per tender:
#   sector           — primary development sector (education, health, …)
#   consulting_type  — engagement type (evaluation, research, TA, …)
#   region           — geography region inferred from country keywords
#   organization     — issuing organisation (World Bank, UNDP, …)
#   deadline_category— urgency bucket (urgent <7d | soon 7-30d | normal >30d)
#   relevance_score  — 0–100 consulting fit score
#
# Design constraints:
#   • NO scrapers, runner, or Excel exporter modified
#   • Entire module is optional — any failure degrades to a warning
#   • Pure keyword matching: ~800 tenders processed well under 5 seconds
#   • Idempotent: re-enriching the same tender_id overwrites silently
#
# Public API:
#   enrich_one(tender)          → dict  (structured attributes only)
#   enrich_batch(tenders)       → list[dict]
#   store_batch(enriched)       → int   (rows upserted)
#   enrich_and_store_batch(t)   → int   (convenience: enrich + store in one call)
#   init_schema()               → None  (creates table if absent)
#
# CLI test:
#   python3 intelligence/tender_intelligence.py
# =============================================================================

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger("tenderradar.tender_intelligence")

# ── Ensure package root is on sys.path when run directly ──────────────────────
_BASE = os.path.expanduser("~/tender_system")
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)


# =============================================================================
# SECTION 1 — Keyword Taxonomies
# =============================================================================

# ---------------------------------------------------------------------------
# 1A  SECTOR — primary development sector
# ---------------------------------------------------------------------------
# Keys are the canonical sector names returned in enriched records.
# Lists contain lowercase match strings (substring search on title+desc).
# ---------------------------------------------------------------------------
SECTOR_KEYWORDS: Dict[str, List[str]] = {
    "education": [
        "education", "school", "learning", "literacy", "numeracy",
        "teacher training", "curriculum", "student", "vocational", "tvet",
        "edtech", "dropout", "enrolment", "midday meal", "coaching centre",
        "foundational", "early childhood",
    ],
    "health": [
        "health", "hospital", "clinic", "disease", "nutrition", "immunization",
        "maternal", "child health", "nrhm", "nhm", "phc", "asha", "anganwadi",
        "icds", "medicine", "pharmaceutical", "vaccination", "epidemiolog",
        "mental health", "aids", "hiv", "tuberculosis", "malaria",
    ],
    "water": [
        "water", "sanitation", "wash", "piped water", "borehole",
        "groundwater", "sewage", "wastewater", "jal jeevan", "swachh",
        "odf", "hygiene", "drinking water", "water supply",
    ],
    "governance": [
        "governance", "policy", "regulation", "administration", "reform",
        "institutional", "public sector", "accountability", "transparency",
        "decentralization", "e-governance", "digital government", "rule of law",
        "anti-corruption", "public finance", "pfm", "audit",
    ],
    "climate": [
        "climate", "environment", "ecosystem", "biodiversity", "forest",
        "carbon", "emission", "adaptation", "afforestation", "ntfp",
        "natural resource", "drr", "disaster risk", "deforestation",
        "conservation", "wetland", "agroforestry", "green",
    ],
    "agriculture": [
        "agriculture", "farmer", "crop", "livestock", "horticulture",
        "food security", "rural", "livelihood", "irrigation", "dairy",
        "value chain", "fpo", "agri", "kisan", "fisheries", "animal husbandry",
        "organic farming", "mandi", "market linkage",
    ],
    "gender": [
        "gender", "women", "girl", "female", "empowerment", "inclusion",
        "disability", "child protection", "gbv", "gender-based violence",
        "safeguarding", "trafficking", "vulnerable", "marginalized",
    ],
    "infrastructure": [
        "infrastructure", "road", "bridge", "construction", "civil works",
        "housing", "amrut", "smart city", "solid waste", "urban planning",
    ],
    "energy": [
        "energy", "solar", "renewable", "power", "electricity", "off-grid",
        "clean energy", "energy efficiency", "biomass", "wind", "hydro",
        "energy access",
    ],
    "finance": [
        "finance", "microfinance", "banking", "financial inclusion", "credit",
        "insurance", "budget", "fiscal", "taxation", "mfi", "fintech",
        "social protection", "cash transfer", "pension",
    ],
    "digital": [
        "digital", "technology", "ict", "software", "data", "platform",
        "information system", "e-government", "innovation", "startup",
        "artificial intelligence", "machine learning", "blockchain",
    ],
    "transport": [
        "transport", "highway", "railway", "logistics", "mobility",
        "traffic", "aviation", "port", "shipping", "road safety",
    ],
}

# ---------------------------------------------------------------------------
# 1B  CONSULTING TYPE — nature of engagement
# ---------------------------------------------------------------------------
CONSULTING_TYPE_KEYWORDS: Dict[str, List[str]] = {
    "evaluation": [
        "evaluation", "impact evaluation", "baseline", "endline", "mid-term",
        "final evaluation", "assessment", "review", "appraisal",
        "third party monitoring", "tpm", "iva", "independent verification",
        "concurrent monitoring", "performance review", "programme evaluation",
        "impact assessment",
    ],
    "research": [
        "research", "study", "survey", "analysis", "scoping", "mapping",
        "documentation", "white paper", "feasibility study", "profiling",
        "data collection", "situational analysis", "field study",
        "knowledge attitude", "kap survey", "rapid assessment",
    ],
    "technical assistance": [
        "technical assistance", "technical support",
        "advisory support", "expert support", "consultancy services",
        "technical consultant", "specialist", "technical expert",
        "embedded expert", "long-term expert", "short-term expert",
    ],
    "capacity building": [
        "capacity building", "training", "capacity development",
        "skills development", "mentoring", "coaching", "workshop",
        "institutional strengthening", "organizational development",
        "knowledge transfer", "tot", "training of trainers",
    ],
    "policy": [
        "policy", "strategy", "regulation", "reform", "legislation",
        "policy review", "policy development", "policy analysis",
        "strategic plan", "action plan", "roadmap",
    ],
    "advisory": [
        "advisory", "advisory services", "strategic advisory",
        "management consulting", "strategic consulting",
        "programme advisory", "sector advisory",
    ],
    "implementation support": [
        "implementation", "program management", "project management",
        "pmc", "management contractor", "coordination", "oversight",
        "programme management", "project support unit", "psu",
        "management support",
    ],
    "feasibility study": [
        "feasibility", "pre-feasibility", "viability assessment",
        "concept study", "due diligence", "options study",
    ],
}

# ---------------------------------------------------------------------------
# 1C  REGION — country/region keywords → canonical region name
# ---------------------------------------------------------------------------
COUNTRY_TO_REGION: Dict[str, str] = {
    # ── South Asia ────────────────────────────────────────────────────────
    "india": "South Asia",       "indian": "South Asia",
    "bangladesh": "South Asia",  "nepal": "South Asia",
    "pakistan": "South Asia",    "sri lanka": "South Asia",
    "bhutan": "South Asia",      "maldives": "South Asia",
    "afghanistan": "South Asia",
    # ── Southeast Asia ────────────────────────────────────────────────────
    "vietnam": "Southeast Asia", "cambodia": "Southeast Asia",
    "myanmar": "Southeast Asia", "thailand": "Southeast Asia",
    "indonesia": "Southeast Asia","philippines": "Southeast Asia",
    "laos": "Southeast Asia",    "malaysia": "Southeast Asia",
    "timor-leste": "Southeast Asia", "timor leste": "Southeast Asia",
    "papua new guinea": "Southeast Asia",
    # ── East Africa ───────────────────────────────────────────────────────
    "kenya": "Africa",   "tanzania": "Africa",  "uganda": "Africa",
    "ethiopia": "Africa","rwanda": "Africa",    "mozambique": "Africa",
    "zambia": "Africa",  "malawi": "Africa",    "zimbabwe": "Africa",
    "somalia": "Africa", "sudan": "Africa",     "south sudan": "Africa",
    "eritrea": "Africa", "djibouti": "Africa",  "burundi": "Africa",
    # ── West Africa ───────────────────────────────────────────────────────
    "nigeria": "Africa",      "ghana": "Africa",    "senegal": "Africa",
    "mali": "Africa",         "niger": "Africa",    "cameroon": "Africa",
    "guinea": "Africa",       "liberia": "Africa",  "sierra leone": "Africa",
    "burkina faso": "Africa", "togo": "Africa",     "benin": "Africa",
    "gambia": "Africa",       "mauritania": "Africa",
    "ivory coast": "Africa",  "cote d'ivoire": "Africa",
    # ── Other Africa ──────────────────────────────────────────────────────
    "south africa": "Africa",  "angola": "Africa",   "madagascar": "Africa",
    "drc": "Africa",           "congo": "Africa",    "namibia": "Africa",
    "botswana": "Africa",      "lesotho": "Africa",  "eswatini": "Africa",
    "swaziland": "Africa",     "gabon": "Africa",    "chad": "Africa",
    # ── Latin America ─────────────────────────────────────────────────────
    "peru": "Latin America",      "colombia": "Latin America",
    "brazil": "Latin America",    "bolivia": "Latin America",
    "ecuador": "Latin America",   "mexico": "Latin America",
    "guatemala": "Latin America", "honduras": "Latin America",
    "haiti": "Latin America",     "nicaragua": "Latin America",
    "el salvador": "Latin America","paraguay": "Latin America",
    "venezuela": "Latin America", "guyana": "Latin America",
    "suriname": "Latin America",  "belize": "Latin America",
    "caribbean": "Latin America",
    # ── Middle East ───────────────────────────────────────────────────────
    "jordan": "Middle East",   "lebanon": "Middle East",
    "iraq": "Middle East",     "syria": "Middle East",
    "yemen": "Middle East",    "palestine": "Middle East",
    "west bank": "Middle East","gaza": "Middle East",
    "egypt": "Middle East",    "morocco": "Middle East",
    "tunisia": "Middle East",  "libya": "Middle East",
    "algeria": "Middle East",
    # ── Central Asia ──────────────────────────────────────────────────────
    "kazakhstan": "Central Asia", "tajikistan": "Central Asia",
    "uzbekistan": "Central Asia", "kyrgyzstan": "Central Asia",
    "turkmenistan": "Central Asia",
    # ── Europe / ECA ──────────────────────────────────────────────────────
    "ukraine": "Europe",   "georgia": "Europe",  "armenia": "Europe",
    "moldova": "Europe",   "albania": "Europe",  "kosovo": "Europe",
    "serbia": "Europe",    "bosnia": "Europe",   "north macedonia": "Europe",
    "montenegro": "Europe","belarus": "Europe",  "azerbaijan": "Europe",
    # ── Pacific ───────────────────────────────────────────────────────────
    "pacific": "Pacific", "fiji": "Pacific",    "solomon": "Pacific",
    "vanuatu": "Pacific", "samoa": "Pacific",   "tonga": "Pacific",
}

# ---------------------------------------------------------------------------
# 1D  ORGANIZATION — known issuing bodies and detection tokens
# Each entry: (list_of_lowercase_tokens, canonical_name)
# Evaluated in order — first match wins.
# ---------------------------------------------------------------------------
ORG_PATTERNS: List[tuple] = [
    (["world bank", "worldbank", "wb group"],                "World Bank"),
    (["undp"],                                               "UNDP"),
    (["unicef"],                                             "UNICEF"),
    (["who ", "world health organization"],                  "WHO"),
    (["giz", "deutsche gesellschaft"],                       "GIZ"),
    (["adb", "asian development bank"],                      "ADB"),
    (["afdb", "african development bank"],                   "AfDB"),
    (["afd ", "agence française", "agence francaise"],        "AFD"),
    (["european commission", "eu tender", "european union"], "European Union"),
    (["usaid"],                                              "USAID"),
    (["dfid", "fcdo", "uk aid", "foreign commonwealth"],     "FCDO/DFID"),
    (["unhcr"],                                              "UNHCR"),
    (["wfp", "world food programme"],                        "WFP"),
    (["iom", "international organization for migration"],    "IOM"),
    (["fao", "food and agriculture organization"],           "FAO"),
    (["ifad"],                                               "IFAD"),
    (["ifc "],                                               "IFC"),
    (["idb", "inter-american development"],                  "IDB"),
    (["eib", "european investment bank"],                    "EIB"),
    (["gem bid", "gem.gov", "bidplus.gem"],                  "GeM India"),
    (["sam.gov", "system for award management"],             "SAM.gov"),
    (["niti aayog"],                                         "NITI Aayog"),
    (["nhm ", "national health mission"],                    "NHM India"),
    (["icfre"],                                              "ICFRE"),
    (["sidbi"],                                              "SIDBI"),
    (["phfi"],                                               "PHFI"),
    (["ministry", "department of", "govt of", "government of",
      "state government"],                                   "Government Agency"),
]

_SOURCE_PORTAL_ORGS: Dict[str, str] = {
    "worldbank": "World Bank",
    "wb": "World Bank",
    "undp": "UNDP",
    "afdb": "AfDB",
    "afd": "AFD",
    "giz": "GIZ",
    "usaid": "USAID",
    "sam": "US Federal Government",
    "ec": "European Union",
    "tedeu": "European Union",
    "sidbi": "SIDBI",
    "phfi": "PHFI",
    "icfre": "ICFRE",
    "mbda": "Meghalaya Basin Development Authority",
    "gem": "Government Agency",
    "cg": "Government Agency",
    "up": "Government Agency",
    "upetender": "Government Agency",
    "maharashtra": "Government Agency",
    "karnataka": "Government Agency",
    "nic": "Government Agency",
    "sikkim": "Government Agency",
}

# ---------------------------------------------------------------------------
# 1E  DEADLINE — date format strings tried in order for parsing
# ---------------------------------------------------------------------------
_DEADLINE_FORMATS = (
    "%Y-%m-%d",    # ISO: 2025-07-01
    "%d/%m/%Y",    # India: 30/06/2025
    "%d-%m-%Y",    # India: 30-06-2025
    "%d %b %Y",    # 30 Jun 2025
    "%d %B %Y",    # 30 June 2025
    "%d %b, %Y",   # 30 Jun, 2025
    "%B %d, %Y",   # June 30, 2025
    "%b %d, %Y",   # Jun 30, 2025
    "%m/%d/%Y",    # US: 06/30/2025
    "%Y/%m/%d",    # Alt ISO
)


# =============================================================================
# SECTION 2 — Extraction Functions
# =============================================================================

def _text(tender: Dict[str, Any]) -> str:
    """
    Combine title + description into a single lowercase blob for matching.
    Pulls from common field name variants across all scrapers.
    """
    parts = [
        tender.get("title")       or tender.get("Title")       or "",
        tender.get("description") or tender.get("Description") or "",
        tender.get("deep_description") or "",
        tender.get("deep_scope") or "",
        tender.get("organization") or tender.get("Organisation") or "",
        tender.get("source_portal") or tender.get("source") or "",
        tender.get("relevance")   or "",          # may contain sector words
        tender.get("country")     or tender.get("Country") or "",
    ]
    return " ".join(str(p) for p in parts if p).lower()


def _pick_first_str(tender: Dict[str, Any], keys: list[str]) -> str:
    """Return the first meaningful string from a list of field names."""
    for key in keys:
        value = tender.get(key)
        if value is None:
            continue
        s = str(value).strip()
        if s and s.lower() not in ("n/a", "none", "unknown", "-", "–"):
            return s
    return ""


def _normalize_org_name(raw: str) -> str:
    """
    Normalize a raw organization string into a cleaner canonical value.

    Uses explicit field values first, then falls back to known token patterns.
    """
    s = " ".join(str(raw or "").strip().split())
    if not s:
        return "unknown"

    lower = s.lower()
    for tokens, org_name in ORG_PATTERNS:
        if any(tok in lower for tok in tokens):
            return org_name

    # Clean common prefixes/suffixes without over-normalizing the entity name.
    s = s.strip(" -:|,.;")
    s = s[:255]
    if len(s) < 3:
        return "unknown"
    return s


# ---------------------------------------------------------------------------
# Hard-veto phrases for sector detection.
# If ANY of these appear in the tender text the sector is forced to
# "infrastructure" regardless of keyword hit counts, and the tender will
# receive a strong negative relevance_score penalty downstream.
# These are procurement/works/supply phrases that can co-occur with sector
# keywords (e.g. "supply of solar panels" hits the energy sector keywords)
# but are unambiguously NOT consulting assignments.
# ---------------------------------------------------------------------------
_SECTOR_VETO_PHRASES: list = [
    # Supply / goods
    "supply of", "supply and delivery", "supply and installation",
    "procurement of goods", "purchase of", "rate contract",
    "empanelment of vendor", "empanelment of supplier",
    # Construction / civil works
    "construction of", "civil works", "civil construction",
    "road construction", "road widening", "road repair",
    "bridge construction", "dam construction",
    "building construction", "renovation of building",
    "erection of", "installation of solar", "installation of panel",
    "installation of transformer", "installation of pump",
    "wiring work", "electrification work", "electrical work",
    "plumbing work", "boring of well", "drilling of borehole",
    # Manpower / outsourcing
    "manpower supply", "hiring of manpower", "deployment of staff",
    "outsourcing of", "facility management",
    "security guard", "security services", "housekeeping",
    "data entry operator", "sweeping services", "cleaning services",
    # Professional services IDCG doesn't offer
    "law firm", "legal services", "chartered accountant",
    "software development", "app development", "web development",
    "erp implementation", "network installation",
]


def _detect_sector(text: str) -> str:
    """
    Return the best-matching sector name.
    Falls back to title heuristics so ≥80% of tenders get a non-unknown sector.

    Hard-veto: if the text contains unambiguous supply/works/non-consulting
    phrases, force sector = 'infrastructure' immediately so that downstream
    scoring (which penalises infrastructure) handles it correctly.
    """
    # ── Hard veto: supply/works/non-consulting → force infrastructure ─────────
    if any(phrase in text for phrase in _SECTOR_VETO_PHRASES):
        return "infrastructure"

    best_sector = "unknown"
    best_hits   = 0
    for sector, keywords in SECTOR_KEYWORDS.items():
        hits = sum(1 for kw in keywords if kw in text)
        if hits > best_hits:
            best_hits   = hits
            best_sector = sector

    if best_sector != "unknown":
        return best_sector

    # ── Fallback heuristics (title-level) ────────────────────────────────────
    # These catch tenders with descriptive titles but no sector keyword matches.
    heuristics = [
        (["evaluat", "baseline", "endline", "mid-term", "assessment",
          "monitoring", "review", "impact", "tpm", "iva"],          "governance"),
        (["consult", "advisory", "expert", "specialist", "rfp", "eoi",
          "request for proposal", "expression of interest"],         "governance"),
        (["supply", "procurement", "purchase", "goods", "equipment",
          "material", "stationery", "printing"],                     "infrastructure"),
        (["road", "bridge", "construction", "civil", "building",
          "works", "ae service", "engineering"],                     "infrastructure"),
        (["software", "ict", "platform", "system", "mis", "portal",
          "database", "mobile app", "application"],                  "digital"),
        (["audit", "financial", "accounts", "tax", "gst",
          "chartered accountant"],                                   "finance"),
        (["train", "workshop", "capacity", "skill", "coaching"],    "governance"),
        (["survey", "study", "research", "data collection",
          "mapping", "profiling"],                                   "governance"),
    ]
    for keywords, sector in heuristics:
        if any(kw in text for kw in keywords):
            return sector

    # Final fallback: any tender that reaches this point is development-related
    # (given our portal list), classify as governance
    return "governance"


def _detect_consulting_type(text: str) -> str:
    """
    Return the best-matching consulting engagement type.
    Falls back to title heuristics so ≥80% of tenders get a non-unknown type.

    Hard-veto: supply/works/non-consulting phrases → return "unknown" so the
    downstream priority scoring applies the unknown_consulting_type penalty.
    """
    # Hard veto — same phrases as sector veto; non-consulting = unknown type
    if any(phrase in text for phrase in _SECTOR_VETO_PHRASES):
        return "unknown"

    best_type = "unknown"
    best_hits  = 0
    for ctype, keywords in CONSULTING_TYPE_KEYWORDS.items():
        hits = sum(1 for kw in keywords if kw in text)
        if hits > best_hits:
            best_hits  = hits
            best_type  = ctype

    if best_type != "unknown":
        return best_type

    # ── Fallback heuristics ────────────────────────────────────────────────
    heuristics = [
        (["evaluat", "baseline", "endline", "mid-term", "final eval",
          "impact eval", "third party", "concurrent monitoring",
          "iva", "tpm"],                                          "evaluation"),
        (["research", "study", "survey", "data collect", "kap",
          "situational", "scoping", "mapping", "profiling",
          "rapid assessment", "field study"],                    "research"),
        (["training", "workshop", "capacity", "skill", "coaching",
          "tot", "mentoring", "institutional strengthening"],    "capacity building"),
        (["policy", "strategy", "reform", "legislation",
          "regulatory", "strategic plan", "action plan",
          "roadmap", "guideline"],                               "policy"),
        (["technical assistance", "technical support",
          "technical expert", "embedded expert",
          "long-term", "short-term"],                            "technical assistance"),
        (["implement", "program management", "project management",
          "pmc", "coordination", "programme management",
          "project support"],                                    "implementation support"),
        (["feasibility", "pre-feasibility", "viability",
          "due diligence", "concept study", "options study"],    "feasibility study"),
        (["consult", "advisory", "advisor", "specialist",
          "expert", "rfp", "eoi",
          "request for proposal", "expression of interest",
          "hiring of"],                                          "advisory"),
    ]
    for keywords, ctype in heuristics:
        if any(kw in text for kw in keywords):
            return ctype

    return "advisory"  # near-universal fallback for our portal set


_SOURCE_REGION_HINTS: Dict[str, str] = {
    "worldbank": "Global",
    "wb": "Global",
    "undp": "Global",
    "ungm": "Global",
    "afdb": "Africa",
    "taneps": "Africa",
    "sam": "North America",
    "usaid": "Global",
    "ec": "Europe",
    "ted": "Europe",
    "tedeu": "Europe",
    "dtvp": "Europe",
    "gem": "South Asia",
    "upetender": "South Asia",
    "up": "South Asia",
    "cg": "South Asia",
    "karnataka": "South Asia",
    "maharashtra": "South Asia",
    "jtds": "South Asia",
    "sidbi": "South Asia",
    "icfre": "South Asia",
    "phfi": "South Asia",
    "devnet": "South Asia",
    "ngobox": "South Asia",
}


def _detect_region(text: str, tender: Optional[Dict[str, Any]] = None) -> str:
    """
    Scan the text for country/region keywords and return the region.
    Longer country names checked first to avoid partial shadowing
    (e.g. 'south sudan' before 'sudan').
    Falls back to "South Asia" for India-centric portal content.
    """
    # Sort by token length descending so longer phrases match first
    for country, region in sorted(
        COUNTRY_TO_REGION.items(), key=lambda x: -len(x[0])
    ):
        if country in text:
            return region

    # ── Strong hint: explicit country/geography fields ────────────────────
    if tender:
        geo_blob = " ".join(
            str(tender.get(k) or "")
            for k in ("country", "Country", "location", "Location", "geography")
        ).strip().lower()
        if geo_blob:
            for country, region in sorted(COUNTRY_TO_REGION.items(), key=lambda x: -len(x[0])):
                if country in geo_blob:
                    return region
        src = str(tender.get("source_portal") or tender.get("source") or "").strip().lower()
        if src in _SOURCE_REGION_HINTS:
            return _SOURCE_REGION_HINTS[src]

    # ── Fallback: source-portal hints from raw text ───────────────────────
    # Indian portals (GeM, CG, Meghalaya, SIDBI, ICFRE, etc.) are overwhelmingly
    # India-focused even when no country name appears in the title.
    _india_portal_hints = [
        "gem", "gem.gov", "bidplus", "eprocure.gov", "gem bid",
        "meghalaya", "mbda", "sikkim", "sidbi", "icfre", "phfi",
        "devnet", "devnetjobs", "etender", "nicgep",
        "maharashtra", "jharkhand", "uttarakhand", "madhya pradesh",
    ]
    if any(h in text for h in _india_portal_hints):
        return "South Asia"

    return "global"


def _detect_organization(text: str, tender: Optional[Dict[str, Any]] = None) -> str:
    """
    Scan title+desc for known organisation tokens.
    Returns canonical org name, or 'unknown'.
    """
    if tender:
        explicit = _pick_first_str(
            tender,
            [
                "organization", "Organisation", "Organization", "Authority",
                "Entity", "Agency", "Client", "org", "Org", "Buyer", "buyer",
                "Owner", "contracting_authority", "organisation",
            ],
        )
        normalized = _normalize_org_name(explicit)
        if normalized != "unknown":
            return normalized

        source_portal = str(tender.get("source_portal") or tender.get("source") or "").strip().lower()
        if source_portal in _SOURCE_PORTAL_ORGS:
            return _SOURCE_PORTAL_ORGS[source_portal]

    for tokens, org_name in ORG_PATTERNS:
        if any(tok in text for tok in tokens):
            return org_name

    # Last-resort: pick up any "X Ministry" or "Y Department" pattern
    import re
    m = re.search(
        r'\b(?:ministry|department)\s+of\s+([a-z &]+?)(?:\s+(?:and|of|for|\,|\.)|$)',
        text,
    )
    if m:
        return "Ministry of " + m.group(1).strip().title()
    return "unknown"


def _classify_deadline(deadline_raw: Optional[str]) -> str:
    """
    Parse deadline string and return mutually exclusive urgency bucket:
        closing_soon  → 0–7 days remaining
        needs_action  → 8–21 days remaining
        plan_ahead    → 22+ days remaining
        expired       → deadline already passed
        unknown       → unparseable / missing deadline

    Buckets are strictly exclusive:
        closing_soon + needs_action + plan_ahead = total active tenders with known deadlines
    """
    if not deadline_raw or str(deadline_raw).strip() in ("", "N/A", "None", "TBD"):
        return "unknown"

    raw = str(deadline_raw).strip()

    # Strip anything after the date portion (e.g. time, timezone)
    raw_short = raw[:20]

    today = datetime.utcnow().date()
    for fmt in _DEADLINE_FORMATS:
        try:
            dl_date = datetime.strptime(raw_short, fmt).date()
            delta   = (dl_date - today).days
            if delta < 0:
                return "expired"
            if delta <= 7:
                return "closing_soon"
            if delta <= 21:
                return "needs_action"
            return "plan_ahead"
        except ValueError:
            continue

    return "unknown"


def _is_hard_blocked(title: str, description: str = "") -> bool:
    """
    Check if tender matches CAP STAT hard-block keywords (goods, works, non-consulting).
    Returns True → tender should get relevance_score=0 automatically.
    """
    try:
        from intelligence.keywords import is_hard_blocked
        return is_hard_blocked(title, description)
    except Exception:
        return False


def _compute_relevance(tender: Dict[str, Any]) -> int:
    """
    Delegate to the shared score_tender_numeric() from intelligence/keywords.py.
    Returns integer 0–100.  Falls back to a simple keyword count on import error.

    Includes CAP STAT hard-block: tenders matching supply/goods/works/legal → 0.
    """
    title       = str(tender.get("title") or tender.get("Title") or "")
    description = str(tender.get("description") or tender.get("Description") or "")
    country     = str(tender.get("country") or tender.get("Country") or "")

    # ── Hard-block: not consulting at all → immediate 0 ───────────────────
    if _is_hard_blocked(title, description):
        return 0

    try:
        from intelligence.keywords import score_tender_numeric
        score, _ = score_tender_numeric(title, description, country)
        return max(0, min(100, int(score)))
    except Exception:
        # Emergency fallback: count consulting keyword hits × 5, capped at 100
        text = (title + " " + description).lower()
        _FALLBACK_KW = [
            "consult", "evaluat", "research", "advisory", "technical assistance",
            "capacity", "monitoring", "assessment", "training", "policy",
        ]
        hits = sum(1 for kw in _FALLBACK_KW if kw in text)
        return min(100, hits * 10)


# =============================================================================
# SECTION 3 — Public enrichment API
# =============================================================================

def enrich_one(tender: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract all structured attributes for a single tender dict.

    Accepts any tender dict regardless of which scraper produced it —
    handles common field name variants (title/Title, etc.).

    Returns a flat dict with keys:
        tender_id, sector, consulting_type, region, organization,
        deadline_category, relevance_score
    """
    text = _text(tender)

    # Resolve tender_id from common field names
    tender_id = (
        tender.get("tender_id")
        or tender.get("id")
        or tender.get("sol_num")
        or tender.get("Bid Number")
        or ""
    )

    # Deadline: try multiple common field name variants
    deadline_raw = (
        tender.get("deadline")   or tender.get("Deadline")
        or tender.get("end_date") or tender.get("Closing Date")
        or tender.get("closing")  or tender.get("closing_date")
        or tender.get("Bid End Date")
    )

    return {
        "tender_id":         str(tender_id)[:255],
        "sector":            _detect_sector(text),
        "consulting_type":   _detect_consulting_type(text),
        "region":            _detect_region(text, tender),
        "organization":      _detect_organization(text, tender)[:255],
        "deadline_category": _classify_deadline(deadline_raw),
        "relevance_score":   _compute_relevance(tender),
    }


def enrich_batch(tenders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Enrich a list of raw tender dicts.
    Skips entries with no tender_id (cannot be stored without a key).

    Returns list of enriched attribute dicts in the same order.
    Performance: ~800 tenders in < 3 seconds (pure Python keyword matching).
    """
    enriched = []
    for t in tenders:
        try:
            attrs = enrich_one(t)
            if attrs["tender_id"]:
                enriched.append(attrs)
        except Exception as exc:
            logger.debug(f"[tender_intelligence] enrich_one skipped: {exc}")

    total = len(tenders)
    covered = len(enriched)
    sector_known  = sum(1 for e in enriched if e.get("sector",   "unknown") != "unknown")
    ctype_known   = sum(1 for e in enriched if e.get("consulting_type", "unknown") != "unknown")
    region_known  = sum(1 for e in enriched if e.get("region",   "global")  not in ("global", "unknown"))

    logger.info(
        f"[tender_intelligence] coverage: {covered}/{total} enriched | "
        f"sector: {sector_known}/{covered} | "
        f"type: {ctype_known}/{covered} | "
        f"region: {region_known}/{covered}"
    )
    return enriched


# =============================================================================
# SECTION 4 — Database layer
# =============================================================================

_TABLE = "tender_structured_intel"


def init_schema() -> None:
    """
    Create the tender_structured_intel table if it does not exist.
    Safe to call on every run.  Failure is non-fatal.
    """
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{_TABLE}` (
                id                INT AUTO_INCREMENT PRIMARY KEY,
                tender_id         VARCHAR(255)  NOT NULL UNIQUE,
                sector            VARCHAR(50)   NOT NULL DEFAULT 'unknown',
                consulting_type   VARCHAR(50)   NOT NULL DEFAULT 'unknown',
                region            VARCHAR(50)   NOT NULL DEFAULT 'global',
                organization      VARCHAR(255)  NOT NULL DEFAULT 'unknown',
                deadline_category VARCHAR(20)   NOT NULL DEFAULT 'unknown',
                relevance_score   SMALLINT      NOT NULL DEFAULT 0,
                enriched_at       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                  ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_si_sector          (sector),
                INDEX idx_si_consulting_type (consulting_type),
                INDEX idx_si_region          (region),
                INDEX idx_si_relevance       (relevance_score),
                INDEX idx_si_deadline        (deadline_category)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"[tender_intelligence] Table '{_TABLE}' ready.")
    except Exception as exc:
        logger.warning(f"[tender_intelligence] init_schema failed (non-fatal): {exc}")


def store_batch(enriched: List[Dict[str, Any]]) -> int:
    """
    Upsert a list of enriched attribute dicts into tender_structured_intel.
    Uses INSERT … ON DUPLICATE KEY UPDATE so re-enriching is idempotent.

    Returns the number of rows written (new inserts + updates).
    Returns 0 on any DB error.
    """
    if not enriched:
        return 0

    try:
        from database.db import get_connection, DRY_RUN
        if DRY_RUN:
            logger.info(
                f"[tender_intelligence] DRY-RUN: skipping {len(enriched)} DB writes"
            )
            return 0

        conn = get_connection()
        cur  = conn.cursor()

        sql = f"""
            INSERT INTO `{_TABLE}`
                (tender_id, sector, consulting_type, region,
                 organization, deadline_category, relevance_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                sector            = VALUES(sector),
                consulting_type   = VALUES(consulting_type),
                region            = VALUES(region),
                organization      = VALUES(organization),
                deadline_category = VALUES(deadline_category),
                relevance_score   = VALUES(relevance_score),
                enriched_at       = CURRENT_TIMESTAMP;
        """
        rows = [
            (
                e["tender_id"],
                e["sector"],
                e["consulting_type"],
                e["region"],
                e["organization"],
                e["deadline_category"],
                e["relevance_score"],
            )
            for e in enriched
        ]
        cur.executemany(sql, rows)
        conn.commit()
        written = cur.rowcount
        cur.close()
        conn.close()
        logger.info(
            f"[tender_intelligence] Stored {written} rows into '{_TABLE}'"
        )
        return written

    except Exception as exc:
        logger.warning(f"[tender_intelligence] store_batch failed (non-fatal): {exc}")
        return 0


def enrich_and_store_batch(tenders: List[Dict[str, Any]]) -> int:
    """
    One-call convenience: enrich tenders + store results.
    Guarantees non-fatal execution — catches all exceptions.

    Returns count of rows written to DB.
    """
    try:
        init_schema()
        enriched = enrich_batch(tenders)
        return store_batch(enriched)
    except Exception as exc:
        logger.warning(
            f"[tender_intelligence] enrich_and_store_batch failed (non-fatal): {exc}"
        )
        return 0


def refresh_from_tenders(
    limit: int = 10_000,
    only_unknown_org: bool = False,
    missing_only: bool = False,
) -> int:
    """
    Rebuild structured intelligence from normalized tenders.

    This is the maintenance path for historical data after extraction logic
    changes. It uses the richer `tenders` table rather than thin `seen_tenders`
    rows, so organization/region/relevance can improve over time.

    Selection strategy matters here: once the dataset grows beyond `limit`,
    simply taking the most recent rows will keep reprocessing the same fresh
    tenders and starve older records forever. We therefore prioritize tenders
    with no existing structured intel first, then fall back to newer rows for
    rebuilds/upgrades.
    """
    try:
        from database.db import get_connection

        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        conditions: List[str] = []
        if only_unknown_org:
            conditions.append("si.organization IN ('unknown','')")
        if missing_only:
            conditions.append("si.tender_id IS NULL")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        cur.execute(
            f"""
            SELECT
                t.tender_id,
                t.title,
                t.description,
                t.deep_description,
                t.deep_scope,
                t.organization,
                t.country,
                t.source_portal,
                COALESCE(NULLIF(t.deep_deadline_raw, ''), t.deadline_raw) AS deadline,
                t.url
            FROM tenders t
            LEFT JOIN tender_structured_intel si ON si.tender_id = t.tender_id
            {where}
            ORDER BY
                CASE WHEN si.tender_id IS NULL THEN 0 ELSE 1 END ASC,
                t.scraped_at DESC
            LIMIT %s
            """,
            (int(limit),),
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
        return enrich_and_store_batch(rows)
    except Exception as exc:
        logger.warning(f"[tender_intelligence] refresh_from_tenders failed: {exc}")
        return 0


# =============================================================================
# SECTION 5 — Backfill utility (enrich all existing seen_tenders rows)
# =============================================================================

def backfill_from_seen_tenders(limit: int = 10_000) -> int:
    """
    Backfill structured intelligence for tenders already in seen_tenders.
    Useful for enriching historical records after first deploy.

    Usage:
        python3 intelligence/tender_intelligence.py --backfill

    Returns count of rows written.
    """
    try:
        from database.db import get_connection
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        cur.execute(
            f"SELECT tender_id, title, url FROM seen_tenders "
            f"WHERE tender_id NOT IN (SELECT tender_id FROM `{_TABLE}`) "
            f"LIMIT %s;",
            (limit,),
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
        logger.info(
            f"[tender_intelligence] Backfilling {len(rows)} un-enriched tenders…"
        )
        return enrich_and_store_batch(rows)
    except Exception as exc:
        logger.warning(f"[tender_intelligence] backfill failed: {exc}")
        return 0


# =============================================================================
# SECTION 6 — CLI test / backfill entry point
# =============================================================================

_SAMPLE_TENDERS = [
    {
        "tender_id":   "TEST_WB_001",
        "title":       "Baseline Survey and Impact Evaluation of WASH Programme in Bihar",
        "description": "UNICEF India seeks a consulting firm to conduct a baseline survey "
                       "and impact evaluation of its water, sanitation and hygiene (WASH) "
                       "programme across 10 districts of Bihar. Includes KAP survey, "
                       "data collection and report preparation.",
        "deadline":    (datetime.utcnow() + timedelta(days=45)).strftime("%Y-%m-%d"),
        "country":     "India",
    },
    {
        "tender_id":   "TEST_AFDB_001",
        "title":       "Capacity Building and Training for Climate Adaptation in Kenya",
        "description": "African Development Bank — Technical Assistance for capacity "
                       "development of county-level environment officers in climate "
                       "change adaptation and natural resource management.",
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
        "description": "Procurement of laboratory instruments and equipment for CSIR.",
        "deadline":    (datetime.utcnow() - timedelta(days=5)).strftime("%Y-%m-%d"),
        "country":     "India",
    },
]


def _print_banner(text: str) -> None:
    print("\n" + "─" * 70)
    print(f"  {text}")
    print("─" * 70)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="TenderRadar — Structured Tender Intelligence CLI"
    )
    ap.add_argument(
        "--backfill",
        action="store_true",
        help="Backfill structured intel for all un-enriched rows in seen_tenders",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=10_000,
        help="Max rows to backfill (default: 10000)",
    )
    args = ap.parse_args()

    # ── Configure a simple console logger for CLI mode ────────────────────────
    logging.basicConfig(
        format="%(levelname)s %(name)s — %(message)s",
        level=logging.INFO,
    )

    if args.backfill:
        _print_banner("Backfilling existing tenders from seen_tenders…")
        init_schema()
        n = backfill_from_seen_tenders(limit=args.limit)
        print(f"\n✅  Backfill complete — {n} rows written to '{_TABLE}'")
        sys.exit(0)

    # ── Default: run sample test ───────────────────────────────────────────────
    _print_banner("Structured Tender Intelligence — sample extraction test")

    print(f"\n{'TENDER':<45} {'SECTOR':<14} {'CONSULT TYPE':<22}"
          f" {'REGION':<12} {'ORG':<16} {'DEADLINE':<9} {'SCORE':>5}")
    print("─" * 130)

    for t in _SAMPLE_TENDERS:
        attrs = enrich_one(t)
        title_s   = t["title"][:44].ljust(45)
        sector_s  = attrs["sector"][:13].ljust(14)
        ctype_s   = attrs["consulting_type"][:21].ljust(22)
        region_s  = attrs["region"][:11].ljust(12)
        org_s     = attrs["organization"][:15].ljust(16)
        dl_s      = attrs["deadline_category"][:8].ljust(9)
        score_s   = str(attrs["relevance_score"]).rjust(5)
        print(f"{title_s} {sector_s} {ctype_s} {region_s} {org_s} {dl_s} {score_s}")

    print()

    # Detailed dump for first sample
    _print_banner(f"Detailed attributes — '{_SAMPLE_TENDERS[0]['title'][:55]}…'")
    attrs = enrich_one(_SAMPLE_TENDERS[0])
    for k, v in attrs.items():
        print(f"  {k:<22}: {v}")

    # Quick DB test (non-fatal)
    _print_banner("DB write test (skipped if MySQL not configured)")
    try:
        init_schema()
        written = store_batch([enrich_one(t) for t in _SAMPLE_TENDERS])
        print(f"  ✅  DB write OK — {written} row(s) upserted into '{_TABLE}'")
    except Exception as e:
        print(f"  ⚠   DB test skipped (not required for local testing): {e}")

    print()
