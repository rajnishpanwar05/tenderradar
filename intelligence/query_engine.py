# =============================================================================
# intelligence/query_engine.py — Natural Language Query + Semantic Search
#
# Responsibilities:
#   1. Accept user natural language query
#   2. Extract lightweight filters (sector, region, priority hints)
#   3. Run vector similarity search via ChromaDB
#   4. Enrich results with DB intel (priority_score, bid_fit_score, sector, etc.)
#   5. Compute composite ranking score:
#      0.4 × semantic + 0.25 × priority + 0.2 × fit + 0.15 × lexical evidence
#   6. Apply filter boosts and return top-N results
#
# Fail-open: if vector store is empty or unavailable, falls back to pure DB query.
# Performance target: < 500ms end-to-end (model is lazy-loaded and cached).
# =============================================================================

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import date as _date_type
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger("tenderradar.query_engine")

# =============================================================================
# IDCG Capability Profile — what IDCG DOES and DOES NOT do
# Used to boost/penalise results so irrelevant sectors don't pollute searches.
# =============================================================================

# Sectors IDCG actively bids — strong boost
_IDCG_STRONG_SECTORS = {
    "evaluation_monitoring", "research", "governance", "education",
    "health", "agriculture", "water_sanitation", "gender_inclusion",
    "environment", "social_protection", "finance",
}

# Sectors IDCG NEVER works in — hard penalty (not excluded, but pushed way down)
# Electricity generation, road construction, IT systems, legal, supply chain
_IDCG_EXCLUDED_SECTORS = {
    "energy", "infrastructure", "urban_development", "communications",
    "circular_economy", "tourism",
}

# Title/description keywords that indicate a tender is clearly NOT for IDCG
# (IT software, construction, legal, supply, manufacturing)
_IDCG_EXCLUDED_KEYWORDS = [
    "supply of equipment", "procurement of goods", "civil works", "construction of",
    "road construction", "bridge construction", "electrical works", "solar panel installation",
    "power plant", "transmission line", "software development", "it system",
    "legal services", "legal advisory", "law firm", "advocate",
    "manufacturing", "fabrication", "printing", "catering", "security services",
    "vehicle", "fleet", "ambulance", "medical equipment supply",
    "furniture", "stationery", "cleaning services",
    # NIC portal plantation / maintenance works (Karnataka, CG, UP)
    "monsoon raising", "roadside plantation", "block plantation", "raising of plantation",
    "raising of monsoon", "maintenance of plantation", "maintenance of 1st year",
    "maintenance of 2nd year", "maintenance of 3rd year",
    # Pure infrastructure works that slip past sector filter
    "laying of pipeline", "repair of road", "repair building", "flooring works",
    "painting works", "whitewashing", "bore well", "overhead tank",
    "operation and maintenance", "operatation and maintance", "stp",
]

# Regex pattern for NIC reference-number-only titles (no useful text)
# e.g. "E-TENDER/TKA/29/2025-26", "E-TENDER NO.05/2025-26_03", "TENDER NO. 123/2025"
import re as _re
_NIC_REF_TITLE_PATTERN = _re.compile(
    r"^(e-?tender[\s/]|tender[\s/]*no\.?\s*\d|ref[\s/]*no\.?\s*\d)",
    _re.IGNORECASE
)

# Known portal names in the database
_KNOWN_PORTALS = [
    "World Bank", "GeM", "DevNet", "CG", "GIZ", "UNDP", "MBDA",
    "AFD", "IUCN", "ICFRE", "JTDS", "NGOBox", "AfDB", "UNGM",
    "TANEPS", "TED-EU", "DTVP", "Welthungerhilfe",
    "Karnataka", "Maharashtra", "UP eTender",
]
_PORTAL_ALIASES: dict[str, str] = {
    "gem": "GeM", "government e marketplace": "GeM",
    "world bank": "World Bank", "wb": "World Bank",
    "undp": "UNDP", "ungm": "UNGM",
    "giz": "GIZ", "afdb": "AfDB", "african development bank": "AfDB",
    "ted": "TED-EU", "european": "TED-EU", "eu": "TED-EU",
    "devnet": "DevNet", "ngobox": "NGOBox",
    "afd": "AFD", "iucn": "IUCN", "icfre": "ICFRE",
    "karnataka": "Karnataka",
    "maharashtra": "Maharashtra",
    "cg": "CG", "chhattisgarh": "CG",
    "up": "UP eTender", "up e tender": "UP eTender", "upetender": "UP eTender",
}

_INFRA_PORTAL_MARKERS = (
    "karnataka",
    "maharashtra",
    "upetender",
    "up etender",
    "up e-tender",
    "up e tender",
    "up etenders",
    "cg",
    "chhattisgarh",
)

_INFRA_INTENT_TOKENS = (
    "civil work", "civil works", "construction", "infrastructure",
    "road", "bridge", "pipeline", "plantation", "boq", "epc",
)

_CONSULTING_INTENT_TOKENS = (
    "consulting", "consultancy", "advisory", "technical assistance",
    "evaluation", "monitoring", "research", "assessment", "study",
    "capacity building", "mel", "m&e", "tpm",
)


def _normalize_portal_name(value: str) -> str:
    """Normalize portal labels to canonical names used by filters."""
    s = (value or "").strip().lower()
    if not s:
        return ""
    if s in _PORTAL_ALIASES:
        return _PORTAL_ALIASES[s]
    # Match aliases within longer labels like "gem india" / "world bank tenders"
    for alias, canonical in _PORTAL_ALIASES.items():
        if alias in s:
            return canonical
    # Normalize known canonical entries (case-insensitive)
    for p in _KNOWN_PORTALS:
        if p.lower() == s:
            return p
    return value.strip()


def _matches_requested_portal(source_value: str, requested: list[str]) -> bool:
    src = _normalize_portal_name(str(source_value or ""))
    req_norm = {_normalize_portal_name(str(p)) for p in (requested or []) if str(p).strip()}
    if not src or not req_norm:
        return False
    return src in req_norm


def _is_infra_portal(source_site: str) -> bool:
    s = (source_site or "").strip().lower()
    if not s:
        return False
    return any(m in s for m in _INFRA_PORTAL_MARKERS)


def _query_wants_infra(query: str, filters: dict[str, Any]) -> bool:
    q = (query or "").lower()
    if any(tok in q for tok in _INFRA_INTENT_TOKENS):
        return True
    for p in (filters.get("source_portals") or []):
        if _is_infra_portal(str(p)):
            return True
    return False


def _query_is_consulting_first(query: str) -> bool:
    q = (query or "").lower()
    return any(tok in q for tok in _CONSULTING_INTENT_TOKENS)


def _query_tokens(query: str) -> set[str]:
    """Tokenize user query into a small normalized token set for lexical matching."""
    stopwords = {
        "the", "a", "an", "for", "of", "and", "or", "to", "in", "on", "at", "from",
        "with", "show", "find", "give", "list", "top", "best", "tenders", "tender",
        "opportunities", "opportunity", "need", "want", "me", "us", "this", "that",
    }
    toks = re.findall(r"[a-z0-9][a-z0-9/-]{1,}", (query or "").lower())
    return {t for t in toks if t not in stopwords and len(t) >= 3}


def _lexical_match_score(query_toks: set[str], text: str) -> float:
    """Return lexical overlap score in [0,1] using token intersection over query tokens."""
    if not query_toks:
        return 0.0
    doc_toks = set(re.findall(r"[a-z0-9][a-z0-9/-]{1,}", (text or "").lower()))
    if not doc_toks:
        return 0.0
    overlap = len(query_toks & doc_toks)
    return min(1.0, overlap / max(1, len(query_toks)))


def _specificity_boosted_similarity_floor(query: str, filters: dict[str, Any]) -> float:
    """
    Dynamic similarity floor:
      - specific query (more constraints): keep higher floor
      - broad query (few constraints): allow lower floor to avoid empty answers
    """
    qtok = _query_tokens(query)
    constraints = 0
    if filters.get("sectors"):
        constraints += 1
    if filters.get("regions"):
        constraints += 1
    if filters.get("source_portals"):
        constraints += 1
    if filters.get("posted_since_days"):
        constraints += 1
    if filters.get("closing_soon"):
        constraints += 1
    if filters.get("priority_hint"):
        constraints += 1

    if len(qtok) >= 6 or constraints >= 2:
        return 0.25
    return 0.15


def _safe_seen_date(raw_date: Any) -> _date_type | None:
    """Parse DB date field safely."""
    if not raw_date:
        return None
    try:
        if isinstance(raw_date, _date_type):
            return raw_date
        return datetime.strptime(str(raw_date)[:10], "%Y-%m-%d").date()
    except Exception:
        return None

# =============================================================================
# Filter keyword maps (lightweight NLP — no LLM required)
# =============================================================================

# sector slug → trigger words in user query
_SECTOR_KEYWORDS: dict[str, list[str]] = {
    "evaluation_monitoring": [
        "monitoring", "evaluation", "m&e", "assessment", "baseline",
        "mid-term", "endline", "impact", "tpm", "iva", "verification",
    ],
    "education": [
        "education", "school", "learning", "training", "skill", "literacy",
        "vocational", "curriculum", "teacher", "student",
    ],
    "health": [
        "health", "medical", "hospital", "nutrition", "healthcare",
        "reproductive", "maternal", "vaccination", "immunization",
    ],
    "environment": [
        "environment", "climate", "ecology", "forest", "biodiversity",
        "carbon", "emission", "adaptation", "esia",
    ],
    "water_sanitation": [
        "water", "sanitation", "wash", "hygiene", "drinking water",
        "wastewater", "sewage", "jal jeevan",
    ],
    "governance": [
        "governance", "policy", "reform", "transparency", "accountability",
        "anti-corruption", "rule of law",
    ],
    "agriculture": [
        "agriculture", "farming", "livelihood", "rural", "farmer",
        "irrigation", "horticulture", "fisheries",
    ],
    "gender_inclusion": [
        "gender", "women", "inclusion", "empowerment", "equality", "minority",
    ],
    "research": [
        "research", "study", "documentation", "analysis", "scoping",
        "feasibility", "mapping",
    ],
    "finance": [
        "finance", "audit", "accounting", "financial", "fiduciary", "budget",
    ],
    "urban_development": [
        "urban", "city", "municipal", "metro", "housing", "smart city",
    ],
    "energy": [
        "energy", "solar", "renewable", "electricity", "clean energy",
        "off-grid", "biomass",
    ],
    "infrastructure": [
        "infrastructure", "road", "bridge", "construction", "civil works",
    ],
    "social_protection": [
        "social protection", "poverty", "welfare", "cash transfer",
        "disability", "elderly",
    ],
}

# region label → trigger words
_REGION_KEYWORDS: dict[str, list[str]] = {
    "Africa": [
        "africa", "african", "east africa", "west africa", "sub-saharan",
        "kenya", "tanzania", "ethiopia", "ghana", "nigeria", "mozambique",
        "rwanda", "uganda",
    ],
    "South Asia": [
        "india", "south asia", "south asian", "bangladesh", "nepal",
        "pakistan", "sri lanka",
    ],
    "Southeast Asia": [
        "southeast asia", "cambodia", "myanmar", "vietnam", "indonesia",
        "philippines",
    ],
    "Global": [
        "global", "international", "worldwide", "multi-country",
    ],
    "Latin America": [
        "latin america", "south america", "central america", "caribbean",
    ],
    "Middle East": [
        "middle east", "mena", "jordan", "lebanon",
    ],
    "Europe": [
        "europe", "european", "balkans",
    ],
}

# Priority / urgency trigger phrases
_HIGH_PRIORITY_HINTS = [
    "high priority", "urgent", "top priority", "critical",
    "important", "high-priority", "priority tenders",
]
_CLOSING_SOON_HINTS = [
    "closing soon", "due soon", "deadline near", "urgent deadline",
    "closing this week", "closing this month", "soon closing",
]


# =============================================================================
# Filter extraction
# =============================================================================

def extract_filters(query: str) -> dict[str, Any]:
    """
    Parse lightweight filters from a natural language query string using the
    shared LLM client selection (Gemini first, OpenAI fallback).
    Falls back to regex heuristics if no hosted LLM is available.
    """
    # Source strictness: user explicitly asks to restrict to one portal.
    strict_source = bool(re.search(r"\b(only|strictly|exclusively)\b", (query or "").lower()))

    # ── Attempt hosted LLM filter parsing first ────────────────────────────
    try:
        from intelligence.openai_utils import get_llm_client, note_llm_error, throttle_openai
        client, model = get_llm_client()
        if client:
            avail_sectors = list(_SECTOR_KEYWORDS.keys())
            avail_regions = list(_REGION_KEYWORDS.keys())
            prompt = f'''You are a JSON extraction engine. Parse the user query into structured search filters.

RULES:
1. "explicit_limit": If the user says "top 5", "best 10", "give me 3", "first 20", or any number indicating how many results they want, set this to that integer. Otherwise null.
2. "semantic_search_term": Extract ONLY the core subject keywords for vector search. Remove all filler like "give me", "I need", "find", "show", "top N", "best", "most urgent", "from GEM", "from today", "this week", etc. Example: "give me top 5 urgent water tenders in Africa from GEM" → "water tenders". If the query is generic like "top 10 tenders from today in india", use "consulting advisory tenders". NEVER leave this empty.
3. "sectors": Match to slugs from this list ONLY: {avail_sectors}. Use [] if no match.
4. "regions": Match to labels from this list ONLY: {avail_regions}. Use [] if no match. Map sub-regions to the closest parent (e.g. "East Africa" → "Africa", "India" → "South Asia").
5. "priority_hint": Set to "high" if the user mentions urgency, priority, importance, or critical. Otherwise null.
6. "closing_soon": true if user mentions deadlines, closing soon, due soon, expiring. Otherwise false.
7. "source_portals": If user mentions a specific data source/portal like "GEM", "World Bank", "UNDP", "UNGM", etc., return those names. Available portals: {_KNOWN_PORTALS}. Use [] if not specified.
8. "posted_since_days": If user mentions a recency window, return the number of days back to filter by date_first_seen. Examples: "today" or "from today" → 1, "this week" or "last 7 days" → 7, "this month" or "last 30 days" → 30, "recent" → 14. Otherwise null.

Return ONLY valid JSON. No explanations.

Example:
Query: "show me the top 5 urgent water sanitation tenders from GEM India"
Answer: {{"sectors":["water_sanitation"],"regions":["South Asia"],"priority_hint":"high","closing_soon":false,"explicit_limit":5,"semantic_search_term":"water sanitation tenders","source_portals":["GeM"],"posted_since_days":null}}

Example:
Query: "give me top 10 tenders from today in india"
Answer: {{"sectors":[],"regions":["South Asia"],"priority_hint":null,"closing_soon":false,"explicit_limit":10,"semantic_search_term":"consulting advisory tenders India","source_portals":[],"posted_since_days":1}}

Query: "{query}"
Answer:'''
            throttle_openai()
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                max_tokens=200,
                temperature=0,
            )
            structured = json.loads(resp.choices[0].message.content or "{}")
            return {
                "sectors":            structured.get("sectors", []),
                "regions":            structured.get("regions", []),
                "priority_hint":      structured.get("priority_hint"),
                "closing_soon":       bool(structured.get("closing_soon")),
                "explicit_limit":     structured.get("explicit_limit"),
                "semantic_search_term": structured.get("semantic_search_term", query),
                "source_portals":     structured.get("source_portals", []),
                "strict_source":      strict_source,
                "posted_since_days":  structured.get("posted_since_days"),
                "used_llm": True,
                "llm_model": model,
            }
    except Exception as exc:
        try:
            note_llm_error(exc, model if "model" in locals() else None)
        except Exception:
            pass
        logger.debug("[query_engine] hosted LLM filter parsing failed: %s. Using heuristics.", exc)

    # ── Fallback Heuristics ──
    q = query.lower()
    sectors: list[str] = []
    regions:  list[str] = []

    # Only match sectors when keywords appear as standalone concepts,
    # not embedded in portal names or filler phrases.
    for slug, kws in _SECTOR_KEYWORDS.items():
        if any(f" {kw}" in f" {q}" for kw in kws):
            sectors.append(slug)

    for region, kws in _REGION_KEYWORDS.items():
        if any(kw in q for kw in kws):
            regions.append(region)

    priority_hint = "high" if any(h in q for h in _HIGH_PRIORITY_HINTS) else None
    closing_soon  = any(h in q for h in _CLOSING_SOON_HINTS)

    # Extract limit from phrases like "top 10", "best 5", "give me 15"
    explicit_limit = None
    match = re.search(r"(?:top|best|give me)\s+(\d+)", q)
    if match:
        try:
            val = int(match.group(1))
            if 1 <= val <= 200:
                explicit_limit = val
        except ValueError:
            pass

    # Extract date recency window
    posted_since_days = None
    if re.search(r"\btoday\b|\bfrom today\b", q):
        posted_since_days = 1
    elif re.search(r"\bthis week\b|\blast 7 days\b|\bpast week\b", q):
        posted_since_days = 7
    elif re.search(r"\bthis month\b|\blast 30 days\b|\bpast month\b", q):
        posted_since_days = 30
    elif re.search(r"\brecent\b|\blatest\b|\bnew\b", q):
        posted_since_days = 14

    return {
        "sectors":          sectors,
        "regions":          regions,
        "priority_hint":    priority_hint,
        "closing_soon":     closing_soon,
        "explicit_limit":   explicit_limit,
        "source_portals":   _extract_portals_heuristic(q),
        "strict_source":    strict_source,
        "posted_since_days": posted_since_days,
        "used_llm":         False,
    }


def _extract_portals_heuristic(q: str) -> list[str]:
    """Fallback: extract portal names from query using aliases."""
    found: list[str] = []
    for alias, canonical in _PORTAL_ALIASES.items():
        if alias in q and canonical not in found:
            found.append(canonical)
    return found


# =============================================================================
# DB enrichment helper — bulk fetch by URL
# (vector store stores URL in metadata; use it to JOIN back to intel table)
# =============================================================================

def _fetch_by_urls(urls: list[str]) -> dict[str, dict]:
    """
    Bulk-fetch seen_tenders + tender_structured_intel rows keyed by URL.
    Returns: {url: row_dict}  (missing URLs simply absent from dict)
    """
    if not urls:
        return {}

    try:
        from database.db import get_connection

        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        placeholders = ", ".join(["%s"] * len(urls))
        cur.execute(f"""
            SELECT
                st.tender_id,
                st.title,
                st.url,
                st.source_site,
                st.date_first_seen,
                si.sector,
                si.region,
                si.organization,
                si.deadline_category,
                si.relevance_score    AS bid_fit_score,
                si.priority_score,
                si.competition_level,
                si.opportunity_size,
                si.opportunity_insight,
                t.description,
                t.deep_scope,
                t.deep_ai_summary,
                t.deep_document_links
            FROM seen_tenders st
            LEFT JOIN tender_structured_intel si
                   ON si.tender_id = st.tender_id
            LEFT JOIN tenders t
                   ON t.tender_id = st.tender_id
            WHERE st.url IN ({placeholders})
        """, urls)

        rows = cur.fetchall()
        cur.close()
        conn.close()

        return {row["url"]: row for row in rows if row.get("url")}

    except Exception as exc:
        logger.warning("[query_engine] _fetch_by_urls failed: %s", exc)
        return {}


# =============================================================================
# Fallback: pure DB search when vector store is empty
# =============================================================================

def _fallback_db_search(
    query:   str,
    filters: dict[str, Any],
    limit:   int,
) -> dict[str, Any]:
    """
    Pure database search using extracted filter hints.
    Used when the ChromaDB vector store has no documents yet.
    """
    try:
        from database.db import get_intel_tenders

        sector       = filters.get("sectors")
        region       = filters.get("regions")
        min_priority = 60 if filters.get("priority_hint") == "high" else 0
        source_site  = filters.get("source_portals")

        raw = get_intel_tenders(
            limit=limit,
            sector=sector,
            region=region,
            min_priority=min_priority,
            source_site=source_site,
        )

        # Auto-relax ladder: if strict filter intersection is empty, back off once.
        # This avoids "0 results" dead-ends for long-tail queries while keeping
        # the original strict attempt as first priority.
        if not raw.get("results"):
            relax_attempts = []
            if sector and region:
                # Keep region, drop sector first (region tends to be more user-critical).
                relax_attempts.append({"sector": None, "region": region})
            if sector:
                # Then keep sector only (if region data is sparse/missing in source rows).
                relax_attempts.append({"sector": sector, "region": None})
            # Last: drop both and rely on min_priority/source portal only.
            relax_attempts.append({"sector": None, "region": None})

            for relaxed in relax_attempts:
                probe = get_intel_tenders(
                    limit=limit,
                    sector=relaxed["sector"],
                    region=relaxed["region"],
                    min_priority=min_priority,
                    source_site=source_site,
                )
                if probe.get("results"):
                    raw = probe
                    break

        infra_intent = _query_wants_infra(query, filters)
        results = []
        for row in raw.get("results", []):
            ps  = int(row.get("priority_score") or 0)
            fit = int(row.get("bid_fit_score")  or 0)
            source_site = str(row.get("source_site") or "")
            base = (0.3 * ps / 100 + 0.2 * fit / 100)
            if _is_infra_portal(source_site):
                if not infra_intent:
                    base -= 0.18
                else:
                    base += 0.10
            results.append({
                **row,
                "similarity":      0.0,
                "composite_score": round(max(0.0, base), 3),
            })

        return {
            "results":            results,
            "total":              len(results),
            "query":              query,
            "filters_extracted":  filters,
            "query_ms":           0.0,
            "vector_candidates":  0,
            "fallback":           True,
        }

    except Exception as exc:
        logger.error("[query_engine] fallback DB search failed: %s", exc)
        return {
            "results": [], "total": 0, "query": query,
            "filters_extracted": filters,
            "query_ms": 0.0, "vector_candidates": 0,
            "error": str(exc),
        }


# =============================================================================
# Main search entry point
# =============================================================================

def search(query: str, limit: int = 25) -> dict[str, Any]:
    """
    Execute a natural language semantic search over stored tenders.

    Pipeline:
      1. Extract lightweight filters from query text
      2. Semantic vector search (top 3× limit for pre-filtering)
      3. Bulk-fetch DB enrichment by URL
      4. Compute composite scores; apply filter boosts
      5. Deduplicate + sort + return top `limit` results

    Args:
        query: Natural language search string, e.g.
               "high priority education tenders in Africa closing soon"
        limit: Maximum results to return (default 20, max 50)

    Returns:
        dict with keys:
          results           — list of merged tender dicts (sorted by composite_score)
          total             — number of results
          query             — original query string
          filters_extracted — parsed filter dict
          query_ms          — wall-clock time in milliseconds
          vector_candidates — how many vector results were considered
    """
    t0 = time.perf_counter()

    # ── 1. Extract filters ────────────────────────────────────────────────────
    filters = extract_filters(query)
    query_toks = _query_tokens(query)
    infra_intent = _query_wants_infra(query, filters)
    consulting_first_intent = _query_is_consulting_first(query)
    strict_source = bool(filters.get("strict_source")) and bool(filters.get("source_portals"))
    
    # Override limit if explicitly requested in natural language
    if filters.get("explicit_limit"):
        try:
            limit = int(filters["explicit_limit"])
        except (ValueError, TypeError):
            pass
            
    # Use cleaned semantic term if LLM provided one, else fallback to raw query
    search_term = filters.get("semantic_search_term", query)
    if not search_term or not isinstance(search_term, str):
        search_term = query
        
    logger.debug("[query_engine] query=%r filters=%s", query[:80], filters)

    has_portal_filter = bool(filters.get("source_portals"))

    # ── 2. Try vector similarity search ───────────────────────────────────────
    top_k = min(200, limit * 10) if has_portal_filter else min(60, limit * 3)
    vector_results = []
    if os.getenv("DISABLE_VECTOR_SEARCH", "0") == "1":
        logger.info("[query_engine] vector search disabled by DISABLE_VECTOR_SEARCH=1")
    else:
        try:
            from intelligence.vector_store import find_similar_tenders
            vector_results = find_similar_tenders(search_term, top_k=top_k)
        except Exception as exc:
            logger.warning("[query_engine] vector search failed: %s", exc)

    # ── 3. If vector returned enough data, merge + score ──────────────────────
    candidates: list[dict] = []

    # Pre-compute date cutoff if user requested recency filter
    _date_cutoff = None
    _posted_since = filters.get("posted_since_days")
    if _posted_since:
        _date_cutoff = (datetime.utcnow() - timedelta(days=int(_posted_since))).date()
    similarity_floor = _specificity_boosted_similarity_floor(query, filters)

    if vector_results:
        urls      = [r.get("url", "") for r in vector_results if r.get("url")]
        db_by_url = _fetch_by_urls(urls)

        for vr in vector_results:
            url        = vr.get("url", "")
            similarity = float(vr.get("similarity", 0.0))

            db = db_by_url.get(url, {})
            priority_score = int(db.get("priority_score") or 0)
            bid_fit_score  = int(db.get("bid_fit_score") or float(vr.get("fit_score") or 0))

            if filters["priority_hint"] == "high" and priority_score < 50:
                continue

            # Hard date filter — skip if outside requested recency window
            if _date_cutoff:
                seen_date = _safe_seen_date(db.get("date_first_seen"))
                if seen_date and seen_date < _date_cutoff:
                    continue

            source_portals = filters.get("source_portals", [])
            if source_portals:
                db_source = str(db.get("source_site") or vr.get("source") or "")
                if not _matches_requested_portal(db_source, source_portals):
                    continue

            # Dynamic confidence gate based on query specificity.
            if similarity < similarity_floor:
                continue

            title_for_rank = str(db.get("title") or vr.get("title") or "")
            rank_text = " ".join(
                x for x in [
                    title_for_rank,
                    str(db.get("description") or ""),
                    str(db.get("deep_scope") or ""),
                    str(db.get("deep_ai_summary") or ""),
                    str(db.get("opportunity_insight") or ""),
                ] if x
            )
            lexical = _lexical_match_score(query_toks, rank_text)

            # Canonical weighted ranking (upgraded):
            # 0.40 semantic + 0.25 priority + 0.20 fit + 0.15 lexical evidence
            composite = (
                0.40 * similarity
                + 0.25 * (priority_score / 100.0)
                + 0.20 * (bid_fit_score / 100.0)
                + 0.15 * lexical
            )
            
            # Apply explicit Metadata Filter Boosts / Penalties
            db_sector = (db.get("sector") or "").lower()
            db_region = (db.get("region") or "").lower()
            has_db_sector = bool(db_sector and db_sector != "unknown" and db_sector != "null")
            has_db_region = bool(db_region and db_region != "global" and db_region != "null")

            if filters["sectors"]:
                # For explicit consulting queries, enforce sector intent when
                # the tender already has sector metadata.
                if consulting_first_intent and has_db_sector and not any(s.lower() in db_sector for s in filters["sectors"]):
                    continue
                if any(s.lower() in db_sector for s in filters["sectors"]):
                    composite += 0.15  # Significant boost for explicitly matching parsed sector
                elif has_db_sector:
                    # Gentle penalty — only if ALL filter sectors mismatch
                    # (avoids killing results that might still be relevant via title/description)
                    composite -= 0.08

            if filters["regions"]:
                # Same rule for region constraints: if region metadata exists
                # and query explicitly asks for a region, require alignment.
                if consulting_first_intent and has_db_region and not any(r.lower() in db_region or db_region in r.lower() for r in filters["regions"]):
                    continue
                if any(r.lower() in db_region or db_region in r.lower() for r in filters["regions"]):
                    composite += 0.15
                elif has_db_region:
                    composite -= 0.08

            if filters["closing_soon"] and db.get("deadline_category") in ("urgent", "soon"):
                composite += 0.10

            # --- IDCG CAPABILITY PROFILE SCORING ---
            tender_text = (
                str(db.get("title") or vr.get("title") or "") + " " +
                str(db.get("opportunity_insight") or "")
            ).lower()

            # Hard exclusion: NIC reference-number-only titles (no useful content)
            # e.g. "E-TENDER/TKA/29/2025-26" — these are never useful for consulting
            tender_title = title_for_rank
            if _NIC_REF_TITLE_PATTERN.match(tender_title.strip()):
                continue  # Skip entirely — no title = no value

            # Hard exclusion: titles that signal construction/supply/IT tenders
            if any(kw in tender_text for kw in _IDCG_EXCLUDED_KEYWORDS):
                composite -= 0.40   # Push these well below similarity threshold floor

            # Intent-aware portal prior:
            # Default user intent is consulting opportunities; infra state portals
            # should be strongly downranked unless user explicitly asks for infra.
            source_site = str(db.get("source_site") or vr.get("source") or "")
            if _is_infra_portal(source_site):
                if not infra_intent:
                    composite -= 0.18
                else:
                    composite += 0.10

            # Sector-level boost/penalty from IDCG profile
            if db_sector:
                if any(s in db_sector for s in _IDCG_STRONG_SECTORS):
                    composite += 0.10   # IDCG core sector — boost
                elif any(s in db_sector for s in _IDCG_EXCLUDED_SECTORS):
                    composite -= 0.15   # Non-IDCG sector — significant penalty

            # Strong keyword boost for IDCG's signature services
            idcg_core_keywords = [
                "baseline", "impact assessment", "third party monitoring", "tpm",
                "endline", "evaluation", "survey", "capacity building",
                "monitoring and evaluation", "m&e", "rapid assessment",
                "needs assessment", "program evaluation", "performance evaluation",
            ]
            if any(k in tender_text for k in idcg_core_keywords):
                composite += 0.12

            # Evidence richness boost: prefer tenders where we actually extracted detail/docs.
            deep_summary = str(db.get("deep_ai_summary") or "")
            deep_scope = str(db.get("deep_scope") or "")
            description = str(db.get("description") or "")
            deep_docs = db.get("deep_document_links")
            has_extracted_doc = False
            if isinstance(deep_docs, str):
                try:
                    deep_docs = json.loads(deep_docs)
                except Exception:
                    deep_docs = []
            if isinstance(deep_docs, list):
                has_extracted_doc = any(bool(d.get("extracted")) for d in deep_docs if isinstance(d, dict))

            if deep_summary or deep_scope:
                composite += 0.04
            elif description:
                composite += 0.02

            if has_extracted_doc:
                composite += 0.04

            # Penalize low-evidence records unless semantic+lexical is clearly strong.
            has_min_evidence = bool(deep_summary or deep_scope or description)
            if not has_min_evidence and (similarity < 0.35 or lexical < 0.20):
                composite -= 0.08

            # Light recency boost when user did not request strict recency.
            if not _date_cutoff:
                seen_date = _safe_seen_date(db.get("date_first_seen"))
                if seen_date:
                    age_days = (datetime.utcnow().date() - seen_date).days
                    if age_days <= 7:
                        composite += 0.04
                    elif age_days <= 30:
                        composite += 0.02

            candidates.append({
                "tender_id":          str(db.get("tender_id") or ""),
                "title":              str(db.get("title") or vr.get("title") or ""),
                "url":                url,
                "source_site":        str(db.get("source_site") or vr.get("source") or ""),
                "organization":       str(db.get("organization") or ""),
                "sector":             str(db.get("sector") or "unknown"),
                "region":             str(db.get("region") or "global"),
                "deadline_category":  str(db.get("deadline_category") or "unknown"),
                "priority_score":     priority_score,
                "bid_fit_score":      bid_fit_score,
                "opportunity_insight": db.get("opportunity_insight"),
                "description":        str(db.get("description") or ""),
                "deep_scope":         str(db.get("deep_scope") or ""),
                "deep_ai_summary":    str(db.get("deep_ai_summary") or ""),
                "deep_document_links": db.get("deep_document_links") or [],
                "competition_level":  str(db.get("competition_level") or "medium"),
                "opportunity_size":   str(db.get("opportunity_size") or "medium"),
                "similarity":         round(similarity, 3),
                "lexical_score":      round(lexical, 3),
                "composite_score":    round(min(composite, 1.0), 3),
                "date_first_seen":    db.get("date_first_seen"),
            })

    # ── 4. Deduplicate vector results ─────────────────────────────────────────
    candidates.sort(key=lambda x: x["composite_score"], reverse=True)
    seen_ids: set[str] = set()
    results: list[dict] = []
    for c in candidates:
        tid = c["tender_id"]
        if tid:
            if tid in seen_ids:
                continue
            seen_ids.add(tid)
        results.append(c)
        if len(results) >= limit:
            break

    # ── 5. If vector path returned too few results, supplement with DB ────────
    if len(results) < limit:
        logger.info(
            "[query_engine] vector returned %d/%d results — supplementing with DB",
            len(results), limit,
        )
        db_result = _fallback_db_search(query, filters, limit)
        for row in db_result.get("results", []):
            tid = str(row.get("tender_id", ""))
            if tid and tid in seen_ids:
                continue
            seen_ids.add(tid)
            results.append(row)
            if len(results) >= limit:
                break

    # Final intent-aware rerank pass across merged results (vector + fallback).
    if results:
        if strict_source:
            req = list(filters.get("source_portals") or [])
            filtered = [r for r in results if _matches_requested_portal(str(r.get("source_site") or ""), req)]
            if filtered:
                results = filtered

        if consulting_first_intent and (not infra_intent):
            cleaned = []
            for r in results:
                ttxt = (
                    str(r.get("title") or "") + " " +
                    str(r.get("opportunity_insight") or "")
                ).lower()
                if any(kw in ttxt for kw in _IDCG_EXCLUDED_KEYWORDS):
                    continue
                cleaned.append(r)
            if cleaned:
                results = cleaned
        for r in results:
            if _is_infra_portal(str(r.get("source_site") or "")):
                try:
                    base = float(r.get("composite_score") or 0.0)
                    base = base - 0.18 if not infra_intent else base + 0.10
                    r["composite_score"] = round(max(0.0, base), 3)
                except Exception:
                    pass
        results.sort(key=lambda x: float(x.get("composite_score") or 0.0), reverse=True)
        if not infra_intent:
            non_infra = [r for r in results if not _is_infra_portal(str(r.get("source_site") or ""))]
            if len(non_infra) >= limit:
                results = non_infra
        results = results[:limit]

    elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
    logger.info(
        "[query_engine] query=%r → %d results in %sms "
        "(vector_candidates=%d, filters=%s)",
        query[:80], len(results), elapsed_ms, len(vector_results),
        {k: v for k, v in filters.items() if v},
    )

    return {
        "results":           results,
        "total":             len(results),
        "query":             query,
        "filters_extracted": filters,
        "query_ms":          elapsed_ms,
        "vector_candidates": len(vector_results),
    }
