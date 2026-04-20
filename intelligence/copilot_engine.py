# =============================================================================
# intelligence/copilot_engine.py — LLM-powered bid recommendation engine
#
# Entry point:
#   from intelligence.copilot_engine import generate_bid_recommendation
#
# Returns a structured recommendation dict:
#   {
#     "recommendation": "BID" | "CONSIDER" | "SKIP",
#     "confidence":      int (0-100),
#     "why":             [str, ...],   # top reasons to bid
#     "risks":           [str, ...],   # risks / concerns
#     "strategy":        [str, ...],   # concrete next steps
#   }
#
# Performance:   < 5 seconds (gpt-4o-mini fast-path with ~600-token prompt)
# Cache:         in-memory dict, TTL = 3600 seconds per tender_id
# =============================================================================

from __future__ import annotations

import json
import time
import logging
from collections import OrderedDict
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Bounded LRU-TTL cache — prevents unbounded memory growth.
#
# Max 500 entries; eldest evicted on overflow.
# Each entry expires after CACHE_TTL seconds regardless of eviction.
# Safe for single-process use (FastAPI with threading — GIL-protected dict ops).
# ---------------------------------------------------------------------------

_CACHE_TTL  = 3_600    # seconds (1 hour)
_CACHE_MAX  = 500      # maximum concurrent entries; eldest evicted above this
_cache: OrderedDict[str, dict] = OrderedDict()   # tender_id → {"ts", "result"}


def _cached(tender_id: str) -> Optional[dict]:
    """Return cached result if it exists and is still fresh, else None."""
    entry = _cache.get(tender_id)
    if not entry:
        return None
    if (time.monotonic() - entry["ts"]) >= _CACHE_TTL:
        _cache.pop(tender_id, None)   # expired — evict proactively
        return None
    # Move to end (MRU) so LRU eviction hits the least-recently-used entry
    _cache.move_to_end(tender_id)
    return {**entry["result"], "cached": True}


def _store(tender_id: str, result: dict) -> None:
    """Persist result to bounded LRU-TTL cache."""
    _cache[tender_id] = {"ts": time.monotonic(), "result": result}
    _cache.move_to_end(tender_id)
    # Evict oldest entries when over capacity
    while len(_cache) > _CACHE_MAX:
        _cache.popitem(last=False)


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

# =============================================================================
# 3-PASS REASONING CHAIN PROMPTS
#
# Pass 1 — Extract  : structured facts from raw tender text
# Pass 2 — Assess   : 6-dimension fit scoring against IDCG profile
# Pass 3 — Recommend: final bid decision + actionable strategy
#
# Design rationale (why 3 passes beats 1 pass):
#   - Pass 1 forces the model to understand the tender before judging it.
#     Without this step, single-pass prompts conflate "understanding" with
#     "scoring" and the model shortcuts to pattern-matching.
#   - Pass 2 gives granular, dimension-level scores so the final
#     recommendation is numerically grounded (not vibes-based).
#   - Pass 3 is grounded in concrete scores → strategy is coherent and
#     directly tied to the weakest scoring dimensions.
# =============================================================================

_IDCG_FIRM_PROFILE = """
IDCG (International Development Consulting Group) firm profile:
• Core expertise: Monitoring & Evaluation (MEL), impact assessments, baseline/endline surveys
• Research, policy analysis, knowledge management
• Capacity building, training, institutional strengthening
• Governance, accountability, public financial management
• Gender equality, social inclusion, human rights
• Health systems strengthening, WASH, nutrition
• Climate adaptation, environment, agriculture, food security
• Education sector advisory
• Primary geographies: South Asia (India, Nepal, Bangladesh, Pakistan, Sri Lanka),
  Sub-Saharan Africa, Southeast Asia, East Africa
• Typical contract size: USD 100K – 5M (sweet spot USD 300K–2M)
• NOT suitable for: goods/supply contracts, construction, ICT infrastructure,
  large civil works, manufacturing, financial services
""".strip()

_EXTRACT_SYSTEM_PROMPT = f"""You are a procurement document analyst.
Given a tender notice, extract structured facts. Return ONLY valid JSON — no prose, no markdown.

Required JSON structure:
{{
  "scope_summary":         "<2-4 sentence summary of what the consultancy must deliver>",
  "funding_org":           "<primary donor / funding organization>",
  "implementing_agency":   "<government or implementing body>",
  "country":               "<primary country or region>",
  "sector":                "<primary sector, e.g. health / education / governance>",
  "deadline_date":         "<ISO date YYYY-MM-DD or null>",
  "contract_duration":     "<e.g. 18 months, 2 years, or null>",
  "estimated_budget":      "<budget string with currency, or null>",
  "key_deliverables":      ["<deliverable>", ...],
  "eligibility_highlights": ["<key requirement>", ...],
  "eval_technical_weight": <int percentage or null>,
  "eval_financial_weight": <int percentage or null>,
  "team_required":         ["<expert type>", ...],
  "unusual_conditions":    ["<any unusual condition>", ...]
}}

If a field cannot be determined from the text, use null.
Extract only what is explicitly stated — do not infer or hallucinate."""

_ASSESS_SYSTEM_PROMPT = f"""You are a bid strategy analyst at IDCG consulting firm.

{_IDCG_FIRM_PROFILE}

Given extracted tender facts (from a previous analysis) plus any scoring context,
rate fit across 6 dimensions (0–100 each). Return ONLY valid JSON — no prose, no markdown.

Required JSON structure:
{{
  "technical_fit":           <int 0-100>,
  "technical_fit_note":      "<one sentence explaining this score>",
  "geographic_fit":          <int 0-100>,
  "geographic_fit_note":     "<one sentence>",
  "timeline_feasibility":    <int 0-100>,
  "timeline_feasibility_note": "<one sentence — is there enough time to prepare a strong proposal?>",
  "financial_attractiveness":<int 0-100>,
  "financial_attractiveness_note": "<one sentence — is the contract value worth pursuing?>",
  "competition_outlook":     <int 0-100>,
  "competition_outlook_note":"<one sentence — 100=low competition, 0=highly contested>",
  "capacity_match":          <int 0-100>,
  "capacity_match_note":     "<one sentence — does IDCG have the team/CVs to win this?>",
  "composite_score":         <int 0-100, weighted average>,
  "critical_gap":            "<most important weakness or gap, or null if none>"
}}

Scoring guide:
  technical_fit:       Does IDCG's expertise directly match the scope?
  geographic_fit:      Is this in IDCG's active geography?
  timeline_feasibility:Given deadline_category, can IDCG produce a strong proposal?
  financial_attractiveness: Is contract value worthwhile? (factor: size vs effort)
  competition_outlook: How competitive is this expected to be? Lower = harder.
  capacity_match:      Can IDCG staff this with strong CVs + track record?"""

_RECOMMEND_SYSTEM_PROMPT = """You are the lead bid strategist at IDCG consulting firm.

You have been given:
  1. Extracted tender facts (Pass 1)
  2. 6-dimension fit assessment (Pass 2)

Now generate the final bid recommendation and actionable strategy.
Return ONLY valid JSON — no prose, no markdown.

Required JSON structure:
{
  "recommendation": "BID" | "CONSIDER" | "SKIP",
  "confidence": <int 0-100>,
  "why": ["<reason>", ...],
  "risks": ["<risk or red flag>", ...],
  "strategy": ["<concrete next step>", ...],
  "win_theme": "<1-sentence core value proposition IDCG should anchor the bid on, or null>",
  "partner_needed": <true | false>,
  "partner_note": "<what type of partner is needed, or null>"
}

Guidelines:
  BID     → composite_score ≥ 65 AND no critical show-stopper
  CONSIDER→ composite_score 40–64 OR one major concern
  SKIP    → composite_score < 40 OR goods/supply contract OR critical eligibility gap
  confidence → certainty about the recommendation (70+ = very sure)
  why     → 2-4 specific reasons (tied to the strongest assessment dimensions)
  risks   → 1-3 concrete risks (tied to the weakest assessment dimensions or unusual_conditions)
  strategy→ 2-4 actionable next steps; be specific (e.g. "Map 3 past MEL assignments in India")
  win_theme → the single most powerful differentiator for IDCG in this bid
  partner_needed → true if eligibility requires a local firm or missing expertise"""

# =============================================================================
# Single-pass prompt (kept for the fast-path / fallback)
# =============================================================================

_SYSTEM_PROMPT = """You are a senior bid strategist for IDCG, an international development consulting firm.

IDCG's core expertise:
• Monitoring, Evaluation & Learning (MEL), impact assessments, baseline/endline surveys
• Research, policy analysis, knowledge management
• Capacity building, training, institutional strengthening
• Governance, accountability, public financial management
• Gender equality, social inclusion, human rights
• Health systems strengthening, WASH, nutrition
• Climate adaptation, environment, agriculture, food security
• Education sector advisory
• Primarily active in South Asia, Sub-Saharan Africa, Southeast Asia

Your task: analyse the tender below and return ONLY valid JSON — no markdown, no explanation outside the JSON.

Required JSON structure:
{
  "recommendation": "BID" | "CONSIDER" | "SKIP",
  "confidence": <integer 0-100>,
  "why": ["<reason>", ...],
  "risks": ["<risk>", ...],
  "strategy": ["<action step>", ...]
}

Guidelines:
- "BID"     → strong fit; high priority; meets expertise, geography, and timeline
- "CONSIDER"→ partial fit; worth investigating; has one or more concerns
- "SKIP"    → poor fit, goods/supply contract, no IDCG expertise match, or expired
- confidence → how certain you are about the recommendation (70+ = very sure)
- why       → 2-4 specific reasons why IDCG should (or should not) pursue this
- risks     → 1-3 concrete risks or red flags (timeline, competition, scope gaps)
- strategy  → 2-4 actionable next steps if pursuing (e.g. partner search, EOI, team)

Be concise. Each list item should be one punchy sentence (max 20 words)."""


def _build_user_prompt(tender: dict) -> str:
    """
    Construct the richest possible tender context block for the LLM.

    Data sources (in order of richness):
      1. Basic fields from tenders / seen_tenders table
      2. Deep scraper fields (deep_scope, deep_budget_raw, deep_eval_criteria,
         deep_team_reqs, deep_pdf_text) — if deep_scraper has run on this tender
      3. tender_intelligence fields (fit_explanation, top_reasons, red_flags)
         — fetched live from DB when tender_id is available
      4. Cross-portal sources — unique info merged from other portals
    """
    import json as _json

    tid = tender.get("tender_id") or tender.get("id") or ""

    # ── Pull intelligence layer record if not already merged in ──────────────
    intel: dict = {}
    if tid:
        try:
            from database.db import get_intelligence as _get_intel
            intel = _get_intel(str(tid)) or {}
        except Exception:
            pass

    # ── Pull cross-portal sources ─────────────────────────────────────────────
    cross_sources: list = tender.get("cross_sources") or []
    if not cross_sources and tid:
        try:
            from intelligence.fuzzy_dedup import get_cross_sources as _get_cs
            cross_sources = _get_cs(str(tid)) or []
        except Exception:
            pass

    # ── Merge intel fields (intel table takes priority over tender dict) ──────
    fit_explanation = (
        intel.get("fit_explanation") or
        tender.get("fit_explanation") or
        tender.get("opportunity_insight") or ""
    )
    top_reasons = _parse_json_field(intel.get("fit_reasons") or tender.get("top_reasons"))
    red_flags   = _parse_json_field(intel.get("red_flags")   or tender.get("red_flags"))

    # ── Scores ────────────────────────────────────────────────────────────────
    fit_score      = float(intel.get("fit_score")     or tender.get("fit_score")      or tender.get("bid_fit_score") or 0)
    priority_score = float(tender.get("priority_score") or 0)
    budget_usd     = intel.get("budget_usd") or tender.get("estimated_budget_usd")

    # ── Deadline ──────────────────────────────────────────────────────────────
    deadline_str = (
        str(tender.get("deadline") or "")[:10] or
        tender.get("deadline_raw") or
        tender.get("deadline_category") or
        "unknown"
    )

    # ── Budget string ─────────────────────────────────────────────────────────
    budget_str = ""
    if budget_usd:
        usd = int(budget_usd)
        budget_str = f"~USD {usd/1_000_000:.1f}M" if usd >= 1_000_000 else f"~USD {usd:,}"
    elif tender.get("deep_budget_raw"):
        budget_str = tender["deep_budget_raw"]
    elif tender.get("opportunity_size"):
        budget_str = {"small": "<USD 100K", "medium": "USD 100K–500K", "large": ">USD 500K"}.get(
            tender["opportunity_size"], tender["opportunity_size"]
        )

    # ── Base description (prefer deep_description over plain description) ─────
    desc = (
        tender.get("deep_description") or
        tender.get("description") or
        intel.get("ai_summary") or ""
    ).strip()
    if len(desc) > 800:
        desc = desc[:800] + "…"

    # ── Build the prompt ──────────────────────────────────────────────────────
    lines = [
        f"TITLE:          {tender.get('title_clean') or tender.get('title', 'N/A')}",
        f"ORGANIZATION:   {tender.get('organization', 'N/A')}",
        f"PORTAL:         {tender.get('source_portal') or tender.get('source_site', 'N/A')}",
        f"COUNTRY/REGION: {tender.get('country', '')} / {tender.get('region', 'global')}",
        f"SECTOR:         {tender.get('primary_sector') or tender.get('sector', 'unknown')}",
        f"SERVICE TYPE:   {tender.get('consulting_type') or ', '.join(tender.get('service_types') or [])}",
        f"DEADLINE:       {deadline_str}",
        f"BUDGET:         {budget_str or 'unknown'}",
        f"FIT SCORE:      {int(fit_score)}/100",
        f"PRIORITY SCORE: {int(priority_score)}/100",
        f"COMPETITION:    {tender.get('competition_level', 'medium')}",
    ]

    if desc:
        lines += ["", "DESCRIPTION:", desc]

    # ── Deep scraper fields (the real intelligence premium) ───────────────────
    if tender.get("deep_scope"):
        scope = tender["deep_scope"][:2000]
        lines += ["", "SCOPE OF WORK (extracted from tender document):", scope]

    if tender.get("deep_eval_criteria"):
        lines += ["", "EVALUATION CRITERIA:", tender["deep_eval_criteria"][:800]]

    if tender.get("deep_team_reqs"):
        lines += ["", "TEAM REQUIREMENTS:", tender["deep_team_reqs"][:800]]

    if tender.get("deep_pdf_text") and not tender.get("deep_scope"):
        # Use PDF text as fallback scope if structured scope wasn't extracted
        pdf_snippet = tender["deep_pdf_text"][:2000]
        lines += ["", "DOCUMENT EXTRACT (from attached PDF):", pdf_snippet]

    # ── Pre-computed intelligence ─────────────────────────────────────────────
    if fit_explanation:
        lines += ["", f"SYSTEM ANALYSIS: {fit_explanation[:500]}"]
    if top_reasons:
        lines += ["", "PRE-COMPUTED STRENGTHS:"] + [f"  • {r}" for r in top_reasons[:4]]
    if red_flags:
        lines += ["", "PRE-COMPUTED FLAGS:"] + [f"  • {f}" for f in red_flags[:3]]

    # ── Cross-portal intelligence ─────────────────────────────────────────────
    if cross_sources:
        lines += ["", f"CROSS-PORTAL INTELLIGENCE (also listed on {len(cross_sources)} other portal(s)):"]
        for cs in cross_sources[:3]:
            portal = cs.get("source_portal", "unknown")
            url    = cs.get("source_url", "")
            unique = cs.get("unique_fields") or {}
            if isinstance(unique, str):
                try:
                    unique = _json.loads(unique)
                except Exception:
                    unique = {}
            entry = f"  • {portal}: {url}"
            if unique:
                unique_str = "; ".join(f"{k}={v}" for k, v in list(unique.items())[:3])
                entry += f"  [{unique_str}]"
            lines.append(entry)

    return "\n".join(lines)


def _parse_json_field(val) -> list:
    """Safely parse a JSON list that may be a string or already a list."""
    if not val:
        return []
    if isinstance(val, list):
        return val
    try:
        import json as _j
        parsed = _j.loads(val)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


# ---------------------------------------------------------------------------
# LLM client (lazy initialisation — prefers Gemini, falls back to OpenAI)
# ---------------------------------------------------------------------------

_llm_client = None
_llm_model  = None


def _get_llm() -> tuple:
    """Return (client, model_name) singleton, lazy-initialised.

    Prefers Gemini (free, GEMINI_API_KEY) over OpenAI (OPENAI_API_KEY).
    Raises ValueError if neither key is configured.
    """
    global _llm_client, _llm_model
    if _llm_client is not None:
        return _llm_client, _llm_model

    from intelligence.openai_utils import get_llm_client
    _llm_client, _llm_model = get_llm_client()
    if _llm_client is None:
        raise ValueError(
            "No LLM key configured. Set GEMINI_API_KEY or OPENAI_API_KEY in .env"
        )
    return _llm_client, _llm_model


# Keep old name as alias for any external callers
def _get_openai_client():
    return _get_llm()[0]


# ---------------------------------------------------------------------------
# Fallback (no OpenAI key / API error) — rule-based heuristic
# ---------------------------------------------------------------------------

def _heuristic_recommendation(tender: dict) -> dict:
    """
    Lightweight rule-based fallback used when OpenAI is unavailable.
    Returns the same dict shape as the LLM path.
    """
    fit       = int(tender.get("fit_score") or tender.get("bid_fit_score") or 0)
    priority  = int(tender.get("priority_score") or 0)
    comp      = tender.get("competition_level") or "medium"
    dead_cat  = tender.get("deadline_category") or "normal"

    score = fit * 0.5 + priority * 0.5

    if score >= 70 and dead_cat not in ("unknown",) and comp != "high":
        rec  = "BID"
        conf = min(85, int(score))
        why  = [
            f"High composite score ({int(score)}/100) indicates strong alignment.",
            "Priority score meets IDCG bid threshold.",
        ]
    elif score >= 45:
        rec  = "CONSIDER"
        conf = min(65, int(score))
        why  = [
            f"Moderate score ({int(score)}/100) — worth a closer look.",
            "Sector and geography partially match IDCG expertise.",
        ]
    else:
        rec  = "SKIP"
        conf = 70
        why  = [f"Low composite score ({int(score)}/100) indicates poor fit."]

    risks = []
    if comp == "high":
        risks.append("High competition expected — strong incumbent likely.")
    if dead_cat in ("urgent",):
        risks.append("Very tight deadline — proposal preparation time is limited.")

    strategy = [
        "Review full tender document on the portal.",
        "Identify relevant past projects for credential alignment.",
        "Check for sub-contracting / consortium opportunities.",
    ]

    return {
        "recommendation": rec,
        "confidence":     conf,
        "why":            why,
        "risks":          risks if risks else ["No major red flags identified by heuristic."],
        "strategy":       strategy,
        "cached":         False,
        "fallback":       True,   # signals that LLM was not used
    }


# ---------------------------------------------------------------------------
# Low-level LLM call helper
# ---------------------------------------------------------------------------

def _run_llm_pass(
    client,
    system_prompt: str,
    user_content: str,
    model: str = "gemini-2.0-flash",
    max_tokens: int = 600,
    temperature: float = 0.1,
) -> dict:
    """
    Execute a single LLM pass and return the parsed JSON dict.

    Args:
        client:        openai.OpenAI client instance
        system_prompt: role / instructions for this pass
        user_content:  the input text / context block
        model:         model string (gpt-4o-mini / gpt-4o)
        max_tokens:    max response tokens for this pass
        temperature:   0.0–0.4 recommended (we want deterministic outputs)

    Returns:
        Parsed dict, or {} on any failure (caller handles fallback).

    Raises:
        Exception on OpenAI API error (caller decides whether to continue chain).
    """
    try:
        response = client.chat.completions.create(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
        )
        raw = response.choices[0].message.content or "{}"
        return json.loads(raw)
    except Exception as exc:
        try:
            from intelligence.openai_utils import note_llm_error
            note_llm_error(exc, model)
        except Exception:
            pass
        raise


# ---------------------------------------------------------------------------
# 3-pass deep recommendation chain
# ---------------------------------------------------------------------------

def generate_bid_recommendation_deep(
    tender:    dict,
    tender_id: Optional[str] = None,
) -> dict:
    """
    Premium 3-pass reasoning chain for a single tender.

    Pass 1 — Extract  : gpt-4o-mini  — structured fact extraction from raw text
    Pass 2 — Assess   : gpt-4o-mini  — 6-dimension fit scoring vs IDCG profile
    Pass 3 — Recommend: gpt-4o-mini  — final BID/CONSIDER/SKIP + strategy

    The output is a superset of generate_bid_recommendation():
        recommendation     — "BID" | "CONSIDER" | "SKIP"
        confidence         — int 0-100
        why                — list[str]
        risks              — list[str]
        strategy           — list[str]
        win_theme          — str | None   (core value proposition)
        partner_needed     — bool
        partner_note       — str | None
        assessment         — dict         (Pass 2 dimension scores)
        extraction         — dict         (Pass 1 structured facts)
        cached             — bool
        fallback           — bool         (True if chain fell back to heuristic)
        reasoning_passes   — int          (number of LLM passes completed)

    Falls back gracefully to single-pass if any pass fails.
    Cache key: "{tid}:deep" (separate from fast-path cache)
    """
    tid       = str(tender_id or tender.get("tender_id") or "unknown")
    cache_key = f"{tid}:deep"

    # ── Cache hit ─────────────────────────────────────────────────────────────
    cached = _cached(cache_key)
    if cached:
        logger.debug("[copilot:deep] cache hit for %s", tid)
        return cached

    # ── LLM client ────────────────────────────────────────────────────────────
    try:
        client, llm_model = _get_llm()
    except (ValueError, ImportError) as exc:
        logger.warning("[copilot:deep] LLM not available (%s) — heuristics", exc)
        result = _heuristic_recommendation(tender)
        result["reasoning_passes"] = 0
        return result

    extraction: dict = {}
    assessment: dict = {}

    # ── Pass 1: Extract ───────────────────────────────────────────────────────
    # Build the extraction input from all available text sources
    p1_lines = [
        f"TITLE: {tender.get('title_clean') or tender.get('title', 'N/A')}",
        f"ORGANIZATION: {tender.get('organization', 'N/A')}",
        f"PORTAL: {tender.get('source_portal') or tender.get('source_site', '')}",
        f"COUNTRY/REGION: {tender.get('country', '')} / {tender.get('region', 'global')}",
        f"DEADLINE (raw): {tender.get('deadline_raw') or str(tender.get('deadline') or '')}",
        f"DEADLINE CATEGORY: {tender.get('deadline_category', 'unknown')}",
    ]
    # Attach the richest text we have — PDF > scope > description
    for src_key in ("deep_pdf_text", "deep_scope", "deep_description", "description"):
        text_blob = (tender.get(src_key) or "").strip()
        if text_blob:
            p1_lines += ["", f"--- {src_key.upper()} ---", text_blob[:3000]]
            break  # stop at first non-empty source
    # Also append eval criteria and team requirements if available
    if tender.get("deep_eval_criteria"):
        p1_lines += ["", "--- EVALUATION CRITERIA ---", tender["deep_eval_criteria"][:1000]]
    if tender.get("deep_team_reqs"):
        p1_lines += ["", "--- TEAM REQUIREMENTS ---", tender["deep_team_reqs"][:800]]
    # Pre-populated extraction columns (from deep_scraper)
    if tender.get("deep_budget_raw"):
        p1_lines.append(f"BUDGET (scraped): {tender['deep_budget_raw']}")
    if tender.get("deep_contract_duration"):
        p1_lines.append(f"CONTRACT DURATION (scraped): {tender['deep_contract_duration']}")
    if tender.get("deep_eval_technical_weight") is not None:
        p1_lines.append(f"EVAL WEIGHTS (scraped): Technical {tender['deep_eval_technical_weight']}% / Financial {tender.get('deep_eval_financial_weight', '')}%")
    if tender.get("deep_min_years_experience") is not None:
        p1_lines.append(f"MIN EXPERIENCE (scraped): {tender['deep_min_years_experience']} years")

    p1_user = "\n".join(p1_lines)

    passes_completed = 0
    try:
        extraction = _run_llm_pass(
            client, _EXTRACT_SYSTEM_PROMPT, p1_user,
            model=llm_model, max_tokens=700, temperature=0.1,
        )
        passes_completed = 1
        logger.debug("[copilot:deep] Pass 1 (extract) OK for %s", tid)
    except Exception as exc:
        logger.error("[copilot:deep] Pass 1 failed for %s: %s — falling back to single-pass", tid, exc)
        result = generate_bid_recommendation(tender, tender_id=tid)
        result["reasoning_passes"] = 0
        result["extraction"]       = {}
        result["assessment"]       = {}
        return result

    # ── Pass 2: Assess ────────────────────────────────────────────────────────
    # Feed Pass 1 output + pre-computed scores into assessment
    pre_scores = {
        "priority_score":  tender.get("priority_score", 0),
        "fit_score":       tender.get("fit_score") or tender.get("bid_fit_score", 0),
        "competition":     tender.get("competition_level", "medium"),
        "opportunity_size":tender.get("opportunity_size", "medium"),
        "decision_tag":    tender.get("decision_tag", ""),
    }
    p2_user = (
        "EXTRACTED TENDER FACTS (Pass 1 output):\n"
        + json.dumps(extraction, indent=2)
        + "\n\nPRE-COMPUTED SCORES (from rule-based pipeline):\n"
        + json.dumps(pre_scores, indent=2)
    )
    try:
        assessment = _run_llm_pass(
            client, _ASSESS_SYSTEM_PROMPT, p2_user,
            model=llm_model, max_tokens=700, temperature=0.1,
        )
        passes_completed = 2
        logger.debug("[copilot:deep] Pass 2 (assess) OK for %s — composite=%s",
                     tid, assessment.get("composite_score"))
    except Exception as exc:
        logger.error("[copilot:deep] Pass 2 failed for %s: %s — degrading to Pass 1 output", tid, exc)
        # Can still generate a recommendation from Pass 1 alone using single-pass
        result = generate_bid_recommendation(tender, tender_id=tid)
        result["reasoning_passes"] = passes_completed
        result["extraction"]       = extraction
        result["assessment"]       = {}
        return result

    # ── Pass 3: Recommend ─────────────────────────────────────────────────────
    p3_user = (
        "EXTRACTED TENDER FACTS (Pass 1):\n"
        + json.dumps(extraction, indent=2)
        + "\n\nFIT ASSESSMENT (Pass 2):\n"
        + json.dumps(assessment, indent=2)
    )
    try:
        recommendation_raw = _run_llm_pass(
            client, _RECOMMEND_SYSTEM_PROMPT, p3_user,
            model=llm_model, max_tokens=600, temperature=0.15,
        )
        passes_completed = 3
        logger.debug("[copilot:deep] Pass 3 (recommend) OK for %s: %s",
                     tid, recommendation_raw.get("recommendation"))
    except Exception as exc:
        logger.error("[copilot:deep] Pass 3 failed for %s: %s — using heuristic final step", tid, exc)
        # Synthesise from Pass 2 scores
        comp = int(assessment.get("composite_score") or 0)
        rec  = "BID" if comp >= 65 else ("CONSIDER" if comp >= 40 else "SKIP")
        recommendation_raw = {
            "recommendation": rec,
            "confidence":     comp,
            "why":            ["Based on multi-dimension assessment."],
            "risks":          [assessment.get("critical_gap") or "Review carefully."],
            "strategy":       ["Review full tender document before committing."],
        }

    # ── Normalise and merge ───────────────────────────────────────────────────
    rec = str(recommendation_raw.get("recommendation", "CONSIDER")).upper()
    if rec not in {"BID", "CONSIDER", "SKIP"}:
        rec = "CONSIDER"

    result = {
        "recommendation":   rec,
        "confidence":       max(0, min(100, int(recommendation_raw.get("confidence", 60)))),
        "why":              [str(x) for x in (recommendation_raw.get("why") or [])[:5]],
        "risks":            [str(x) for x in (recommendation_raw.get("risks") or [])[:4]],
        "strategy":         [str(x) for x in (recommendation_raw.get("strategy") or [])[:5]],
        "win_theme":        recommendation_raw.get("win_theme") or None,
        "partner_needed":   bool(recommendation_raw.get("partner_needed", False)),
        "partner_note":     recommendation_raw.get("partner_note") or None,
        "assessment":       assessment,
        "extraction":       extraction,
        "cached":           False,
        "fallback":         False,
        "reasoning_passes": passes_completed,
    }

    _store(cache_key, result)
    logger.info(
        "[copilot:deep] %d-pass chain complete for %s: %s (%d%%) composite=%s",
        passes_completed, tid, result["recommendation"], result["confidence"],
        assessment.get("composite_score"),
    )
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_bid_recommendation(tender: dict, tender_id: Optional[str] = None) -> dict:
    """
    Generate a structured bid recommendation for a single tender.

    Args:
        tender:     Dict of tender fields (from tenders / seen_tenders JOIN).
        tender_id:  Override for cache key. Falls back to tender["tender_id"].

    Returns:
        {
            "recommendation": "BID" | "CONSIDER" | "SKIP",
            "confidence":      int 0-100,
            "why":             [str, ...],
            "risks":           [str, ...],
            "strategy":        [str, ...],
            "cached":          bool,
            "fallback":        bool,    # True if LLM was skipped
        }
    """
    tid = str(tender_id or tender.get("tender_id") or "unknown")

    # ── 1. Cache hit ──────────────────────────────────────────────────────────
    cached = _cached(tid)
    if cached:
        logger.debug("[copilot] cache hit for %s", tid)
        return cached

    # ── 2. Try LLM (Gemini preferred, OpenAI fallback) ────────────────────────
    try:
        client, llm_model = _get_llm()
        user_prompt = _build_user_prompt(tender)

        response = client.chat.completions.create(
            model       = llm_model,
            temperature = 0.2,
            max_tokens  = 512,
            response_format = {"type": "json_object"},
            messages = [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": user_prompt},
            ],
        )

        raw_text = response.choices[0].message.content or "{}"
        data     = json.loads(raw_text)

        # Validate and normalise
        recommendation = str(data.get("recommendation", "CONSIDER")).upper()
        if recommendation not in {"BID", "CONSIDER", "SKIP"}:
            recommendation = "CONSIDER"

        result = {
            "recommendation": recommendation,
            "confidence":     max(0, min(100, int(data.get("confidence", 60)))),
            "why":            [str(x) for x in (data.get("why") or [])[:5]],
            "risks":          [str(x) for x in (data.get("risks") or [])[:4]],
            "strategy":       [str(x) for x in (data.get("strategy") or [])[:5]],
            "cached":         False,
            "fallback":       False,
        }

        _store(tid, result)
        logger.info("[copilot] LLM recommendation for %s: %s (%d%%)",
                    tid, result["recommendation"], result["confidence"])
        return result

    except ValueError as exc:
        # API key not configured — fall back to heuristics silently
        logger.warning("[copilot] LLM not configured (%s) — using heuristics.", exc)
        return _heuristic_recommendation(tender)

    except Exception as exc:   # noqa: BLE001
        try:
            from intelligence.openai_utils import note_llm_error
            note_llm_error(exc, llm_model if "llm_model" in locals() else None)
        except Exception:
            pass
        logger.error("[copilot] LLM call failed for %s: %s", tid, exc)
        return _heuristic_recommendation(tender)
