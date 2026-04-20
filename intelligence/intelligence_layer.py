# =============================================================================
# intelligence_layer.py — AI Intelligence Layer for TenderRadar
#
# Modules:
#   1A. TenderExtractor   — GPT-4o structured JSON extraction (Pydantic v2)
#   1B. SemanticFitScorer — sentence-transformers cosine similarity (local)
#   1C. FitExplainer      — GPT-4o 2-sentence narrative (score ≥ 65 only)
#   1D. RedFlagDetector   — rule-based flag tagging (<10ms, no API)
#
# Public API:
#   enriched_list = process_batch(raw_tenders: list[dict]) -> list[EnrichedTender]
#
# Guarantees:
#   - Never crashes the pipeline; every failure degrades gracefully
#   - If OpenAI is down: enrichment skipped, plain alert still fires
#   - If sentence-transformers unavailable: keyword score used as fallback
#   - Processing time logged per tender (target <2s)
# =============================================================================

import json
import time
import hashlib
import logging
import os
import platform
import sys
from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field
from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    retry_if_exception_type, RetryError,
)

logger = logging.getLogger(__name__)

# Module-level caches (populated once per process on first call)
_st_model       = None   # sentence-transformers SentenceTransformer
_firm_embedding = None   # pre-computed IDCG profile numpy array
_semantic_runtime_block_logged = False


def _semantic_runtime_enabled() -> bool:
    """
    Safety gate for sentence-transformers native stack.
    Prevent known macOS+Python3.9 native crashes unless explicitly overridden.
    """
    if os.getenv("DISABLE_SEMANTIC_EMBEDDINGS", "0") == "1":
        return False
    if os.getenv("FORCE_SEMANTIC_EMBEDDINGS", "0") == "1":
        return True
    if platform.system() == "Darwin" and sys.version_info[:2] <= (3, 9):
        return False
    return True


def _log_semantic_runtime_block_once() -> None:
    global _semantic_runtime_block_logged
    if _semantic_runtime_block_logged:
        return
    _semantic_runtime_block_logged = True
    logger.warning(
        "[intelligence] Semantic embeddings disabled for runtime safety "
        "(macOS + Python 3.9 can segfault in native deps). "
        "Set FORCE_SEMANTIC_EMBEDDINGS=1 only on a stabilized runtime."
    )


# =============================================================================
# IDCG FIRM PROFILE — rich text representation used for embedding
# =============================================================================

IDCG_PROFILE = """
IDCG (International Development Consulting Group) — boutique development consulting firm,
New Delhi, India. 8-15 expert consultants, 15+ years of delivery experience.

CORE SERVICES (IDCG bids on these):
Monitoring Evaluation Accountability Learning (MEAL): impact evaluation, baseline survey,
endline survey, mid-term evaluation, final evaluation, programme evaluation, project
evaluation, policy evaluation, third-party monitoring TPM, independent verification agency
IVA, concurrent monitoring, real-time monitoring, outcome monitoring, process monitoring,
KAP survey knowledge attitude practice, needs assessment, rapid assessment, outcome
harvesting, most significant change, theory of change review, logical framework review,
MEAL framework design, performance monitoring system, MIS design, data quality assessment DQA.

Research and Studies: qualitative research, quantitative research, mixed methods study,
household survey, community survey, beneficiary assessment, stakeholder analysis, scoping
study, feasibility study, situational analysis, landscape analysis, literature review,
systematic review, evidence synthesis, cost benefit analysis, cost effectiveness analysis,
value for money assessment, social audit, public expenditure tracking survey PETS, citizen
report card, social accountability, participatory rural appraisal PRA.

Technical Assistance and Advisory: technical assistance, capacity building, institutional
strengthening, organizational development, training needs assessment, training design,
training delivery, policy advisory, policy formulation, strategy development, system
strengthening, project management consultancy PMC, quality assurance, mentoring coaching.

Education sector: school education, foundational literacy, foundational numeracy FLN,
NIPUN Bharat, mid-day meal programme, teacher training, learning outcomes, ASER assessment,
vocational training TVET, EdTech, dropout study, school readiness, early childhood education
ECE, anganwadi evaluation, ICDS assessment, Samagra Shiksha.

Health and Nutrition: maternal health, child health, immunization, reproductive health,
Ayushman Bharat, NHM National Health Mission, NRHM, RMNCH, malnutrition, stunting, wasting,
POSHAN Abhiyaan, Swachh Bharat Mission health, community health worker ASHA AWW, mental
health, disease surveillance, health system strengthening, universal health coverage UHC,
primary healthcare.

WASH Water Sanitation Hygiene: water supply, sanitation, Jal Jeevan Mission JJM, ODF open
defecation free, faecal sludge management FSSM, solid waste management, hygiene behaviour
change communication BCC, groundwater, piped water scheme, rural water supply.

Agriculture and Livelihoods: rural livelihoods, agricultural extension, value chain, farmer
producer organisation FPO, self help group SHG, microfinance, MGNREGS, food security, crop
insurance, PM-KISAN, horticulture, fisheries, dairy, PMFBY, watershed development.

Climate and Environment: climate adaptation, climate resilience, natural resource management
NRM, forest governance, REDD+, biodiversity, NTFP non-timber forest products, watershed
management, soil conservation, disaster risk reduction DRR, green economy, carbon markets.

Gender and Social Inclusion: women empowerment, gender mainstreaming, gender-based violence
GBV, child protection, disability inclusion, SC ST communities, tribal welfare, social
protection, social safety nets, gender audit.

Governance: e-governance, digital public infrastructure, decentralisation, panchayati raj,
local self-government, public financial management PFM, regulatory reform, RTI, grievance
redressal, citizen engagement, open government, anti-corruption.

Social Protection: direct benefit transfer DBT, cash transfer, PMAY, beneficiary
identification, poverty targeting, below poverty line BPL survey, welfare scheme evaluation,
food distribution PDS, MGNREGS evaluation.

PRIMARY CLIENTS: World Bank, UNDP, UNICEF, WHO, FAO, UNFPA, UN Women, WFP, ILO, GIZ,
USAID, FCDO UK DFID, European Union, AFD France, ADB, AfDB, Gates Foundation, Aga Khan
Foundation, Government of India ministries, state governments Bihar UP Rajasthan MP
Jharkhand Odisha Chhattisgarh Maharashtra Assam, CARE India, Save the Children, Oxfam,
ActionAid, Plan International, PwC Deloitte KPMG as sub-contractor.

GEOGRAPHY: India primary all states especially Bihar Uttar Pradesh Rajasthan Madhya Pradesh
Jharkhand Odisha Chhattisgarh Assam Northeast India. South Asia Bangladesh Nepal Sri Lanka.
East Africa Kenya Tanzania Ethiopia Uganda. West Africa Ghana Nigeria. Southeast Asia.

NOT FOR IDCG: civil works, road construction, bridge construction, building construction,
supply of goods, supply of equipment, furniture, vehicles, medicines, software development,
IT system development, legal services, manufacturing, printing, catering, security services.
""".strip()


# =============================================================================
# PYDANTIC SCHEMAS
# =============================================================================

class TenderExtraction(BaseModel):
    """Structured extraction of key fields from a tender notice."""
    title_clean:            str            = Field(description="Cleaned, normalized title — remove ref numbers and jargon")
    one_liner:              str            = Field(description="One sentence: what assignment + who is client + where")
    summary:                list[str]      = Field(description="2–3 bullet points in plain English describing scope", default_factory=list)
    sector:                 list[str]      = Field(description="Sector tags e.g. ['Health', 'M&E', 'Education']", default_factory=list)
    service_type:           list[str]      = Field(description="Service type e.g. ['Consultancy', 'Research', 'Advisory']", default_factory=list)
    geography:              list[str]      = Field(description="Countries/states/regions e.g. ['India', 'Bihar']", default_factory=list)
    client_org:             Optional[str]  = Field(description="Hiring organization name e.g. 'World Bank'", default=None)
    estimated_budget_usd:  Optional[int]  = Field(description="Estimated contract value in USD integers, or null if unknown", default=None)
    deadline:               Optional[str]  = Field(description="Submission deadline as YYYY-MM-DD string, or null", default=None)
    eligibility:            Optional[str]  = Field(description="Key eligibility requirements in one sentence, or null", default=None)
    is_goods_only:          bool           = Field(description="True ONLY if this is purely supply/equipment procurement — no consulting component whatsoever", default=False)
    is_relevant_for_consulting: bool       = Field(description="True if a consulting/advisory firm could realistically bid on this", default=True)
    confidence:             float          = Field(description="Extraction confidence score 0.0–1.0", default=0.8)


class EnrichedTender(BaseModel):
    """Raw tender dict enriched with full AI intelligence."""
    # ── Original fields ───────────────────────────────────────────────────────
    title:   str
    url:     str
    source:  str
    deadline: Optional[str] = None
    value:    Optional[str] = None

    # ── 1A: GPT-4o Extraction ─────────────────────────────────────────────────
    extraction: Optional[TenderExtraction] = None

    # ── 1B: Semantic + Keyword Fit Scores ─────────────────────────────────────
    fit_score:      float      = 0.0
    semantic_score: float      = 0.0
    keyword_score:  float      = 0.0
    top_reasons:    list[str]  = Field(default_factory=list)

    # ── 1C: Narrative Explanation ─────────────────────────────────────────────
    fit_explanation: Optional[str] = None

    # ── 1D: Red Flags ─────────────────────────────────────────────────────────
    red_flags: list[str] = Field(default_factory=list)

    # ── Meta ──────────────────────────────────────────────────────────────────
    processing_time_ms: float          = 0.0
    embedding_id:       Optional[str]  = None


# =============================================================================
# MODULE 1A — Structured Tender Extraction (GPT-4o)
# =============================================================================

from intelligence.openai_utils import (
    get_openai_client,
    get_llm_client,
    note_llm_error,
    throttle_openai,
)


def _extraction_fallback(retry_state) -> None:
    """Tenacity callback: return None after exhausting retries."""
    logger.warning("[intelligence] extract_tender_fields — retries exhausted, returning None")
    return None


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
)
def extract_tender_fields(title: str, description: str = "") -> Optional[TenderExtraction]:
    """
    Use LLM structured outputs to extract key tender fields into TenderExtraction.
    Anti-hallucination: temperature=0, strict grounding rules, all fields must
    come ONLY from the provided text.
    """
    client, model = get_llm_client()
    if client is None:
        return None

    # Use deep content if available (much richer than title alone)
    source_text = description[:15000] if description else ""
    has_body = len(source_text.strip()) > 100

    prompt = f"""You are a procurement analyst extracting structured data from a tender notice.

STRICT RULE: Extract ONLY information explicitly stated in the SOURCE TEXT below.
- If a field is not clearly stated, output null (or [] for arrays, false for booleans).
- Do NOT invent, infer, or hallucinate any values.
- Do NOT use your general knowledge to fill gaps — only use what is written below.
- Confidence should reflect how much information is present in the text.

SOURCE TITLE: {title}

SOURCE TEXT:
{source_text if has_body else "(No body text available — extract from title only)"}

Return ONLY valid JSON — no markdown code fences, no prose outside the JSON object.

Required JSON structure:
{{
  "title_clean":            "<cleaned title: remove ref numbers, tender IDs, abbreviations — keep the meaningful description>",
  "one_liner":              "<one sentence: WHAT assignment + WHO is the client + WHERE — only if all 3 are in the text, else null>",
  "summary":                ["<bullet: exact scope point from text>", "<bullet 2>"],
  "sector":                 ["<sector tag ONLY if mentioned>"],
  "service_type":           ["<service type ONLY if mentioned>"],
  "geography":              ["<country/state ONLY if mentioned in text>"],
  "client_org":             "<exact org name from text, or null>",
  "estimated_budget_usd":   <integer in USD if stated, null if not stated — convert INR÷83, EUR×1.08, GBP×1.27>,
  "deadline":               "<YYYY-MM-DD if a specific date is stated, null otherwise>",
  "eligibility":            "<copy the exact eligibility sentence from text, or null>",
  "is_goods_only":          <true ONLY if text explicitly says supply/equipment with NO consulting component>,
  "is_relevant_for_consulting": <true if a consulting/advisory firm could bid, false if goods/works only>,
  "confidence":             <0.0 if title-only, 0.5 if partial text, 0.9 if full ToR/RFP available>
}}"""

    try:
        throttle_openai()
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            max_tokens=700,
            temperature=0,
        )
        raw  = resp.choices[0].message.content or "{}"
        data = json.loads(raw)
        data.setdefault("title_clean", title[:200])
        data.setdefault("one_liner", "")
        return TenderExtraction(**data)
    except Exception as e:
        note_llm_error(e, model)
        msg = str(e).lower()
        if "insufficient_quota" in msg or "exceeded your current quota" in msg:
            logger.error("[intelligence] LLM quota exhausted — skipping extraction")
            return None
        logger.warning(f"[intelligence] LLM extraction error ({model}): {e}")
        raise


# =============================================================================
# MODULE 1B — Semantic Fit Scoring (sentence-transformers, local)
# =============================================================================

def _get_st_model():
    """Lazy-load all-MiniLM-L6-v2 (cached after first call, ~90MB download once)."""
    global _st_model
    if not _semantic_runtime_enabled():
        _log_semantic_runtime_block_once()
        return None
    if _st_model is None:
        try:
            from sentence_transformers import SentenceTransformer
            _st_model = SentenceTransformer("all-MiniLM-L6-v2")
            logger.info("[intelligence] Loaded all-MiniLM-L6-v2 model OK")
        except Exception as e:
            logger.error(f"[intelligence] sentence-transformers load failed: {e}")
    return _st_model


def _get_firm_embedding():
    """Return cached IDCG profile embedding (computed once per process)."""
    global _firm_embedding
    if _firm_embedding is None:
        model = _get_st_model()
        if model is not None:
            _firm_embedding = model.encode(IDCG_PROFILE, convert_to_numpy=True)
            logger.info("[intelligence] IDCG profile embedding computed and cached")
    return _firm_embedding


def compute_semantic_score(tender_text: str) -> tuple[float, list[str]]:
    """
    Compute cosine similarity between the tender and IDCG's profile embedding.
    Returns (score_0_to_100, top_matching_reasons).
    Falls back to (0.0, []) if model is unavailable.
    """
    model    = _get_st_model()
    firm_emb = _get_firm_embedding()

    if model is None or firm_emb is None:
        return 0.0, []

    try:
        import numpy as np
        tender_emb = model.encode(tender_text, convert_to_numpy=True)

        # Cosine similarity (dot product of unit vectors)
        norm = np.linalg.norm(tender_emb) * np.linalg.norm(firm_emb)
        cosine = float(np.dot(tender_emb, firm_emb) / (norm + 1e-9))

        # Scale to 0–100 (cosine is typically 0.0–0.8 for text; cap at 1.0)
        score  = round(min(100.0, max(0.0, cosine * 115)), 1)  # slight scaling for better spread
        return score, _extract_keyword_reasons(tender_text)
    except Exception as e:
        logger.warning(f"[intelligence] Semantic score failed: {e}")
        return 0.0, []


def compute_keyword_score(title: str, description: str = "") -> tuple[float, list[str]]:
    """
    Score against FIRM_EXPERTISE categories from keywords.py.
    Returns (score_0_to_100, list_of_matched_category_names).
    """
    from intelligence.keywords import FIRM_EXPERTISE

    text = (title + " " + description).lower()
    matched_cats = [cat for cat, kws in FIRM_EXPERTISE.items()
                    if any(kw in text for kw in kws)]

    if not matched_cats:
        return 0.0, []

    # Base score: proportional to categories matched (max ~40 raw)
    base  = (len(matched_cats) / len(FIRM_EXPERTISE)) * 100

    # Boost for high-signal categories
    HIGH_VALUE = {"M&E / Evaluation", "Research & Documentation",
                  "Capacity Building & Advisory", "Governance & Institutional"}
    boost = sum(10 for c in matched_cats if c in HIGH_VALUE)

    score = round(min(100.0, base + boost), 1)
    return score, matched_cats


def score_tender_fit(title: str, description: str = "") -> tuple[float, float, float, list[str]]:
    """
    Compute the final blended fit score: 70% semantic + 30% keyword.

    Returns:
        (fit_score, semantic_score, keyword_score, top_3_reasons)
    """
    semantic_score, _ = compute_semantic_score(title + " " + description)
    keyword_score, keyword_cats = compute_keyword_score(title, description)

    fit_score = round(0.70 * semantic_score + 0.30 * keyword_score, 1)

    # Build human-readable reasons
    reasons = [f"Matches {c}" for c in keyword_cats[:3]]
    if not reasons and semantic_score > 40:
        reasons = ["Semantic match to IDCG's work profile"]

    return fit_score, semantic_score, keyword_score, reasons[:3]


def _extract_keyword_reasons(tender_text: str) -> list[str]:
    """Return top-3 matching FIRM_EXPERTISE categories for a tender text."""
    from intelligence.keywords import FIRM_EXPERTISE
    text    = tender_text.lower()
    matched = [cat for cat, kws in FIRM_EXPERTISE.items()
               if any(kw in text for kw in kws)]
    return [f"Matches {c}" for c in matched[:3]]


# =============================================================================
# MODULE 1C — AI Narrative Fit Explanation (GPT-4o, score ≥ 65 only)
# =============================================================================

def _explanation_fallback(retry_state) -> None:
    """Tenacity callback: return None after retries."""
    return None


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=2, max=8),
    retry=retry_if_exception_type(Exception),
)
def generate_fit_explanation(title: str, extraction: Optional[TenderExtraction]) -> Optional[str]:
    """
    Generate a concise 2-sentence narrative explaining why this tender fits IDCG.
    Only called for tenders with fit_score >= 65 (saves API cost on low-fit tenders).

    Returns:
        2-sentence string, or None if OpenAI unavailable.
    """
    client, model = get_llm_client()
    if client is None:
        return None

    # Build context block from extraction if available
    context_parts = [f"Title: {title}"]
    if extraction:
        if extraction.sector:
            context_parts.append(f"Sectors: {', '.join(extraction.sector)}")
        if extraction.geography:
            context_parts.append(f"Geography: {', '.join(extraction.geography)}")
        if extraction.client_org:
            context_parts.append(f"Client: {extraction.client_org}")
        if extraction.one_liner:
            context_parts.append(f"Scope: {extraction.one_liner}")
        if extraction.eligibility:
            context_parts.append(f"Eligibility: {extraction.eligibility}")

    prompt = f"""Write a 2-sentence fit note (max 45 words total) for IDCG, a boutique Indian development consulting firm.

IDCG's strengths: M&E/impact evaluation, education, health/WASH, gender, governance, climate/livelihoods, capacity building. Clients: World Bank, UNDP, GIZ, USAID, state govts. Team of 5–15. Strong India presence.

Tender:
{chr(10).join(context_parts)}

Rules:
- Be specific — name actual tender requirements and link to IDCG's exact capabilities
- Do NOT start with "This tender" or "IDCG"
- Do NOT use generic filler phrases like "aligns well" without specifics
- Output only the 2 sentences, nothing else"""

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=120,
            temperature=0.3,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        note_llm_error(e, model)
        logger.warning(f"[intelligence] Fit explanation failed: {e}")
        raise   # let tenacity retry


def generate_ai_summary(title: str, description: str, extraction: Optional[TenderExtraction] = None) -> Optional[str]:
    """
    Generate a rich, grounded AI summary of the tender for display on the detail page.

    Rules:
    - 4–6 sentences covering: what, who, where, when, how much, key requirements
    - ONLY facts from the provided source text — no hallucinations
    - Structured and readable for a consultant deciding whether to bid

    Returns a string summary, or None if LLM unavailable.
    """
    client, model = get_llm_client()
    if client is None:
        return None

    source_text = description[:12000] if description else ""
    if not source_text and not title:
        return None

    # Build context from extraction if available
    extra_context = ""
    if extraction:
        parts = []
        if extraction.sector:
            parts.append(f"Sectors: {', '.join(extraction.sector)}")
        if extraction.geography:
            parts.append(f"Geography: {', '.join(extraction.geography)}")
        if extraction.client_org:
            parts.append(f"Client: {extraction.client_org}")
        if extraction.eligibility:
            parts.append(f"Eligibility: {extraction.eligibility}")
        if extraction.estimated_budget_usd:
            parts.append(f"Budget: ~USD {extraction.estimated_budget_usd:,}")
        if extraction.deadline:
            parts.append(f"Deadline: {extraction.deadline}")
        if parts:
            extra_context = "\n".join(parts)

    extracted_fields_block = ("EXTRACTED FIELDS:\n" + extra_context) if extra_context else ""
    source_text_block = source_text if source_text else "(No body text — summarize from title only, keep to 1 sentence)"

    prompt = f"""Write a clear, factual summary of this procurement tender for a consulting firm evaluating whether to bid.

STRICT RULES:
1. Use ONLY information from the source text below — do NOT add facts from your training data
2. If a field (budget, deadline, eligibility) is not in the text, do NOT mention it
3. Write 4–6 sentences covering: what the assignment is, who the client is, what geography, key deliverables, and any stated requirements
4. Plain English — no jargon, no marketing language
5. Output only the summary paragraphs, nothing else

TENDER TITLE: {title}
{extracted_fields_block}

SOURCE TEXT:
{source_text_block}"""

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
            temperature=0,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        note_llm_error(e, model)
        logger.warning(f"[intelligence] AI summary generation failed: {e}")
        return None


# =============================================================================
# MODULE 1D — Red Flag Detection (rule-based, <10ms, no API)
# =============================================================================

_GOODS_PHRASES = [
    "supply of ", "procurement of ", "purchase of ", "supply and installation",
    "supply of equipment", "supply of furniture", "supply of vehicles",
    "supply of computers", "supply of medicines", "supply of drugs",
    "printing of ", "printing and supply", "supply of uniforms",
    "supply of food", "supply of stationery", "supply of tools",
    "purchase and installation", "providing and installation",
]

_INELIGIBLE_PHRASES = [
    "un agency only", "un agencies only", "government entity only",
    "government agency only", "ngos only", "ngo only",
    "bilateral agency", "member states only", "open to ngos",
    "exclusive to implementing partners",
]

_BUDGET_TOO_LARGE_USD = 5_000_000


def detect_red_flags(
    title:       str,
    extraction:  Optional[TenderExtraction],
    deadline_str: Optional[str],
) -> list[str]:
    """
    Detect and return a list of red flag codes for a tender.

    Checks:
        GOODS_ONLY   — supply/procurement tender, no consulting
        TOO_LARGE    — budget > $5M (beyond boutique firm capacity)
        EXPIRED      — submission deadline already passed
        INELIGIBLE   — explicitly restricted to UN/govt agencies only

    Note: DUPLICATE flag is added separately in process_batch via vector_store.

    Returns:
        List of flag strings, e.g. ['GOODS_ONLY', 'EXPIRED']
    """
    flags = []
    text  = title.lower()

    # 1. GOODS_ONLY
    is_goods = (extraction.is_goods_only if extraction else False)
    if not is_goods:
        is_goods = any(phrase in text for phrase in _GOODS_PHRASES)
    if is_goods:
        flags.append("GOODS_ONLY")

    # 2. TOO_LARGE
    if extraction and extraction.estimated_budget_usd:
        if extraction.estimated_budget_usd > _BUDGET_TOO_LARGE_USD:
            flags.append("TOO_LARGE")

    # 3. EXPIRED
    # Use extraction deadline preferentially; fall back to raw deadline_str
    dl_raw = (extraction.deadline if extraction and extraction.deadline else deadline_str)
    if dl_raw:
        try:
            dl = datetime.strptime(str(dl_raw)[:10], "%Y-%m-%d").date()
            if dl < date.today():
                flags.append("EXPIRED")
        except ValueError:
            pass

    # 4. INELIGIBLE
    if any(phrase in text for phrase in _INELIGIBLE_PHRASES):
        flags.append("INELIGIBLE")

    return flags


# =============================================================================
# MAIN ENTRY POINT — process_batch
# =============================================================================

def process_batch(tenders: list[dict]) -> list[EnrichedTender]:
    """
    Enrich a batch of raw tender dicts with AI intelligence.

    Each input dict must have:
        title  (str)   — tender title
        url    (str)   — source URL
        source (str)   — pipeline name e.g. 'worldbank', 'gem'
    Optional keys:
        deadline / end_date (str)
        value               (str)
        description         (str) — full body text for deeper analysis

    Processing per tender:
        1A. GPT-4o structured extraction
        1B. Semantic + keyword fit scoring (70/30 blend)
        1C. GPT-4o fit explanation (score ≥ 65 only)
        1D. Rule-based red flag detection
        2.  ChromaDB duplicate check + embedding storage

    Returns:
        List of EnrichedTender sorted by fit_score descending.
        Guaranteed not to raise — all failures degrade gracefully.
    """
    if not tenders:
        return []

    # Warm up models once before the batch loop
    _get_st_model()
    _get_firm_embedding()

    # Attempt to load vector_store (optional dependency)
    vs_available = False
    try:
        import intelligence.vector_store as vs
        vs_available = True
    except Exception as e:
        logger.warning(f"[intelligence] vector_store unavailable, skipping duplicate check: {e}")

    results: list[EnrichedTender] = []

    for raw in tenders:
        t_start = time.perf_counter()

        title = (raw.get("title") or "").strip()
        if not title:
            continue

        url         = raw.get("url")         or ""
        source      = raw.get("source")      or "unknown"
        
        # ── Augment description with Deep PDF Scraper fields if available ──
        description = raw.get("description") or raw.get("Description") or ""
        deep_chunks = [
            raw.get("deep_scope"), 
            raw.get("deep_eval_criteria"), 
            raw.get("deep_eligibility_raw"), 
            raw.get("deep_pdf_text"),
            raw.get("deep_description")
        ]
        rich_text = "\n\n".join(filter(None, deep_chunks)).strip()
        if len(rich_text) > 100:
            description = rich_text
        deadline    = raw.get("deadline")    or raw.get("Deadline") or raw.get("end_date") or ""
        value       = raw.get("value")       or raw.get("Estimated Bid Value") or raw.get("estimated_value") or ""

        # Derive a stable tender ID
        tender_id = (raw.get("tender_id") or raw.get("id")
                     or hashlib.md5(url.encode()).hexdigest()[:16])

        # Normalise "N/A" / "None" strings to None
        deadline = deadline if deadline not in ("N/A", "None", "", None) else None
        value    = value    if value    not in ("N/A", "None", "", None) else None

        # ── Pre-enrichment: normalise + classify (zero-cost, always runs) ────
        normalized   = None
        classification = None
        try:
            from intelligence.normalizer  import normalize_tender
            from intelligence.classifier  import classify_tender
            normalized     = normalize_tender(raw, tender_id=str(tender_id))
            classification = classify_tender(title, description)
        except Exception as _e:
            logger.debug(f"[intelligence] normalizer/classifier skipped: {_e}")

        enriched = EnrichedTender(
            title=title, url=url, source=source,
            deadline=deadline, value=value,
        )

        try:
            # ── 1A: Structured extraction ────────────────────────────────────
            try:
                enriched.extraction = extract_tender_fields(title, description)
            except RetryError:
                logger.warning(f"[intelligence] extract_tender_fields retries exhausted for '{title[:40]}'")
                enriched.extraction = None

            # ── 1B: Fit scoring ──────────────────────────────────────────────
            fit, sem, kw, reasons = score_tender_fit(title, description)
            enriched.fit_score      = fit
            enriched.semantic_score = sem
            enriched.keyword_score  = kw
            enriched.top_reasons    = reasons

            # ── 1C: Fit explanation (only if worthwhile) ─────────────────────
            if fit >= 65:
                try:
                    enriched.fit_explanation = generate_fit_explanation(
                        title, enriched.extraction
                    )
                except RetryError:
                    logger.warning(f"[intelligence] generate_fit_explanation retries exhausted for '{title[:40]}'")
                    enriched.fit_explanation = None

            # ── 1C.2: AI Summary (grounded, for tender detail view) ──────────
            _ai_summary = None
            if len(description) > 100:   # only worth generating if we have text
                try:
                    _ai_summary = generate_ai_summary(title, description, enriched.extraction)
                except Exception:
                    _ai_summary = None

            # ── 1D: Red flags ────────────────────────────────────────────────
            enriched.red_flags = detect_red_flags(
                title, enriched.extraction, deadline
            )

            # ── 2: Duplicate check + store embedding ─────────────────────────
            if vs_available:
                try:
                    model = _get_st_model()
                    if model is not None:
                        import numpy as np
                        emb = model.encode(title + " " + description, convert_to_numpy=True)

                        if vs.is_duplicate(emb, threshold=0.95):
                            enriched.red_flags.append("DUPLICATE")

                        emb_id = vs.store_tender(
                            str(tender_id), emb.tolist(),
                            {"title": title[:200], "source": source,
                             "fit_score": fit, "url": url[:500]},
                        )
                        enriched.embedding_id = emb_id
                except Exception as e:
                    logger.warning(f"[intelligence] vector_store op failed for '{title[:40]}': {e}")

            # ── Persist to DB ─────────────────────────────────────────────────
            try:
                from database.db import save_intelligence, save_normalized_tender, update_tender_enrichment
                save_intelligence(str(tender_id), enriched)

                # Save normalised record + classification to tenders table
                if normalized is not None:
                    save_normalized_tender(normalized)
                    if classification is not None:
                        is_dup  = "DUPLICATE" in enriched.red_flags
                        is_exp  = "EXPIRED"   in enriched.red_flags
                        update_tender_enrichment(
                            tender_id            = str(tender_id),
                            sectors              = classification.sectors,
                            service_types        = classification.service_types,
                            primary_sector       = classification.primary_sector,
                            fit_score            = enriched.fit_score,
                            semantic_score       = enriched.semantic_score,
                            keyword_score        = enriched.keyword_score,
                            fit_explanation      = enriched.fit_explanation or "",
                            top_reasons          = enriched.top_reasons,
                            red_flags            = enriched.red_flags,
                            estimated_budget_usd = (enriched.extraction.estimated_budget_usd
                                                    if enriched.extraction else None),
                            is_duplicate         = is_dup,
                            is_expired           = is_exp,
                        )
                        # Save AI summary to deep_ai_summary column
                        if _ai_summary:
                            try:
                                from database.db import get_connection
                                _conn = get_connection()
                                _cur  = _conn.cursor()
                                _cur.execute(
                                    "UPDATE tenders SET deep_ai_summary=%s WHERE tender_id=%s",
                                    (_ai_summary[:5000], str(tender_id))
                                )
                                _conn.commit()
                                _cur.close()
                                _conn.close()
                            except Exception as _dbe:
                                logger.debug(f"[intelligence] deep_ai_summary save failed: {_dbe}")
            except Exception as e:
                logger.debug(f"[intelligence] DB save skipped: {e}")

        except Exception as e:
            logger.error(f"[intelligence] Unexpected error for '{title[:40]}': {e}")

        enriched.processing_time_ms = round((time.perf_counter() - t_start) * 1000, 1)
        logger.info(
            f"[intelligence] '{title[:55]}' → fit={enriched.fit_score:.0f} "
            f"flags={enriched.red_flags} ({enriched.processing_time_ms}ms)"
        )
        results.append(enriched)

    results.sort(key=lambda x: x.fit_score, reverse=True)
    logger.info(f"[intelligence] Batch complete — {len(results)} tenders enriched")
    return results
