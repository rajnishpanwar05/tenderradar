import json
import logging
import os
import re
import requests
import time
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional

from intelligence.query_engine import search
from intelligence.rag_validation import validate_chat_payload
from intelligence.ai_quality_loop import log_answer_event, log_feedback

logger = logging.getLogger("tenderradar.api.chat")
router = APIRouter()

# ── LLM Backend Configuration ───────────────────────────────────────────────
# Primary: OpenAI GPT-4o-mini (superior reasoning, $0.01/request)
# Fallback: Local Ollama llama3.2 (free, weaker but works offline)
# NOTE: Read lazily via _get_openai_key() — config.py loads .env at import time
# but os.getenv() at module-level misses that if chat.py is imported first.
_OLLAMA_MODEL   = "llama3.2"
_OLLAMA_URL     = "http://127.0.0.1:11434/api/chat"


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    messages: List[ChatMessage]


class ChatFeedbackRequest(BaseModel):
    answer_id: int
    rating: Optional[int] = None  # 1..5
    labels: List[str] = []
    note: Optional[str] = None


# ── Conversation memory ─────────────────────────────────────────────────────
_MAX_HISTORY_TURNS = 8  # keep last 8 user+assistant pairs


def _call_llm(messages: list[dict], temperature: float = 0.0) -> Optional[str]:
    """
    Call best available LLM: Gemini 2.0 Flash → OpenAI GPT-4o → None.
    Uses unified get_llm_client() so API keys are always found via config.py.
    """
    try:
        from intelligence.openai_utils import get_llm_client, note_llm_error, throttle_openai
        client, model = get_llm_client()
        if client is None:
            return None
        throttle_openai()
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=3000,
            top_p=1.0,
            response_format={"type": "json_object"},
        )
        logger.info(f"[chat] LLM response via {model}")
        return resp.choices[0].message.content
    except Exception as e:
        try:
            note_llm_error(e, model if "model" in locals() else None)
        except Exception:
            pass
        logger.warning(f"[chat] LLM call failed: {e}")
        return None


def _call_ollama(messages: list[dict], temperature: float = 0.0) -> Optional[str]:
    """Call local Ollama as fallback. Returns reply text or None."""
    try:
        payload = {
            "model": _OLLAMA_MODEL,
            "messages": messages,
            "stream": False,
            "keep_alive": -1,
            "options": {
                "temperature": temperature,
                "num_ctx": 8192,
                "top_p": 1.0,
            }
        }
        res = requests.post(_OLLAMA_URL, json=payload, timeout=180)
        res.raise_for_status()
        return res.json()["message"]["content"]
    except Exception as e:
        logger.warning(f"[chat] Ollama failed: {e}")
        return None


def _detect_query_type(query: str) -> str:
    """Classify query intent for optimal temperature + retrieval count."""
    q = query.lower()
    if any(kw in q for kw in ["trend", "pattern", "analyze", "compare", "across", "distribution", "overview"]):
        return "synthesis"
    if any(kw in q for kw in ["top ", "best ", "find ", "show ", "list ", "which "]):
        return "ranking"
    if any(kw in q for kw in ["should we bid", "recommend", "advise", "debate", "argue", "pros and cons"]):
        return "debate"
    if any(kw in q for kw in ["explain", "why", "what is", "how does", "tell me about"]):
        return "explain"
    return "general"


def _fact_seeking_query(query: str) -> bool:
    q = (query or "").lower()
    markers = [
        "which", "what", "who", "when", "where", "deadline", "budget",
        "amount", "cost", "eligibility", "requirement", "documents",
        "give me", "list", "show", "top", "best",
    ]
    return any(m in q for m in markers)


def _has_min_evidence(t: dict) -> bool:
    return bool(
        str(t.get("description") or "").strip()
        or str(t.get("deep_scope") or "").strip()
        or str(t.get("deep_ai_summary") or "").strip()
        or str(t.get("opportunity_insight") or "").strip()
    )


def _has_strong_retrieval_signal(tenders: List[dict]) -> bool:
    """
    Guardrail for fact-seeking queries:
    require at least one reasonably strong match before answering as fact.
    """
    for t in tenders or []:
        try:
            sim = float(t.get("similarity") or 0.0)
            comp = float(t.get("composite_score") or 0.0)
        except Exception:
            sim, comp = 0.0, 0.0
        if sim >= 0.28 or comp >= 0.34:
            return True
    return False


def _norm(s: str) -> str:
    return " ".join(str(s or "").strip().lower().split())


def _portal_match(source_site: str, requested: List[str]) -> bool:
    src = _norm(source_site)
    if not src or not requested:
        return False
    req = [_norm(x) for x in requested if str(x or "").strip()]
    for p in req:
        if p and (p in src or src in p):
            return True
    return False


def _sector_match(sector_val: str, requested: List[str]) -> bool:
    sec = _norm(sector_val)
    if not sec or not requested:
        return False
    req = [_norm(x).replace("_", " ") for x in requested if str(x or "").strip()]
    for s in req:
        if s and (s in sec or sec in s):
            return True
    return False


def _region_match(region_val: str, requested: List[str]) -> bool:
    reg = _norm(region_val)
    if not reg or not requested:
        return False
    req = [_norm(x) for x in requested if str(x or "").strip()]
    for r in req:
        if r and (r in reg or reg in r):
            return True
    return False


def _cited_satisfy_filters(cited: List[dict], filters: dict) -> tuple[bool, str]:
    """
    Validate that citations align with explicit user constraints.
    Returns (ok, reason_if_not_ok).
    """
    if not cited:
        return False, "No cited tenders."

    req_portals = list(filters.get("source_portals") or [])
    req_sectors = list(filters.get("sectors") or [])
    req_regions = list(filters.get("regions") or [])
    strict_source = bool(filters.get("strict_source"))

    if strict_source and len(req_portals) > 1:
        return False, "Ambiguous strict source request (multiple portals)."

    if req_portals:
        if strict_source and len(req_portals) == 1:
            target = _norm(req_portals[0])
            for t in cited:
                src = _norm(str(t.get("source_site") or ""))
                if not (target and (target in src or src in target)):
                    return False, "Strict source constraint violated by citations."
        elif not any(_portal_match(str(t.get("source_site") or ""), req_portals) for t in cited):
            return False, "Citations do not match requested portal filter."
    # Sector/region metadata can be sparse or inconsistently tagged.
    # Keep portal checks strict, but treat sector/region checks as soft
    # to avoid over-abstaining when retrieved evidence is otherwise grounded.

    return True, ""


# ---------------------------------------------------------------------------
# Search intent classifier
#
# Distinguishes between:
#   TENDER_SEARCH  — user wants to find new procurement opportunities
#   WRONG_INTENT   — user is asking about IDCG's own past projects, staff,
#                    financials, or other data not stored in this system
#
# Returns (intent: str, decline_message: str | None)
# If decline_message is set, return it directly without touching vector store.
# ---------------------------------------------------------------------------

_WRONG_INTENT_SIGNALS: list[tuple[list[str], str]] = [
    (
        ["our project", "our projects", "idcg project", "idcg's project",
         "projects for idcg", "projects of idcg", "idcg portfolio",
         "past project", "previous project", "won project", "completed project",
         "idcg won", "we won", "our wins", "our pipeline", "our bid",
         "our assignment", "our work", "we have done", "we have worked"],
        "I can only search **live procurement opportunities** — I don't have records of "
        "IDCG's own past projects, won contracts, or internal portfolio data. "
        "For that, check your internal CRM or project tracker.\n\n"
        "Would you like me to find **new tenders** that match IDCG's expertise instead?",
    ),
    (
        ["revenue", "turnover", "profit", "salary", "employee", "headcount",
         "team size", "staff", "how many people", "who works at", "ceo", "founder",
         "when was idcg", "idcg founded", "about idcg", "idcg history"],
        "I'm a tender intelligence assistant — I don't have access to IDCG's internal "
        "company data, financials, or HR records.\n\n"
        "I can help you find procurement opportunities. What sector or region are you interested in?",
    ),
    (
        ["weather", "news today", "stock price", "cricket", "football", "movie",
         "recipe", "joke", "story", "poem"],
        "I'm specialised for procurement intelligence — I can only help with finding "
        "and analysing tender opportunities. Try asking me about tenders in a specific "
        "sector, region, or portal.",
    ),
]


def _check_intent(query: str) -> Optional[str]:
    """
    Return a decline message if the query is off-topic for tender search,
    or None if the query should proceed to vector search.
    """
    q = query.lower()
    for signals, message in _WRONG_INTENT_SIGNALS:
        if any(signal in q for signal in signals):
            return message
    return None


_BUDGET_PATTERNS = [
    re.compile(r"\b(?:usd|inr|eur|gbp|rs\.?)\s*[\d,]+(?:\.\d+)?\b", re.IGNORECASE),
    re.compile(r"[$€₹]\s*[\d,]+(?:\.\d+)?", re.IGNORECASE),
    re.compile(r"\b[\d,]+(?:\.\d+)?\s*(?:usd|inr|eur|gbp)\b", re.IGNORECASE),
]
_DATE_PATTERNS = [
    re.compile(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b"),
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
    re.compile(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{1,2},?\s+\d{4}\b", re.IGNORECASE),
]
_DEADLINE_HINTS = ("deadline", "closing date", "last date", "submission date", "due date", "bid due")


def _extract_grounded_facts(tender: dict) -> dict:
    """
    Extract budget/deadline snippets from retrieved evidence text.
    Returns {"budget": "...", "deadline": "..."} with empty strings if missing.
    """
    blobs = [
        str(tender.get("description") or ""),
        str(tender.get("deep_scope") or ""),
        str(tender.get("deep_ai_summary") or ""),
        str(tender.get("opportunity_insight") or ""),
        str(tender.get("title") or ""),
    ]
    text = " ".join(b for b in blobs if b).strip()
    budget = ""
    deadline = ""

    if text:
        for pat in _BUDGET_PATTERNS:
            m = pat.search(text)
            if m:
                budget = m.group(0).strip()
                break

        lowered = text.lower()
        if any(h in lowered for h in _DEADLINE_HINTS):
            for pat in _DATE_PATTERNS:
                m = pat.search(text)
                if m:
                    deadline = m.group(0).strip()
                    break

    return {"budget": budget, "deadline": deadline}


def _deterministic_grounded_payload(
    query: str,
    tenders: List[dict],
    query_type: str,
) -> dict:
    """
    Smart no-LLM fallback:
      - query-aware summary
      - evidence-only facts
      - explicit unknowns (no guessing)
    """
    if not tenders:
        return {"answer": "No matching tenders found for your query.", "citations": []}

    q = (query or "").lower()
    fact_query = _fact_seeking_query(query)
    want_budget = any(k in q for k in ("budget", "amount", "cost", "value"))
    want_deadline = any(k in q for k in ("deadline", "closing", "due date", "last date", "submission"))

    top_n = 5 if query_type in ("ranking", "general", "explain") else 6
    selected = tenders[:top_n]
    citations = list(range(1, len(selected) + 1))

    lines: List[str] = []
    found_any_fact = False
    found_complete_fact = False
    for i, t in enumerate(selected, start=1):
        title = str(t.get("title") or "Untitled")
        org = str(t.get("organization") or "Unknown organization")
        source = str(t.get("source_site") or "Unknown portal")
        sector = str(t.get("sector") or "unknown")
        region = str(t.get("region") or "global")
        fit = int(float(t.get("bid_fit_score") or 0))
        priority = int(float(t.get("priority_score") or 0))

        detail_bits = [f"{org} ({source})", f"sector: {sector}", f"region: {region}", f"fit: {fit}/100", f"priority: {priority}/100"]

        if fact_query and (want_budget or want_deadline):
            facts = _extract_grounded_facts(t)
            fact_bits: List[str] = []
            budget_val = facts["budget"] if (want_budget and facts["budget"]) else "not found"
            deadline_val = facts["deadline"] if (want_deadline and facts["deadline"]) else "not found"
            if want_budget:
                fact_bits.append(f"budget: {budget_val}")
            if want_deadline:
                fact_bits.append(f"deadline: {deadline_val}")
            if (want_budget and facts["budget"]) or (want_deadline and facts["deadline"]):
                found_any_fact = True
            if ((not want_budget or bool(facts["budget"])) and (not want_deadline or bool(facts["deadline"]))):
                found_complete_fact = True
            detail_bits.extend(fact_bits)

        lines.append(f"{i}. {title} — " + " | ".join(detail_bits))

    if fact_query and (want_budget or want_deadline) and not found_any_fact:
        header = (
            "Not found in retrieved tenders for exact budget/deadline fields. "
            "Here are the best grounded matches you can open for primary details:"
        )
    elif fact_query and (want_budget or want_deadline) and not found_complete_fact:
        header = (
            "Partially found in retrieved tenders: some requested budget/deadline fields are missing. "
            "Returning only grounded values below:"
        )
    elif query_type == "synthesis":
        header = "Grounded synthesis from retrieved tenders:"
    elif query_type == "debate":
        header = "Grounded shortlist with fit signals:"
    else:
        header = "Top grounded matches from retrieved tenders:"

    answer = f"{header}\n\n" + "\n".join(lines)
    return {"answer": answer, "citations": citations}


@router.post("")
def chat_endpoint(req: ChatRequest):
    """
    RAG Chat Endpoint — GPT-4o-mini primary, Ollama fallback.

    Pipeline:
      0. Intent check — return early if query is off-topic (IDCG history, non-tender)
      1. Classify query type (ranking/synthesis/debate/explain)
      2. Dynamic retrieval (10-40 tenders based on query complexity)
      3. Build debate-grade system prompt with reasoning chain
      4. GPT-4o-mini → Ollama fallback
      5. Return structured reply + source tenders
    """
    if not req.messages:
        raise HTTPException(status_code=400, detail="No messages provided.")

    user_msgs = [m.content for m in req.messages if m.role == "user"]
    if not user_msgs:
        raise HTTPException(status_code=400, detail="No user message provided.")

    latest_query = user_msgs[-1]
    started_at = time.time()

    # ── 0. Intent gate — short-circuit wrong-intent queries immediately ──────
    decline_msg = _check_intent(latest_query)
    if decline_msg:
        logger.info("[chat] intent=wrong_intent query=%r — returning decline message", latest_query[:80])
        return {
            "reply": decline_msg,
            "citations": [],
            "source_tenders": [],
        }

    query_type = _detect_query_type(latest_query)

    # ── 1. Dynamic retrieval based on query type ─────────────────────────
    retrieval_limits = {
        "ranking": 15,
        "synthesis": 40,
        "debate": 25,
        "explain": 10,
        "general": 20,
    }
    limit = retrieval_limits.get(query_type, 20)

    logger.info(f"[chat] Query type: {query_type}, retrieving {limit} tenders for: {latest_query}")
    search_res = search(latest_query, limit=limit)
    tenders = search_res.get("results", [])
    filters = search_res.get("filters_extracted", {})
    strict_source_ambiguous = bool(filters.get("strict_source")) and len(list(filters.get("source_portals") or [])) > 1
    unknown_portal_fact_query = (
        _fact_seeking_query(latest_query)
        and ("portal" in (latest_query or "").lower())
        and not list(filters.get("source_portals") or [])
    )

    # ── 2. Build rich context block ──────────────────────────────────────
    context = ""
    for idx, r in enumerate(tenders):
        title        = r.get("title", "Untitled")
        org          = r.get("organization") or "Not specified"
        sector       = r.get("sector") or "General"
        region       = r.get("region") or "Global"
        source       = r.get("source_site") or "Unknown"
        priority     = r.get("priority_score", 0)
        fit          = r.get("bid_fit_score", 0)
        composite    = r.get("composite_score", 0)
        deadline_cat = r.get("deadline_category") or "unknown"
        competition  = r.get("competition_level") or "unknown"
        opp_size     = r.get("opportunity_size") or "unknown"
        insight      = r.get("opportunity_insight") or ""
        deep_summary = r.get("deep_ai_summary") or ""
        deep_scope   = r.get("deep_scope") or ""
        description  = r.get("description") or ""
        doc_links    = r.get("deep_document_links") or []
        if isinstance(doc_links, str):
            try:
                doc_links = json.loads(doc_links)
            except Exception:
                doc_links = []
        url          = r.get("url") or ""
        seen_date    = r.get("date_first_seen") or ""

        context += f"[{idx+1}] {title}\n"
        context += f"    Org: {org} | Sector: {sector} | Region: {region} | Portal: {source}\n"
        context += f"    Priority: {priority}/100 | Fit: {fit}/100 | Relevance: {composite}\n"
        context += f"    Deadline: {deadline_cat} | Competition: {competition} | Size: {opp_size}\n"
        if insight:
            context += f"    Insight: {insight}\n"
        evidence = deep_summary or deep_scope or description
        if evidence:
            evidence = " ".join(str(evidence).split())[:320]
            context += f"    Evidence: {evidence}\n"
        if isinstance(doc_links, list) and doc_links:
            extracted_count = 0
            for d in doc_links:
                if isinstance(d, dict) and bool(d.get("extracted")):
                    extracted_count += 1
            context += (
                f"    Documents: {len(doc_links)} linked, "
                f"{extracted_count} extracted\n"
            )
        if url:
            context += f"    Link: {url}\n"
        context += "\n"

    filter_summary = ""
    if filters.get("sectors"):
        filter_summary += f"Detected sectors: {', '.join(filters['sectors'])}. "
    if filters.get("regions"):
        filter_summary += f"Detected regions: {', '.join(filters['regions'])}. "
    if filters.get("source_portals"):
        filter_summary += f"Filtered portals: {', '.join(filters['source_portals'])}. "

    # ── 3. Grounded system prompt with strict JSON output ────────────────
    system_prompt = f"""You are the Chief Strategy Analyst at IDCG (International Development Consulting Group) — a premier development consulting firm specializing in M&E, evaluations, and technical assistance across 19 countries.

━━━ IDCG CAPABILITY MATRIX ━━━
CORE SERVICES (what we deliver):
  • Baseline Surveys & Endline Evaluations
  • Impact Assessments & Program Evaluations
  • Third Party Monitoring (TPM) & Independent Verification
  • Needs Assessments & Rapid Assessments
  • Capacity Building & Institutional Strengthening
  • M&E Framework Design & Implementation

CORE SECTORS (where we work):
  • Agriculture & Rural Livelihoods | Environment & Climate Change
  • Education & Skill Development | Energy & Power
  • Health & Nutrition | Water, Sanitation & Hygiene (WASH)
  • Governance & Public Policy | Gender & Social Inclusion

TOP CLIENTS (who trusts us):
  World Bank, GIZ, UNDP, FAO, IFC, USAID, AfDB, UNICEF, WHO,
  The Hans Foundation, PwC, Deloitte, EU, FCDO/DFID

GEOGRAPHIC STRENGTH: South Asia (India HQ), Sub-Saharan Africa, Southeast Asia
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DATABASE: {len(tenders)} tenders retrieved from 23,700+ opportunities across 19 portals.
{filter_summary}

━━━ REASONING CHAIN (apply for every tender) ━━━
For each tender you discuss, mentally evaluate these 5 dimensions:

1. SERVICE FIT: Does this require IDCG's core services?
   (Baseline/M&E/TPM/Evaluation = PERFECT | Advisory/TA = GOOD | IT/Legal/Supply = NO FIT)

2. SECTOR MATCH: Is this in IDCG's sectors?
   (Agriculture/Education/Health/Climate/WASH = STRONG | Infrastructure/Transport = WEAK)

3. CLIENT RELATIONSHIP: Is this a current or target client?
   (World Bank/GIZ/UNDP = PRIORITY | State govt = GOOD | Unknown private = RISKY)

4. GEOGRAPHIC FIT: Is this in our operating regions?
   (India/South Asia = HOME | East Africa = STRONG | Central Asia = STRETCH)

5. COMPETITIVE POSITION: Can IDCG realistically win?
   (Deadline >21d + Medium competition = GO | <7d + High competition = RISKY)

Score: STRONG FIT (4-5 match) → BID | MODERATE (2-3) → WATCH | WEAK (0-1) → SKIP
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ RESPONSE RULES ━━━
1. Use ONLY facts present in RETRIEVED TENDERS below.
2. If evidence is missing, explicitly say: "Not found in retrieved tenders."
3. Do NOT infer tender details not present in context.
4. Return ONLY valid JSON object:
   {{
     "answer": "<concise grounded response>",
     "citations": [1,2]
   }}
5. "citations" must contain only tender indices from RETRIEVED TENDERS.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ RETRIEVED TENDERS ({len(tenders)} results, ranked by relevance) ━━━
{context}
━━━ END TENDERS ━━━"""

    # ── 4. Build message history ─────────────────────────────────────────
    messages = [{"role": "system", "content": system_prompt}]
    all_msgs = [{"role": m.role, "content": m.content} for m in req.messages]
    if len(all_msgs) > _MAX_HISTORY_TURNS * 2:
        all_msgs = all_msgs[-(_MAX_HISTORY_TURNS * 2):]
    messages.extend(all_msgs)

    # ── 5. Generate: GPT-4o-mini primary → Ollama fallback ──────────────
    # Deterministic settings reduce hallucination in production.
    temperature = 0.0

    logger.info("[chat] Calling LLM (Gemini/OpenAI) with %d tenders, temp=%.1f...",
                len(tenders), temperature)

    llm_backend = "unavailable"
    reply_text = _call_llm(messages, temperature=temperature)
    if reply_text is not None:
        llm_backend = "hosted_llm"

    if reply_text is None:
        logger.info("[chat] LLM unavailable — falling back to Ollama (compact context)...")
        # Ollama llama3.2 has 8192 token limit — build a compact version
        # with max 5 tenders and a shorter system prompt to avoid overflow
        ollama_context = ""
        for idx, r in enumerate(tenders[:5]):
            ollama_context += (
                f"[{idx+1}] {r.get('title','Untitled')}\n"
                f"    Org: {r.get('organization','?')} | Sector: {r.get('sector','?')} "
                f"| Region: {r.get('region','?')}\n"
                f"    Priority: {r.get('priority_score',0)}/100 | Fit: {r.get('bid_fit_score',0)}/100\n\n"
            )
        ollama_system = (
            "You are a tender analyst for IDCG, a development consulting firm. "
            "Answer the user's question using ONLY the tenders listed below. "
            "Return a JSON object with keys 'answer' (string) and 'citations' (list of tender indices).\n\n"
            f"TENDERS ({min(5,len(tenders))} results):\n{ollama_context}"
        )
        ollama_messages = [{"role": "system", "content": ollama_system}]
        ollama_messages.extend([{"role": m["role"], "content": m["content"]}
                                 for m in messages if m["role"] != "system"])
        reply_text = _call_ollama(ollama_messages, temperature=temperature)
        if reply_text is not None:
            llm_backend = "ollama"

    if reply_text is None:
        payload = _deterministic_grounded_payload(
            query=latest_query,
            tenders=tenders,
            query_type=query_type,
        )
        reply_text = json.dumps(payload)
        llm_backend = "deterministic_fallback"

    validated = validate_chat_payload(reply_text, max_source_idx=len(tenders))
    citations = validated["citations"] or []
    answer_with_refs = validated["answer"]
    cited_tenders = [
        tenders[i - 1]
        for i in citations
        if isinstance(i, int) and 1 <= i <= len(tenders)
    ]

    fact_query = _fact_seeking_query(latest_query)
    strong_retrieval = _has_strong_retrieval_signal(tenders)
    cited_with_evidence = any(_has_min_evidence(t) for t in cited_tenders)
    cited_filters_ok, cited_filters_reason = _cited_satisfy_filters(cited_tenders, filters)

    # Grounding guardrail: abstain for fact-style queries when grounding is weak.
    if fact_query and (
        (not citations)
        or (not cited_with_evidence)
        or (not cited_filters_ok)
        or strict_source_ambiguous
        or unknown_portal_fact_query
    ):
        logger.info(
            "[chat] abstain_guardrail query=%r citations=%d strong=%s evidence=%s filters_ok=%s reason=%s",
            latest_query[:80],
            len(citations),
            strong_retrieval,
            cited_with_evidence,
            cited_filters_ok,
            cited_filters_reason,
        )
        answer_with_refs = (
            "Not found in retrieved tenders. I don't have enough grounded evidence to answer this reliably yet.\n\n"
            "Try narrowing by portal/sector/date, or ask me to show top matching tenders first."
        )
    else:
        # Soft caveat when cited tenders themselves are thin on extracted evidence.
        if citations and not any(_has_min_evidence(t) for t in cited_tenders):
            answer_with_refs = (
                f"{answer_with_refs}\n\n"
                "Note: this answer is based on limited extracted detail; open the cited tenders for primary source text/doc links."
            )

    source_tender_ids = [
        str(t.get("tender_id") or "").strip()
        for t in tenders
        if str(t.get("tender_id") or "").strip()
    ]
    has_abstain = "not found in retrieved tenders" in answer_with_refs.lower()
    if citations and not has_abstain:
        refs = " ".join(f"[{i}]" for i in citations)
        answer_with_refs = f"{answer_with_refs}\n\nSources: {refs}"
    latency_ms = int((time.time() - started_at) * 1000)
    answer_id = log_answer_event(
        route="/api/v1/chat",
        query_text=latest_query,
        query_type=query_type,
        retrieval_count=len(tenders),
        llm_model=llm_backend,
        citations=citations,
        source_tender_ids=source_tender_ids,
        answer_text=answer_with_refs,
        has_abstain=has_abstain,
        is_validated=True,
        latency_ms=latency_ms,
    )

    return {
        "reply": answer_with_refs,
        "citations": citations,
        "source_tenders": tenders,
        "answer_id": answer_id,
    }


@router.post("/feedback")
def chat_feedback_endpoint(req: ChatFeedbackRequest):
    """
    Save user feedback for one prior chat answer.
    Labels example:
      wrong_retrieval, hallucination, missing_context, bad_ranking, incorrect_reasoning
    """
    if req.answer_id <= 0:
        raise HTTPException(status_code=400, detail="answer_id must be a positive integer.")
    if req.rating is not None and (req.rating < 1 or req.rating > 5):
        raise HTTPException(status_code=400, detail="rating must be between 1 and 5.")

    ok = log_feedback(
        answer_event_id=req.answer_id,
        rating=req.rating,
        labels=[str(x).strip() for x in (req.labels or []) if str(x).strip()],
        note=req.note,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Could not save feedback.")
    return {"ok": True}
