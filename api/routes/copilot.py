# =============================================================================
# api/routes/copilot.py — POST /api/v1/copilot
#
# Accepts a tender_id, fetches full context from the DB, runs it through the
# LLM copilot engine, and returns a structured bid recommendation.
#
# Modes:
#   fast  — single-pass gpt-4o-mini     (~2-3s)   default
#   deep  — 3-pass reasoning chain      (~6-10s)  richer output
#
# Response time target: < 5s fast / < 12s deep (cached = instant)
# =============================================================================

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException

from api.schemas import CopilotRequest, CopilotResponse

router = APIRouter()


@router.post("/copilot", response_model=CopilotResponse)
def get_copilot_recommendation(body: CopilotRequest) -> CopilotResponse:
    """
    Generate a bid recommendation for a single tender using the LLM copilot.

    **Fast mode** (default):
    - Single LLM pass ~2-3 seconds
    - Returns: BID/CONSIDER/SKIP + confidence + why + risks + strategy

    **Deep mode** (`"mode": "deep"`):
    - 3-pass reasoning chain (Extract → Assess → Recommend) ~6-10 seconds
    - Additional output: win_theme, partner_needed, 6-dimension assessment,
      structured extraction facts
    - Use for shortlisted / pipeline tenders where quality matters more than speed

    Results are cached in-process for 1 hour per tender_id+mode combination.

    **Request body examples**
    ```json
    { "tender_id": "worldbank::12345" }
    { "tender_id": "worldbank::12345", "mode": "deep" }
    ```
    """
    tid  = body.tender_id.strip()
    mode = (body.mode or "fast").strip().lower()
    if not tid:
        raise HTTPException(status_code=422, detail="tender_id must not be empty.")
    if mode not in ("fast", "deep"):
        raise HTTPException(status_code=422, detail='mode must be "fast" or "deep".')

    # ── 1. Fetch tender context ───────────────────────────────────────────────
    from database.db import get_tender_for_copilot
    tender = get_tender_for_copilot(tid)

    if tender is None:
        # Tender might only be in seen_tenders (no tenders table row yet)
        tender = _fallback_seen_tender(tid)

    if tender is None:
        raise HTTPException(
            status_code=404,
            detail=f"Tender '{tid}' not found in the database."
        )

    # ── 2. Run copilot (dispatch by mode) ─────────────────────────────────────
    if mode == "deep":
        from intelligence.copilot_engine import generate_bid_recommendation_deep
        result = generate_bid_recommendation_deep(tender, tender_id=tid)
    else:
        from intelligence.copilot_engine import generate_bid_recommendation
        result = generate_bid_recommendation(tender, tender_id=tid)

    # ── 3. Build response ─────────────────────────────────────────────────────
    return CopilotResponse(
        tender_id        = tid,
        recommendation   = result.get("recommendation",   "CONSIDER"),
        confidence       = result.get("confidence",        60),
        why              = result.get("why",               []),
        risks            = result.get("risks",             []),
        strategy         = result.get("strategy",          []),
        cached           = result.get("cached",            False),
        fallback         = result.get("fallback",          False),
        # Deep-mode extras (None in fast mode — Pydantic omits them cleanly)
        win_theme        = result.get("win_theme"),
        partner_needed   = result.get("partner_needed"),
        partner_note     = result.get("partner_note"),
        assessment       = result.get("assessment"),
        extraction       = result.get("extraction"),
        reasoning_passes = result.get("reasoning_passes"),
    )


# ---------------------------------------------------------------------------
# Lightweight fallback: build a minimal tender dict from seen_tenders alone
# (used when no row exists in the normalised tenders table yet)
# ---------------------------------------------------------------------------

def _fallback_seen_tender(tender_id: str) -> Optional[dict]:
    """
    Return a minimal tender dict from seen_tenders + tender_structured_intel.
    This is used when the tenders normalised table has no row for this ID.
    """
    try:
        from database.db import get_connection
        from mysql.connector import Error
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
                st.tender_id,
                st.title,
                st.source_site      AS source_portal,
                st.url,
                si.organization,
                si.sector,
                si.consulting_type,
                si.region,
                si.deadline_category,
                si.relevance_score  AS bid_fit_score,
                si.priority_score,
                si.competition_level,
                si.opportunity_size,
                si.opportunity_insight
            FROM seen_tenders st
            LEFT JOIN tender_structured_intel si ON st.tender_id = si.tender_id
            WHERE st.tender_id = %s
            LIMIT 1;
        """, (tender_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row or None
    except Exception:
        return None
