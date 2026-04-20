# =============================================================================
# api/routes/search.py — Semantic / Natural Language Search endpoint
#
# Route (under /api/v1):
#
#   GET /search?q=<query>&limit=<n>
#       → Accepts a natural language query string
#       → Extracts lightweight filters (sector, region, priority hints)
#       → Performs semantic vector similarity search via ChromaDB
#       → Enriches results with MySQL intelligence data
#       → Ranks by: 0.5×similarity + 0.3×priority/100 + 0.2×fit/100
#       → Returns top N tenders (default 20, max 50)
#
# Performance target: < 500ms (vector model is pre-loaded and cached)
# Does NOT break any existing endpoint.
# =============================================================================

from __future__ import annotations

import logging
import time

from fastapi import APIRouter, HTTPException, Query, status

from api.schemas import (
    ExtractedFilters,
    SemanticSearchResponse,
    SemanticSearchResult,
)

logger = logging.getLogger("tenderradar.api.search")
router = APIRouter()


@router.get(
    "/search",
    response_model=SemanticSearchResponse,
    summary="Natural language semantic search",
    description=(
        "Search tenders using natural language.\n\n"
        "**Examples:**\n"
        "- `high priority education tenders in Africa closing soon`\n"
        "- `M&E evaluation for climate adaptation India`\n"
        "- `WASH baseline survey South Asia urgent`\n\n"
        "**How it works:**\n"
        "1. Lightweight filter extraction (sector, region, priority hints)\n"
        "2. Semantic vector search via ChromaDB + sentence-transformers\n"
        "3. DB enrichment (priority_score, bid_fit_score, insight)\n"
        "4. Composite ranking: `0.5×similarity + 0.3×priority + 0.2×fit`\n\n"
        "Falls back to DB-only keyword search if vector store is empty.\n\n"
        "**Parameters:**\n"
        "- `q` — natural language query (required, min 2 chars)\n"
        "- `limit` — max results to return (1–50, default 20)"
    ),
    responses={
        400: {"description": "Query too short or missing"},
        503: {"description": "Query engine / database unavailable"},
    },
)
def semantic_search(
    q: str = Query(
        ...,
        min_length=2,
        max_length=500,
        description="Natural language search query",
        examples=["high priority education tenders in Africa closing soon"],
    ),
    limit: int = Query(
        20,
        ge=1,
        le=50,
        description="Maximum results to return (default 20, max 50)",
    ),
) -> SemanticSearchResponse:
    t0 = time.perf_counter()

    # ── Lazy-import to avoid module-level sentence-transformers load ──────────
    try:
        from intelligence.query_engine import search as engine_search
    except ImportError as exc:
        logger.error("[api/search] query_engine import failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Query engine unavailable: {exc}",
        )

    # ── Run search ────────────────────────────────────────────────────────────
    try:
        raw = engine_search(q.strip(), limit=limit)
    except Exception as exc:
        logger.error("[api/search] engine_search failed for %r: %s", q[:80], exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Search failed: {exc}",
        )

    # ── Build response ────────────────────────────────────────────────────────
    results = [
        SemanticSearchResult.from_dict(r)
        for r in raw.get("results", [])
    ]

    raw_filters = raw.get("filters_extracted", {})
    filters_obj = ExtractedFilters(
        sectors       = raw_filters.get("sectors",       []),
        regions       = raw_filters.get("regions",       []),
        priority_hint = raw_filters.get("priority_hint"),
        closing_soon  = raw_filters.get("closing_soon",  False),
    )

    elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)

    logger.info(
        "[api/search] q=%r → %d results in %sms (vector_candidates=%d)",
        q[:60], len(results), elapsed_ms, raw.get("vector_candidates", 0),
    )

    return SemanticSearchResponse(
        results           = results,
        total             = len(results),
        query             = q.strip(),
        filters_extracted = filters_obj,
        query_ms          = elapsed_ms,
        vector_candidates = raw.get("vector_candidates", 0),
        fallback          = raw.get("fallback", False),
    )
