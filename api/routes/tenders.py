# =============================================================================
# api/routes/tenders.py — Tender retrieval and search endpoints
#
# Routes (all under /api/v1/tenders):
#
#   GET  /                → intelligence-enriched list (priority_score ranked)
#   GET  /search          → GET-style search (tenders table, fit_score based)
#   POST /search          → POST-style search with TenderSearchQuery JSON body
#   GET  /{tender_id}     → single tender by ID (full detail record)
#
# IMPORTANT: /search must be registered BEFORE /{tender_id} so FastAPI
# does not try to resolve the literal string "search" as a tender_id.
# =============================================================================

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse

from api.dependencies import (
    PaginationParams,
    TenderFilterParams,
    pagination_params,
    tender_filter_params,
)
from api.schemas import TenderIntelItem, TenderIntelListResponse
from intelligence.api_schema import (
    TenderListResponse,
    TenderRecord,
    TenderSearchQuery,
    TenderSearchResult,
    _score_to_bucket,
)

logger  = logging.getLogger("tenderradar.api.tenders")
router  = APIRouter()


# =============================================================================
# Helpers
# =============================================================================

def _parse_json_list(val: Any) -> list:
    """Safely parse a JSON list stored as a MySQL string or already-decoded list."""
    if val is None:
        return []
    if isinstance(val, list):
        return val
    try:
        parsed = json.loads(val)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def _row_to_search_result(row: dict) -> TenderSearchResult:
    """Convert a raw MySQL result dict to a lightweight TenderSearchResult."""
    fit = float(row.get("fit_score") or 0.0)
    return TenderSearchResult(
        tender_id      = row.get("tender_id", ""),
        source_portal  = row.get("source_portal", ""),
        title          = row.get("title", ""),
        title_clean    = row.get("title_clean", ""),
        organization   = row.get("organization", ""),
        country        = row.get("country", ""),
        deadline       = row.get("deadline"),
        primary_sector = row.get("primary_sector"),
        fit_score      = fit,
        fit_bucket     = _score_to_bucket(fit),
        is_expired     = bool(row.get("is_expired", False)),
        is_duplicate   = bool(row.get("is_duplicate", False)),
        url            = row.get("url", ""),
        scraped_at     = row.get("scraped_at"),
    )


def _build_list_response(
    db_result:  dict,
    paging:     PaginationParams,
    query_ms:   float,
) -> TenderListResponse:
    """Assemble a TenderListResponse from a db.search_tenders() result."""
    results = [_row_to_search_result(r) for r in db_result.get("results", [])]
    total   = db_result.get("total", 0)

    return TenderListResponse(
        results     = results,
        total       = total,
        page        = paging.page,
        page_size   = paging.page_size,
        total_pages = 0,    # sentinel — model_validator computes this
        has_next    = False, # sentinel — model_validator computes this
        query_ms    = round(query_ms, 1),
    )


def _call_search_tenders(
    q:       str,
    filters: TenderFilterParams,
    paging:  PaginationParams,
) -> dict:
    """
    Thin bridge to db.search_tenders(). Handles ImportError gracefully.
    Returns the raw dict from search_tenders().
    """
    try:
        from database.db import search_tenders
    except ImportError as exc:
        raise HTTPException(
            status_code = status.HTTP_503_SERVICE_UNAVAILABLE,
            detail      = f"Database module unavailable: {exc}",
        )

    return search_tenders(
        q                  = q,
        sectors            = filters.sectors or None,
        service_types      = filters.service_types or None,
        countries          = filters.countries or None,
        source_portals     = filters.source_portals or None,
        min_fit_score      = filters.min_fit_score,
        exclude_expired    = filters.exclude_expired,
        exclude_duplicates = filters.exclude_duplicates,
        page               = paging.page,
        page_size          = paging.page_size,
        sort_by            = paging.validated_sort_by,
        sort_order         = paging.validated_sort_order,
    )


# =============================================================================
# GET /tenders — intelligence-enriched list (priority_score ranked)
# =============================================================================

@router.get(
    "",
    response_model = TenderIntelListResponse,
    summary        = "List tenders",
    description    = (
        "Retrieve a paginated, intelligence-enriched list of tenders ranked by "
        "`priority_score`.\n\n"
        "Sourced from `seen_tenders JOIN tender_structured_intel`. "
        "All filter columns are indexed — response time < 30ms.\n\n"
        "**Filters:**\n"
        "- `sector` — e.g. `education`, `health`, `environment`, `water_sanitation`\n"
        "- `region` — e.g. `South Asia`, `Africa`, `Global`\n"
        "- `min_priority` — integer 0–100 (default 0 = no filter)\n"
        "- `source_site` — portal slug, e.g. `worldbank`, `undp`, `gem`\n"
        "- `limit` — page size (default 50, max 200)\n"
        "- `offset` — pagination offset (default 0)"
    ),
)
def list_tenders(
    limit:           int           = Query(50,  ge=1, le=200, description="Page size (max 200)"),
    offset:          int           = Query(0,   ge=0,         description="Offset pagination (use after_priority+after_tender_id for large pages)"),
    sector:          Optional[str] = Query(None,              description="Filter by sector slug"),
    region:          Optional[str] = Query(None,              description="Filter by region name"),
    min_priority:    int           = Query(0,   ge=0, le=100, description="Minimum priority_score"),
    source_site:     Optional[str] = Query(None,              description="Filter by portal/source_site"),
    after_priority:  Optional[int] = Query(None, description="Keyset cursor: priority_score of last row on previous page"),
    after_tender_id: Optional[str] = Query(None, description="Keyset cursor: tender_id of last row on previous page"),
) -> TenderIntelListResponse:
    t0 = time.perf_counter()

    try:
        from database.db import get_intel_tenders
    except ImportError as exc:
        raise HTTPException(
            status_code = status.HTTP_503_SERVICE_UNAVAILABLE,
            detail      = f"Database module unavailable: {exc}",
        )

    raw = get_intel_tenders(
        limit           = limit,
        offset          = offset,
        sector          = sector,
        region          = region,
        min_priority    = min_priority,
        source_site     = source_site,
        after_priority  = after_priority,
        after_tender_id = after_tender_id,
    )

    return TenderIntelListResponse(
        results     = [TenderIntelItem.from_db_row(r) for r in raw["results"]],
        total       = raw["total"],
        limit       = limit,
        offset      = offset,
        next_cursor = raw.get("next_cursor"),
        query_ms    = round((time.perf_counter() - t0) * 1000, 1),
    )


# =============================================================================
# GET /tenders/search — GET-style search (alias with explicit /search path)
# MUST be declared BEFORE /{tender_id} to avoid route shadowing.
# =============================================================================

@router.get(
    "/search",
    response_model = TenderListResponse,
    summary        = "Search tenders (GET)",
    description    = (
        "Search and filter tenders using query parameters.\n\n"
        "Identical functionality to `GET /tenders` — provided as a dedicated "
        "`/search` path for clients that prefer an explicit search URL.\n\n"
        "For complex filter bodies, prefer `POST /tenders/search`."
    ),
)
def search_tenders_get(
    q:       str              = Query("", description="Full-text search across tender titles"),
    filters: TenderFilterParams = Depends(tender_filter_params),
    paging:  PaginationParams   = Depends(pagination_params),
) -> TenderListResponse:
    t0        = time.perf_counter()
    db_result = _call_search_tenders(q, filters, paging)
    return _build_list_response(db_result, paging, (time.perf_counter() - t0) * 1000)


# =============================================================================
# POST /tenders/search — POST-style search with JSON body
# =============================================================================

@router.post(
    "/search",
    response_model = TenderListResponse,
    summary        = "Search tenders (POST)",
    description    = (
        "Advanced search with a structured JSON body.\n\n"
        "Accepts a `TenderSearchQuery` object with optional free-text query `q`, "
        "sector/service-type/country filters, min fit score, pagination, and sort options."
    ),
)
def search_tenders_post(body: TenderSearchQuery) -> TenderListResponse:
    t0 = time.perf_counter()

    try:
        from database.db import search_tenders
    except ImportError as exc:
        raise HTTPException(
            status_code = status.HTTP_503_SERVICE_UNAVAILABLE,
            detail      = f"Database module unavailable: {exc}",
        )

    db_result = search_tenders(
        q                  = body.q,
        sectors            = body.sectors   or None,
        service_types      = body.service_types or None,
        countries          = body.countries or None,
        source_portals     = body.source_portals or None,
        min_fit_score      = body.min_fit_score,
        exclude_expired    = body.exclude_expired,
        exclude_duplicates = body.exclude_duplicates,
        page               = body.page,
        page_size          = body.page_size,
        sort_by            = body.sort_by,
        sort_order         = body.sort_order,
    )

    paging = PaginationParams(
        page       = body.page,
        page_size  = body.page_size,
        sort_by    = body.sort_by,
        sort_order = body.sort_order,
    )
    return _build_list_response(db_result, paging, (time.perf_counter() - t0) * 1000)


# =============================================================================
# GET /tenders/{tender_id} — single full record
# MUST be declared AFTER /search to avoid shadowing.
# =============================================================================

@router.get(
    "/{tender_id:path}",
    response_model = TenderRecord,
    summary        = "Get tender by ID",
    description    = (
        "Retrieve the full detail record for a single tender.\n\n"
        "Returns all fields including AI enrichment data: fit score, fit explanation, "
        "sector classification, red flags, and deduplication status."
    ),
    responses      = {
        404: {"description": "Tender not found"},
        503: {"description": "Database unavailable"},
    },
)
def get_tender(tender_id: str) -> TenderRecord:
    try:
        from database.db import get_tender as db_get_tender
    except ImportError as exc:
        raise HTTPException(
            status_code = status.HTTP_503_SERVICE_UNAVAILABLE,
            detail      = f"Database module unavailable: {exc}",
        )

    row = db_get_tender(tender_id)

    if row is None:
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail      = f"Tender '{tender_id}' not found",
        )

    try:
        record = TenderRecord.from_db_row(row)
    except Exception as exc:
        logger.error(f"[api] Failed to serialise tender '{tender_id}': {exc}")
        raise HTTPException(
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail      = "Failed to serialise tender record",
        )

    # Attach cross-portal sources (non-fatal)
    try:
        from intelligence.fuzzy_dedup import get_cross_sources
        record.cross_sources = get_cross_sources(tender_id)
    except Exception as _ce:
        logger.debug("[api] cross_sources lookup failed: %s", _ce)

    return record
