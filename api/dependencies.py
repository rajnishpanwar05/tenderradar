# =============================================================================
# api/dependencies.py — Shared FastAPI dependency functions
#
# Used across all route modules via FastAPI's Depends() injection system.
# =============================================================================

from __future__ import annotations

import hmac
import os
from typing import List, Optional

from fastapi import Depends, HTTPException, Query, Security, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

# =============================================================================
# API Key Authentication
# =============================================================================

_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(api_key: Optional[str] = Security(_API_KEY_HEADER)) -> None:
    """
    Dependency: enforce X-API-Key header on all /api/v1/* routes.

    Uses hmac.compare_digest to prevent timing attacks.
    Raises 401 if the key is missing or wrong.

    Add to a router:
        router = APIRouter(dependencies=[Depends(verify_api_key)])
    """
    expected = os.environ.get("API_SECRET_KEY", "")
    if not expected:
        # Key not configured — fail closed (deny all) rather than open
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API authentication not configured. Set API_SECRET_KEY in environment.",
        )
    if not api_key or not hmac.compare_digest(api_key, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key. Pass X-API-Key header.",
            headers={"WWW-Authenticate": "ApiKey"},
        )


# =============================================================================
# Pagination + sorting parameters
# =============================================================================

class PaginationParams(BaseModel):
    """
    Standardised pagination / sorting parameters injected into list endpoints.

    Usage in a route:
        from api.dependencies import PaginationParams
        from fastapi import Depends

        @router.get("/")
        def list_tenders(paging: PaginationParams = Depends()):
            ...
    """
    page:       int   = Field(1,           ge=1,          description="Page number (1-based)")
    page_size:  int   = Field(20,          ge=1, le=100,  description="Items per page (max 100)")
    sort_by:    str   = Field("fit_score",                description="Sort field: fit_score | scraped_at | deadline | title_clean")
    sort_order: str   = Field("desc",                     description="Sort direction: asc | desc")

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size

    @property
    def validated_sort_by(self) -> str:
        allowed = {"fit_score", "scraped_at", "deadline", "title_clean"}
        return self.sort_by if self.sort_by in allowed else "fit_score"

    @property
    def validated_sort_order(self) -> str:
        return "desc" if self.sort_order.lower() not in ("asc", "desc") else self.sort_order.lower()


# =============================================================================
# Common filter parameters for GET /tenders
# =============================================================================

class TenderFilterParams(BaseModel):
    """
    Structured filter block injected into list/search endpoints.

    All fields are optional — absent means "no filter applied".
    List fields accept repeated query params: ?sectors=health&sectors=education
    """
    sectors:            List[str] = Field(default_factory=list)
    service_types:      List[str] = Field(default_factory=list)
    countries:          List[str] = Field(default_factory=list)
    source_portals:     List[str] = Field(default_factory=list)
    min_fit_score:      float     = Field(0.0, ge=0.0, le=100.0)
    exclude_expired:    bool      = Field(True)
    exclude_duplicates: bool      = Field(True)


# =============================================================================
# Dependency factories
# =============================================================================

def pagination_params(
    page:       int = Query(1,           ge=1,         description="Page number (1-based)"),
    page_size:  int = Query(20,          ge=1, le=100, description="Items per page (max 100)"),
    sort_by:    str = Query("fit_score",               description="Sort field"),
    sort_order: str = Query("desc",                    description="asc or desc"),
) -> PaginationParams:
    """Dependency: parse and validate pagination + sort parameters."""
    return PaginationParams(
        page       = page,
        page_size  = page_size,
        sort_by    = sort_by,
        sort_order = sort_order,
    )


def tender_filter_params(
    sectors:            List[str] = Query(default=[],   description="Filter by sector slug(s): health, education, environment, …"),
    service_types:      List[str] = Query(default=[],   description="Filter by service type(s): evaluation_monitoring, consulting_advisory, …"),
    countries:          List[str] = Query(default=[],   description="Filter by country name(s)"),
    source_portals:     List[str] = Query(default=[],   description="Filter by portal slug(s): worldbank, undp, gem, …"),
    min_fit_score:      float     = Query(0.0, ge=0.0, le=100.0, description="Minimum fit score (0–100)"),
    exclude_expired:    bool      = Query(True,          description="Exclude tenders past their deadline"),
    exclude_duplicates: bool      = Query(True,          description="Exclude cross-portal duplicate tenders"),
) -> TenderFilterParams:
    """Dependency: parse and validate tender filter parameters from query string."""
    return TenderFilterParams(
        sectors            = list(sectors),
        service_types      = list(service_types),
        countries          = list(countries),
        source_portals     = list(source_portals),
        min_fit_score      = min_fit_score,
        exclude_expired    = exclude_expired,
        exclude_duplicates = exclude_duplicates,
    )
