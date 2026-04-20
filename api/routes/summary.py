# =============================================================================
# api/routes/summary.py — Dashboard summary endpoint
#
# Routes (under /api/v1):
#
#   GET /summary  → headline stats for the dashboard panel
# =============================================================================

from __future__ import annotations

import logging
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException, status

from api.schemas import (
    OrgCount, SectorCount, SummaryResponse,
    DeadlineBreakdown, DeadlineBucketStats,
)

logger = logging.getLogger("tenderradar.api.summary")
router = APIRouter()


@router.get(
    "/summary",
    response_model = SummaryResponse,
    summary        = "Dashboard summary",
    description    = (
        "Returns all headline metrics needed to render the dashboard overview panel.\n\n"
        "**Includes:**\n"
        "- `total_tenders` — total records in tender_structured_intel\n"
        "- `high_priority_count` — tenders with priority_score ≥ 70\n"
        "- `portals_active` — number of distinct source portals with data\n"
        "- `pipeline_counts` — breakdown of bid_pipeline entries by stage\n"
        "- `top_sectors` — top 10 sectors by tender volume\n"
        "- `top_organizations` — top 10 organisations by tender volume\n\n"
        "All data comes from indexed aggregate queries — response time < 50ms."
    ),
)
def get_summary() -> SummaryResponse:
    t0 = time.perf_counter()

    try:
        from database.db import get_dashboard_summary
    except ImportError as exc:
        raise HTTPException(
            status_code = status.HTTP_503_SERVICE_UNAVAILABLE,
            detail      = f"Database module unavailable: {exc}",
        )

    raw = get_dashboard_summary()

    elapsed = round((time.perf_counter() - t0) * 1000, 1)
    logger.debug("[api] summary query took %sms", elapsed)

    # ── Build deadline breakdown ──────────────────────────────────────────────
    def _bucket(key: str) -> DeadlineBucketStats:
        d = raw.get("deadline_breakdown", {}).get(key, {})
        return DeadlineBucketStats(
            total   = int(d.get("total",   0) or 0),
            bid_now = int(d.get("bid_now", 0) or 0),
            strong  = int(d.get("strong",  0) or 0),
        )

    dl = raw.get("deadline_breakdown", {})
    deadline_breakdown = DeadlineBreakdown(
        closing_soon = _bucket("closing_soon"),
        needs_action = _bucket("needs_action"),
        plan_ahead   = _bucket("plan_ahead"),
        unknown      = _bucket("unknown"),
        expired      = _bucket("expired"),
        active_total = int(dl.get("active_total", 0) or 0),
    )

    return SummaryResponse(
        total_tenders       = raw["total_tenders"],
        high_priority_count = raw["high_priority_count"],
        portals_active      = raw["portals_active"],
        pipeline_counts     = raw["pipeline_counts"],
        top_sectors         = [
            SectorCount(sector=s, count=c) for s, c in raw["top_sectors"]
        ],
        top_organizations   = [
            OrgCount(organization=o, count=c) for o, c in raw["top_organizations"]
        ],
        deadline_breakdown  = deadline_breakdown,
        generated_at        = datetime.utcnow(),
    )
