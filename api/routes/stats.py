# =============================================================================
# api/routes/stats.py — System statistics and portal health endpoints
#
# Routes (all under /api/v1):
#
#   GET /stats    → SystemStats  (totals, recent counts, sector breakdown)
#   GET /portals  → List[PortalStats]  (per-portal tender counts + health)
# =============================================================================

from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, status

from intelligence.api_schema import PortalStats, SystemStats

logger = logging.getLogger("tenderradar.api.stats")
router = APIRouter()


# =============================================================================
# GET /stats
# =============================================================================

@router.get(
    "/stats",
    response_model = SystemStats,
    summary        = "System statistics",
    description    = (
        "Returns high-level statistics about the TenderRadar database.\n\n"
        "Includes total tender count, recent ingestion rates, fit-score distribution, "
        "duplicate count, per-sector breakdown, and vector store size."
    ),
)
def get_stats() -> SystemStats:
    try:
        from database.db import get_api_stats
    except ImportError as exc:
        raise HTTPException(
            status_code = status.HTTP_503_SERVICE_UNAVAILABLE,
            detail      = f"Database module unavailable: {exc}",
        )

    raw = get_api_stats()

    if not raw:
        # DB unavailable or empty — return zeroed stats rather than 503
        return SystemStats(
            total_tenders       = 0,
            total_portals       = 0,
            tenders_last_24h    = 0,
            tenders_last_7_days = 0,
            high_fit_count      = 0,
            duplicate_count     = 0,
        )

    # ── Build per-portal stats ─────────────────────────────────────────────
    portal_breakdown: List[PortalStats] = []
    for p in raw.get("portal_breakdown", []):
        portal_breakdown.append(
            PortalStats(
                portal          = p.get("source_portal", ""),
                total_tenders   = int(p.get("total_tenders", 0)),
                new_last_7_days = int(p.get("new_last_7_days", 0) or 0),
                avg_fit_score   = round(float(p.get("avg_fit_score", 0.0) or 0.0), 1),
                high_fit_count  = int(p.get("high_fit_count", 0) or 0),
                last_scraped_at = _to_datetime(p.get("last_scraped_at")),
            )
        )

    # ── Pull vector store doc count (optional dep) ─────────────────────────
    vs_docs = 0
    try:
        from intelligence.vector_store import get_store_stats
        vs_docs = get_store_stats().get("total_docs", 0)
    except Exception:
        pass

    return SystemStats(
        total_tenders       = int(raw.get("total_tenders", 0)),
        total_portals       = len(portal_breakdown),
        tenders_last_24h    = int(raw.get("tenders_last_24h", 0)),
        tenders_last_7_days = int(raw.get("tenders_last_7_days", 0)),
        high_fit_count      = int(raw.get("high_fit_count", 0)),
        duplicate_count     = int(raw.get("duplicate_count", 0)),
        portal_breakdown    = portal_breakdown,
        sector_breakdown    = raw.get("sector_breakdown", {}),
        vector_store_docs   = vs_docs,
        generated_at        = datetime.utcnow(),
    )


# =============================================================================
# GET /portals
# =============================================================================

@router.get(
    "/portals",
    response_model = List[PortalStats],
    summary        = "Portal health and coverage",
    description    = (
        "Returns per-portal tender counts, average fit scores, "
        "and when each portal was last successfully scraped.\n\n"
        "Portals that have never ingested any tenders are **not** included.\n\n"
        "For detailed scraper run history (errors, warnings, elapsed time), "
        "see the monitoring database at `monitoring/health.db`."
    ),
)
def get_portals() -> List[PortalStats]:
    try:
        from database.db import get_api_stats
    except ImportError as exc:
        raise HTTPException(
            status_code = status.HTTP_503_SERVICE_UNAVAILABLE,
            detail      = f"Database module unavailable: {exc}",
        )

    raw = get_api_stats()
    if not raw:
        return []

    # ── Merge MySQL portal data with pipeline health (last run times) ──────
    health_index: dict = {}
    try:
        from monitoring.health_report import get_health_summary
        for entry in get_health_summary(days=30):
            # get_health_summary returns list of dicts with 'source' key
            src = entry.get("source", "")
            health_index[src] = entry
    except Exception as exc:
        logger.debug(f"[api] pipeline health unavailable: {exc}")

    portals: List[PortalStats] = []
    for p in raw.get("portal_breakdown", []):
        portal_slug  = p.get("source_portal", "")
        health_entry = health_index.get(portal_slug, {})

        # Prefer health.db last_run over scraped_at from tenders table
        last_scraped = _to_datetime(
            health_entry.get("last_run_at") or p.get("last_scraped_at")
        )

        portals.append(
            PortalStats(
                portal          = portal_slug,
                total_tenders   = int(p.get("total_tenders", 0)),
                new_last_7_days = int(p.get("new_last_7_days", 0) or 0),
                avg_fit_score   = round(float(p.get("avg_fit_score", 0.0) or 0.0), 1),
                high_fit_count  = int(p.get("high_fit_count", 0) or 0),
                last_scraped_at = last_scraped,
            )
        )

    # Sort by total_tenders descending
    portals.sort(key=lambda x: x.total_tenders, reverse=True)
    return portals


# =============================================================================
# Helper
# =============================================================================

def _to_datetime(val) -> Optional[datetime]:
    """Safely coerce a string or datetime to datetime, or return None."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        # MySQL TIMESTAMP comes back as datetime already; string fallback
        return datetime.fromisoformat(str(val))
    except Exception:
        return None
