# =============================================================================
# api/routes/health.py — GET /api/v1/health
#
# Returns per-portal scraper reliability metrics + data quality snapshot.
# Powered entirely by monitoring/scraper_health_manager.py — no DB writes.
# =============================================================================

from fastapi import APIRouter

from api.schemas import HealthResponse, PortalHealth

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def get_health() -> HealthResponse:
    """
    Return a live snapshot of scraper reliability and data quality.

    **Portal stability levels**
    - `stable`   — success_rate ≥ 70% AND consecutive_failures < 2
    - `partial`  — success_rate 30–70% OR 1–2 consecutive failures
    - `unstable` — auto-disabled (≥ 3 zero-row runs OR success_rate < 30%)

    **Metrics per portal**
    - `success_rate`         — % of runs in the last 10 that returned rows
    - `average_rows`         — mean row count over the last 10 runs
    - `consecutive_failures` — how many most-recent runs returned 0 rows
    - `last_success_time`    — ISO timestamp of the last run that found rows

    **Data quality**
    The `data_confidence_score` column (0–100) in `tender_structured_intel`
    is computed per-tender by the health manager based on:
    - Source success rate (40 pts)
    - Description completeness (20 pts)
    - Deadline presence (20 pts)
    - Organisation presence (10 pts)
    - Sector presence (10 pts)

    This endpoint is read-only and safe to poll frequently (cached at SQLite level).
    """
    from monitoring.scraper_health_manager import get_all_health

    raw = get_all_health()

    portals = [
        PortalHealth(
            source               = p["source"],
            stability            = p["stability"],
            success_rate         = p["success_rate"],
            average_rows         = p["average_rows"],
            consecutive_failures = p["consecutive_failures"],
            total_runs           = p["total_runs"],
            last_success_time    = p.get("last_success_time"),
            disabled_reason      = p.get("disabled_reason"),
        )
        for p in raw.get("portals", [])
    ]

    # Sort: unstable first, then partial, then stable — most urgent on top
    _order = {"unstable": 0, "partial": 1, "stable": 2}
    portals.sort(key=lambda p: (_order.get(p.stability, 3), p.source))

    return HealthResponse(
        portals        = portals,
        stable_count   = raw.get("stable_count",   0),
        partial_count  = raw.get("partial_count",  0),
        unstable_count = raw.get("unstable_count", 0),
        generated_at   = raw.get("generated_at",   ""),
    )
