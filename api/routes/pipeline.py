# =============================================================================
# api/routes/pipeline.py — Bid pipeline endpoints
#
# Routes (all under /api/v1/pipeline):
#
#   GET  /        → list pipeline entries with tender metadata (status-ordered)
#   POST /update  → partial-update status / owner / notes / proposal_deadline
# =============================================================================

from __future__ import annotations

import logging
import time
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status as http_status
from pydantic import BaseModel, field_validator

from api.schemas import (
    PipelineEntry,
    PipelineListResponse,
    PipelineUpdateRequest,
    PipelineUpdateResponse,
    VALID_STATUSES,
)

# ── Outcome request / response models ────────────────────────────────────────

_VALID_OUTCOMES  = {"won", "lost", "no_submission", "pending"}
_VALID_BID_DECS  = {"bid", "no_bid", "review_later"}


class OutcomeRequest(BaseModel):
    tender_id:    str
    outcome:      str
    bid_decision: str = "bid"

    @field_validator("outcome")
    @classmethod
    def _validate_outcome(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in _VALID_OUTCOMES:
            raise ValueError(
                f"Invalid outcome '{v}'. Must be one of: {', '.join(sorted(_VALID_OUTCOMES))}"
            )
        return v

    @field_validator("bid_decision")
    @classmethod
    def _validate_bid_decision(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in _VALID_BID_DECS:
            raise ValueError(
                f"Invalid bid_decision '{v}'. Must be one of: {', '.join(sorted(_VALID_BID_DECS))}"
            )
        return v


class OutcomeResponse(BaseModel):
    success:   bool
    tender_id: str
    outcome:   str
    message:   str = ""

logger = logging.getLogger("tenderradar.api.pipeline")
router = APIRouter()


# =============================================================================
# GET /pipeline
# =============================================================================

@router.get(
    "",
    response_model = PipelineListResponse,
    summary        = "List pipeline entries",
    description    = (
        "Returns all entries in the bid pipeline, joined with tender title, "
        "sector, region, organization, and opportunity insight.\n\n"
        "Results are ordered by lifecycle stage importance "
        "(proposal_in_progress → shortlisted → discovered → submitted → won → lost), "
        "then by `priority_score` descending.\n\n"
        "**Filters:**\n"
        "- `status` — filter to one lifecycle stage\n"
        "- `owner` — filter to one team member\n"
        "- `limit` / `offset` — pagination"
    ),
)
def list_pipeline(
    status_filter: Optional[str] = Query(
        None, alias="status",
        description=f"Filter by status. One of: {', '.join(sorted(VALID_STATUSES))}",
    ),
    owner_filter: Optional[str] = Query(
        None, alias="owner",
        description="Filter by owner name/email",
    ),
    limit:  int = Query(100, ge=1, le=500, description="Page size (max 500)"),
    offset: int = Query(0,   ge=0,         description="Pagination offset"),
) -> PipelineListResponse:
    t0 = time.perf_counter()

    # Validate status filter if provided
    if status_filter and status_filter.strip().lower() not in VALID_STATUSES:
        raise HTTPException(
            status_code = http_status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail      = (
                f"Invalid status '{status_filter}'. "
                f"Must be one of: {', '.join(sorted(VALID_STATUSES))}"
            ),
        )

    try:
        from database.db import get_pipeline_entries
    except ImportError as exc:
        raise HTTPException(
            status_code = http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail      = f"Database module unavailable: {exc}",
        )

    raw = get_pipeline_entries(
        status_filter = status_filter,
        owner_filter  = owner_filter,
        limit         = limit,
        offset        = offset,
    )

    entries = [PipelineEntry.from_db_row(r) for r in raw["results"]]

    # Aggregate counts by status from this result set
    by_status: dict = {}
    for e in entries:
        by_status[e.status] = by_status.get(e.status, 0) + 1

    return PipelineListResponse(
        results   = entries,
        total     = raw["total"],
        by_status = by_status,
        query_ms  = round((time.perf_counter() - t0) * 1000, 1),
    )


# =============================================================================
# POST /pipeline/update
# =============================================================================

@router.post(
    "/update",
    response_model = PipelineUpdateResponse,
    summary        = "Update a pipeline entry",
    description    = (
        "Partially update a bid_pipeline row.\n\n"
        "Only fields explicitly provided in the request body are written. "
        "Omitting a field leaves it unchanged.\n\n"
        "**Example:** Move to shortlisted and assign owner:\n"
        "```json\n"
        '{"tender_id": "abc123", "status": "shortlisted", "owner": "priya@idcg.in"}\n'
        "```\n"
        "**Lifecycle stages (in order):**\n"
        "`discovered` → `shortlisted` → `proposal_in_progress` → `submitted` → `won` / `lost`"
    ),
    responses      = {
        404: {"description": "Tender not found in pipeline"},
        422: {"description": "Validation error (invalid status / date format)"},
        503: {"description": "Database unavailable"},
    },
)
def update_pipeline(body: PipelineUpdateRequest) -> PipelineUpdateResponse:
    try:
        from database.db import update_pipeline_entry
    except ImportError as exc:
        raise HTTPException(
            status_code = http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail      = f"Database module unavailable: {exc}",
        )

    # Collect only explicitly set fields
    updated_fields: dict = {}
    if body.status is not None:
        updated_fields["status"] = body.status
    if body.owner is not None:
        updated_fields["owner"] = body.owner
    if body.notes is not None:
        updated_fields["notes"] = body.notes
    if body.proposal_deadline is not None:
        updated_fields["proposal_deadline"] = body.proposal_deadline

    # Validate status transition if status is being changed
    if body.status is not None:
        try:
            from database.db import get_pipeline_entry, validate_status_transition
            # Correct: fetch this specific tender by primary key, not page 1 of all entries
            current_row    = get_pipeline_entry(body.tender_id)
            current_status = (current_row or {}).get("status")
            if current_status and not validate_status_transition(current_status, body.status):
                raise HTTPException(
                    status_code=http_status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"Invalid transition: '{current_status}' → '{body.status}'. "
                        f"Allowed next statuses: "
                        f"{', '.join(sorted(_allowed_from(current_status))) or 'none (terminal state)'}"
                    ),
                )
        except HTTPException:
            raise
        except Exception as _te:
            logger.warning("[api] transition validation skipped: %s", _te)

    success = update_pipeline_entry(
        tender_id         = body.tender_id,
        status            = body.status,
        owner             = body.owner,
        notes             = body.notes,
        proposal_deadline = body.proposal_deadline,
    )

    if not success:
        raise HTTPException(
            status_code = http_status.HTTP_404_NOT_FOUND,
            detail      = (
                f"Tender '{body.tender_id}' not found in pipeline, or no fields were changed. "
                "Use pipeline --init to register it first."
            ),
        )

    logger.info(
        "[api] pipeline updated: tender_id=%s fields=%s",
        body.tender_id, list(updated_fields.keys()),
    )

    return PipelineUpdateResponse(
        success   = True,
        tender_id = body.tender_id,
        updated   = updated_fields,
        message   = f"Updated {len(updated_fields)} field(s) successfully.",
    )


# =============================================================================
# Helper used in error messages above
# =============================================================================

def _allowed_from(status: str) -> set:
    """Return the set of statuses reachable from `status`."""
    _TRANSITIONS = {
        "discovered":           {"shortlisted", "lost"},
        "shortlisted":          {"proposal_in_progress", "discovered", "lost"},
        "proposal_in_progress": {"submitted", "shortlisted", "lost"},
        "submitted":            {"won", "lost"},
        "won":                  set(),
        "lost":                 set(),
    }
    return _TRANSITIONS.get(status, set())


# =============================================================================
# POST /pipeline/outcome — record win/loss for ML feedback loop
# =============================================================================

@router.post(
    "/outcome",
    response_model = OutcomeResponse,
    summary        = "Record pipeline outcome",
    description    = (
        "Record the final result of a bid opportunity.\n\n"
        "Updates `bid_pipeline.status` to `won` or `lost` and stores "
        "`outcome` + `bid_decision` for ML feedback loop training.\n\n"
        "**outcome values:** `won` | `lost` | `no_submission` | `pending`\n"
        "**bid_decision values:** `bid` | `no_bid` | `review_later`\n\n"
        "**Rule:** `won` and `lost` require `bid_decision = 'bid'`."
    ),
    responses={
        404: {"description": "Tender not found in pipeline"},
        422: {"description": "Invalid outcome or bid_decision combination"},
    },
)
def record_outcome(body: OutcomeRequest) -> OutcomeResponse:
    if body.outcome in ("won", "lost") and body.bid_decision != "bid":
        raise HTTPException(
            status_code = http_status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail      = (
                f"outcome='{body.outcome}' requires bid_decision='bid', "
                f"got '{body.bid_decision}'"
            ),
        )

    try:
        from database.db import record_pipeline_outcome
    except ImportError as exc:
        raise HTTPException(
            status_code = http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail      = f"Database module unavailable: {exc}",
        )

    success = record_pipeline_outcome(
        tender_id    = body.tender_id,
        outcome      = body.outcome,
        bid_decision = body.bid_decision,
    )

    if not success:
        raise HTTPException(
            status_code = http_status.HTTP_404_NOT_FOUND,
            detail      = f"Tender '{body.tender_id}' not found in pipeline.",
        )

    logger.info(
        "[api] outcome recorded: tender_id=%s outcome=%s bid_decision=%s",
        body.tender_id, body.outcome, body.bid_decision,
    )

    return OutcomeResponse(
        success   = True,
        tender_id = body.tender_id,
        outcome   = body.outcome,
        message   = f"Outcome '{body.outcome}' recorded successfully.",
    )
