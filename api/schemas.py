# =============================================================================
# api/schemas.py — Pydantic models for the new dashboard API endpoints
#
# Covers:
#   GET  /api/v1/tenders        → TenderIntelItem / TenderIntelListResponse
#   GET  /api/v1/tenders/{id}   → (uses existing TenderRecord from intelligence/api_schema.py)
#   GET  /api/v1/pipeline       → PipelineEntry / PipelineListResponse
#   POST /api/v1/pipeline/update → PipelineUpdateRequest / PipelineUpdateResponse
#   GET  /api/v1/summary        → SummaryResponse
# =============================================================================

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, field_validator, model_validator


# =============================================================================
# HELPERS
# =============================================================================

def _safe_int(val: Any, default: int = 0) -> int:
    try:
        return int(val or default)
    except (TypeError, ValueError):
        return default


def _safe_str(val: Any, default: str = "") -> str:
    if val is None:
        return default
    s = str(val).strip()
    return s if s not in ("None", "none", "NULL") else default


# =============================================================================
# GET /api/v1/tenders — intelligence-enriched tender list
# =============================================================================

class TenderIntelItem(BaseModel):
    """
    Single item returned by GET /api/v1/tenders.

    Sourced from a 4-way LEFT JOIN:
      seen_tenders ← tender_structured_intel ← tenders ← tender_intelligence

    All list items are now visible even if they haven't been through the
    intelligence pipeline yet (they appear with zero scores rather than
    being silently excluded from the response).

    fit_score and bid_fit_score are the same value.  Both names exist so
    the detail page (fit_score) and list page (bid_fit_score) are always consistent.

    Deadline categories (stored in deadline_category):
      closing_soon  — 0–7 days remaining
      needs_action  — 8–21 days remaining
      plan_ahead    — 22+ days remaining
      expired       — past deadline
      unknown       — no parseable deadline found
    """
    # Core identity
    tender_id:           str
    title:               str
    url:                 str               = ""
    source_site:         str               = ""
    date_first_seen:     Optional[datetime] = None

    # Organisation & geography
    organization:        str               = ""
    region:              str               = "global"
    country:             str               = ""

    # Classification
    sector:              str               = "unknown"
    consulting_type:     str               = "unknown"

    # Deadline
    deadline_category:   str               = "unknown"
    deadline:            Optional[date]    = None      # actual date (YYYY-MM-DD)
    deadline_raw:        str               = ""        # raw string from portal

    # Scores
    bid_fit_score:       int               = 0         # relevance_score from structured intel
    fit_score:           int               = 0         # alias — always identical to bid_fit_score
    priority_score:      int               = 0
    complexity_score:    int               = 0

    # Competition & sizing
    competition_level:   str               = "medium"
    opportunity_size:    str               = "medium"

    # Budget
    estimated_budget_usd: Optional[int]   = None

    # Flags
    is_expired:          bool              = False
    is_duplicate:        bool              = False

    # AI narrative
    opportunity_insight: Optional[str]    = None
    fit_explanation:     Optional[str]    = None

    # ── Amendment tracking ────────────────────────────────────────────────────
    amendment_count:     int               = 0
    last_amended_at:     Optional[datetime] = None

    # ── Deep extraction — bid-critical fields (Task 2) ────────────────────────
    # These are populated after deep scrape (priority ≥ 60 tenders only).
    # They are None / "" when the tender has not yet been deep-scraped.
    deep_contract_duration:      str            = ""     # e.g. "18 months"
    deep_budget_currency:        str            = ""     # e.g. "USD"
    deep_date_pre_bid:           str            = ""     # pre-bid conference date
    deep_date_qa_deadline:       str            = ""     # Q&A cut-off date
    deep_date_contract_start:    str            = ""     # expected start date
    deep_eval_technical_weight:  Optional[int]  = None   # e.g. 70
    deep_eval_financial_weight:  Optional[int]  = None   # e.g. 30
    deep_min_years_experience:   Optional[int]  = None   # minimum experience years
    deep_min_similar_projects:   Optional[int]  = None   # minimum similar projects

    # ── Rich content for detail/preview (populated after deep scrape) ─────────
    deep_scope:          Optional[str]       = None    # scope of work / ToR summary
    deep_ai_summary:     Optional[str]       = None    # AI-generated grounded summary
    deep_document_links: List[Dict[str, Any]] = Field(default_factory=list)  # [{url, label, file_type}]
    extracted_document_count: int             = 0
    total_document_count:     int             = 0
    description:         str                 = ""      # full description text

    @classmethod
    def from_db_row(cls, row: dict) -> "TenderIntelItem":
        # fit_score: prefer AI score from tender_intelligence, fallback to relevance_score
        fit = _safe_int(
            row.get("fit_score") or
            row.get("bid_fit_score") or
            row.get("relevance_score")
        )
        _doc_links = (
            row.get("deep_document_links")
            if isinstance(row.get("deep_document_links"), list)
            else (
                __import__("json").loads(row["deep_document_links"])
                if row.get("deep_document_links") and isinstance(row.get("deep_document_links"), str)
                else []
            )
        )
        if not isinstance(_doc_links, list):
            _doc_links = []
        _extracted_count = 0
        for _d in _doc_links:
            if isinstance(_d, dict) and bool(_d.get("extracted")):
                _extracted_count += 1

        return cls(
            tender_id            = _safe_str(row.get("tender_id")),
            title                = _safe_str(row.get("title")),
            url                  = _safe_str(row.get("url")),
            source_site          = _safe_str(row.get("source_site")),
            date_first_seen      = row.get("date_first_seen"),
            organization         = _safe_str(row.get("organization")),
            region               = _safe_str(row.get("region"),           "global"),
            country              = _safe_str(row.get("country")),
            sector               = _safe_str(row.get("sector"),           "unknown"),
            consulting_type      = _safe_str(row.get("consulting_type"),  "unknown"),
            deadline_category    = _safe_str(row.get("deadline_category"),"unknown"),
            deadline             = row.get("deadline"),
            deadline_raw         = _safe_str(row.get("deadline_raw")),
            bid_fit_score        = fit,
            fit_score            = fit,
            priority_score       = _safe_int(row.get("priority_score")),
            complexity_score     = _safe_int(row.get("complexity_score")),
            competition_level    = _safe_str(row.get("competition_level"),"medium"),
            opportunity_size     = _safe_str(row.get("opportunity_size"), "medium"),
            estimated_budget_usd = row.get("estimated_budget_usd") or row.get("budget_usd"),
            is_expired           = bool(row.get("is_expired",    False)),
            is_duplicate         = bool(row.get("is_duplicate",  False)),
            opportunity_insight  = _safe_str(row.get("opportunity_insight")) or None,
            fit_explanation      = _safe_str(row.get("fit_explanation"))    or None,
            # Amendment tracking
            amendment_count      = _safe_int(row.get("amendment_count")),
            last_amended_at      = row.get("last_amended_at"),
            # Deep extraction — bid-critical fields (Task 2)
            deep_contract_duration     = _safe_str(row.get("deep_contract_duration")),
            deep_budget_currency       = _safe_str(row.get("deep_budget_currency")),
            deep_date_pre_bid          = _safe_str(row.get("deep_date_pre_bid")),
            deep_date_qa_deadline      = _safe_str(row.get("deep_date_qa_deadline")),
            deep_date_contract_start   = _safe_str(row.get("deep_date_contract_start")),
            deep_eval_technical_weight = (
                int(row["deep_eval_technical_weight"])
                if row.get("deep_eval_technical_weight") is not None else None
            ),
            deep_eval_financial_weight = (
                int(row["deep_eval_financial_weight"])
                if row.get("deep_eval_financial_weight") is not None else None
            ),
            deep_min_years_experience  = (
                int(row["deep_min_years_experience"])
                if row.get("deep_min_years_experience") is not None else None
            ),
            deep_min_similar_projects  = (
                int(row["deep_min_similar_projects"])
                if row.get("deep_min_similar_projects") is not None else None
            ),
            # Rich content fields
            deep_scope          = _safe_str(row.get("deep_scope"))      or None,
            deep_ai_summary     = _safe_str(row.get("deep_ai_summary")) or None,
            deep_document_links = _doc_links,
            extracted_document_count = _extracted_count,
            total_document_count = len(_doc_links),
            description         = _safe_str(row.get("description")),
        )


class TenderIntelListResponse(BaseModel):
    """Paginated response for GET /api/v1/tenders."""
    results:     List[TenderIntelItem]
    total:       int
    limit:       int
    offset:      int
    has_more:    bool             = False
    next_cursor: Optional[Dict[str, Any]] = None   # keyset cursor for O(log n) next page
    query_ms:    Optional[float]  = None

    def model_post_init(self, __context: Any) -> None:
        self.has_more = (self.offset + self.limit) < self.total


# =============================================================================
# GET /api/v1/pipeline — bid pipeline entries
# =============================================================================

VALID_STATUSES = frozenset({
    "discovered", "shortlisted", "proposal_in_progress",
    "submitted", "won", "lost",
})

STATUS_LABELS: Dict[str, str] = {
    "discovered":           "Discovered",
    "shortlisted":          "Shortlisted",
    "proposal_in_progress": "Proposal In Progress",
    "submitted":            "Submitted",
    "won":                  "Won",
    "lost":                 "Lost",
}


class PipelineEntry(BaseModel):
    """
    Single bid_pipeline row joined with tender metadata.
    Returned by GET /api/v1/pipeline.
    """
    tender_id:          str
    status:             str
    status_label:       str              = ""
    owner:              Optional[str]    = None
    notes:              Optional[str]    = None
    proposal_deadline:  Optional[date]   = None
    created_at:         Optional[datetime] = None
    updated_at:         Optional[datetime] = None

    # Tender metadata (from JOIN)
    title:              str              = ""
    url:                str              = ""
    source_site:        str              = ""
    sector:             str              = "unknown"
    region:             str              = "global"
    organization:       str              = ""
    priority_score:     int              = 0
    opportunity_insight: Optional[str]   = None

    def model_post_init(self, __context: Any) -> None:
        self.status_label = STATUS_LABELS.get(self.status, self.status.replace("_", " ").title())

    @classmethod
    def from_db_row(cls, row: dict) -> "PipelineEntry":
        return cls(
            tender_id          = _safe_str(row.get("tender_id")),
            status             = _safe_str(row.get("status"), "discovered"),
            owner              = _safe_str(row.get("owner")) or None,
            notes              = _safe_str(row.get("notes")) or None,
            proposal_deadline  = row.get("proposal_deadline"),
            created_at         = row.get("created_at"),
            updated_at         = row.get("updated_at"),
            title              = _safe_str(row.get("title")),
            url                = _safe_str(row.get("url")),
            source_site        = _safe_str(row.get("source_site")),
            sector             = _safe_str(row.get("sector"),       "unknown"),
            region             = _safe_str(row.get("region"),       "global"),
            organization       = _safe_str(row.get("organization")),
            priority_score     = _safe_int(row.get("priority_score")),
            opportunity_insight= _safe_str(row.get("opportunity_insight")) or None,
        )


class PipelineListResponse(BaseModel):
    """Paginated response for GET /api/v1/pipeline."""
    results:    List[PipelineEntry]
    total:      int
    by_status:  Dict[str, int]  = Field(default_factory=dict)
    query_ms:   Optional[float] = None


# =============================================================================
# POST /api/v1/pipeline/update — partial update
# =============================================================================

class PipelineUpdateRequest(BaseModel):
    """
    Request body for POST /api/v1/pipeline/update.
    All fields except tender_id are optional — only provided fields are written.
    """
    tender_id:          str   = Field(..., description="ID of the tender to update")
    status:             Optional[str]  = Field(None, description=f"New status. One of: {', '.join(sorted(VALID_STATUSES))}")
    owner:              Optional[str]  = Field(None, description="Owner name / email")
    notes:              Optional[str]  = Field(None, description="Free-text notes")
    proposal_deadline:  Optional[str]  = Field(None, description="Proposal deadline YYYY-MM-DD")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip().lower()
        if v not in VALID_STATUSES:
            raise ValueError(
                f"Invalid status '{v}'. Must be one of: {', '.join(sorted(VALID_STATUSES))}"
            )
        return v

    @field_validator("proposal_deadline")
    @classmethod
    def validate_deadline(cls, v: Optional[str]) -> Optional[str]:
        if v is None or not v.strip():
            return None
        from datetime import date as _date
        try:
            _date.fromisoformat(v.strip()[:10])
            return v.strip()[:10]
        except ValueError:
            raise ValueError(f"proposal_deadline must be YYYY-MM-DD, got '{v}'")


class PipelineUpdateResponse(BaseModel):
    """Response for POST /api/v1/pipeline/update."""
    success:   bool
    tender_id: str
    updated:   Dict[str, Any] = Field(default_factory=dict)
    message:   str            = ""


# =============================================================================
# GET /api/v1/summary — dashboard headline stats
# =============================================================================

class SectorCount(BaseModel):
    sector: str
    count:  int


class OrgCount(BaseModel):
    organization: str
    count:        int


class DeadlineBucketStats(BaseModel):
    """
    Counts for one deadline bucket (closing_soon / needs_action / plan_ahead).
    Tier counts are included when the decision_tag column is populated.
    """
    total:   int = 0   # tenders in this bucket
    bid_now: int = 0   # model-tagged BID_NOW within bucket
    strong:  int = 0   # model-tagged STRONG_CONSIDER within bucket


class DeadlineBreakdown(BaseModel):
    """
    Mutually exclusive deadline bucket counts.

    Guarantee:  closing_soon.total + needs_action.total + plan_ahead.total
                = active_total   (excludes unknown + expired)

    Buckets:
        closing_soon  — 0–7 days remaining
        needs_action  — 8–21 days remaining
        plan_ahead    — 22+ days remaining
        unknown       — no parseable deadline
        expired       — deadline already passed
    """
    closing_soon: DeadlineBucketStats  = Field(default_factory=DeadlineBucketStats)
    needs_action: DeadlineBucketStats  = Field(default_factory=DeadlineBucketStats)
    plan_ahead:   DeadlineBucketStats  = Field(default_factory=DeadlineBucketStats)
    unknown:      DeadlineBucketStats  = Field(default_factory=DeadlineBucketStats)
    expired:      DeadlineBucketStats  = Field(default_factory=DeadlineBucketStats)
    active_total: int                  = 0   # closing_soon + needs_action + plan_ahead


class SummaryResponse(BaseModel):
    """
    Response for GET /api/v1/summary.
    Provides all data needed to render a dashboard headline panel.
    """
    total_tenders:       int
    high_priority_count: int              # priority_score >= 70
    portals_active:      int
    pipeline_counts:     Dict[str, int]   = Field(default_factory=dict)
    top_sectors:         List[SectorCount] = Field(default_factory=list)
    top_organizations:   List[OrgCount]    = Field(default_factory=list)
    deadline_breakdown:  DeadlineBreakdown = Field(default_factory=DeadlineBreakdown)
    generated_at:        datetime          = Field(default_factory=datetime.utcnow)


# =============================================================================
# GET /api/v1/search — semantic / NL query results
# =============================================================================

class SemanticSearchResult(BaseModel):
    """
    Single result item from GET /api/v1/search.
    Combines vector similarity with DB intelligence enrichment.
    """
    tender_id:           str              = ""
    title:               str              = ""
    url:                 str              = ""
    source_site:         str              = ""
    organization:        str              = ""
    sector:              str              = "unknown"
    region:              str              = "global"
    deadline_category:   str              = "unknown"
    priority_score:      int              = 0
    bid_fit_score:       int              = 0
    opportunity_insight: Optional[str]    = None
    competition_level:   str              = "medium"
    opportunity_size:    str              = "medium"
    similarity:          float            = 0.0   # cosine similarity 0–1
    composite_score:     float            = 0.0   # final ranking score 0–1
    date_first_seen:     Optional[datetime] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SemanticSearchResult":
        """Build from a query_engine result dict; ignores unknown keys."""
        valid = {k for k in cls.model_fields}
        return cls(**{k: v for k, v in d.items() if k in valid})


class ExtractedFilters(BaseModel):
    """Filters parsed from the natural language query."""
    sectors:       List[str] = Field(default_factory=list)
    regions:       List[str] = Field(default_factory=list)
    priority_hint: Optional[str] = None   # "high" | None
    closing_soon:  bool          = False


class SemanticSearchResponse(BaseModel):
    """Response for GET /api/v1/search."""
    results:            List[SemanticSearchResult]
    total:              int
    query:              str
    filters_extracted:  ExtractedFilters  = Field(default_factory=ExtractedFilters)
    query_ms:           float             = 0.0
    vector_candidates:  int               = 0
    fallback:           bool              = False  # True when vector store was empty


# =============================================================================
# GET /api/v1/health — Scraper reliability + data quality snapshot
# =============================================================================

class PortalHealth(BaseModel):
    """
    Per-portal health record.
    stability: "stable" | "partial" | "unstable"
    """
    source:               str
    stability:            str              # stable | partial | unstable
    success_rate:         float            # 0–100 (percent of ok runs in last 10)
    average_rows:         float            # avg row_count over last 10 runs
    consecutive_failures: int             # consecutive zero-row runs (newest first)
    total_runs:           int
    last_success_time:    Optional[str]   = None   # ISO datetime of last run with rows
    disabled_reason:      Optional[str]   = None   # why auto-disabled (if unstable)


class HealthResponse(BaseModel):
    """
    Response for GET /api/v1/health.
    Returns per-portal reliability metrics + aggregate counts.
    """
    portals:         List[PortalHealth]
    stable_count:    int
    partial_count:   int
    unstable_count:  int
    generated_at:    str                  # ISO datetime


# =============================================================================
# GET /api/v1/performance — Decision accuracy & feedback loop metrics
# =============================================================================

class DataCoverage(BaseModel):
    """How many pipeline entries have evaluated outcomes."""
    total_in_pipeline: int   = 0
    evaluated:         int   = 0
    coverage_pct:      float = 0.0
    low_confidence:    bool  = True
    note:              str   = ""


class OverallMetrics(BaseModel):
    """Portfolio-level bid conversion and win rate."""
    total_evaluated:      int            = 0
    total_bids_placed:    int            = 0
    total_wins:           int            = 0
    bid_conversion_rate:  Optional[float] = None   # bids / evaluated
    win_rate:             Optional[float] = None   # wins / evaluated
    win_from_bid_rate:    Optional[float] = None   # wins / bids placed


class TierStats(BaseModel):
    """
    Per decision-tier accuracy metrics.
    All rate fields are None when total == 0 (no data for that tier).
    """
    total:               int            = 0
    bid_count:           int            = 0
    no_bid_count:        int            = 0
    win_count:           int            = 0
    loss_count:          int            = 0
    no_bid_outcome:      int            = 0
    bid_rate:            Optional[float] = None   # bid_count / total
    win_rate:            Optional[float] = None   # win_count / total
    win_from_bid_rate:   Optional[float] = None   # win_count / bid_count
    false_positive_rate: Optional[float] = None   # no_bid_count / total


class ModelAccuracy(BaseModel):
    """BID_NOW precision and recall."""
    precision_bid_now: Optional[float] = None   # fraction of BID_NOW we bid on
    recall_bid_now:    Optional[float] = None   # fraction of all wins flagged BID_NOW
    note:              str             = ""


class WinningSignalItem(BaseModel):
    """Win-rate stat for a single signal value (sector/type/org)."""
    value:      str
    win_rate:   Optional[float] = None
    win_count:  int             = 0
    loss_count: int             = 0
    sample:     int             = 0


class WinningSignals(BaseModel):
    """Top-N signal correlations by win rate."""
    consulting_type: List[WinningSignalItem] = Field(default_factory=list)
    sector:          List[WinningSignalItem] = Field(default_factory=list)
    organization:    List[WinningSignalItem] = Field(default_factory=list)
    note:            str                     = ""


class ThresholdSuggestion(BaseModel):
    """Adaptive threshold tuning suggestion from the calibrator."""
    no_change:            bool            = True
    rule_triggered:       Optional[str]   = None
    reason:               str             = ""
    confidence:           str             = "low"   # low | medium | high
    current_thresholds:   Dict[str, Any]  = Field(default_factory=dict)
    suggested_thresholds: Dict[str, Any]  = Field(default_factory=dict)


class PerformanceResponse(BaseModel):
    """
    Response for GET /api/v1/performance.

    Returns the full feedback-loop dashboard:
      - data_coverage       — how many outcomes have been recorded
      - overall             — portfolio bid conversion + win rates
      - by_tier             — per-tier accuracy metrics (BID_NOW / STRONG / WEAK / IGNORE)
      - model_accuracy      — precision & recall of BID_NOW classification
      - winning_signals     — which sectors / types / orgs correlate with wins
      - threshold_suggestion — adaptive tuning recommendation
    """
    ok:                   bool                      = True
    data_coverage:        DataCoverage              = Field(default_factory=DataCoverage)
    overall:              OverallMetrics            = Field(default_factory=OverallMetrics)
    by_tier:              Dict[str, TierStats]      = Field(default_factory=dict)
    model_accuracy:       ModelAccuracy             = Field(default_factory=ModelAccuracy)
    winning_signals:      WinningSignals            = Field(default_factory=WinningSignals)
    threshold_suggestion: ThresholdSuggestion       = Field(default_factory=ThresholdSuggestion)
    generated_at:         str                       = ""
    error:                Optional[str]             = None


# ---------------------------------------------------------------------------
# POST /api/v1/pipeline/outcome — record a bid outcome
# ---------------------------------------------------------------------------

VALID_OUTCOMES      = frozenset({"won", "lost", "no_submission", "pending"})
VALID_BID_DECISIONS = frozenset({"bid", "no_bid", "review_later"})


class OutcomeRequest(BaseModel):
    """
    Request body for POST /api/v1/pipeline/outcome.
    Records the real-world result of a tender so the calibrator can learn.
    """
    tender_id:    str = Field(..., description="Tender ID to record outcome for")
    outcome:      str = Field(..., description="One of: won, lost, no_submission, pending")
    bid_decision: str = Field("bid", description="One of: bid, no_bid, review_later")

    @field_validator("outcome")
    @classmethod
    def validate_outcome(cls, v: str) -> str:
        aliases = {
            "no bid": "no_submission",
            "no_bid": "no_submission",
            "no submission": "no_submission",
        }
        v = aliases.get(v.strip().lower(), v.strip().lower())
        if v not in VALID_OUTCOMES:
            raise ValueError(
                f"Invalid outcome '{v}'. Must be one of: {', '.join(sorted(VALID_OUTCOMES))}"
            )
        return v

    @field_validator("bid_decision")
    @classmethod
    def validate_bid_decision(cls, v: str) -> str:
        aliases = {
            "review later": "review_later",
            "later": "review_later",
            "pending": "review_later",
            "no": "no_bid",
            "no bid": "no_bid",
        }
        v = aliases.get(v.strip().lower(), v.strip().lower())
        if v not in VALID_BID_DECISIONS:
            raise ValueError(
                f"Invalid bid_decision '{v}'. "
                f"Must be one of: {', '.join(sorted(VALID_BID_DECISIONS))}"
            )
        return v

    @model_validator(mode="after")
    def validate_outcome_bid_consistency(self) -> "OutcomeRequest":
        # Deterministic consistency rules prevent polluted feedback metrics.
        if self.outcome in {"won", "lost"} and self.bid_decision != "bid":
            raise ValueError("For outcome 'won'/'lost', bid_decision must be 'bid'.")
        if self.outcome == "no_submission" and self.bid_decision not in {"no_bid", "review_later"}:
            raise ValueError("For outcome 'no_submission', bid_decision must be 'no_bid' or 'review_later'.")
        return self


class OutcomeResponse(BaseModel):
    """Response for POST /api/v1/pipeline/outcome."""
    success:   bool
    tender_id: str
    outcome:   str
    message:   str = ""


# =============================================================================
# POST /api/v1/copilot — LLM bid recommendation
# =============================================================================

class CopilotRequest(BaseModel):
    """Request body for POST /api/v1/copilot.

    mode:
      "fast"  — single LLM pass (~2-3s).  Default.
      "deep"  — 3-pass reasoning chain (~6-10s).  Richer output: win theme,
                partner recommendation, 6-dimension assessment, structured
                extraction.  Use for shortlisted / pipeline tenders.
    """
    tender_id: str  = Field(..., description="Tender ID to analyse")
    mode:      str  = Field(default="fast", description='"fast" (default) or "deep" (3-pass chain)')


class CopilotResponse(BaseModel):
    """
    Structured bid recommendation from the LLM copilot.

    Fast mode (single-pass):
      recommendation, confidence, why, risks, strategy, cached, fallback

    Deep mode (3-pass reasoning chain) adds:
      win_theme       — Core value proposition IDCG should anchor the bid on
      partner_needed  — Whether a partner / sub-contractor is needed
      partner_note    — What kind of partner (if needed)
      assessment      — 6-dimension fit scores from Pass 2
      extraction      — Structured facts extracted from the document in Pass 1
      reasoning_passes— Number of LLM passes that completed (1, 2, or 3)
    """
    tender_id:       str
    recommendation:  str                                  # "BID" | "CONSIDER" | "SKIP"
    confidence:      int                                  # 0–100
    why:             List[str]  = Field(default_factory=list)
    risks:           List[str]  = Field(default_factory=list)
    strategy:        List[str]  = Field(default_factory=list)
    cached:          bool       = False
    fallback:        bool       = False                   # True → heuristic path
    # Deep-mode extras (all optional / None in fast mode)
    win_theme:       Optional[str]  = None
    partner_needed:  Optional[bool] = None
    partner_note:    Optional[str]  = None
    assessment:      Optional[Dict[str, Any]] = None      # Pass 2 dimension scores
    extraction:      Optional[Dict[str, Any]] = None      # Pass 1 structured facts
    reasoning_passes:Optional[int] = None                 # 1, 2, or 3
