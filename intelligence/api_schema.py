# =============================================================================
# intelligence/api_schema.py — Pydantic Models for the TenderRadar REST API
#
# These schemas define the public contract for:
#   GET  /api/v1/tenders          → TenderListResponse
#   GET  /api/v1/tenders/{id}     → TenderRecord
#   POST /api/v1/search           → TenderSearchResult
#   GET  /api/v1/stats            → SystemStats
#
# All models use Pydantic v2. They are designed to be:
#   - Serialisable to JSON (FastAPI response_model)
#   - Reusable in dashboard.py for rendering
#   - Forward-compatible with a future public SaaS API
#
# Import:
#   from intelligence.api_schema import TenderRecord, TenderListResponse
# =============================================================================

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# =============================================================================
# ENUMERATIONS (string literals — avoids enum import overhead)
# =============================================================================

# Valid sector slugs (matches intelligence/classifier.py output)
VALID_SECTORS = {
    "health", "education", "environment", "agriculture", "water_sanitation",
    "urban_development", "energy", "governance", "gender_inclusion",
    "infrastructure", "research", "finance", "communications",
    "circular_economy", "tourism", "evaluation_monitoring",
}

# Valid service type slugs
VALID_SERVICE_TYPES = {
    "evaluation_monitoring", "consulting_advisory", "research_study",
    "capacity_building", "audit_finance", "communications_media",
    "project_management",
}

# Fit-score bucket labels
FIT_BUCKET_HIGH = "HIGH"     # ≥ 80
FIT_BUCKET_GOOD = "GOOD"     # 65–79
FIT_BUCKET_FAIR = "FAIR"     # 50–64
FIT_BUCKET_LOW  = "LOW"      # < 50


# =============================================================================
# CORE TENDER RECORD
# Represents one fully normalised + enriched tender from the database.
# =============================================================================

class TenderRecord(BaseModel):
    """
    Complete tender record returned by GET /api/v1/tenders/{id}.
    """

    # ── Identity ──────────────────────────────────────────────────────────────
    tender_id:      str   = Field(...,  description="Stable unique ID (URL hash)")
    content_hash:   str   = Field("",  description="MD5 cross-portal dedup fingerprint")
    source_portal:  str   = Field(...,  description="Portal slug (e.g. 'worldbank', 'undp')")
    url:            str   = Field(...,  description="Direct link to the tender page")

    # ── Core metadata ─────────────────────────────────────────────────────────
    title:          str   = Field(...,  description="Raw title from source")
    title_clean:    str   = Field("",   description="Normalised title (no ref numbers)")
    organization:   str   = Field("",   description="Publishing organisation")
    country:        str   = Field("",   description="Primary geography")
    deadline:       Optional[date] = Field(None, description="Submission deadline")
    deadline_raw:   str   = Field("",   description="Original deadline string")

    # ── Description ───────────────────────────────────────────────────────────
    description:    str   = Field("",   description="Full tender description / summary")
    word_count:     int   = Field(0,    description="Description word count")
    has_description: bool = Field(False, description="True if description is non-empty")

    # ── Classification ────────────────────────────────────────────────────────
    sectors:        List[str] = Field(default_factory=list,
                                      description="Sector slugs from classifier")
    service_types:  List[str] = Field(default_factory=list,
                                      description="Service type slugs from classifier")
    primary_sector: Optional[str] = Field(None, description="Top-ranked sector")

    # ── AI enrichment ─────────────────────────────────────────────────────────
    fit_score:      float  = Field(0.0,  description="Composite relevance score 0–100")
    semantic_score: float  = Field(0.0,  description="Embedding similarity score 0–100")
    keyword_score:  float  = Field(0.0,  description="Keyword match score 0–100")
    fit_bucket:     str    = Field("LOW", description="HIGH / GOOD / FAIR / LOW")
    fit_explanation: str   = Field("",   description="Human-readable fit rationale")
    top_reasons:    List[str] = Field(default_factory=list,
                                       description="Top 3 fit reasons")
    red_flags:      List[str] = Field(default_factory=list,
                                       description="Reasons this tender may be unsuitable")

    # ── Financial ─────────────────────────────────────────────────────────────
    estimated_budget_usd: Optional[int] = Field(None, description="Budget in USD if extractable")

    # ── Deep enrichment (populated by deep_scraper after PDF/detail extraction) ──
    deep_scope:                  Optional[str]   = Field(None, description="Detailed scope of work summary")
    deep_budget_raw:             Optional[str]   = Field(None, description="Raw budget string from document")
    deep_budget_currency:        Optional[str]   = Field(None, description="Currency of the budget")
    deep_date_pre_bid:           Optional[str]   = Field(None, description="Pre-bid meeting date")
    deep_date_qa_deadline:       Optional[str]   = Field(None, description="Q&A / clarification deadline")
    deep_date_contract_start:    Optional[str]   = Field(None, description="Expected contract start date")
    deep_contract_duration:      Optional[str]   = Field(None, description="Contract duration (e.g. '18 months')")
    deep_eval_technical_weight:  Optional[int]   = Field(None, description="Technical evaluation weight %")
    deep_eval_financial_weight:  Optional[int]   = Field(None, description="Financial evaluation weight %")
    deep_eval_criteria:          Optional[str]   = Field(None, description="Evaluation criteria text")
    deep_eligibility_raw:        Optional[str]   = Field(None, description="Eligibility requirements text")
    deep_min_turnover_raw:       Optional[str]   = Field(None, description="Minimum annual turnover requirement")
    deep_min_years_experience:   Optional[int]   = Field(None, description="Minimum years of experience required")
    deep_min_similar_projects:   Optional[int]   = Field(None, description="Minimum number of similar past projects")
    deep_team_reqs:              Optional[str]   = Field(None, description="Team composition requirements")
    deep_contact_block:          Optional[str]   = Field(None, description="Contact person / submission address")
    amendment_count:             int             = Field(0,    description="Number of amendments detected")
    last_amended_at:             Optional[datetime] = Field(None, description="When the last amendment was detected")
    deep_ai_summary:             Optional[str]   = Field(None, description="AI-generated grounded summary of the tender")
    deep_document_links:         List[Dict[str, Any]] = Field(default_factory=list, description="Documents found: [{url, label, file_type, extracted, char_count}]")
    extracted_document_count:    int             = Field(0, description="Number of linked documents successfully text-extracted")
    total_document_count:        int             = Field(0, description="Total number of linked documents discovered")
    extracted_text_chars:        int             = Field(0, description="Approximate extracted text character count from document links")
    deep_evidence_snippets:      List[str]       = Field(default_factory=list, description="Grounded evidence snippets from extracted deep text")

    # ── Deduplication & cross-portal sources ─────────────────────────────────
    is_duplicate:   bool           = Field(False, description="True if cross-portal duplicate")
    duplicate_of:   Optional[str]  = Field(None,  description="tender_id of the canonical original")
    cross_sources:  List[Dict[str, str]] = Field(
        default_factory=list,
        description="All portals where this tender appears: [{portal, url}, ...]"
    )

    # ── Timestamps ────────────────────────────────────────────────────────────
    scraped_at:     datetime = Field(default_factory=datetime.now)
    is_expired:     bool     = Field(False, description="True if deadline has passed")

    # ── Computed helpers ──────────────────────────────────────────────────────
    @field_validator("fit_bucket", mode="before")
    @classmethod
    def derive_fit_bucket(cls, v: Any, info: Any) -> str:
        """Allow explicit override, or derive from fit_score if not set."""
        if v and v in (FIT_BUCKET_HIGH, FIT_BUCKET_GOOD, FIT_BUCKET_FAIR, FIT_BUCKET_LOW):
            return v
        # Access sibling field via info.data (pydantic v2 pattern)
        score = info.data.get("fit_score", 0.0) if hasattr(info, "data") else 0.0
        return _score_to_bucket(score)

    @classmethod
    def from_db_row(cls, row: dict) -> "TenderRecord":
        """
        Construct from a MySQL dict row.
        Handles JSON-encoded columns (sectors, service_types, red_flags, top_reasons).
        """
        import json as _json

        def _parse_json(val: Any, default: Any) -> Any:
            if val is None:
                return default
            if isinstance(val, (list, dict)):
                return val
            try:
                return _json.loads(val)
            except Exception:
                return default

        doc_links = _parse_json(row.get("deep_document_links"), [])
        if not isinstance(doc_links, list):
            doc_links = []

        extracted_document_count = 0
        extracted_text_chars = 0
        for d in doc_links:
            if not isinstance(d, dict):
                continue
            if bool(d.get("extracted")):
                extracted_document_count += 1
            try:
                extracted_text_chars += int(d.get("char_count") or 0)
            except Exception:
                pass

        evidence_snippets: list[str] = []
        for key in ("deep_scope", "deep_eval_criteria", "deep_team_reqs", "deep_pdf_text", "description"):
            txt = str(row.get(key) or "").strip()
            if not txt:
                continue
            cleaned = " ".join(txt.split())
            if len(cleaned) < 60:
                continue
            evidence_snippets.append(cleaned[:320])
            if len(evidence_snippets) >= 5:
                break

        return cls(
            tender_id             = row.get("tender_id", ""),
            content_hash          = row.get("content_hash", ""),
            source_portal         = row.get("source_portal", ""),
            url                   = row.get("url", ""),
            title                 = row.get("title", ""),
            title_clean           = row.get("title_clean", ""),
            organization          = row.get("organization", ""),
            country               = row.get("country", ""),
            deadline              = row.get("deadline"),
            deadline_raw          = row.get("deadline_raw", ""),
            description           = row.get("description", ""),
            word_count            = row.get("word_count", 0) or 0,
            has_description       = bool(row.get("has_description", False)),
            sectors               = _parse_json(row.get("sectors"), []),
            service_types         = _parse_json(row.get("service_types"), []),
            primary_sector        = row.get("primary_sector"),
            fit_score             = float(row.get("fit_score") or 0.0),
            semantic_score        = float(row.get("semantic_score") or 0.0),
            keyword_score         = float(row.get("keyword_score") or 0.0),
            fit_bucket            = _score_to_bucket(float(row.get("fit_score") or 0.0)),
            fit_explanation       = row.get("fit_explanation") or "",
            top_reasons           = _parse_json(row.get("top_reasons"), []),
            red_flags             = _parse_json(row.get("red_flags"), []),
            estimated_budget_usd         = row.get("estimated_budget_usd"),
            deep_scope                   = row.get("deep_scope") or None,
            deep_budget_raw              = row.get("deep_budget_raw") or None,
            deep_budget_currency         = row.get("deep_budget_currency") or None,
            deep_date_pre_bid            = row.get("deep_date_pre_bid") or None,
            deep_date_qa_deadline        = row.get("deep_date_qa_deadline") or None,
            deep_date_contract_start     = row.get("deep_date_contract_start") or None,
            deep_contract_duration       = row.get("deep_contract_duration") or None,
            deep_eval_technical_weight   = row.get("deep_eval_technical_weight") or None,
            deep_eval_financial_weight   = row.get("deep_eval_financial_weight") or None,
            deep_eval_criteria           = row.get("deep_eval_criteria") or None,
            deep_eligibility_raw         = row.get("deep_eligibility_raw") or None,
            deep_min_turnover_raw        = row.get("deep_min_turnover_raw") or None,
            deep_min_years_experience    = row.get("deep_min_years_experience") or None,
            deep_min_similar_projects    = row.get("deep_min_similar_projects") or None,
            deep_team_reqs               = row.get("deep_team_reqs") or None,
            deep_contact_block           = row.get("deep_contact_block") or None,
            amendment_count              = int(row.get("amendment_count") or 0),
            last_amended_at              = row.get("last_amended_at") or None,
            deep_ai_summary              = row.get("deep_ai_summary") or None,
            deep_document_links          = doc_links,
            extracted_document_count     = extracted_document_count,
            total_document_count         = len(doc_links),
            extracted_text_chars         = extracted_text_chars,
            deep_evidence_snippets       = evidence_snippets,
            is_duplicate          = bool(row.get("is_duplicate", False)),
            duplicate_of          = row.get("duplicate_of"),
            scraped_at            = row.get("scraped_at") or datetime.now(),
            is_expired            = bool(row.get("is_expired", False)),
        )


# =============================================================================
# SEARCH
# =============================================================================

class TenderSearchQuery(BaseModel):
    """
    POST /api/v1/search  — request body.
    Supports free-text + structured filters.
    """
    q:              str   = Field("", description="Free-text search query")
    sectors:        List[str] = Field(default_factory=list)
    service_types:  List[str] = Field(default_factory=list)
    countries:      List[str] = Field(default_factory=list)
    source_portals: List[str] = Field(default_factory=list)
    min_fit_score:  float     = Field(0.0,   ge=0.0, le=100.0)
    exclude_expired: bool     = Field(True)
    exclude_duplicates: bool  = Field(True)
    page:           int       = Field(1,     ge=1)
    page_size:      int       = Field(20,    ge=1, le=100)
    sort_by:        str       = Field("fit_score",
                                      description="fit_score | scraped_at | deadline")
    sort_order:     str       = Field("desc", description="asc | desc")

    @field_validator("sort_by")
    @classmethod
    def validate_sort_by(cls, v: str) -> str:
        v = (v or "").strip().lower()
        # Backward-compatible alias
        if v == "title":
            v = "title_clean"
        allowed = {"fit_score", "scraped_at", "deadline", "title_clean"}
        if v not in allowed:
            raise ValueError(f"sort_by must be one of {sorted(allowed)}")
        return v


class TenderSearchResult(BaseModel):
    """
    Single item in a search result set — lighter than full TenderRecord.
    """
    tender_id:      str
    source_portal:  str
    title:          str
    title_clean:    str = ""
    organization:   str = ""
    country:        str = ""
    deadline:       Optional[date] = None
    primary_sector: Optional[str]  = None
    fit_score:      float          = 0.0
    fit_bucket:     str            = "LOW"
    is_expired:     bool           = False
    is_duplicate:   bool           = False
    url:            str            = ""
    scraped_at:     Optional[datetime] = None
    similarity:     Optional[float]    = None   # set by semantic search


# =============================================================================
# LIST RESPONSE (paginated)
# =============================================================================

class TenderListResponse(BaseModel):
    """
    GET /api/v1/tenders  — paginated list response.
    Also used as the response body for POST /api/v1/search.
    """
    results:      List[TenderSearchResult]
    total:        int   = Field(...,  description="Total matching records (un-paginated)")
    page:         int   = Field(...,  description="Current page number (1-based)")
    page_size:    int   = Field(...,  description="Items per page")
    total_pages:  int   = Field(...,  description="Total pages")
    has_next:     bool  = Field(...,  description="True if more pages exist")
    query_ms:     Optional[float] = Field(None, description="Query execution time ms")

    @model_validator(mode="after")
    def derive_pagination(self) -> "TenderListResponse":
        if self.total_pages == 0 and self.page_size > 0:
            self.total_pages = max(1, -(-self.total // self.page_size))  # ceiling div
        self.has_next = self.page < self.total_pages
        return self


# =============================================================================
# STATS
# =============================================================================

class PortalStats(BaseModel):
    """Per-portal statistics row."""
    portal:          str
    total_tenders:   int
    new_last_7_days: int
    avg_fit_score:   float
    high_fit_count:  int   = 0   # fit_score >= 80
    last_scraped_at: Optional[datetime] = None


class SystemStats(BaseModel):
    """
    GET /api/v1/stats
    High-level system health + data coverage summary.
    """
    total_tenders:        int
    total_portals:        int
    tenders_last_24h:     int
    tenders_last_7_days:  int
    high_fit_count:       int
    duplicate_count:      int
    portal_breakdown:     List[PortalStats] = Field(default_factory=list)
    sector_breakdown:     Dict[str, int]    = Field(default_factory=dict)
    vector_store_docs:    int               = 0
    generated_at:         datetime          = Field(default_factory=datetime.now)


# =============================================================================
# Helpers
# =============================================================================

def _score_to_bucket(score: float) -> str:
    """Convert numeric fit_score to human-readable bucket label."""
    if score >= 80:
        return FIT_BUCKET_HIGH
    if score >= 65:
        return FIT_BUCKET_GOOD
    if score >= 50:
        return FIT_BUCKET_FAIR
    return FIT_BUCKET_LOW
