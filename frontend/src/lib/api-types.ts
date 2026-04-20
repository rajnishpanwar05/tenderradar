// =============================================================================
// lib/api-types.ts — TypeScript interfaces mirroring intelligence/api_schema.py
//
// These are the canonical type definitions for the entire frontend.
// Every component, hook, and page imports types from here.
// =============================================================================

// ---------------------------------------------------------------------------
// Enumerations
// ---------------------------------------------------------------------------

export type FitBucket = "HIGH" | "GOOD" | "FAIR" | "LOW";

export type SortField = "fit_score" | "scraped_at" | "deadline" | "title_clean";
export type SortOrder = "asc" | "desc";
export type ViewMode  = "table" | "cards";

// Valid sector slugs (mirrors VALID_SECTORS in api_schema.py)
export type SectorSlug =
  | "health"
  | "education"
  | "environment"
  | "agriculture"
  | "water_sanitation"
  | "urban_development"
  | "energy"
  | "governance"
  | "gender_inclusion"
  | "infrastructure"
  | "research"
  | "finance"
  | "communications"
  | "circular_economy"
  | "tourism"
  | "evaluation_monitoring";

// Valid service type slugs
export type ServiceTypeSlug =
  | "evaluation_monitoring"
  | "consulting_advisory"
  | "research_study"
  | "capacity_building"
  | "audit_finance"
  | "communications_media"
  | "project_management";

// ---------------------------------------------------------------------------
// Core tender types
// ---------------------------------------------------------------------------

/** Lightweight tender for list/search results */
export interface TenderSearchResult {
  tender_id:      string;
  source_portal:  string;
  title:          string;
  title_clean:    string;
  organization:   string;
  country:        string;
  deadline:       string | null;   // ISO date "YYYY-MM-DD"
  primary_sector: string | null;
  fit_score:      number;
  fit_bucket:     FitBucket;
  is_expired:     boolean;
  is_duplicate:   boolean;
  url:            string;
  scraped_at:     string | null;   // ISO datetime
  similarity?:    number;          // present only in semantic search results
}

/** Full tender record from GET /api/v1/tenders/{id} */
export interface TenderRecord extends TenderSearchResult {
  content_hash:         string;
  description:          string;
  word_count:           number;
  has_description:      boolean;
  sectors:              string[];
  service_types:        string[];
  semantic_score:       number;
  keyword_score:        number;
  fit_explanation:      string;
  top_reasons:          string[];
  red_flags:            string[];
  estimated_budget_usd:        number | null;
  duplicate_of:                string | null;
  deadline_raw:                string;
  value_raw?:                  string;
  // Deep enrichment fields (populated after PDF/detail page extraction)
  deep_scope?:                 string | null;
  deep_budget_raw?:            string | null;
  deep_budget_currency?:       string | null;
  deep_date_pre_bid?:          string | null;
  deep_date_qa_deadline?:      string | null;
  deep_date_contract_start?:   string | null;
  deep_contract_duration?:     string | null;
  deep_eval_technical_weight?: number | null;
  deep_eval_financial_weight?: number | null;
  deep_eval_criteria?:         string | null;
  deep_eligibility_raw?:       string | null;
  deep_min_turnover_raw?:      string | null;
  deep_min_years_experience?:  number | null;
  deep_min_similar_projects?:  number | null;
  deep_team_reqs?:             string | null;
  deep_contact_block?:         string | null;
  amendment_count?:            number;
  last_amended_at?:            string | null;
}

// ---------------------------------------------------------------------------
// API response envelopes
// ---------------------------------------------------------------------------

export interface TenderListResponse {
  results:     TenderSearchResult[];
  total:       number;
  page:        number;
  page_size:   number;
  total_pages: number;
  has_next:    boolean;
  query_ms:    number | null;
}

// ---------------------------------------------------------------------------
// Stats & portals
// ---------------------------------------------------------------------------

export interface PortalStats {
  portal:          string;
  total_tenders:   number;
  new_last_7_days: number;
  avg_fit_score:   number;
  high_fit_count:  number;
  last_scraped_at: string | null;   // ISO datetime
}

export interface SystemStats {
  total_tenders:        number;
  total_portals:        number;
  tenders_last_24h:     number;
  tenders_last_7_days:  number;
  high_fit_count:       number;
  duplicate_count:      number;
  portal_breakdown:     PortalStats[];
  sector_breakdown:     Record<string, number>;
  vector_store_docs:    number;
  generated_at:         string;    // ISO datetime
}

// ---------------------------------------------------------------------------
// Search query (mirrors TenderSearchQuery Pydantic model)
// ---------------------------------------------------------------------------

export interface TenderSearchQuery {
  q?:                 string;
  sectors?:           string[];
  service_types?:     string[];
  countries?:         string[];
  source_portals?:    string[];
  min_fit_score?:     number;
  exclude_expired?:   boolean;
  exclude_duplicates?: boolean;
  page?:              number;
  page_size?:         number;
  sort_by?:           SortField;
  sort_order?:        SortOrder;
}

// ---------------------------------------------------------------------------
// Frontend filter state (superset of TenderSearchQuery + UI-only fields)
// ---------------------------------------------------------------------------

export interface TenderFilters {
  q:                  string;
  sectors:            string[];
  service_types:      string[];
  countries:          string[];
  source_portals:     string[];
  min_fit_score:      number;
  exclude_expired:    boolean;
  exclude_duplicates: boolean;
  sort_by:            SortField;
  sort_order:         SortOrder;
  page:               number;
  page_size:          number;
  view:               ViewMode;
}

export const DEFAULT_FILTERS: TenderFilters = {
  q:                  "",
  sectors:            [],
  service_types:      [],
  countries:          [],
  source_portals:     [],
  min_fit_score:      0,
  exclude_expired:    true,
  exclude_duplicates: true,
  sort_by:            "fit_score",
  sort_order:         "desc",
  page:               1,
  page_size:          20,
  view:               "table",
};

// ---------------------------------------------------------------------------
// NEW: Intelligence-enriched tender (GET /api/v1/tenders)
// Sourced from seen_tenders JOIN tender_structured_intel
// ---------------------------------------------------------------------------

export type DeadlineCategory = "urgent" | "soon" | "normal" | "unknown";
export type CompetitionLevel = "low" | "medium" | "high";
export type OpportunitySize  = "small" | "medium" | "large";

export interface TenderIntelItem {
  tender_id:           string;
  title:               string;
  url:                 string;
  source_site:         string;
  organization:        string;
  sector:              string;          // single slug e.g. "health"
  consulting_type:     string;
  region:              string;          // e.g. "South Asia"
  deadline_category:   DeadlineCategory;
  bid_fit_score:       number;          // 0–100
  priority_score:      number;          // 0–100
  competition_level:   CompetitionLevel;
  opportunity_size:    OpportunitySize;
  opportunity_insight: string | null;
  date_first_seen:     string | null;
}

export interface TenderIntelListResponse {
  results:  TenderIntelItem[];
  total:    number;
  limit:    number;
  offset:   number;
  has_more: boolean;
  query_ms: number | null;
}

// ---------------------------------------------------------------------------
// Pipeline types
// ---------------------------------------------------------------------------

export type PipelineStatus =
  | "discovered"
  | "shortlisted"
  | "proposal_in_progress"
  | "submitted"
  | "won"
  | "lost";

export interface PipelineEntry {
  tender_id:           string;
  status:              PipelineStatus;
  status_label:        string;
  owner:               string | null;
  notes:               string | null;
  proposal_deadline:   string | null;
  created_at:          string | null;
  updated_at:          string | null;
  // Joined tender metadata
  title:               string;
  url:                 string;
  source_site:         string;
  sector:              string;
  region:              string;
  organization:        string;
  priority_score:      number;
  opportunity_insight: string | null;
}

export interface PipelineListResponse {
  results:   PipelineEntry[];
  total:     number;
  by_status: Partial<Record<PipelineStatus, number>>;
  query_ms:  number | null;
}

export interface PipelineUpdateRequest {
  tender_id:           string;
  status?:             PipelineStatus;
  owner?:              string;
  notes?:              string;
  proposal_deadline?:  string;
}

export interface PipelineUpdateResponse {
  success:   boolean;
  tender_id: string;
  updated:   Record<string, unknown>;
  message:   string;
}

// ---------------------------------------------------------------------------
// Summary / dashboard types
// ---------------------------------------------------------------------------

export interface SectorCount {
  sector: string;
  count:  number;
}

export interface OrgCount {
  organization: string;
  count:        number;
}

/**
 * Counts for a single deadline urgency bucket.
 * bid_now / strong populated when decision_tag data is available.
 */
export interface DeadlineBucketStats {
  total:   number;
  bid_now: number;
  strong:  number;
}

/**
 * Mutually exclusive deadline bucket breakdown from GET /api/v1/summary.
 * Guarantee: closing_soon.total + needs_action.total + plan_ahead.total === active_total
 */
export interface DeadlineBreakdown {
  closing_soon: DeadlineBucketStats;  // 0–7 days
  needs_action: DeadlineBucketStats;  // 8–21 days
  plan_ahead:   DeadlineBucketStats;  // 22+ days
  unknown:      DeadlineBucketStats;  // no parseable deadline
  expired:      DeadlineBucketStats;  // deadline already passed
  active_total: number;               // sum of the three active buckets
}

export interface SummaryResponse {
  total_tenders:       number;
  high_priority_count: number;
  portals_active:      number;
  pipeline_counts:     Partial<Record<PipelineStatus, number>>;
  top_sectors:         SectorCount[];
  top_organizations:   OrgCount[];
  deadline_breakdown:  DeadlineBreakdown;
  generated_at:        string;
}

// ---------------------------------------------------------------------------
// Semantic / NL search types  (GET /api/v1/search?q=)
// ---------------------------------------------------------------------------

/**
 * Single result from the semantic search endpoint.
 * Extends TenderIntelItem with similarity + composite ranking scores.
 */
export interface SemanticSearchResult extends TenderIntelItem {
  /** Cosine similarity to the query embedding (0–1) */
  similarity:      number;
  /** Final composite ranking score: 0.5×sim + 0.3×priority + 0.2×fit */
  composite_score: number;
}

/** Filters that were auto-detected from the natural language query */
export interface ExtractedFilters {
  sectors:       string[];
  regions:       string[];
  priority_hint: "high" | null;
  closing_soon:  boolean;
}

/** Response envelope for GET /api/v1/search */
export interface SemanticSearchResponse {
  results:            SemanticSearchResult[];
  total:              number;
  query:              string;
  filters_extracted:  ExtractedFilters;
  query_ms:           number;
  vector_candidates:  number;
  /** true when vector store was empty — results come from DB-only fallback */
  fallback:           boolean;
}

// ---------------------------------------------------------------------------
// Portal health types  (GET /api/v1/health)
// ---------------------------------------------------------------------------

export type PortalStability = "stable" | "partial" | "unstable";

export interface PortalHealth {
  source:               string;
  stability:            PortalStability;
  /** Percent of runs (last 10) that returned rows */
  success_rate:         number;
  /** Mean row count over last 10 runs */
  average_rows:         number;
  /** Number of most-recent consecutive zero-row runs */
  consecutive_failures: number;
  total_runs:           number;
  last_success_time:    string | null;
  /** Populated when stability === "unstable" */
  disabled_reason:      string | null;
}

export interface HealthResponse {
  portals:         PortalHealth[];
  stable_count:    number;
  partial_count:   number;
  unstable_count:  number;
  generated_at:    string;
}

// ---------------------------------------------------------------------------
// Copilot / LLM bid recommendation types  (POST /api/v1/copilot)
// ---------------------------------------------------------------------------

export type CopilotVerdict = "BID" | "CONSIDER" | "SKIP";

export interface CopilotRequest {
  tender_id: string;
  mode?: "fast" | "deep";
}

export interface CopilotResponse {
  tender_id:      string;
  recommendation: CopilotVerdict;
  /** Confidence level 0–100 */
  confidence:     number;
  /** Top reasons supporting the recommendation */
  why:            string[];
  /** Risks and concerns */
  risks:          string[];
  /** Concrete next-action steps */
  strategy:       string[];
  /** True if this was served from the in-process cache */
  cached:         boolean;
  /** True if the LLM was unavailable and heuristics were used instead */
  fallback:       boolean;
  /** Deep-mode extras */
  win_theme?:       string | null;
  partner_needed?:  boolean | null;
  partner_note?:    string | null;
  assessment?:      Record<string, unknown> | null;
  extraction?:      Record<string, unknown> | null;
  reasoning_passes?: number | null;
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

export interface ApiErrorBody {
  error?:  string;
  detail?: string;
  path?:   string;
}
