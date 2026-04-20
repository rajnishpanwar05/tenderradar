// =============================================================================
// lib/api.ts — TenderRadar API Client
//
// Two namespaces:
//   apiClient.server.*  — use in Next.js Server Components (direct fetch to
//                         FastAPI, ISR revalidation via next: { revalidate })
//   apiClient.client.*  — use in SWR hooks / Client Components
//                         (goes through /api/proxy/* route handler so the
//                          backend URL is never exposed in browser bundles)
//
// Usage:
//   import { apiClient } from "@/lib/api"
//   const stats = await apiClient.server.getStats()    // Server Component
//   const stats = await apiClient.client.getStats()    // SWR hook
// =============================================================================

import type {
  PortalStats,
  SystemStats,
  TenderFilters,
  TenderIntelListResponse,
  TenderListResponse,
  TenderRecord,
  TenderSearchQuery,
  PipelineListResponse,
  PipelineUpdateRequest,
  PipelineUpdateResponse,
  SummaryResponse,
  SemanticSearchResponse,
  CopilotRequest,
  CopilotResponse,
  HealthResponse,
} from "@/lib/api-types";

// ---------------------------------------------------------------------------
// Base URLs
// ---------------------------------------------------------------------------

const SERVER_BASE = `${process.env.API_URL ?? "http://localhost:8000"}/api/v1`;

// In the browser, always go through the Next.js proxy route (/api/proxy/*)
// which forwards to the backend server-side. This works in all environments
// (local, preview sandbox, production) without exposing the backend URL.
const CLIENT_BASE =
  typeof window !== "undefined"
    ? `${window.location.origin}/api/proxy`
    : SERVER_BASE;

// ---------------------------------------------------------------------------
// Custom error
// ---------------------------------------------------------------------------

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly detail: string,
    public readonly path: string
  ) {
    super(`API ${status} on ${path}: ${detail}`);
    this.name = "ApiError";
  }

  get isNotFound() { return this.status === 404; }
  get isServerError() { return this.status >= 500; }
}

// ---------------------------------------------------------------------------
// Core fetch helper
// ---------------------------------------------------------------------------

type FetchOptions = Omit<RequestInit, "body"> & {
  params?: Record<string, string | string[] | number | boolean | undefined | null>;
  body?:   unknown;
  next?:   { revalidate?: number | false; tags?: string[] };
};

async function apiFetch<T>(base: string, path: string, opts: FetchOptions = {}): Promise<T> {
  const { params, body, next, ...rest } = opts;

  const url = new URL(`${base}${path}`);

  if (params) {
    for (const [key, val] of Object.entries(params)) {
      if (val === undefined || val === null) continue;
      if (Array.isArray(val)) {
        // Repeated query params: ?sectors=health&sectors=education
        for (const v of val) url.searchParams.append(key, String(v));
      } else {
        url.searchParams.set(key, String(val));
      }
    }
  }

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(rest.headers as Record<string, string> | undefined),
  };

  // Server-side calls go directly to FastAPI — attach API key.
  // Client-side calls go through /api/proxy/* which injects the key server-side.
  if (typeof window === "undefined") {
    const apiKey = process.env.API_SECRET_KEY ?? "";
    if (apiKey) headers["X-API-Key"] = apiKey;
  }

  const fetchOpts: RequestInit & { next?: unknown } = {
    ...rest,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  };

  // Next.js ISR revalidation — only meaningful in Server Components
  if (next !== undefined) {
    (fetchOpts as Record<string, unknown>).next = next;
  }

  const res = await fetch(url.toString(), fetchOpts);

  if (!res.ok) {
    const err = await res.json().catch(() => ({})) as Record<string, unknown>;
    throw new ApiError(
      res.status,
      String(err.detail ?? err.error ?? res.statusText),
      path
    );
  }

  return res.json() as Promise<T>;
}

// ---------------------------------------------------------------------------
// Shared operation definitions (used by both server and client namespaces)
// ---------------------------------------------------------------------------

function filtersToParams(f: Partial<TenderFilters>): Record<string, string | string[] | number | boolean | undefined> {
  return {
    q:                  f.q         || undefined,
    sectors:            f.sectors?.length   ? f.sectors   : undefined,
    service_types:      f.service_types?.length ? f.service_types : undefined,
    countries:          f.countries?.length  ? f.countries : undefined,
    source_portals:     f.source_portals?.length ? f.source_portals : undefined,
    min_fit_score:      f.min_fit_score || undefined,
    exclude_expired:    f.exclude_expired,
    exclude_duplicates: f.exclude_duplicates,
    sort_by:            f.sort_by,
    sort_order:         f.sort_order,
    page:               f.page,
    page_size:          f.page_size,
  };
}

function intelFiltersToParams(f: Partial<TenderFilters>): Record<string, string | number | undefined> {
  return {
    limit:        f.page_size ?? 50,
    offset:       f.page_size && f.page ? (f.page - 1) * f.page_size : 0,
    sector:       f.sectors?.[0] || undefined,
    region:       (f as { region?: string }).region || undefined,
    min_priority: (f as { min_priority?: number }).min_priority || undefined,
    source_site:  (f as { source_site?: string }).source_site || undefined,
  };
}

// ---------------------------------------------------------------------------
// Public client
// ---------------------------------------------------------------------------

export const apiClient = {
  // ── Server namespace ──────────────────────────────────────────────────────
  server: {
    getStats: () =>
      apiFetch<SystemStats>(SERVER_BASE, "/stats", { next: { revalidate: 60 } }),

    getSummary: () =>
      apiFetch<SummaryResponse>(SERVER_BASE, "/summary", { next: { revalidate: 30 } }),

    getPortals: () =>
      apiFetch<PortalStats[]>(SERVER_BASE, "/portals", { next: { revalidate: 60 } }),

    getTender: (id: string) =>
      apiFetch<TenderRecord>(SERVER_BASE, `/tenders/${encodeURIComponent(id)}`, {
        next: { revalidate: 30 },
      }),

    getTenders: (filters: Partial<TenderFilters> = {}) =>
      apiFetch<TenderIntelListResponse>(SERVER_BASE, "/tenders", {
        params: intelFiltersToParams(filters),
        next:   { revalidate: 30 },
      }),
  },

  // ── Client namespace (SWR hooks) ──────────────────────────────────────────
  client: {
    getStats: () =>
      apiFetch<SystemStats>(CLIENT_BASE, "/stats"),

    getSummary: () =>
      apiFetch<SummaryResponse>(CLIENT_BASE, "/summary"),

    getPortals: () =>
      apiFetch<PortalStats[]>(CLIENT_BASE, "/portals"),

    getTender: (id: string) =>
      apiFetch<TenderRecord>(CLIENT_BASE, `/tenders/${encodeURIComponent(id)}`),

    getTenders: (filters: Partial<TenderFilters> = {}) =>
      apiFetch<TenderIntelListResponse>(CLIENT_BASE, "/tenders", {
        params: intelFiltersToParams(filters),
      }),

    searchTenders: (body: TenderSearchQuery) =>
      apiFetch<TenderListResponse>(CLIENT_BASE, "/tenders/search", {
        method: "POST",
        body,
      }),

    getPipeline: (status?: string, owner?: string) =>
      apiFetch<PipelineListResponse>(CLIENT_BASE, "/pipeline", {
        params: { status: status || undefined, owner: owner || undefined, limit: 500 },
      }),

    updatePipeline: (body: PipelineUpdateRequest) =>
      apiFetch<PipelineUpdateResponse>(CLIENT_BASE, "/pipeline/update", {
        method: "POST",
        body,
      }),

    recordOutcome: (tender_id: string, outcome: "won" | "lost" | "no_submission" | "pending", bid_decision: "bid" | "no_bid" | "review_later" = "bid") =>
      apiFetch<{ success: boolean; tender_id: string; outcome: string; message: string }>(
        CLIENT_BASE, "/pipeline/outcome", {
          method: "POST",
          body:   { tender_id, outcome, bid_decision },
        }
      ),

    /** Natural language semantic search — GET /api/v1/search?q=&limit= */
    semanticSearch: (q: string, limit = 20) =>
      apiFetch<SemanticSearchResponse>(CLIENT_BASE, "/search", {
        params: { q, limit },
      }),

    /** LLM bid recommendation — POST /api/v1/copilot */
    getCopilotRecommendation: (tender_id: string, mode: "fast" | "deep" = "fast") =>
      apiFetch<CopilotResponse>(CLIENT_BASE, "/copilot", {
        method: "POST",
        body:   { tender_id, mode } satisfies CopilotRequest,
      }),

    /** Scraper reliability + data quality snapshot — GET /api/v1/health */
    getHealth: () =>
      apiFetch<HealthResponse>(CLIENT_BASE, "/health"),
  },
};
