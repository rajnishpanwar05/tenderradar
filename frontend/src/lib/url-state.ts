// =============================================================================
// lib/url-state.ts — Typed URL search param helpers
//
// All filter state lives in the URL. These helpers convert between
// TenderFilters objects and URLSearchParams so filters are shareable,
// bookmarkable, and preserved across browser navigation.
// =============================================================================

import type { TenderFilters, SortField, SortOrder, ViewMode } from "@/lib/api-types";
import { DEFAULT_FILTERS } from "@/lib/api-types";

// ---------------------------------------------------------------------------
// Parse URL params → TenderFilters
// ---------------------------------------------------------------------------

export function parseFiltersFromUrl(params: URLSearchParams): TenderFilters {
  return {
    q:                  params.get("q")               ?? DEFAULT_FILTERS.q,
    sectors:            params.getAll("sectors"),
    service_types:      params.getAll("service_types"),
    countries:          params.getAll("countries"),
    source_portals:     params.getAll("source_portals"),
    min_fit_score:      Number(params.get("min_fit_score") ?? DEFAULT_FILTERS.min_fit_score),
    exclude_expired:    params.get("exclude_expired")    !== "false",
    exclude_duplicates: params.get("exclude_duplicates") !== "false",
    sort_by:            (params.get("sort_by")    as SortField | null) ?? DEFAULT_FILTERS.sort_by,
    sort_order:         (params.get("sort_order") as SortOrder | null) ?? DEFAULT_FILTERS.sort_order,
    page:               Number(params.get("page")      ?? DEFAULT_FILTERS.page),
    page_size:          Number(params.get("page_size") ?? DEFAULT_FILTERS.page_size),
    view:               (params.get("view") as ViewMode | null) ?? DEFAULT_FILTERS.view,
  };
}

// ---------------------------------------------------------------------------
// TenderFilters → URLSearchParams
// ---------------------------------------------------------------------------

export function filtersToSearchParams(f: TenderFilters): URLSearchParams {
  const p = new URLSearchParams();

  if (f.q)                                    p.set("q", f.q);
  f.sectors.forEach(s      => p.append("sectors",        s));
  f.service_types.forEach(s => p.append("service_types", s));
  f.countries.forEach(c    => p.append("countries",      c));
  f.source_portals.forEach(s => p.append("source_portals", s));
  if (f.min_fit_score > 0)                    p.set("min_fit_score",      String(f.min_fit_score));
  if (!f.exclude_expired)                     p.set("exclude_expired",    "false");
  if (!f.exclude_duplicates)                  p.set("exclude_duplicates", "false");
  if (f.sort_by    !== DEFAULT_FILTERS.sort_by)    p.set("sort_by",    f.sort_by);
  if (f.sort_order !== DEFAULT_FILTERS.sort_order) p.set("sort_order", f.sort_order);
  if (f.page       !== DEFAULT_FILTERS.page)       p.set("page",       String(f.page));
  if (f.page_size  !== DEFAULT_FILTERS.page_size)  p.set("page_size",  String(f.page_size));
  if (f.view       !== DEFAULT_FILTERS.view)       p.set("view",       f.view);

  return p;
}

// ---------------------------------------------------------------------------
// Check if any non-default filter is active (for "Reset filters" visibility)
// ---------------------------------------------------------------------------

export function hasActiveFilters(f: TenderFilters): boolean {
  return (
    f.q !== "" ||
    f.sectors.length > 0 ||
    f.service_types.length > 0 ||
    f.countries.length > 0 ||
    f.source_portals.length > 0 ||
    f.min_fit_score > 0 ||
    !f.exclude_expired ||
    !f.exclude_duplicates
  );
}

// ---------------------------------------------------------------------------
// Toggle a value in an array filter
// ---------------------------------------------------------------------------

export function toggleArrayItem(arr: string[], item: string): string[] {
  return arr.includes(item) ? arr.filter(v => v !== item) : [...arr, item];
}
