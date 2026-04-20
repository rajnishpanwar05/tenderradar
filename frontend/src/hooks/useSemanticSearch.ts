"use client";
// =============================================================================
// hooks/useSemanticSearch.ts — SWR hook for GET /api/v1/search?q=
// =============================================================================

import useSWR from "swr";
import { apiClient } from "@/lib/api";
import type { SemanticSearchResponse } from "@/lib/api-types";

/**
 * Fetch semantic search results for a given query string.
 *
 * - Returns `undefined` data while `isLoading` is true (no stale flash)
 * - Deduplicates requests: same query string = same SWR cache key
 * - Does NOT revalidate on focus (search results don't change behind the scenes)
 *
 * @param query  Natural language query string (empty string → skip fetch)
 * @param limit  Max results (default 20)
 */
export function useSemanticSearch(query: string, limit = 20) {
  // Only fetch when query is non-empty and at least 2 chars
  const key = query.trim().length >= 2
    ? `search:${query.trim()}:${limit}`
    : null;

  return useSWR<SemanticSearchResponse>(
    key,
    () => apiClient.client.semanticSearch(query.trim(), limit),
    {
      revalidateOnFocus:    false,
      revalidateOnReconnect: false,
      keepPreviousData:     false,   // don't show stale results for a new query
      dedupingInterval:     5_000,   // 5s cache for same query
    }
  );
}
