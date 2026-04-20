"use client";
// =============================================================================
// hooks/usePortalHealth.ts — SWR hook for GET /api/v1/health
// =============================================================================

import useSWR from "swr";
import { apiClient } from "@/lib/api";
import type { HealthResponse } from "@/lib/api-types";

/**
 * Fetch scraper reliability + data-quality health snapshot.
 *
 * - Revalidates every 5 minutes (health data changes slowly)
 * - Keeps previous data visible while refetching
 */
export function usePortalHealth(fallback?: HealthResponse) {
  return useSWR<HealthResponse>(
    "portal-health",
    () => apiClient.client.getHealth(),
    {
      fallbackData:         fallback,
      revalidateOnFocus:    false,
      revalidateOnReconnect: true,
      refreshInterval:      5 * 60 * 1000,  // 5 minutes
      keepPreviousData:     true,
    }
  );
}
