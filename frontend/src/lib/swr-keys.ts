// =============================================================================
// lib/swr-keys.ts — SWR cache key factory
//
// All SWR cache keys go through this module so the keys are consistent
// across hooks and easy to invalidate with mutate().
// =============================================================================

import type { TenderFilters } from "@/lib/api-types";

export const swrKeys = {
  /** Key for the tenders list — depends on full filter state */
  tenders: (filters: Partial<TenderFilters>) =>
    ["tenders", JSON.stringify(filters)] as const,

  /** Key for a single tender detail */
  tender: (id: string) =>
    ["tender", id] as const,

  /** Key for system stats */
  stats: () => ["stats"] as const,

  /** Key for portals list */
  portals: () => ["portals"] as const,
};
