"use client";
import useSWR from "swr";
import { apiClient } from "@/lib/api";
import type { TenderFilters } from "@/lib/api-types";

function buildKey(filters: Partial<TenderFilters>): string {
  // Stable cache key from filters that affect intel endpoint
  const f = filters as Record<string, unknown>;
  return JSON.stringify({
    limit:        f.page_size ?? 50,
    offset:       f.page_size && f.page ? (Number(f.page) - 1) * Number(f.page_size) : 0,
    sector:       (filters.sectors as string[] | undefined)?.[0] ?? f.sector ?? "",
    region:       f.region ?? "",
    min_priority: f.min_priority ?? 0,
    source_site:  f.source_site ?? "",
  });
}

export function useTenders(filters: Partial<TenderFilters>) {
  return useSWR(
    buildKey(filters),
    () => apiClient.client.getTenders(filters),
    { keepPreviousData: true, revalidateOnFocus: false }
  );
}
