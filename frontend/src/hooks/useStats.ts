"use client";
import useSWR from "swr";
import { apiClient } from "@/lib/api";
import { swrKeys } from "@/lib/swr-keys";
import type { SystemStats } from "@/lib/api-types";

export function useStats(fallbackData?: SystemStats) {
  return useSWR(
    swrKeys.stats(),
    () => apiClient.client.getStats(),
    { fallbackData, refreshInterval: 300_000, revalidateOnFocus: false }
  );
}
