"use client";
import useSWR from "swr";
import { apiClient } from "@/lib/api";
import type { SummaryResponse } from "@/lib/api-types";

export function useSummary(fallbackData?: SummaryResponse) {
  return useSWR(
    "/api/v1/summary",
    () => apiClient.client.getSummary(),
    {
      fallbackData,
      refreshInterval:    120_000,  // refresh every 2 minutes
      revalidateOnFocus:  false,
    }
  );
}
