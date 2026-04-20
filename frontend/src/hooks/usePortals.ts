"use client";
import useSWR from "swr";
import { apiClient } from "@/lib/api";
import { swrKeys } from "@/lib/swr-keys";
import type { PortalStats } from "@/lib/api-types";

export function usePortals(fallbackData?: PortalStats[]) {
  return useSWR(
    swrKeys.portals(),
    () => apiClient.client.getPortals(),
    { fallbackData, refreshInterval: 120_000, revalidateOnFocus: false }
  );
}
