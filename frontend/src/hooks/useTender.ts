"use client";
import useSWR from "swr";
import { apiClient } from "@/lib/api";
import { swrKeys } from "@/lib/swr-keys";

export function useTender(id: string) {
  return useSWR(
    id ? swrKeys.tender(id) : null,
    () => apiClient.client.getTender(id),
    { revalidateOnFocus: false }
  );
}
