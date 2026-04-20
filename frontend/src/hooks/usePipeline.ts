"use client";
import useSWR from "swr";
import { apiClient } from "@/lib/api";
import type { PipelineListResponse, PipelineUpdateRequest } from "@/lib/api-types";
import { toast } from "sonner";

export function usePipeline(statusFilter?: string) {
  const key = statusFilter ? `/api/v1/pipeline?status=${statusFilter}` : "/api/v1/pipeline";

  const swr = useSWR<PipelineListResponse>(
    key,
    () => apiClient.client.getPipeline(statusFilter),
    {
      revalidateOnFocus:   false,
      keepPreviousData:    true,
    }
  );

  async function updateEntry(req: PipelineUpdateRequest) {
    try {
      await apiClient.client.updatePipeline(req);
      await swr.mutate();
      toast.success("Pipeline updated");
    } catch (err) {
      toast.error("Failed to update pipeline entry");
      throw err;
    }
  }

  return { ...swr, updateEntry };
}
