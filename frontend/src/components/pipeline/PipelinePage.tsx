"use client";

import { KanbanBoard } from "./KanbanBoard";
import { usePipeline } from "@/hooks/usePipeline";
import { RefreshCw } from "lucide-react";

export function PipelinePage() {
  const { data, isLoading, isValidating, mutate } = usePipeline();

  const entries = data?.results ?? [];
  const total   = data?.total   ?? 0;

  return (
    <div className="flex flex-col h-[calc(100vh-56px)] overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 bg-white border-b border-slate-200 flex-shrink-0">
        <div>
          <h1 className="text-sm font-semibold text-slate-900">Pipeline</h1>
          <p className="text-xs text-slate-500 mt-0.5">
            {isLoading
              ? "Loading pipeline…"
              : `${total} tender${total !== 1 ? "s" : ""} tracked — drag cards to move stages`}
          </p>
        </div>
        <button
          onClick={() => mutate()}
          disabled={isValidating}
          className="flex items-center gap-1.5 h-8 px-3 text-xs font-medium text-slate-700 border border-slate-200 bg-white rounded-md hover:bg-slate-50 disabled:opacity-50 transition-colors"
        >
          <RefreshCw className={`h-3.5 w-3.5 ${isValidating ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      {/* Kanban area */}
      <div className="flex-1 overflow-auto p-4 bg-[#f8fafc]">
        {isLoading ? (
          <div className="flex gap-3">
            {Array.from({ length: 6 }).map((_, i) => (
              <div key={i} className="flex w-56 shrink-0 flex-col gap-2">
                <div className="h-9 animate-pulse rounded-md bg-slate-200" />
                {Array.from({ length: 2 }).map((_, j) => (
                  <div key={j} className="h-24 animate-pulse rounded-md bg-slate-100 border border-slate-200" />
                ))}
              </div>
            ))}
          </div>
        ) : (
          <KanbanBoard entries={entries} onMutate={() => mutate()} />
        )}
      </div>
    </div>
  );
}
