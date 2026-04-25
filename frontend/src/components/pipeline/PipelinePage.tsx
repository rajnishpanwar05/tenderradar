"use client";

import { KanbanBoard } from "./KanbanBoard";
import { usePipeline } from "@/hooks/usePipeline";
import { Layers, RefreshCw } from "lucide-react";

export function PipelinePage() {
  const { data, isLoading, isValidating, mutate } = usePipeline();

  const entries  = data?.results ?? [];
  const total    = data?.total   ?? 0;

  return (
    <div className="flex flex-col gap-4 p-6 min-h-full">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight flex items-center gap-2 text-slate-950">
            <Layers className="h-6 w-6 text-slate-700" />
            Pipeline
          </h1>
          <p className="mt-0.5 text-sm text-slate-500">
            {isLoading
              ? "Loading pipeline…"
              : `${total} tender${total !== 1 ? "s" : ""} tracked  •  drag cards to move stages`}
          </p>
        </div>
        <button
          onClick={() => mutate()}
          disabled={isValidating}
          className="flex items-center gap-2 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-50 transition-colors"
          title="Refresh pipeline"
        >
          <RefreshCw className={`h-4 w-4 ${isValidating ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      {/* Kanban */}
      {isLoading ? (
        <div className="flex gap-3 overflow-x-auto py-2">
          {[0, 1, 2, 3, 4, 5].map((i) => (
            <div key={i} className="flex w-60 shrink-0 flex-col gap-2">
              <div className="h-9 animate-pulse rounded-lg bg-muted" />
              {[0, 1].map((j) => (
                <div key={j} className="h-28 animate-pulse rounded-xl bg-muted" />
              ))}
            </div>
          ))}
        </div>
      ) : (
        <KanbanBoard entries={entries} onMutate={() => mutate()} />
      )}
    </div>
  );
}
