"use client";

import { useState } from "react";
import { Activity, RefreshCw, ChevronDown, ChevronUp } from "lucide-react";
import { cn } from "@/lib/utils";
import { usePortalHealth } from "@/hooks/usePortalHealth";
import type { PortalHealth, PortalStability } from "@/lib/api-types";

const STABILITY: Record<PortalStability, { dot: string; label: string; row: string }> = {
  stable:   { dot: "bg-emerald-500", label: "Stable",   row: "border-slate-200" },
  partial:  { dot: "bg-amber-400",   label: "Partial",  row: "border-slate-200" },
  unstable: { dot: "bg-rose-500",      label: "Unstable", row: "border-slate-200" },
};

function PortalRow({ portal }: { portal: PortalHealth }) {
  const [expanded, setExpanded] = useState(false);
  const cfg = STABILITY[portal.stability];

  return (
    <div className={cn("rounded-lg border bg-white transition-colors", cfg.row, expanded && "bg-slate-50")}>
      <button type="button" onClick={() => setExpanded(o => !o)}
        className="flex w-full items-center gap-2.5 px-3 py-2.5 text-left">
        <span className={cn("w-2 h-2 rounded-full flex-shrink-0", cfg.dot)} />
        <span className="flex-1 truncate text-sm text-slate-700">{portal.source}</span>
        <span className="text-[11px] text-slate-500 font-mono tabular-nums">{portal.success_rate.toFixed(0)}%</span>
        <span className="text-[11px] text-slate-400 font-mono tabular-nums">~{Math.round(portal.average_rows)}r</span>
        {expanded ? <ChevronUp className="h-3 w-3 text-slate-400" /> : <ChevronDown className="h-3 w-3 text-slate-400" />}
      </button>

      {expanded && (
        <div className="px-3 pb-3 pt-0 space-y-1.5 border-t border-slate-200 text-xs text-slate-500">
          <div className="flex justify-between pt-2">
            <span>Total runs</span>
            <span className="font-mono text-slate-700">{portal.total_runs}</span>
          </div>
          <div className="flex justify-between">
            <span>Consecutive failures</span>
            <span className={cn("font-mono", portal.consecutive_failures >= 3 ? "text-rose-600" : portal.consecutive_failures >= 1 ? "text-amber-600" : "text-slate-700")}>
              {portal.consecutive_failures}
            </span>
          </div>
          {portal.last_success_time && (
            <div className="flex justify-between">
              <span>Last success</span>
              <span className="font-mono text-slate-700">{new Date(portal.last_success_time).toLocaleDateString()}</span>
            </div>
          )}
          {portal.disabled_reason && (
            <p className="mt-2 rounded-md bg-rose-50 px-2 py-1 text-rose-700 border border-rose-200">
              {portal.disabled_reason}
            </p>
          )}
        </div>
      )}
    </div>
  );
}

export function PortalHealthPanel() {
  const { data, isLoading, isValidating, mutate } = usePortalHealth();
  const [showAll, setShowAll] = useState(false);
  const portals = data?.portals ?? [];
  const visible = showAll ? portals : portals.slice(0, 7);

  return (
    <div className="shell-panel rounded-xl p-5">
      <div className="flex items-center justify-between mb-4">
        <div>
          <div className="flex items-center gap-2">
            <Activity className="w-4 h-4 text-slate-700" />
            <h2 className="text-sm font-semibold text-slate-900">Portal Health</h2>
          </div>
          {data && (
            <div className="flex items-center gap-3 mt-1 text-[11px]">
              <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-emerald-500" /><span className="text-slate-500">{data.stable_count} stable</span></span>
              <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-amber-400" /><span className="text-slate-500">{data.partial_count} partial</span></span>
              <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-rose-500" /><span className="text-slate-500">{data.unstable_count} unstable</span></span>
            </div>
          )}
        </div>
        <button onClick={() => mutate()} disabled={isValidating}
          className="p-1.5 rounded-lg text-slate-400 hover:text-slate-900 hover:bg-slate-100 transition-all disabled:opacity-40"
          title="Refresh">
          <RefreshCw className={cn("w-3.5 h-3.5", isValidating && "animate-spin")} />
        </button>
      </div>

      {isLoading && !data ? (
        <div className="space-y-2">
          {[0,1,2,3,4].map(i => <div key={i} className="h-10 shimmer rounded-lg" style={{ animationDelay: `${i*60}ms` }} />)}
        </div>
      ) : portals.length === 0 ? (
        <p className="py-8 text-center text-xs text-slate-400">
          NO RUN HISTORY YET<br />
          <span className="text-[10px] text-slate-400">Health recorded automatically after each run</span>
        </p>
      ) : (
        <div className="space-y-1.5">
          {visible.map(p => <PortalRow key={p.source} portal={p} />)}
          {portals.length > 7 && (
            <button onClick={() => setShowAll(o => !o)}
              className="w-full py-2 text-center text-xs text-slate-500 hover:text-slate-900 transition-colors">
              {showAll ? "SHOW LESS" : `+ ${portals.length - 7} MORE PORTALS`}
            </button>
          )}
        </div>
      )}

      {data?.generated_at && (
        <p className="mt-3 text-right text-[10px] text-slate-400">
          {new Date(data.generated_at).toLocaleTimeString()}
        </p>
      )}
    </div>
  );
}
