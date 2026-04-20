"use client";

import { useState } from "react";
import { Activity, RefreshCw, ChevronDown, ChevronUp } from "lucide-react";
import { cn } from "@/lib/utils";
import { usePortalHealth } from "@/hooks/usePortalHealth";
import type { PortalHealth, PortalStability } from "@/lib/api-types";

const STABILITY: Record<PortalStability, { dot: string; label: string; row: string }> = {
  stable:   { dot: "bg-emerald-500 shadow-[0_0_6px_1px] shadow-emerald-500/40", label: "Stable",   row: "border-emerald-500/10" },
  partial:  { dot: "bg-amber-400  shadow-[0_0_6px_1px] shadow-amber-400/40",   label: "Partial",  row: "border-amber-500/10" },
  unstable: { dot: "bg-red-500    shadow-[0_0_6px_1px] shadow-red-500/40",      label: "Unstable", row: "border-red-500/15" },
};

function PortalRow({ portal }: { portal: PortalHealth }) {
  const [expanded, setExpanded] = useState(false);
  const cfg = STABILITY[portal.stability];

  return (
    <div className={cn("rounded-lg border bg-white/[0.02] transition-colors", cfg.row, expanded && "bg-white/[0.04]")}>
      <button type="button" onClick={() => setExpanded(o => !o)}
        className="flex w-full items-center gap-2.5 px-3 py-2.5 text-left">
        <span className={cn("w-2 h-2 rounded-full flex-shrink-0", cfg.dot)} />
        <span className="flex-1 truncate text-sm text-white/70">{portal.source}</span>
        <span className="text-[11px] text-white/30 font-mono tabular-nums">{portal.success_rate.toFixed(0)}%</span>
        <span className="text-[11px] text-white/25 font-mono tabular-nums">~{Math.round(portal.average_rows)}r</span>
        {expanded ? <ChevronUp className="h-3 w-3 text-white/20" /> : <ChevronDown className="h-3 w-3 text-white/20" />}
      </button>

      {expanded && (
        <div className="px-3 pb-3 pt-0 space-y-1.5 border-t border-white/[0.06] text-xs text-white/35">
          <div className="flex justify-between pt-2">
            <span>Total runs</span>
            <span className="font-mono text-white/50">{portal.total_runs}</span>
          </div>
          <div className="flex justify-between">
            <span>Consecutive failures</span>
            <span className={cn("font-mono", portal.consecutive_failures >= 3 ? "text-red-400" : portal.consecutive_failures >= 1 ? "text-amber-400" : "text-white/50")}>
              {portal.consecutive_failures}
            </span>
          </div>
          {portal.last_success_time && (
            <div className="flex justify-between">
              <span>Last success</span>
              <span className="font-mono text-white/50">{new Date(portal.last_success_time).toLocaleDateString()}</span>
            </div>
          )}
          {portal.disabled_reason && (
            <p className="mt-2 rounded-md bg-red-500/10 px-2 py-1 text-red-400 border border-red-500/20">
              ⚠ {portal.disabled_reason}
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
    <div className="glass rounded-xl p-5 border border-white/[0.07]">
      <div className="flex items-center justify-between mb-4">
        <div>
          <div className="flex items-center gap-2">
            <Activity className="w-4 h-4 text-blue-400" />
            <h2 className="text-sm font-semibold text-white/80">Portal Health</h2>
          </div>
          {data && (
            <div className="flex items-center gap-3 mt-1 text-[11px]">
              <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-emerald-500" /><span className="text-white/30">{data.stable_count} stable</span></span>
              <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-amber-400" /><span className="text-white/30">{data.partial_count} partial</span></span>
              <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-red-500" /><span className="text-white/30">{data.unstable_count} unstable</span></span>
            </div>
          )}
        </div>
        <button onClick={() => mutate()} disabled={isValidating}
          className="p-1.5 rounded-lg text-white/25 hover:text-white/50 hover:bg-white/[0.06] transition-all disabled:opacity-40"
          title="Refresh">
          <RefreshCw className={cn("w-3.5 h-3.5", isValidating && "animate-spin")} />
        </button>
      </div>

      {isLoading && !data ? (
        <div className="space-y-2">
          {[0,1,2,3,4].map(i => <div key={i} className="h-10 shimmer rounded-lg" style={{ animationDelay: `${i*60}ms` }} />)}
        </div>
      ) : portals.length === 0 ? (
        <p className="py-8 text-center text-xs text-white/20 font-mono">
          NO RUN HISTORY YET<br />
          <span className="text-[10px] text-white/15">Health recorded automatically after each run</span>
        </p>
      ) : (
        <div className="space-y-1.5">
          {visible.map(p => <PortalRow key={p.source} portal={p} />)}
          {portals.length > 7 && (
            <button onClick={() => setShowAll(o => !o)}
              className="w-full py-2 text-center text-xs text-white/25 hover:text-white/45 transition-colors font-mono">
              {showAll ? "SHOW LESS" : `+ ${portals.length - 7} MORE PORTALS`}
            </button>
          )}
        </div>
      )}

      {data?.generated_at && (
        <p className="mt-3 text-right text-[10px] text-white/15 font-mono">
          {new Date(data.generated_at).toLocaleTimeString()}
        </p>
      )}
    </div>
  );
}
