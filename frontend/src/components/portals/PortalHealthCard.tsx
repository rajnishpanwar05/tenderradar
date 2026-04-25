"use client";
import { PortalFreshnessIndicator } from "./PortalFreshnessIndicator";
import { portalLabel } from "@/lib/constants";
import type { PortalStats } from "@/lib/api-types";
import { cn } from "@/lib/utils";

interface PortalHealthCardProps {
  portal: PortalStats;
}

export function PortalHealthCard({ portal }: PortalHealthCardProps) {
  const displayName = portalLabel(portal.portal);

  return (
    <div className="bg-white border border-slate-200 rounded-lg shadow-sm p-4 hover:shadow-md transition-shadow">
      {/* Header */}
      <div className="flex items-start justify-between gap-2 mb-3">
        <div>
          <p className="text-sm font-semibold text-slate-900 leading-tight">{displayName}</p>
          <p className="text-xs text-slate-400 mt-0.5">{portal.portal}</p>
        </div>
        <PortalFreshnessIndicator lastScrapedAt={portal.last_scraped_at} showLabel />
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 gap-2 mb-3">
        <Stat label="Total" value={portal.total_tenders.toLocaleString()} />
        <Stat label="New (7d)" value={`+${portal.new_last_7_days}`} />
        <Stat label="Avg Score" value={portal.avg_fit_score.toFixed(0)} />
        <Stat label="High Match" value={portal.high_fit_count.toLocaleString()} highlight={portal.high_fit_count > 0} />
      </div>

      {/* Score bar */}
      <div className="mb-3">
        <div className="flex items-center justify-between text-xs text-slate-500 mb-1">
          <span>Avg fit score</span>
          <span className="font-medium text-slate-700">{portal.avg_fit_score.toFixed(0)}</span>
        </div>
        <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden">
          <div
            className="h-full bg-slate-900 rounded-full transition-all"
            style={{ width: `${Math.min(100, portal.avg_fit_score)}%` }}
          />
        </div>
      </div>

      {/* CTA */}
      <button
        onClick={() => (window.location.href = `/tenders?source_portals=${encodeURIComponent(portal.portal)}`)}
        className="w-full h-8 text-xs font-medium text-slate-700 border border-slate-200 bg-white rounded-md hover:bg-slate-50 transition-colors"
      >
        Browse tenders →
      </button>
    </div>
  );
}

function Stat({ label, value, highlight }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div>
      <p className="text-[10px] text-slate-400 uppercase tracking-wide">{label}</p>
      <p className={cn("text-sm font-semibold tabular-nums", highlight ? "text-emerald-700" : "text-slate-800")}>
        {value}
      </p>
    </div>
  );
}
