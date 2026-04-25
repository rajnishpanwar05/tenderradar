"use client";
import { PortalFreshnessIndicator } from "./PortalFreshnessIndicator";
import { portalLabel } from "@/lib/constants";
import type { PortalStats } from "@/lib/api-types";
import { cn } from "@/lib/utils";

interface PortalHealthCardProps {
  portal: PortalStats;
  maxTenders?: number;
}

function timeAgo(iso: string | null): string {
  if (!iso) return "never";
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

export function PortalHealthCard({ portal, maxTenders = 1 }: PortalHealthCardProps) {
  const displayName = portalLabel(portal.portal);
  const lastSeen = portal.last_scraped_at ? new Date(portal.last_scraped_at) : null;
  const hoursAgo = lastSeen ? (Date.now() - lastSeen.getTime()) / 3600000 : null;
  const isFresh = hoursAgo !== null && hoursAgo < 6;
  const isStale = hoursAgo !== null && hoursAgo > 24;
  const statusColor = isFresh ? "bg-emerald-500" : isStale ? "bg-red-500" : "bg-amber-400";
  const volumePct = maxTenders > 0 ? Math.min(100, (portal.total_tenders / maxTenders) * 100) : 0;

  return (
    <div className="bg-white border border-slate-200 rounded-xl shadow-sm p-4 hover:shadow-md hover:border-slate-300 transition-all">
      {/* Header */}
      <div className="flex items-start justify-between gap-2 mb-3">
        <div className="flex items-center gap-2">
          <span className={cn("w-2.5 h-2.5 rounded-full flex-shrink-0 mt-0.5", statusColor, isFresh && "animate-pulse")} />
          <div>
            <p className="text-sm font-semibold text-slate-900 leading-tight">{displayName}</p>
            <p className="text-[10px] text-slate-400 mt-0.5">{timeAgo(portal.last_scraped_at)}</p>
          </div>
        </div>
        <PortalFreshnessIndicator lastScrapedAt={portal.last_scraped_at} showLabel />
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 gap-2 mb-3">
        <Stat label="Total" value={portal.total_tenders.toLocaleString()} />
        <Stat label="New (7d)" value={`+${portal.new_last_7_days}`} highlight={portal.new_last_7_days > 0} />
        <Stat label="Avg Score" value={portal.avg_fit_score.toFixed(0)} />
        <Stat label="High Match" value={portal.high_fit_count.toLocaleString()} highlight={portal.high_fit_count > 0} />
      </div>

      {/* Volume bar (relative to max portal) */}
      <div className="mb-3">
        <div className="flex items-center justify-between text-xs text-slate-500 mb-1">
          <span>Relative volume</span>
          <span className="font-medium text-slate-700">{portal.total_tenders.toLocaleString()}</span>
        </div>
        <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden">
          <div
            className="h-full bg-slate-900 rounded-full transition-all"
            style={{ width: `${volumePct}%` }}
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
