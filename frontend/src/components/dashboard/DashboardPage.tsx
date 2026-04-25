"use client";

import { useSummary } from "@/hooks/useSummary";
import { useTenders } from "@/hooks/useTenders";
import { usePortals } from "@/hooks/usePortals";
import type { SummaryResponse, TenderIntelItem, PortalStats } from "@/lib/api-types";
import { portalLabel, sectorLabel } from "@/lib/constants";
import { useRouter } from "next/navigation";
import { FileText, Globe, AlertTriangle, Clock, ChevronRight, ExternalLink } from "lucide-react";
import { cn } from "@/lib/utils";

export function DashboardPage({ fallback }: { fallback?: SummaryResponse }) {
  const router = useRouter();
  const { data, isLoading } = useSummary(fallback);
  const { data: tendersData, isLoading: tendersLoading } = useTenders({ page_size: 10, page: 1 });
  const { data: portalData } = usePortals();

  const topTenders = tendersData?.results ?? [];
  const portals = portalData ?? [];

  return (
    <div className="p-6 lg:p-8 max-w-7xl mx-auto space-y-6">

      {/* Stat cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
        <StatCard
          title="Total Tenders"
          value={isLoading ? null : (data?.total_tenders ?? 0)}
          icon={FileText}
        />
        <StatCard
          title="Active Portals"
          value={isLoading ? null : (data?.portals_active ?? 0)}
          icon={Globe}
        />
        <StatCard
          title="High Priority"
          value={isLoading ? null : (data?.high_priority_count ?? 0)}
          icon={AlertTriangle}
          accent="amber"
        />
        <StatCard
          title="Today's New"
          value={isLoading ? null : (data?.deadline_breakdown?.closing_soon?.total ?? 0)}
          icon={Clock}
          accent="emerald"
        />
      </div>

      {/* Recent tenders table */}
      <div className="bg-white border border-slate-200 rounded-lg shadow-sm overflow-hidden">
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-200">
          <h2 className="text-sm font-semibold text-slate-900">Recent Tenders</h2>
          <button
            onClick={() => router.push("/tenders")}
            className="text-xs text-slate-500 hover:text-slate-900 transition-colors flex items-center gap-1"
          >
            View all <ChevronRight className="w-3.5 h-3.5" />
          </button>
        </div>
        {tendersLoading ? (
          <div className="divide-y divide-slate-100">
            {Array.from({ length: 5 }).map((_, i) => (
              <div key={i} className="px-5 py-4 flex items-center gap-4">
                <div className="w-16 h-5 bg-slate-100 rounded animate-pulse" />
                <div className="flex-1 h-4 bg-slate-100 rounded animate-pulse" />
                <div className="w-10 h-5 bg-slate-100 rounded animate-pulse" />
              </div>
            ))}
          </div>
        ) : topTenders.length === 0 ? (
          <div className="py-12 text-center text-sm text-slate-500">No tenders found.</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-slate-50 border-b border-slate-200">
                  <th className="text-left px-5 py-3 text-xs font-medium text-slate-500 uppercase tracking-wide">Portal</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-slate-500 uppercase tracking-wide">Title</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-slate-500 uppercase tracking-wide">Sector</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-slate-500 uppercase tracking-wide">Score</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-slate-500 uppercase tracking-wide">Status</th>
                  <th className="w-10" />
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {topTenders.map((t) => (
                  <RecentTenderRow
                    key={t.tender_id}
                    tender={t}
                    onClick={() => router.push(`/tenders/${encodeURIComponent(t.tender_id)}`)}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Portal health grid */}
      {portals.length > 0 && (
        <div>
          <h2 className="text-sm font-semibold text-slate-900 mb-3">Portal Health</h2>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
            {portals.slice(0, 8).map((p) => (
              <MiniPortalCard key={p.portal} portal={p} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------

function StatCard({
  title, value, icon: Icon, accent,
}: {
  title: string;
  value: number | null;
  icon: React.ComponentType<{ className?: string }>;
  accent?: "amber" | "emerald";
}) {
  return (
    <div className="bg-white border border-slate-200 rounded-lg shadow-sm p-5">
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs font-medium text-slate-500 uppercase tracking-wide">{title}</p>
        <div className={cn(
          "w-8 h-8 rounded-md flex items-center justify-center",
          accent === "amber"   ? "bg-amber-50"   : accent === "emerald" ? "bg-emerald-50" : "bg-slate-100"
        )}>
          <Icon className={cn(
            "w-4 h-4",
            accent === "amber"   ? "text-amber-600"   : accent === "emerald" ? "text-emerald-600" : "text-slate-500"
          )} />
        </div>
      </div>
      {value === null ? (
        <div className="h-8 w-20 bg-slate-100 rounded animate-pulse" />
      ) : (
        <p className="text-2xl font-semibold text-slate-900 tabular-nums">{value.toLocaleString()}</p>
      )}
    </div>
  );
}

function RecentTenderRow({ tender, onClick }: { tender: TenderIntelItem; onClick: () => void }) {
  const score = tender.priority_score ?? 0;
  return (
    <tr className="hover:bg-slate-50 cursor-pointer transition-colors" onClick={onClick}>
      <td className="px-5 py-3.5">
        <span className="text-[10px] font-medium uppercase tracking-wide text-slate-600 bg-slate-100 border border-slate-200 px-2 py-0.5 rounded">
          {portalLabel(tender.source_site)}
        </span>
      </td>
      <td className="px-5 py-3.5 max-w-xs">
        <span className="text-sm text-slate-900 line-clamp-1 font-medium">{tender.title}</span>
      </td>
      <td className="px-5 py-3.5">
        <span className="text-xs text-slate-500">
          {tender.sector && tender.sector !== "unknown" ? sectorLabel(tender.sector) : "—"}
        </span>
      </td>
      <td className="px-5 py-3.5">
        <span className={cn(
          "text-xs font-semibold px-2 py-0.5 rounded border",
          score >= 80 ? "bg-emerald-50 text-emerald-700 border-emerald-200" :
          score >= 60 ? "bg-amber-50 text-amber-700 border-amber-200" :
          "bg-slate-100 text-slate-600 border-slate-200"
        )}>
          {score}
        </span>
      </td>
      <td className="px-5 py-3.5">
        <span className="text-xs text-slate-500 capitalize">{tender.deadline_category || "—"}</span>
      </td>
      <td className="px-3 py-3.5">
        <ChevronRight className="w-4 h-4 text-slate-300" />
      </td>
    </tr>
  );
}

function MiniPortalCard({ portal }: { portal: PortalStats }) {
  const lastSeen = portal.last_scraped_at
    ? new Date(portal.last_scraped_at)
    : null;
  const hoursAgo = lastSeen
    ? Math.round((Date.now() - lastSeen.getTime()) / 3600000)
    : null;
  const isFresh = hoursAgo !== null && hoursAgo < 6;
  const isStale = hoursAgo !== null && hoursAgo > 24;

  return (
    <div className="bg-white border border-slate-200 rounded-lg p-4 shadow-sm">
      <div className="flex items-start justify-between mb-2">
        <p className="text-sm font-semibold text-slate-900 leading-tight">{portalLabel(portal.portal)}</p>
        <span className={cn(
          "w-2 h-2 rounded-full flex-shrink-0 mt-1",
          isFresh ? "bg-emerald-500" : isStale ? "bg-red-500" : "bg-amber-400"
        )} />
      </div>
      <div className="flex items-center gap-3 text-xs text-slate-500">
        <span>{portal.total_tenders.toLocaleString()} tenders</span>
        {hoursAgo !== null && (
          <span>{hoursAgo}h ago</span>
        )}
      </div>
    </div>
  );
}
