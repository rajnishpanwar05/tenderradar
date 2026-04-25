export const dynamic = "force-dynamic";
import type { Metadata } from "next";
import { apiClient } from "@/lib/api";
import { SectorBreakdownChart } from "@/components/analytics/SectorBreakdownChart";
import { PortalBreakdownTable } from "@/components/analytics/PortalBreakdownTable";
import { StatsKpiGrid } from "@/components/analytics/StatsKpiGrid";
import { Skeleton } from "@/components/ui/skeleton";
import { Suspense } from "react";

export const metadata: Metadata = {
  title: "Analytics — ProcureIQ",
  description: "Procurement pipeline analytics and system insights.",
};

async function AnalyticsContent() {
  const stats = await apiClient.server.getStats();

  return (
    <div className="space-y-6 w-full max-w-7xl mx-auto">
      {/* KPI Grid */}
      <StatsKpiGrid stats={stats} />

      {/* Charts */}
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        {/* Sector chart */}
        <div className="col-span-2 bg-white border border-slate-200 rounded-lg shadow-sm overflow-hidden">
          <div className="px-5 py-4 border-b border-slate-200">
            <h2 className="text-sm font-semibold text-slate-900">Sector Breakdown</h2>
            <p className="text-xs text-slate-500 mt-0.5">Tenders by sector across all portals</p>
          </div>
          <div className="p-5">
            <SectorBreakdownChart data={stats.sector_breakdown} />
          </div>
        </div>

        {/* Operations panel */}
        <div className="col-span-1 bg-white border border-slate-200 rounded-lg shadow-sm overflow-hidden">
          <div className="px-5 py-4 border-b border-slate-200">
            <h2 className="text-sm font-semibold text-slate-900">System Overview</h2>
            <p className="text-xs text-slate-500 mt-0.5">Platform telemetry</p>
          </div>
          <div className="p-5 space-y-4">
            <OverviewItem label="Active Portals" value={stats.total_portals} />
            <OverviewItem label="Indexed Documents" value={stats.vector_store_docs} />
            <OverviewItem label="Duplicates Blocked" value={stats.duplicate_count} />
          </div>
        </div>
      </div>

      {/* Portal table */}
      <div className="bg-white border border-slate-200 rounded-lg shadow-sm overflow-hidden">
        <div className="px-5 py-4 border-b border-slate-200">
          <h2 className="text-sm font-semibold text-slate-900">Portal Coverage</h2>
          <p className="text-xs text-slate-500 mt-0.5">Source portal data and scrape cadence</p>
        </div>
        <div>
          <PortalBreakdownTable portals={stats.portal_breakdown} />
        </div>
      </div>
    </div>
  );
}

function OverviewItem({ label, value }: { label: string; value: number }) {
  return (
    <div className="flex items-center justify-between py-2 border-b border-slate-100 last:border-0">
      <span className="text-sm text-slate-600">{label}</span>
      <span className="text-sm font-semibold text-slate-900 tabular-nums">{value.toLocaleString()}</span>
    </div>
  );
}

function AnalyticsSkeleton() {
  return (
    <div className="space-y-6 max-w-7xl mx-auto w-full">
      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
        {[1, 2, 3, 4].map((i) => (
          <Skeleton key={i} className="h-28 w-full rounded-lg bg-slate-100" />
        ))}
      </div>
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        <Skeleton className="h-80 rounded-lg col-span-2 bg-slate-100" />
        <Skeleton className="h-80 rounded-lg col-span-1 bg-slate-100" />
      </div>
    </div>
  );
}

export default function AnalyticsPage() {
  return (
    <div className="p-6 lg:p-8">
      <Suspense fallback={<AnalyticsSkeleton />}>
        <AnalyticsContent />
      </Suspense>
    </div>
  );
}
