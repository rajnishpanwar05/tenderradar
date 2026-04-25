export const dynamic = "force-dynamic";
import type { Metadata } from "next";
import { apiClient } from "@/lib/api";
import { SectorBreakdownChart } from "@/components/analytics/SectorBreakdownChart";
import { PortalBreakdownTable } from "@/components/analytics/PortalBreakdownTable";
import { StatsKpiGrid } from "@/components/analytics/StatsKpiGrid";
import { Skeleton } from "@/components/ui/skeleton";
import { Suspense } from "react";
import { Radar, Activity, Zap } from "lucide-react";

export const metadata: Metadata = {
  title: "Intelligence Analytics",
  description: "Advanced procurement pipeline and system insights.",
};

async function AnalyticsContent() {
  const stats = await apiClient.server.getStats();

  return (
    <div className="space-y-8 relative z-10 w-full max-w-7xl mx-auto pb-20">
      
      {/* ── KPI Grid ── */}
      <StatsKpiGrid stats={stats} />

      {/* ── Main Charts Area ── */}
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 mt-8">
        {/* Sector Analytics / Radar & Bars */}
        <div className="col-span-2 rounded-[2.5rem] shell-panel-strong overflow-hidden relative">
          <div className="absolute top-0 right-0 p-8 opacity-[0.03] pointer-events-none">
            <Radar className="w-64 h-64 text-slate-900" />
          </div>
          <div className="p-8 pb-4 border-b border-slate-100 flex justify-between items-center bg-slate-50/70">
            <div>
              <h2 className="text-2xl font-semibold tracking-tight text-slate-900 flex items-center gap-3">
                <div className="p-2 bg-slate-100 text-slate-700 rounded-xl">
                  <Activity className="h-5 w-5" />
                </div>
                Sector Intelligence
              </h2>
              <p className="text-sm text-slate-500 mt-1">Multi-dimensional capability modeling across your targeted sectors.</p>
            </div>
          </div>
          <div className="p-8 pt-6">
            <SectorBreakdownChart data={stats.sector_breakdown} />
          </div>
        </div>

        {/* Engine Pipeline Status */}
        <div className="col-span-1 rounded-[2.5rem] shell-panel-strong relative overflow-hidden flex flex-col">
          <div className="absolute top-0 right-0 p-32 bg-slate-200/40 blur-[80px] rounded-full pointer-events-none" />
          <div className="p-8 pb-4 border-b border-slate-100 bg-slate-50/70 relative z-10">
            <h2 className="text-2xl font-semibold tracking-tight text-slate-900 flex items-center gap-3">
              <div className="p-2 bg-slate-100 text-slate-700 rounded-xl">
                <Zap className="h-5 w-5" />
              </div>
              Operations Pipeline
            </h2>
            <p className="text-sm text-slate-500 mt-1">Real-time telemetry.</p>
          </div>
          <div className="p-8 space-y-6 flex-1 bg-white relative z-10">
            
            <div className="group rounded-2xl p-5 border border-slate-100 bg-slate-50 hover:bg-white hover:shadow-md transition-all">
              <p className="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-1">Active Portals</p>
              <div className="flex items-end justify-between">
                <p className="text-4xl font-semibold text-slate-900">{stats.total_portals}</p>
                <div className="text-sm font-medium text-slate-600 bg-slate-100 px-2 py-1 rounded-md mb-1">+2 synced</div>
              </div>
            </div>

            <div className="group rounded-2xl p-5 border border-slate-100 bg-slate-50 hover:bg-white hover:shadow-md transition-all">
              <p className="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-1">Semantic Memory</p>
              <div className="flex items-end justify-between">
                <p className="text-4xl font-semibold text-slate-900">{stats.vector_store_docs.toLocaleString()}</p>
                <div className="text-sm font-medium text-slate-600 bg-slate-100 px-2 py-1 rounded-md mb-1">Vectors</div>
              </div>
            </div>
            
            <div className="group rounded-2xl p-5 border border-slate-100 bg-slate-50 hover:bg-white hover:shadow-md transition-all">
              <p className="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-1">Noise Blocked</p>
              <div className="flex items-end justify-between">
                <p className="text-4xl font-semibold text-slate-900">{stats.duplicate_count.toLocaleString()}</p>
                <div className="text-sm font-medium text-slate-600 bg-slate-100 px-2 py-1 rounded-md mb-1">Duplicates</div>
              </div>
            </div>

          </div>
        </div>
      </div>

      {/* ── Detailed coverage ── */}
      <div className="rounded-[2.5rem] shell-panel-strong overflow-hidden mt-8">
        <div className="p-8 border-b border-slate-100 bg-slate-50/70">
          <h2 className="text-2xl font-semibold tracking-tight text-slate-900 flex items-center gap-3">
             Portal Coverage Matrix
          </h2>
          <p className="text-sm text-slate-500 mt-1">Source origin integrity and scrape cadence.</p>
        </div>
        <div className="p-0">
          <PortalBreakdownTable portals={stats.portal_breakdown} />
        </div>
      </div>

    </div>
  );
}

function AnalyticsSkeleton() {
  return (
    <div className="space-y-6 max-w-7xl mx-auto w-full">
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
        {[1,2,3,4].map((i) => (
          <Skeleton key={i} className="h-32 w-full rounded-[2rem] bg-slate-100 border border-slate-200" />
        ))}
      </div>
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        <Skeleton className="h-[500px] rounded-[2.5rem] col-span-2 bg-slate-100 border border-slate-200" />
        <Skeleton className="h-[500px] rounded-[2.5rem] col-span-1 bg-slate-100 border border-slate-200" />
      </div>
    </div>
  );
}

export default function AnalyticsPage() {
  return (
    <div className="w-full relative px-6 lg:px-12 pt-8">
      <div className="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-slate-300 to-transparent" />

      <div className="max-w-7xl mx-auto relative z-10 mb-10">
        <h1 className="text-4xl font-semibold tracking-tight text-slate-950 flex items-center gap-3">
          <div className="p-2 bg-slate-900 rounded-xl shadow-sm">
            <Activity className="h-6 w-6 text-white" />
          </div>
          Intelligence Analytics
        </h1>
        <p className="text-lg text-slate-500 mt-2 max-w-2xl">
          Real-time intelligence mapping and capability extraction overview.
        </p>
      </div>

      <Suspense fallback={<AnalyticsSkeleton />}>
        <AnalyticsContent />
      </Suspense>
    </div>
  );
}
