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
        <div className="col-span-2 rounded-[2.5rem] bg-white border border-slate-200 shadow-xl overflow-hidden relative">
          <div className="absolute top-0 right-0 p-8 opacity-[0.03] pointer-events-none">
            <Radar className="w-64 h-64 text-indigo-900" />
          </div>
          <div className="p-8 pb-4 border-b border-slate-100 flex justify-between items-center bg-slate-50/50">
            <div>
              <h2 className="text-2xl font-black tracking-tight text-slate-900 flex items-center gap-3">
                <div className="p-2 bg-indigo-100 text-indigo-600 rounded-xl">
                  <Activity className="h-5 w-5" />
                </div>
                Sector Intelligence
              </h2>
              <p className="text-sm text-slate-500 mt-1 font-medium">Multi-dimensional capability modeling across your targeted sectors.</p>
            </div>
          </div>
          <div className="p-8 pt-6">
            <SectorBreakdownChart data={stats.sector_breakdown} />
          </div>
        </div>

        {/* Engine Pipeline Status */}
        <div className="col-span-1 rounded-[2.5rem] border border-slate-200 bg-white shadow-xl relative overflow-hidden flex flex-col">
          <div className="absolute top-0 right-0 p-32 bg-cyan-500/10 blur-[80px] rounded-full pointer-events-none" />
          <div className="p-8 pb-4 border-b border-slate-100 bg-slate-50/50 relative z-10">
            <h2 className="text-2xl font-black tracking-tight text-slate-900 flex items-center gap-3">
              <div className="p-2 bg-emerald-100 text-emerald-600 rounded-xl">
                <Zap className="h-5 w-5" />
              </div>
              Neural Pipeline
            </h2>
            <p className="text-sm text-slate-500 mt-1 font-medium">Real-time telemetry.</p>
          </div>
          <div className="p-8 space-y-6 flex-1 bg-white relative z-10">
            
            <div className="group rounded-2xl p-5 border border-slate-100 bg-slate-50 hover:bg-white hover:shadow-lg transition-all">
              <p className="text-xs font-black text-slate-400 uppercase tracking-widest mb-1">Active Portals</p>
              <div className="flex items-end justify-between">
                <p className="text-4xl font-black text-slate-900">{stats.total_portals}</p>
                <div className="text-sm font-bold text-emerald-500 bg-emerald-50 px-2 py-1 rounded-md mb-1">+2 synced</div>
              </div>
            </div>

            <div className="group rounded-2xl p-5 border border-slate-100 bg-slate-50 hover:bg-white hover:shadow-lg transition-all">
              <p className="text-xs font-black text-slate-400 uppercase tracking-widest mb-1">Semantic Memory</p>
              <div className="flex items-end justify-between">
                <p className="text-4xl font-black text-slate-900">{stats.vector_store_docs.toLocaleString()}</p>
                <div className="text-sm font-bold text-indigo-500 bg-indigo-50 px-2 py-1 rounded-md mb-1">Vectors</div>
              </div>
            </div>
            
            <div className="group rounded-2xl p-5 border border-slate-100 bg-slate-50 hover:bg-white hover:shadow-lg transition-all">
              <p className="text-xs font-black text-slate-400 uppercase tracking-widest mb-1">Noise Blocked</p>
              <div className="flex items-end justify-between">
                <p className="text-4xl font-black text-slate-900">{stats.duplicate_count.toLocaleString()}</p>
                <div className="text-sm font-bold text-rose-500 bg-rose-50 px-2 py-1 rounded-md mb-1">Duplicates</div>
              </div>
            </div>

          </div>
        </div>
      </div>

      {/* ── Detailed coverage ── */}
      <div className="rounded-[2.5rem] border border-slate-200 bg-white shadow-xl overflow-hidden mt-8">
        <div className="p-8 border-b border-slate-100 bg-slate-50/50">
          <h2 className="text-2xl font-black tracking-tight text-slate-900 flex items-center gap-3">
             Portal Coverage Matrix
          </h2>
          <p className="text-sm text-slate-500 mt-1 font-medium">Source origin integrity and scrape cadence.</p>
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
      {/* Background aurora */}
      <div className="absolute top-0 right-10 w-[600px] h-[600px] bg-gradient-to-br from-indigo-300/40 via-purple-300/30 to-fuchsia-300/20 blur-[120px] rounded-full pointer-events-none -z-10 mix-blend-multiply" />
      <div className="absolute bottom-40 left-10 w-[500px] h-[500px] bg-gradient-to-tr from-cyan-300/40 via-emerald-300/20 to-teal-200/40 blur-[100px] rounded-full pointer-events-none -z-10 mix-blend-multiply" />

      <div className="max-w-7xl mx-auto relative z-10 mb-10">
        <h1 className="text-4xl font-black tracking-tighter text-slate-900 flex items-center gap-3">
          <div className="p-2 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-xl shadow-lg shadow-indigo-500/30">
            <Activity className="h-6 w-6 text-white" />
          </div>
          Deep Analytics
        </h1>
        <p className="text-lg font-light text-slate-500 mt-2 max-w-2xl">
          Real-time intelligence mapping and capability extraction overview.
        </p>
      </div>

      <Suspense fallback={<AnalyticsSkeleton />}>
        <AnalyticsContent />
      </Suspense>
    </div>
  );
}
