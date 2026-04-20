"use client";
import { FileText, Clock, Calendar, Star } from "lucide-react";
import type { SystemStats } from "@/lib/api-types";

function KpiCard({ title, value, icon: Icon, subtitle, trend }: any) {
  return (
    <div className="relative group overflow-hidden rounded-[2rem] border border-slate-200 bg-white p-8 transition-all hover:-translate-y-1 hover:shadow-2xl hover:shadow-indigo-500/10">
      <div className="relative flex items-start justify-between">
        <div className="space-y-4">
          <p className="text-xs font-black tracking-widest text-slate-400 uppercase">{title}</p>
          <div className="flex items-baseline gap-3">
            <span className="text-5xl font-black text-slate-900 tracking-tighter leading-none">
              {value.toLocaleString()}
            </span>
            {trend === "up" && (
              <span className="flex items-center text-xs font-bold text-emerald-600 bg-emerald-50 px-2.5 py-1 rounded-md border border-emerald-100">
                +12%
              </span>
            )}
          </div>
          {subtitle && (
            <p className="text-sm font-medium text-slate-500 flex items-center gap-1">
              {subtitle}
            </p>
          )}
        </div>
        <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-slate-50 border border-slate-100 shadow-sm group-hover:bg-indigo-50 group-hover:border-indigo-100 transition-colors duration-500">
          <Icon className="h-8 w-8 text-slate-400 group-hover:text-indigo-600 transition-colors" />
        </div>
      </div>
    </div>
  );
}

interface StatsKpiGridProps {
  stats: SystemStats;
}

export function StatsKpiGrid({ stats }: StatsKpiGridProps) {
  return (
    <div className="grid grid-cols-1 gap-6 md:grid-cols-2 xl:grid-cols-4 w-full">
      <KpiCard
        title="Total Database"
        value={stats.total_tenders}
        icon={FileText}
        subtitle="Across all tracked portals"
      />
      <KpiCard
        title="Discovered (24h)"
        value={stats.tenders_last_24h}
        icon={Clock}
        trend="up"
        subtitle="Newly published today"
      />
      <KpiCard
        title="Past 7 Days"
        value={stats.tenders_last_7_days}
        icon={Calendar}
        subtitle="Active pipeline volume"
      />
      <KpiCard
        title="AI Match >= 80"
        value={stats.high_fit_count}
        icon={Star}
        subtitle="Highest priority opportunities"
      />
    </div>
  );
}
