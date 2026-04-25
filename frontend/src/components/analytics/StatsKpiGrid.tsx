"use client";
import { FileText, Clock, Calendar, Star } from "lucide-react";
import { KpiCard } from "./KpiCard";
import type { SystemStats } from "@/lib/api-types";

interface StatsKpiGridProps {
  stats: SystemStats;
}

export function StatsKpiGrid({ stats }: StatsKpiGridProps) {
  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4 w-full">
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
        subtitle="Newly published today"
      />
      <KpiCard
        title="Past 7 Days"
        value={stats.tenders_last_7_days}
        icon={Calendar}
        subtitle="Active pipeline volume"
      />
      <KpiCard
        title="High Match"
        value={stats.high_fit_count}
        icon={Star}
        subtitle="Score ≥ 80"
      />
    </div>
  );
}
