"use client";

import { useSummary } from "@/hooks/useSummary";
import { useTenders } from "@/hooks/useTenders";
import type { SummaryResponse, TenderIntelItem } from "@/lib/api-types";
import { sectorLabel, portalLabel } from "@/lib/constants";
import { useRouter } from "next/navigation";
import { 
  ArrowRight, Activity, TrendingUp, Inbox, 
  ChevronRight, CircleDollarSign, Fingerprint, Flame, Globe, Radar
} from "lucide-react";
import { motion } from "framer-motion";

export function DashboardPage({ fallback }: { fallback?: SummaryResponse }) {
  const router = useRouter();
  const { data, isLoading } = useSummary(fallback);
  const { data: tendersData } = useTenders({ page_size: 5, page: 1 });
  
  const topTenders = tendersData?.results ?? [];
  
  if (isLoading && !data) return <DashboardSkeleton />;

  return (
    <div className="flex flex-col w-full max-w-7xl mx-auto pb-24 pt-8 px-6 lg:px-12 relative">

      {/* ─── HEADER ─── */}
      <motion.div 
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="flex items-center justify-between mb-12"
      >
        <div>
          <h1 className="text-4xl font-semibold tracking-tight text-slate-950 flex items-center gap-3">
             <div className="p-2 bg-slate-900 rounded-xl shadow-sm">
               <Radar className="w-6 h-6 text-white" />
             </div>
             Operations Dashboard
          </h1>
          <p className="text-slate-500 mt-2 text-lg">Global procurement intelligence at a glance.</p>
        </div>
        <div className="hidden sm:flex items-center gap-3 px-4 py-2 rounded-full border border-slate-200 bg-white shadow-sm">
          <div className="w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse" />
          <span className="text-xs font-semibold text-slate-600 tracking-widest uppercase">System live</span>
        </div>
      </motion.div>

      {/* ─── COLORFUL METRICS ROW ─── */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-12">
        <MetricCard 
          delay={0.1}
          title="Indexed Opportunities" 
          value={data?.total_tenders ?? 0} 
          subtitle="Real-time global sync"
          icon={<Globe className="w-6 h-6 text-slate-900" />}
          gradient="from-slate-900 to-slate-700"
          shadow="shadow-[0_18px_50px_rgba(15,23,42,0.08)]"
        />
        <MetricCard 
          delay={0.2}
          title="Critical Priority" 
          value={data?.high_priority_count ?? 0} 
          subtitle="Capability match > 80%"
          icon={<Activity className="w-6 h-6 text-slate-900" />}
          gradient="from-slate-700 to-slate-500"
          shadow="shadow-[0_18px_50px_rgba(15,23,42,0.08)]"
        />
        <MetricCard 
          delay={0.3}
          title="Pipeline Velocity" 
          value="$14.2M" 
          subtitle="Estimated active bid value"
          icon={<TrendingUp className="w-6 h-6 text-slate-900" />}
          gradient="from-slate-800 to-slate-600"
          shadow="shadow-[0_18px_50px_rgba(15,23,42,0.08)]"
        />
      </div>

      {/* ─── MAGNETIC FEED ─── */}
      <motion.div 
        initial={{ opacity: 0, y: 30 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.4 }}
        className="mb-6 flex items-center justify-between mt-16"
      >
        <h2 className="text-sm font-semibold tracking-[0.2em] uppercase text-slate-600 flex items-center gap-2">
          <Fingerprint className="w-4 h-4" /> Action Required Feed
        </h2>
        <button 
          onClick={() => router.push("/tenders")}
          className="text-sm font-medium text-slate-500 hover:text-slate-900 transition-colors flex items-center gap-2 group"
        >
          View Database <ArrowRight className="w-4 h-4 group-hover:translate-x-1 transition-transform" />
        </button>
      </motion.div>

      <motion.div 
        initial={{ opacity: 0, scale: 0.98 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ delay: 0.5 }}
        className="shell-panel-strong rounded-[2rem] overflow-hidden relative"
      >
        <div className="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-slate-400 to-transparent opacity-70" />
        
        {topTenders.length === 0 ? (
          <div className="py-20 text-center text-sm text-slate-500 font-medium tracking-widest uppercase">No opportunities found.</div>
        ) : (
          <div className="p-4 flex flex-col gap-2">
            {topTenders.map((t, idx) => (
              <TenderListItem key={t.tender_id} tender={t} onClick={() => router.push(`/tenders/${encodeURIComponent(t.tender_id)}`)} />
            ))}
          </div>
        )}
      </motion.div>

    </div>
  );
}

// ---------------------------------------------------------------------------
// Magnetic List Item
// ---------------------------------------------------------------------------
function TenderListItem({ tender, onClick }: { tender: TenderIntelItem, onClick: () => void }) {
  const score = tender.priority_score ?? 0;
  
  return (
    <div 
      onClick={onClick}
      className={`group flex items-center gap-6 px-6 py-5 rounded-2xl cursor-pointer transition-all duration-300 hover:bg-slate-50 border border-transparent hover:border-slate-200 relative overflow-hidden`}
    >
      {/* Priority Indicator */}
      <div className="flex-shrink-0 relative z-10">
        <div className={`flex flex-col items-center justify-center w-14 h-14 rounded-xl border ${
          score >= 90 ? "bg-slate-900 border-slate-900 text-white shadow-sm" :
          score >= 75 ? "bg-slate-800 border-slate-700 text-white shadow-sm" :
          "bg-slate-100 border-slate-200 text-slate-500"
        }`}>
          <span className="font-mono text-xl font-semibold leading-none">{score}</span>
          <span className={`text-[7px] uppercase tracking-[0.2em] font-semibold ${score >= 75 ? "text-white/80" : "text-slate-400"}`}>Match</span>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 min-w-0 pr-4 relative z-10">
        <div className="flex flex-wrap items-center gap-2 mb-2">
          <span className="text-[10px] font-semibold tracking-[0.2em] uppercase text-slate-700 bg-slate-100 px-2.5 py-1 rounded-md border border-slate-200">
            {portalLabel(tender.source_site)}
          </span>
          {tender.sector && tender.sector !== "unknown" && (
            <span className="text-[10px] font-semibold tracking-widest uppercase text-slate-500">
              {sectorLabel(tender.sector)}
            </span>
          )}
        </div>
        <h3 className="text-base font-semibold text-slate-900 truncate group-hover:text-slate-700 transition-colors">
          {tender.title}
        </h3>
      </div>

      {/* Meta */}
      <div className="hidden md:flex items-center gap-8 flex-shrink-0 relative z-10">
        <div className="text-xs text-right">
          <div className="font-semibold text-slate-400 tracking-widest uppercase mb-1">Status</div>
          <div className="text-slate-800 flex items-center gap-2">
            {score >= 90 && <Flame className="w-4 h-4 text-slate-500" />}
            <span className="capitalize font-mono font-medium">{tender.deadline_category || "Analyzing"}</span>
          </div>
        </div>
        <div className="w-10 h-10 rounded-full bg-slate-50 flex items-center justify-center group-hover:bg-slate-900 group-hover:text-white transition-colors border border-slate-200 shadow-sm">
          <ChevronRight className="w-5 h-5 text-slate-400 group-hover:text-white transition-colors" />
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Glowing Metric Card
// ---------------------------------------------------------------------------
function MetricCard({ title, value, subtitle, icon, delay, gradient, shadow }: any) {
  return (
    <motion.div 
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay }}
      className={`p-8 rounded-[2rem] relative overflow-hidden group shell-panel ${shadow}`}
    >
      <div className={`absolute inset-0 bg-gradient-to-br ${gradient} opacity-5 transition-transform duration-700 group-hover:scale-105`} />
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(15,23,42,0.06),transparent_25%)] pointer-events-none" />

      <div className="relative z-10 flex justify-between items-start mb-6">
        <span className="text-xs font-semibold tracking-widest uppercase text-slate-500">{title}</span>
        <div className="p-2 rounded-xl bg-slate-900 backdrop-blur-md shadow-sm border border-slate-900/10">
          {icon}
        </div>
      </div>
      <div className="relative z-10 text-5xl font-semibold tracking-tighter text-slate-950 mb-2">{value}</div>
      <div className="relative z-10 text-xs text-slate-500 font-medium tracking-wide uppercase">{subtitle}</div>
    </motion.div>
  );
}

function DashboardSkeleton() {
  return (
    <div className="flex flex-col w-full max-w-7xl mx-auto pb-24 pt-8 px-12">
      <div className="h-10 w-64 bg-slate-200 rounded mb-12 animate-pulse" />
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-12">
        {[1,2,3].map(i => <div key={i} className="h-40 bg-slate-100 rounded-[2rem] border border-slate-200 animate-pulse" />)}
      </div>
      <div className="h-6 w-48 bg-slate-200 rounded mb-6 animate-pulse" />
      <div className="bg-white border border-slate-200 rounded-[2.5rem] h-[500px] animate-pulse" />
    </div>
  );
}
