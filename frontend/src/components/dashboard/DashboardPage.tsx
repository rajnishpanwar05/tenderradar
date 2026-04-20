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
    <div className="flex flex-col w-full max-w-7xl mx-auto pb-24 pt-8 px-6 lg:px-12 selection:bg-indigo-500/30 selection:text-indigo-900 relative">
      
      {/* ─── AURORA BACKGROUND MESH ─── */}
      <div className="absolute top-0 right-10 w-[600px] h-[600px] bg-gradient-to-br from-indigo-300/40 via-purple-300/30 to-fuchsia-300/20 blur-[120px] rounded-full pointer-events-none -z-10 mix-blend-multiply" />
      <div className="absolute bottom-40 left-10 w-[500px] h-[500px] bg-gradient-to-tr from-cyan-300/40 via-emerald-300/20 to-teal-200/40 blur-[100px] rounded-full pointer-events-none -z-10 mix-blend-multiply" />

      {/* ─── HEADER ─── */}
      <motion.div 
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="flex items-center justify-between mb-12"
      >
        <div>
          <h1 className="text-4xl font-black tracking-tighter text-slate-900 flex items-center gap-3">
             <div className="p-2 bg-indigo-600 rounded-xl shadow-lg shadow-indigo-600/30">
               <Radar className="w-6 h-6 text-white" />
             </div>
             Command Center
          </h1>
          <p className="text-slate-500 mt-2 text-lg font-light">Global procurement intelligence network.</p>
        </div>
        <div className="hidden sm:flex items-center gap-3 px-4 py-2 rounded-full border border-emerald-200 bg-white/80 backdrop-blur-md shadow-sm">
          <div className="w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse shadow-[0_0_10px_2px_rgba(16,185,129,0.3)]" />
          <span className="text-xs font-bold text-emerald-700 tracking-widest uppercase">Neural Net Active</span>
        </div>
      </motion.div>

      {/* ─── COLORFUL METRICS ROW ─── */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-12">
        <MetricCard 
          delay={0.1}
          title="Indexed Opportunities" 
          value={data?.total_tenders ?? 0} 
          subtitle="Real-time global sync"
          icon={<Globe className="w-6 h-6 text-white" />}
          gradient="from-indigo-500 to-purple-600"
          shadow="shadow-indigo-500/20"
        />
        <MetricCard 
          delay={0.2}
          title="Critical Priority" 
          value={data?.high_priority_count ?? 0} 
          subtitle="Capability match > 80%"
          icon={<Activity className="w-6 h-6 text-white" />}
          gradient="from-rose-500 to-orange-500"
          shadow="shadow-rose-500/20"
        />
        <MetricCard 
          delay={0.3}
          title="Pipeline Velocity" 
          value="$14.2M" 
          subtitle="Estimated active bid value"
          icon={<TrendingUp className="w-6 h-6 text-white" />}
          gradient="from-emerald-500 to-teal-500"
          shadow="shadow-emerald-500/20"
        />
      </div>

      {/* ─── MAGNETIC FEED ─── */}
      <motion.div 
        initial={{ opacity: 0, y: 30 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.4 }}
        className="mb-6 flex items-center justify-between mt-16"
      >
        <h2 className="text-sm font-black tracking-[0.2em] uppercase text-indigo-600 flex items-center gap-2">
          <Fingerprint className="w-4 h-4" /> Action Required Feed
        </h2>
        <button 
          onClick={() => router.push("/tenders")}
          className="text-sm font-bold text-slate-500 hover:text-indigo-600 transition-colors flex items-center gap-2 group"
        >
          View Database <ArrowRight className="w-4 h-4 group-hover:translate-x-1 transition-transform" />
        </button>
      </motion.div>

      <motion.div 
        initial={{ opacity: 0, scale: 0.98 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ delay: 0.5 }}
        className="bg-white/70 backdrop-blur-3xl rounded-[2.5rem] border border-white shadow-[0_30px_60px_-15px_rgba(0,0,0,0.05)] overflow-hidden relative"
      >
        {/* Subtle top glow */}
        <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-indigo-500 via-purple-500 to-emerald-500 opacity-20" />
        
        {topTenders.length === 0 ? (
          <div className="py-20 text-center text-sm text-slate-500 font-mono tracking-widest uppercase">No target locks found.</div>
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
      className={`group flex items-center gap-6 px-6 py-5 rounded-2xl cursor-pointer transition-all duration-300 hover:bg-white hover:shadow-xl hover:shadow-indigo-500/5 border border-transparent hover:border-slate-100 relative overflow-hidden`}
    >
      {/* Priority Indicator */}
      <div className="flex-shrink-0 relative z-10">
        <div className={`flex flex-col items-center justify-center w-14 h-14 rounded-xl border ${
          score >= 90 ? "bg-gradient-to-br from-rose-500 to-orange-500 border-rose-500/30 text-white shadow-lg shadow-rose-500/30" :
          score >= 75 ? "bg-gradient-to-br from-cyan-500 to-blue-500 border-cyan-500/30 text-white shadow-lg shadow-cyan-500/30" :
          "bg-slate-100 border-slate-200 text-slate-500"
        }`}>
          <span className="font-mono text-xl font-black leading-none">{score}</span>
          <span className={`text-[7px] uppercase tracking-[0.2em] font-black ${score >= 75 ? "text-white/80" : "text-slate-400"}`}>Match</span>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 min-w-0 pr-4 relative z-10">
        <div className="flex flex-wrap items-center gap-2 mb-2">
          <span className="text-[10px] font-black tracking-[0.2em] uppercase text-indigo-700 bg-indigo-50 px-2.5 py-1 rounded-md border border-indigo-100">
            {portalLabel(tender.source_site)}
          </span>
          {tender.sector && tender.sector !== "unknown" && (
            <span className="text-[10px] font-bold tracking-widest uppercase text-slate-500">
              {sectorLabel(tender.sector)}
            </span>
          )}
        </div>
        <h3 className="text-base font-bold text-slate-900 truncate group-hover:text-indigo-600 transition-colors">
          {tender.title}
        </h3>
      </div>

      {/* Meta */}
      <div className="hidden md:flex items-center gap-8 flex-shrink-0 relative z-10">
        <div className="text-xs text-right">
          <div className="font-black text-slate-400 tracking-widest uppercase mb-1">Status</div>
          <div className="text-slate-800 flex items-center gap-2">
            {score >= 90 && <Flame className="w-4 h-4 text-rose-500" />}
            <span className="capitalize font-mono font-medium">{tender.deadline_category || "Analyzing"}</span>
          </div>
        </div>
        <div className="w-10 h-10 rounded-full bg-slate-50 flex items-center justify-center group-hover:bg-indigo-600 group-hover:text-white transition-colors border border-slate-200 shadow-sm">
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
      className={`p-8 rounded-[2rem] relative overflow-hidden group shadow-2xl ${shadow}`}
    >
      <div className={`absolute inset-0 bg-gradient-to-br ${gradient} transition-transform duration-700 group-hover:scale-105`} />
      <div className="absolute inset-0 bg-[url('https://grainy-gradients.vercel.app/noise.svg')] opacity-20 mix-blend-overlay pointer-events-none" />

      <div className="relative z-10 flex justify-between items-start mb-6">
        <span className="text-xs font-black tracking-widest uppercase text-white/80">{title}</span>
        <div className="p-2 rounded-xl bg-white/20 backdrop-blur-md shadow-lg border border-white/20">
          {icon}
        </div>
      </div>
      <div className="relative z-10 text-5xl font-black tracking-tighter text-white mb-2 drop-shadow-md">{value}</div>
      <div className="relative z-10 text-xs text-white/70 font-bold tracking-wide uppercase">{subtitle}</div>
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
