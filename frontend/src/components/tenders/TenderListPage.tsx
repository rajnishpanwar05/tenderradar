"use client";
import { useRouter } from "next/navigation";
import { useState, useCallback } from "react";
import { useTenders } from "@/hooks/useTenders";
import { TenderTable } from "./TenderTable";
import { EmptyState } from "./EmptyState";
import { sectorLabel } from "@/lib/constants";
import { Filter, X, Search, Activity } from "lucide-react";
import { cn } from "@/lib/utils";
import type { TenderFilters } from "@/lib/api-types";
import { motion, AnimatePresence } from "framer-motion";

const REGIONS = [
  "South Asia", "East Asia", "Africa", "Latin America",
  "Middle East", "Europe", "Global", "North America",
];

const SECTORS = [
  "health", "education", "environment", "agriculture",
  "water_sanitation", "governance", "energy", "infrastructure",
  "gender_inclusion", "research", "finance", "evaluation_monitoring",
];

const DEFAULT: Partial<TenderFilters> = {
  page_size: 50,
  page:      1,
};

export function TenderListPage() {
  const router = useRouter();

  const [sector,      setSector]      = useState("");
  const [region,      setRegion]      = useState("");
  const [minPriority, setMinPriority] = useState(0);
  const [page,        setPage]        = useState(1);
  const [sortBy,      setSortBy]      = useState("priority_score");
  const [sortOrder,   setSortOrder]   = useState<"asc" | "desc">("desc");
  const [showFilters, setShowFilters] = useState(false);

  const extFilters = {
    ...DEFAULT,
    page,
    sectors:      sector ? [sector] : [],
    sector,
    region,
    min_priority: minPriority,
  } as Partial<TenderFilters>;

  const { data, isLoading, isValidating } = useTenders(extFilters);

  const results  = data?.results ?? [];
  const total    = data?.total   ?? 0;
  const hasMore  = data?.has_more ?? false;
  const totalPages = Math.max(1, Math.ceil(total / 50));

  const handleTenderClick = useCallback(
    (id: string) => router.push(`/tenders/${encodeURIComponent(id)}`),
    [router],
  );

  function handleSort(field: string) {
    if (sortBy === field) {
      setSortOrder((o) => o === "desc" ? "asc" : "desc");
    } else {
      setSortBy(field);
      setSortOrder("desc");
    }
  }

  function clearFilters() {
    setSector("");
    setRegion("");
    setMinPriority(0);
    setPage(1);
  }

  const hasActiveFilters = sector || region || minPriority > 0;

  return (
    <div className="flex flex-col w-full max-w-[1500px] mx-auto min-h-screen bg-[#fafafa] p-4 sm:p-8 md:p-12 fade-in selection:bg-blue-100 selection:text-blue-900">
      
      {/* ─── MASSIVE BREADCRUMB / HDR ─── */}
      <div className="flex flex-col gap-6 md:flex-row md:items-end md:justify-between mb-8 pb-8 border-b border-gray-200">
        <div>
          <div className="flex items-center gap-4 mb-2">
            <h1 className="text-4xl font-black tracking-tight text-slate-900">Tender Feed</h1>
            <AnimatePresence>
              {(isLoading || isValidating) && (
                <motion.span 
                  initial={{ opacity: 0, scale: 0.8 }}
                  animate={{ opacity: 1, scale: 1 }}
                  exit={{ opacity: 0, scale: 0.8 }}
                  className="inline-flex items-center gap-2 rounded-full border border-blue-200 bg-blue-50 px-3 py-1 text-xs font-bold tracking-widest text-blue-700 uppercase shadow-sm"
                >
                  <Activity className="h-3.5 w-3.5 animate-pulse" />
                  Syncing
                </motion.span>
              )}
            </AnimatePresence>
          </div>
          <p className="text-lg font-medium text-slate-500">
            {isLoading ? "Fetching intelligence…" : (
              <><strong className="text-slate-900 font-black">{total.toLocaleString()}</strong> active opportunities tracked by the LLM.</>
            )}
          </p>
        </div>
        
        <button
          onClick={() => setShowFilters((f) => !f)}
          className={cn(
            "flex items-center gap-2.5 rounded-full border px-6 py-3 text-sm font-bold transition-all shadow-sm active:scale-95",
            showFilters
              ? "border-blue-500 bg-blue-600 text-white shadow-md shadow-blue-500/20"
              : "border-gray-300 bg-white text-slate-700 hover:bg-slate-50 hover:border-gray-400",
          )}
        >
          <Filter className="h-4 w-4" />
          Filter & Refine
          {hasActiveFilters && (
            <span className={cn("flex h-5 w-5 items-center justify-center rounded-full text-[10px] font-black", showFilters ? "bg-white text-blue-600" : "bg-blue-100 text-blue-700")}>
              {[sector, region, minPriority > 0].filter(Boolean).length}
            </span>
          )}
        </button>
      </div>

      {/* ─── BRIGHT E-COMMERCE FILTER BAR ─── */}
      <AnimatePresence>
        {showFilters && (
          <motion.div 
            initial={{ opacity: 0, y: -20, height: 0 }}
            animate={{ opacity: 1, y: 0, height: "auto" }}
            exit={{ opacity: 0, y: -20, height: 0 }}
            className="overflow-hidden mb-8"
          >
            <div className="flex flex-wrap items-center gap-6 rounded-[2rem] border border-gray-200 bg-white p-6 shadow-[0_8px_30px_rgb(0,0,0,0.04)]">
              {/* Sector Dropdown */}
              <div className="flex flex-col gap-2 min-w-[200px]">
                <label className="text-[11px] font-black tracking-widest uppercase text-slate-400">Sector</label>
                <div className="relative">
                  <select
                    value={sector}
                    onChange={(e) => { setSector(e.target.value); setPage(1); }}
                    className="w-full h-12 appearance-none rounded-xl border border-gray-200 bg-gray-50 px-4 pr-10 text-sm font-bold text-slate-700 focus:outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-500/20 transition-all cursor-pointer"
                  >
                    <option value="">All sectors</option>
                    {SECTORS.map((s) => (
                      <option key={s} value={s}>{sectorLabel(s)}</option>
                    ))}
                  </select>
                  <Search className="absolute right-3.5 top-3.5 h-5 w-5 text-gray-400 pointer-events-none" />
                </div>
              </div>

              {/* Region Dropdown */}
              <div className="flex flex-col gap-2 min-w-[200px]">
                <label className="text-[11px] font-black tracking-widest uppercase text-slate-400">Region</label>
                <div className="relative">
                  <select
                    value={region}
                    onChange={(e) => { setRegion(e.target.value); setPage(1); }}
                    className="w-full h-12 appearance-none rounded-xl border border-gray-200 bg-gray-50 px-4 pr-10 text-sm font-bold text-slate-700 focus:outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-500/20 transition-all cursor-pointer"
                  >
                    <option value="">Global (All Regions)</option>
                    {REGIONS.map((r) => (
                      <option key={r} value={r}>{r}</option>
                    ))}
                  </select>
                  <GlobeIcon className="absolute right-3.5 top-3.5 h-5 w-5 text-gray-400 pointer-events-none" />
                </div>
              </div>

              {/* Minimum AI Score Slider */}
              <div className="flex flex-col gap-2 flex-grow max-w-sm pl-4 border-l border-gray-100">
                <div className="flex justify-between items-center">
                  <label className="text-[11px] font-black tracking-widest uppercase text-slate-400">Minimum AI Match</label>
                  <span className="text-sm font-black text-blue-600 bg-blue-50 px-2.5 py-0.5 rounded-md border border-blue-100">{minPriority || "Any Score"}</span>
                </div>
                <input
                  type="range"
                  min={0} max={90} step={10}
                  value={minPriority}
                  onChange={(e) => { setMinPriority(Number(e.target.value)); setPage(1); }}
                  className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer accent-blue-600 mt-3"
                />
              </div>

              {/* Clear Action */}
              {hasActiveFilters && (
                <button
                  onClick={clearFilters}
                  className="flex items-center gap-2 ml-auto rounded-full bg-slate-100 px-5 py-3.5 text-sm font-bold text-slate-500 hover:bg-slate-200 hover:text-slate-800 transition-colors"
                >
                  <X className="h-4 w-4" /> Reset 
                </button>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Active Filter Chips */}
      {hasActiveFilters && (
        <div className="flex flex-wrap gap-2 mb-8">
          {sector && <FilterChip onRemove={() => setSector("")}>Sector: {sectorLabel(sector)}</FilterChip>}
          {region && <FilterChip onRemove={() => setRegion("")}>Region: {region}</FilterChip>}
          {minPriority > 0 && <FilterChip onRemove={() => setMinPriority(0)}>AI Match ≥ {minPriority}</FilterChip>}
        </div>
      )}

      {/* ─── MASSIVE CLEAN LIST / TABLE ─── */}
      <div className="flex-1 min-h-0 bg-white rounded-[2rem] shadow-[0_4px_40px_rgba(0,0,0,0.03)] border border-gray-100 overflow-hidden mb-8">
        {results.length === 0 && !isLoading ? (
          <EmptyState onReset={clearFilters} />
        ) : (
          <TenderTable
            tenders={results}
            sortBy={sortBy}
            sortOrder={sortOrder}
            onSort={handleSort}
            onTenderClick={handleTenderClick}
          />
        )}
      </div>

      {/* ─── Pagination ─── */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between pb-10 px-4">
          <span className="text-sm font-bold text-slate-400 tracking-wide uppercase">
            Page <span className="text-slate-900 border border-gray-200 bg-white px-2.5 py-1 rounded-md mx-1">{page}</span> of {totalPages}
          </span>
          <div className="flex gap-4">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page === 1}
              className="rounded-full border border-gray-200 bg-white px-6 py-2.5 text-sm font-bold text-slate-700 shadow-sm hover:bg-slate-50 disabled:opacity-30 disabled:hover:bg-white transition-all active:scale-95"
            >
              ← Previous
            </button>
            <button
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={!hasMore && page >= totalPages}
              className="rounded-full border border-gray-200 bg-white px-6 py-2.5 text-sm font-bold text-slate-700 shadow-sm hover:bg-slate-50 disabled:opacity-30 disabled:hover:bg-white transition-all active:scale-95"
            >
              Next →
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function FilterChip({ children, onRemove }: { children: React.ReactNode; onRemove: () => void }) {
  return (
    <span className="inline-flex items-center gap-2 rounded-full border border-blue-200 bg-blue-50 px-3.5 py-1.5 text-xs font-bold text-blue-700 shadow-sm">
      {children}
      <button onClick={onRemove} className="rounded-full hover:bg-blue-200 p-0.5 transition-colors text-blue-500">
        <X className="h-3.5 w-3.5" />
      </button>
    </span>
  );
}

function GlobeIcon(props: any) {
  return (
    <svg {...props} xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>
  );
}
