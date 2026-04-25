"use client";
import { useRouter } from "next/navigation";
import { useState, useCallback } from "react";
import { useTenders } from "@/hooks/useTenders";
import { TenderTable } from "./TenderTable";
import { EmptyState } from "./EmptyState";
import { sectorLabel } from "@/lib/constants";
import { Filter, X, ChevronDown } from "lucide-react";
import { cn } from "@/lib/utils";
import type { TenderFilters } from "@/lib/api-types";

const REGIONS = [
  "South Asia", "East Asia", "Africa", "Latin America",
  "Middle East", "Europe", "Global", "North America",
];

const SECTORS = [
  "health", "education", "environment", "agriculture",
  "water_sanitation", "governance", "energy", "infrastructure",
  "gender_inclusion", "research", "finance", "evaluation_monitoring",
];

const SORT_OPTIONS = [
  { value: "priority_score", label: "Priority Score" },
  { value: "title",          label: "Title" },
];

const DEFAULT: Partial<TenderFilters> = { page_size: 50, page: 1 };

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
    ...DEFAULT, page,
    sectors: sector ? [sector] : [],
    sector, region,
    min_priority: minPriority,
  } as Partial<TenderFilters>;

  const { data, isLoading, isValidating } = useTenders(extFilters);

  const results    = data?.results  ?? [];
  const total      = data?.total    ?? 0;
  const hasMore    = data?.has_more ?? false;
  const totalPages = Math.max(1, Math.ceil(total / 50));

  const handleTenderClick = useCallback(
    (id: string) => router.push(`/tenders/${encodeURIComponent(id)}`),
    [router],
  );

  function handleSort(field: string) {
    if (sortBy === field) setSortOrder((o) => o === "desc" ? "asc" : "desc");
    else { setSortBy(field); setSortOrder("desc"); }
  }

  function clearFilters() {
    setSector(""); setRegion(""); setMinPriority(0); setPage(1);
  }

  const hasActiveFilters = !!(sector || region || minPriority > 0);

  return (
    <div className="p-6 lg:p-8 max-w-[1400px] mx-auto">

      {/* Toolbar */}
      <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
        <div className="flex items-center gap-2">
          <span className="text-sm text-slate-500">
            {isLoading ? "Loading…" : <><strong className="text-slate-900">{total.toLocaleString()}</strong> opportunities</>}
          </span>
          {isValidating && !isLoading && (
            <span className="text-xs text-slate-400">Syncing…</span>
          )}
        </div>

        <div className="flex items-center gap-2">
          {/* Sort */}
          <div className="relative">
            <select
              value={sortBy}
              onChange={(e) => setSortBy(e.target.value)}
              className="appearance-none h-9 pl-3 pr-8 text-sm border border-slate-200 bg-white rounded-md text-slate-700 focus:outline-none focus:border-slate-400 cursor-pointer"
            >
              {SORT_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))}
            </select>
            <ChevronDown className="absolute right-2.5 top-2.5 h-4 w-4 text-slate-400 pointer-events-none" />
          </div>

          {/* Filter toggle */}
          <button
            onClick={() => setShowFilters((f) => !f)}
            className={cn(
              "flex items-center gap-1.5 h-9 px-3 text-sm font-medium rounded-md border transition-colors",
              showFilters
                ? "bg-slate-900 text-white border-slate-900"
                : "bg-white text-slate-700 border-slate-200 hover:bg-slate-50"
            )}
          >
            <Filter className="h-3.5 w-3.5" />
            Filters
            {hasActiveFilters && (
              <span className={cn(
                "flex h-4 w-4 items-center justify-center rounded-full text-[10px] font-semibold",
                showFilters ? "bg-white text-slate-900" : "bg-slate-900 text-white"
              )}>
                {[sector, region, minPriority > 0].filter(Boolean).length}
              </span>
            )}
          </button>
        </div>
      </div>

      {/* Filter bar */}
      {showFilters && (
        <div className="mb-4 p-4 bg-white border border-slate-200 rounded-lg shadow-sm flex flex-wrap items-end gap-4">
          <div className="flex flex-col gap-1 min-w-[180px]">
            <label className="text-xs font-medium text-slate-500">Sector</label>
            <select
              value={sector}
              onChange={(e) => { setSector(e.target.value); setPage(1); }}
              className="h-9 appearance-none border border-slate-200 rounded-md px-3 text-sm text-slate-700 bg-white focus:outline-none focus:border-slate-400"
            >
              <option value="">All sectors</option>
              {SECTORS.map((s) => <option key={s} value={s}>{sectorLabel(s)}</option>)}
            </select>
          </div>

          <div className="flex flex-col gap-1 min-w-[180px]">
            <label className="text-xs font-medium text-slate-500">Region</label>
            <select
              value={region}
              onChange={(e) => { setRegion(e.target.value); setPage(1); }}
              className="h-9 appearance-none border border-slate-200 rounded-md px-3 text-sm text-slate-700 bg-white focus:outline-none focus:border-slate-400"
            >
              <option value="">All regions</option>
              {REGIONS.map((r) => <option key={r} value={r}>{r}</option>)}
            </select>
          </div>

          <div className="flex flex-col gap-1 min-w-[200px]">
            <label className="text-xs font-medium text-slate-500">
              Min Score: <span className="font-semibold text-slate-700">{minPriority || "Any"}</span>
            </label>
            <input
              type="range" min={0} max={90} step={10}
              value={minPriority}
              onChange={(e) => { setMinPriority(Number(e.target.value)); setPage(1); }}
              className="w-full h-1.5 accent-slate-900 cursor-pointer"
            />
          </div>

          {hasActiveFilters && (
            <button
              onClick={clearFilters}
              className="flex items-center gap-1.5 h-9 px-3 text-sm text-slate-600 bg-slate-100 hover:bg-slate-200 rounded-md transition-colors ml-auto"
            >
              <X className="h-3.5 w-3.5" /> Reset
            </button>
          )}
        </div>
      )}

      {/* Active filter chips */}
      {hasActiveFilters && (
        <div className="flex flex-wrap gap-2 mb-4">
          {sector && (
            <FilterChip onRemove={() => setSector("")}>Sector: {sectorLabel(sector)}</FilterChip>
          )}
          {region && (
            <FilterChip onRemove={() => setRegion("")}>Region: {region}</FilterChip>
          )}
          {minPriority > 0 && (
            <FilterChip onRemove={() => setMinPriority(0)}>Score ≥ {minPriority}</FilterChip>
          )}
        </div>
      )}

      {/* Table */}
      <div className="bg-white border border-slate-200 rounded-lg shadow-sm overflow-hidden mb-4">
        {isLoading ? (
          <TableSkeleton />
        ) : results.length === 0 ? (
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

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <span className="text-sm text-slate-500">
            Page {page} of {totalPages}
          </span>
          <div className="flex gap-2">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page === 1}
              className="h-9 px-4 text-sm border border-slate-200 bg-white text-slate-700 rounded-md hover:bg-slate-50 disabled:opacity-40 transition-colors"
            >
              Previous
            </button>
            <button
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={!hasMore && page >= totalPages}
              className="h-9 px-4 text-sm border border-slate-200 bg-white text-slate-700 rounded-md hover:bg-slate-50 disabled:opacity-40 transition-colors"
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function FilterChip({ children, onRemove }: { children: React.ReactNode; onRemove: () => void }) {
  return (
    <span className="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium text-slate-700 border border-slate-200 bg-white rounded-md">
      {children}
      <button onClick={onRemove} className="text-slate-400 hover:text-slate-700 transition-colors">
        <X className="h-3 w-3" />
      </button>
    </span>
  );
}

function TableSkeleton() {
  return (
    <div className="divide-y divide-slate-100">
      {Array.from({ length: 8 }).map((_, i) => (
        <div key={i} className="px-5 py-4 flex items-center gap-4">
          <div className="w-20 h-5 bg-slate-100 rounded animate-pulse" />
          <div className="flex-1 h-4 bg-slate-100 rounded animate-pulse" />
          <div className="w-12 h-5 bg-slate-100 rounded animate-pulse" />
          <div className="w-24 h-5 bg-slate-100 rounded animate-pulse" />
        </div>
      ))}
    </div>
  );
}
